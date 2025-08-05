import time
import uuid
import os
import torch
import torchaudio
import numpy as np
import sys
import asyncio
import multiprocessing as mp
import logging
import json
import httpx
from contextlib import suppress
from asyncio.exceptions import TimeoutError as AsyncTimeoutError
from asyncio.exceptions import CancelledError as AsyncCancelledError
from typing import List, Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import redis.asyncio as aioredis

from recosyvoice.config import settings
from recosyvoice.constants import (
    SPLITTER_TASK_QUEUE, NOTIFY_CHANNEL_PREFIX, RESULT_KEY_PREFIX, STOP_CHANNEL
)
from recosyvoice.api.models import SynthesisRequest
from recosyvoice.utils.redis_utils import get_redis_client, get_async_redis_client
from recosyvoice.utils.process_utils import watch_and_restart_process
from recosyvoice.processes.splitter import splitter_process_main
from recosyvoice.processes.worker import worker_process_main

try:
    if mp.get_start_method(allow_none=True) != 'spawn':
        mp.set_start_method('spawn', force=True)
        logging.info("多进程启动模式已成功设置为 'spawn'")
except Exception:
    logging.warning("设置 'spawn' 启动模式失败，可能已被设置或环境不支持")

app = FastAPI(title="ReCoSyVoice API", version="1.2.0")
api_logger = logging.getLogger("主应用API")
app.state.shutdown_event = asyncio.Event()

def perform_startup_checks():
    if not os.path.isdir(settings.MODEL_PATH): return f"模型路径不存在 -> {settings.MODEL_PATH}"
    if not settings.PROMPTS_CONFIG: return "PROMPTS_CONFIG 为空"
    valid_voices = set()
    for prompt in settings.PROMPTS_CONFIG:
        if not os.path.exists(prompt.path): return f"提示音文件不存在 -> {prompt.path}"
        valid_voices.add(prompt.name)
    app.state.valid_voices = valid_voices
    try:
        import pynvml
        pynvml.nvmlInit()
        device_count = pynvml.nvmlDeviceGetCount()
        pynvml.nvmlShutdown()
        if device_count == 0: return "未检测到任何 GPU"
        return device_count
    except Exception as e:
        return f"GPU 检测失败: {e}"

@app.on_event("startup")
async def startup_event():
    api_logger.info("服务启动中...")
    app.state.is_ready = False
    
    try:
        app.state.redis_client = get_redis_client()
        app.state.redis_client.ping()
        app.state.async_redis_client = await get_async_redis_client()
        await app.state.async_redis_client.ping()
        api_logger.info("Redis连接成功")
    except Exception as e:
        app.state.error_message = f"启动失败：无法连接到 Redis，错误: {e}"
        api_logger.critical(app.state.error_message)
        return

    api_logger.info("正在清理旧的任务和状态...")
    keys_to_delete = list(app.state.redis_client.scan_iter(f"{settings.REDIS_PREFIX}*"))
    if keys_to_delete: app.state.redis_client.delete(*keys_to_delete)
    api_logger.info(f"已清理 {len(keys_to_delete)} 个旧的Redis键")

    error_message = perform_startup_checks()
    if isinstance(error_message, str):
        app.state.error_message = f"启动失败: {error_message}"
        api_logger.critical(app.state.error_message)
        return

    device_count = error_message
    import pynvml
    pynvml.nvmlInit()
    deployment_plan = [0] * device_count
    if settings.GPU_MEMORY_PER_MODEL_MB == -1:
        for i in range(settings.NUM_MODELS): deployment_plan[i % device_count] += 1
    else:
        free_mem_per_gpu = [pynvml.nvmlDeviceGetMemoryInfo(pynvml.nvmlDeviceGetHandleByIndex(i)).free / (1024**2) for i in range(device_count)]
        deployed_count = 0
        while deployed_count < settings.NUM_MODELS:
            best_gpu_idx = np.argmax(free_mem_per_gpu)
            if free_mem_per_gpu[best_gpu_idx] < settings.GPU_MEMORY_PER_MODEL_MB: break
            deployment_plan[best_gpu_idx] += 1
            free_mem_per_gpu[best_gpu_idx] -= settings.GPU_MEMORY_PER_MODEL_MB
            deployed_count += 1
    pynvml.nvmlShutdown()
    api_logger.info(f"部署计划 (每张GPU的模型数): {deployment_plan}")
    
    all_proc_configs = [{"target": splitter_process_main, "args": (), "name": "Splitter"}]
    for gpu_id, num_workers in enumerate(deployment_plan):
        for i in range(num_workers):
            all_proc_configs.append({"target": worker_process_main, "args": (gpu_id, i), "name": f"Worker-GPU{gpu_id}-{i}"})

    app.state.managed_processes_info = []
    app.state.monitor_tasks = []
    for proc_config in all_proc_configs:
        process = mp.Process(target=proc_config["target"], args=proc_config["args"], daemon=True)
        process.start()
        proc_info = {**proc_config, "process": process}
        app.state.managed_processes_info.append(proc_info)
        monitor_task = asyncio.create_task(watch_and_restart_process(app.state, proc_info))
        app.state.monitor_tasks.append(monitor_task)

    deployed_count = len(all_proc_configs) - 1
    if deployed_count > 0:
        app.state.is_ready = True
        api_logger.info(f"成功启动并监听 {deployed_count} 个工作进程和1个分割器进程，服务准备就绪")
    else:
        app.state.error_message = "启动失败：未能部署任何工作进程"
        api_logger.critical(app.state.error_message)
        if hasattr(app.state, 'redis_client'): app.state.redis_client.publish(STOP_CHANNEL, 'stop')

@app.on_event("shutdown")
async def shutdown_event():
    api_logger.info("服务关闭中...")
    app.state.shutdown_event.set()
    if hasattr(app.state, 'redis_client'):
        api_logger.info("发送停止信号....")
        app.state.redis_client.publish(STOP_CHANNEL, 'stop')
        app.state.redis_client.close()
    
    if hasattr(app.state, 'monitor_tasks') and app.state.monitor_tasks:
        api_logger.info("等待所有进程监听器完成....")
        await asyncio.gather(*app.state.monitor_tasks, return_exceptions=True)
        api_logger.info("所有进程监听器已停止")

    procs_info = getattr(app.state, 'managed_processes_info', [])
    for proc_info in procs_info:
        p = proc_info['process']
        if p.is_alive():
            api_logger.warning(f"进程 {proc_info['name']} (PID: {p.pid}) 未能按照信号退出，将强制终止")
            p.terminate(); p.join()

    if hasattr(app.state, 'async_redis_client'): await app.state.async_redis_client.close()
    api_logger.info("服务关闭完成")

app.mount("/audio", StaticFiles(directory=settings.STATIC_DIR), name="audio")

@app.get("/", summary="服务状态检查", tags=["状态"])
async def root():
    if not getattr(app.state, 'is_ready', False):
        return {"status": "未就绪", "reason": getattr(app.state, 'error_message', '启动中发生错误')}
    return {"status": "就绪", "available_voices": list(app.state.valid_voices)}

@app.get("/voices", response_model=List[str], summary="获取可用音色列表", tags=["音色"])
async def get_voices():
    if not getattr(app.state, 'is_ready', False): raise HTTPException(status_code=503, detail="服务尚未就绪")
    return list(app.state.valid_voices)

async def wait_for_notification(pubsub: aioredis.client.PubSub, timeout: int = 600):
    try:
        async for message in pubsub.listen():
            if message["type"] == "message": return json.loads(message["data"])
    except AsyncCancelledError:
        api_logger.warning("等待通知的任务被客户端取消")
        raise
    raise AsyncTimeoutError(f"在 {timeout} 秒内未收到通知")

@app.post("/synthesize", summary="合成音频", tags=["合成"])
async def synthesize_audio(request: Request, body: SynthesisRequest):
    if not getattr(app.state, 'is_ready', False): raise HTTPException(status_code=503, detail=f"服务未就绪: {getattr(app.state, 'error_message', '未知错误')}")
    if not settings.VALID_API_KEYS or body.apikey not in settings.VALID_API_KEYS: raise HTTPException(status_code=401, detail="无效的 API Key")
    if body.voice and body.voice not in app.state.valid_voices: raise HTTPException(status_code=400, detail=f"无效音色: '{body.voice}'. 可用: {list(app.state.valid_voices)}")

    start_process_time = time.time()
    parent_job_id = str(uuid.uuid4())
    r_async = app.state.async_redis_client
    notification_channel = f"{NOTIFY_CHANNEL_PREFIX}{parent_job_id}"
    temp_voice_path = None
    voice_info = {}

    try:
        if body.voice_url:
            api_logger.info(f"收到URL音色合成请求 (URL: '{body.voice_url[:70]}...'), 父任务ID: {parent_job_id}")
            temp_voice_path = os.path.join(settings.TEMP_VOICE_DIR, f"{parent_job_id}.wav")
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.get(body.voice_url, follow_redirects=True, timeout=30.0)
                    resp.raise_for_status()
                    with open(temp_voice_path, "wb") as f:
                        f.write(resp.content)
                except (httpx.RequestError, httpx.HTTPStatusError) as e:
                    raise HTTPException(status_code=400, detail=f"无法下载或访问音色URL: {e}")

            try:
                await asyncio.to_thread(torchaudio.info, temp_voice_path)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"提供的URL不是有效的音频文件: {e}")
            
            voice_info = {"voice_path": temp_voice_path, "voice_prompt_text": body.voice_prompt_text}
        else:
            api_logger.info(f"收到合成请求 (音色: '{body.voice}'), 父任务ID: {parent_job_id}")
            voice_info = {"voice": body.voice}

        async with r_async.pubsub() as pubsub:
            await pubsub.subscribe(notification_channel)
            task = {
                'parent_job_id': parent_job_id,
                'text': body.target,
                'notification_channel': notification_channel,
                **voice_info
            }
            await r_async.lpush(SPLITTER_TASK_QUEUE, json.dumps(task))
            
            try:
                notification = await asyncio.wait_for(wait_for_notification(pubsub), timeout=600.0)
            except AsyncTimeoutError:
                api_logger.error(f"任务 {parent_job_id} 等待结果超时")
                return JSONResponse(status_code=504, content={"error": "任务处理超时"})
            except AsyncCancelledError:
                api_logger.warning(f"客户端断开连接，任务 {parent_job_id} 取消")
                return
            
            if notification.get('status') == 'error':
                api_logger.error(f"任务 {parent_job_id} 处理失败: {notification.get('message')}")
                return JSONResponse(status_code=500, content={"error": notification.get('message', '未知错误')})
            
            num_chunks = notification.get('num_chunks')
            api_logger.info(f"任务 {parent_job_id} 所有子任务处理完毕，共 {num_chunks} 块，开始合并...")

        temp_files_to_clean = []
        try:
            audio_chunks, sample_rate = [], None
            for i in range(num_chunks):
                sub_job_id = f"{parent_job_id}-{i}"
                result_str = await r_async.get(f"{RESULT_KEY_PREFIX}{sub_job_id}")
                if not result_str: raise RuntimeError(f"无法获取子任务 {sub_job_id} 的结果")
                res = json.loads(result_str)
                if sample_rate is None: sample_rate = res['sample_rate']
                temp_path = res['path']
                temp_files_to_clean.append(temp_path)
                wav, _ = await asyncio.to_thread(torchaudio.load, temp_path)
                audio_chunks.append(wav)
                await r_async.delete(f"{RESULT_KEY_PREFIX}{sub_job_id}")

            final_output_wav = torch.cat(audio_chunks, dim=1)
            output_filename = f"{parent_job_id}.wav"
            output_path = os.path.join(settings.STATIC_DIR, output_filename)
            await asyncio.to_thread(torchaudio.save, output_path, final_output_wav, sample_rate)
            
            process_duration = time.time() - start_process_time
            api_logger.info(f"任务 {parent_job_id} 合并完成，总耗时: {process_duration:.2f} 秒")
            return {"download_url": f"{str(request.base_url).strip('/')}/audio/{output_filename}", "processing_time_seconds": round(process_duration, 2), "num_chunks": num_chunks}
        except Exception as e:
            api_logger.error(f"任务 {parent_job_id} 在合并阶段失败", exc_info=True)
            return JSONResponse(status_code=500, content={"error": f"合并音频时发生错误: {e}"})
        finally:
            if temp_files_to_clean:
                api_logger.info(f"正在为任务 {parent_job_id} 清理 {len(temp_files_to_clean)} 个临时音频文件")
                for f_path in temp_files_to_clean:
                    try: await asyncio.to_thread(os.remove, f_path)
                    except OSError as e: api_logger.warning(f"清理临时文件 {f_path} 失败: {e}")
    finally:
        if temp_voice_path and os.path.exists(temp_voice_path):
            try:
                await asyncio.to_thread(os.remove, temp_voice_path)
                api_logger.info(f"已清理临时音色文件: {temp_voice_path}")
            except OSError as e:
                api_logger.warning(f"清理临时音色文件 {temp_voice_path} 失败: {e}")