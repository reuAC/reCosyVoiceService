import os
import sys
import time
import json
import logging
import atexit
import threading
import multiprocessing as mp
import redis.exceptions

from recosyvoice.config import settings
from recosyvoice.constants import (
    PROCESSING_QUEUE_PREFIX, WORKER_TASK_QUEUE, DEAD_LETTER_QUEUE,
    RESULT_KEY_PREFIX, COUNTER_KEY_PREFIX, CONTEXT_KEY_PREFIX
)
from recosyvoice.utils.redis_utils import get_redis_client
from recosyvoice.utils.process_utils import shutdown_listener

def worker_process_main(gpu_id: int, worker_idx: int):
    worker_id = f"gpu{gpu_id}-idx{worker_idx}-pid{os.getpid()}"
    logger = logging.getLogger(f"工作进程-{worker_id}")
    mp.current_process().name = f"Worker-{worker_id}"
    
    try:
        processing_queue = f"{PROCESSING_QUEUE_PREFIX}{worker_id}"
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)
        stop_event = threading.Event()
        threading.Thread(target=shutdown_listener, args=(stop_event, logger.name), daemon=True).start()
        
        import torch, torchaudio, librosa
        sys.path.append('third_party/Matcha-TTS')
        from cosyvoice.cli.cosyvoice import CosyVoice2
        from cosyvoice.utils.file_utils import load_wav
        
        r = get_redis_client()

        def cleanup_processing_queue():
            logger.info(f"进程退出前清理'处理中'队列 {processing_queue}....")
            try:
                clean_r = get_redis_client()
                while True:
                    task_data = clean_r.rpoplpush(processing_queue, WORKER_TASK_QUEUE)
                    if task_data is None: break
                    logger.warning(f"一个未完成的任务被归还至主队列")
                clean_r.close()
            except Exception as e:
                logger.error(f"清理'处理中'队列时发生错误: {e}")
            logger.info("清理完成")

        atexit.register(cleanup_processing_queue)

        def postprocess(speech, top_db=60, hop_length=220, win_length=440):
            speech, _ = librosa.effects.trim(speech, top_db=top_db, frame_length=win_length, hop_length=hop_length)
            if torch.abs(speech).max() > 0.8: speech = speech / torch.abs(speech).max() * 0.8
            speech = torch.concat([speech, torch.zeros(1, int(22050 * 0.2))], dim=1)
            return speech

        logger.info("正在加载 CosyVoice 模型...")
        model = CosyVoice2(settings.MODEL_PATH, load_jit=False, load_trt=False, fp16=False)
        preloaded_prompts = {}
        for prompt_info in settings.PROMPTS_CONFIG:
            raw_speech = load_wav(prompt_info.path, 16000)
            preloaded_prompts[prompt_info.name] = {"speech": postprocess(raw_speech), "text": prompt_info.text}
        logger.info("初始化完成，进入待命状态")

        while not stop_event.is_set():
            task_data = None
            task = None
            try:
                task_data = r.brpoplpush(WORKER_TASK_QUEUE, processing_queue, timeout=1)
                if task_data is None: continue
                
                task = json.loads(task_data)
                parent_job_id, sub_job_id = task['parent_job_id'], task['sub_job_id']
                logger.info(f"开始处理子任务 {sub_job_id}")
                
                prompt_data = None
                if task.get('voice_path'):
                    logger.info(f"使用临时音色文件: {task['voice_path']}")
                    raw_speech = load_wav(task['voice_path'], 16000)
                    prompt_speech = postprocess(raw_speech)
                    prompt_text = task.get('voice_prompt_text', '')
                    prompt_data = {"speech": prompt_speech, "text": prompt_text}
                else:
                    voice_name = task.get('voice')
                    if not voice_name: raise ValueError("任务中既未提供 voice 也未提供 voice_path")
                    prompt_data = preloaded_prompts.get(voice_name)
                    if not prompt_data: raise ValueError(f"无效的预设音色名称: {voice_name}")

                output_wav = None
                for _, chunk in enumerate(model.inference_zero_shot(task['text'], prompt_data["text"], prompt_data["speech"], stream=False, text_frontend=False)):
                    output_wav = chunk['tts_speech']
                if output_wav is None: raise RuntimeError("模型未能生成有效的音频数据")

                temp_path = os.path.join(settings.TEMP_AUDIO_DIR, f"{sub_job_id}.wav")
                torchaudio.save(temp_path, output_wav, model.sample_rate)

                pipe = r.pipeline()
                result_payload = {'status': 'success', 'path': temp_path, 'sample_rate': model.sample_rate}
                pipe.set(f"{RESULT_KEY_PREFIX}{sub_job_id}", json.dumps(result_payload), ex=settings.RESULT_EXPIRY_SECONDS)
                counter_key = f"{COUNTER_KEY_PREFIX}{parent_job_id}"
                pipe.incr(counter_key)
                pipe.expire(counter_key, settings.RESULT_EXPIRY_SECONDS)
                pipe.lrem(processing_queue, 1, task_data)
                results = pipe.execute()
                
                logger.info(f"子任务 {sub_job_id} 处理成功")
                completed_count = results[1]
                
                context_str = r.get(f"{CONTEXT_KEY_PREFIX}{parent_job_id}")
                if context_str:
                    context_data = json.loads(context_str)
                    if completed_count >= context_data['num_chunks']:
                        logger.info(f"父任务 {parent_job_id} 的所有子任务已全部完成")
                        r.publish(context_data['notification_channel'], json.dumps({'status': 'success', 'num_chunks': context_data['num_chunks']}))
                        r.delete(f"{CONTEXT_KEY_PREFIX}{parent_job_id}", counter_key)
                
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                logger.error("Redis 连接错误: %s", e)
                time.sleep(5)
            except Exception as e:
                if task_data is None:
                    logger.error(f"发生未知错误（无任务数据）: {e}", exc_info=True)
                    continue

                current_retry = task.get('retry_count', 0)
                if current_retry < settings.WORKER_RETRY_ATTEMPTS:
                    task['retry_count'] = current_retry + 1
                    logger.warning(f"处理子任务 {task['sub_job_id']} 时出错: {e}. 将在 {settings.WORKER_RETRY_DELAY_SECONDS}s 后重试 ({current_retry+1}/{settings.WORKER_RETRY_ATTEMPTS})")
                    pipe = r.pipeline()
                    pipe.lrem(processing_queue, 1, task_data)
                    pipe.lpush(WORKER_TASK_QUEUE, json.dumps(task))
                    pipe.execute()
                    time.sleep(settings.WORKER_RETRY_DELAY_SECONDS)
                else:
                    logger.error(f"子任务 {task['sub_job_id']} 在尝试 {settings.WORKER_RETRY_ATTEMPTS+1} 次后仍然失败，移入死信队列", exc_info=True)
                    dead_letter = {'failed_at': time.time(), 'worker_id': worker_id, 'error': str(e), 'task': task}
                    pipe = r.pipeline()
                    pipe.lpush(DEAD_LETTER_QUEUE, json.dumps(dead_letter))
                    pipe.lrem(processing_queue, 1, task_data)
                    pipe.execute()

                    context_str = r.get(f"{CONTEXT_KEY_PREFIX}{task['parent_job_id']}")
                    if context_str:
                        context_data = json.loads(context_str)
                        error_payload = {'status': 'error', 'message': f"子任务 {task['sub_job_id']} 合成失败: {e}"}
                        r.publish(context_data['notification_channel'], json.dumps(error_payload))
                        r.delete(f"{CONTEXT_KEY_PREFIX}{task['parent_job_id']}", f"{COUNTER_KEY_PREFIX}{task['parent_job_id']}")
    
    except KeyboardInterrupt:
        logger.info("收到退出信号，进程将退出")
    except Exception:
        logger.exception("初始化或主循环中发生严重错误，进程将退出")
    finally:
        logger.info("进程正常关闭")