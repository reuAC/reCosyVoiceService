import os
import sys
import time
import json
import logging
import threading
import multiprocessing as mp
import redis.exceptions

from recosyvoice.config import settings
from recosyvoice.constants import (
    SPLITTER_TASK_QUEUE, WORKER_TASK_QUEUE, CONTEXT_KEY_PREFIX
)
from recosyvoice.utils.redis_utils import get_redis_client
from recosyvoice.utils.process_utils import shutdown_listener

def splitter_process_main():
    logger = logging.getLogger("分割器进程")
    mp.current_process().name = "Splitter"
    sys.path.append('third_party/Matcha-TTS')

    logger.info("进程启动中....")
    
    try:
        def get_lightweight_frontend(model_dir):
            from hyperpyyaml import load_hyperpyyaml
            from cosyvoice.cli.frontend import CosyVoiceFrontEnd
            from cosyvoice.utils.class_utils import get_model_type
            from cosyvoice.cli.model import CosyVoice2Model
            hyper_yaml_path = f'{model_dir}/cosyvoice2.yaml'
            if not os.path.exists(hyper_yaml_path): raise ValueError(f'{hyper_yaml_path} 未找到！')
            with open(hyper_yaml_path, 'r') as f:
                configs = load_hyperpyyaml(f, overrides={'qwen_pretrain_path': os.path.join(model_dir, 'CosyVoice-BlankEN')})
            assert get_model_type(configs) == CosyVoice2Model, f'请勿使用 {model_dir} 初始化 CosyVoice2！'
            frontend = CosyVoiceFrontEnd(configs['get_tokenizer'], configs['feat_extractor'], f'{model_dir}/campplus.onnx', f'{model_dir}/speech_tokenizer_v2.onnx', f'{model_dir}/spk2info.pt', configs['allowed_special'])
            return frontend

        r = get_redis_client()
        stop_event = threading.Event()
        threading.Thread(target=shutdown_listener, args=(stop_event, logger.name), daemon=True).start()

        logger.info(f"正在加载轻量级前端分割器 (模型路径: {settings.MODEL_PATH})...")
        frontend = get_lightweight_frontend(settings.MODEL_PATH)
        logger.info("前端分割器加载成功，进入待命状态")
        
        while not stop_event.is_set():
            try:
                task_data = r.brpop(SPLITTER_TASK_QUEUE, timeout=1)
                if task_data is None: continue
                task = json.loads(task_data[1])
                parent_job_id = task['parent_job_id']
                logger.info(f"收到长文本任务 {parent_job_id}，开始分割....")
                chunks = list(frontend.text_normalize(task['text'], split=True, text_frontend=True))
                num_chunks = len(chunks)
                if num_chunks == 0:
                    logger.warning(f"任务 {parent_job_id} 未能分割出任何文本块")
                    r.publish(task['notification_channel'], json.dumps({'status': 'error', 'message': '输入文本为空或无法被有效分割'}))
                    continue
                logger.info(f"任务 {parent_job_id} 已被分割成 {num_chunks} 个子任务")
                context_key = f"{CONTEXT_KEY_PREFIX}{parent_job_id}"
                context_data = {'num_chunks': num_chunks, 'notification_channel': task['notification_channel']}
                r.set(context_key, json.dumps(context_data), ex=settings.RESULT_EXPIRY_SECONDS)
                
                voice_details = {}
                if 'voice' in task:
                    voice_details['voice'] = task['voice']
                elif 'voice_path' in task:
                    voice_details['voice_path'] = task['voice_path']
                    voice_details['voice_prompt_text'] = task.get('voice_prompt_text', '')

                with r.pipeline() as pipe:
                    for i, chunk_text in enumerate(chunks):
                        sub_task = {
                            'parent_job_id': parent_job_id,
                            'sub_job_id': f"{parent_job_id}-{i}",
                            'text': chunk_text,
                            'retry_count': 0,
                            **voice_details
                        }
                        pipe.lpush(WORKER_TASK_QUEUE, json.dumps(sub_task))
                    pipe.execute()
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                logger.error("Redis 连接错误: %s", e); time.sleep(5)
                try: r = get_redis_client()
                except Exception as recon_e: logger.error("重连 Redis 失败: %s", recon_e)
            except Exception:
                job_id = locals().get('parent_job_id', 'N/A')
                logger.exception(f"处理父任务ID {job_id} 时发生未知错误")
                if 'task' in locals() and 'notification_channel' in locals()['task']:
                    r.publish(locals()['task']['notification_channel'], json.dumps({'status': 'error', 'message': '分割文本时发生内部错误'}))
    except KeyboardInterrupt:
        logger.info("收到退出信号，进程将退出")
    except Exception:
        logger.exception("初始化过程中发生严重错误，进程将退出")
    finally:
        logger.info("进程正常关闭")