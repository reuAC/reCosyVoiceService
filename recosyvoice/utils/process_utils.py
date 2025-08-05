import threading
import logging
import asyncio
import multiprocessing as mp
from typing import Dict, Any
import redis.exceptions
from recosyvoice.utils.redis_utils import get_redis_client
from recosyvoice.constants import STOP_CHANNEL, PROCESSING_QUEUE_PREFIX, WORKER_TASK_QUEUE

def shutdown_listener(stop_event: threading.Event, logger_name: str):
    """用于监听Redis的关闭信号"""
    logger = logging.getLogger(logger_name)
    r = get_redis_client()
    pubsub = r.pubsub()
    pubsub.subscribe(STOP_CHANNEL)
    logger.info("关闭信号监听器已启动")
    try:
        for message in pubsub.listen():
            if message['type'] == 'message' and message['data'] == 'stop':
                logger.info("收到关闭信号，准备退出...")
                stop_event.set()
                break
    except (redis.exceptions.ConnectionError, OSError):
        logger.warning("关闭监听器在监听时连接中断")
    finally:
        pubsub.close()
        logger.info("关闭信号监听器已停止")

async def watch_and_restart_process(app_state: Any, proc_info: Dict[str, Any]):
    """用于监听子进程，若子进程意外退出，则重启"""
    is_worker = "Worker" in proc_info["name"]
    monitor_logger = logging.getLogger(f"监听器-{proc_info['name']}")
    r_sync = get_redis_client()

    while not app_state.shutdown_event.is_set():
        process = proc_info["process"]
        pid = process.pid
        
        if is_worker:
            gpu_id, worker_idx = proc_info["args"]
            worker_id = f"gpu{gpu_id}-idx{worker_idx}-pid{pid}"
            processing_queue = f"{PROCESSING_QUEUE_PREFIX}{worker_id}"
            monitor_logger.info(f"正在监听工作进程 {worker_id} (PID: {pid})...")
        else:
            monitor_logger.info(f"正在监听分割器进程 (PID: {pid})...")

        await asyncio.to_thread(process.join)
        
        if app_state.shutdown_event.is_set():
            monitor_logger.info(f"进程 (PID: {pid}) 已正常关闭")
            break

        monitor_logger.warning(f"检测到进程 (PID: {pid}) 已意外退出 (退出码: {process.exitcode})")

        if is_worker:
            try:
                monitor_logger.info(f"正在检查 {processing_queue} 中的遗留任务...")
                recovered_count = 0
                while True:
                    task_data = r_sync.rpoplpush(processing_queue, WORKER_TASK_QUEUE)
                    if task_data is None: break
                    recovered_count += 1
                if recovered_count > 0:
                    monitor_logger.warning(f"成功从 {processing_queue} 恢复了 {recovered_count} 个任务到主队列")
            except Exception as e:
                monitor_logger.error(f"恢复遗留任务时发生错误: {e}", exc_info=True)
        
        monitor_logger.info("将在2秒后重启进程...")
        await asyncio.sleep(2)

        new_process = mp.Process(target=proc_info["target"], args=proc_info["args"], daemon=True)
        new_process.start()
        proc_info["process"] = new_process
        monitor_logger.info(f"已成功重启进程 {proc_info['name']}，新PID: {new_process.pid}")
    
    r_sync.close()