from recosyvoice.config import settings

REDIS_PREFIX = settings.REDIS_PREFIX
SPLITTER_TASK_QUEUE = f"{REDIS_PREFIX}splitter_tasks"
WORKER_TASK_QUEUE = f"{REDIS_PREFIX}worker_tasks"
PROCESSING_QUEUE_PREFIX = f"{REDIS_PREFIX}processing:"
DEAD_LETTER_QUEUE = f"{REDIS_PREFIX}dead_letter_queue"
STOP_CHANNEL = f"{REDIS_PREFIX}stop_channel"
RESULT_KEY_PREFIX = f"{REDIS_PREFIX}result:"
CONTEXT_KEY_PREFIX = f"{REDIS_PREFIX}context:"
COUNTER_KEY_PREFIX = f"{REDIS_PREFIX}counter:"
NOTIFY_CHANNEL_PREFIX = f"{REDIS_PREFIX}notify:"