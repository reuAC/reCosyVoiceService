import os
import logging
import json
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class PromptDetail(BaseModel):
    name: str
    path: str
    text: str

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    MODEL_PATH: str = "pretrained_models/CosyVoice2-0.5B"
    GPU_MEMORY_PER_MODEL_MB: int = 5000
    NUM_MODELS: int = 8
    PROMPTS_CONFIG: List[PromptDetail]
    VALID_API_KEYS: List[str] = []

    STATIC_DIR: str = "static/audio"
    TEMP_AUDIO_DIR: str = "static/audio/temp_chunks"
    TEMP_VOICE_DIR: str = "static/audio/temp_voices"

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PREFIX: str = "cosyvoice:"
    RESULT_EXPIRY_SECONDS: int = 3600

    WORKER_RETRY_ATTEMPTS: int = 3
    WORKER_RETRY_DELAY_SECONDS: int = 2

settings = Settings()

os.makedirs("prompts", exist_ok=True)
os.makedirs(settings.STATIC_DIR, exist_ok=True)
os.makedirs(settings.TEMP_AUDIO_DIR, exist_ok=True)
os.makedirs(settings.TEMP_VOICE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] [%(process)d] %(message)s",
    force=True,
)
logging.getLogger("torch").setLevel(logging.WARNING)
logging.getLogger("torchaudio").setLevel(logging.WARNING)
logging.getLogger("uvicorn").setLevel(logging.INFO)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("redis").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)