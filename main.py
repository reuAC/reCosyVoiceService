import uvicorn
import logging

from recosyvoice.config import settings
from recosyvoice.api.main import app

if __name__ == "__main__":
    logging.info("服务即将启动...")
    try:
        uvicorn.run(
            "recosyvoice.api.main:app",
            host="0.0.0.0",
            port=8000,
            log_level="info"
        )
    except KeyboardInterrupt:
        logging.info("服务已关闭。")
        pass