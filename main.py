import sys
import logging
import traceback

logging.basicConfig(
    filename="arbitrage.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def log_unhandled(exctype, value, tb):
    logger.critical("Unhandled exception", exc_info=(exctype, value, tb))


sys.excepthook = log_unhandled

import uvicorn
from config import settings

if __name__ == "__main__":
    logger.info("Application starting")
    try:
        uvicorn.run(
            "ui.server:app",
            host=settings.ui_host,
            port=settings.ui_port,
            reload=settings.ui_reload,
            log_level="info",
        )
    except Exception as e:
        logger.critical(f"Startup failed: {e}\n{traceback.format_exc()}")
        sys.exit(1)
