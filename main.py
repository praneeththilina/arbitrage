import sys
import os
import socket
import subprocess
import logging
import traceback


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    file_handler = logging.FileHandler("arbitrage.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root.addHandler(file_handler)


def log_unhandled(exctype, value, tb):
    logging.critical("Unhandled exception", exc_info=(exctype, value, tb))


def free_port(port: int):
    try:
        output = subprocess.check_output(
            f"netstat -ano | findstr \":{port}\"", shell=True
        ).decode()
        for line in output.splitlines():
            if "LISTENING" in line:
                parts = line.strip().split()
                pid = parts[-1]
                subprocess.run(["taskkill", "/f", "/pid", pid],
                               capture_output=True)
                logging.info(f"Killed PID {pid} holding port {port}")
    except subprocess.CalledProcessError:
        pass


sys.excepthook = log_unhandled
setup_logging()

import uvicorn
from config import settings

if __name__ == "__main__":
    logging.info("Application starting")
    free_port(settings.ui_port)
    try:
        uvicorn.run(
            "ui.server:app",
            host=settings.ui_host,
            port=settings.ui_port,
            reload=settings.ui_reload,
            log_level="info",
        )
    except Exception as e:
        logging.critical(f"Startup failed: {e}\n{traceback.format_exc()}")
        sys.exit(1)
