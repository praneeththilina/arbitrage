#!/usr/bin/env python3
import sys
import os
import logging
import subprocess
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fh = logging.FileHandler("arbitrage.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(fh)


def free_port(port: int):
    try:
        result = subprocess.run(
            ["netstat", "-ano"], capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW
        )
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 5 and f":{port}" in parts[1] and "LISTENING" in parts[3]:
                pid = parts[4]
                subprocess.run(["taskkill", "/f", "/pid", pid],
                               capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
                time.sleep(1)
    except:
        pass


if __name__ == "__main__":
    free_port(8000)
    import uvicorn
    from config import settings
    uvicorn.run("ui.server:app", host=settings.ui_host, port=settings.ui_port, reload=settings.ui_reload)
