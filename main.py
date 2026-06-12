import uvicorn
from config import settings

if __name__ == "__main__":
    uvicorn.run(
        "ui.server:app",
        host=settings.ui_host,
        port=settings.ui_port,
        reload=settings.ui_reload,
        log_level="info",
    )
