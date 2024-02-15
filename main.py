import subprocess
import uvicorn
from loguru import logger

from src.config import AppSettings
from src.api import app

settings = AppSettings()

if __name__ == "__main__":
    if settings.debug_mode:
        uvicorn.run(app,
            host='localhost',
            port=settings.http_port,
            reload=False,
        )
    else:
        subprocess.run(["gunicorn",
            "main:app",
            "--bind", f"0.0.0.0:{settings.http_port}",
            "--worker-class", "uvicorn.workers.UvicornWorker",
        ], check=True)
