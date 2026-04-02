import os

APP_VERSION = (os.getenv("APP_VERSION", "1.46.16") or "1.46.16").strip() or "1.46.16"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
