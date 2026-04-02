import os

APP_VERSION = (os.getenv("APP_VERSION", "1.46.13") or "1.46.13").strip() or "1.46.13"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
