import os

APP_VERSION = (os.getenv("APP_VERSION", "1.46.6") or "1.46.6").strip() or "1.46.6"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
