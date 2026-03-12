import os

APP_VERSION = (os.getenv("APP_VERSION", "1.42.1") or "1.42.1").strip() or "1.42.1"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
