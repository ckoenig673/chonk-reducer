import os

APP_VERSION = (os.getenv("APP_VERSION", "1.43.2") or "1.43.2").strip() or "1.43.2"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
