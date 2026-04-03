import os

APP_VERSION = (os.getenv("APP_VERSION", "1.48.2") or "1.48.2").strip() or "1.48.2"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
