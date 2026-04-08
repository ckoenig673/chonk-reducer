import os

APP_VERSION = (os.getenv("APP_VERSION", "1.51.0") or "1.51.0").strip() or "1.51.0"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
