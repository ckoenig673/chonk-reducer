import os

APP_VERSION = (os.getenv("APP_VERSION", "1.43.5") or "1.43.5").strip() or "1.43.5"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
