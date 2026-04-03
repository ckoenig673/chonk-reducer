import os

APP_VERSION = (os.getenv("APP_VERSION", "1.46.24") or "1.46.24").strip() or "1.46.24"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
