import os

APP_VERSION = (os.getenv("APP_VERSION", "dev") or "dev").strip() or "dev"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
