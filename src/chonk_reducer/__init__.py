import os

APP_VERSION = (os.getenv("APP_VERSION", "1.46.22") or "1.46.22").strip() or "1.46.22"

__all__ = ["__version__", "APP_VERSION"]
__version__ = APP_VERSION
