from __future__ import annotations


def build_web_app(fastapi_cls, fallback_app_cls):
    if fastapi_cls is not None:
        return fastapi_cls(title="Chonk Reducer Service")
    return fallback_app_cls()
