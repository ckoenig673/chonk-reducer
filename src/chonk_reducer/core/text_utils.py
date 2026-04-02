from __future__ import annotations


def normalize_csv_text(value: str) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in str(value or "").split(","):
        token = raw.strip().lower()
        if not token or token in seen:
            continue
        parts.append(token)
        seen.add(token)
    return ",".join(parts)


def sanitize_token(value: str, replacement: str = "-") -> str:
    return "".join(ch if ch.isalnum() else replacement for ch in str(value))
