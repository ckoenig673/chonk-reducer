from __future__ import annotations

from pathlib import Path

from .config import Config


def evaluate_skip(src: Path, probe: dict | None, cfg: Config) -> tuple[str, str] | None:
    """Return (category, reason) if the file should be skipped, else None.

    Categories:
      - "codec"
      - "resolution"
    """
    # 1) Codec-based skip
    codec = (probe or {}).get("codec")
    if codec:
        c = str(codec).lower()
        if getattr(cfg, "skip_codecs", ()):
            if c in cfg.skip_codecs:
                return ("codec", f"codec={c}")

    # 2) Resolution-based skip (height)
    h = (probe or {}).get("height")
    try:
        h_i = int(h) if h is not None else None
    except Exception:
        h_i = None

    if h_i is not None and getattr(cfg, "skip_min_height", 0):
        if h_i >= int(cfg.skip_min_height):
            return ("resolution", f"height={h_i}")

    # 3) Resolution tags (filename)
    if getattr(cfg, "skip_resolution_tags", ()):
        name_l = src.name.lower()
        for tag in cfg.skip_resolution_tags:
            t = str(tag).lower()
            if t and t in name_l:
                return ("resolution", f"tag={t}")

    return None
