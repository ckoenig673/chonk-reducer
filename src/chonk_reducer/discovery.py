from __future__ import annotations

from pathlib import Path

from .config import Config


def is_excluded(path: Path, cfg: Config) -> bool:
    parts_lower = [p.lower() for p in path.parts]
    for ex in cfg.exclude_path_parts:
        ex_l = ex.lower()
        if any(ex_l == part for part in parts_lower):
            return True
        if any(ex_l in part for part in parts_lower):
            return True
    return False


def gather_candidates(cfg: Config) -> list[Path]:
    min_bytes = int(cfg.min_size_gb * 1024**3)
    cands: list[Path] = []
    for p in cfg.media_root.rglob("*.mkv"):
        try:
            if is_excluded(p, cfg):
                continue
            if ".bak." in p.name:
                continue
            if p.name.endswith(".encoded.mkv"):
                continue
            if p.stat().st_size < min_bytes:
                continue
            cands.append(p)
        except FileNotFoundError:
            continue
        except Exception:
            continue

    cands.sort(key=lambda x: x.stat().st_size, reverse=True)
    return cands
