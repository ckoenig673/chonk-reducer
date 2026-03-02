from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    return (os.environ.get(name) or default).strip()


def _env_int(name: str, default: int) -> int:
    v = _env(name, str(default))
    try:
        return int(v)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    v = _env(name, str(default))
    try:
        return float(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, "1" if default else "0").lower()
    return v in ("1", "true", "yes", "y", "on")


def _parse_mode(val: str, default: str) -> int:
    s = (val or "").strip() or default
    if s.startswith(("0o", "0O")):
        return int(s, 8)
    if s.isdigit():
        return int(s, 8)
    return int(default, 8)


def _split_csv(val: str) -> list[str]:
    out: list[str] = []
    for raw in (val or "").split(","):
        p = raw.strip()
        if p:
            out.append(p)
    return out


@dataclass(frozen=True)
class Config:
    media_root: Path
    work_root: Path

    min_size_gb: float
    max_files: int

    qsv_quality: int
    qsv_preset: int

    post_encode_validate: bool
    validate_mode: str
    validate_seconds: int

    out_uid: int
    out_gid: int
    out_mode: int
    out_dir_mode: int

    bak_retention_days: int
    log_retention_days: int

    lock_stale_hours: int
    work_cleanup_hours: int

    probe_timeout_secs: int
    ffprobe_analyzeduration: int
    ffprobe_probesize: int

    exclude_path_parts: tuple[str, ...]

    log_prefix: str

    # NEW
    fail_fast: bool


def load_config() -> Config:
    excl = _split_csv(_env("EXCLUDE_PATH_PARTS", "#recycle,@eaDir"))

    return Config(
        media_root=Path(_env("MEDIA_ROOT", "/movies")),
        work_root=Path(_env("WORK_ROOT", "/work")),

        min_size_gb=_env_float("MIN_SIZE_GB", 18.0),
        max_files=_env_int("MAX_FILES", 2),

        qsv_quality=_env_int("QSV_QUALITY", 21),
        qsv_preset=_env_int("QSV_PRESET", 7),

        post_encode_validate=_env_bool("POST_ENCODE_VALIDATE", True),
        validate_mode=_env("VALIDATE_MODE", "decode"),
        validate_seconds=_env_int("VALIDATE_SECONDS", 10),

        out_uid=_env_int("OUT_UID", 1028),
        out_gid=_env_int("OUT_GID", 100),
        out_mode=_parse_mode(_env("OUT_MODE", "664"), "664"),
        out_dir_mode=_parse_mode(_env("OUT_DIR_MODE", "775"), "775"),

        bak_retention_days=_env_int("BAK_RETENTION_DAYS", 60),
        log_retention_days=_env_int("LOG_RETENTION_DAYS", 30),

        lock_stale_hours=_env_int("LOCK_STALE_HOURS", 12),
        work_cleanup_hours=_env_int("WORK_CLEANUP_HOURS", 0),

        probe_timeout_secs=_env_int("PROBE_TIMEOUT_SECS", 60),
        ffprobe_analyzeduration=_env_int("FFPROBE_ANALYZEDURATION", 50_000_000),
        ffprobe_probesize=_env_int("FFPROBE_PROBESIZE", 50_000_000),

        exclude_path_parts=tuple(excl),

        log_prefix=_env("LOG_PREFIX", ""),

        # NEW
        fail_fast=_env_bool("FAIL_FAST", False),
    )