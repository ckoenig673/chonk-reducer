# src/chonk_reducer/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name)
    if v is None:
        if default is None:
            return ""
        return default
    return v.strip()


def _env_int(name: str, default: int) -> int:
    v = _env(name, "")
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _parse_mode(val: str, default: str) -> int:
    """
    Accepts:
      "664" -> 0o664
      "0775" -> 0o775
      "0o775" -> 0o775
      "775" -> 0o775
    """
    s = (val or "").strip()
    if not s:
        s = default.strip()

    # allow "0o775"
    if s.lower().startswith("0o"):
        s = s[2:]

    # if looks like octal digits, parse base 8
    # common inputs: 664, 775, 0775
    try:
        if all(c in "01234567" for c in s):
            return int(s, 8)
    except Exception:
        pass

    # fallback: try int as-is
    try:
        return int(s)
    except Exception:
        return int(default, 8) if all(c in "01234567" for c in default) else 0o664


@dataclass(frozen=True)
class Config:
    # Core paths
    media_root: Path
    work_root: Path

    # Selection
    min_size_gb: int
    max_files: int

    # QSV
    qsv_quality: int
    qsv_preset: int

    # Validation
    post_encode_validate: bool
    validate_mode: str
    validate_seconds: int

    # Permissions
    out_uid: int
    out_gid: int
    out_mode: int
    out_dir_mode: int

    # Housekeeping
    bak_retention_days: int
    log_retention_days: int

    # Locking / cleanup
    lock_stale_hours: int
    work_cleanup_hours: int

    # ffprobe tuning
    probe_timeout_secs: int
    ffprobe_analyzeduration: int
    ffprobe_probesize: int

    # misc
    exclude_path_parts: list[str]
    log_prefix: str


def load_config() -> Config:
    media_root = Path(_env("MEDIA_ROOT", "/movies"))
    work_root = Path(_env("WORK_ROOT", "/work"))

    min_size_gb = _env_int("MIN_SIZE_GB", 20)
    max_files = _env_int("MAX_FILES", 2)

    qsv_quality = _env_int("QSV_QUALITY", 21)
    qsv_preset = _env_int("QSV_PRESET", 7)

    post_encode_validate = _env("POST_ENCODE_VALIDATE", "1") in ("1", "true", "TRUE", "yes", "YES")
    validate_mode = _env("VALIDATE_MODE", "probe").lower()
    validate_seconds = _env_int("VALIDATE_SECONDS", 10)

    out_uid = _env_int("OUT_UID", 1028)
    out_gid = _env_int("OUT_GID", 100)

    out_mode = _parse_mode(_env("OUT_MODE", "664"), "664")
    out_dir_mode = _parse_mode(_env("OUT_DIR_MODE", "775"), "775")

    bak_retention_days = _env_int("BAK_RETENTION_DAYS", 60)
    log_retention_days = _env_int("LOG_RETENTION_DAYS", 30)

    lock_stale_hours = _env_int("LOCK_STALE_HOURS", 12)
    work_cleanup_hours = _env_int("WORK_CLEANUP_HOURS", 24)

    probe_timeout_secs = _env_int("PROBE_TIMEOUT_SECS", 60)
    ffprobe_analyzeduration = _env_int("FFPROBE_ANALYZEDURATION", 50_000_000)
    ffprobe_probesize = _env_int("FFPROBE_PROBESIZE", 50_000_000)

    # default matches what you’ve been using
    raw_excludes = _env("EXCLUDE_PATH_PARTS", "#recycle,@eadir,@eaDir")
    exclude_path_parts = [p.strip() for p in raw_excludes.split(",") if p.strip()]

    log_prefix = _env("LOG_PREFIX", "").lower()

    return Config(
        media_root=media_root,
        work_root=work_root,
        min_size_gb=min_size_gb,
        max_files=max_files,
        qsv_quality=qsv_quality,
        qsv_preset=qsv_preset,
        post_encode_validate=post_encode_validate,
        validate_mode=validate_mode,
        validate_seconds=validate_seconds,
        out_uid=out_uid,
        out_gid=out_gid,
        out_mode=out_mode,
        out_dir_mode=out_dir_mode,
        bak_retention_days=bak_retention_days,
        log_retention_days=log_retention_days,
        lock_stale_hours=lock_stale_hours,
        work_cleanup_hours=work_cleanup_hours,
        probe_timeout_secs=probe_timeout_secs,
        ffprobe_analyzeduration=ffprobe_analyzeduration,
        ffprobe_probesize=ffprobe_probesize,
        exclude_path_parts=exclude_path_parts,
        log_prefix=log_prefix,
    )