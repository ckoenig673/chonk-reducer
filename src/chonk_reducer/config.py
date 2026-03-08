from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from chonk_reducer import __version__ as PACKAGE_VERSION



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
    # Paths
    media_root: Path
    work_root: Path

    # Selection
    min_size_gb: float
    max_files: int
    min_media_free_gb: float
    max_gb_per_run: float

    # Savings policy window
    max_savings_percent: float

    # Encoder
    qsv_quality: int
    qsv_preset: int

    # Validation
    post_encode_validate: bool
    validate_mode: str
    validate_seconds: int

    # Output perms
    out_uid: int
    out_gid: int
    out_mode: int
    out_dir_mode: int

    # Retention / cleanup
    bak_retention_days: int
    log_retention_days: int
    lock_stale_hours: int
    lock_self_heal: bool
    work_cleanup_hours: int

    # Probe
    probe_timeout_secs: int
    ffprobe_analyzeduration: int
    ffprobe_probesize: int

    # Discovery exclusions
    exclude_path_parts: tuple[str, ...]

    # Behavior
    log_prefix: str
    fail_fast: bool
    dry_run: bool
    min_savings_percent: float
    log_skips: bool
    top_candidates: int
    retry_count: int
    retry_backoff_secs: int
    preview: bool

    # Skip policies (Story 40/41)
    skip_codecs: tuple[str, ...] = ()
    skip_min_height: int = 0
    skip_resolution_tags: tuple[str, ...] = ()
    min_file_age_minutes: int = 0

    # Stats (SQLite)
    stats_enabled: bool = False
    stats_path: Path = Path("/config/chonk.db")
    library: str = ""
    version: str = "unknown"
    encoder: str = "hevc_qsv"


def load_config() -> Config:
    excl = _split_csv(_env("EXCLUDE_PATH_PARTS", "#recycle,@eaDir"))

    media_root = Path(_env("MEDIA_ROOT", "/movies"))
    work_root = Path(_env("WORK_ROOT", "/work"))

    # Skip policies
    skip_codecs = tuple(s.lower() for s in _split_csv(_env("SKIP_CODECS", "")))
    skip_min_height = _env_int("SKIP_MIN_HEIGHT", 0)
    skip_tags = tuple(s.lower() for s in _split_csv(_env("SKIP_RESOLUTION_TAGS", "")))
    min_file_age_minutes = max(0, _env_int("MIN_FILE_AGE_MINUTES", 0))

    # Stats defaults
    stats_enabled = _env_bool("STATS_ENABLED", True)
    default_stats_path = Path("/config/chonk.db")
    stats_path = Path(_env("STATS_PATH", str(default_stats_path)))
    library = _env("LIBRARY", "")
    encoder = _env("ENCODER", "hevc_qsv")
    app_version = (os.getenv("APP_VERSION") or "").strip() or (PACKAGE_VERSION or "").strip() or "unknown"

    return Config(
        version=app_version,
        media_root=media_root,
        work_root=work_root,

        min_size_gb=_env_float("MIN_SIZE_GB", 0.0),
        max_files=_env_int("MAX_FILES", 1),
        min_media_free_gb=_env_float("MIN_MEDIA_FREE_GB", 0.0),
        max_gb_per_run=_env_float("MAX_GB_PER_RUN", 0.0),
        max_savings_percent=_env_float("MAX_SAVINGS_PERCENT", 0.0),

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
        lock_self_heal=_env_bool("LOCK_SELF_HEAL", True),
        work_cleanup_hours=_env_int("WORK_CLEANUP_HOURS", 0),

        probe_timeout_secs=_env_int("PROBE_TIMEOUT_SECS", 60),
        ffprobe_analyzeduration=_env_int("FFPROBE_ANALYZEDURATION", 50_000_000),
        ffprobe_probesize=_env_int("FFPROBE_PROBESIZE", 50_000_000),

        exclude_path_parts=tuple(excl),

        log_prefix=_env("LOG_PREFIX", ""),

        fail_fast=_env_bool("FAIL_FAST", False),
        dry_run=_env_bool("DRY_RUN", False),
        min_savings_percent=_env_float("MIN_SAVINGS_PERCENT", 15.0),
        log_skips=_env_bool("LOG_SKIPS", False),
        top_candidates=_env_int("TOP_CANDIDATES", 5),
        retry_count=_env_int("RETRY_COUNT", 1),
        retry_backoff_secs=_env_int("RETRY_BACKOFF_SECS", 5),
        preview=_env_bool("PREVIEW", False),

        skip_codecs=skip_codecs,
        skip_min_height=skip_min_height,
        skip_resolution_tags=skip_tags,
        min_file_age_minutes=min_file_age_minutes,

        stats_enabled=stats_enabled,
        stats_path=stats_path,
        library=library,
        encoder=encoder,
    )
