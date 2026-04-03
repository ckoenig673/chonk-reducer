from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

from .config import load_config
from .services.discord_utils import send_discord_message, notify_healthcheck_enabled
from .logging_utils import Logger


def _env(name: str, default: str) -> str:
    return (os.environ.get(name) or default).strip()

def _env_bool(name: str, default: bool = False) -> bool:
    v = _env(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "y", "on")


def _env(name: str, default: str) -> str:
    return (os.environ.get(name) or default).strip()


def _ok(logger: Logger, msg: str) -> None:
    logger.log(f"[OK] {msg}")


def _fail(logger: Logger, msg: str) -> None:
    logger.log(f"[FAIL] {msg}")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_healthcheck() -> int:
    """Strict, read-only healthcheck. Returns 0 if healthy, non-zero otherwise."""
    logger = Logger()

    failures: list[str] = []
    logger.log("===== HEALTHCHECK START =====")

    # 1) Config load
    try:
        cfg = load_config()
        _ok(logger, "Config loaded")
    except Exception as e:
        _fail(logger, f"Config failed to load: {e}")
        failures.append(f"Config load failed: {e}")
        _finish(logger, failures)
        return _notify_and_exit(logger, failures)

    # 2) Mounts / paths
    try:
        if not cfg.media_root.exists():
            raise FileNotFoundError(f"MEDIA_ROOT missing: {cfg.media_root}")
        # basic read test
        _ = next(cfg.media_root.iterdir(), None)
        _ok(logger, f"MEDIA_ROOT readable: {cfg.media_root}")
    except Exception as e:
        _fail(logger, str(e))
        failures.append(str(e))

    try:
        if not cfg.work_root.exists():
            raise FileNotFoundError(f"WORK_ROOT missing: {cfg.work_root}")
        # basic write test in work root
        with tempfile.NamedTemporaryFile(dir=str(cfg.work_root), prefix=".chonk_hc_", delete=True) as _tmp:
            pass
        _ok(logger, f"WORK_ROOT writable: {cfg.work_root}")
    except Exception as e:
        _fail(logger, str(e))
        failures.append(str(e))

    # Ensure logs + reports dirs can be created
    try:
        logs_dir = cfg.work_root / "logs"
        _ensure_dir(logs_dir)
        _ok(logger, f"logs dir OK: {logs_dir}")
    except Exception as e:
        _fail(logger, f"logs dir failed: {e}")
        failures.append(f"logs dir failed: {e}")

    try:
        reports_dir = Path(_env("REPORTS_DIR", str(cfg.work_root / "reports")))
        _ensure_dir(reports_dir)
        _ok(logger, f"reports dir OK: {reports_dir}")
    except Exception as e:
        _fail(logger, f"reports dir failed: {e}")
        failures.append(f"reports dir failed: {e}")

    # 3) ffmpeg / ffprobe
    for tool in ("ffmpeg", "ffprobe"):
        p = shutil.which(tool)
        if p:
            _ok(logger, f"{tool} found: {p}")
        else:
            msg = f"{tool} not found in PATH"
            _fail(logger, msg)
            failures.append(msg)

    # 4) QSV device check (only if using qsv encoder)
    try:
        if "qsv" in (cfg.encoder or "").lower():
            dri = Path("/dev/dri")
            if not dri.exists():
                raise FileNotFoundError("/dev/dri not present (QSV device missing)")
            _ok(logger, "QSV device present (/dev/dri)")
    except Exception as e:
        _fail(logger, str(e))
        failures.append(str(e))

    # 5) Stats path writeability check (without modifying stats file)
    try:
        if cfg.stats_enabled:
            stats_dir = cfg.stats_path.parent
            if not stats_dir.exists():
                raise FileNotFoundError(f"Stats directory missing: {stats_dir}")
            # create+delete a temp file next to stats file
            with tempfile.NamedTemporaryFile(dir=str(stats_dir), prefix=".chonk_stats_hc_", delete=True) as _tmp:
                pass
            _ok(logger, f"Stats path writable: {cfg.stats_path}")
    except Exception as e:
        _fail(logger, str(e))
        failures.append(str(e))

    _finish(logger, failures)
    return _notify_and_exit(logger, failures)


def _finish(logger: Logger, failures: list[str]) -> None:
    if failures:
        logger.log("===== HEALTHCHECK FAIL =====")
        logger.log(f"Failures: {len(failures)}")
        for f in failures:
            logger.log(f" - {f}")
    else:
        logger.log("===== HEALTHCHECK OK =====")
    logger.log("===== HEALTHCHECK END =====")


def _notify_and_exit(logger: Logger, failures: list[str]) -> int:
    """Send optional Discord notification and return appropriate exit code."""
    strict = _env_bool("HEALTHCHECK_STRICT", True)

    ping_fail = _env_bool("DISCORD_PING_ON_FAILURE", True)
    ping_ok = _env_bool("DISCORD_PING_ON_SUCCESS", False)

    if notify_healthcheck_enabled():
        if failures:
            content = "Chonk healthcheck FAILED\n" + "\n".join(failures[:8])
            send_discord_message(content, ping_user=ping_fail)
        else:
            send_discord_message("Chonk healthcheck OK", ping_user=ping_ok)

    if failures:
        return 2 if strict else 0
    return 0
