from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from .config import Config, load_config
from .logging_utils import Logger, make_run_stamp


def _run(cmd: list[str], timeout: int | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )


def _is_stale(path: Path, hours: int) -> bool:
    if not path.exists():
        return False
    age_s = time.time() - path.stat().st_mtime
    return age_s > (hours * 3600)


def _cleanup(cfg: Config, log: Logger) -> None:
    # Clean old logs
    log_dir = cfg.work_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    if cfg.log_retention_days >= 0:
        cutoff = time.time() - (cfg.log_retention_days * 86400)
        for p in log_dir.glob("*.log"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
            except Exception:
                pass

    # Clean old baks in media root (mtime based)
    if cfg.bak_retention_days >= 0:
        cutoff = time.time() - (cfg.bak_retention_days * 86400)
        for p in cfg.media_root.rglob("*.bak.*"):
            try:
                if p.is_file() and p.stat().st_mtime < cutoff:
                    p.unlink()
            except Exception:
                pass

    # Clean work artifacts
    if cfg.work_cleanup_hours >= 0:
        cutoff = time.time() - (cfg.work_cleanup_hours * 3600)
        for p in cfg.work_root.rglob("*"):
            try:
                if p.is_file() and p.stat().st_mtime < cutoff:
                    p.unlink()
            except Exception:
                pass


def _path_is_excluded(p: Path, exclude_parts: list[str]) -> bool:
    if not exclude_parts:
        return False
    s = str(p).lower()
    return any(part and (part.lower() in s) for part in exclude_parts)


def _ffprobe_ok(cfg: Config, src: Path, log: Logger) -> bool:
    cmd = [
        "ffprobe",
        "-hide_banner",
        "-v", "error",
        "-analyzeduration", str(cfg.ffprobe_analyzeduration),
        "-probesize", str(cfg.ffprobe_probesize),
        str(src),
    ]
    r = _run(cmd, timeout=cfg.probe_timeout_secs)
    if r.returncode == 0:
        return True
    log.log("FAIL: ffprobe")
    if r.stdout:
        log.log(r.stdout[-2000:])
    return False


def _validate_file(cfg: Config, path: Path, log: Logger) -> bool:
    if not cfg.post_encode_validate:
        return True

    mode = (cfg.validate_mode or "decode").lower()
    secs = max(1, int(cfg.validate_seconds))

    if mode == "probe":
        return _ffprobe_ok(cfg, path, log)

    # decode mode (default)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-v", "error",
        "-xerror",
        "-i", str(path),
        "-t", str(secs),
        "-f", "null",
        "-",
    ]
    r = _run(cmd, timeout=max(30, cfg.probe_timeout_secs))
    if r.returncode == 0:
        return True
    log.log("FAIL: decode validation")
    if r.stdout:
        log.log(r.stdout[-2000:])
    return False


def _enforce_perms(cfg: Config, target: Path, log: Logger) -> None:
    # file perms
    try:
        os.chown(target, cfg.out_uid, cfg.out_gid)
    except Exception as e:
        log.log(f"WARN: chown file failed: {e}")

    try:
        os.chmod(target, cfg.out_mode)
    except Exception as e:
        log.log(f"WARN: chmod file failed: {e}")

    # parent dir perms
    try:
        os.chown(target.parent, cfg.out_uid, cfg.out_gid)
    except Exception:
        pass
    try:
        os.chmod(target.parent, cfg.out_dir_mode)
    except Exception:
        pass


def run() -> int:
    cfg = load_config()

    prefix = (cfg.log_prefix + "_") if cfg.log_prefix else ""
    run_stamp = make_run_stamp()

    log_dir = cfg.work_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    run_log = log_dir / f"{prefix}transcode_{run_stamp}.log"
    cand_log = log_dir / f"{prefix}candidates_{run_stamp}.log"

    logger = Logger(str(run_log))

    # lock file per prefix
    lock_name = f"{prefix}chonkreducer.lock" if prefix else "chonkreducer.lock"
    lock_file = cfg.work_root / lock_name
    if lock_file.exists():
        if _is_stale(lock_file, cfg.lock_stale_hours):
            try:
                lock_file.unlink()
            except Exception:
                logger.log(f"Lock exists ({lock_file}). Skipping.")
                return 0
        else:
            logger.log(f"Lock exists ({lock_file}). Skipping.")
            return 0

    try:
        lock_file.write_text(str(os.getpid()), encoding="utf-8")

        logger.log("===== TRANSCODE START =====")
        logger.log(f"MEDIA_ROOT={cfg.media_root}")
        logger.log(f"WORK_ROOT={cfg.work_root}")
        logger.log(f"MIN_SIZE_GB={cfg.min_size_gb} MAX_FILES={cfg.max_files}")
        logger.log(f"QSV_QUALITY={cfg.qsv_quality} QSV_PRESET={cfg.qsv_preset}")
        logger.log(f"POST_ENCODE_VALIDATE={int(cfg.post_encode_validate)} VALIDATE_MODE={cfg.validate_mode} VALIDATE_SECONDS={cfg.validate_seconds}")
        logger.log(f"OUT_UID={cfg.out_uid} OUT_GID={cfg.out_gid} OUT_MODE={oct(cfg.out_mode)} OUT_DIR_MODE={oct(cfg.out_dir_mode)}")
        logger.log(f"EXCLUDE_PATH_PARTS={','.join(cfg.exclude_path_parts) if cfg.exclude_path_parts else '<none>'}")
        logger.log(f"Run log: {run_log}")

        _cleanup(cfg, logger)

        # candidates
        min_bytes = cfg.min_size_gb * (1024**3)
        candidates: list[Path] = []
        for p in cfg.media_root.rglob("*"):
            try:
                if not p.is_file():
                    continue
                if p.suffix.lower() not in (".mkv", ".mp4"):
                    continue
                if _path_is_excluded(p, cfg.exclude_path_parts):
                    continue
                if p.stat().st_size <= min_bytes:
                    continue
                candidates.append(p)
            except Exception:
                continue

        candidates.sort(key=lambda x: str(x))
        cand_log.write_text("\n".join(str(p) for p in candidates) + ("\n" if candidates else ""), encoding="utf-8")
        logger.log(f"Found {len(candidates)} candidates")

        processed = 0
        considered = 0
        skipped_marker = 0
        skipped_backup = 0
        failed = 0

        run_id = run_stamp

        for src in candidates:
            if processed >= cfg.max_files:
                break

            marker = Path(str(src) + ".optimized")
            if marker.exists():
                skipped_marker += 1
                continue

            if list(src.parent.glob(src.name + ".bak.*")):
                skipped_backup += 1
                continue

            considered += 1
            logger.log(f"Processing: {src}")

            if not _ffprobe_ok(cfg, src, logger):
                failed += 1
                continue

            bak = src.with_name(src.name + f".bak.{run_id}")
            out = src.with_name(src.name + f".{run_id}.encoded.mkv")

            cmd = [
                "ffmpeg", "-hide_banner", "-y",
                "-analyzeduration", str(cfg.ffprobe_analyzeduration),
                "-probesize", str(cfg.ffprobe_probesize),
                "-hwaccel", "qsv",
                "-hwaccel_output_format", "qsv",
                "-extra_hw_frames", str(cfg.extra_hw_frames),
                "-i", str(src),
                "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?",
                "-c:v:0", "hevc_qsv",
                "-global_quality", str(cfg.qsv_quality),
                "-preset", str(cfg.qsv_preset),
                "-c:a", "copy", "-c:s", "copy",
                "-movflags", "+faststart",
                str(out)
            ]

            r = _run(cmd)
            if r.returncode != 0 or (not out.exists()) or out.stat().st_size == 0:
                logger.log("FAIL: ffmpeg encode failed")
                if r.stdout:
                    logger.log(r.stdout[-2000:])
                try:
                    out.unlink(missing_ok=True)
                except Exception:
                    pass
                failed += 1
                continue

            if not _validate_file(cfg, out, logger):
                logger.log("FAIL: validation failed; keeping original")
                try:
                    out.unlink(missing_ok=True)
                except Exception:
                    pass
                failed += 1
                continue

            # swap (same dir => atomic)
            try:
                os.replace(src, bak)
                os.replace(out, src)
            except Exception as e:
                logger.log(f"FAIL: swap error: {e}")
                # rollback attempt
                try:
                    if bak.exists() and not src.exists():
                        os.replace(bak, src)
                except Exception:
                    pass
                try:
                    out.unlink(missing_ok=True)
                except Exception:
                    pass
                failed += 1
                continue

            try:
                marker.touch()
            except Exception as e:
                logger.log(f"WARN: marker touch failed: {e}")

            _enforce_perms(cfg, src, logger)

            processed += 1
            logger.log(f"OK: swapped + marked: {src}")

        logger.log("===== SUMMARY =====")
        logger.log(f"Candidates found:     {len(candidates)}")
        logger.log(f"Considered:           {considered}")
        logger.log(f"Processed:            {processed}")
        logger.log(f"Skipped (marker):     {skipped_marker}")
        logger.log(f"Skipped (backup):     {skipped_backup}")
        logger.log(f"Failed:               {failed}")
        logger.log("===== END =====")
        return 0

    finally:
        try:
            lock_file.unlink()
        except Exception:
            pass
