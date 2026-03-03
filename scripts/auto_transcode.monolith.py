#!/usr/bin/env python3
import os
import sys
import re
import json
import time
import shutil
import signal
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

# -----------------------------
# Helpers
# -----------------------------

def env_str(name: str, default: str) -> str:
    v = os.environ.get(name, default)
    return v.strip() if isinstance(v, str) else default

def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name, str(default)))
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    try:
        return float(env_str(name, str(default)))
    except Exception:
        return default

def env_bool(name: str, default: bool) -> bool:
    v = env_str(name, "1" if default else "0").lower()
    return v in ("1", "true", "yes", "y", "on")

def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def now_local() -> datetime:
    # Use local timezone (container-local). TZ env + /etc/localtime decide what “local” is.
    return datetime.now().astimezone()

def now_stamp() -> str:
    return now_local().strftime("[%Y-%m-%d %H:%M:%S]")

# -----------------------------
# Logger
# -----------------------------

class Logger:
    def __init__(self, run_log: Optional[Path] = None):
        self.run_log = run_log

    def log(self, msg: str) -> None:
        line = f"{now_stamp()} {msg}"
        print(line, flush=True)
        if self.run_log:
            try:
                self.run_log.parent.mkdir(parents=True, exist_ok=True)
                with self.run_log.open("a", encoding="utf-8", newline="\n") as f:
                    f.write(line + "\n")
            except Exception:
                pass

# -----------------------------
# Config
# -----------------------------

@dataclass
class Config:
    media_root: Path
    work_root: Path
    log_prefix: str

    min_size_gb: int
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

    lock_name: str
    lock_stale_hours: int
    work_cleanup_hours: int

    probe_timeout_secs: int
    ffprobe_analyzeduration: int
    ffprobe_probesize: int

    exclude_path_parts: List[str]

def load_config() -> Config:
    # NOTE: out_mode/out_dir_mode can be set as "664" or "0o664"; accept both.
    def parse_mode(s: str, default: int) -> int:
        s = env_str(s, "")
        if not s:
            return default
        try:
            if s.startswith("0o"):
                return int(s, 8)
            return int(s, 8) if re.fullmatch(r"[0-7]{3,4}", s) else int(s)
        except Exception:
            return default

    exclude_raw = env_str("EXCLUDE_PATH_PARTS", "#recycle,@eaDir")
    exclude_parts = [p.strip() for p in exclude_raw.split(",") if p.strip()]

    return Config(
        media_root=Path(env_str("MEDIA_ROOT", "/movies")),
        work_root=Path(env_str("WORK_ROOT", "/work")),
        log_prefix=env_str("LOG_PREFIX", "transcode"),

        min_size_gb=env_int("MIN_SIZE_GB", 20),
        max_files=env_int("MAX_FILES", 2),

        qsv_quality=env_int("QSV_QUALITY", 21),
        qsv_preset=env_int("QSV_PRESET", 7),

        post_encode_validate=env_bool("POST_ENCODE_VALIDATE", True),
        validate_mode=env_str("VALIDATE_MODE", "probe").lower(),
        validate_seconds=env_int("VALIDATE_SECONDS", 10),

        out_uid=env_int("OUT_UID", 1028),
        out_gid=env_int("OUT_GID", 100),
        out_mode=parse_mode("OUT_MODE", 0o664),
        out_dir_mode=parse_mode("OUT_DIR_MODE", 0o775),

        bak_retention_days=env_int("BAK_RETENTION_DAYS", 60),
        log_retention_days=env_int("LOG_RETENTION_DAYS", 30),

        lock_name=env_str("LOCK_NAME", "chonkreducer.lock"),
        lock_stale_hours=env_int("LOCK_STALE_HOURS", 12),
        work_cleanup_hours=env_int("WORK_CLEANUP_HOURS", 24),

        probe_timeout_secs=env_int("PROBE_TIMEOUT_SECS", 60),
        ffprobe_analyzeduration=env_int("FFPROBE_ANALYZEDURATION", 50_000_000),
        ffprobe_probesize=env_int("FFPROBE_PROBESIZE", 50_000_000),

        exclude_path_parts=exclude_parts,
    )

# -----------------------------
# Locking
# -----------------------------

def lock_path(cfg: Config) -> Path:
    return cfg.work_root / cfg.lock_name

def lock_is_stale(lp: Path, stale_hours: int) -> bool:
    try:
        age = time.time() - lp.stat().st_mtime
        return age > (stale_hours * 3600)
    except Exception:
        return False

def acquire_lock(cfg: Config, logger: Logger) -> bool:
    lp = lock_path(cfg)
    safe_mkdir(cfg.work_root)

    if lp.exists():
        if lock_is_stale(lp, cfg.lock_stale_hours):
            logger.log(f"Lock is stale ({lp}). Removing.")
            try:
                lp.unlink()
            except Exception:
                pass
        else:
            logger.log(f"Lock exists ({lp}). Skipping.")
            return False

    try:
        lp.write_text(str(os.getpid()), encoding="utf-8")
        return True
    except Exception as e:
        logger.log(f"FAILED: could not create lock {lp}: {e}")
        return False

def release_lock(cfg: Config) -> None:
    lp = lock_path(cfg)
    try:
        if lp.exists():
            lp.unlink()
    except Exception:
        pass

# -----------------------------
# Housekeeping
# -----------------------------

def cleanup_logs(cfg: Config, logger: Logger) -> None:
    log_dir = cfg.work_root / "logs"
    if not log_dir.exists():
        return

    cutoff = time.time() - (cfg.log_retention_days * 86400)

    removed = 0
    # legacy
    for p in log_dir.glob("transcode_*.log"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
        except Exception:
            pass

    # new prefix logs
    for p in log_dir.glob(f"{cfg.log_prefix}_*.log"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
        except Exception:
            pass

    for p in log_dir.glob("candidates_*.log"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
        except Exception:
            pass

    if removed:
        logger.log(f"Log cleanup: removed {removed} old log files (>{cfg.log_retention_days}d).")

def cleanup_baks(cfg: Config, logger: Logger) -> None:
    cutoff = time.time() - (cfg.bak_retention_days * 86400)

    removed = 0
    for p in cfg.media_root.rglob("*.bak.*"):
        try:
            if p.is_file() and p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
        except Exception:
            pass

    if removed:
        logger.log(f"Backup cleanup: removed {removed} *.bak.* files (>{cfg.bak_retention_days}d).")

def cleanup_work(cfg: Config, logger: Logger) -> None:
    # remove stale temp artifacts (encoded files, partials) in /work
    safe_mkdir(cfg.work_root)
    cutoff = time.time() - (cfg.work_cleanup_hours * 3600)

    removed = 0
    for p in cfg.work_root.glob("*.encoded.mkv"):
        try:
            if p.is_file() and p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
        except Exception:
            pass

    if removed:
        logger.log(f"Recovery cleanup: removed {removed} work artifacts (>{cfg.work_cleanup_hours}h).")

# -----------------------------
# Candidate selection
# -----------------------------

def is_excluded(path: Path, exclude_parts: List[str]) -> bool:
    s = str(path).lower()
    for part in exclude_parts:
        if part.lower() in s:
            return True
    return False

def gather_candidates(cfg: Config) -> List[Path]:
    min_bytes = cfg.min_size_gb * (1024 ** 3)
    out: List[Path] = []

    for p in cfg.media_root.rglob("*.mkv"):
        try:
            if is_excluded(p, cfg.exclude_path_parts):
                continue
            if p.name.endswith(".encoded.mkv"):
                continue
            if p.name.endswith(".TEST.mkv"):
                continue
            if (p.parent / (p.name + ".optimized")).exists():
                continue
            if any(p.name.startswith(".") for _ in [0]):  # cheap dotfile skip
                pass
            if p.stat().st_size >= min_bytes:
                out.append(p)
        except Exception:
            continue

    # biggest first
    out.sort(key=lambda x: x.stat().st_size, reverse=True)
    return out

# -----------------------------
# ffprobe / ffmpeg
# -----------------------------

def run_cmd(cmd: List[str], timeout: Optional[int] = None) -> Tuple[int, str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        out, err = p.communicate(timeout=timeout)
        return p.returncode, out, err
    except subprocess.TimeoutExpired:
        try:
            p.kill()
        except Exception:
            pass
        return 124, "", "timeout"

def ffprobe_quick(cfg: Config, mkv: Path) -> bool:
    cmd = [
        "ffprobe",
        "-hide_banner",
        "-v", "error",
        "-analyzeduration", str(cfg.ffprobe_analyzeduration),
        "-probesize", str(cfg.ffprobe_probesize),
        "-show_entries", "format=duration:stream=index,codec_type,codec_name",
        "-of", "json",
        str(mkv),
    ]
    rc, out, err = run_cmd(cmd, timeout=cfg.probe_timeout_secs)
    return rc == 0 and bool(out.strip())

def ffmpeg_decode_test(cfg: Config, mkv: Path, seconds: int) -> bool:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-v", "error",
        "-xerror",
        "-i", str(mkv),
        "-t", str(seconds),
        "-f", "null",
        "-",
    ]
    rc, out, err = run_cmd(cmd, timeout=max(cfg.probe_timeout_secs, seconds + 30))
    return rc == 0

def encode_qsv(cfg: Config, src: Path, out_path: Path) -> bool:
    # QSV HEVC encode while keeping other streams
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-init_hw_device", "qsv=hw:/dev/dri/renderD128",
        "-filter_hw_device", "hw",
        "-hwaccel", "qsv",
        "-hwaccel_output_format", "qsv",
        "-i", str(src),
        "-map", "0",
        "-c:v", "hevc_qsv",
        "-global_quality", str(cfg.qsv_quality),
        "-preset", str(cfg.qsv_preset),
        "-c:a", "copy",
        "-c:s", "copy",
        "-c:d", "copy",
        "-c:t", "copy",
        str(out_path),
    ]
    rc, out, err = run_cmd(cmd, timeout=None)
    return rc == 0

# -----------------------------
# Swap / perms
# -----------------------------

def apply_perms(cfg: Config, movie_dir: Path, movie_file: Path) -> None:
    try:
        os.chown(movie_dir, cfg.out_uid, cfg.out_gid)
    except Exception:
        pass
    try:
        os.chmod(movie_dir, cfg.out_dir_mode)
    except Exception:
        pass
    try:
        os.chown(movie_file, cfg.out_uid, cfg.out_gid)
    except Exception:
        pass
    try:
        os.chmod(movie_file, cfg.out_mode)
    except Exception:
        pass

def swap_in(cfg: Config, logger: Logger, src: Path, encoded: Path) -> bool:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    bak = src.with_name(src.name + f".bak.{ts}")

    try:
        # rename original -> bak
        src.rename(bak)
    except Exception as e:
        logger.log(f"FAILED: could not rename to bak: {e}")
        return False

    try:
        # move encoded into place (same filesystem so rename is ok)
        encoded.rename(src)
    except Exception as e:
        logger.log(f"FAILED: could not swap encoded into place: {e}")
        # best effort restore
        try:
            if src.exists():
                src.unlink()
        except Exception:
            pass
        try:
            bak.rename(src)
        except Exception:
            pass
        return False

    # marker
    try:
        (src.parent / (src.name + ".optimized")).write_text("", encoding="utf-8")
    except Exception:
        pass

    # perms
    apply_perms(cfg, src.parent, src)

    logger.log(f"OK: swapped + marked: {src}")
    return True

# -----------------------------
# Main
# -----------------------------

def main() -> int:
    cfg = load_config()

    # run_id used for filenames (kept UTC-stable)
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    log_dir = cfg.work_root / "logs"
    safe_mkdir(log_dir)

    run_log = log_dir / f"{cfg.log_prefix}_transcode_{run_id}.log"
    logger = Logger(run_log)

    # Acquire lock early so we don’t create confusing partial logs on skip
    if not acquire_lock(cfg, logger):
        return 0

    try:
        logger.log("===== TRANSCODE START =====")
        logger.log(f"MEDIA_ROOT={cfg.media_root}")
        logger.log(f"WORK_ROOT={cfg.work_root}")
        logger.log(f"LOG_PREFIX={cfg.log_prefix}")
        logger.log(f"MIN_SIZE_GB={cfg.min_size_gb} MAX_FILES={cfg.max_files}")
        logger.log(f"QSV_QUALITY={cfg.qsv_quality} QSV_PRESET={cfg.qsv_preset}")
        logger.log(f"POST_ENCODE_VALIDATE={int(cfg.post_encode_validate)} VALIDATE_MODE={cfg.validate_mode} VALIDATE_SECONDS={cfg.validate_seconds}")
        logger.log(f"OUT_UID={cfg.out_uid} OUT_GID={cfg.out_gid} OUT_MODE={oct(cfg.out_mode)} OUT_DIR_MODE={oct(cfg.out_dir_mode)}")
        logger.log(f"TZ={env_str('TZ','<unset>')} (offset {now_local().strftime('%z')})")
        logger.log(f"EXCLUDE_PATH_PARTS={','.join(cfg.exclude_path_parts)}")
        logger.log(f"Run log: {run_log}")

        cleanup_work(cfg, logger)
        cleanup_logs(cfg, logger)
        cleanup_baks(cfg, logger)

        candidates = gather_candidates(cfg)
        logger.log(f"Found {len(candidates)} candidates")

        cand_log = log_dir / f"{cfg.log_prefix}_candidates_{run_id}.log"
        try:
            with cand_log.open("w", encoding="utf-8", newline="\n") as f:
                for p in candidates:
                    f.write(str(p) + "\n")
        except Exception:
            pass

        considered = 0
        processed = 0
        skipped_marker = 0
        skipped_backup = 0
        failed = 0

        for src in candidates:
            if processed >= cfg.max_files:
                break

            # marker check again (race safe)
            if (src.parent / (src.name + ".optimized")).exists():
                skipped_marker += 1
                continue

            # if already has .bak.* (avoid reprocessing a folder mid-chaos)
            try:
                if list(src.parent.glob(src.name + ".bak.*")):
                    skipped_backup += 1
                    continue
            except Exception:
                pass

            considered += 1
            logger.log(f"Processing: {src}")

            # encode output in same dir (avoids cross-device rename problems)
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            encoded = src.with_name(src.name + f".{ts}.encoded.mkv")

            ok = encode_qsv(cfg, src, encoded)
            if not ok:
                logger.log(f"FAILED: encode: {src}")
                failed += 1
                try:
                    if encoded.exists():
                        encoded.unlink()
                except Exception:
                    pass
                continue

            # validate
            if cfg.post_encode_validate:
                if cfg.validate_mode == "decode":
                    v_ok = ffmpeg_decode_test(cfg, encoded, cfg.validate_seconds)
                else:
                    v_ok = ffprobe_quick(cfg, encoded)
                if not v_ok:
                    logger.log(f"FAILED: validate: {src}")
                    failed += 1
                    try:
                        if encoded.exists():
                            encoded.unlink()
                    except Exception:
                        pass
                    continue

            if not swap_in(cfg, logger, src, encoded):
                failed += 1
                continue

            processed += 1

        logger.log("===== SUMMARY =====")
        logger.log(f"Candidates found:     {len(candidates)}")
        logger.log(f"Considered:           {considered}")
        logger.log(f"Processed:            {processed}")
        logger.log(f"Skipped (marker):     {skipped_marker}")
        logger.log(f"Skipped (backup):     {skipped_backup}")
        logger.log(f"Failed:               {failed}")
        logger.log("===== END =====")

        return 0 if failed == 0 else 2

    finally:
        release_lock(cfg)

if __name__ == "__main__":
    raise SystemExit(main())