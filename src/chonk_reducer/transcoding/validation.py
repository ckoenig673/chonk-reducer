from __future__ import annotations

import json
from pathlib import Path

from ..config import Config
from .ffmpeg_utils import run_cmd, CmdError
from ..logging_utils import Logger


def ffprobe_quick(src: Path, cfg: Config, logger: Logger) -> dict:
    args = [
        "ffprobe",
        "-hide_banner",
        "-v", "error",
        "-analyzeduration", str(cfg.ffprobe_analyzeduration),
        "-probesize", str(cfg.ffprobe_probesize),
        "-show_entries", "format=duration:stream=index,codec_type,codec_name,disposition",
        "-of", "json",
        str(src),
    ]
    rc, out = run_cmd(args, logger, timeout=cfg.probe_timeout_secs)
    if rc != 0:
        raise CmdError(f"ffprobe failed rc={rc}")
    try:
        return json.loads(out)
    except Exception as e:
        raise CmdError("ffprobe returned non-json output") from e


def validate_decode(src: Path, seconds: int, logger: Logger) -> bool:
    args = ["ffmpeg", "-hide_banner", "-v", "error", "-xerror", "-i", str(src), "-t", str(seconds), "-f", "null", "-"]
    rc, _ = run_cmd(args, logger, timeout=max(30, seconds * 5))
    return rc == 0


def validate_probe(src: Path, cfg: Config, logger: Logger) -> bool:
    try:
        ffprobe_quick(src, cfg, logger)
        return True
    except Exception:
        return False


def validate_post_encode(encoded: Path, cfg: Config, logger: Logger) -> bool:
    if not cfg.post_encode_validate:
        return True
    mode = (cfg.validate_mode or "probe").lower()
    if mode == "decode":
        return validate_decode(encoded, cfg.validate_seconds, logger)
    return validate_probe(encoded, cfg, logger)
