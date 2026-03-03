from __future__ import annotations

import subprocess
import json
from pathlib import Path
from typing import Sequence

from .logging_utils import Logger


class CmdError(RuntimeError):
    pass


def run_cmd(args: Sequence[str], logger: Logger, timeout: int | None = None) -> tuple[int, str]:
    logger.log("CMD: " + " ".join(args))
    try:
        p = subprocess.run(
            list(args),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        raise CmdError(f"Command timed out after {timeout}s") from e

    out = p.stdout or ""
    if out:
        for line in out.splitlines()[-30:]:
            logger.log("  " + line)
    return p.returncode, out


def run_cmd_capture(args: Sequence[str], timeout: int | None = None) -> tuple[int, str]:
    """Run a command and capture combined stdout/stderr without logging output."""
    try:
        p = subprocess.run(
            list(args),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        raise CmdError(f"Command timed out after {timeout}s") from e

    out = p.stdout or ""
    return p.returncode, out


def probe_video_stream(path: Path, analyzeduration: int, probesize: int, logger: Logger, timeout: int | None = None) -> dict:
    """Return basic v:0 stream info from ffprobe.

    Dict keys: codec, width, height, bit_rate (int bits/s or None).
    """
    args = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-analyzeduration",
        str(analyzeduration),
        "-probesize",
        str(probesize),
        "-show_entries",
        "stream=codec_name,width,height,bit_rate",
        "-of",
        "json",
        str(path),
    ]
    logger.log("CMD: " + " ".join(args))
    rc, out = run_cmd_capture(args, timeout=timeout)
    if rc != 0:
        raise CmdError(f"ffprobe failed (rc={rc})")

    try:
        data = json.loads(out)
    except Exception as e:
        raise CmdError("ffprobe returned non-JSON output") from e

    streams = data.get("streams") or []
    s0 = streams[0] if streams else {}
    br = s0.get("bit_rate")
    try:
        br_i = int(br) if br is not None else None
    except Exception:
        br_i = None

    return {
        "codec": s0.get("codec_name"),
        "width": s0.get("width"),
        "height": s0.get("height"),
        "bit_rate": br_i,
    }
