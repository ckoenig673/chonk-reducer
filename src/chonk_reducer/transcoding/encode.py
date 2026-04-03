from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, Optional

from ..config import Config
from .ffmpeg_utils import CmdError, run_cmd, run_cmd_capture
from ..logging_utils import Logger


def parse_ffmpeg_progress_line(line: str) -> Optional[tuple[str, str]]:
    text = str(line or "").strip()
    if not text or "=" not in text:
        return None
    key, value = text.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None
    return key, value


def _probe_duration_ms(src: Path, timeout: Optional[int]) -> int:
    args = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(src),
    ]
    rc, out = run_cmd_capture(args, timeout=timeout)
    if rc != 0:
        return 0
    raw = str(out or "").strip()
    if not raw:
        return 0
    try:
        seconds = float(raw)
    except Exception:
        return 0
    if seconds <= 0:
        return 0
    return int(seconds * 1000.0)


def _encoding_progress_update(progress: Dict[str, str], duration_ms: int) -> Dict[str, str]:
    out_time_ms_raw = str(progress.get("out_time_ms", "") or "").strip()
    out_time_ms_value = 0
    if out_time_ms_raw:
        try:
            out_time_ms_value = max(0, int(out_time_ms_raw))
        except Exception:
            out_time_ms_value = 0

    percent = ""
    eta = ""
    if duration_ms > 0 and out_time_ms_value >= 0:
        ratio = min(1.0, float(out_time_ms_value) / float(duration_ms))
        percent = "%.1f" % (ratio * 100.0)
        speed_raw = str(progress.get("speed", "") or "").strip().lower()
        speed_value = 0.0
        if speed_raw.endswith("x"):
            try:
                speed_value = float(speed_raw[:-1])
            except Exception:
                speed_value = 0.0
        remaining_ms = max(0, duration_ms - out_time_ms_value)
        if speed_value > 0:
            eta_seconds = int(round((remaining_ms / 1000.0) / speed_value))
            eta = str(max(0, eta_seconds))

    snapshot = {
        "encode_percent": percent,
        "encode_speed": str(progress.get("speed", "") or "").strip(),
        "encode_eta": eta,
        "encode_out_time": out_time_ms_raw,
    }
    return snapshot


def encode_qsv(
    src: Path,
    encoded_out: Path,
    cfg: Config,
    logger: Logger,
    cancel_requested=None,
    on_process_start=None,
    progress_callback: Optional[Callable[[Dict[str, str]], None]] = None,
) -> None:
    duration_ms = _probe_duration_ms(src, timeout=cfg.probe_timeout_secs)
    args = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-hwaccel", "qsv",
        "-hwaccel_output_format", "qsv",
        "-i", str(src),
        "-map", "0",
        "-c:v", "hevc_qsv",
        "-global_quality", str(cfg.qsv_quality),
        "-preset", str(cfg.qsv_preset),
        "-c:a", "copy",
        "-c:s", "copy",
        "-map_metadata", "0",
        "-map_chapters", "0",
        "-progress", "pipe:1",
        "-nostats",
        str(encoded_out),
    ]

    progress_state: Dict[str, str] = {}

    def _handle_output_line(line: str) -> None:
        parsed = parse_ffmpeg_progress_line(line)
        if parsed is None:
            return
        key, value = parsed
        if key not in ("out_time_ms", "speed", "progress"):
            return
        progress_state[key] = value
        if key != "progress":
            return
        if not callable(progress_callback):
            return
        if value != "continue" and value != "end":
            return
        try:
            progress_callback(_encoding_progress_update(progress_state, duration_ms))
        except Exception:
            pass

    rc, _ = run_cmd(
        args,
        logger,
        timeout=None,
        cancel_requested=cancel_requested,
        on_process_start=on_process_start,
        on_output_line=_handle_output_line,
    )
    if rc != 0:
        raise CmdError(f"ffmpeg encode failed rc={rc}")
