from __future__ import annotations

from pathlib import Path

from .config import Config
from .ffmpeg_utils import CmdError, run_cmd
from .logging_utils import Logger


def encode_qsv(
    src: Path,
    encoded_out: Path,
    cfg: Config,
    logger: Logger,
    cancel_requested=None,
    on_process_start=None,
) -> None:
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
        str(encoded_out),
    ]
    rc, _ = run_cmd(
        args,
        logger,
        timeout=None,
        cancel_requested=cancel_requested,
        on_process_start=on_process_start,
    )
    if rc != 0:
        raise CmdError(f"ffmpeg encode failed rc={rc}")
