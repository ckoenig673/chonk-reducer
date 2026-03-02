from __future__ import annotations

import subprocess
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
