from __future__ import annotations

import os
import sys
import time

import pytest

from chonk_reducer.transcoding.ffmpeg_utils import CmdError, run_cmd
from chonk_reducer.core.logging_utils import Logger


def test_run_cmd_terminates_process_when_cancelled(tmp_path):
    log_path = tmp_path / "run.log"
    logger = Logger(str(log_path))
    script = "import time\ntime.sleep(30)"

    proc = {"pid": None}

    def on_start(p):
        if p is not None:
            proc["pid"] = p.pid

    start = time.monotonic()
    with pytest.raises(CmdError, match="cancelled"):
        run_cmd(
            [sys.executable, "-c", script],
            logger,
            timeout=None,
            cancel_requested=lambda: time.monotonic() - start > 0.3,
            on_process_start=on_start,
        )

    assert proc["pid"] is not None
    with pytest.raises(ProcessLookupError):
        os.kill(proc["pid"], 0)


def test_run_cmd_streams_output_lines_to_callback(tmp_path):
    log_path = tmp_path / "run.log"
    logger = Logger(str(log_path))
    lines = []
    script = "import sys, time\nfor i in range(3):\n print('line=%s' % i, flush=True)\n time.sleep(0.05)"

    rc, out = run_cmd([sys.executable, "-c", script], logger, on_output_line=lines.append)

    assert rc == 0
    assert "line=0" in out
    assert "line=2" in out
    assert any("line=1" in line for line in lines)
