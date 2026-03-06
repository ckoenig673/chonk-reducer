from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # py3.9+
except ModuleNotFoundError:  # py3.8 (Synology)
    from backports.zoneinfo import ZoneInfo


def _get_tz() -> ZoneInfo | None:
    tz = (os.environ.get("TZ") or "").strip()
    if not tz:
        return None
    try:
        return ZoneInfo(tz)
    except Exception:
        return None


def now_ts() -> str:
    z = _get_tz()
    if z is None:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return datetime.now(tz=z).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class Logger:
    logfile: str | None = None

    def log(self, msg: str) -> None:
        line = f"[{now_ts()}] {msg}"
        print(line, flush=True)
        if self.logfile:
            try:
                with open(self.logfile, "a", encoding="utf-8", newline="\n") as f:
                    f.write(line + "\n")
            except Exception:
                pass


def log_prefix() -> str:
    return (os.environ.get("LOG_PREFIX") or "").strip().lower()


def make_run_stamp() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())
