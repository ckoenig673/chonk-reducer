from __future__ import annotations

import argparse
import sys

from .transcoding.runner import run
from .healthcheck import run_healthcheck
from .weekly_report import generate_weekly_report
from .service import run_service, ServiceSettings


def main(argv: list[str] | None = None) -> int:
    """Entry point for `python -m chonk_reducer`.

    Backwards compatible:
      - no args => run normal transcoder
    New commands:
      - healthcheck
      - weekly-report
    """
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(prog="chonk_reducer")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("run", help="Run transcoding pipeline (default)")
    sub.add_parser("healthcheck", help="Strict read-only healthcheck (no media processing)")
    sub.add_parser("weekly-report", help="Generate weekly savings report from SQLite stats")
    sub.add_parser("service", help="Run long-lived scheduler + health service")

    args = parser.parse_args(argv)

    if args.cmd is None:
        if ServiceSettings.from_env().enabled:
            return run_service()
        return run()
    if args.cmd == "run":
        return run()
    if args.cmd == "healthcheck":
        return run_healthcheck()
    if args.cmd == "weekly-report":
        return generate_weekly_report()
    if args.cmd == "service":
        return run_service()

    return 1
