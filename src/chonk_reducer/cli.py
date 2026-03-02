from __future__ import annotations

import sys

from .runner import run


def main(argv: list[str] | None = None) -> int:
    # argv reserved for future subcommands; current flow uses env only
    _ = argv if argv is not None else sys.argv[1:]
    return run()
