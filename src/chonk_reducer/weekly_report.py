from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

from .discord_utils import send_discord_message, notify_weekly_enabled
from .logging_utils import Logger


def _env(name: str, default: str) -> str:
    return (os.environ.get(name) or default).strip()


def _env_int(name: str, default: int) -> int:
    v = _env(name, str(default))
    try:
        return int(v)
    except ValueError:
        return default


def _fmt_gb(n_bytes: int) -> str:
    return f"{n_bytes / (1024**3):.2f}GB"


@dataclass
class Totals:
    success: int = 0
    failed: int = 0
    skipped: int = 0
    unknown: int = 0
    before_bytes: int = 0
    after_bytes: int = 0
    saved_bytes: int = 0
    saved_pct_weighted: float = 0.0  # saved/ before
    saved_pct_avg: float = 0.0       # mean of per-file saved_pct
    _saved_pct_sum: float = 0.0

    def finalize(self) -> None:
        if self.before_bytes > 0:
            self.saved_pct_weighted = (self.saved_bytes / self.before_bytes) * 100.0
        if self.success > 0:
            self.saved_pct_avg = self._saved_pct_sum / self.success


def _parse_ts(ts: str) -> Optional[datetime]:
    try:
        # NDJSON uses ISO without timezone, e.g. 2026-03-03T13:13:13
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _read_ndjson(path: Path) -> Iterable[dict]:
    if not path.exists():
        return []
    rows = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    except Exception:
        return []
    return rows


def generate_weekly_report() -> int:
    logger = Logger()
    days = _env_int("WEEKLY_REPORT_DAYS", 7)
    now = datetime.now()
    start = now - timedelta(days=days)

    stats_paths_raw = _env("WEEKLY_STATS_PATHS", "/tv_shows/.chonkstats.ndjson,/movies/.chonkstats.ndjson")
    stats_paths = [Path(p.strip()) for p in stats_paths_raw.split(",") if p.strip()]

    report_dir = Path(_env("REPORTS_DIR", "/work/reports"))
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"chonk_weekly_{now.strftime('%Y%m%d')}.txt"

    # Aggregate
    by_lib: dict[str, Totals] = {"tv": Totals(), "movies": Totals()}
    combined = Totals()

    # For top 5 saves
    top_saves: list[dict] = []  # store rows with saved_bytes

    # Failure stage breakdown
    fail_stage: dict[str, int] = {}

    total_rows = 0
    for sp in stats_paths:
        for row in _read_ndjson(sp):
            total_rows += 1
            ts = _parse_ts(str(row.get("ts", "")))
            if not ts or ts < start:
                continue

            lib = str(row.get("library", "")).lower().strip() or "unknown"
            if lib == "movie":
                lib = "movies"
            if lib not in by_lib:
                by_lib[lib] = Totals()

            status = str(row.get("status", "")).lower().strip()
            stage = str(row.get("stage", "")).lower().strip()

            if status == "success":
                b = int(row.get("size_before_bytes") or 0)
                a = int(row.get("size_after_bytes") or 0)
                s = int(row.get("saved_bytes") or max(b - a, 0))
                pct = float(row.get("saved_pct") or 0.0)

                for t in (by_lib[lib], combined):
                    t.success += 1
                    t.before_bytes += b
                    t.after_bytes += a
                    t.saved_bytes += s
                    t._saved_pct_sum += pct

                top_saves.append({
                    "saved_bytes": s,
                    "saved_pct": pct,
                    "path": row.get("path") or row.get("filename") or "",
                    "library": lib,
                })
            elif status == "failed":
                for t in (by_lib[lib], combined):
                    t.failed += 1
                if stage:
                    fail_stage[stage] = fail_stage.get(stage, 0) + 1
            elif status == "skipped":
                for t in (by_lib[lib], combined):
                    t.skipped += 1
            else:
                for t in (by_lib[lib], combined):
                    t.unknown += 1

    for t in list(by_lib.values()) + [combined]:
        t.finalize()

    # Sort top saves
    top_saves.sort(key=lambda r: int(r.get("saved_bytes", 0)), reverse=True)
    top_saves = top_saves[:5]

    # Write report
    window = f"{start.strftime('%Y-%m-%d')} -> {now.strftime('%Y-%m-%d')}"
    lines: list[str] = []
    lines.append("=" * 50)
    lines.append("CHONK REDUCER — WEEKLY SAVINGS REPORT")
    lines.append(f"Window: {window}")
    lines.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 50)
    lines.append("")

    def add_lib(name: str, t: Totals) -> None:
        lines.append(f"LIBRARY: {name.upper()}")
        lines.append("-" * 50)
        lines.append(f"Files Processed (success): {t.success}")
        lines.append(f"Files Failed:              {t.failed}")
        lines.append(f"Files Skipped:             {t.skipped}")
        lines.append(f"Files Unknown:             {t.unknown}")
        lines.append("")
        lines.append(f"Total Before:  {_fmt_gb(t.before_bytes)}")
        lines.append(f"Total After:   {_fmt_gb(t.after_bytes)}")
        lines.append(f"Total Saved:   {_fmt_gb(t.saved_bytes)}")
        lines.append(f"Total Saved %: {t.saved_pct_weighted:.2f}%")
        lines.append(f"Avg Saved %:   {t.saved_pct_avg:.2f}%")
        lines.append("")
        lines.append("")

    # TV and Movies if present
    if "tv" in by_lib:
        add_lib("tv", by_lib["tv"])
    if "movies" in by_lib:
        add_lib("movies", by_lib["movies"])

    lines.append("COMBINED TOTALS (TV + MOVIES)")
    lines.append("-" * 50)
    lines.append(f"Total Files Success: {combined.success}")
    lines.append(f"Total Files Failed:  {combined.failed}")
    lines.append(f"Total Files Skipped: {combined.skipped}")
    lines.append(f"Total Files Unknown: {combined.unknown}")
    lines.append("")
    lines.append(f"Grand Total Before: {_fmt_gb(combined.before_bytes)}")
    lines.append(f"Grand Total After:  {_fmt_gb(combined.after_bytes)}")
    lines.append(f"Grand Total Saved:  {_fmt_gb(combined.saved_bytes)}")
    lines.append(f"Grand Total Saved %: {combined.saved_pct_weighted:.2f}%")
    lines.append("")
    lines.append("Top 5 Saves:")
    if top_saves:
        for i, r in enumerate(top_saves, start=1):
            lines.append(f"{i}) {_fmt_gb(int(r['saved_bytes']))} saved ({float(r['saved_pct']):.1f}%)  [{r['library']}]  {r['path']}")
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("Failure Breakdown (by stage):")
    if fail_stage:
        for st, cnt in sorted(fail_stage.items(), key=lambda x: x[0]):
            lines.append(f" - {st}: {cnt}")
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("=" * 50)
    lines.append("END OF REPORT")
    lines.append("=" * 50)
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")

    # Always print key totals to stdout for DSM logs
    logger.log(f"Weekly report written: {report_path}")
    logger.log(f"Window: {window}")
    logger.log(f"TV saved: {_fmt_gb(by_lib.get('tv', Totals()).saved_bytes)}")
    logger.log(f"Movies saved: {_fmt_gb(by_lib.get('movies', Totals()).saved_bytes)}")
    logger.log(f"Total saved: {_fmt_gb(combined.saved_bytes)}")
    if combined.skipped or combined.unknown:
        logger.log(f"Skipped: {combined.skipped}  Unknown: {combined.unknown}")
    logger.log(f"Failures: {combined.failed}")

    # Optional Discord
    if notify_weekly_enabled():
        msg = (
            f"Weekly Chonk Report ({days} days)\n"
            f"TV: {_fmt_gb(by_lib.get('tv', Totals()).saved_bytes)} saved ({by_lib.get('tv', Totals()).success} files)\n"
            f"Movies: {_fmt_gb(by_lib.get('movies', Totals()).saved_bytes)} saved ({by_lib.get('movies', Totals()).success} files)\n"
            f"Total: {_fmt_gb(combined.saved_bytes)} saved\n"
            f"Failures: {combined.failed}\n"
            f"Report: {report_path}"
        )
        send_discord_message(msg, ping_user=False)

    return 0
