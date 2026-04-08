from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RunBudgetType(str, Enum):
    """Allowed run-budget concepts for candidate selection groundwork."""

    MAX_FILES = "max_files"
    ESTIMATED_RUNTIME_MINUTES = "estimated_runtime_minutes"
    ESTIMATED_SAVINGS_BYTES = "estimated_savings_bytes"
    SCORE_CUTOFF = "score_cutoff"


@dataclass(frozen=True)
class RunBudget:
    budget_type: RunBudgetType
    raw_value: str

    def max_files_limit(self, *, fallback_max_files: int) -> int:
        """
        Backwards-compatible run cap used by current runner flow.

        For story 4.1 groundwork, all non-max_files budget modes map to the existing
        MAX_FILES runtime cap so behavior remains stable.
        """
        fallback = max(1, int(fallback_max_files))
        if self.budget_type is not RunBudgetType.MAX_FILES:
            return fallback
        try:
            parsed = int(str(self.raw_value).strip() or str(fallback))
        except (TypeError, ValueError):
            return fallback
        return max(1, parsed)


def parse_budget_type(value: str | None) -> RunBudgetType | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    for member in RunBudgetType:
        if member.value == normalized:
            return member
    return None


def normalize_run_budget(*, budget_type_raw: str | None, max_files: int) -> RunBudget:
    parsed_budget_type = parse_budget_type(budget_type_raw)
    if parsed_budget_type is None:
        parsed_budget_type = RunBudgetType.MAX_FILES

    safe_max_files = max(1, int(max_files))
    raw_value = str(safe_max_files)
    return RunBudget(budget_type=parsed_budget_type, raw_value=raw_value)
