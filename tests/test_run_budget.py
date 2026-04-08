from __future__ import annotations

from chonk_reducer.transcoding.run_budget import RunBudgetType, normalize_run_budget, parse_budget_type


def test_parse_budget_type_accepts_supported_values():
    assert parse_budget_type("max_files") is RunBudgetType.MAX_FILES
    assert parse_budget_type("estimated_runtime_minutes") is RunBudgetType.ESTIMATED_RUNTIME_MINUTES
    assert parse_budget_type("estimated_savings_bytes") is RunBudgetType.ESTIMATED_SAVINGS_BYTES
    assert parse_budget_type("score_cutoff") is RunBudgetType.SCORE_CUTOFF


def test_parse_budget_type_rejects_unknown_values():
    assert parse_budget_type("unknown") is None
    assert parse_budget_type("") is None
    assert parse_budget_type(None) is None


def test_normalize_run_budget_defaults_to_max_files_on_invalid_type():
    budget = normalize_run_budget(budget_type_raw="not-a-real-type", max_files=3)

    assert budget.budget_type is RunBudgetType.MAX_FILES
    assert budget.max_files_limit(fallback_max_files=99) == 3


def test_normalize_run_budget_keeps_compatibility_for_non_max_files_types():
    budget = normalize_run_budget(budget_type_raw="estimated_runtime_minutes", max_files=4)

    assert budget.budget_type is RunBudgetType.ESTIMATED_RUNTIME_MINUTES
    assert budget.max_files_limit(fallback_max_files=4) == 4


def test_normalize_run_budget_uses_explicit_budget_value_when_present():
    budget = normalize_run_budget(
        budget_type_raw="estimated_savings_bytes",
        max_files=4,
        budget_value_raw="123456",
    )

    assert budget.budget_type is RunBudgetType.ESTIMATED_SAVINGS_BYTES
    assert budget.estimated_savings_bytes_limit() == 123456
