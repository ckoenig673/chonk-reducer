from __future__ import annotations

from chonk_reducer.config import load_config


def test_load_config_returns_expected_defaults(monkeypatch):
    for key in [
        "MEDIA_ROOT",
        "WORK_ROOT",
        "MIN_SIZE_GB",
        "MAX_FILES",
        "MIN_SAVINGS_PERCENT",
        "OUT_MODE",
        "OUT_DIR_MODE",
        "MIN_FILE_AGE_MINUTES",
    ]:
        monkeypatch.delenv(key, raising=False)

    cfg = load_config()

    assert str(cfg.media_root) == "/movies"
    assert str(cfg.work_root) == "/work"
    assert cfg.min_size_gb == 0.0
    assert cfg.max_files == 1
    assert cfg.min_savings_percent == 15.0
    assert cfg.out_mode == int("664", 8)
    assert cfg.out_dir_mode == int("775", 8)
    assert cfg.min_file_age_minutes == 0


def test_invalid_config_values_fall_back_to_safe_defaults(monkeypatch):
    monkeypatch.setenv("MAX_FILES", "not-an-int")
    monkeypatch.setenv("MIN_SIZE_GB", "not-a-float")
    monkeypatch.setenv("OUT_MODE", "invalid")

    cfg = load_config()

    assert cfg.max_files == 1
    assert cfg.min_size_gb == 0.0
    assert cfg.out_mode == int("664", 8)


def test_min_file_age_minutes_parsing(monkeypatch):
    monkeypatch.setenv("MIN_FILE_AGE_MINUTES", "30")
    cfg = load_config()
    assert cfg.min_file_age_minutes == 30


def test_min_file_age_minutes_invalid_or_negative_defaults_to_zero(monkeypatch):
    monkeypatch.setenv("MIN_FILE_AGE_MINUTES", "-5")
    assert load_config().min_file_age_minutes == 0

    monkeypatch.setenv("MIN_FILE_AGE_MINUTES", "not-a-number")
    assert load_config().min_file_age_minutes == 0


def test_retry_backoff_seconds_prefers_new_env_name(monkeypatch):
    monkeypatch.setenv("RETRY_BACKOFF_SECONDS", "12")
    monkeypatch.setenv("RETRY_BACKOFF_SECS", "5")

    cfg = load_config()

    assert cfg.retry_backoff_seconds == 12


def test_retry_backoff_seconds_falls_back_to_legacy_env_name(monkeypatch):
    monkeypatch.delenv("RETRY_BACKOFF_SECONDS", raising=False)
    monkeypatch.setenv("RETRY_BACKOFF_SECS", "9")

    cfg = load_config()

    assert cfg.retry_backoff_seconds == 9
