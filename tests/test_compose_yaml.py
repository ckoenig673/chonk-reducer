from pathlib import Path


def test_compose_contains_only_service_runtime_container() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")

    assert "movie-transcoder:" not in compose_text
    assert "tv-transcoder:" not in compose_text
    assert "chonk-service:" in compose_text
    assert "SERVICE_MODE: \"true\"" in compose_text


def test_compose_omits_db_backed_operational_settings() -> None:
    compose_text = Path("compose.yaml").read_text(encoding="utf-8")

    for env_var in (
        "MIN_FILE_AGE_MINUTES",
        "TOP_CANDIDATES",
        "MAX_SAVINGS_PERCENT",
        "SKIP_CODECS",
        "SKIP_MIN_HEIGHT",
        "SKIP_RESOLUTION_TAGS",
        "VALIDATE_SECONDS",
        "LOG_RETENTION_DAYS",
        "BAK_RETENTION_DAYS",
        "MIN_MEDIA_FREE_GB",
        "MAX_GB_PER_RUN",
        "FAIL_FAST",
        "LOG_SKIPS",
    ):
        assert env_var not in compose_text
