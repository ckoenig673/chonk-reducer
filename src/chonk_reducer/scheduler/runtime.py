from __future__ import annotations

import logging

LOGGER = logging.getLogger("chonk_reducer.service")


def build_scheduler(background_scheduler_cls, fallback_scheduler_cls, timezone_name: str, import_error):
    if background_scheduler_cls is not None:
        scheduler_class = background_scheduler_cls
        LOGGER.info(
            "Instantiating scheduler class: %s.%s",
            getattr(scheduler_class, "__module__", "<unknown_module>"),
            getattr(scheduler_class, "__qualname__", getattr(scheduler_class, "__name__", "<unknown_class>")),
        )
        LOGGER.info("APScheduler import status: available")
        return scheduler_class(timezone=timezone_name)

    if import_error is not None:
        LOGGER.warning("APScheduler import status: unavailable (%s: %s)", type(import_error).__name__, import_error)
    else:
        LOGGER.warning("APScheduler import status: unavailable (no import exception captured)")

    LOGGER.info(
        "Instantiating scheduler class: %s.%s",
        fallback_scheduler_cls.__module__,
        fallback_scheduler_cls.__qualname__,
    )
    return fallback_scheduler_cls()


def attach_scheduler_listeners(scheduler, callback, event_job_executed, event_job_error, event_job_missed) -> None:
    add_listener = getattr(scheduler, "add_listener", None)
    if not callable(add_listener):
        return
    if event_job_executed is None or event_job_error is None or event_job_missed is None:
        return
    try:
        add_listener(callback, event_job_executed | event_job_error | event_job_missed)
    except Exception:
        LOGGER.warning("Unable to attach APScheduler event listeners", exc_info=True)
