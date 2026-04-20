"""Dual-sink structlog configuration: JSON to stdout + per-asset JSONL file.

Usage in Dagster assets:
    from datarift.logging import configure_logging, get_jsonl_path

    configure_logging(run_id="abc123", asset_name="bronze_leagues")
    log = structlog.get_logger()
    log.info("started", endpoint="/lol/league/v4/entries")
"""

from __future__ import annotations

import logging
import pathlib
from typing import Any

import structlog


_REQUIRED_FIELDS = frozenset(
    {"run_id", "asset_name", "endpoint", "severity", "event_type", "message", "ts"}
)

_JSONL_HANDLER_NAME = "datarift_jsonl"


def get_jsonl_path(run_id: str, asset_name: str) -> pathlib.Path:
    """Return the JSONL log path: data/_logs/<run_id>/<asset>.jsonl."""
    return pathlib.Path("data") / "_logs" / run_id / f"{asset_name}.jsonl"


def _inject_context(
    run_id: str, asset_name: str
) -> structlog.types.Processor:
    """Return a processor that injects run_id and asset_name into every event."""

    def processor(
        logger: Any, method_name: str, event_dict: dict[str, Any]
    ) -> dict[str, Any]:
        event_dict.setdefault("run_id", run_id)
        event_dict.setdefault("asset_name", asset_name)
        # Map structlog level to severity and capture event as message
        event_dict.setdefault("severity", method_name.upper())
        event_dict.setdefault("event_type", event_dict.get("event", ""))
        event_dict.setdefault("message", event_dict.get("event", ""))
        # endpoint defaults to empty if not provided by caller
        event_dict.setdefault("endpoint", "")
        return event_dict

    return processor


_STDOUT_HANDLER_NAME = "datarift_stdout"


def configure_logging(run_id: str, asset_name: str) -> None:
    """Configure structlog with dual sinks for one asset invocation.

    - Stdout: human-readable colored output (visible in Dagster UI logs)
    - File: data/_logs/<run_id>/<asset>.jsonl (machine-readable)

    Safe to call multiple times — clears previous handlers to avoid stacking.
    """
    # --- stdlib file handler for JSONL sink ---
    jsonl_path = get_jsonl_path(run_id, asset_name)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()

    # Remove previous datarift handlers to prevent stacking
    for h in list(root.handlers):
        if getattr(h, "name", None) in (_JSONL_HANDLER_NAME, _STDOUT_HANDLER_NAME):
            root.removeHandler(h)
            h.close()

    file_handler = logging.FileHandler(str(jsonl_path), mode="a", encoding="utf-8")
    file_handler.name = _JSONL_HANDLER_NAME
    file_handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
    ))
    root.addHandler(file_handler)
    root.setLevel(logging.DEBUG)

    # --- structlog configuration ---
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            _inject_context(run_id, asset_name),
            structlog.processors.TimeStamper(fmt="iso", key="ts"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,  # run_id changes per invocation
    )

    # Stdout handler — human-readable console output
    _ensure_stdout_handler(root)


def _ensure_stdout_handler(root: logging.Logger) -> None:
    """Add a human-readable stdout handler if none exists yet."""
    import sys

    for h in root.handlers:
        if getattr(h, "name", None) == _STDOUT_HANDLER_NAME:
            return  # already present

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.name = _STDOUT_HANDLER_NAME
    stdout_handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(),
    ))
    root.addHandler(stdout_handler)
