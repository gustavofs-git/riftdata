"""Tests for dual-sink structlog logging module."""

from __future__ import annotations

import json
import logging

import structlog

from datarift.logging import configure_logging, get_jsonl_path, _JSONL_HANDLER_NAME


class TestGetJsonlPath:
    def test_returns_correct_path(self):
        p = get_jsonl_path("run123", "bronze_leagues")
        assert str(p) == "data/_logs/run123/bronze_leagues.jsonl"

    def test_different_run_ids(self):
        p1 = get_jsonl_path("a", "asset")
        p2 = get_jsonl_path("b", "asset")
        assert p1 != p2
        assert "a" in str(p1)
        assert "b" in str(p2)


class TestConfigureLogging:
    def test_creates_jsonl_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        configure_logging("run1", "bronze_leagues")
        log = structlog.get_logger()
        log.info("test_event", endpoint="/test")
        # Flush handlers
        for h in logging.getLogger().handlers:
            h.flush()
        jsonl = tmp_path / "data" / "_logs" / "run1" / "bronze_leagues.jsonl"
        assert jsonl.exists()
        assert jsonl.stat().st_size > 0

    def test_log_events_contain_required_fields(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        configure_logging("run42", "silver_matches")
        log = structlog.get_logger()
        log.info("extraction_done", endpoint="/lol/match/v5", event_type="extract")
        for h in logging.getLogger().handlers:
            h.flush()
        jsonl = tmp_path / "data" / "_logs" / "run42" / "silver_matches.jsonl"
        line = jsonl.read_text().strip().splitlines()[0]
        event = json.loads(line)
        for field in ("run_id", "asset_name", "endpoint", "severity", "event_type", "message", "ts"):
            assert field in event, f"Missing required field: {field}"
        assert event["run_id"] == "run42"
        assert event["asset_name"] == "silver_matches"

    def test_no_handler_stacking(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        configure_logging("run1", "asset_a")
        configure_logging("run2", "asset_b")
        configure_logging("run3", "asset_c")
        root = logging.getLogger()
        jsonl_handlers = [
            h for h in root.handlers
            if getattr(h, "name", None) == _JSONL_HANDLER_NAME
        ]
        assert len(jsonl_handlers) == 1, f"Expected 1 JSONL handler, got {len(jsonl_handlers)}"

    def test_stdout_handler_present(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        configure_logging("run1", "asset_a")
        root = logging.getLogger()
        stream_handlers = [
            h for h in root.handlers
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]
        assert len(stream_handlers) >= 1

    def _cleanup_handlers(self):
        """Remove datarift handlers after test."""
        root = logging.getLogger()
        for h in list(root.handlers):
            if getattr(h, "name", None) == _JSONL_HANDLER_NAME:
                root.removeHandler(h)
                h.close()
