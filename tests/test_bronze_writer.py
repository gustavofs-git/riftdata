"""Tests for BronzeWriter Delta table operations."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from datarift.bronze_writer import BronzeWriter


def _make_records(keys: list[str]) -> list[dict]:
    return [{"puuid": k, "raw_json": f'{{"id": "{k}"}}'} for k in keys]


class TestBronzeWriter:
    def test_table_path(self):
        w = BronzeWriter("league_entries_raw", "puuid", "data/bronze")
        assert w.table_path == "data/bronze/league_entries_raw"

    def test_write_batch_creates_table(self, tmp_path: Path):
        w = BronzeWriter("test_table", "puuid", str(tmp_path))
        records = _make_records(["aaa", "bbb"])
        w.write_batch(records, endpoint="/lol/league/v4", status_code=200, region="kr")

        df = pl.read_delta(str(tmp_path / "test_table"))
        assert len(df) == 2
        assert set(df["puuid"].to_list()) == {"aaa", "bbb"}

    def test_append_adds_rows(self, tmp_path: Path):
        w = BronzeWriter("test_table", "puuid", str(tmp_path))
        w.write_batch(_make_records(["a"]), endpoint="/ep", status_code=200, region="kr")
        w.write_batch(_make_records(["b"]), endpoint="/ep", status_code=200, region="kr")

        df = pl.read_delta(str(tmp_path / "test_table"))
        assert len(df) == 2
        assert set(df["puuid"].to_list()) == {"a", "b"}

    def test_existing_keys_returns_written_keys(self, tmp_path: Path):
        w = BronzeWriter("test_table", "puuid", str(tmp_path))
        w.write_batch(
            _make_records(["x", "y", "z"]),
            endpoint="/ep",
            status_code=200,
            region="br",
        )
        assert w.existing_keys() == {"x", "y", "z"}

    def test_existing_keys_empty_for_nonexistent_table(self, tmp_path: Path):
        w = BronzeWriter("no_such_table", "puuid", str(tmp_path))
        assert w.existing_keys() == set()

    def test_schema_columns_and_types(self, tmp_path: Path):
        w = BronzeWriter("test_table", "puuid", str(tmp_path))
        w.write_batch(
            _make_records(["test"]),
            endpoint="/lol/test",
            status_code=200,
            region="na",
        )
        df = pl.read_delta(str(tmp_path / "test_table"))
        expected_cols = {"puuid", "raw_json", "ingested_at", "endpoint", "status_code", "region"}
        assert set(df.columns) == expected_cols
        assert df["puuid"].dtype == pl.Utf8
        assert df["raw_json"].dtype == pl.Utf8
        assert df["endpoint"].dtype == pl.Utf8
        assert df["region"].dtype == pl.Utf8
        assert df["status_code"].dtype in (pl.Int64, pl.Int32)
