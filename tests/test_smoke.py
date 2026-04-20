"""Test that the smoke pipeline creates all 11 Silver tables and Gold tables from committed fixtures."""

from __future__ import annotations

from pathlib import Path

from scripts.smoke import EXPECTED_GOLD_TABLES, EXPECTED_SILVER_TABLES, run_smoke
from scripts.validate_gold import validate_gold


class TestSmokePipeline:
    def test_all_silver_tables_created(self, tmp_path: Path) -> None:
        """Run the full smoke pipeline in a temp dir and assert all 11 Silver tables exist with >0 rows."""
        result = run_smoke(smoke_dir=tmp_path / "smoke")

        for table_name in EXPECTED_SILVER_TABLES:
            assert table_name in result, f"Missing Silver table: {table_name}"
            assert result[table_name] > 0, f"{table_name} should have >0 rows, got {result[table_name]}"

    def test_gold_tables_created(self, tmp_path: Path) -> None:
        """Run the full smoke pipeline and assert Gold matchup_detail exists with >0 rows."""
        result = run_smoke(smoke_dir=tmp_path / "smoke")

        for table_name in EXPECTED_GOLD_TABLES:
            assert table_name in result, f"Missing Gold table: {table_name}"
            assert result[table_name] > 0, f"{table_name} should have >0 rows, got {result[table_name]}"

    def test_duckdb_gold_validation(self, tmp_path: Path) -> None:
        """Run the smoke pipeline and validate Gold-Silver consistency via DuckDB."""
        run_smoke(smoke_dir=tmp_path / "smoke")
        validate_gold(str(tmp_path / "smoke"))
