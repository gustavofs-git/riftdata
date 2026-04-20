"""Integration tests: verify Postgres export matches Gold Delta tables."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import polars as pl
import psycopg
import pytest

PG_URL = "postgresql://datarift:datarift@localhost:5432/datarift"
GOLD_PATH = Path(__file__).resolve().parent.parent / "data" / "_smoke" / "gold"
EXPORT_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "export_to_postgres.py"

GOLD_TABLES = ["matchup_detail", "matchup_intervals", "matchup_aggregates"]


@pytest.fixture(scope="session")
def pg_conn():
    try:
        conn = psycopg.connect(PG_URL)
    except psycopg.OperationalError:
        pytest.skip("Postgres not available")
    yield conn
    conn.close()


@pytest.fixture(scope="session", autouse=True)
def run_export(pg_conn):
    result = subprocess.run(
        [sys.executable, str(EXPORT_SCRIPT), "--gold-path", str(GOLD_PATH)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Export failed: {result.stderr}"


@pytest.mark.postgres
class TestRowCounts:
    @pytest.mark.parametrize("table", GOLD_TABLES)
    def test_row_count_matches(self, pg_conn, table):
        delta_df = pl.read_delta(str(GOLD_PATH / table))
        expected = len(delta_df)

        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM gold.{table}")
            actual = cur.fetchone()[0]

        assert actual == expected, f"{table}: Postgres has {actual} rows, Delta has {expected}"


@pytest.mark.postgres
class TestSpotCheckValues:
    def test_matchup_aggregates_values(self, pg_conn):
        delta_df = pl.read_delta(str(GOLD_PATH / "matchup_aggregates"))
        row = delta_df.row(0, named=True)

        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT win_rate_a, sample_size
                FROM gold.matchup_aggregates
                WHERE champion_a_id = %s
                  AND champion_b_id = %s
                  AND lane = %s
                  AND interval_min IS NOT DISTINCT FROM %s
                  AND patch = %s
                  AND tier = %s
                """,
                (
                    row["champion_a_id"],
                    row["champion_b_id"],
                    row["lane"],
                    row.get("interval_min"),
                    row["patch"],
                    row["tier"],
                ),
            )
            pg_row = cur.fetchone()

        assert pg_row is not None, "Expected row not found in Postgres"
        pg_win_rate, pg_sample = pg_row

        if row["win_rate_a"] is None:
            assert pg_win_rate is None
        else:
            assert abs(pg_win_rate - row["win_rate_a"]) < 1e-6, (
                f"win_rate_a mismatch: PG={pg_win_rate}, Delta={row['win_rate_a']}"
            )

        assert pg_sample == row["sample_size"], (
            f"sample_size mismatch: PG={pg_sample}, Delta={row['sample_size']}"
        )


@pytest.mark.postgres
class TestSchemaCompleteness:
    @pytest.mark.parametrize("table", GOLD_TABLES)
    def test_all_delta_columns_exist_in_postgres(self, pg_conn, table):
        delta_cols = set(pl.read_delta(str(GOLD_PATH / table)).columns)

        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'gold' AND table_name = %s
                """,
                (table,),
            )
            pg_cols = {r[0] for r in cur.fetchall()}

        missing = delta_cols - pg_cols
        assert not missing, f"{table}: columns in Delta but not Postgres: {missing}"
