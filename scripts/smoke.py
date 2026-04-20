"""Smoke test: seed Bronze Delta tables from committed fixtures, run Silver transforms, assert all 11 tables."""

from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path

import polars as pl

from datarift.bronze_writer import BronzeWriter
from datarift.silver_league import materialize_silver_league
from datarift.silver_match import materialize_silver_matches
from datarift.silver_timeline import materialize_silver_timelines

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
SMOKE_DIR = Path(__file__).resolve().parent.parent / "data" / "_smoke"

# Bronze table configs: (fixture_file, table_name, primary_key_col, endpoint)
BRONZE_TABLES = [
    ("match_details.json", "match_details_raw", "match_id", "/lol/match/v5/matches"),
    ("match_timelines.json", "match_timelines_raw", "match_id", "/lol/match/v5/matches/by-match/timeline"),
    ("league_entries.json", "league_entries_raw", "puuid", "/lol/league/v4/entries"),
    ("summoners.json", "summoners_raw", "puuid", "/lol/summoner/v4/summoners"),
    ("accounts.json", "accounts_raw", "puuid", "/riot/account/v1/accounts"),
]

# All 11 expected Silver tables
EXPECTED_SILVER_TABLES = [
    "matches",
    "match_participants",
    "match_teams",
    "match_teams_bans",
    "match_teams_objectives",
    "match_timeline_frames",
    "match_timeline_participant_frames",
    "match_timeline_events",
    "league_entries",
    "summoners",
    "accounts",
]


def _primary_key_for_fixture(fixture_file: str, record: dict) -> str:
    """Extract the primary key value from a raw fixture record for match_details/timelines."""
    if fixture_file in ("match_details.json", "match_timelines.json"):
        return record["metadata"]["matchId"]
    return record["puuid"]


def seed_bronze(bronze_path: str) -> None:
    """Read fixture JSON files and write them into Bronze Delta tables via BronzeWriter."""
    for fixture_file, table_name, pk_col, endpoint in BRONZE_TABLES:
        fixture_path = FIXTURES_DIR / fixture_file
        with open(fixture_path) as f:
            raw_records = json.load(f)

        writer = BronzeWriter(
            table_name=table_name,
            primary_key_col=pk_col,
            base_path=bronze_path,
        )
        records = [
            {pk_col: _primary_key_for_fixture(fixture_file, rec), "raw_json": json.dumps(rec)}
            for rec in raw_records
        ]
        writer.write_batch(
            records=records,
            endpoint=endpoint,
            status_code=200,
            region="americas",
        )


def run_silver(bronze_path: str, silver_path: str) -> dict[str, int]:
    """Run all three Silver materializers and return combined table→row-count map."""
    result: dict[str, int] = {}
    result.update(materialize_silver_matches(bronze_path, silver_path))
    result.update(materialize_silver_timelines(bronze_path, silver_path))
    result.update(materialize_silver_league(bronze_path, silver_path))
    return result


def verify_silver(silver_path: str) -> dict[str, int]:
    """Assert all 11 Silver tables exist with >0 rows. Returns table→row-count map."""
    result: dict[str, int] = {}
    for table_name in EXPECTED_SILVER_TABLES:
        table_path = f"{silver_path}/{table_name}"
        df = pl.read_delta(table_path)
        row_count = len(df)
        if row_count == 0:
            raise AssertionError(f"Silver table {table_name} has 0 rows")
        result[table_name] = row_count
    return result


def run_smoke(smoke_dir: Path | None = None) -> dict[str, int]:
    """Full smoke run: clean up, seed Bronze, run Silver, verify. Returns table→row-count."""
    if smoke_dir is None:
        smoke_dir = SMOKE_DIR

    # Clean up for idempotency
    if smoke_dir.exists():
        shutil.rmtree(smoke_dir)

    bronze_path = str(smoke_dir / "bronze")
    silver_path = str(smoke_dir / "silver")

    seed_bronze(bronze_path)
    run_silver(bronze_path, silver_path)
    return verify_silver(silver_path)


def main() -> None:
    start = time.monotonic()
    print("Smoke test: seeding Bronze from fixtures and running Silver transforms...")

    result = run_smoke()

    elapsed = time.monotonic() - start
    print(f"\nAll {len(result)} Silver tables verified ({elapsed:.1f}s):")
    for table_name, row_count in sorted(result.items()):
        print(f"  {table_name}: {row_count} rows")

    if elapsed > 30:
        print(f"\nWARNING: smoke took {elapsed:.1f}s (>30s budget)", file=sys.stderr)
        sys.exit(1)

    print("\nSmoke test PASSED.")


if __name__ == "__main__":
    main()
