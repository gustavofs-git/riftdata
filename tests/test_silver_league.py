"""Tests for Silver league, summoner, and account transforms."""

from __future__ import annotations

import json

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from datarift.bronze_writer import BronzeWriter
from datarift.silver_league import (
    materialize_silver_league,
    transform_accounts,
    transform_league_entries,
    transform_summoners,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_league_entry_json(
    puuid: str = "puuid1",
    league_id: str = "league-abc-123",
    queue_type: str = "RANKED_SOLO_5x5",
    tier: str = "CHALLENGER",
    rank: str = "I",
    league_points: int = 742,
    wins: int = 200,
    losses: int = 150,
    veteran: bool = True,
    inactive: bool = False,
    fresh_blood: bool = False,
    hot_streak: bool = True,
) -> str:
    return json.dumps(
        {
            "puuid": puuid,
            "leagueId": league_id,
            "queueType": queue_type,
            "tier": tier,
            "rank": rank,
            "leaguePoints": league_points,
            "wins": wins,
            "losses": losses,
            "veteran": veteran,
            "inactive": inactive,
            "freshBlood": fresh_blood,
            "hotStreak": hot_streak,
        }
    )


def _make_summoner_json(
    puuid: str = "puuid1",
    profile_icon_id: int = 7035,
    revision_date: int = 1773941590152,
    summoner_level: int = 100,
) -> str:
    return json.dumps(
        {
            "puuid": puuid,
            "profileIconId": profile_icon_id,
            "revisionDate": revision_date,
            "summonerLevel": summoner_level,
        }
    )


def _make_account_json(
    puuid: str = "puuid1",
    game_name: str = "Player1",
    tag_line: str = "NA1",
) -> str:
    return json.dumps(
        {
            "puuid": puuid,
            "gameName": game_name,
            "tagLine": tag_line,
        }
    )


def _raw_df(jsons: list[str]) -> pl.DataFrame:
    """Wrap JSON strings into a single-column DataFrame matching Bronze schema."""
    return pl.DataFrame({"raw_json": pl.Series(jsons, dtype=pl.Utf8)})


# ---------------------------------------------------------------------------
# TestTransformLeagueEntries
# ---------------------------------------------------------------------------

LEAGUE_ENTRIES_COLUMNS = {
    "puuid",
    "league_id",
    "queue_type",
    "tier",
    "rank",
    "league_points",
    "wins",
    "losses",
    "is_veteran",
    "is_inactive",
    "is_fresh_blood",
    "is_hot_streak",
}


class TestTransformLeagueEntries:
    def test_schema(self):
        df = _raw_df([_make_league_entry_json()])
        result = transform_league_entries(df)
        assert set(result.columns) == LEAGUE_ENTRIES_COLUMNS
        assert len(result.columns) == 12

    def test_row_count(self):
        df = _raw_df([_make_league_entry_json(), _make_league_entry_json(puuid="puuid2")])
        result = transform_league_entries(df)
        assert len(result) == 2

    def test_values(self):
        df = _raw_df([_make_league_entry_json(puuid="abc", tier="GOLD", league_points=50)])
        result = transform_league_entries(df)
        row = result.to_dicts()[0]
        assert row["puuid"] == "abc"
        assert row["tier"] == "GOLD"
        assert row["league_points"] == 50

    def test_boolean_is_prefix(self):
        """Boolean columns must have is_ prefix — not bare camelCase snake conversions."""
        df = _raw_df([_make_league_entry_json(veteran=True, inactive=False)])
        result = transform_league_entries(df)
        # is_ prefixed columns exist
        assert "is_veteran" in result.columns
        assert "is_inactive" in result.columns
        assert "is_fresh_blood" in result.columns
        assert "is_hot_streak" in result.columns
        # bare names must NOT exist
        assert "veteran" not in result.columns
        assert "inactive" not in result.columns
        assert "fresh_blood" not in result.columns
        assert "hot_streak" not in result.columns

    def test_boolean_values(self):
        df = _raw_df([_make_league_entry_json(veteran=True, inactive=False, fresh_blood=True, hot_streak=False)])
        result = transform_league_entries(df)
        row = result.to_dicts()[0]
        assert row["is_veteran"] is True
        assert row["is_inactive"] is False
        assert row["is_fresh_blood"] is True
        assert row["is_hot_streak"] is False

    def test_empty_input(self):
        df = _raw_df([])
        result = transform_league_entries(df)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# TestTransformSummoners
# ---------------------------------------------------------------------------

SUMMONERS_COLUMNS = {"puuid", "profile_icon_id", "revision_date", "summoner_level"}


class TestTransformSummoners:
    def test_schema(self):
        df = _raw_df([_make_summoner_json()])
        result = transform_summoners(df)
        assert set(result.columns) == SUMMONERS_COLUMNS
        assert len(result.columns) == 4

    def test_row_count(self):
        df = _raw_df([_make_summoner_json(), _make_summoner_json(puuid="puuid2")])
        result = transform_summoners(df)
        assert len(result) == 2

    def test_values(self):
        df = _raw_df([_make_summoner_json(puuid="xyz", summoner_level=350, profile_icon_id=999)])
        result = transform_summoners(df)
        row = result.to_dicts()[0]
        assert row["puuid"] == "xyz"
        assert row["summoner_level"] == 350
        assert row["profile_icon_id"] == 999

    def test_empty_input(self):
        df = _raw_df([])
        result = transform_summoners(df)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# TestTransformAccounts
# ---------------------------------------------------------------------------

ACCOUNTS_COLUMNS = {"puuid", "game_name", "tag_line"}


class TestTransformAccounts:
    def test_schema(self):
        df = _raw_df([_make_account_json()])
        result = transform_accounts(df)
        assert set(result.columns) == ACCOUNTS_COLUMNS
        assert len(result.columns) == 3

    def test_row_count(self):
        df = _raw_df([_make_account_json(), _make_account_json(puuid="puuid2")])
        result = transform_accounts(df)
        assert len(result) == 2

    def test_values(self):
        df = _raw_df([_make_account_json(puuid="abc", game_name="Faker", tag_line="KR1")])
        result = transform_accounts(df)
        row = result.to_dicts()[0]
        assert row["puuid"] == "abc"
        assert row["game_name"] == "Faker"
        assert row["tag_line"] == "KR1"

    def test_empty_input(self):
        df = _raw_df([])
        result = transform_accounts(df)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Integration test helpers — write synthetic Bronze data via BronzeWriter
# ---------------------------------------------------------------------------


def _write_bronze_league_entry(bronze_path: str, puuid: str, **kwargs) -> None:
    raw_json = _make_league_entry_json(puuid=puuid, **kwargs)
    writer = BronzeWriter(
        table_name="league_entries_raw",
        primary_key_col="puuid",
        base_path=bronze_path,
    )
    writer.write_batch(
        records=[{"puuid": puuid, "raw_json": raw_json}],
        endpoint="/lol/league/v4/entries",
        status_code=200,
        region="br1",
    )


def _write_bronze_summoner(bronze_path: str, puuid: str, **kwargs) -> None:
    raw_json = _make_summoner_json(puuid=puuid, **kwargs)
    writer = BronzeWriter(
        table_name="summoners_raw",
        primary_key_col="puuid",
        base_path=bronze_path,
    )
    writer.write_batch(
        records=[{"puuid": puuid, "raw_json": raw_json}],
        endpoint="/lol/summoner/v4/summoners",
        status_code=200,
        region="br1",
    )


def _write_bronze_account(bronze_path: str, puuid: str, **kwargs) -> None:
    raw_json = _make_account_json(puuid=puuid, **kwargs)
    writer = BronzeWriter(
        table_name="accounts_raw",
        primary_key_col="puuid",
        base_path=bronze_path,
    )
    writer.write_batch(
        records=[{"puuid": puuid, "raw_json": raw_json}],
        endpoint="/riot/account/v1/accounts",
        status_code=200,
        region="americas",
    )


def _write_all_bronze(bronze_path: str, puuid: str) -> None:
    """Write one entry to each of the 3 Bronze tables for a given puuid."""
    _write_bronze_league_entry(bronze_path, puuid)
    _write_bronze_summoner(bronze_path, puuid)
    _write_bronze_account(bronze_path, puuid)


# ---------------------------------------------------------------------------
# materialize_silver_league integration tests
# ---------------------------------------------------------------------------


class TestMaterializeSilverLeagueCreatesAllThreeTables:
    """Write 2 entries per Bronze table → materialize → verify 3 Silver Delta tables."""

    def test_creates_all_three_tables(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        _write_all_bronze(bronze_path, "puuid_a")
        _write_all_bronze(bronze_path, "puuid_b")

        result = materialize_silver_league(bronze_path, silver_path)

        assert set(result.keys()) == {"league_entries", "summoners", "accounts"}
        assert result["league_entries"] == 2
        assert result["summoners"] == 2
        assert result["accounts"] == 2

        # Verify all tables are queryable with polars.scan_delta()
        for table_name in result:
            lf = pl.scan_delta(f"{silver_path}/{table_name}")
            df = lf.collect()
            assert len(df) == result[table_name]


class TestMaterializeSilverLeagueIncremental:
    """Incremental MERGE: re-run on same data adds zero rows; new data adds correctly."""

    def test_incremental_adds_only_new(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        # First materialization: 2 entries
        _write_all_bronze(bronze_path, "puuid_a")
        _write_all_bronze(bronze_path, "puuid_b")
        first = materialize_silver_league(bronze_path, silver_path)

        # Second materialization: same data → no new rows
        materialize_silver_league(bronze_path, silver_path)
        for table_name in first:
            actual = len(pl.read_delta(f"{silver_path}/{table_name}"))
            assert actual == first[table_name], (
                f"{table_name}: expected {first[table_name]} rows after re-run, got {actual}"
            )

        # Third materialization: append 1 new entry → rows increase to 3
        _write_all_bronze(bronze_path, "puuid_c")
        materialize_silver_league(bronze_path, silver_path)

        for table_name in first:
            assert len(pl.read_delta(f"{silver_path}/{table_name}")) == 3


class TestMaterializeSilverLeagueEmptyBronze:
    """Empty Bronze tables → all Silver tables created with 0 rows."""

    def test_empty_bronze(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        # Create Bronze tables with real data, then overwrite with empty tables
        _write_all_bronze(bronze_path, "TEMP")

        for table_name in ("league_entries_raw", "summoners_raw", "accounts_raw"):
            dt = DeltaTable(f"{bronze_path}/{table_name}")
            schema = dt.to_pyarrow_table().schema
            empty_table = pa.table(
                {col: pa.array([], type=schema.field(col).type) for col in schema.names}
            )
            write_deltalake(f"{bronze_path}/{table_name}", empty_table, mode="overwrite")

        result = materialize_silver_league(bronze_path, silver_path)

        assert set(result.keys()) == {"league_entries", "summoners", "accounts"}
        for table_name, count in result.items():
            assert count == 0, f"{table_name} should have 0 rows, got {count}"


class TestMaterializeSilverLeagueMissingBronze:
    """Missing Bronze tables are skipped gracefully — only existing tables are materialized."""

    def test_missing_bronze_tables_skipped(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        # Only write league_entries_raw — summoners_raw and accounts_raw don't exist
        _write_bronze_league_entry(bronze_path, "puuid_a")

        result = materialize_silver_league(bronze_path, silver_path)

        # Only league_entries should be materialized
        assert "league_entries" in result
        assert "summoners" not in result
        assert "accounts" not in result
        assert result["league_entries"] == 1
