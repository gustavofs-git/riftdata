"""Silver-layer transforms for League, Summoner, and Account Bronze tables."""

from __future__ import annotations

import polars as pl
import structlog
from deltalake import DeltaTable

from datarift.silver_match import _json_int, _json_str, write_silver

log = structlog.get_logger()


def _json_bool(path: str, alias: str) -> pl.Expr:
    """Extract a boolean field from raw_json via JSONPath, converting 'true'/'false' string to Boolean."""
    return (pl.col("raw_json").str.json_path_match(path) == "true").alias(alias)


def transform_league_entries(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw_json from league_entries_raw into a flat DataFrame.

    Columns: puuid, league_id, queue_type, tier, rank, league_points,
    wins, losses, is_veteran, is_inactive, is_fresh_blood, is_hot_streak.

    Pure function — no I/O, no side effects.
    """
    return df.select(
        _json_str("$.puuid", "puuid"),
        _json_str("$.leagueId", "league_id"),
        _json_str("$.queueType", "queue_type"),
        _json_str("$.tier", "tier"),
        _json_str("$.rank", "rank"),
        _json_int("$.leaguePoints", "league_points"),
        _json_int("$.wins", "wins"),
        _json_int("$.losses", "losses"),
        _json_bool("$.veteran", "is_veteran"),
        _json_bool("$.inactive", "is_inactive"),
        _json_bool("$.freshBlood", "is_fresh_blood"),
        _json_bool("$.hotStreak", "is_hot_streak"),
    )


def transform_summoners(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw_json from summoners_raw into a flat DataFrame.

    Columns: puuid, profile_icon_id, revision_date, summoner_level.

    Pure function — no I/O, no side effects.
    """
    return df.select(
        _json_str("$.puuid", "puuid"),
        _json_int("$.profileIconId", "profile_icon_id"),
        _json_int("$.revisionDate", "revision_date"),
        _json_int("$.summonerLevel", "summoner_level"),
    )


def transform_accounts(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw_json from accounts_raw into a flat DataFrame.

    Columns: puuid, game_name, tag_line.

    Pure function — no I/O, no side effects.
    """
    return df.select(
        _json_str("$.puuid", "puuid"),
        _json_str("$.gameName", "game_name"),
        _json_str("$.tagLine", "tag_line"),
    )


# ---------------------------------------------------------------------------
# Silver league/summoner/account materialization orchestrator
# ---------------------------------------------------------------------------

_SILVER_LEAGUE_TABLES: list[dict] = [
    {
        "bronze_name": "league_entries_raw",
        "silver_name": "league_entries",
        "transform": transform_league_entries,
        "predicate": "s.puuid = t.puuid",
    },
    {
        "bronze_name": "summoners_raw",
        "silver_name": "summoners",
        "transform": transform_summoners,
        "predicate": "s.puuid = t.puuid",
    },
    {
        "bronze_name": "accounts_raw",
        "silver_name": "accounts",
        "transform": transform_accounts,
        "predicate": "s.puuid = t.puuid",
    },
]


def materialize_silver_league(bronze_path: str, silver_path: str) -> dict[str, int]:
    """Orchestrate Silver materialization for league, summoner, and account tables.

    Reads three separate Bronze tables (league_entries_raw, summoners_raw, accounts_raw),
    transforms each, and MERGEs into Silver Delta tables keyed on puuid.

    Missing Bronze tables are skipped with a warning — not all Bronze tables
    may be populated in every run.

    Returns a dict mapping Silver table name → row count written.
    """
    log.info("silver_league_materialize_start", bronze_path=bronze_path, silver_path=silver_path)

    result: dict[str, int] = {}

    for spec in _SILVER_LEAGUE_TABLES:
        bronze_name: str = spec["bronze_name"]
        silver_name: str = spec["silver_name"]
        bronze_table_path = f"{bronze_path}/{bronze_name}"

        if not DeltaTable.is_deltatable(bronze_table_path):
            log.warning("silver_league_bronze_missing", table=bronze_name)
            continue

        dt = DeltaTable(bronze_table_path)
        raw_df = pl.DataFrame(dt.to_pyarrow_table())

        transformed = spec["transform"](raw_df)
        row_count = len(transformed)

        table_path = f"{silver_path}/{silver_name}"
        write_silver(transformed, table_path, spec["predicate"])

        log.info("silver_table_written", table=silver_name, rows=row_count)
        result[silver_name] = row_count

    log.info("silver_league_materialize_complete", tables=list(result.keys()), total_rows=sum(result.values()))
    return result
