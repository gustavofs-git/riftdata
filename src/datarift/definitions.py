"""Dagster software-defined assets for DataRift Bronze→Silver pipeline.

Bronze extraction is split into 6 independent assets — one per Riot API
entity — so each can be materialized, monitored, and retried individually.
Silver assets depend on the relevant Bronze assets.

Entrypoint: ``defs`` — a :class:`dagster.Definitions` object registered at module level.
"""

import asyncio
import json
import pathlib
import time
import uuid

import polars as pl
from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    MaterializeResult,
    asset,
    define_asset_job,
    in_process_executor,
)
from dagster._core.errors import DagsterInvalidPropertyError
from deltalake import DeltaTable

from datarift.bronze_writer import BronzeWriter
from datarift.config import ExtractionConfig
from datarift.extractors import (
    extract_accounts,
    extract_league_entries,
    extract_match_details,
    extract_match_ids,
    extract_match_timelines,
    extract_summoners,
)
from datarift.logging import configure_logging
from datarift.riot_client import RiotRateLimiter
from datarift.silver_league import (
    transform_accounts,
    transform_league_entries,
    transform_summoners,
)
from datarift.silver_match import (
    transform_match_participants,
    transform_match_teams,
    transform_match_teams_bans,
    transform_match_teams_objectives,
    transform_matches,
    write_silver,
)
from datarift.silver_timeline import (
    transform_match_timeline_events,
    transform_match_timeline_frames,
    transform_match_timeline_participant_frames,
)
from datarift.gold_matchup import transform_matchup_aggregates, transform_matchup_detail, transform_matchup_intervals, write_gold


def _get_run_id(context: AssetExecutionContext) -> str:
    """Extract run_id from context, with fallback for direct invocation."""
    try:
        return context.run.run_id
    except DagsterInvalidPropertyError:
        return uuid.uuid4().hex[:8]


def _load_config() -> ExtractionConfig:
    """Load extraction config from sample.yaml or defaults."""
    config_path = pathlib.Path("config/sample.yaml")
    if config_path.exists():
        import yaml

        with open(config_path) as f:
            raw = yaml.safe_load(f)
        return ExtractionConfig(**raw)
    return ExtractionConfig(
        region="br",
        tiers=["CHALLENGER", "GRANDMASTER", "MASTER"],
    )


def _read_puuids_from_bronze(bronze_path: str) -> list[str]:
    """Read all puuids from the league_entries_raw Bronze table."""
    table_path = f"{bronze_path}/league_entries_raw"
    if not DeltaTable.is_deltatable(table_path):
        return []
    dt = DeltaTable(table_path)
    df = pl.DataFrame(dt.to_pyarrow_table(columns=["puuid"]))
    return df["puuid"].unique().to_list()


def _read_match_ids_from_bronze(bronze_path: str) -> list[str]:
    """Read all match IDs from the match_ids_raw Bronze table."""
    table_path = f"{bronze_path}/match_ids_raw"
    if not DeltaTable.is_deltatable(table_path):
        return []
    dt = DeltaTable(table_path)
    df = pl.DataFrame(dt.to_pyarrow_table(columns=["raw_json"]))
    all_ids: list[str] = []
    for row in df.iter_rows(named=True):
        all_ids.extend(json.loads(row["raw_json"]))
    return list(set(all_ids))


# ---------------------------------------------------------------------------
# Bronze assets — one per entity
# ---------------------------------------------------------------------------


@asset(group_name="bronze")
def bronze_league_entries(context: AssetExecutionContext) -> MaterializeResult:
    """Extract league entries from Riot API (League-Exp-V4).

    This is the root Bronze asset — all other Bronze extractors depend on
    the puuids produced here.
    """
    configure_logging(_get_run_id(context), "bronze_league_entries")
    api_key = EnvVar("RIOT_API_KEY").get_value()
    config = _load_config()

    writer = BronzeWriter("league_entries_raw", "puuid", config.bronze_path)
    t0 = time.monotonic()

    async def _run():
        async with RiotRateLimiter(api_key=api_key, base_url=config.platform_host) as client:
            return await extract_league_entries(client, config, writer)

    puuids = asyncio.run(_run())
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={"puuids": len(puuids), "total_wall_time": wall_time},
    )


@asset(deps=[bronze_league_entries], group_name="bronze")
def bronze_accounts(context: AssetExecutionContext) -> MaterializeResult:
    """Extract account data for each puuid (Account-V1, regional routing)."""
    configure_logging(_get_run_id(context), "bronze_accounts")
    api_key = EnvVar("RIOT_API_KEY").get_value()
    config = _load_config()

    puuids = _read_puuids_from_bronze(config.bronze_path)
    writer = BronzeWriter("accounts_raw", "puuid", config.bronze_path)
    t0 = time.monotonic()

    async def _run():
        async with RiotRateLimiter(api_key=api_key, base_url=config.regional_host) as client:
            await extract_accounts(client, puuids, config, writer)

    asyncio.run(_run())
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={"puuids": len(puuids), "total_wall_time": wall_time},
    )


@asset(deps=[bronze_league_entries], group_name="bronze")
def bronze_summoners(context: AssetExecutionContext) -> MaterializeResult:
    """Extract summoner data for each puuid (Summoner-V4, platform routing)."""
    configure_logging(_get_run_id(context), "bronze_summoners")
    api_key = EnvVar("RIOT_API_KEY").get_value()
    config = _load_config()

    puuids = _read_puuids_from_bronze(config.bronze_path)
    writer = BronzeWriter("summoners_raw", "puuid", config.bronze_path)
    t0 = time.monotonic()

    async def _run():
        async with RiotRateLimiter(api_key=api_key, base_url=config.platform_host) as client:
            await extract_summoners(client, puuids, config, writer)

    asyncio.run(_run())
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={"puuids": len(puuids), "total_wall_time": wall_time},
    )


@asset(deps=[bronze_league_entries], group_name="bronze")
def bronze_match_ids(context: AssetExecutionContext) -> MaterializeResult:
    """Extract match ID lists for each puuid (Match-V5, regional routing)."""
    configure_logging(_get_run_id(context), "bronze_match_ids")
    api_key = EnvVar("RIOT_API_KEY").get_value()
    config = _load_config()

    puuids = _read_puuids_from_bronze(config.bronze_path)
    writer = BronzeWriter("match_ids_raw", "puuid", config.bronze_path)
    t0 = time.monotonic()

    async def _run():
        async with RiotRateLimiter(api_key=api_key, base_url=config.regional_host) as client:
            return await extract_match_ids(client, puuids, config, writer)

    match_ids = asyncio.run(_run())
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={
            "puuids": len(puuids),
            "match_ids": len(match_ids),
            "total_wall_time": wall_time,
        },
    )


@asset(deps=[bronze_match_ids], group_name="bronze")
def bronze_match_details(context: AssetExecutionContext) -> MaterializeResult:
    """Extract match detail data for each match_id (Match-V5, regional routing)."""
    configure_logging(_get_run_id(context), "bronze_match_details")
    api_key = EnvVar("RIOT_API_KEY").get_value()
    config = _load_config()

    match_ids = _read_match_ids_from_bronze(config.bronze_path)
    writer = BronzeWriter("match_details_raw", "match_id", config.bronze_path)
    t0 = time.monotonic()

    async def _run():
        async with RiotRateLimiter(api_key=api_key, base_url=config.regional_host) as client:
            await extract_match_details(client, match_ids, config, writer)

    asyncio.run(_run())
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={"match_ids": len(match_ids), "total_wall_time": wall_time},
    )


@asset(deps=[bronze_match_ids], group_name="bronze")
def bronze_match_timelines(context: AssetExecutionContext) -> MaterializeResult:
    """Extract match timeline data for each match_id (Match-V5, regional routing)."""
    configure_logging(_get_run_id(context), "bronze_match_timelines")
    api_key = EnvVar("RIOT_API_KEY").get_value()
    config = _load_config()

    match_ids = _read_match_ids_from_bronze(config.bronze_path)
    writer = BronzeWriter("match_timelines_raw", "match_id", config.bronze_path)
    t0 = time.monotonic()

    async def _run():
        async with RiotRateLimiter(api_key=api_key, base_url=config.regional_host) as client:
            await extract_match_timelines(client, match_ids, config, writer)

    asyncio.run(_run())
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={"match_ids": len(match_ids), "total_wall_time": wall_time},
    )


# ---------------------------------------------------------------------------
# Silver assets — one per table
# ---------------------------------------------------------------------------


def _materialize_silver(
    context: AssetExecutionContext,
    asset_name: str,
    bronze_table: str,
    silver_table: str,
    transform,
    predicate: str,
    chunked: bool = False,
) -> MaterializeResult:
    """Shared helper: read one Bronze table, transform, write one Silver table.

    When chunked=True, processes silver_batch_size rows at a time to cap memory.
    The MERGE write is idempotent, so this is resume-safe after a crash.
    """
    import gc

    import structlog

    log = structlog.get_logger()
    configure_logging(_get_run_id(context), asset_name)

    bronze_path = f"data/bronze/{bronze_table}"
    if not DeltaTable.is_deltatable(bronze_path):
        return MaterializeResult(metadata={"rows": 0, "skipped": True})

    config = _load_config()
    silver_path = f"data/silver/{silver_table}"
    t0 = time.monotonic()

    if not chunked:
        raw_df = pl.scan_delta(bronze_path).collect()
        transformed = transform(raw_df)
        row_count = len(transformed)
        write_silver(transformed, silver_path, predicate)
    else:
        batch_size = config.silver_batch_size
        dt = DeltaTable(bronze_path)
        total_rows = dt.to_pyarrow_dataset().count_rows()
        row_count = 0

        for offset in range(0, total_rows, batch_size):
            chunk_df = pl.scan_delta(bronze_path).slice(offset, batch_size).collect()
            if chunk_df.is_empty():
                break

            transformed = transform(chunk_df)
            chunk_rows = len(transformed)
            row_count += chunk_rows

            if chunk_rows > 0:
                write_silver(transformed, silver_path, predicate)

            log.info(
                "silver_chunk_written",
                asset=asset_name,
                offset=offset,
                chunk_rows=chunk_rows,
                total_so_far=row_count,
            )
            del chunk_df, transformed
            gc.collect()

    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={"rows": row_count, "total_wall_time": wall_time},
    )


# --- Match-detail Silver tables (from bronze_match_details) ---


@asset(deps=[bronze_match_details], group_name="silver")
def silver_matches(context: AssetExecutionContext) -> MaterializeResult:
    """Silver matches table — one row per match with metadata and game info."""
    return _materialize_silver(
        context, "silver_matches", "match_details_raw", "matches",
        transform_matches, "s.match_id = t.match_id",
    )


@asset(deps=[bronze_match_details], group_name="silver")
def silver_match_participants(context: AssetExecutionContext) -> MaterializeResult:
    """Silver match_participants table — one row per match × participant."""
    return _materialize_silver(
        context, "silver_match_participants", "match_details_raw", "match_participants",
        transform_match_participants,
        "s.match_id = t.match_id AND s.participant_id = t.participant_id",
        chunked=True,
    )


@asset(deps=[bronze_match_details], group_name="silver")
def silver_match_teams(context: AssetExecutionContext) -> MaterializeResult:
    """Silver match_teams table — one row per match × team."""
    return _materialize_silver(
        context, "silver_match_teams", "match_details_raw", "match_teams",
        transform_match_teams, "s.match_id = t.match_id AND s.team_id = t.team_id",
    )


@asset(deps=[bronze_match_details], group_name="silver")
def silver_match_teams_bans(context: AssetExecutionContext) -> MaterializeResult:
    """Silver match_teams_bans table — one row per match × team × ban."""
    return _materialize_silver(
        context, "silver_match_teams_bans", "match_details_raw", "match_teams_bans",
        transform_match_teams_bans,
        "s.match_id = t.match_id AND s.team_id = t.team_id AND s.pick_turn = t.pick_turn",
    )


@asset(deps=[bronze_match_details], group_name="silver")
def silver_match_teams_objectives(context: AssetExecutionContext) -> MaterializeResult:
    """Silver match_teams_objectives table — one row per match × team × objective."""
    return _materialize_silver(
        context, "silver_match_teams_objectives", "match_details_raw", "match_teams_objectives",
        transform_match_teams_objectives,
        "s.match_id = t.match_id AND s.team_id = t.team_id AND s.objective_name = t.objective_name",
    )


# --- Timeline Silver tables (from bronze_match_timelines) ---


@asset(deps=[bronze_match_timelines], group_name="silver")
def silver_match_timeline_frames(context: AssetExecutionContext) -> MaterializeResult:
    """Silver match_timeline_frames table — one row per match × frame."""
    return _materialize_silver(
        context, "silver_match_timeline_frames", "match_timelines_raw", "match_timeline_frames",
        transform_match_timeline_frames,
        "s.match_id = t.match_id AND s.frame_index = t.frame_index",
        chunked=True,
    )


@asset(deps=[bronze_match_timelines], group_name="silver")
def silver_match_timeline_participant_frames(context: AssetExecutionContext) -> MaterializeResult:
    """Silver match_timeline_participant_frames table — one row per match × frame × participant."""
    return _materialize_silver(
        context, "silver_match_timeline_participant_frames", "match_timelines_raw",
        "match_timeline_participant_frames", transform_match_timeline_participant_frames,
        "s.match_id = t.match_id AND s.frame_index = t.frame_index AND s.participant_id = t.participant_id",
        chunked=True,
    )


@asset(deps=[bronze_match_timelines], group_name="silver")
def silver_match_timeline_events(context: AssetExecutionContext) -> MaterializeResult:
    """Silver match_timeline_events table — one row per match × frame × event."""
    return _materialize_silver(
        context, "silver_match_timeline_events", "match_timelines_raw", "match_timeline_events",
        transform_match_timeline_events,
        "s.match_id = t.match_id AND s.frame_index = t.frame_index AND s.event_index = t.event_index",
        chunked=True,
    )


# --- League/Summoner/Account Silver tables (each from its own Bronze) ---


@asset(deps=[bronze_league_entries], group_name="silver")
def silver_league_entries(context: AssetExecutionContext) -> MaterializeResult:
    """Silver league_entries table — one row per puuid with tier, rank, LP."""
    return _materialize_silver(
        context, "silver_league_entries", "league_entries_raw", "league_entries",
        transform_league_entries, "s.puuid = t.puuid",
    )


@asset(deps=[bronze_summoners], group_name="silver")
def silver_summoners(context: AssetExecutionContext) -> MaterializeResult:
    """Silver summoners table — one row per puuid with profile info."""
    return _materialize_silver(
        context, "silver_summoners", "summoners_raw", "summoners",
        transform_summoners, "s.puuid = t.puuid",
    )


@asset(deps=[bronze_accounts], group_name="silver")
def silver_accounts(context: AssetExecutionContext) -> MaterializeResult:
    """Silver accounts table — one row per puuid with game name and tag."""
    return _materialize_silver(
        context, "silver_accounts", "accounts_raw", "accounts",
        transform_accounts, "s.puuid = t.puuid",
    )


# ---------------------------------------------------------------------------
# Gold assets
# ---------------------------------------------------------------------------


@asset(deps=[silver_match_participants], group_name="gold")
def gold_matchup_detail(context: AssetExecutionContext) -> MaterializeResult:
    """Gold matchup_detail table — one row per match × lane with champion-vs-champion stats."""
    import structlog

    configure_logging(_get_run_id(context), "gold_matchup_detail")
    log = structlog.get_logger()

    silver_path = "data/silver/match_participants"
    if not DeltaTable.is_deltatable(silver_path):
        return MaterializeResult(metadata={"rows": 0, "skipped": True})

    t0 = time.monotonic()
    participants = pl.scan_delta(silver_path).collect()
    transformed = transform_matchup_detail(participants)
    row_count = len(transformed)

    filtered_rows = len(participants) - len(
        participants.filter(
            pl.col("team_position").is_not_null() & (pl.col("team_position") != "")
        )
    )

    if row_count > 0:
        write_gold(transformed, "data/gold/matchup_detail")

    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={
            "rows": row_count,
            "filtered_rows": filtered_rows,
            "total_wall_time": wall_time,
        },
    )


@asset(
    deps=[
        silver_match_participants,
        silver_match_timeline_frames,
        silver_match_timeline_participant_frames,
        gold_matchup_detail,
    ],
    group_name="gold",
)
def gold_matchup_intervals(context: AssetExecutionContext) -> MaterializeResult:
    """Gold matchup_intervals table — per-interval (5/10/15/20 min) stat snapshots per matchup."""
    import structlog

    configure_logging(_get_run_id(context), "gold_matchup_intervals")
    log = structlog.get_logger()

    silver_participants_path = "data/silver/match_participants"
    silver_frames_path = "data/silver/match_timeline_frames"
    silver_pframes_path = "data/silver/match_timeline_participant_frames"
    gold_matchup_path = "data/gold/matchup_detail"

    for path in [silver_participants_path, silver_frames_path, silver_pframes_path, gold_matchup_path]:
        if not DeltaTable.is_deltatable(path):
            return MaterializeResult(metadata={"rows": 0, "skipped": True})

    t0 = time.monotonic()
    matchup_detail_df = pl.scan_delta(gold_matchup_path).collect()
    participants = pl.scan_delta(silver_participants_path).collect()
    timeline_frames = pl.scan_delta(silver_frames_path).collect()
    participant_frames = pl.scan_delta(silver_pframes_path).collect()

    transformed = transform_matchup_intervals(
        matchup_detail_df, participants, timeline_frames, participant_frames,
    )
    row_count = len(transformed)

    missing_intervals = transformed.select(
        pl.col("total_gold_a").is_null().sum()
    ).item() if row_count > 0 else 0

    if row_count > 0:
        write_gold(transformed, "data/gold/matchup_intervals")

    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={
            "rows": row_count,
            "missing_intervals": missing_intervals,
            "total_wall_time": wall_time,
        },
    )


@asset(
    deps=[
        gold_matchup_detail,
        gold_matchup_intervals,
        silver_matches,
        silver_match_participants,
        silver_league_entries,
    ],
    group_name="gold",
)
def gold_matchup_aggregates(context: AssetExecutionContext) -> MaterializeResult:
    """Gold matchup_aggregates table — averaged stats per (champion, opponent, lane, interval, patch, tier)."""
    configure_logging(_get_run_id(context), "gold_matchup_aggregates")

    gold_detail_path = "data/gold/matchup_detail"
    gold_intervals_path = "data/gold/matchup_intervals"
    silver_matches_path = "data/silver/matches"
    silver_participants_path = "data/silver/match_participants"
    silver_league_path = "data/silver/league_entries"

    for path in [gold_detail_path, gold_intervals_path, silver_matches_path, silver_participants_path, silver_league_path]:
        if not DeltaTable.is_deltatable(path):
            return MaterializeResult(metadata={"rows": 0, "skipped": True})

    t0 = time.monotonic()
    matchup_detail_df = pl.scan_delta(gold_detail_path).collect()
    intervals_df = pl.scan_delta(gold_intervals_path).collect()
    matches_df = pl.scan_delta(silver_matches_path).collect()
    participants_df = pl.scan_delta(silver_participants_path).collect()
    league_entries_df = pl.scan_delta(silver_league_path).collect()

    transformed = transform_matchup_aggregates(
        matchup_detail_df, intervals_df, matches_df, participants_df, league_entries_df,
        min_sample_size=1,
    )
    row_count = len(transformed)

    if row_count > 0:
        write_gold(transformed, "data/gold/matchup_aggregates")

    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={"rows": row_count, "total_wall_time": wall_time},
    )


# ---------------------------------------------------------------------------
# Jobs — in-process executor avoids OOM from parallel Silver materializations
# ---------------------------------------------------------------------------

all_assets_job = define_asset_job(
    name="all_assets_job",
    selection="*",
    executor_def=in_process_executor,
)

# ---------------------------------------------------------------------------
# Definitions entrypoint
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[
        # Bronze
        bronze_league_entries,
        bronze_accounts,
        bronze_summoners,
        bronze_match_ids,
        bronze_match_details,
        bronze_match_timelines,
        # Silver — match detail tables
        silver_matches,
        silver_match_participants,
        silver_match_teams,
        silver_match_teams_bans,
        silver_match_teams_objectives,
        # Silver — timeline tables
        silver_match_timeline_frames,
        silver_match_timeline_participant_frames,
        silver_match_timeline_events,
        # Silver — league / summoner / account tables
        silver_league_entries,
        silver_summoners,
        silver_accounts,
        # Gold
        gold_matchup_detail,
        gold_matchup_intervals,
        gold_matchup_aggregates,
    ],
    jobs=[all_assets_job],
    executor=in_process_executor,
)
