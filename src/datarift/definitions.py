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
from datarift.silver_league import materialize_silver_league
from datarift.silver_match import materialize_silver_matches
from datarift.silver_timeline import materialize_silver_timelines


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
# Silver assets
# ---------------------------------------------------------------------------


@asset(
    deps=[bronze_match_details],
    group_name="silver",
)
def silver_matches(context: AssetExecutionContext) -> MaterializeResult:
    """Transform Bronze match data into Silver match tables."""
    configure_logging(_get_run_id(context), "silver_matches")

    t0 = time.monotonic()
    row_counts = materialize_silver_matches("data/bronze", "data/silver")
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={**row_counts, "total_wall_time": wall_time},
    )


@asset(
    deps=[bronze_match_timelines],
    group_name="silver",
)
def silver_timelines(context: AssetExecutionContext) -> MaterializeResult:
    """Transform Bronze match data into Silver timeline tables."""
    configure_logging(_get_run_id(context), "silver_timelines")

    t0 = time.monotonic()
    row_counts = materialize_silver_timelines("data/bronze", "data/silver")
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={**row_counts, "total_wall_time": wall_time},
    )


@asset(
    deps=[bronze_league_entries, bronze_summoners, bronze_accounts],
    group_name="silver",
)
def silver_league(context: AssetExecutionContext) -> MaterializeResult:
    """Transform Bronze league/summoner/account data into Silver tables."""
    configure_logging(_get_run_id(context), "silver_league")

    t0 = time.monotonic()
    row_counts = materialize_silver_league("data/bronze", "data/silver")
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={**row_counts, "total_wall_time": wall_time},
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
        # Silver
        silver_matches,
        silver_timelines,
        silver_league,
    ],
)
