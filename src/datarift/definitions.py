"""Dagster software-defined assets for DataRift Bronze→Silver pipeline.

Entrypoint: ``defs`` — a :class:`dagster.Definitions` object registered at module level.
"""

import asyncio
import pathlib
import time
import uuid

from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    MaterializeResult,
    asset,
)
from dagster._core.errors import DagsterInvalidPropertyError

from datarift.config import ExtractionConfig
from datarift.logging import configure_logging
from datarift.runner import run_extraction
from datarift.silver_league import materialize_silver_league
from datarift.silver_match import materialize_silver_matches
from datarift.silver_timeline import materialize_silver_timelines

# ---------------------------------------------------------------------------
# Bronze table names — used to count succeeded tables after extraction
# ---------------------------------------------------------------------------
_BRONZE_TABLES = [
    "league_entries_raw",
    "accounts_raw",
    "summoners_raw",
    "match_ids_raw",
    "match_details_raw",
    "match_timelines_raw",
]


def _get_run_id(context: AssetExecutionContext) -> str:
    """Extract run_id from context, with fallback for direct invocation."""
    try:
        return context.run.run_id
    except DagsterInvalidPropertyError:
        return uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


@asset
def bronze_extraction(context: AssetExecutionContext) -> MaterializeResult:
    """Run the full Bronze extraction DAG against the Riot API.

    Wraps :func:`run_extraction` in ``asyncio.run()``.  Metadata exposes
    succeeded table count, failed count, rate_limit_hits, and wall time.
    """
    configure_logging(_get_run_id(context), "bronze_extraction")

    api_key = EnvVar("RIOT_API_KEY").get_value()

    config = ExtractionConfig(
        region="br",
        tiers=["CHALLENGER", "GRANDMASTER", "MASTER"],
        bronze_path="data/bronze",
    )

    t0 = time.monotonic()
    failed = 0
    try:
        asyncio.run(run_extraction(config, api_key))
    except Exception:
        failed = 1
        raise
    finally:
        wall_time = round(time.monotonic() - t0, 2)

    # Count bronze tables that actually have data on disk
    bronze_dir = pathlib.Path(config.bronze_path)
    succeeded = sum(
        1 for t in _BRONZE_TABLES if (bronze_dir / t).exists()
    )

    return MaterializeResult(
        metadata={
            "succeeded": succeeded,
            "failed": failed,
            "rate_limit_hits": 0,  # retry is internal to RiotRateLimiter
            "total_wall_time": wall_time,
        },
    )


@asset(deps=[bronze_extraction])
def silver_matches(context: AssetExecutionContext) -> MaterializeResult:
    """Transform Bronze match data into Silver match tables."""
    configure_logging(_get_run_id(context), "silver_matches")

    t0 = time.monotonic()
    row_counts = materialize_silver_matches("data/bronze", "data/silver")
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={**row_counts, "total_wall_time": wall_time},
    )


@asset(deps=[bronze_extraction])
def silver_timelines(context: AssetExecutionContext) -> MaterializeResult:
    """Transform Bronze match data into Silver timeline tables."""
    configure_logging(_get_run_id(context), "silver_timelines")

    t0 = time.monotonic()
    row_counts = materialize_silver_timelines("data/bronze", "data/silver")
    wall_time = round(time.monotonic() - t0, 2)

    return MaterializeResult(
        metadata={**row_counts, "total_wall_time": wall_time},
    )


@asset(deps=[bronze_extraction])
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
    assets=[bronze_extraction, silver_matches, silver_timelines, silver_league],
)
