"""Top-level extraction DAG runner with SIGINT graceful shutdown."""

from __future__ import annotations

import asyncio
import signal

import structlog

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
from datarift.riot_client import RiotRateLimiter

logger = structlog.get_logger(__name__)


async def run_extraction(
    config: ExtractionConfig,
    api_key: str,
    transport: object | None = None,
) -> None:
    """Execute the full Bronze extraction DAG with SIGINT-aware shutdown.

    Creates platform and regional HTTP clients from *config*, wires up
    BronzeWriter instances for each table, registers a SIGINT handler,
    and runs the six extraction stages in dependency order.  Any stage
    that finds ``shutdown_event`` set will flush its in-flight batch and
    return early, skipping downstream stages.

    Parameters
    ----------
    transport
        Optional ``httpx.AsyncBaseTransport`` injected for testing.
        Both platform and regional clients share the same transport.
    """
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _on_sigint() -> None:
        logger.info("sigint_received")
        shutdown_event.set()

    loop.add_signal_handler(signal.SIGINT, _on_sigint)

    logger.info("extraction_start", region=config.region, tiers=config.tiers)

    # --- writers (6 tables) ---
    league_writer = BronzeWriter("league_entries_raw", "puuid", config.bronze_path)
    accounts_writer = BronzeWriter("accounts_raw", "puuid", config.bronze_path)
    summoners_writer = BronzeWriter("summoners_raw", "puuid", config.bronze_path)
    match_ids_writer = BronzeWriter("match_ids_raw", "puuid", config.bronze_path)
    details_writer = BronzeWriter("match_details_raw", "match_id", config.bronze_path)
    timelines_writer = BronzeWriter("match_timelines_raw", "match_id", config.bronze_path)

    try:
        client_kwargs: dict = {}
        if transport is not None:
            client_kwargs["transport"] = transport

        async with (
            RiotRateLimiter(api_key=api_key, base_url=config.platform_host, **client_kwargs) as platform_client,
            RiotRateLimiter(api_key=api_key, base_url=config.regional_host, **client_kwargs) as regional_client,
        ):
            # Stage 1 — league entries (platform)
            puuids = await extract_league_entries(
                platform_client, config, league_writer, shutdown_event=shutdown_event,
            )

            if not shutdown_event.is_set():
                # Stage 2 — accounts (regional)
                await extract_accounts(
                    regional_client, puuids, config, accounts_writer,
                    shutdown_event=shutdown_event,
                )

            if not shutdown_event.is_set():
                # Stage 3 — summoners (platform)
                await extract_summoners(
                    platform_client, puuids, config, summoners_writer,
                    shutdown_event=shutdown_event,
                )

            if not shutdown_event.is_set():
                # Stage 4 — match IDs (regional)
                match_ids = await extract_match_ids(
                    regional_client, puuids, config, match_ids_writer,
                    shutdown_event=shutdown_event,
                )
            else:
                match_ids = []

            if not shutdown_event.is_set():
                # Stage 5 — match details (regional)
                await extract_match_details(
                    regional_client, match_ids, config, details_writer,
                    shutdown_event=shutdown_event,
                )

            if not shutdown_event.is_set():
                # Stage 6 — match timelines (regional)
                await extract_match_timelines(
                    regional_client, match_ids, config, timelines_writer,
                    shutdown_event=shutdown_event,
                )

        if shutdown_event.is_set():
            logger.info("extraction_interrupted", region=config.region)
        else:
            logger.info("extraction_complete", region=config.region)

    finally:
        loop.remove_signal_handler(signal.SIGINT)
