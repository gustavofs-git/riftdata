"""Async Bronze extraction functions for puuid-keyed Riot API endpoints."""

from __future__ import annotations

import asyncio
import json

import structlog

from datarift.bronze_writer import BronzeWriter
from datarift.config import ExtractionConfig
from datarift.riot_client import RiotRateLimiter

logger = structlog.get_logger(__name__)


async def extract_league_entries(
    client: RiotRateLimiter,
    config: ExtractionConfig,
    writer: BronzeWriter,
    shutdown_event: asyncio.Event | None = None,
) -> list[str]:
    """Extract league entries for all configured tiers, returning all puuids.

    Paginates League-Exp-V4 per tier until an empty page is returned.
    Writes each page as a batch to the league_entries_raw Delta table.
    Skips puuids already present via anti-join.
    """
    existing = writer.existing_keys()
    all_puuids: list[str] = list(existing)

    logger.info(
        "stage_start",
        extractor="league_entries",
        endpoint="/lol/league-exp/v4/entries",
        tiers=config.tiers,
        existing=len(existing),
    )

    if existing:
        logger.info(
            "resume_skipped",
            extractor="league_entries",
            skipped=len(existing),
        )

    new_count = 0
    for tier in config.tiers:
        page = 1
        while True:
            if shutdown_event and shutdown_event.is_set():
                return all_puuids

            path = f"/lol/league-exp/v4/entries/{config.queue}/{tier}/I?page={page}"
            resp = await client.request("GET", path)
            entries = resp.json()

            if not entries:
                break

            # Filter out already-existing puuids for writing
            new_entries = [e for e in entries if e["puuid"] not in existing]
            page_puuids = [e["puuid"] for e in entries]
            all_puuids.extend(p for p in page_puuids if p not in existing)

            if new_entries:
                records = [
                    {"puuid": e["puuid"], "raw_json": json.dumps(e)}
                    for e in new_entries
                ]
                writer.write_batch(
                    records=records,
                    endpoint=path,
                    status_code=resp.status_code,
                    region=config.region,
                )
                new_count += len(records)
                logger.info(
                    "batch_written",
                    extractor="league_entries",
                    tier=tier,
                    page=page,
                    batch=len(records),
                    total_new=new_count,
                )

            page += 1

    logger.info(
        "stage_complete",
        extractor="league_entries",
        total_puuids=len(all_puuids),
        new=new_count,
    )
    return all_puuids


async def extract_accounts(
    client: RiotRateLimiter,
    puuids: list[str],
    config: ExtractionConfig,
    writer: BronzeWriter,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Extract account data for each puuid via Account-V1.

    Uses regional routing. Skips puuids already in accounts_raw via anti-join.
    Writes in batches of config.batch_size.
    """
    existing = writer.existing_keys()
    todo = [p for p in puuids if p not in existing]

    logger.info(
        "stage_start",
        extractor="accounts",
        endpoint="/riot/account/v1/accounts/by-puuid",
        total=len(puuids),
        new=len(todo),
        skipped=len(puuids) - len(todo),
    )

    processed = 0
    batch: list[dict] = []
    for puuid in todo:
        if shutdown_event and shutdown_event.is_set():
            if batch:
                writer.write_batch(
                    records=batch,
                    endpoint="/riot/account/v1/accounts/by-puuid",
                    status_code=200,
                    region=config.region,
                )
                processed += len(batch)
                logger.info("batch_written", extractor="accounts", batch=len(batch), progress=f"{processed}/{len(todo)}")
            return

        path = f"/riot/account/v1/accounts/by-puuid/{puuid}"
        resp = await client.request("GET", path)
        data = resp.json()
        batch.append({"puuid": puuid, "raw_json": json.dumps(data)})

        if len(batch) >= config.batch_size:
            writer.write_batch(
                records=batch,
                endpoint="/riot/account/v1/accounts/by-puuid",
                status_code=resp.status_code,
                region=config.region,
            )
            processed += len(batch)
            logger.info("batch_written", extractor="accounts", batch=len(batch), progress=f"{processed}/{len(todo)}")
            batch = []

    if batch:
        writer.write_batch(
            records=batch,
            endpoint="/riot/account/v1/accounts/by-puuid",
            status_code=200,
            region=config.region,
        )
        processed += len(batch)
        logger.info("batch_written", extractor="accounts", batch=len(batch), progress=f"{processed}/{len(todo)}")

    logger.info("stage_complete", extractor="accounts", processed=processed)


async def extract_summoners(
    client: RiotRateLimiter,
    puuids: list[str],
    config: ExtractionConfig,
    writer: BronzeWriter,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Extract summoner data for each puuid via Summoner-V4.

    Uses platform routing. Skips puuids already in summoners_raw via anti-join.
    Writes in batches of config.batch_size.
    """
    existing = writer.existing_keys()
    todo = [p for p in puuids if p not in existing]

    logger.info(
        "stage_start",
        extractor="summoners",
        endpoint="/lol/summoner/v4/summoners/by-puuid",
        total=len(puuids),
        new=len(todo),
        skipped=len(puuids) - len(todo),
    )

    processed = 0
    batch: list[dict] = []
    for puuid in todo:
        if shutdown_event and shutdown_event.is_set():
            if batch:
                writer.write_batch(
                    records=batch,
                    endpoint="/lol/summoner/v4/summoners/by-puuid",
                    status_code=200,
                    region=config.region,
                )
                processed += len(batch)
                logger.info("batch_written", extractor="summoners", batch=len(batch), progress=f"{processed}/{len(todo)}")
            return

        path = f"/lol/summoner/v4/summoners/by-puuid/{puuid}"
        resp = await client.request("GET", path)
        data = resp.json()
        batch.append({"puuid": puuid, "raw_json": json.dumps(data)})

        if len(batch) >= config.batch_size:
            writer.write_batch(
                records=batch,
                endpoint="/lol/summoner/v4/summoners/by-puuid",
                status_code=resp.status_code,
                region=config.region,
            )
            processed += len(batch)
            logger.info("batch_written", extractor="summoners", batch=len(batch), progress=f"{processed}/{len(todo)}")
            batch = []

    if batch:
        writer.write_batch(
            records=batch,
            endpoint="/lol/summoner/v4/summoners/by-puuid",
            status_code=200,
            region=config.region,
        )
        processed += len(batch)
        logger.info("batch_written", extractor="summoners", batch=len(batch), progress=f"{processed}/{len(todo)}")

    logger.info("stage_complete", extractor="summoners", processed=processed)


async def extract_match_ids(
    client: RiotRateLimiter,
    puuids: list[str],
    config: ExtractionConfig,
    writer: BronzeWriter,
    shutdown_event: asyncio.Event | None = None,
) -> list[str]:
    """Extract match ID lists for each puuid via Match-V5.

    Uses regional routing. Stores one row per puuid with the full match ID
    array as raw_json. Returns the flattened list of ALL match IDs across
    both new and existing puuids.
    """
    existing = writer.existing_keys()
    todo = [p for p in puuids if p not in existing]

    logger.info(
        "stage_start",
        extractor="match_ids",
        endpoint="/lol/match/v5/matches/by-puuid/{puuid}/ids",
        total=len(puuids),
        new=len(todo),
        skipped=len(puuids) - len(todo),
    )

    # Collect match IDs from existing records
    all_match_ids: list[str] = []
    if existing:
        import polars as pl
        from deltalake import DeltaTable

        try:
            dt = DeltaTable(writer.table_path)
            df = pl.DataFrame(dt.to_pyarrow_table(columns=["puuid", "raw_json"]))
            for row in df.iter_rows(named=True):
                all_match_ids.extend(json.loads(row["raw_json"]))
        except Exception:
            pass

    processed = 0
    batch: list[dict] = []
    for puuid in todo:
        if shutdown_event and shutdown_event.is_set():
            if batch:
                writer.write_batch(
                    records=batch,
                    endpoint="/lol/match/v5/matches/by-puuid",
                    status_code=200,
                    region=config.region,
                )
                processed += len(batch)
                logger.info("batch_written", extractor="match_ids", batch=len(batch), progress=f"{processed}/{len(todo)}")
                for rec in batch:
                    all_match_ids.extend(json.loads(rec["raw_json"]))
            return all_match_ids

        path = f"/lol/match/v5/matches/by-puuid/{puuid}/ids"
        resp = await client.request("GET", path)
        match_id_list = resp.json()  # JSON array of strings
        batch.append({"puuid": puuid, "raw_json": json.dumps(match_id_list)})
        all_match_ids.extend(match_id_list)

        if len(batch) >= config.batch_size:
            writer.write_batch(
                records=batch,
                endpoint="/lol/match/v5/matches/by-puuid",
                status_code=resp.status_code,
                region=config.region,
            )
            processed += len(batch)
            logger.info("batch_written", extractor="match_ids", batch=len(batch), progress=f"{processed}/{len(todo)}")
            batch = []

    if batch:
        writer.write_batch(
            records=batch,
            endpoint="/lol/match/v5/matches/by-puuid",
            status_code=200,
            region=config.region,
        )
        processed += len(batch)
        logger.info("batch_written", extractor="match_ids", batch=len(batch), progress=f"{processed}/{len(todo)}")

    logger.info(
        "stage_complete",
        extractor="match_ids",
        processed=processed,
        total_match_ids=len(all_match_ids),
    )
    return all_match_ids


async def extract_match_details(
    client: RiotRateLimiter,
    match_ids: list[str],
    config: ExtractionConfig,
    writer: BronzeWriter,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Extract match detail data for each match_id via Match-V5.

    Uses regional routing. Skips match_ids already in match_details_raw via anti-join.
    Writes in batches of config.batch_size.
    """
    existing = writer.existing_keys()
    todo = [m for m in match_ids if m not in existing]

    logger.info(
        "stage_start",
        extractor="match_details",
        endpoint="/lol/match/v5/matches/{matchId}",
        total=len(match_ids),
        new=len(todo),
        skipped=len(match_ids) - len(todo),
    )

    processed = 0
    batch: list[dict] = []
    for match_id in todo:
        if shutdown_event and shutdown_event.is_set():
            if batch:
                writer.write_batch(
                    records=batch,
                    endpoint="/lol/match/v5/matches",
                    status_code=200,
                    region=config.region,
                )
                processed += len(batch)
                logger.info("batch_written", extractor="match_details", batch=len(batch), progress=f"{processed}/{len(todo)}")
            return

        path = f"/lol/match/v5/matches/{match_id}"
        resp = await client.request("GET", path)
        data = resp.json()
        batch.append({"match_id": match_id, "raw_json": json.dumps(data)})

        if len(batch) >= config.batch_size:
            writer.write_batch(
                records=batch,
                endpoint="/lol/match/v5/matches",
                status_code=resp.status_code,
                region=config.region,
            )
            processed += len(batch)
            logger.info("batch_written", extractor="match_details", batch=len(batch), progress=f"{processed}/{len(todo)}")
            batch = []

    if batch:
        writer.write_batch(
            records=batch,
            endpoint="/lol/match/v5/matches",
            status_code=200,
            region=config.region,
        )
        processed += len(batch)
        logger.info("batch_written", extractor="match_details", batch=len(batch), progress=f"{processed}/{len(todo)}")

    logger.info(
        "stage_complete", extractor="match_details", processed=processed
    )


async def extract_match_timelines(
    client: RiotRateLimiter,
    match_ids: list[str],
    config: ExtractionConfig,
    writer: BronzeWriter,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Extract match timeline data for each match_id via Match-V5.

    Uses regional routing. Skips match_ids already in match_timelines_raw via anti-join.
    Writes in batches of config.batch_size.
    """
    existing = writer.existing_keys()
    todo = [m for m in match_ids if m not in existing]

    logger.info(
        "stage_start",
        extractor="match_timelines",
        endpoint="/lol/match/v5/matches/{matchId}/timeline",
        total=len(match_ids),
        new=len(todo),
        skipped=len(match_ids) - len(todo),
    )

    processed = 0
    batch: list[dict] = []
    for match_id in todo:
        if shutdown_event and shutdown_event.is_set():
            if batch:
                writer.write_batch(
                    records=batch,
                    endpoint="/lol/match/v5/matches/timeline",
                    status_code=200,
                    region=config.region,
                )
                processed += len(batch)
                logger.info("batch_written", extractor="match_timelines", batch=len(batch), progress=f"{processed}/{len(todo)}")
            return

        path = f"/lol/match/v5/matches/{match_id}/timeline"
        resp = await client.request("GET", path)
        data = resp.json()
        batch.append({"match_id": match_id, "raw_json": json.dumps(data)})

        if len(batch) >= config.batch_size:
            writer.write_batch(
                records=batch,
                endpoint="/lol/match/v5/matches/timeline",
                status_code=resp.status_code,
                region=config.region,
            )
            processed += len(batch)
            logger.info("batch_written", extractor="match_timelines", batch=len(batch), progress=f"{processed}/{len(todo)}")
            batch = []

    if batch:
        writer.write_batch(
            records=batch,
            endpoint="/lol/match/v5/matches/timeline",
            status_code=200,
            region=config.region,
        )
        processed += len(batch)
        logger.info("batch_written", extractor="match_timelines", batch=len(batch), progress=f"{processed}/{len(todo)}")

    logger.info(
        "stage_complete", extractor="match_timelines", processed=processed
    )
