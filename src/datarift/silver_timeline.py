"""Silver-layer transforms for Match-V5 Timeline → flat relational tables."""

from __future__ import annotations

import json

import polars as pl
import structlog

from deltalake import DeltaTable

from datarift.silver_match import camel_to_snake, write_silver

log = structlog.get_logger()


def transform_match_timeline_frames(df: pl.DataFrame) -> pl.DataFrame:
    """Extract timeline frames into a flat table with one row per frame.

    Columns: match_id (Utf8), frame_index (Int64), timestamp (Int64).

    Pure function — no I/O.
    """
    rows: list[dict] = []
    for raw in df["raw_json"].to_list():
        doc = json.loads(raw)
        match_id = doc["metadata"]["matchId"]
        for frame_index, frame in enumerate(doc["info"]["frames"]):
            rows.append(
                {
                    "match_id": match_id,
                    "frame_index": frame_index,
                    "timestamp": frame["timestamp"],
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={"match_id": pl.Utf8, "frame_index": pl.Int64, "timestamp": pl.Int64}
        )

    return pl.DataFrame(rows).cast({"frame_index": pl.Int64, "timestamp": pl.Int64})


def transform_match_timeline_participant_frames(df: pl.DataFrame) -> pl.DataFrame:
    """Extract participant frames into a flat table with one row per match × frame × participant.

    Flattens championStats (25 fields, ``champion_stats_`` prefix) and
    damageStats (12 fields, ``damage_stats_`` prefix) via ``camel_to_snake()``.

    Pure function — no I/O.
    """
    rows: list[dict] = []
    for raw in df["raw_json"].to_list():
        doc = json.loads(raw)
        match_id = doc["metadata"]["matchId"]
        for frame_index, frame in enumerate(doc["info"]["frames"]):
            participant_frames = frame.get("participantFrames") or {}
            for key, pf in participant_frames.items():
                row: dict = {
                    "match_id": match_id,
                    "frame_index": frame_index,
                    "participant_id": int(key),
                    "current_gold": pf["currentGold"],
                    "gold_per_second": pf["goldPerSecond"],
                    "jungle_minions_killed": pf["jungleMinionsKilled"],
                    "level": pf["level"],
                    "minions_killed": pf["minionsKilled"],
                    "position_x": pf["position"]["x"],
                    "position_y": pf["position"]["y"],
                    "time_enemy_spent_controlled": pf["timeEnemySpentControlled"],
                    "total_gold": pf["totalGold"],
                    "xp": pf["xp"],
                }
                # Flatten championStats with prefix
                for cs_key, cs_val in pf.get("championStats", {}).items():
                    row[f"champion_stats_{camel_to_snake(cs_key)}"] = cs_val
                # Flatten damageStats with prefix
                for ds_key, ds_val in pf.get("damageStats", {}).items():
                    row[f"damage_stats_{camel_to_snake(ds_key)}"] = ds_val
                rows.append(row)

    if not rows:
        return pl.DataFrame(
            schema={
                "match_id": pl.Utf8,
                "frame_index": pl.Int64,
                "participant_id": pl.Int64,
            }
        )

    return pl.DataFrame(rows).cast(
        {"frame_index": pl.Int64, "participant_id": pl.Int64}
    )


def transform_match_timeline_events(df: pl.DataFrame) -> pl.DataFrame:
    """Extract timeline events into a flat table with one row per event.

    Common typed columns are extracted; the full event object is stored as
    ``event_json`` (Utf8) to handle variable event schemas without drift.

    Columns: match_id (Utf8), frame_index (Int64), event_index (Int64),
    timestamp (Int64), real_timestamp (Int64, nullable), type (Utf8),
    participant_id (Int64, nullable), event_json (Utf8).

    Pure function — no I/O.
    """
    rows: list[dict] = []
    for raw in df["raw_json"].to_list():
        doc = json.loads(raw)
        match_id = doc["metadata"]["matchId"]
        for frame_index, frame in enumerate(doc["info"]["frames"]):
            for event_index, event in enumerate(frame.get("events", [])):
                rows.append(
                    {
                        "match_id": match_id,
                        "frame_index": frame_index,
                        "event_index": event_index,
                        "timestamp": event["timestamp"],
                        "real_timestamp": event.get("realTimestamp"),
                        "type": event["type"],
                        "participant_id": event.get("participantId"),
                        "event_json": json.dumps(event),
                    }
                )

    if not rows:
        return pl.DataFrame(
            schema={
                "match_id": pl.Utf8,
                "frame_index": pl.Int64,
                "event_index": pl.Int64,
                "timestamp": pl.Int64,
                "real_timestamp": pl.Int64,
                "type": pl.Utf8,
                "participant_id": pl.Int64,
                "event_json": pl.Utf8,
            }
        )

    return pl.DataFrame(rows).cast(
        {
            "frame_index": pl.Int64,
            "event_index": pl.Int64,
            "timestamp": pl.Int64,
            "real_timestamp": pl.Int64,
            "participant_id": pl.Int64,
        }
    )


# ---------------------------------------------------------------------------
# Silver timeline materialization orchestrator
# ---------------------------------------------------------------------------

_SILVER_TIMELINE_TABLES: list[dict] = [
    {
        "name": "match_timeline_frames",
        "transform": transform_match_timeline_frames,
        "predicate": "s.match_id = t.match_id AND s.frame_index = t.frame_index",
    },
    {
        "name": "match_timeline_participant_frames",
        "transform": transform_match_timeline_participant_frames,
        "predicate": "s.match_id = t.match_id AND s.frame_index = t.frame_index AND s.participant_id = t.participant_id",
    },
    {
        "name": "match_timeline_events",
        "transform": transform_match_timeline_events,
        "predicate": "s.match_id = t.match_id AND s.frame_index = t.frame_index AND s.event_index = t.event_index",
    },
]


def materialize_silver_timelines(bronze_path: str, silver_path: str) -> dict[str, int]:
    """Orchestrate Silver timeline materialization: read Bronze match_timelines_raw, transform, and MERGE into Silver tables.

    Returns a dict mapping table name → row count written.
    """
    log.info("silver_timeline_materialize_start", bronze_path=bronze_path, silver_path=silver_path)

    bronze_table_path = f"{bronze_path}/match_timelines_raw"
    dt = DeltaTable(bronze_table_path)
    raw_df = pl.DataFrame(dt.to_pyarrow_table())

    result: dict[str, int] = {}

    for spec in _SILVER_TIMELINE_TABLES:
        table_name: str = spec["name"]
        transformed = spec["transform"](raw_df)
        row_count = len(transformed)

        table_path = f"{silver_path}/{table_name}"
        write_silver(transformed, table_path, spec["predicate"])

        log.info("silver_timeline_table_written", table=table_name, rows=row_count)
        result[table_name] = row_count

    log.info("silver_timeline_materialize_complete", tables=list(result.keys()), total_rows=sum(result.values()))
    return result
