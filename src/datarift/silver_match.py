"""Silver-layer transforms for Match-V5 detail → flat relational tables, plus reusable Delta MERGE writer."""

from __future__ import annotations

import json
import re

import polars as pl
import structlog
from deltalake import DeltaTable, write_deltalake

log = structlog.get_logger()


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case, handling leading digits."""
    result = re.sub(r"([A-Z])", r"_\1", name).lower()
    # Clean up any leading underscore created if name started with uppercase
    if result.startswith("_") and not name.startswith("_"):
        result = result[1:]
    return result


def _json_str(path: str, alias: str) -> pl.Expr:
    """Extract a string field from raw_json via JSONPath."""
    return pl.col("raw_json").str.json_path_match(path).alias(alias)


def _json_int(path: str, alias: str) -> pl.Expr:
    """Extract an integer field from raw_json via JSONPath."""
    return pl.col("raw_json").str.json_path_match(path).cast(pl.Int64).alias(alias)


def transform_matches(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw_json and extract info-level scalar fields into a flat DataFrame.

    Pure function — no I/O, no side effects.
    """
    return df.select(
        _json_str("$.metadata.matchId", "match_id"),
        _json_str("$.metadata.dataVersion", "data_version"),
        _json_str("$.info.endOfGameResult", "end_of_game_result"),
        _json_int("$.info.gameCreation", "game_creation"),
        _json_int("$.info.gameDuration", "game_duration"),
        _json_int("$.info.gameEndTimestamp", "game_end_timestamp"),
        _json_int("$.info.gameId", "game_id"),
        _json_str("$.info.gameMode", "game_mode"),
        _json_str("$.info.gameName", "game_name"),
        _json_int("$.info.gameStartTimestamp", "game_start_timestamp"),
        _json_str("$.info.gameType", "game_type"),
        _json_str("$.info.gameVersion", "game_version"),
        _json_int("$.info.mapId", "map_id"),
        _json_str("$.info.platformId", "platform_id"),
        _json_int("$.info.queueId", "queue_id"),
        _json_str("$.info.tournamentCode", "tournament_code"),
    )


def _parse_teams(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw_json and explode info.teams[] into one row per team with match_id."""
    rows = []
    for raw in df["raw_json"].to_list():
        doc = json.loads(raw)
        match_id = doc["metadata"]["matchId"]
        for team in doc["info"]["teams"]:
            rows.append({"match_id": match_id, "_team": team})
    return pl.DataFrame(rows)


def transform_match_teams(df: pl.DataFrame) -> pl.DataFrame:
    """Extract info.teams[] into a flat match_teams table.

    Columns: match_id, team_id, win.
    """
    parsed = _parse_teams(df)
    return pl.DataFrame(
        {
            "match_id": [r["match_id"] for r in parsed.to_dicts()],
            "team_id": [r["_team"]["teamId"] for r in parsed.to_dicts()],
            "win": [r["_team"]["win"] for r in parsed.to_dicts()],
        }
    )


def transform_match_teams_bans(df: pl.DataFrame) -> pl.DataFrame:
    """Extract info.teams[].bans[] into a flat match_teams_bans table.

    Columns: match_id, team_id, champion_id, pick_turn.
    """
    parsed = _parse_teams(df)
    rows = []
    for r in parsed.to_dicts():
        match_id = r["match_id"]
        team_id = r["_team"]["teamId"]
        for ban in r["_team"]["bans"]:
            rows.append(
                {
                    "match_id": match_id,
                    "team_id": team_id,
                    "champion_id": ban["championId"],
                    "pick_turn": ban["pickTurn"],
                }
            )
    return pl.DataFrame(rows)


def transform_match_teams_objectives(df: pl.DataFrame) -> pl.DataFrame:
    """Extract info.teams[].objectives into unpivoted rows.

    Each objective type (baron, champion, dragon, etc.) becomes a row.
    Columns: match_id, team_id, objective_name, is_first, kills.
    """
    parsed = _parse_teams(df)
    rows = []
    for r in parsed.to_dicts():
        match_id = r["match_id"]
        team_id = r["_team"]["teamId"]
        objectives = r["_team"]["objectives"]
        for obj_name, obj_data in sorted(objectives.items()):
            rows.append(
                {
                    "match_id": match_id,
                    "team_id": team_id,
                    "objective_name": camel_to_snake(obj_name),
                    "is_first": obj_data["first"],
                    "kills": obj_data["kills"],
                }
            )
    return pl.DataFrame(rows)


# Top-level scalar field names from the participant object (camelCase, as in API).
# Excludes: challenges, perks, missions, PlayerBehavior (complex objects handled separately).
PARTICIPANT_SCALAR_FIELDS: list[str] = [
    "participantId",
    "teamId",
    "championId",
    "championName",
    "championTransform",
    "kills",
    "deaths",
    "assists",
    "win",
    "puuid",
    "riotIdGameName",
    "riotIdTagline",
    "summonerId",
    "summonerLevel",
    "summonerName",
    "profileIcon",
    "role",
    "lane",
    "individualPosition",
    "teamPosition",
    "eligibleForProgression",
    "champExperience",
    "champLevel",
    "goldEarned",
    "goldSpent",
    "item0",
    "item1",
    "item2",
    "item3",
    "item4",
    "item5",
    "item6",
    "itemsPurchased",
    "consumablesPurchased",
    "visionScore",
    "visionWardsBoughtInGame",
    "sightWardsBoughtInGame",
    "wardsKilled",
    "wardsPlaced",
    "detectorWardsPlaced",
    "totalMinionsKilled",
    "neutralMinionsKilled",
    "totalAllyJungleMinionsKilled",
    "totalEnemyJungleMinionsKilled",
    "killingSprees",
    "largestKillingSpree",
    "largestMultiKill",
    "largestCriticalStrike",
    "doubleKills",
    "tripleKills",
    "quadraKills",
    "pentaKills",
    "unrealKills",
    "longestTimeSpentLiving",
    "totalDamageDealt",
    "totalDamageDealtToChampions",
    "physicalDamageDealt",
    "physicalDamageDealtToChampions",
    "magicDamageDealt",
    "magicDamageDealtToChampions",
    "trueDamageDealt",
    "trueDamageDealtToChampions",
    "totalDamageTaken",
    "physicalDamageTaken",
    "magicDamageTaken",
    "trueDamageTaken",
    "damageSelfMitigated",
    "damageDealtToBuildings",
    "damageDealtToObjectives",
    "damageDealtToTurrets",
    "damageDealtToEpicMonsters",
    "totalDamageShieldedOnTeammates",
    "totalHeal",
    "totalHealsOnTeammates",
    "totalUnitsHealed",
    "totalTimeCCDealt",
    "timeCCingOthers",
    "timePlayed",
    "totalTimeSpentDead",
    "turretKills",
    "turretTakedowns",
    "turretsLost",
    "inhibitorKills",
    "inhibitorTakedowns",
    "inhibitorsLost",
    "nexusKills",
    "nexusTakedowns",
    "nexusLost",
    "objectivesStolen",
    "objectivesStolenAssists",
    "baronKills",
    "dragonKills",
    "firstBloodKill",
    "firstBloodAssist",
    "firstTowerKill",
    "firstTowerAssist",
    "gameEndedInEarlySurrender",
    "gameEndedInSurrender",
    "teamEarlySurrendered",
    "spell1Casts",
    "spell2Casts",
    "spell3Casts",
    "spell4Casts",
    "summoner1Casts",
    "summoner1Id",
    "summoner2Casts",
    "summoner2Id",
    "allInPings",
    "assistMePings",
    "basicPings",
    "commandPings",
    "dangerPings",
    "enemyMissingPings",
    "enemyVisionPings",
    "getBackPings",
    "holdPings",
    "needVisionPings",
    "onMyWayPings",
    "pushPings",
    "retreatPings",
    "visionClearedPings",
    "placement",
    "subteamPlacement",
    "playerSubteamId",
    "playerAugment1",
    "playerAugment2",
    "playerAugment3",
    "playerAugment4",
    "playerAugment5",
    "playerAugment6",
    "roleBoundItem",
    "PlayerScore0",
    "PlayerScore1",
    "PlayerScore2",
    "PlayerScore3",
    "PlayerScore4",
    "PlayerScore5",
    "PlayerScore6",
    "PlayerScore7",
    "PlayerScore8",
    "PlayerScore9",
    "PlayerScore10",
    "PlayerScore11",
]

# Complex object fields serialized to JSON strings.
_PARTICIPANT_JSON_FIELDS = {"perks", "missions", "PlayerBehavior"}


def transform_match_participants(df: pl.DataFrame) -> pl.DataFrame:
    """Extract info.participants[] into a flat match_participants table.

    - Scalar fields are selected by name and renamed to snake_case.
    - challenges sub-object is flattened with ``challenges_`` prefix (snake_case).
      Missing challenge fields become null; unknown top-level fields are dropped (D007).
    - perks, missions, PlayerBehavior are serialized as JSON string columns.

    Pure function — no I/O.
    """
    rows: list[dict] = []
    for raw in df["raw_json"].to_list():
        doc = json.loads(raw)
        match_id = doc["metadata"]["matchId"]
        for p in doc["info"]["participants"]:
            row: dict = {"match_id": match_id}

            # --- scalar fields (explicit select → unknown dropped) ---
            for field in PARTICIPANT_SCALAR_FIELDS:
                row[camel_to_snake(field)] = p.get(field)

            # --- challenges: flatten with prefix ---
            challenges = p.get("challenges")
            if challenges and isinstance(challenges, dict):
                for ckey, cval in challenges.items():
                    row[f"challenges_{camel_to_snake(ckey)}"] = cval
            # (missing challenges object → no challenges_ columns added for this row;
            #  they'll be filled with null when the DataFrame aligns columns across rows)

            # --- complex objects → JSON strings ---
            for jfield, col_name in [
                ("perks", "perks_json"),
                ("missions", "missions_json"),
                ("PlayerBehavior", "player_behavior_json"),
            ]:
                val = p.get(jfield)
                row[col_name] = json.dumps(val) if val is not None else None

            rows.append(row)

    return pl.DataFrame(rows) if rows else pl.DataFrame()


def write_silver(
    df: pl.DataFrame,
    table_path: str,
    predicate: str,
    merge_cols: dict | None = None,
) -> None:
    """Write a Polars DataFrame to a Delta table with MERGE-on-natural-key semantics.

    On first run (no existing table), writes in overwrite mode.
    On subsequent runs, performs a MERGE using the given SQL predicate
    (e.g. ``"s.match_id = t.match_id"``).
    """
    if df.is_empty() and len(df.columns) == 0:
        return

    # Cast Null-typed columns (all values None) to Utf8 so Delta Lake accepts them.
    null_cols = [col for col in df.columns if df[col].dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8).alias(c) for c in null_cols])

    arrow_table = df.to_arrow()

    if not DeltaTable.is_deltatable(table_path):
        write_deltalake(table_path, arrow_table, mode="overwrite")
        return

    dt = DeltaTable(table_path)
    (
        dt.merge(
            source=arrow_table,
            predicate=predicate,
            source_alias="s",
            target_alias="t",
        )
        .when_not_matched_insert_all()
        .execute()
    )


# ---------------------------------------------------------------------------
# Silver materialization orchestrator
# ---------------------------------------------------------------------------

_SILVER_TABLES: list[dict] = [
    {
        "name": "matches",
        "transform": transform_matches,
        "predicate": "s.match_id = t.match_id",
    },
    {
        "name": "match_participants",
        "transform": transform_match_participants,
        "predicate": "s.match_id = t.match_id AND s.participant_id = t.participant_id",
    },
    {
        "name": "match_teams",
        "transform": transform_match_teams,
        "predicate": "s.match_id = t.match_id AND s.team_id = t.team_id",
    },
    {
        "name": "match_teams_bans",
        "transform": transform_match_teams_bans,
        "predicate": "s.match_id = t.match_id AND s.team_id = t.team_id AND s.pick_turn = t.pick_turn",
    },
    {
        "name": "match_teams_objectives",
        "transform": transform_match_teams_objectives,
        "predicate": "s.match_id = t.match_id AND s.team_id = t.team_id AND s.objective_name = t.objective_name",
    },
]


def materialize_silver_matches(bronze_path: str, silver_path: str) -> dict[str, int]:
    """Orchestrate Silver materialization: read Bronze match_details_raw, transform, and MERGE into Silver tables.

    Returns a dict mapping table name → row count written.
    """
    log.info("silver_materialize_start", bronze_path=bronze_path, silver_path=silver_path)

    bronze_table_path = f"{bronze_path}/match_details_raw"
    dt = DeltaTable(bronze_table_path)
    raw_df = pl.DataFrame(dt.to_pyarrow_table())

    result: dict[str, int] = {}

    for spec in _SILVER_TABLES:
        table_name: str = spec["name"]
        transformed = spec["transform"](raw_df)
        row_count = len(transformed)

        table_path = f"{silver_path}/{table_name}"
        write_silver(transformed, table_path, spec["predicate"])

        log.info("silver_table_written", table=table_name, rows=row_count)
        result[table_name] = row_count

    log.info("silver_materialize_complete", tables=list(result.keys()), total_rows=sum(result.values()))
    return result
