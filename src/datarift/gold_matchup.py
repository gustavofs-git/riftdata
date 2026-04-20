"""Gold-layer transforms for champion-vs-champion matchup statistics."""

from __future__ import annotations

import polars as pl
import structlog
from deltalake import write_deltalake

log = structlog.get_logger()

_STAT_COLS = [
    "kills",
    "deaths",
    "assists",
    "gold_earned",
    "total_damage_dealt_to_champions",
    "total_minions_killed",
    "vision_score",
]


def write_gold(df: pl.DataFrame, table_path: str) -> None:
    """Write a Polars DataFrame to a Delta table in overwrite mode (full recompute)."""
    if df.is_empty() and len(df.columns) == 0:
        return

    null_cols = [col for col in df.columns if df[col].dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8).alias(c) for c in null_cols])

    write_deltalake(table_path, df.to_arrow(), mode="overwrite")


def transform_matchup_detail(participants_df: pl.DataFrame) -> pl.DataFrame:
    """Produce champion-vs-champion matchup rows by self-joining participants on opposite teams, same lane.

    Expects a Silver match_participants DataFrame with columns:
    match_id, team_id, team_position, champion_id, champion_name, win,
    kills, deaths, assists, gold_earned, total_damage_dealt_to_champions,
    total_minions_killed, vision_score.

    Returns one row per (match_id, lane) with stats for both sides.
    Champion A is from team_id 100, champion B from team_id 200.
    """
    if participants_df.is_empty() and len(participants_df.columns) == 0:
        return pl.DataFrame()

    total_before = len(participants_df)
    filtered = participants_df.filter(
        pl.col("team_position").is_not_null()
        & (pl.col("team_position") != "")
    )
    filtered_count = total_before - len(filtered)
    if filtered_count > 0:
        log.warning(
            "matchup_empty_team_position_filtered",
            rows_filtered=filtered_count,
            rows_remaining=len(filtered),
        )

    team_a = filtered.filter(pl.col("team_id") == 100)
    team_b = filtered.filter(pl.col("team_id") == 200)

    a_rename = {col: f"{col}_a" for col in _STAT_COLS}
    a_rename["champion_id"] = "champion_a_id"
    a_rename["champion_name"] = "champion_a_name"
    a_rename["win"] = "win_a"

    b_rename = {col: f"{col}_b" for col in _STAT_COLS}
    b_rename["champion_id"] = "champion_b_id"
    b_rename["champion_name"] = "champion_b_name"

    select_cols_a = ["match_id", "team_position", "champion_id", "champion_name", "win"] + _STAT_COLS
    select_cols_b = ["match_id", "team_position", "champion_id", "champion_name"] + _STAT_COLS

    team_a_sel = team_a.select(select_cols_a).rename(a_rename)
    team_b_sel = team_b.select(select_cols_b).rename(b_rename)

    joined = team_a_sel.join(
        team_b_sel,
        on=["match_id", "team_position"],
        how="inner",
    )

    result = joined.rename({"team_position": "lane"})

    col_order = [
        "match_id",
        "champion_a_id", "champion_a_name",
        "champion_b_id", "champion_b_name",
        "lane", "win_a",
    ]
    for stat in _STAT_COLS:
        col_order.append(f"{stat}_a")
        col_order.append(f"{stat}_b")

    result = result.select(col_order)

    log.info(
        "matchup_detail_produced",
        rows=len(result),
        matches=result["match_id"].n_unique() if len(result) > 0 else 0,
    )

    return result
