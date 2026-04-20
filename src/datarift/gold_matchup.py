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


_INTERVAL_STAT_COLS = [
    "total_gold",
    "xp",
    "level",
    "minions_killed",
    "jungle_minions_killed",
    "current_gold",
]

_INTERVAL_OUTPUT_SCHEMA = {
    "match_id": pl.Utf8,
    "champion_a_id": pl.Int64,
    "champion_a_name": pl.Utf8,
    "champion_b_id": pl.Int64,
    "champion_b_name": pl.Utf8,
    "lane": pl.Utf8,
    "interval_min": pl.Int64,
    "total_gold_a": pl.Int64,
    "total_gold_b": pl.Int64,
    "xp_a": pl.Int64,
    "xp_b": pl.Int64,
    "level_a": pl.Int64,
    "level_b": pl.Int64,
    "minions_killed_a": pl.Int64,
    "minions_killed_b": pl.Int64,
    "jungle_minions_killed_a": pl.Int64,
    "jungle_minions_killed_b": pl.Int64,
    "current_gold_a": pl.Int64,
    "current_gold_b": pl.Int64,
}


def transform_matchup_intervals(
    matchup_detail: pl.DataFrame,
    participants: pl.DataFrame,
    timeline_frames: pl.DataFrame,
    participant_frames: pl.DataFrame,
) -> pl.DataFrame:
    """Produce per-interval stat snapshots for each matchup row.

    Joins Silver timeline participant frames back to matchup_detail via
    match_participants participant_id→champion_id bridge.

    Returns one row per (match_id, lane, interval_min) with stats for both sides.
    """
    empty = pl.DataFrame(schema=_INTERVAL_OUTPUT_SCHEMA)
    if any(
        df.is_empty()
        for df in [matchup_detail, participants, timeline_frames, participant_frames]
    ):
        log.info("matchup_intervals_produced", rows=0, missing_intervals=0)
        return empty

    target_intervals = pl.DataFrame({"interval_min": [5, 10, 15, 20]})

    matchup_base = matchup_detail.select(
        "match_id", "champion_a_id", "champion_a_name",
        "champion_b_id", "champion_b_name", "lane",
    )

    skeleton = matchup_base.join(target_intervals, how="cross").with_columns(
        (pl.col("interval_min") * 60000).alias("target_ms"),
    )

    champ_map = participants.select("match_id", "participant_id", "champion_id").unique()

    tpf = timeline_frames.join(
        participant_frames.select(
            "match_id", "frame_index", "participant_id", *_INTERVAL_STAT_COLS,
        ),
        on=["match_id", "frame_index"],
        how="inner",
    ).filter(pl.col("timestamp") > 0)

    max_dist_ms = 120_000  # 2 minutes; beyond this the game likely ended
    nearest_frames = (
        tpf.join(target_intervals, how="cross")
        .with_columns(
            (pl.col("interval_min") * 60000).alias("target_ms"),
            (pl.col("timestamp") - pl.col("interval_min") * 60000).abs().alias("dist"),
        )
        .filter(pl.col("dist") <= max_dist_ms)
        .sort("dist")
        .group_by("match_id", "participant_id", "interval_min")
        .first()
    )

    nearest_with_champ = nearest_frames.join(
        champ_map,
        on=["match_id", "participant_id"],
        how="left",
    )

    stats_a = nearest_with_champ.select(
        "match_id",
        pl.col("champion_id").alias("champion_a_id"),
        "interval_min",
        *[pl.col(c).alias(f"{c}_a") for c in _INTERVAL_STAT_COLS],
    )

    stats_b = nearest_with_champ.select(
        "match_id",
        pl.col("champion_id").alias("champion_b_id"),
        "interval_min",
        *[pl.col(c).alias(f"{c}_b") for c in _INTERVAL_STAT_COLS],
    )

    result = skeleton.join(
        stats_a,
        on=["match_id", "champion_a_id", "interval_min"],
        how="left",
    ).join(
        stats_b,
        on=["match_id", "champion_b_id", "interval_min"],
        how="left",
    )

    output_cols = list(_INTERVAL_OUTPUT_SCHEMA.keys())
    result = result.select(output_cols)

    missing_intervals = result.select(
        pl.col("total_gold_a").is_null().sum()
    ).item()

    log.info(
        "matchup_intervals_produced",
        rows=len(result),
        missing_intervals=missing_intervals,
    )
    if missing_intervals > 0:
        log.warning(
            "matchup_intervals_missing_frames",
            missing_count=missing_intervals,
        )

    return result


def transform_matchup_aggregates(
    matchup_detail: pl.DataFrame,
    matchup_intervals: pl.DataFrame,
    matches: pl.DataFrame,
    match_participants: pl.DataFrame,
    league_entries: pl.DataFrame,
    *,
    min_sample_size: int = 1,
) -> pl.DataFrame:
    """Aggregate matchup stats per (champion pair, lane, interval, patch, tier).

    Joins Gold matchup data with Silver metadata to produce averaged statistics
    grouped by champion matchup, lane, time interval, game patch, and rank tier.
    """
    if matchup_detail.is_empty():
        return pl.DataFrame()

    patch_df = matches.select(
        "match_id",
        pl.col("game_version")
        .str.split(".")
        .list.slice(0, 2)
        .list.join(".")
        .alias("patch"),
    )

    participants_with_tier = match_participants.select("match_id", "puuid").join(
        league_entries.select("puuid", "tier"),
        on="puuid",
        how="left",
    ).with_columns(pl.col("tier").fill_null("UNKNOWN"))

    modal_tier = (
        participants_with_tier.group_by("match_id", "tier")
        .agg(pl.len().alias("cnt"))
        .sort("cnt", descending=True)
        .group_by("match_id")
        .first()
        .select("match_id", "tier")
    )

    detail_with_meta = matchup_detail.join(patch_df, on="match_id", how="left").join(
        modal_tier, on="match_id", how="left"
    ).with_columns(
        pl.col("patch").fill_null("UNKNOWN"),
        pl.col("tier").fill_null("UNKNOWN"),
    )

    detail_stat_cols_a = [f"{s}_a" for s in _STAT_COLS]
    detail_stat_cols_b = [f"{s}_b" for s in _STAT_COLS]

    interval_stat_cols_a = [f"{s}_a" for s in _INTERVAL_STAT_COLS]
    interval_stat_cols_b = [f"{s}_b" for s in _INTERVAL_STAT_COLS]

    if matchup_intervals.is_empty():
        combined = detail_with_meta.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("interval_min"),
            *[pl.lit(None, dtype=pl.Float64).alias(c) for c in interval_stat_cols_a + interval_stat_cols_b],
        )
    else:
        intervals_with_meta = matchup_intervals.join(
            patch_df, on="match_id", how="left"
        ).join(modal_tier, on="match_id", how="left").with_columns(
            pl.col("patch").fill_null("UNKNOWN"),
            pl.col("tier").fill_null("UNKNOWN"),
        )

        detail_interval_cols = [
            "match_id", "champion_a_id", "champion_a_name",
            "champion_b_id", "champion_b_name", "lane",
            "interval_min", "patch", "tier",
        ] + interval_stat_cols_a + interval_stat_cols_b

        interval_select = intervals_with_meta.select(detail_interval_cols)

        combined = detail_with_meta.join(
            interval_select,
            on=["match_id", "champion_a_id", "champion_b_id", "lane", "patch", "tier"],
            how="left",
        )

    group_keys = [
        "champion_a_id", "champion_a_name",
        "champion_b_id", "champion_b_name",
        "lane", "interval_min", "patch", "tier",
    ]

    all_stat_cols = detail_stat_cols_a + detail_stat_cols_b + interval_stat_cols_a + interval_stat_cols_b
    present_stat_cols = [c for c in all_stat_cols if c in combined.columns]

    agg_exprs = [
        pl.col(c).cast(pl.Float64).mean().alias(f"avg_{c}") for c in present_stat_cols
    ] + [
        pl.col("win_a").cast(pl.Float64).mean().alias("win_rate_a"),
        pl.len().alias("sample_size"),
    ]

    result = combined.group_by(group_keys).agg(agg_exprs)

    result = result.filter(pl.col("sample_size") >= min_sample_size)

    log.info(
        "matchup_aggregates_produced",
        rows=len(result),
        groups=len(result.columns),
    )

    return result
