"""Unit tests for Gold matchup_detail, matchup_intervals, and matchup_aggregates transforms."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from datarift.gold_matchup import (
    transform_matchup_aggregates,
    transform_matchup_detail,
    transform_matchup_intervals,
)
from datarift.silver_league import transform_league_entries
from datarift.silver_match import transform_match_participants, transform_matches
from datarift.silver_timeline import (
    transform_match_timeline_frames,
    transform_match_timeline_participant_frames,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "match_details_10p.json"
TIMELINE_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "match_timelines.json"
LEAGUE_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "league_entries.json"


@pytest.fixture()
def silver_participants() -> pl.DataFrame:
    raw_json = json.dumps(json.loads(FIXTURE_PATH.read_text()))
    rows = [{"raw_json": json.dumps(m)} for m in json.loads(raw_json)]
    bronze_df = pl.DataFrame(rows)
    return transform_match_participants(bronze_df)


def test_matchup_detail_produces_correct_row_count(silver_participants: pl.DataFrame) -> None:
    result = transform_matchup_detail(silver_participants)
    assert len(result) == 10, f"Expected 10 rows (2 matches × 5 lanes), got {len(result)}"


def test_matchup_detail_columns(silver_participants: pl.DataFrame) -> None:
    result = transform_matchup_detail(silver_participants)
    expected_cols = {
        "match_id",
        "champion_a_id", "champion_a_name",
        "champion_b_id", "champion_b_name",
        "lane", "win_a",
        "kills_a", "kills_b",
        "deaths_a", "deaths_b",
        "assists_a", "assists_b",
        "gold_earned_a", "gold_earned_b",
        "total_damage_dealt_to_champions_a", "total_damage_dealt_to_champions_b",
        "total_minions_killed_a", "total_minions_killed_b",
        "vision_score_a", "vision_score_b",
    }
    assert expected_cols.issubset(set(result.columns)), (
        f"Missing columns: {expected_cols - set(result.columns)}"
    )


def test_matchup_detail_lane_values(silver_participants: pl.DataFrame) -> None:
    result = transform_matchup_detail(silver_participants)
    lanes = set(result["lane"].unique().to_list())
    assert lanes == {"TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"}


def test_matchup_detail_team_ordering(silver_participants: pl.DataFrame) -> None:
    result = transform_matchup_detail(silver_participants)
    match1 = result.filter(pl.col("match_id") == "BR1_GOLD001")
    top = match1.filter(pl.col("lane") == "TOP")
    assert len(top) == 1
    row = top.to_dicts()[0]
    assert row["champion_a_name"] == "Garen", "Champion A should be from team 100"
    assert row["champion_b_name"] == "Jax", "Champion B should be from team 200"


def test_matchup_detail_empty_position_filtered(silver_participants: pl.DataFrame) -> None:
    bad_row = silver_participants.head(1).with_columns(
        pl.lit("").alias("team_position"),
        pl.lit("FAKE_MATCH").alias("match_id"),
    )
    combined = pl.concat([silver_participants, bad_row])
    result = transform_matchup_detail(combined)
    assert "FAKE_MATCH" not in result["match_id"].to_list()
    assert len(result) == 10


def test_matchup_detail_null_position_filtered(silver_participants: pl.DataFrame) -> None:
    bad_row = silver_participants.head(1).with_columns(
        pl.lit(None).cast(pl.Utf8).alias("team_position"),
        pl.lit("NULL_MATCH").alias("match_id"),
    )
    combined = pl.concat([silver_participants, bad_row])
    result = transform_matchup_detail(combined)
    assert "NULL_MATCH" not in result["match_id"].to_list()


def test_matchup_detail_empty_input() -> None:
    empty_df = pl.DataFrame()
    result = transform_matchup_detail(empty_df)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Fixtures for matchup_intervals tests
# ---------------------------------------------------------------------------


def _build_bronze_df(fixture_path: Path) -> pl.DataFrame:
    raw = json.loads(fixture_path.read_text())
    return pl.DataFrame([{"raw_json": json.dumps(m)} for m in raw])


@pytest.fixture()
def interval_result(silver_participants: pl.DataFrame) -> pl.DataFrame:
    timeline_bronze = _build_bronze_df(TIMELINE_FIXTURE_PATH)
    timeline_frames = transform_match_timeline_frames(timeline_bronze)
    participant_frames = transform_match_timeline_participant_frames(timeline_bronze)
    matchup_detail = transform_matchup_detail(silver_participants)
    return transform_matchup_intervals(
        matchup_detail, silver_participants, timeline_frames, participant_frames,
    )


# ---------------------------------------------------------------------------
# Matchup intervals tests
# ---------------------------------------------------------------------------


def test_matchup_intervals_row_count(interval_result: pl.DataFrame) -> None:
    assert len(interval_result) == 40, (
        f"Expected 40 rows (2 matches × 5 lanes × 4 intervals), got {len(interval_result)}"
    )


def test_matchup_intervals_columns(interval_result: pl.DataFrame) -> None:
    expected = {
        "match_id", "champion_a_id", "champion_a_name",
        "champion_b_id", "champion_b_name", "lane", "interval_min",
        "total_gold_a", "total_gold_b", "xp_a", "xp_b",
        "level_a", "level_b", "minions_killed_a", "minions_killed_b",
        "jungle_minions_killed_a", "jungle_minions_killed_b",
        "current_gold_a", "current_gold_b",
    }
    assert expected == set(interval_result.columns), (
        f"Column mismatch: missing={expected - set(interval_result.columns)}, "
        f"extra={set(interval_result.columns) - expected}"
    )


def test_matchup_intervals_interval_values(interval_result: pl.DataFrame) -> None:
    values = set(interval_result["interval_min"].unique().to_list())
    assert values == {5, 10, 15, 20}


def test_matchup_intervals_stats_monotonic(interval_result: pl.DataFrame) -> None:
    row = interval_result.filter(
        (pl.col("match_id") == "BR1_GOLD001") & (pl.col("lane") == "TOP")
    ).sort("interval_min")
    golds = row["total_gold_a"].to_list()
    for i in range(1, len(golds)):
        assert golds[i] > golds[i - 1], (
            f"total_gold_a not monotonic at intervals {i-1}→{i}: {golds}"
        )


def test_matchup_intervals_team_ordering(interval_result: pl.DataFrame) -> None:
    row = interval_result.filter(
        (pl.col("match_id") == "BR1_GOLD001")
        & (pl.col("lane") == "TOP")
        & (pl.col("interval_min") == 5)
    ).to_dicts()[0]
    assert row["champion_a_name"] == "Garen"
    assert row["champion_b_name"] == "Jax"


def test_matchup_intervals_short_game_nulls() -> None:
    matchup_detail = pl.DataFrame({
        "match_id": ["SHORT1"] * 5,
        "champion_a_id": [1, 2, 3, 4, 5],
        "champion_a_name": ["A1", "A2", "A3", "A4", "A5"],
        "champion_b_id": [6, 7, 8, 9, 10],
        "champion_b_name": ["B1", "B2", "B3", "B4", "B5"],
        "lane": ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"],
    })
    participants = pl.DataFrame({
        "match_id": ["SHORT1"] * 10,
        "participant_id": list(range(1, 11)),
        "champion_id": list(range(1, 11)),
        "team_id": [100]*5 + [200]*5,
    })
    frames_per_min = 11  # 0-10 min only
    timeline_frames = pl.DataFrame({
        "match_id": ["SHORT1"] * frames_per_min,
        "frame_index": list(range(frames_per_min)),
        "timestamp": [i * 60000 for i in range(frames_per_min)],
    })
    pf_rows = []
    for fi in range(frames_per_min):
        for pid in range(1, 11):
            pf_rows.append({
                "match_id": "SHORT1",
                "frame_index": fi,
                "participant_id": pid,
                "total_gold": 500 + fi * 200,
                "xp": fi * 150,
                "level": min(1 + fi // 3, 18),
                "minions_killed": fi * 6,
                "jungle_minions_killed": fi * 2 if pid in [2, 7] else 0,
                "current_gold": int((500 + fi * 200) * 0.6),
            })
    participant_frames = pl.DataFrame(pf_rows)

    result = transform_matchup_intervals(
        matchup_detail, participants, timeline_frames, participant_frames,
    )
    assert len(result) == 20  # 5 lanes × 4 intervals
    at_15 = result.filter(pl.col("interval_min") == 15)
    assert at_15["total_gold_a"].is_null().all()
    at_20 = result.filter(pl.col("interval_min") == 20)
    assert at_20["total_gold_a"].is_null().all()
    at_5 = result.filter(pl.col("interval_min") == 5)
    assert at_5["total_gold_a"].is_not_null().all()


def test_matchup_intervals_empty_input() -> None:
    empty = pl.DataFrame()
    participants = pl.DataFrame()
    frames = pl.DataFrame()
    pf = pl.DataFrame()
    result = transform_matchup_intervals(empty, participants, frames, pf)
    assert len(result) == 0
    assert set(result.columns) == {
        "match_id", "champion_a_id", "champion_a_name",
        "champion_b_id", "champion_b_name", "lane", "interval_min",
        "total_gold_a", "total_gold_b", "xp_a", "xp_b",
        "level_a", "level_b", "minions_killed_a", "minions_killed_b",
        "jungle_minions_killed_a", "jungle_minions_killed_b",
        "current_gold_a", "current_gold_b",
    }


# ---------------------------------------------------------------------------
# Fixtures for matchup_aggregates tests
# ---------------------------------------------------------------------------


def _build_league_bronze() -> pl.DataFrame:
    raw = json.loads(LEAGUE_FIXTURE_PATH.read_text())
    return pl.DataFrame([{"raw_json": json.dumps(e)} for e in raw])


@pytest.fixture()
def aggregate_result(silver_participants: pl.DataFrame) -> pl.DataFrame:
    det_bronze = _build_bronze_df(FIXTURE_PATH)
    tl_bronze = _build_bronze_df(TIMELINE_FIXTURE_PATH)
    le_bronze = _build_league_bronze()

    matches = transform_matches(det_bronze)
    league_entries = transform_league_entries(le_bronze)
    timeline_frames = transform_match_timeline_frames(tl_bronze)
    participant_frames = transform_match_timeline_participant_frames(tl_bronze)

    matchup_detail = transform_matchup_detail(silver_participants)
    matchup_intervals = transform_matchup_intervals(
        matchup_detail, silver_participants, timeline_frames, participant_frames,
    )
    return transform_matchup_aggregates(
        matchup_detail, matchup_intervals, matches, silver_participants, league_entries,
    )


# ---------------------------------------------------------------------------
# Matchup aggregates tests
# ---------------------------------------------------------------------------


def test_aggregate_row_count(aggregate_result: pl.DataFrame) -> None:
    assert len(aggregate_result) == 40, (
        f"Expected 40 rows (10 unique matchups × 4 intervals), got {len(aggregate_result)}"
    )


def test_aggregate_columns(aggregate_result: pl.DataFrame) -> None:
    expected = {
        "champion_a_id", "champion_a_name",
        "champion_b_id", "champion_b_name",
        "lane", "interval_min", "patch", "tier",
        "win_rate_a", "sample_size",
    }
    assert expected.issubset(set(aggregate_result.columns)), (
        f"Missing columns: {expected - set(aggregate_result.columns)}"
    )
    avg_cols = [c for c in aggregate_result.columns if c.startswith("avg_")]
    assert len(avg_cols) > 0, "Expected avg_* stat columns"


def test_aggregate_patch_extraction(aggregate_result: pl.DataFrame) -> None:
    patches = aggregate_result["patch"].unique().to_list()
    assert patches == ["16.6"], f"Expected patch '16.6', got {patches}"


def test_aggregate_tier_includes_unknown(aggregate_result: pl.DataFrame) -> None:
    tiers = set(aggregate_result["tier"].unique().to_list())
    assert "UNKNOWN" in tiers, f"Expected UNKNOWN tier for participants not in league_entries, got {tiers}"


def test_aggregate_win_rate(aggregate_result: pl.DataFrame) -> None:
    garen_jax = aggregate_result.filter(
        (pl.col("champion_a_name") == "Garen")
        & (pl.col("champion_b_name") == "Jax")
        & (pl.col("interval_min") == 5)
    )
    assert len(garen_jax) == 1
    row = garen_jax.to_dicts()[0]
    assert row["win_rate_a"] == 1.0, "Garen won the only match vs Jax"
    assert row["sample_size"] == 1


def test_aggregate_min_sample_filter(silver_participants: pl.DataFrame) -> None:
    det_bronze = _build_bronze_df(FIXTURE_PATH)
    tl_bronze = _build_bronze_df(TIMELINE_FIXTURE_PATH)
    le_bronze = _build_league_bronze()

    matches = transform_matches(det_bronze)
    league_entries = transform_league_entries(le_bronze)
    timeline_frames = transform_match_timeline_frames(tl_bronze)
    participant_frames = transform_match_timeline_participant_frames(tl_bronze)

    matchup_detail = transform_matchup_detail(silver_participants)
    matchup_intervals = transform_matchup_intervals(
        matchup_detail, silver_participants, timeline_frames, participant_frames,
    )
    result = transform_matchup_aggregates(
        matchup_detail, matchup_intervals, matches, silver_participants, league_entries,
        min_sample_size=999,
    )
    assert len(result) == 0, "All matchups have sample_size=1, so min_sample_size=999 should filter everything"


def test_aggregate_missing_tier_fallback(aggregate_result: pl.DataFrame) -> None:
    tiers = set(aggregate_result["tier"].unique().to_list())
    assert "UNKNOWN" in tiers, (
        "Participants without league entries should produce UNKNOWN tier"
    )


def test_aggregate_empty_input() -> None:
    empty_detail = pl.DataFrame()
    empty_intervals = pl.DataFrame()
    matches = pl.DataFrame({"match_id": [], "game_version": []})
    participants = pl.DataFrame({"match_id": [], "puuid": []})
    league = pl.DataFrame({"puuid": [], "tier": []})
    result = transform_matchup_aggregates(
        empty_detail, empty_intervals, matches, participants, league,
    )
    assert len(result) == 0
