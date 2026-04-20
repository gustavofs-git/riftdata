"""Unit tests for Gold matchup_detail transform."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from datarift.gold_matchup import transform_matchup_detail
from datarift.silver_match import transform_match_participants

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "match_details_10p.json"


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
