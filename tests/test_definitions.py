"""Tests for Dagster Definitions entrypoint: asset loading, names, deps, metadata."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from dagster import AssetKey, build_asset_context

from datarift.definitions import (
    bronze_extraction,
    defs,
    silver_league,
    silver_matches,
    silver_timelines,
)


# ---------------------------------------------------------------------------
# Definitions object tests
# ---------------------------------------------------------------------------


def test_definitions_loads_all_assets():
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert keys == {"bronze_extraction", "silver_matches", "silver_timelines", "silver_league"}


def test_silver_assets_depend_on_bronze():
    graph = defs.resolve_asset_graph()
    for name in ("silver_matches", "silver_timelines", "silver_league"):
        node = graph.get(AssetKey(name))
        parent_keys = {k.to_user_string() for k in node.parent_keys}
        assert "bronze_extraction" in parent_keys, f"{name} should depend on bronze_extraction"


# ---------------------------------------------------------------------------
# Bronze asset metadata test
# ---------------------------------------------------------------------------


@patch("datarift.definitions.EnvVar")
@patch("datarift.definitions.run_extraction", new_callable=AsyncMock)
def test_bronze_extraction_metadata(mock_run, mock_envvar, tmp_path):
    mock_envvar.return_value.get_value.return_value = "RGAPI-fake"
    mock_run.return_value = None

    # Create some fake bronze table dirs so succeeded count > 0
    for table in ("league_entries_raw", "accounts_raw", "summoners_raw"):
        (tmp_path / table).mkdir()

    with patch("datarift.definitions.ExtractionConfig") as mock_cfg_cls:
        mock_cfg = mock_cfg_cls.return_value
        mock_cfg.bronze_path = str(tmp_path)
        mock_cfg.region = "br"
        mock_cfg.tiers = ["CHALLENGER"]

        context = build_asset_context()
        result = bronze_extraction(context)

    assert result.metadata["succeeded"] == 3
    assert result.metadata["failed"] == 0
    assert "rate_limit_hits" in result.metadata
    assert "total_wall_time" in result.metadata


# ---------------------------------------------------------------------------
# Silver asset metadata tests
# ---------------------------------------------------------------------------


@patch("datarift.definitions.materialize_silver_matches")
def test_silver_matches_metadata(mock_mat):
    mock_mat.return_value = {"match_details": 100, "match_events": 500}

    context = build_asset_context()
    result = silver_matches(context)

    assert result.metadata["match_details"] == 100
    assert result.metadata["match_events"] == 500
    assert "total_wall_time" in result.metadata


@patch("datarift.definitions.materialize_silver_timelines")
def test_silver_timelines_metadata(mock_mat):
    mock_mat.return_value = {"participant_frames": 200}

    context = build_asset_context()
    result = silver_timelines(context)

    assert result.metadata["participant_frames"] == 200
    assert "total_wall_time" in result.metadata


@patch("datarift.definitions.materialize_silver_league")
def test_silver_league_metadata(mock_mat):
    mock_mat.return_value = {"league_entries": 50, "summoners": 30, "accounts": 30}

    context = build_asset_context()
    result = silver_league(context)

    assert result.metadata["league_entries"] == 50
    assert "total_wall_time" in result.metadata
