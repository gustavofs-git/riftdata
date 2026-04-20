"""Tests for Dagster Definitions entrypoint: asset loading, names, deps, metadata."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from dagster import AssetKey, build_asset_context

from datarift.definitions import (
    bronze_league_entries,
    bronze_accounts,
    bronze_summoners,
    bronze_match_ids,
    bronze_match_details,
    bronze_match_timelines,
    defs,
    silver_league,
    silver_matches,
    silver_timelines,
)


# ---------------------------------------------------------------------------
# Asset graph structure tests
# ---------------------------------------------------------------------------

_BRONZE_ASSETS = {
    "bronze_league_entries",
    "bronze_accounts",
    "bronze_summoners",
    "bronze_match_ids",
    "bronze_match_details",
    "bronze_match_timelines",
}

_SILVER_ASSETS = {
    "silver_matches",
    "silver_timelines",
    "silver_league",
}


def test_definitions_loads_all_assets():
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert keys == _BRONZE_ASSETS | _SILVER_ASSETS


def test_bronze_dependency_chain():
    """Accounts, summoners, and match_ids depend on league_entries.
    Match details and timelines depend on match_ids."""
    graph = defs.resolve_asset_graph()

    for name in ("bronze_accounts", "bronze_summoners", "bronze_match_ids"):
        node = graph.get(AssetKey(name))
        parent_keys = {k.to_user_string() for k in node.parent_keys}
        assert "bronze_league_entries" in parent_keys, f"{name} should depend on bronze_league_entries"

    for name in ("bronze_match_details", "bronze_match_timelines"):
        node = graph.get(AssetKey(name))
        parent_keys = {k.to_user_string() for k in node.parent_keys}
        assert "bronze_match_ids" in parent_keys, f"{name} should depend on bronze_match_ids"


def test_silver_dependencies():
    """Silver assets depend on the correct Bronze assets."""
    graph = defs.resolve_asset_graph()

    node = graph.get(AssetKey("silver_matches"))
    assert "bronze_match_details" in {k.to_user_string() for k in node.parent_keys}

    node = graph.get(AssetKey("silver_timelines"))
    assert "bronze_match_timelines" in {k.to_user_string() for k in node.parent_keys}

    node = graph.get(AssetKey("silver_league"))
    parent_keys = {k.to_user_string() for k in node.parent_keys}
    assert "bronze_league_entries" in parent_keys
    assert "bronze_summoners" in parent_keys
    assert "bronze_accounts" in parent_keys


# ---------------------------------------------------------------------------
# Bronze asset metadata tests
# ---------------------------------------------------------------------------


@patch("datarift.definitions.EnvVar")
@patch("datarift.definitions.extract_league_entries", new_callable=AsyncMock)
@patch("datarift.definitions._load_config")
def test_bronze_league_entries_metadata(mock_config, mock_extract, mock_envvar):
    mock_envvar.return_value.get_value.return_value = "RGAPI-fake"
    mock_extract.return_value = ["puuid_a", "puuid_b"]

    cfg = MagicMock()
    cfg.platform_host = "https://br1.api.riotgames.com"
    cfg.bronze_path = "/tmp/test_bronze"
    mock_config.return_value = cfg

    context = build_asset_context()
    result = bronze_league_entries(context)

    assert result.metadata["puuids"] == 2
    assert "total_wall_time" in result.metadata


@patch("datarift.definitions.EnvVar")
@patch("datarift.definitions.extract_accounts", new_callable=AsyncMock)
@patch("datarift.definitions._read_puuids_from_bronze")
@patch("datarift.definitions._load_config")
def test_bronze_accounts_metadata(mock_config, mock_read_puuids, mock_extract, mock_envvar):
    mock_envvar.return_value.get_value.return_value = "RGAPI-fake"
    mock_read_puuids.return_value = ["puuid_a", "puuid_b"]
    mock_extract.return_value = None

    cfg = MagicMock()
    cfg.regional_host = "https://americas.api.riotgames.com"
    cfg.bronze_path = "/tmp/test_bronze"
    mock_config.return_value = cfg

    context = build_asset_context()
    result = bronze_accounts(context)

    assert result.metadata["puuids"] == 2
    assert "total_wall_time" in result.metadata


@patch("datarift.definitions.EnvVar")
@patch("datarift.definitions.extract_match_ids", new_callable=AsyncMock)
@patch("datarift.definitions._read_puuids_from_bronze")
@patch("datarift.definitions._load_config")
def test_bronze_match_ids_metadata(mock_config, mock_read_puuids, mock_extract, mock_envvar):
    mock_envvar.return_value.get_value.return_value = "RGAPI-fake"
    mock_read_puuids.return_value = ["puuid_a"]
    mock_extract.return_value = ["KR_m1", "KR_m2"]

    cfg = MagicMock()
    cfg.regional_host = "https://americas.api.riotgames.com"
    cfg.bronze_path = "/tmp/test_bronze"
    mock_config.return_value = cfg

    context = build_asset_context()
    result = bronze_match_ids(context)

    assert result.metadata["puuids"] == 1
    assert result.metadata["match_ids"] == 2
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
