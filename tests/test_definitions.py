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
    silver_accounts,
    silver_league_entries,
    silver_match_participants,
    silver_match_teams,
    silver_match_teams_bans,
    silver_match_teams_objectives,
    silver_match_timeline_events,
    silver_match_timeline_frames,
    silver_match_timeline_participant_frames,
    silver_matches,
    silver_summoners,
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
    "silver_match_participants",
    "silver_match_teams",
    "silver_match_teams_bans",
    "silver_match_teams_objectives",
    "silver_match_timeline_frames",
    "silver_match_timeline_participant_frames",
    "silver_match_timeline_events",
    "silver_league_entries",
    "silver_summoners",
    "silver_accounts",
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


def test_silver_match_detail_dependencies():
    """All match-detail Silver assets depend on bronze_match_details."""
    graph = defs.resolve_asset_graph()
    for name in (
        "silver_matches",
        "silver_match_participants",
        "silver_match_teams",
        "silver_match_teams_bans",
        "silver_match_teams_objectives",
    ):
        node = graph.get(AssetKey(name))
        parent_keys = {k.to_user_string() for k in node.parent_keys}
        assert "bronze_match_details" in parent_keys, f"{name} should depend on bronze_match_details"


def test_silver_timeline_dependencies():
    """All timeline Silver assets depend on bronze_match_timelines."""
    graph = defs.resolve_asset_graph()
    for name in (
        "silver_match_timeline_frames",
        "silver_match_timeline_participant_frames",
        "silver_match_timeline_events",
    ):
        node = graph.get(AssetKey(name))
        parent_keys = {k.to_user_string() for k in node.parent_keys}
        assert "bronze_match_timelines" in parent_keys, f"{name} should depend on bronze_match_timelines"


def test_silver_entity_dependencies():
    """Each entity Silver asset depends on its own Bronze asset."""
    graph = defs.resolve_asset_graph()

    cases = [
        ("silver_league_entries", "bronze_league_entries"),
        ("silver_summoners", "bronze_summoners"),
        ("silver_accounts", "bronze_accounts"),
    ]
    for silver_name, bronze_name in cases:
        node = graph.get(AssetKey(silver_name))
        parent_keys = {k.to_user_string() for k in node.parent_keys}
        assert bronze_name in parent_keys, f"{silver_name} should depend on {bronze_name}"


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


def _empty_bronze_arrow():
    """Return an empty Arrow table with raw_json typed as Utf8 (not Null)."""
    import polars as pl
    return pl.DataFrame({"raw_json": pl.Series([], dtype=pl.Utf8)}).to_arrow()


@patch("datarift.definitions.write_silver")
@patch("datarift.definitions.DeltaTable")
def test_silver_matches_metadata(mock_dt_cls, mock_write):
    mock_dt_cls.is_deltatable.return_value = True
    mock_dt = MagicMock()
    mock_dt.to_pyarrow_table.return_value = _empty_bronze_arrow()
    mock_dt_cls.return_value = mock_dt

    context = build_asset_context()
    result = silver_matches(context)

    assert "rows" in result.metadata
    assert "total_wall_time" in result.metadata


@patch("datarift.definitions.write_silver")
@patch("datarift.definitions.DeltaTable")
def test_silver_league_entries_metadata(mock_dt_cls, mock_write):
    mock_dt_cls.is_deltatable.return_value = True
    mock_dt = MagicMock()
    mock_dt.to_pyarrow_table.return_value = _empty_bronze_arrow()
    mock_dt_cls.return_value = mock_dt

    context = build_asset_context()
    result = silver_league_entries(context)

    assert "rows" in result.metadata
    assert "total_wall_time" in result.metadata


@patch("datarift.definitions.DeltaTable")
def test_silver_skips_missing_bronze(mock_dt_cls):
    mock_dt_cls.is_deltatable.return_value = False

    context = build_asset_context()
    result = silver_accounts(context)

    assert result.metadata["rows"] == 0
    assert result.metadata["skipped"] is True
