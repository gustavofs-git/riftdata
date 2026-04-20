"""Tests for Silver match transforms and Delta MERGE writer."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

from datarift.bronze_writer import BronzeWriter
from datarift.silver_match import (
    PARTICIPANT_SCALAR_FIELDS,
    camel_to_snake,
    materialize_silver_matches,
    transform_match_participants,
    transform_match_teams,
    transform_match_teams_bans,
    transform_match_teams_objectives,
    transform_matches,
    write_silver,
)


def _make_match_json(
    match_id: str = "BR1_3219665682",
    game_duration: int = 1568,
    platform_id: str = "BR1",
) -> str:
    """Return a minimal but structurally complete Match-V5 JSON string."""
    return json.dumps(
        {
            "metadata": {
                "dataVersion": "2",
                "matchId": match_id,
                "participants": ["puuid1", "puuid2"],
            },
            "info": {
                "endOfGameResult": "GameComplete",
                "gameCreation": 1773941590152,
                "gameDuration": game_duration,
                "gameEndTimestamp": 1773943191612,
                "gameId": 3219665682,
                "gameMode": "CLASSIC",
                "gameName": f"teambuilder-match-{match_id}",
                "gameStartTimestamp": 1773941623911,
                "gameType": "MATCHED_GAME",
                "gameVersion": "16.6.753.8272",
                "mapId": 11,
                "platformId": platform_id,
                "queueId": 420,
                "tournamentCode": "",
                "participants": [
                    {
                        "participantId": 1,
                        "teamId": 100,
                        "championId": 79,
                        "championName": "Gragas",
                        "championTransform": 0,
                        "kills": 1,
                        "deaths": 5,
                        "assists": 1,
                        "win": False,
                        "puuid": "puuid1",
                        "riotIdGameName": "DustyElementalis",
                        "riotIdTagline": "8269",
                        "summonerId": "sum1",
                        "summonerLevel": 44,
                        "summonerName": "",
                        "profileIcon": 7035,
                        "role": "NONE",
                        "lane": "JUNGLE",
                        "individualPosition": "TOP",
                        "teamPosition": "TOP",
                        "challenges": {
                            "12AssistStreakCount": 0,
                            "HealFromMapSources": 0,
                            "InfernalScalePickup": 3,
                        },
                        "perks": {
                            "statPerks": {"defense": 5001, "flex": 5008, "offense": 5005},
                            "styles": [],
                        },
                        "missions": {"playerScore0": 0, "playerScore1": 0},
                        "PlayerBehavior": {"PlayerBehavior_IsHeroInCombat": 0},
                    },
                    {
                        "participantId": 2,
                        "teamId": 200,
                        "championId": 236,
                        "championName": "Lucian",
                        "championTransform": 0,
                        "kills": 5,
                        "deaths": 2,
                        "assists": 3,
                        "win": True,
                        "puuid": "puuid2",
                        "riotIdGameName": "SomePlayer",
                        "riotIdTagline": "1234",
                        "summonerId": "sum2",
                        "summonerLevel": 100,
                        "summonerName": "SomePlayer",
                        "profileIcon": 936,
                        "role": "CARRY",
                        "lane": "BOTTOM",
                        "individualPosition": "BOTTOM",
                        "teamPosition": "BOTTOM",
                        "challenges": {
                            "12AssistStreakCount": 1,
                            "HealFromMapSources": 100,
                            "InfernalScalePickup": 0,
                        },
                        "perks": {
                            "statPerks": {"defense": 5002, "flex": 5008, "offense": 5005},
                            "styles": [],
                        },
                        "missions": {"playerScore0": 1, "playerScore1": 2},
                        "PlayerBehavior": {"PlayerBehavior_IsHeroInCombat": 1},
                    },
                ],
                "teams": [
                    {
                        "teamId": 100,
                        "win": False,
                        "bans": [
                            {"championId": 51, "pickTurn": 1},
                            {"championId": 67, "pickTurn": 2},
                            {"championId": 234, "pickTurn": 3},
                            {"championId": 145, "pickTurn": 4},
                            {"championId": 22, "pickTurn": 5},
                        ],
                        "objectives": {
                            "baron": {"first": False, "kills": 0},
                            "champion": {"first": True, "kills": 19},
                            "dragon": {"first": False, "kills": 0},
                            "inhibitor": {"first": False, "kills": 0},
                            "riftHerald": {"first": False, "kills": 0},
                            "tower": {"first": False, "kills": 2},
                        },
                    },
                    {
                        "teamId": 200,
                        "win": True,
                        "bans": [
                            {"championId": 7, "pickTurn": 6},
                            {"championId": 84, "pickTurn": 7},
                            {"championId": 92, "pickTurn": 8},
                            {"championId": 157, "pickTurn": 9},
                            {"championId": 238, "pickTurn": 10},
                        ],
                        "objectives": {
                            "baron": {"first": True, "kills": 1},
                            "champion": {"first": False, "kills": 30},
                            "dragon": {"first": True, "kills": 3},
                            "inhibitor": {"first": True, "kills": 1},
                            "riftHerald": {"first": True, "kills": 1},
                            "tower": {"first": True, "kills": 9},
                        },
                    },
                ],
            },
        }
    )


# ---------------------------------------------------------------------------
# camel_to_snake tests
# ---------------------------------------------------------------------------


class TestCamelToSnake:
    def test_basic(self):
        assert camel_to_snake("gameMode") == "game_mode"

    def test_leading_digit(self):
        assert camel_to_snake("12AssistStreakCount") == "12_assist_streak_count"

    def test_single_word(self):
        assert camel_to_snake("queueId") == "queue_id"

    def test_already_snake(self):
        assert camel_to_snake("game_mode") == "game_mode"


# ---------------------------------------------------------------------------
# transform_matches tests
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {
    "match_id",
    "data_version",
    "end_of_game_result",
    "game_creation",
    "game_duration",
    "game_end_timestamp",
    "game_id",
    "game_mode",
    "game_name",
    "game_start_timestamp",
    "game_type",
    "game_version",
    "map_id",
    "platform_id",
    "queue_id",
    "tournament_code",
}


class TestTransformMatches:
    @pytest.fixture()
    def match_df(self):
        return pl.DataFrame({"raw_json": [_make_match_json()]})

    def test_schema(self, match_df):
        result = transform_matches(match_df)
        assert set(result.columns) == EXPECTED_COLUMNS

    def test_row_count(self, match_df):
        result = transform_matches(match_df)
        assert len(result) == 1

    def test_values(self, match_df):
        result = transform_matches(match_df)
        row = result.row(0, named=True)
        assert row["match_id"] == "BR1_3219665682"
        assert row["game_duration"] == 1568
        assert row["platform_id"] == "BR1"
        assert row["game_mode"] == "CLASSIC"
        assert row["queue_id"] == 420


# ---------------------------------------------------------------------------
# Negative / boundary tests
# ---------------------------------------------------------------------------


class TestTransformMatchesNegative:
    def test_invalid_json_returns_nulls(self):
        """json_path_match returns null for unparseable JSON — all fields will be null."""
        df = pl.DataFrame({"raw_json": ["NOT VALID JSON"]})
        result = transform_matches(df)
        assert len(result) == 1
        row = result.row(0, named=True)
        assert row["match_id"] is None
        assert row["game_duration"] is None

    def test_empty_dataframe(self):
        df = pl.DataFrame({"raw_json": pl.Series([], dtype=pl.Utf8)})
        # json_decode on empty series may raise or return empty — either is acceptable
        try:
            result = transform_matches(df)
            assert len(result) == 0
        except pl.exceptions.ComputeError:
            pass  # acceptable: polars can't infer struct schema from empty series


# ---------------------------------------------------------------------------
# write_silver integration tests
# ---------------------------------------------------------------------------


class TestWriteSilver:
    @pytest.fixture()
    def tmp_table_path(self, tmp_path):
        return str(tmp_path / "silver_matches")

    def _matches_df(self, match_id: str = "BR1_3219665682") -> pl.DataFrame:
        raw = _make_match_json(match_id=match_id)
        return transform_matches(pl.DataFrame({"raw_json": [raw]}))

    def test_creates_and_merges(self, tmp_table_path):
        predicate = "s.match_id = t.match_id"

        # First write — bootstrap (no existing table)
        df1 = self._matches_df("BR1_001")
        write_silver(df1, tmp_table_path, predicate)
        result = pl.read_delta(tmp_table_path)
        assert len(result) == 1

        # Second write — same match_id, should MERGE (dedup)
        write_silver(df1, tmp_table_path, predicate)
        result = pl.read_delta(tmp_table_path)
        assert len(result) == 1

        # Third write — different match_id, should insert
        df2 = self._matches_df("BR1_002")
        write_silver(df2, tmp_table_path, predicate)
        result = pl.read_delta(tmp_table_path)
        assert len(result) == 2

    def test_empty_df_bootstrap(self, tmp_table_path):
        """Writing an empty DataFrame should produce an empty table."""
        raw = _make_match_json()
        schema_df = transform_matches(pl.DataFrame({"raw_json": [raw]}))
        empty_df = schema_df.clear()
        write_silver(empty_df, tmp_table_path, "s.match_id = t.match_id")
        result = pl.read_delta(tmp_table_path)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# transform_match_teams tests
# ---------------------------------------------------------------------------

EXPECTED_MATCH_TEAMS_COLUMNS = {"match_id", "team_id", "win"}


class TestTransformMatchTeams:
    @pytest.fixture()
    def match_df(self):
        return pl.DataFrame({"raw_json": [_make_match_json()]})

    def test_transform_match_teams_schema(self, match_df):
        result = transform_match_teams(match_df)
        assert set(result.columns) == EXPECTED_MATCH_TEAMS_COLUMNS

    def test_transform_match_teams_row_count(self, match_df):
        """1 match → 2 teams → 2 rows."""
        result = transform_match_teams(match_df)
        assert len(result) == 2

    def test_transform_match_teams_values(self, match_df):
        result = transform_match_teams(match_df)
        rows = result.sort("team_id").to_dicts()
        assert rows[0]["team_id"] == 100
        assert rows[0]["win"] is False
        assert rows[1]["team_id"] == 200
        assert rows[1]["win"] is True


# ---------------------------------------------------------------------------
# transform_match_teams_bans tests
# ---------------------------------------------------------------------------

EXPECTED_MATCH_TEAMS_BANS_COLUMNS = {"match_id", "team_id", "champion_id", "pick_turn"}


class TestTransformMatchTeamsBans:
    @pytest.fixture()
    def match_df(self):
        return pl.DataFrame({"raw_json": [_make_match_json()]})

    def test_transform_match_teams_bans_schema(self, match_df):
        result = transform_match_teams_bans(match_df)
        assert set(result.columns) == EXPECTED_MATCH_TEAMS_BANS_COLUMNS

    def test_transform_match_teams_bans_row_count(self, match_df):
        """1 match, 2 teams × 5 bans = 10 rows."""
        result = transform_match_teams_bans(match_df)
        assert len(result) == 10

    def test_transform_match_teams_bans_values(self, match_df):
        result = transform_match_teams_bans(match_df)
        # Team 100, first ban
        team100 = result.filter(pl.col("team_id") == 100).sort("pick_turn")
        assert team100.row(0, named=True)["champion_id"] == 51
        assert team100.row(0, named=True)["pick_turn"] == 1


# ---------------------------------------------------------------------------
# transform_match_teams_objectives tests
# ---------------------------------------------------------------------------

EXPECTED_MATCH_TEAMS_OBJECTIVES_COLUMNS = {
    "match_id",
    "team_id",
    "objective_name",
    "is_first",
    "kills",
}


class TestTransformMatchTeamsObjectives:
    @pytest.fixture()
    def match_df(self):
        return pl.DataFrame({"raw_json": [_make_match_json()]})

    def test_transform_match_teams_objectives_schema(self, match_df):
        result = transform_match_teams_objectives(match_df)
        assert set(result.columns) == EXPECTED_MATCH_TEAMS_OBJECTIVES_COLUMNS

    def test_transform_match_teams_objectives_row_count(self, match_df):
        """1 match, 2 teams × 6 objectives = 12 rows."""
        result = transform_match_teams_objectives(match_df)
        assert len(result) == 12

    def test_transform_match_teams_objectives_values(self, match_df):
        """Spot-check: team 200 baron objective should have first=True, kills=1."""
        result = transform_match_teams_objectives(match_df)
        baron_200 = result.filter(
            (pl.col("team_id") == 200) & (pl.col("objective_name") == "baron")
        )
        assert len(baron_200) == 1
        row = baron_200.row(0, named=True)
        assert row["is_first"] is True
        assert row["kills"] == 1

    def test_transform_match_teams_objectives_snake_case(self, match_df):
        """riftHerald should become rift_herald in objective_name."""
        result = transform_match_teams_objectives(match_df)
        names = result["objective_name"].unique().to_list()
        assert "rift_herald" in names
        assert "riftHerald" not in names


# ---------------------------------------------------------------------------
# transform_match_participants tests
# ---------------------------------------------------------------------------


class TestTransformMatchParticipants:
    @pytest.fixture()
    def match_df(self):
        return pl.DataFrame({"raw_json": [_make_match_json()]})

    def test_transform_match_participants_row_count(self, match_df):
        """1 match with 2 participants → 2 rows."""
        result = transform_match_participants(match_df)
        assert len(result) == 2

    def test_transform_match_participants_scalar_fields(self, match_df):
        """Spot-check champion_name, kills, deaths, assists, team_id from fixture."""
        result = transform_match_participants(match_df)
        rows = result.sort("participant_id").to_dicts()
        p1 = rows[0]
        assert p1["champion_name"] == "Gragas"
        assert p1["kills"] == 1
        assert p1["deaths"] == 5
        assert p1["assists"] == 1
        assert p1["team_id"] == 100
        assert p1["match_id"] == "BR1_3219665682"

        p2 = rows[1]
        assert p2["champion_name"] == "Lucian"
        assert p2["kills"] == 5
        assert p2["deaths"] == 2
        assert p2["assists"] == 3
        assert p2["team_id"] == 200

    def test_transform_match_participants_challenges_prefix(self, match_df):
        """All challenges columns start with 'challenges_' and are snake_case."""
        result = transform_match_participants(match_df)
        challenge_cols = [c for c in result.columns if c.startswith("challenges_")]
        assert len(challenge_cols) > 0
        for col in challenge_cols:
            # Must be snake_case (no uppercase letters)
            assert col == col.lower(), f"Column {col} is not snake_case"
            assert col.startswith("challenges_")

    def test_transform_match_participants_json_columns(self, match_df):
        """perks_json, missions_json, player_behavior_json are valid JSON strings."""
        result = transform_match_participants(match_df)
        for col in ["perks_json", "missions_json", "player_behavior_json"]:
            assert col in result.columns, f"Missing column {col}"
            for val in result[col].to_list():
                assert val is not None
                parsed = json.loads(val)
                assert isinstance(parsed, dict)

    def test_transform_match_participants_missing_challenge(self):
        """Fixture with one challenge field removed → that column is null, no error."""
        # Build a match where participant 1 has InfernalScalePickup but participant 2 does not
        raw = json.dumps(
            {
                "metadata": {"dataVersion": "2", "matchId": "BR1_MISS", "participants": []},
                "info": {
                    "endOfGameResult": "GameComplete",
                    "gameCreation": 0,
                    "gameDuration": 100,
                    "gameEndTimestamp": 0,
                    "gameId": 1,
                    "gameMode": "CLASSIC",
                    "gameName": "test",
                    "gameStartTimestamp": 0,
                    "gameType": "MATCHED_GAME",
                    "gameVersion": "1.0",
                    "mapId": 11,
                    "platformId": "BR1",
                    "queueId": 420,
                    "tournamentCode": "",
                    "participants": [
                        {
                            "participantId": 1,
                            "teamId": 100,
                            "championId": 1,
                            "championName": "A",
                            "kills": 0,
                            "deaths": 0,
                            "assists": 0,
                            "win": False,
                            "puuid": "p1",
                            "challenges": {
                                "12AssistStreakCount": 0,
                                "HealFromMapSources": 5,
                            },
                        },
                        {
                            "participantId": 2,
                            "teamId": 200,
                            "championId": 2,
                            "championName": "B",
                            "kills": 0,
                            "deaths": 0,
                            "assists": 0,
                            "win": True,
                            "puuid": "p2",
                            "challenges": {
                                "12AssistStreakCount": 1,
                                # HealFromMapSources intentionally missing
                            },
                        },
                    ],
                    "teams": [],
                },
            }
        )
        df = pl.DataFrame({"raw_json": [raw]})
        result = transform_match_participants(df)
        assert len(result) == 2
        rows = result.sort("participant_id").to_dicts()
        # Participant 1 has the field
        assert rows[0]["challenges_heal_from_map_sources"] == 5
        # Participant 2 is missing it → null
        assert rows[1]["challenges_heal_from_map_sources"] is None

    def test_transform_match_participants_schema_tripwire(self, match_df):
        """Total column count is in expected range: scalars + challenges + JSON cols + match_id."""
        result = transform_match_participants(match_df)
        n_scalars = len(PARTICIPANT_SCALAR_FIELDS)
        n_json = 3  # perks_json, missions_json, player_behavior_json
        n_match_id = 1
        # challenges count depends on fixture (3 challenge fields in fixture)
        n_challenges = len([c for c in result.columns if c.startswith("challenges_")])
        expected = n_match_id + n_scalars + n_challenges + n_json
        assert len(result.columns) == expected

    def test_transform_match_participants_no_challenges(self):
        """Participant with no challenges object → no challenges_ columns for that row."""
        raw = json.dumps(
            {
                "metadata": {"dataVersion": "2", "matchId": "BR1_NOCH", "participants": []},
                "info": {
                    "endOfGameResult": "GameComplete",
                    "gameCreation": 0,
                    "gameDuration": 100,
                    "gameEndTimestamp": 0,
                    "gameId": 1,
                    "gameMode": "CLASSIC",
                    "gameName": "test",
                    "gameStartTimestamp": 0,
                    "gameType": "MATCHED_GAME",
                    "gameVersion": "1.0",
                    "mapId": 11,
                    "platformId": "BR1",
                    "queueId": 420,
                    "tournamentCode": "",
                    "participants": [
                        {
                            "participantId": 1,
                            "teamId": 100,
                            "championId": 1,
                            "championName": "A",
                            "kills": 0,
                            "deaths": 0,
                            "assists": 0,
                            "win": False,
                            "puuid": "p1",
                            # No challenges key at all
                        },
                    ],
                    "teams": [],
                },
            }
        )
        df = pl.DataFrame({"raw_json": [raw]})
        result = transform_match_participants(df)
        assert len(result) == 1
        challenge_cols = [c for c in result.columns if c.startswith("challenges_")]
        assert len(challenge_cols) == 0  # no challenges columns at all

    def test_transform_match_participants_unknown_fields_dropped(self, match_df):
        """Unknown top-level participant fields should not appear in output (D007)."""
        result = transform_match_participants(match_df)
        # The fixture participants have fields like riotIdGameName etc.
        # but no unknown fields. Verify that only known columns exist.
        known_snake = {"match_id"} | {camel_to_snake(f) for f in PARTICIPANT_SCALAR_FIELDS}
        known_snake |= {"perks_json", "missions_json", "player_behavior_json"}
        for col in result.columns:
            if col.startswith("challenges_"):
                continue
            assert col in known_snake, f"Unexpected column {col} in output"


# ---------------------------------------------------------------------------
# materialize_silver_matches integration tests
# ---------------------------------------------------------------------------


def _write_bronze_match(bronze_path: str, match_id: str) -> None:
    """Write a single synthetic match to Bronze via BronzeWriter."""
    raw_json = _make_match_json(match_id=match_id)
    writer = BronzeWriter(
        table_name="match_details_raw",
        primary_key_col="match_id",
        base_path=bronze_path,
    )
    writer.write_batch(
        records=[{"match_id": match_id, "raw_json": raw_json}],
        endpoint="/lol/match/v5/matches",
        status_code=200,
        region="americas",
    )


class TestMaterializeCreatesAllFiveTables:
    """Write 2 synthetic matches to Bronze → materialize → verify 5 Silver Delta tables."""

    def test_materialize_creates_all_five_tables(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        _write_bronze_match(bronze_path, "BR1_001")
        _write_bronze_match(bronze_path, "BR1_002")

        result = materialize_silver_matches(bronze_path, silver_path)

        # All 5 tables exist
        assert set(result.keys()) == {
            "matches",
            "match_participants",
            "match_teams",
            "match_teams_bans",
            "match_teams_objectives",
        }

        # Row counts: 2 matches, 2 participants each = 4, 2 teams each = 4,
        # 2 teams × 5 bans each = 10 per match → 20, 2 teams × 6 objectives each = 12 per match → 24
        assert result["matches"] == 2
        assert result["match_participants"] == 4
        assert result["match_teams"] == 4
        assert result["match_teams_bans"] == 20
        assert result["match_teams_objectives"] == 24

        # Verify all tables are queryable with polars.scan_delta()
        for table_name in result:
            lf = pl.scan_delta(f"{silver_path}/{table_name}")
            df = lf.collect()
            assert len(df) == result[table_name]


class TestMaterializeIncremental:
    """Incremental MERGE: re-run on same data adds zero rows; new data adds correctly."""

    def test_materialize_incremental_adds_only_new(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        # First materialization: 2 matches
        _write_bronze_match(bronze_path, "BR1_001")
        _write_bronze_match(bronze_path, "BR1_002")
        first = materialize_silver_matches(bronze_path, silver_path)

        # Second materialization: same Bronze data → no new rows
        second = materialize_silver_matches(bronze_path, silver_path)
        for table_name in first:
            actual = len(pl.read_delta(f"{silver_path}/{table_name}"))
            assert actual == first[table_name], (
                f"{table_name}: expected {first[table_name]} rows after re-run, got {actual}"
            )

        # Third materialization: append 1 new match to Bronze → rows increase
        _write_bronze_match(bronze_path, "BR1_003")
        third = materialize_silver_matches(bronze_path, silver_path)

        # Now Silver should have 3 matches worth of data
        assert len(pl.read_delta(f"{silver_path}/matches")) == 3
        assert len(pl.read_delta(f"{silver_path}/match_participants")) == 6
        assert len(pl.read_delta(f"{silver_path}/match_teams")) == 6
        assert len(pl.read_delta(f"{silver_path}/match_teams_bans")) == 30
        assert len(pl.read_delta(f"{silver_path}/match_teams_objectives")) == 36


class TestMaterializeEmptyBronze:
    """Empty Bronze table → all Silver tables created with 0 rows."""

    def test_materialize_empty_bronze(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        # Create an empty Bronze table with the right schema
        writer = BronzeWriter(
            table_name="match_details_raw",
            primary_key_col="match_id",
            base_path=bronze_path,
        )
        # Write and then read to create the table, then we need an empty table
        _write_bronze_match(bronze_path, "BR1_TEMP")
        # Read the table, delete it, rewrite empty
        from deltalake import write_deltalake, DeltaTable

        dt = DeltaTable(f"{bronze_path}/match_details_raw")
        schema = dt.to_pyarrow_table().schema
        import pyarrow as pa

        empty_table = pa.table({col: pa.array([], type=schema.field(col).type) for col in schema.names})
        # Overwrite with empty
        write_deltalake(f"{bronze_path}/match_details_raw", empty_table, mode="overwrite")

        result = materialize_silver_matches(bronze_path, silver_path)

        # All tables should exist with 0 rows
        for table_name, count in result.items():
            assert count == 0, f"{table_name} should have 0 rows, got {count}"
