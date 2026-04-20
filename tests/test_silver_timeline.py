"""Tests for Silver timeline transforms."""

from __future__ import annotations

import json

import polars as pl
import pytest

from datarift.bronze_writer import BronzeWriter
from datarift.silver_timeline import (
    materialize_silver_timelines,
    transform_match_timeline_events,
    transform_match_timeline_frames,
    transform_match_timeline_participant_frames,
)


def _make_timeline_json(
    match_id: str = "BR1_3219665682",
) -> str:
    """Return a structurally complete Match-V5 Timeline JSON string.

    Contains 2 frames, 2 participants per frame, and 3+ event types.
    """
    champion_stats = {
        "abilityHaste": 0,
        "abilityPower": 0,
        "armor": 28,
        "armorPen": 0,
        "armorPenPercent": 0,
        "attackDamage": 60,
        "attackSpeed": 100,
        "bonusArmorPenPercent": 0,
        "bonusMagicPenPercent": 0,
        "ccReduction": 0,
        "cooldownReduction": 0,
        "health": 570,
        "healthMax": 570,
        "healthRegen": 7,
        "lifesteal": 0,
        "magicPen": 0,
        "magicPenPercent": 0,
        "magicResist": 32,
        "movementSpeed": 330,
        "omnivamp": 0,
        "physicalVamp": 0,
        "power": 0,
        "powerMax": 300,
        "powerRegen": 0,
        "spellVamp": 0,
    }

    damage_stats = {
        "magicDamageDone": 0,
        "magicDamageDoneToChampions": 0,
        "magicDamageTaken": 0,
        "physicalDamageDone": 0,
        "physicalDamageDoneToChampions": 0,
        "physicalDamageTaken": 0,
        "totalDamageDone": 0,
        "totalDamageDoneToChampions": 0,
        "totalDamageTaken": 0,
        "trueDamageDone": 0,
        "trueDamageDoneToChampions": 0,
        "trueDamageTaken": 0,
    }

    def _make_participant_frame(participant_id: int) -> dict:
        return {
            "championStats": champion_stats,
            "currentGold": 500,
            "damageStats": damage_stats,
            "goldPerSecond": 0,
            "jungleMinionsKilled": 0,
            "level": 1,
            "minionsKilled": 0,
            "participantId": participant_id,
            "position": {"x": 1000 + participant_id * 100, "y": 2000 + participant_id * 100},
            "timeEnemySpentControlled": 0,
            "totalGold": 500,
            "xp": 0,
        }

    frames = [
        {
            "timestamp": 0,
            "participantFrames": {
                "1": _make_participant_frame(1),
                "2": _make_participant_frame(2),
            },
            "events": [
                {
                    "timestamp": 0,
                    "realTimestamp": 1700000000000,
                    "type": "PAUSE_END",
                },
                {
                    "timestamp": 0,
                    "realTimestamp": 1700000000100,
                    "type": "ITEM_PURCHASED",
                    "participantId": 1,
                    "itemId": 1055,
                },
                {
                    "timestamp": 0,
                    "realTimestamp": 1700000000200,
                    "type": "ITEM_PURCHASED",
                    "participantId": 2,
                    "itemId": 1054,
                },
            ],
        },
        {
            "timestamp": 60000,
            "participantFrames": {
                "1": {
                    **_make_participant_frame(1),
                    "currentGold": 750,
                    "totalGold": 750,
                    "minionsKilled": 6,
                    "xp": 300,
                    "level": 2,
                },
                "2": {
                    **_make_participant_frame(2),
                    "currentGold": 680,
                    "totalGold": 680,
                    "minionsKilled": 4,
                    "xp": 250,
                },
            },
            "events": [
                {
                    "timestamp": 55000,
                    "realTimestamp": 1700000055000,
                    "type": "WARD_PLACED",
                    "creatorId": 1,
                    "wardType": "YELLOW_TRINKET",
                },
            ],
        },
    ]

    return json.dumps(
        {
            "metadata": {
                "matchId": match_id,
                "dataVersion": "2",
                "participants": ["puuid1", "puuid2"],
            },
            "info": {
                "frameInterval": 60000,
                "frames": frames,
            },
        }
    )


class TestTransformMatchTimelineFrames:
    """Tests for transform_match_timeline_frames."""

    def _make_df(self, *jsons: str) -> pl.DataFrame:
        return pl.DataFrame({"raw_json": list(jsons)})

    def test_schema(self):
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_frames(df)
        assert set(result.columns) == {"match_id", "frame_index", "timestamp"}

    def test_row_count(self):
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_frames(df)
        assert len(result) == 2

    def test_values(self):
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_frames(df)
        rows = result.sort("frame_index").to_dicts()
        assert rows[0]["frame_index"] == 0
        assert rows[0]["timestamp"] == 0
        assert rows[0]["match_id"] == "BR1_3219665682"
        assert rows[1]["frame_index"] == 1
        assert rows[1]["timestamp"] == 60000

    def test_empty_frames(self):
        """Empty frames array produces 0 rows."""
        doc = json.loads(_make_timeline_json())
        doc["info"]["frames"] = []
        df = self._make_df(json.dumps(doc))
        result = transform_match_timeline_frames(df)
        assert len(result) == 0
        assert set(result.columns) == {"match_id", "frame_index", "timestamp"}


class TestTransformMatchTimelineParticipantFrames:
    """Tests for transform_match_timeline_participant_frames."""

    def _make_df(self, *jsons: str) -> pl.DataFrame:
        return pl.DataFrame({"raw_json": list(jsons)})

    def test_schema_tripwire(self):
        """50 columns: match_id + frame_index + 9 scalars + 2 position + 25 champion_stats + 12 damage_stats."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_participant_frames(df)
        assert len(result.columns) == 50

    def test_row_count(self):
        """1 match × 2 frames × 2 participants = 4 rows."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_participant_frames(df)
        assert len(result) == 4

    def test_champion_stats_prefix(self):
        """All 25 champion_stats_ columns present and snake_case."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_participant_frames(df)
        cs_cols = [c for c in result.columns if c.startswith("champion_stats_")]
        assert len(cs_cols) == 25
        for col in cs_cols:
            # snake_case: no uppercase letters
            assert col == col.lower(), f"{col} is not snake_case"

    def test_damage_stats_prefix(self):
        """All 12 damage_stats_ columns present and snake_case."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_participant_frames(df)
        ds_cols = [c for c in result.columns if c.startswith("damage_stats_")]
        assert len(ds_cols) == 12
        for col in ds_cols:
            assert col == col.lower(), f"{col} is not snake_case"

    def test_scalar_values(self):
        """Spot-check scalar fields from fixture."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_participant_frames(df)
        rows = result.sort(["frame_index", "participant_id"]).to_dicts()
        # Frame 0, participant 1
        assert rows[0]["current_gold"] == 500
        assert rows[0]["level"] == 1
        assert rows[0]["position_x"] == 1100  # 1000 + 1*100
        assert rows[0]["position_y"] == 2100  # 2000 + 1*100
        # Frame 1, participant 1 — updated values
        assert rows[2]["current_gold"] == 750
        assert rows[2]["level"] == 2
        assert rows[2]["minions_killed"] == 6
        assert rows[2]["xp"] == 300

    def test_participant_id_from_key(self):
        """participant_id correctly parsed from keyed object key."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_participant_frames(df)
        pids = sorted(result["participant_id"].unique().to_list())
        assert pids == [1, 2]


class TestTransformMatchTimelineEvents:
    """Tests for transform_match_timeline_events."""

    def _make_df(self, *jsons: str) -> pl.DataFrame:
        return pl.DataFrame({"raw_json": list(jsons)})

    def test_schema(self):
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_events(df)
        assert set(result.columns) == {
            "match_id",
            "frame_index",
            "event_index",
            "timestamp",
            "real_timestamp",
            "type",
            "participant_id",
            "event_json",
        }

    def test_row_count(self):
        """Fixture has 3 events in frame 0 + 1 in frame 1 = 4 total."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_events(df)
        assert len(result) == 4

    def test_event_json_parseable(self):
        """Every event_json value round-trips to a dict."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_events(df)
        for val in result["event_json"].to_list():
            parsed = json.loads(val)
            assert isinstance(parsed, dict)

    def test_participant_id_null_for_no_participant(self):
        """Events without participantId (e.g. PAUSE_END) have null participant_id."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_events(df)
        pause_rows = result.filter(pl.col("type") == "PAUSE_END")
        assert len(pause_rows) == 1
        assert pause_rows["participant_id"][0] is None

    def test_participant_id_present(self):
        """Events with participantId (e.g. ITEM_PURCHASED) have non-null participant_id."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_events(df)
        item_rows = result.filter(pl.col("type") == "ITEM_PURCHASED")
        assert len(item_rows) == 2
        for pid in item_rows["participant_id"].to_list():
            assert pid is not None

    def test_multiple_event_types(self):
        """At least 3 distinct type values present."""
        df = self._make_df(_make_timeline_json())
        result = transform_match_timeline_events(df)
        types = result["type"].unique().to_list()
        assert len(types) >= 3


# ---------------------------------------------------------------------------
# materialize_silver_timelines integration tests
# ---------------------------------------------------------------------------


def _write_bronze_timeline(bronze_path: str, match_id: str) -> None:
    """Write a single synthetic timeline to Bronze via BronzeWriter."""
    raw_json = _make_timeline_json(match_id=match_id)
    writer = BronzeWriter(
        table_name="match_timelines_raw",
        primary_key_col="match_id",
        base_path=bronze_path,
    )
    writer.write_batch(
        records=[{"match_id": match_id, "raw_json": raw_json}],
        endpoint="/lol/match/v5/matches/by-match/timeline",
        status_code=200,
        region="americas",
    )


class TestMaterializeCreatesAllThreeTables:
    """Write 2 synthetic timelines to Bronze → materialize → verify 3 Silver Delta tables."""

    def test_materialize_creates_all_three_tables(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        _write_bronze_timeline(bronze_path, "BR1_001")
        _write_bronze_timeline(bronze_path, "BR1_002")

        result = materialize_silver_timelines(bronze_path, silver_path)

        # All 3 tables exist
        assert set(result.keys()) == {
            "match_timeline_frames",
            "match_timeline_participant_frames",
            "match_timeline_events",
        }

        # Row counts: 2 matches × 2 frames = 4 frame rows
        assert result["match_timeline_frames"] == 4
        # 2 matches × 2 frames × 2 participants = 8 participant_frame rows
        assert result["match_timeline_participant_frames"] == 8
        # 2 matches × (3 events in frame 0 + 1 event in frame 1) = 8 event rows
        assert result["match_timeline_events"] == 8

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
        _write_bronze_timeline(bronze_path, "BR1_001")
        _write_bronze_timeline(bronze_path, "BR1_002")
        first = materialize_silver_timelines(bronze_path, silver_path)

        # Second materialization: same Bronze data → no new rows
        second = materialize_silver_timelines(bronze_path, silver_path)
        for table_name in first:
            actual = len(pl.read_delta(f"{silver_path}/{table_name}"))
            assert actual == first[table_name], (
                f"{table_name}: expected {first[table_name]} rows after re-run, got {actual}"
            )

        # Third materialization: append 1 new match to Bronze → rows increase
        _write_bronze_timeline(bronze_path, "BR1_003")
        third = materialize_silver_timelines(bronze_path, silver_path)

        # Now Silver should have 3 matches worth of data
        assert len(pl.read_delta(f"{silver_path}/match_timeline_frames")) == 6
        assert len(pl.read_delta(f"{silver_path}/match_timeline_participant_frames")) == 12
        assert len(pl.read_delta(f"{silver_path}/match_timeline_events")) == 12


class TestMaterializeEmptyBronze:
    """Empty Bronze table → all Silver tables created with 0 rows."""

    def test_materialize_empty_bronze(self, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        # Create an empty Bronze table with the right schema
        _write_bronze_timeline(bronze_path, "BR1_TEMP")
        from deltalake import DeltaTable, write_deltalake
        import pyarrow as pa

        dt = DeltaTable(f"{bronze_path}/match_timelines_raw")
        schema = dt.to_pyarrow_table().schema
        empty_table = pa.table(
            {col: pa.array([], type=schema.field(col).type) for col in schema.names}
        )
        write_deltalake(f"{bronze_path}/match_timelines_raw", empty_table, mode="overwrite")

        result = materialize_silver_timelines(bronze_path, silver_path)

        # All tables should exist with 0 rows
        for table_name, count in result.items():
            assert count == 0, f"{table_name} should have 0 rows, got {count}"
