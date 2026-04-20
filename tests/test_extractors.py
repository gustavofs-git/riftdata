"""Tests for Bronze extraction functions (league entries, accounts, summoners)."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from datarift.bronze_writer import BronzeWriter
from datarift.config import ExtractionConfig
from datarift.extractors import (
    extract_accounts,
    extract_league_entries,
    extract_match_details,
    extract_match_ids,
    extract_match_timelines,
    extract_summoners,
)
from datarift.riot_client import RiotRateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RATE_HEADERS = {
    "content-type": "application/json",
    "X-App-Rate-Limit": "100:1,1000:120",
    "X-App-Rate-Limit-Count": "1:1,1:120",
    "X-Method-Rate-Limit": "500:10",
    "X-Method-Rate-Limit-Count": "1:10",
}


def _make_config(tmp_path, batch_size: int = 200) -> ExtractionConfig:
    return ExtractionConfig(
        region="kr",
        tiers=["CHALLENGER"],
        batch_size=batch_size,
        bronze_path=str(tmp_path / "bronze"),
    )


def _league_entry(puuid: str) -> dict:
    return {
        "puuid": puuid,
        "summonerId": f"sid-{puuid}",
        "leaguePoints": 100,
        "wins": 10,
        "losses": 5,
    }


def _account_data(puuid: str) -> dict:
    return {"puuid": puuid, "gameName": f"Player-{puuid[:4]}", "tagLine": "KR1"}


def _summoner_data(puuid: str) -> dict:
    return {
        "puuid": puuid,
        "id": f"sid-{puuid}",
        "accountId": f"aid-{puuid}",
        "name": f"Summoner-{puuid[:4]}",
        "summonerLevel": 30,
    }


# ---------------------------------------------------------------------------
# League entries tests
# ---------------------------------------------------------------------------


class TestExtractLeagueEntries:
    """Tests for extract_league_entries."""

    @pytest.mark.asyncio
    async def test_correct_api_path(self, tmp_path, make_mock_transport):
        """Verify the correct League-Exp-V4 path is called."""
        called_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            called_paths.append(str(request.url))
            if "page=1" in str(request.url):
                return httpx.Response(
                    200,
                    json=[_league_entry("p1")],
                    headers=_RATE_HEADERS,
                )
            return httpx.Response(200, json=[], headers=_RATE_HEADERS)

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("league_entries_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_league_entries(client, config, writer)

        assert any(
            "/lol/league-exp/v4/entries/RANKED_SOLO_5x5/CHALLENGER/I" in p
            for p in called_paths
        )

    @pytest.mark.asyncio
    async def test_pagination_two_pages_then_empty(self, tmp_path, make_mock_transport):
        """Mock returns 2 pages then empty — all entries collected."""
        entries_p1 = [_league_entry(f"p{i}") for i in range(3)]
        entries_p2 = [_league_entry(f"p{i}") for i in range(3, 5)]

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "page=1" in url:
                return httpx.Response(200, json=entries_p1, headers=_RATE_HEADERS)
            if "page=2" in url:
                return httpx.Response(200, json=entries_p2, headers=_RATE_HEADERS)
            return httpx.Response(200, json=[], headers=_RATE_HEADERS)

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("league_entries_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            puuids = await extract_league_entries(client, config, writer)

        assert len(puuids) == 5
        assert set(puuids) == {f"p{i}" for i in range(5)}

    @pytest.mark.asyncio
    async def test_bronze_table_written(self, tmp_path, make_mock_transport):
        """Verify Bronze table is written with correct data."""
        import polars as pl
        from deltalake import DeltaTable

        entries = [_league_entry("p1"), _league_entry("p2")]

        def handler(request: httpx.Request) -> httpx.Response:
            if "page=1" in str(request.url):
                return httpx.Response(200, json=entries, headers=_RATE_HEADERS)
            return httpx.Response(200, json=[], headers=_RATE_HEADERS)

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("league_entries_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_league_entries(client, config, writer)

        dt = DeltaTable(str(tmp_path / "bronze" / "league_entries_raw"))
        df = pl.DataFrame(dt.to_pyarrow_table())
        assert len(df) == 2
        assert set(df["puuid"].to_list()) == {"p1", "p2"}
        assert "raw_json" in df.columns

    @pytest.mark.asyncio
    async def test_anti_join_skips_existing(self, tmp_path, make_mock_transport):
        """Pre-populate BronzeWriter, run extractor, verify no new writes for existing keys."""
        entries = [_league_entry("existing1"), _league_entry("new1")]

        def handler(request: httpx.Request) -> httpx.Response:
            if "page=1" in str(request.url):
                return httpx.Response(200, json=entries, headers=_RATE_HEADERS)
            return httpx.Response(200, json=[], headers=_RATE_HEADERS)

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("league_entries_raw", "puuid", str(tmp_path / "bronze"))

        # Pre-populate with existing1
        writer.write_batch(
            records=[{"puuid": "existing1", "raw_json": '{"puuid":"existing1"}'}],
            endpoint="/seed",
            status_code=200,
            region="kr",
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            puuids = await extract_league_entries(client, config, writer)

        # Should return both existing and new puuids
        assert "existing1" in puuids
        assert "new1" in puuids

        # Table should have 2 rows: 1 seeded + 1 new (not 3 = 1 seeded + 2 from API)
        from deltalake import DeltaTable
        import polars as pl

        dt = DeltaTable(str(tmp_path / "bronze" / "league_entries_raw"))
        df = pl.DataFrame(dt.to_pyarrow_table())
        assert len(df) == 2

    @pytest.mark.asyncio
    async def test_shutdown_event_stops_extraction(
        self, tmp_path, make_mock_transport
    ):
        """Shutdown event causes extraction to return early."""
        def handler(request: httpx.Request) -> httpx.Response:
            if "page=1" in str(request.url):
                return httpx.Response(
                    200,
                    json=[_league_entry("p1")],
                    headers=_RATE_HEADERS,
                )
            return httpx.Response(200, json=[], headers=_RATE_HEADERS)

        config = ExtractionConfig(
            region="kr",
            tiers=["CHALLENGER", "GRANDMASTER"],
            bronze_path=str(tmp_path / "bronze"),
        )
        transport = make_mock_transport(handler)
        writer = BronzeWriter("league_entries_raw", "puuid", str(tmp_path / "bronze"))

        shutdown = asyncio.Event()
        shutdown.set()  # Already set — should return immediately

        async with RiotRateLimiter("test-key", transport=transport) as client:
            puuids = await extract_league_entries(
                client, config, writer, shutdown_event=shutdown
            )

        # Should return early with whatever was collected (existing keys)
        assert isinstance(puuids, list)


# ---------------------------------------------------------------------------
# Accounts tests
# ---------------------------------------------------------------------------


class TestExtractAccounts:
    """Tests for extract_accounts."""

    @pytest.mark.asyncio
    async def test_correct_api_path(self, tmp_path, make_mock_transport):
        """Verify the correct Account-V1 path is called."""
        called_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            called_paths.append(str(request.url))
            return httpx.Response(
                200,
                json=_account_data("p1"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("accounts_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_accounts(client, ["p1"], config, writer)

        assert any("/riot/account/v1/accounts/by-puuid/p1" in p for p in called_paths)

    @pytest.mark.asyncio
    async def test_bronze_table_written(self, tmp_path, make_mock_transport):
        """Verify accounts are written to Bronze Delta table."""
        import polars as pl
        from deltalake import DeltaTable

        puuids = ["p1", "p2"]

        def handler(request: httpx.Request) -> httpx.Response:
            puuid = str(request.url).split("/")[-1]
            return httpx.Response(
                200, json=_account_data(puuid), headers=_RATE_HEADERS
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("accounts_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_accounts(client, puuids, config, writer)

        dt = DeltaTable(str(tmp_path / "bronze" / "accounts_raw"))
        df = pl.DataFrame(dt.to_pyarrow_table())
        assert len(df) == 2
        assert set(df["puuid"].to_list()) == {"p1", "p2"}

    @pytest.mark.asyncio
    async def test_anti_join_skips_existing(self, tmp_path, make_mock_transport):
        """Pre-populate accounts_raw, verify no new writes for existing keys."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            puuid = str(request.url).split("/")[-1]
            return httpx.Response(
                200, json=_account_data(puuid), headers=_RATE_HEADERS
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("accounts_raw", "puuid", str(tmp_path / "bronze"))

        # Pre-populate
        writer.write_batch(
            records=[{"puuid": "existing1", "raw_json": '{"puuid":"existing1"}'}],
            endpoint="/seed",
            status_code=200,
            region="kr",
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_accounts(client, ["existing1", "new1"], config, writer)

        # Only new1 should have been fetched
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_batch_size_honored(self, tmp_path, make_mock_transport):
        """Mock 5 records with batch_size=2 → 3 write_batch calls."""

        def handler(request: httpx.Request) -> httpx.Response:
            puuid = str(request.url).split("/")[-1]
            return httpx.Response(
                200, json=_account_data(puuid), headers=_RATE_HEADERS
            )

        config = _make_config(tmp_path, batch_size=2)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("accounts_raw", "puuid", str(tmp_path / "bronze"))

        original_write = writer.write_batch
        write_calls = []

        def tracking_write(*args, **kwargs):
            write_calls.append(kwargs.get("records") or args[0])
            return original_write(*args, **kwargs)

        writer.write_batch = tracking_write

        puuids = [f"p{i}" for i in range(5)]
        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_accounts(client, puuids, config, writer)

        # 5 records / batch_size 2 = 2 full batches + 1 remainder = 3 calls
        assert len(write_calls) == 3


# ---------------------------------------------------------------------------
# Summoners tests
# ---------------------------------------------------------------------------


class TestExtractSummoners:
    """Tests for extract_summoners."""

    @pytest.mark.asyncio
    async def test_correct_api_path(self, tmp_path, make_mock_transport):
        """Verify the correct Summoner-V4 path is called."""
        called_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            called_paths.append(str(request.url))
            return httpx.Response(
                200,
                json=_summoner_data("p1"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("summoners_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_summoners(client, ["p1"], config, writer)

        assert any(
            "/lol/summoner/v4/summoners/by-puuid/p1" in p for p in called_paths
        )

    @pytest.mark.asyncio
    async def test_bronze_table_written(self, tmp_path, make_mock_transport):
        """Verify summoners are written to Bronze Delta table."""
        import polars as pl
        from deltalake import DeltaTable

        puuids = ["p1", "p2", "p3"]

        def handler(request: httpx.Request) -> httpx.Response:
            puuid = str(request.url).split("/")[-1]
            return httpx.Response(
                200, json=_summoner_data(puuid), headers=_RATE_HEADERS
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("summoners_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_summoners(client, puuids, config, writer)

        dt = DeltaTable(str(tmp_path / "bronze" / "summoners_raw"))
        df = pl.DataFrame(dt.to_pyarrow_table())
        assert len(df) == 3
        assert set(df["puuid"].to_list()) == {"p1", "p2", "p3"}

    @pytest.mark.asyncio
    async def test_anti_join_skips_existing(self, tmp_path, make_mock_transport):
        """Pre-populate summoners_raw, verify no API calls for existing keys."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            puuid = str(request.url).split("/")[-1]
            return httpx.Response(
                200, json=_summoner_data(puuid), headers=_RATE_HEADERS
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("summoners_raw", "puuid", str(tmp_path / "bronze"))

        # Pre-populate
        writer.write_batch(
            records=[{"puuid": "existing1", "raw_json": '{"puuid":"existing1"}'}],
            endpoint="/seed",
            status_code=200,
            region="kr",
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_summoners(client, ["existing1", "new1"], config, writer)

        # Only new1 should have been fetched
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_batch_size_honored(self, tmp_path, make_mock_transport):
        """Mock 5 records with batch_size=2 → 3 write_batch calls."""

        def handler(request: httpx.Request) -> httpx.Response:
            puuid = str(request.url).split("/")[-1]
            return httpx.Response(
                200, json=_summoner_data(puuid), headers=_RATE_HEADERS
            )

        config = _make_config(tmp_path, batch_size=2)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("summoners_raw", "puuid", str(tmp_path / "bronze"))

        original_write = writer.write_batch
        write_calls = []

        def tracking_write(*args, **kwargs):
            write_calls.append(kwargs.get("records") or args[0])
            return original_write(*args, **kwargs)

        writer.write_batch = tracking_write

        puuids = [f"p{i}" for i in range(5)]
        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_summoners(client, puuids, config, writer)

        assert len(write_calls) == 3


# ---------------------------------------------------------------------------
# Match IDs tests
# ---------------------------------------------------------------------------


def _match_id_response(puuid: str) -> list[str]:
    return [f"KR_{puuid}_1", f"KR_{puuid}_2"]


def _match_detail_data(match_id: str) -> dict:
    return {"metadata": {"matchId": match_id}, "info": {"gameDuration": 1800}}


def _match_timeline_data(match_id: str) -> dict:
    return {"metadata": {"matchId": match_id}, "info": {"frames": []}}


class TestExtractMatchIds:
    """Tests for extract_match_ids."""

    @pytest.mark.asyncio
    async def test_correct_api_path(self, tmp_path, make_mock_transport):
        """Verify the correct Match-V5 IDs path is called."""
        called_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            called_paths.append(str(request.url))
            return httpx.Response(
                200,
                json=["KR_1", "KR_2"],
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("match_ids_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_ids(client, ["p1"], config, writer)

        assert any(
            "/lol/match/v5/matches/by-puuid/p1/ids" in p for p in called_paths
        )

    @pytest.mark.asyncio
    async def test_returns_flattened_match_ids(self, tmp_path, make_mock_transport):
        """Mock returns array per puuid, verify all IDs returned flattened."""

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            puuid = url.split("/by-puuid/")[1].split("/ids")[0]
            return httpx.Response(
                200,
                json=_match_id_response(puuid),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("match_ids_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            result = await extract_match_ids(client, ["p1", "p2"], config, writer)

        assert set(result) == {"KR_p1_1", "KR_p1_2", "KR_p2_1", "KR_p2_2"}
        assert len(result) == 4

    @pytest.mark.asyncio
    async def test_anti_join_skips_existing_but_returns_their_match_ids(
        self, tmp_path, make_mock_transport
    ):
        """Pre-populated puuid is skipped by anti-join but its match IDs are still returned."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            url = str(request.url)
            puuid = url.split("/by-puuid/")[1].split("/ids")[0]
            return httpx.Response(
                200,
                json=_match_id_response(puuid),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("match_ids_raw", "puuid", str(tmp_path / "bronze"))

        # Pre-populate existing1 with known match IDs
        import json as _json

        writer.write_batch(
            records=[
                {
                    "puuid": "existing1",
                    "raw_json": _json.dumps(["KR_existing1_1", "KR_existing1_2"]),
                }
            ],
            endpoint="/seed",
            status_code=200,
            region="kr",
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            result = await extract_match_ids(
                client, ["existing1", "new1"], config, writer
            )

        # Only new1 should have been fetched from the API
        assert call_count == 1
        # But all match IDs should be returned (existing + new)
        assert "KR_existing1_1" in result
        assert "KR_existing1_2" in result
        assert "KR_new1_1" in result
        assert "KR_new1_2" in result

    @pytest.mark.asyncio
    async def test_bronze_table_written(self, tmp_path, make_mock_transport):
        """Verify match_ids_raw table contains one row per puuid."""
        import polars as pl
        from deltalake import DeltaTable

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, json=["KR_1", "KR_2"], headers=_RATE_HEADERS
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter("match_ids_raw", "puuid", str(tmp_path / "bronze"))

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_ids(client, ["p1", "p2"], config, writer)

        dt = DeltaTable(str(tmp_path / "bronze" / "match_ids_raw"))
        df = pl.DataFrame(dt.to_pyarrow_table())
        assert len(df) == 2
        assert set(df["puuid"].to_list()) == {"p1", "p2"}


# ---------------------------------------------------------------------------
# Match details tests
# ---------------------------------------------------------------------------


class TestExtractMatchDetails:
    """Tests for extract_match_details."""

    @pytest.mark.asyncio
    async def test_correct_api_path(self, tmp_path, make_mock_transport):
        """Verify the correct Match-V5 details path is called."""
        called_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            called_paths.append(str(request.url))
            return httpx.Response(
                200,
                json=_match_detail_data("KR_1"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter(
            "match_details_raw", "match_id", str(tmp_path / "bronze")
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_details(client, ["KR_1"], config, writer)

        assert any("/lol/match/v5/matches/KR_1" in p for p in called_paths)

    @pytest.mark.asyncio
    async def test_anti_join_skips_existing(self, tmp_path, make_mock_transport):
        """Pre-populated match_id is skipped."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                200,
                json=_match_detail_data("KR_new"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter(
            "match_details_raw", "match_id", str(tmp_path / "bronze")
        )

        # Pre-populate
        import json as _json

        writer.write_batch(
            records=[
                {"match_id": "KR_existing", "raw_json": _json.dumps({"matchId": "KR_existing"})}
            ],
            endpoint="/seed",
            status_code=200,
            region="kr",
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_details(
                client, ["KR_existing", "KR_new"], config, writer
            )

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_batch_size_honored(self, tmp_path, make_mock_transport):
        """5 match_ids with batch_size=2 → 3 write_batch calls."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=_match_detail_data("KR_x"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path, batch_size=2)
        transport = make_mock_transport(handler)
        writer = BronzeWriter(
            "match_details_raw", "match_id", str(tmp_path / "bronze")
        )

        original_write = writer.write_batch
        write_calls = []

        def tracking_write(*args, **kwargs):
            write_calls.append(kwargs.get("records") or args[0])
            return original_write(*args, **kwargs)

        writer.write_batch = tracking_write

        match_ids = [f"KR_{i}" for i in range(5)]
        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_details(client, match_ids, config, writer)

        assert len(write_calls) == 3


# ---------------------------------------------------------------------------
# Match timelines tests
# ---------------------------------------------------------------------------


class TestExtractMatchTimelines:
    """Tests for extract_match_timelines."""

    @pytest.mark.asyncio
    async def test_correct_api_path(self, tmp_path, make_mock_transport):
        """Verify the correct Match-V5 timeline path is called."""
        called_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            called_paths.append(str(request.url))
            return httpx.Response(
                200,
                json=_match_timeline_data("KR_1"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter(
            "match_timelines_raw", "match_id", str(tmp_path / "bronze")
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_timelines(client, ["KR_1"], config, writer)

        assert any(
            "/lol/match/v5/matches/KR_1/timeline" in p for p in called_paths
        )

    @pytest.mark.asyncio
    async def test_anti_join_skips_existing(self, tmp_path, make_mock_transport):
        """Pre-populated match_id is skipped."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                200,
                json=_match_timeline_data("KR_new"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path)
        transport = make_mock_transport(handler)
        writer = BronzeWriter(
            "match_timelines_raw", "match_id", str(tmp_path / "bronze")
        )

        # Pre-populate
        import json as _json

        writer.write_batch(
            records=[
                {"match_id": "KR_existing", "raw_json": _json.dumps({"matchId": "KR_existing"})}
            ],
            endpoint="/seed",
            status_code=200,
            region="kr",
        )

        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_timelines(
                client, ["KR_existing", "KR_new"], config, writer
            )

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_batch_size_honored(self, tmp_path, make_mock_transport):
        """5 match_ids with batch_size=2 → 3 write_batch calls."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=_match_timeline_data("KR_x"),
                headers=_RATE_HEADERS,
            )

        config = _make_config(tmp_path, batch_size=2)
        transport = make_mock_transport(handler)
        writer = BronzeWriter(
            "match_timelines_raw", "match_id", str(tmp_path / "bronze")
        )

        original_write = writer.write_batch
        write_calls = []

        def tracking_write(*args, **kwargs):
            write_calls.append(kwargs.get("records") or args[0])
            return original_write(*args, **kwargs)

        writer.write_batch = tracking_write

        match_ids = [f"KR_{i}" for i in range(5)]
        async with RiotRateLimiter("test-key", transport=transport) as client:
            await extract_match_timelines(client, match_ids, config, writer)

        assert len(write_calls) == 3
