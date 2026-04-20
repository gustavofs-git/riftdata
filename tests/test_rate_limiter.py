"""Tests for riot_client_models and RiotRateLimiter."""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from datarift.riot_client import RiotRateLimiter
from datarift.riot_client_models import (
    NonJsonResponseError,
    RateLimitState,
    RateWindow,
    RetryExhaustedError,
    parse_rate_count_header,
    parse_rate_limit_header,
)


# --- parse_rate_limit_header: valid inputs ---


class TestParseRateLimitHeaderValid:
    def test_single_window(self):
        result = parse_rate_limit_header("100:120")
        assert len(result) == 1
        assert result[0].calls == 100
        assert result[0].seconds == 120

    def test_multiple_windows(self):
        result = parse_rate_limit_header("20:1,100:120")
        assert len(result) == 2
        assert result[0].calls == 20
        assert result[0].seconds == 1
        assert result[1].calls == 100
        assert result[1].seconds == 120

    def test_many_windows(self):
        result = parse_rate_limit_header("20:1,100:120,500:600")
        assert len(result) == 3
        assert result[2].calls == 500
        assert result[2].seconds == 600

    def test_defaults(self):
        result = parse_rate_limit_header("20:1")
        assert result[0].count == 0
        assert result[0].reset_at is None


# --- parse_rate_limit_header: malformed / edge-case inputs ---


class TestParseRateLimitHeaderMalformed:
    def test_empty_string(self):
        assert parse_rate_limit_header("") == []

    def test_none(self):
        assert parse_rate_limit_header(None) == []

    def test_single_malformed_entry(self):
        assert parse_rate_limit_header("abc:def") == []

    def test_missing_colon(self):
        assert parse_rate_limit_header("100") == []

    def test_extra_whitespace(self):
        result = parse_rate_limit_header("  20 : 1 , 100 : 120  ")
        # int() handles surrounding whitespace on the parts
        assert len(result) == 2

    def test_zero_calls(self):
        assert parse_rate_limit_header("0:120") == []

    def test_zero_seconds(self):
        assert parse_rate_limit_header("20:0") == []

    def test_trailing_comma(self):
        result = parse_rate_limit_header("20:1,100:120,")
        assert len(result) == 2

    def test_mixed_valid_and_invalid(self):
        result = parse_rate_limit_header("20:1,bad:val,100:120")
        assert len(result) == 2
        assert result[0].calls == 20
        assert result[1].calls == 100


# --- parse_rate_count_header ---


class TestParseRateCountHeader:
    def test_valid_input(self):
        result = parse_rate_count_header("5:1,50:120")
        assert result == [5, 50]

    def test_single_entry(self):
        assert parse_rate_count_header("3:10") == [3]

    def test_empty(self):
        assert parse_rate_count_header("") == []

    def test_none(self):
        assert parse_rate_count_header(None) == []


# --- Data model instantiation ---


class TestModels:
    def test_rate_window_creation(self):
        w = RateWindow(calls=20, seconds=1)
        assert w.calls == 20
        assert w.seconds == 1
        assert w.count == 0
        assert w.reset_at is None

    def test_rate_window_with_state(self):
        w = RateWindow(calls=20, seconds=1, count=5, reset_at=1700000000.0)
        assert w.count == 5
        assert w.reset_at == 1700000000.0

    def test_rate_limit_state_defaults(self):
        state = RateLimitState()
        assert state.app_windows == []
        assert state.method_windows == {}

    def test_rate_limit_state_populated(self):
        state = RateLimitState(
            app_windows=[RateWindow(calls=20, seconds=1)],
            method_windows={"getSummoner": [RateWindow(calls=500, seconds=10)]},
        )
        assert len(state.app_windows) == 1
        assert "getSummoner" in state.method_windows


# --- Error classes ---


class TestErrors:
    def test_retry_exhausted_error_attributes(self):
        err = RetryExhaustedError(
            endpoint="/lol/summoner/v4",
            attempts=3,
            last_status=429,
            body_snippet='{"status":{"message":"Rate limit exceeded"}}',
        )
        assert err.endpoint == "/lol/summoner/v4"
        assert err.attempts == 3
        assert err.last_status == 429
        assert "Rate limit exceeded" in err.body_snippet
        assert "429" in str(err)

    def test_non_json_response_error_attributes(self):
        err = NonJsonResponseError(
            endpoint="/lol/match/v5",
            content_type="text/html",
            body_snippet="<html>Service Unavailable</html>",
        )
        assert err.endpoint == "/lol/match/v5"
        assert err.content_type == "text/html"
        assert "text/html" in str(err)


# ---------------------------------------------------------------------------
# Helper: build a mock transport for RiotRateLimiter tests
# ---------------------------------------------------------------------------


def _make_transport(
    status: int = 200,
    json_body: dict | None = None,
    headers: dict[str, str] | None = None,
    handler=None,
) -> httpx.MockTransport:
    """Create an httpx.MockTransport that returns a fixed response or uses a handler."""
    if handler is not None:
        return httpx.MockTransport(handler)

    resp_headers = dict(headers or {})
    # Default content-type to application/json for 200 responses
    if status == 200 and "content-type" not in {k.lower() for k in resp_headers}:
        resp_headers.setdefault("content-type", "application/json")
    import json as _json

    raw_body = _json.dumps(json_body).encode() if json_body is not None else b'{"ok":true}'

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, headers=resp_headers, content=raw_body)

    return httpx.MockTransport(_handler)


# ---------------------------------------------------------------------------
# RiotRateLimiter tests
# ---------------------------------------------------------------------------


class TestRiotRateLimiterBasicRequest:
    """Basic request passes through and returns response."""

    async def test_basic_request_passthrough(self):
        transport = _make_transport(
            status=200,
            headers={
                "X-App-Rate-Limit": "20:1,100:120",
                "X-App-Rate-Limit-Count": "1:1,1:120",
            },
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/lol/summoner/v4/by-name/test")
            assert resp.status_code == 200
            assert resp.json() == {"ok": True}

    async def test_auth_header_set(self):
        """The X-Riot-Token header is sent on every request."""
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["token"] = request.headers.get("X-Riot-Token")
            return httpx.Response(200, headers={"content-type": "application/json"}, content=b'{"ok":true}')

        transport = httpx.MockTransport(handler)
        async with RiotRateLimiter("my-secret-key", transport=transport) as limiter:
            await limiter.request("GET", "/test")
        assert captured["token"] == "my-secret-key"


class TestRiotRateLimiterThrottle:
    """App-level throttling — near-limit counts trigger preemptive delay."""

    async def test_throttle_when_near_limit(self):
        """When app count is at 80%+ of limit, limiter sleeps before next request."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            # First response puts us at 16/20 = 80% on the 1s window
            return httpx.Response(
                200,
                headers={
                    "content-type": "application/json",
                    "X-App-Rate-Limit": "20:1,100:120",
                    "X-App-Rate-Limit-Count": "16:1,10:120",
                },
                content=b'{"n":1}',
            )

        transport = httpx.MockTransport(handler)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            # First request — updates windows to 16/20
            await limiter.request("GET", "/lol/summoner/v4/test")

            # Second request should be delayed because 16/20 >= 80%
            t0 = time.monotonic()
            await limiter.request("GET", "/lol/summoner/v4/test2")
            elapsed = time.monotonic() - t0

            # Should have waited ~1 second (the window duration)
            assert elapsed >= 0.5, f"Expected throttle delay, got {elapsed:.3f}s"
            assert call_count == 2


class TestRiotRateLimiterMethodTracking:
    """Method-level tracking — different endpoints tracked independently."""

    async def test_independent_method_windows(self):
        def handler(request: httpx.Request) -> httpx.Response:
            path = str(request.url.raw_path, "utf-8")
            if "summoner" in path:
                return httpx.Response(
                    200,
                    headers={
                        "content-type": "application/json",
                        "X-App-Rate-Limit": "20:1,100:120",
                        "X-App-Rate-Limit-Count": "1:1,1:120",
                        "X-Method-Rate-Limit": "500:10",
                        "X-Method-Rate-Limit-Count": "1:10",
                    },
                    content=b'{"summoner":true}',
                )
            else:
                return httpx.Response(
                    200,
                    headers={
                        "content-type": "application/json",
                        "X-App-Rate-Limit": "20:1,100:120",
                        "X-App-Rate-Limit-Count": "2:1,2:120",
                        "X-Method-Rate-Limit": "100:60",
                        "X-Method-Rate-Limit-Count": "1:60",
                    },
                    content=b'{"match":true}',
                )

        transport = httpx.MockTransport(handler)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            r1 = await limiter.request("GET", "/lol/summoner/v4/by-name/x")
            r2 = await limiter.request("GET", "/lol/match/v5/by-puuid/y")

            assert r1.json() == {"summoner": True}
            assert r2.json() == {"match": True}

            # Verify independent method windows exist
            assert "lol.summoner.v4" in limiter._method_windows
            assert "lol.match.v5" in limiter._method_windows
            assert limiter._method_windows["lol.summoner.v4"][0].calls == 500
            assert limiter._method_windows["lol.match.v5"][0].calls == 100


class TestRiotRateLimiterConcurrency:
    """Concurrent requests gated by semaphore."""

    async def test_semaphore_limits_concurrency(self):
        """Fire 15 concurrent requests, verify max 10 are in-flight at once."""
        max_concurrent_seen = 0
        current_in_flight = 0
        lock = asyncio.Lock()

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal max_concurrent_seen, current_in_flight
            async with lock:
                current_in_flight += 1
                if current_in_flight > max_concurrent_seen:
                    max_concurrent_seen = current_in_flight
            # Simulate work
            await asyncio.sleep(0.05)
            async with lock:
                current_in_flight -= 1
            return httpx.Response(200, headers={"content-type": "application/json"}, content=b'{"ok":true}')

        transport = httpx.MockTransport(handler)
        async with RiotRateLimiter("fake-key", max_concurrent=10, transport=transport) as limiter:
            tasks = [
                asyncio.create_task(limiter.request("GET", f"/test/{i}"))
                for i in range(15)
            ]
            await asyncio.gather(*tasks)

        assert max_concurrent_seen <= 10, f"Expected max 10 concurrent, saw {max_concurrent_seen}"
        assert max_concurrent_seen >= 5, f"Concurrency too low: {max_concurrent_seen}"


class TestRiotRateLimiterDynamicUpdate:
    """Dynamic header update — window state updates after response."""

    async def test_dynamic_window_update(self):
        transport = _make_transport(
            status=200,
            headers={
                "X-App-Rate-Limit": "50:2,200:60",
                "X-App-Rate-Limit-Count": "10:2,30:60",
            },
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            # Before: seeded defaults 20:1, 100:120
            assert limiter._app_windows[0].calls == 20
            assert limiter._app_windows[1].calls == 100

            await limiter.request("GET", "/lol/summoner/v4/test")

            # After: updated from response headers
            assert limiter._app_windows[0].calls == 50
            assert limiter._app_windows[0].seconds == 2
            assert limiter._app_windows[0].count == 10
            assert limiter._app_windows[1].calls == 200
            assert limiter._app_windows[1].seconds == 60
            assert limiter._app_windows[1].count == 30


class TestRiotRateLimiterMissingHeaders:
    """Missing headers — limiter continues with seeded defaults."""

    async def test_missing_headers_keeps_defaults(self):
        transport = _make_transport(status=200, headers={})
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            await limiter.request("GET", "/lol/summoner/v4/test")

            # App windows should still be the seeded defaults
            assert len(limiter._app_windows) == 2
            assert limiter._app_windows[0].calls == 20
            assert limiter._app_windows[1].calls == 100

            # No method windows should have been created
            assert "lol.summoner.v4" not in limiter._method_windows


class TestRiotRateLimiterBootstrap:
    """Bootstrap case — first request to a new method with no prior limits."""

    async def test_first_method_request_allowed(self):
        """First request to an unknown method proceeds without blocking."""
        transport = _make_transport(
            status=200,
            headers={
                "X-App-Rate-Limit": "20:1,100:120",
                "X-App-Rate-Limit-Count": "1:1,1:120",
                "X-Method-Rate-Limit": "250:10",
                "X-Method-Rate-Limit-Count": "1:10",
            },
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            # No prior method windows
            assert "lol.champion.v4" not in limiter._method_windows

            resp = await limiter.request("GET", "/lol/champion/v4/rotations")
            assert resp.status_code == 200

            # Now method windows are learned
            assert "lol.champion.v4" in limiter._method_windows
            assert limiter._method_windows["lol.champion.v4"][0].calls == 250


class TestRiotRateLimiterNegative:
    """Negative tests: malformed inputs, boundary conditions."""

    async def test_count_exceeding_limit_triggers_wait(self):
        """If count > limit (server-side anomaly), should still trigger throttle."""
        transport = _make_transport(
            status=200,
            headers={
                "X-App-Rate-Limit": "10:1",
                "X-App-Rate-Limit-Count": "15:1",  # count exceeds limit
            },
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            await limiter.request("GET", "/lol/summoner/v4/test")

            # 15/10 = 150% >= 80%, should trigger wait
            wait = limiter._should_wait(limiter._app_windows)
            assert wait > 0

    async def test_exactly_at_limit_triggers_wait(self):
        """Exactly at limit count = calls, should wait."""
        transport = _make_transport(
            status=200,
            headers={
                "X-App-Rate-Limit": "10:2",
                "X-App-Rate-Limit-Count": "10:2",
            },
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            await limiter.request("GET", "/lol/summoner/v4/test")
            wait = limiter._should_wait(limiter._app_windows)
            assert wait > 0

    async def test_one_under_limit_proceeds(self):
        """count = 7 out of 10 = 70% < 80%, should proceed without wait."""
        transport = _make_transport(
            status=200,
            headers={
                "X-App-Rate-Limit": "10:2",
                "X-App-Rate-Limit-Count": "7:2",
            },
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            await limiter.request("GET", "/lol/summoner/v4/test")
            wait = limiter._should_wait(limiter._app_windows)
            assert wait == 0.0

    async def test_headers_with_zero_values_ignored(self):
        """Zero-value headers are silently skipped by parser; defaults remain."""
        transport = _make_transport(
            status=200,
            headers={
                "X-App-Rate-Limit": "0:0",
                "X-App-Rate-Limit-Count": "0:0",
            },
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            await limiter.request("GET", "/lol/summoner/v4/test")
            # Zeros are filtered by parser, so defaults remain
            assert limiter._app_windows[0].calls == 20


class TestRiotRateLimiterContextManager:
    """Async context manager support."""

    async def test_context_manager_lifecycle(self):
        transport = _make_transport(status=200)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            assert limiter is not None
            resp = await limiter.request("GET", "/test")
            assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Retry loop tests
# ---------------------------------------------------------------------------


def _sequence_transport(responses: list[tuple[int, dict[str, str], bytes]]):
    """Build a MockTransport that returns responses in order."""
    call_idx = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_idx
        idx = min(call_idx, len(responses) - 1)
        call_idx += 1
        status, headers, body = responses[idx]
        return httpx.Response(status, headers=headers, content=body)

    return httpx.MockTransport(handler)


class TestRetry429:
    """429 handling: honors Retry-After, adds jitter, retries."""

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_429_with_retry_after_then_success(self, mock_sleep):
        transport = _sequence_transport([
            (429, {"Retry-After": "2"}, b'{"status":{"message":"Rate limit"}}'),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/lol/summoner/v4/test")
            assert resp.status_code == 200
        # Sleep was called for the 429 retry (Retry-After + jitter)
        assert mock_sleep.await_count >= 1
        sleep_val = mock_sleep.await_args_list[0].args[0]
        assert 2.1 <= sleep_val <= 2.5  # 2s + 0.1-0.5 jitter

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_429_without_retry_after_uses_default(self, mock_sleep):
        transport = _sequence_transport([
            (429, {}, b"rate limited"),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/lol/summoner/v4/test")
            assert resp.status_code == 200
        sleep_val = mock_sleep.await_args_list[0].args[0]
        assert 1.1 <= sleep_val <= 1.5  # default 1.0 + jitter

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_429_non_numeric_retry_after_uses_default(self, mock_sleep):
        transport = _sequence_transport([
            (429, {"Retry-After": "not-a-number"}, b""),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/test")
            assert resp.status_code == 200
        sleep_val = mock_sleep.await_args_list[0].args[0]
        assert 1.1 <= sleep_val <= 1.5


class TestRetry5xx:
    """5xx handling: exponential backoff, cap at 60s, max 5 attempts."""

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_5xx_backoff_then_success(self, mock_sleep):
        transport = _sequence_transport([
            (503, {}, b"Service Unavailable"),
            (502, {}, b"Bad Gateway"),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/lol/summoner/v4/test")
            assert resp.status_code == 200
        # Backoff: attempt 1 -> 1s, attempt 2 -> 2s
        assert mock_sleep.await_args_list[0].args[0] == 1.0
        assert mock_sleep.await_args_list[1].args[0] == 2.0

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_5xx_exhaust_all_retries(self, mock_sleep):
        transport = _sequence_transport([
            (500, {}, b"error1"),
            (500, {}, b"error2"),
            (500, {}, b"error3"),
            (500, {}, b"error4"),
            (500, {}, b"error5"),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(RetryExhaustedError) as exc_info:
                await limiter.request("GET", "/lol/summoner/v4/test")
            err = exc_info.value
            assert err.endpoint == "/lol/summoner/v4/test"
            assert err.attempts == 5
            assert err.last_status == 500
            assert err.body_snippet == "error5"

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_backoff_capped_at_60s(self, mock_sleep):
        """Backoff should not exceed 60s even at attempt 5 (base*2^4 = 16s, but test large base)."""
        transport = _sequence_transport([
            (500, {}, b"e1"),
            (500, {}, b"e2"),
            (500, {}, b"e3"),
            (500, {}, b"e4"),
            (500, {}, b"e5"),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(RetryExhaustedError):
                await limiter.request("GET", "/test")
        # All backoff values should be <= 60
        for call in mock_sleep.await_args_list:
            assert call.args[0] <= 60.0

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_exactly_5_retries_then_6th_not_attempted(self, mock_sleep):
        """Max 5 attempts — a 6th response should never be reached."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                return httpx.Response(500, content=b"fail")
            return httpx.Response(200, headers={"content-type": "application/json"}, content=b'{"ok":true}')

        transport = httpx.MockTransport(handler)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(RetryExhaustedError):
                await limiter.request("GET", "/test")
        assert call_count == 5  # 6th never attempted


class TestRetryAuth:
    """401/403 — immediate failure, no retry."""

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_401_immediate_failure(self, mock_sleep):
        transport = _make_transport(status=401)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(RetryExhaustedError) as exc_info:
                await limiter.request("GET", "/lol/summoner/v4/test")
            err = exc_info.value
            assert err.last_status == 401
            assert err.attempts == 1
        # No retry sleep should have been called
        mock_sleep.assert_not_awaited()

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_403_immediate_failure(self, mock_sleep):
        transport = _make_transport(status=403)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(RetryExhaustedError) as exc_info:
                await limiter.request("GET", "/test")
            assert exc_info.value.last_status == 403
        mock_sleep.assert_not_awaited()

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_401_midrun_no_retry(self, mock_sleep):
        """401 after a successful request still fails immediately."""
        transport = _sequence_transport([
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
            (401, {}, b"Unauthorized"),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/lol/summoner/v4/test")
            assert resp.status_code == 200
            with pytest.raises(RetryExhaustedError) as exc_info:
                await limiter.request("GET", "/lol/summoner/v4/test2")
            assert exc_info.value.last_status == 401


class TestContentTypeGuard:
    """Non-JSON 200 responses are retried then raise NonJsonResponseError."""

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_non_json_200_retried_then_raises(self, mock_sleep):
        html_body = b"<html>Service Unavailable</html>"
        transport = _sequence_transport([
            (200, {"content-type": "text/html"}, html_body),
            (200, {"content-type": "text/html"}, html_body),
            (200, {"content-type": "text/html"}, html_body),
            (200, {"content-type": "text/html"}, html_body),
            (200, {"content-type": "text/html"}, html_body),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(NonJsonResponseError) as exc_info:
                await limiter.request("GET", "/lol/match/v5/test")
            err = exc_info.value
            assert err.endpoint == "/lol/match/v5/test"
            assert err.content_type == "text/html"
            assert "<html>" in err.body_snippet

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_non_json_then_json_success(self, mock_sleep):
        transport = _sequence_transport([
            (200, {"content-type": "text/html"}, b"<html>oops</html>"),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/test")
            assert resp.status_code == 200
            assert resp.json() == {"ok": True}

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_empty_content_type_treated_as_non_json(self, mock_sleep):
        transport = _sequence_transport([
            (200, {"content-type": ""}, b"something"),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/test")
            assert resp.status_code == 200


class TestMixedRetryScenarios:
    """Mixed status codes across retries."""

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_429_then_503_then_success(self, mock_sleep):
        transport = _sequence_transport([
            (429, {"Retry-After": "1"}, b"rate limited"),
            (503, {}, b"Service Unavailable"),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            resp = await limiter.request("GET", "/lol/summoner/v4/test")
            assert resp.status_code == 200
            assert resp.json() == {"ok": True}


class TestPreflightCheck:
    """preflight_check validates API key against status endpoint."""

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_preflight_success(self, mock_sleep):
        transport = _make_transport(
            status=200,
            headers={"content-type": "application/json"},
        )
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            result = await limiter.preflight_check()
            assert result is True

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_preflight_403_raises_with_key_message(self, mock_sleep):
        transport = _make_transport(status=403)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(RetryExhaustedError) as exc_info:
                await limiter.preflight_check()
            err = exc_info.value
            assert err.last_status == 403
            assert "expired or invalid" in err.body_snippet

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_preflight_401_raises_with_key_message(self, mock_sleep):
        transport = _make_transport(status=401)
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            with pytest.raises(RetryExhaustedError) as exc_info:
                await limiter.preflight_check()
            assert exc_info.value.last_status == 401
            assert "expired or invalid" in exc_info.value.body_snippet

    @patch("datarift.riot_client.asyncio.sleep", new_callable=AsyncMock)
    async def test_preflight_5xx_retries_normally(self, mock_sleep):
        transport = _sequence_transport([
            (500, {}, b"error"),
            (200, {"content-type": "application/json"}, b'{"ok":true}'),
        ])
        async with RiotRateLimiter("fake-key", transport=transport) as limiter:
            result = await limiter.preflight_check()
            assert result is True
