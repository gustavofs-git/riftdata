"""Async HTTP client with dual-layer rate limiting for the Riot Games API."""

from __future__ import annotations

import asyncio
import random
import time

import httpx
import structlog

from datarift.riot_client_models import (
    NonJsonResponseError,
    RateWindow,
    RetryExhaustedError,
    parse_rate_count_header,
    parse_rate_limit_header,
)

logger = structlog.get_logger(__name__)

# Dev-key defaults: 20 req/1s, 100 req/2min
_DEFAULT_APP_LIMITS: list[tuple[int, int]] = [(20, 1), (100, 120)]


class RiotRateLimiter:
    """Wraps ``httpx.AsyncClient`` with dual-layer sliding-window rate limiting.

    Tracks both app-level and per-method rate-limit windows, gates
    concurrency via ``asyncio.Semaphore``, and preemptively delays
    requests approaching the limit threshold.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://kr.api.riotgames.com",
        max_concurrent: int = 10,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url
        self._semaphore = asyncio.Semaphore(max_concurrent)

        # App-level windows seeded with dev-key defaults
        self._app_windows: list[RateWindow] = [
            RateWindow(calls=calls, seconds=seconds)
            for calls, seconds in _DEFAULT_APP_LIMITS
        ]

        # Per-method windows: method_key -> list[RateWindow]
        self._method_windows: dict[str, list[RateWindow]] = {}

        client_kwargs: dict = {
            "base_url": base_url,
            "headers": {"X-Riot-Token": api_key},
        }
        if transport is not None:
            client_kwargs["transport"] = transport

        self._client = httpx.AsyncClient(**client_kwargs)

    # --- rate-limit helpers ---

    @staticmethod
    def _should_wait(windows: list[RateWindow]) -> float:
        """Return seconds to wait if any window is at or above 80% capacity, else 0.0."""
        now = time.monotonic()
        max_wait = 0.0
        for w in windows:
            if w.calls == 0:
                continue
            usage = w.count / w.calls
            if usage >= 0.8:
                if w.reset_at is not None and w.reset_at > now:
                    wait = w.reset_at - now
                else:
                    # No reset time known — wait the full window duration
                    wait = float(w.seconds)
                max_wait = max(max_wait, wait)
        return max_wait

    def _update_windows_from_headers(
        self, response: httpx.Response, method_key: str
    ) -> None:
        """Update app and method windows from Riot rate-limit response headers."""
        now = time.monotonic()

        # --- App-level ---
        app_limit_hdr = response.headers.get("X-App-Rate-Limit")
        app_count_hdr = response.headers.get("X-App-Rate-Limit-Count")

        if app_limit_hdr:
            parsed_limits = parse_rate_limit_header(app_limit_hdr)
            parsed_counts = parse_rate_count_header(app_count_hdr)
            if parsed_limits:
                for i, w in enumerate(parsed_limits):
                    w.count = parsed_counts[i] if i < len(parsed_counts) else 0
                    w.reset_at = now + w.seconds
                self._app_windows = parsed_limits
                logger.debug(
                    "rate_limit.app_updated",
                    windows=[(w.calls, w.seconds, w.count) for w in parsed_limits],
                )

        # --- Method-level ---
        method_limit_hdr = response.headers.get("X-Method-Rate-Limit")
        method_count_hdr = response.headers.get("X-Method-Rate-Limit-Count")

        if method_limit_hdr:
            parsed_limits = parse_rate_limit_header(method_limit_hdr)
            parsed_counts = parse_rate_count_header(method_count_hdr)
            if parsed_limits:
                for i, w in enumerate(parsed_limits):
                    w.count = parsed_counts[i] if i < len(parsed_counts) else 0
                    w.reset_at = now + w.seconds
                self._method_windows[method_key] = parsed_limits
                logger.debug(
                    "rate_limit.method_updated",
                    method_key=method_key,
                    windows=[(w.calls, w.seconds, w.count) for w in parsed_limits],
                )

    @staticmethod
    def _method_key_from_path(path: str) -> str:
        """Derive a method key from a request path.

        Uses the first three path segments (e.g. ``/lol/summoner/v4/...`` -> ``lol.summoner.v4``).
        """
        parts = [p for p in path.strip("/").split("/") if p]
        return ".".join(parts[:3]) if parts else "unknown"

    # --- public API ---

    _MAX_RETRIES: int = 5
    _BACKOFF_BASE: float = 1.0
    _BACKOFF_CAP: float = 60.0

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Send a rate-limited request with retry, backoff, and content-type guard."""
        method_key = self._method_key_from_path(path)
        last_status = 0
        last_body = ""

        for attempt in range(1, self._MAX_RETRIES + 1):
            async with self._semaphore:
                # Check app-level wait
                wait = self._should_wait(self._app_windows)
                # Check method-level wait
                method_wins = self._method_windows.get(method_key, [])
                method_wait = self._should_wait(method_wins)
                wait = max(wait, method_wait)

                if wait > 0:
                    logger.info(
                        "rate_limit.throttle",
                        method_key=method_key,
                        wait_seconds=round(wait, 3),
                    )
                    await asyncio.sleep(wait)

                response = await self._client.request(method, path, **kwargs)
                self._update_windows_from_headers(response, method_key)

            last_status = response.status_code
            last_body = response.text[:500]

            # 401/403 — raise immediately, never retry
            if response.status_code in (401, 403):
                logger.error(
                    "auth_failure",
                    endpoint=path,
                    status=response.status_code,
                )
                raise RetryExhaustedError(
                    endpoint=path,
                    attempts=1,
                    last_status=response.status_code,
                    body_snippet=last_body,
                )

            # 429 — honor Retry-After header
            if response.status_code == 429:
                raw_retry = response.headers.get("Retry-After", "")
                try:
                    retry_after = float(raw_retry)
                except (ValueError, TypeError):
                    retry_after = 1.0
                jitter = random.uniform(0.1, 0.5)
                sleep_time = retry_after + jitter
                logger.warning(
                    "rate_limit_retry",
                    endpoint=path,
                    retry_after=retry_after,
                    jitter=round(jitter, 3),
                    attempt=attempt,
                )
                await asyncio.sleep(sleep_time)
                continue

            # 5xx — exponential backoff
            if response.status_code >= 500:
                backoff = min(
                    self._BACKOFF_BASE * (2 ** (attempt - 1)), self._BACKOFF_CAP
                )
                logger.warning(
                    "server_error_retry",
                    endpoint=path,
                    status=response.status_code,
                    backoff=backoff,
                    attempt=attempt,
                )
                await asyncio.sleep(backoff)
                continue

            # 200-level — content-type guard
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "")
                if "application/json" not in content_type:
                    logger.warning(
                        "non_json_response",
                        endpoint=path,
                        content_type=content_type,
                        body_preview=response.text[:200],
                        attempt=attempt,
                    )
                    # Retry under 5xx backoff policy
                    backoff = min(
                        self._BACKOFF_BASE * (2 ** (attempt - 1)), self._BACKOFF_CAP
                    )
                    await asyncio.sleep(backoff)
                    continue

            # Success — return the response
            return response

        # All retries exhausted
        # If the last failure was non-JSON 200, raise NonJsonResponseError
        if last_status == 200:
            content_type = response.headers.get("content-type", "")
            raise NonJsonResponseError(
                endpoint=path,
                content_type=content_type,
                body_snippet=last_body,
            )

        logger.error(
            "retry_exhausted",
            endpoint=path,
            attempts=self._MAX_RETRIES,
            last_status=last_status,
        )
        raise RetryExhaustedError(
            endpoint=path,
            attempts=self._MAX_RETRIES,
            last_status=last_status,
            body_snippet=last_body,
        )

    async def preflight_check(self, region: str = "kr") -> bool:
        """Validate API key by hitting the platform status endpoint.

        Returns True on success. Raises RetryExhaustedError with a clear
        message if the key is expired/invalid (401/403).
        """
        try:
            resp = await self.request("GET", "/lol/status/v4/platform-data")
            logger.info("preflight_ok", region=region, status=resp.status_code)
            return True
        except RetryExhaustedError as exc:
            if exc.last_status in (401, 403):
                logger.error(
                    "preflight_failed",
                    region=region,
                    status=exc.last_status,
                    reason="API key expired or invalid",
                )
                raise RetryExhaustedError(
                    endpoint="/lol/status/v4/platform-data",
                    attempts=exc.attempts,
                    last_status=exc.last_status,
                    body_snippet="API key expired or invalid",
                ) from exc
            raise

    # --- context manager ---

    async def __aenter__(self) -> RiotRateLimiter:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._client.aclose()
