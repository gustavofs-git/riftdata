"""Shared test fixtures for datarift tests."""

import httpx
import pytest


@pytest.fixture()
def make_mock_transport():
    """Factory that creates an httpx.MockTransport from a handler function.

    Usage::

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        transport = make_mock_transport(handler)
    """

    def _factory(handler):
        return httpx.MockTransport(handler)

    return _factory


@pytest.fixture()
def sample_rate_limit_headers() -> dict[str, str]:
    """Typical Riot API rate-limit response headers."""
    return {
        "X-App-Rate-Limit": "20:1,100:120",
        "X-App-Rate-Limit-Count": "5:1,50:120",
        "X-Method-Rate-Limit": "500:10",
        "X-Method-Rate-Limit-Count": "3:10",
    }
