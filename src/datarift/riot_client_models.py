"""Data models and header parsing for Riot API rate limiting."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RateWindow:
    """A single rate-limit window (e.g. 20 calls per 1 second)."""

    calls: int
    seconds: int
    count: int = 0
    reset_at: float | None = None


@dataclass
class RateLimitState:
    """Aggregate rate-limit state across app-level and per-method windows."""

    app_windows: list[RateWindow] = field(default_factory=list)
    method_windows: dict[str, list[RateWindow]] = field(default_factory=dict)


class RetryExhaustedError(Exception):
    """All retry attempts exhausted for a request."""

    def __init__(
        self,
        endpoint: str,
        attempts: int,
        last_status: int,
        body_snippet: str = "",
    ) -> None:
        self.endpoint = endpoint
        self.attempts = attempts
        self.last_status = last_status
        self.body_snippet = body_snippet
        super().__init__(
            f"Retry exhausted for {endpoint} after {attempts} attempts "
            f"(last status {last_status})"
        )


class NonJsonResponseError(Exception):
    """Server returned 200 but content-type is not JSON."""

    def __init__(
        self,
        endpoint: str,
        content_type: str,
        body_snippet: str = "",
    ) -> None:
        self.endpoint = endpoint
        self.content_type = content_type
        self.body_snippet = body_snippet
        super().__init__(
            f"Non-JSON response from {endpoint}: content-type={content_type}"
        )


def parse_rate_limit_header(header: str | None) -> list[RateWindow]:
    """Parse a Riot rate-limit header like ``"20:1,100:120"`` into RateWindow list.

    Returns an empty list for None, empty, or fully-malformed input.
    Individual malformed entries are silently skipped.
    """
    if not header:
        return []

    windows: list[RateWindow] = []
    for entry in header.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) != 2:
            continue
        try:
            calls = int(parts[0])
            seconds = int(parts[1])
        except ValueError:
            continue
        if calls <= 0 or seconds <= 0:
            continue
        windows.append(RateWindow(calls=calls, seconds=seconds))
    return windows


def parse_rate_count_header(header: str | None) -> list[int]:
    """Parse a Riot rate-count header like ``"5:1,50:120"`` extracting the count values.

    Each entry is ``count:seconds``; we extract the first element (current count).
    Returns an empty list for None, empty, or fully-malformed input.
    """
    if not header:
        return []

    counts: list[int] = []
    for entry in header.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) != 2:
            continue
        try:
            counts.append(int(parts[0]))
        except ValueError:
            continue
    return counts
