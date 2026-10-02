from __future__ import annotations

from datetime import datetime, timezone


def utc_iso(timestamp_s: float | None = None) -> str:
    if timestamp_s is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(timestamp_s, timezone.utc).isoformat()


def monotonic_s() -> float:
    import time

    return time.monotonic()
