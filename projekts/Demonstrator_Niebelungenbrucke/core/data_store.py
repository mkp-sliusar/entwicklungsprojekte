from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any
import time

from core.time_utils import utc_iso


@dataclass(slots=True)
class SensorSample:
    sensor: str
    timestamp: float
    data: dict[str, Any]

    def to_payload(self, device_id: str) -> dict[str, Any]:
        payload = {
            "device": device_id,
            "type": self.sensor,
            "sensor": self.sensor,
            "timestamp": utc_iso(self.timestamp),
        }
        payload.update(self.data)
        return payload


class RateMeter:
    def __init__(self, window_s: float = 1.0):
        self.window_s = max(float(window_s), 0.1)
        self._last_ts = time.time()
        self._count = 0
        self._rate_hz = 0.0
        self._lock = Lock()

    def tick(self, now_s: float | None = None) -> float:
        now_s = time.time() if now_s is None else now_s
        with self._lock:
            self._count += 1
            elapsed = now_s - self._last_ts
            if elapsed >= self.window_s:
                self._rate_hz = self._count / elapsed if elapsed > 0 else 0.0
                self._count = 0
                self._last_ts = now_s
            return self._rate_hz

    def get(self) -> float:
        with self._lock:
            return self._rate_hz


class DataStore:
    def __init__(self):
        self._lock = Lock()
        self._latest: dict[str, SensorSample] = {}

    def update(self, sample: SensorSample) -> None:
        with self._lock:
            self._latest[sample.sensor] = sample

    def get_latest(self, sensor: str) -> SensorSample | None:
        with self._lock:
            return self._latest.get(sensor)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            items = dict(self._latest)

        result: dict[str, Any] = {}
        for sensor, sample in items.items():
            payload = {
                "timestamp": utc_iso(sample.timestamp),
            }
            payload.update(sample.data)
            result[sensor] = payload
        return result
