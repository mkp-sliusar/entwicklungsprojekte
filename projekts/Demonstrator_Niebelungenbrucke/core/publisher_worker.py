from __future__ import annotations

import logging
import queue
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from core.aggregation import aggregate_samples, build_snapshot_payload
from core.data_store import DataStore, SensorSample

PublishFunc = Callable[[str, dict[str, Any], float], None]
StatusFunc = Callable[[], dict[str, Any]]


class DestinationWorker:
    """One non-blocking output worker for one destination.

    Sensors only put data into this worker's bounded queue. If the destination is
    slow or offline, old output items can be dropped, but sensor measurement
    threads continue at their own configured rates.
    """

    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        device_id: str,
        store: DataStore,
        publish_func: PublishFunc,
        status_func: StatusFunc | None,
        stop_event: threading.Event,
    ):
        self.name = name
        self.config = config
        self.device_id = device_id
        self.store = store
        self.publish_func = publish_func
        self.status_func = status_func
        self.stop_event = stop_event
        self.log = logging.getLogger(f"destination.{name}")

        self.payload_mode = str(config.get("payload", "per_sensor"))
        self.default_mode = str(config.get("mode", "aggregate"))
        self.default_rate_hz = float(config.get("rate_hz", config.get("publish_rate_hz", 1.0)))
        self.sensor_configs = config.get("sensors", {}) or {}
        self.queue_size = int(config.get("queue_size", 5000))
        self.queue: queue.Queue[SensorSample] = queue.Queue(maxsize=max(self.queue_size, 1))

        self.buffers: dict[str, list[SensorSample]] = defaultdict(list)
        self.latest: dict[str, SensorSample] = {}
        self.next_due: dict[str, float] = {}
        self.snapshot_next_due = time.monotonic()
        self.dropped = 0
        self.published = 0
        self.last_error = ""
        self.thread = threading.Thread(target=self._run, name=f"dest-{name}", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def join(self, timeout: float | None = None) -> None:
        self.thread.join(timeout=timeout)

    def stats(self) -> dict[str, Any]:
        return {
            "published": self.published,
            "dropped": self.dropped,
            "queue": self.queue.qsize(),
            "last_error": self.last_error,
        }

    def sensor_settings(self, sensor: str) -> dict[str, Any]:
        settings = {
            "enabled": True,
            "mode": self.default_mode,
            "rate_hz": self.default_rate_hz,
        }
        settings.update(self.sensor_configs.get(sensor, {}) or {})
        return settings

    def submit(self, sample: SensorSample) -> None:
        if self.payload_mode == "snapshot":
            return

        settings = self.sensor_settings(sample.sensor)
        if not settings.get("enabled", True):
            return

        try:
            self.queue.put_nowait(sample)
            return
        except queue.Full:
            pass

        # Drop oldest output item instead of blocking a sensor thread.
        try:
            self.queue.get_nowait()
            self.dropped += 1
        except queue.Empty:
            pass

        try:
            self.queue.put_nowait(sample)
        except queue.Full:
            self.dropped += 1

    def _run(self) -> None:
        while not self.stop_event.is_set():
            timeout = self._next_timeout_s()
            try:
                sample = self.queue.get(timeout=timeout)
                self._handle_sample(sample)
                self._drain_available()
            except queue.Empty:
                pass

            self._publish_due()

    def _drain_available(self, max_items: int = 1000) -> None:
        for _ in range(max_items):
            try:
                sample = self.queue.get_nowait()
            except queue.Empty:
                return
            self._handle_sample(sample)

    def _handle_sample(self, sample: SensorSample) -> None:
        settings = self.sensor_settings(sample.sensor)
        mode = str(settings.get("mode", self.default_mode))

        if mode == "raw":
            payload = sample.to_payload(self.device_id)
            payload["mode"] = "raw"
            self._safe_publish(sample.sensor, payload, sample.timestamp)
            return

        if mode == "latest":
            self.latest[sample.sensor] = sample
            return

        # Default: aggregate samples into windows.
        self.buffers[sample.sensor].append(sample)

    def _publish_due(self) -> None:
        now = time.monotonic()

        if self.payload_mode == "snapshot":
            rate_hz = max(float(self.config.get("rate_hz", 1.0)), 0.001)
            if now >= self.snapshot_next_due:
                status = self.status_func() if self.status_func else None
                payload = build_snapshot_payload(self.device_id, self.store.snapshot(), status)
                self._safe_publish("snapshot", payload, time.time())
                self.snapshot_next_due = now + (1.0 / rate_hz)
            return

        sensors = set(self.buffers) | set(self.latest)
        for sensor in list(sensors):
            settings = self.sensor_settings(sensor)
            if not settings.get("enabled", True):
                continue

            mode = str(settings.get("mode", self.default_mode))
            if mode == "raw":
                continue

            rate_hz = max(float(settings.get("rate_hz", self.default_rate_hz)), 0.001)
            due = self.next_due.setdefault(sensor, now + (1.0 / rate_hz))
            if now < due:
                continue

            if mode == "latest":
                sample = self.latest.pop(sensor, None)
                if sample is not None:
                    payload = sample.to_payload(self.device_id)
                    payload["mode"] = "latest"
                    self._safe_publish(sensor, payload, sample.timestamp)
            else:
                samples = self.buffers.get(sensor) or []
                if samples:
                    payload = aggregate_samples(self.device_id, sensor, samples)
                    self.buffers[sensor] = []
                    timestamp = samples[-1].timestamp
                    self._safe_publish(sensor, payload, timestamp)

            # Keep the worker stable even if the destination stalls.
            now2 = time.monotonic()
            next_due = due + (1.0 / rate_hz)
            if next_due < now2:
                next_due = now2 + (1.0 / rate_hz)
            self.next_due[sensor] = next_due

    def _safe_publish(self, sensor: str, payload: dict[str, Any], timestamp_s: float) -> None:
        try:
            self.publish_func(sensor, payload, timestamp_s)
            self.published += 1
            self.last_error = ""
        except Exception as exc:
            self.last_error = str(exc)
            self.log.exception("publish failed for %s", sensor)

    def _next_timeout_s(self) -> float:
        now = time.monotonic()
        due_times: list[float] = []

        if self.payload_mode == "snapshot":
            due_times.append(self.snapshot_next_due)
        else:
            sensors = set(self.buffers) | set(self.latest)
            for sensor in sensors:
                settings = self.sensor_settings(sensor)
                mode = str(settings.get("mode", self.default_mode))
                if mode == "raw":
                    continue
                rate_hz = max(float(settings.get("rate_hz", self.default_rate_hz)), 0.001)
                due_times.append(self.next_due.setdefault(sensor, now + (1.0 / rate_hz)))

        if not due_times:
            return 0.2
        return max(min(due_times) - now, 0.0)
