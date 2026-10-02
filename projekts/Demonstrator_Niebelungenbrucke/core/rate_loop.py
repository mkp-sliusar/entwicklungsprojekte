from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable


class RateLoop:
    """Runs one callback at a fixed rate in its own thread.

    This is the only place where a scheduled wait is used. The wait belongs to
    the thread that owns this job, so a slow ADS read cannot block MPU, MQTT,
    InfluxDB, WebSocket or system monitoring.
    """

    def __init__(
        self,
        name: str,
        rate_hz: float,
        callback: Callable[[], None],
        stop_event: threading.Event,
        error_pause_s: float = 0.5,
    ):
        self.name = name
        self.rate_hz = float(rate_hz)
        self.callback = callback
        self.stop_event = stop_event
        self.error_pause_s = float(error_pause_s)
        self.thread = threading.Thread(target=self._run, name=name, daemon=True)
        self.log = logging.getLogger(name)

    def start(self) -> None:
        self.thread.start()

    def join(self, timeout: float | None = None) -> None:
        self.thread.join(timeout=timeout)

    def _run(self) -> None:
        if self.rate_hz <= 0:
            period_s = 0.0
        else:
            period_s = 1.0 / self.rate_hz

        next_run = time.monotonic()

        while not self.stop_event.is_set():
            try:
                self.callback()
            except Exception:
                self.log.exception("loop callback failed")
                self.stop_event.wait(self.error_pause_s)

            if period_s <= 0:
                self.stop_event.wait(0.001)
                continue

            next_run += period_s
            now = time.monotonic()

            if next_run < now:
                missed = int((now - next_run) / period_s) + 1
                next_run += missed * period_s

            wait_s = next_run - now
            if wait_s > 0:
                self.stop_event.wait(wait_s)
