from __future__ import annotations

import logging
import queue
import threading
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

LOGGER = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _short_error(exc: BaseException | str, max_len: int = 500) -> str:
    text = str(exc) or (exc.__class__.__name__ if isinstance(exc, BaseException) else "error")
    text = text.replace("\n", " | ").strip()
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


@dataclass
class ModuleHealth:
    name: str
    kind: str = "module"
    enabled: bool = True
    running: bool = False
    healthy: bool = False
    state: str = "starting"
    started_at: Optional[str] = None
    stopped_at: Optional[str] = None
    last_ok_at: Optional[str] = None
    last_error_at: Optional[str] = None
    last_error: Optional[str] = None
    last_traceback: Optional[str] = None
    error_count: int = 0
    consecutive_errors: int = 0
    restart_count: int = 0
    dropped_count: int = 0
    queue_depth: Optional[int] = None
    queue_maxsize: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def public_dict(self, include_traceback: bool = False) -> Dict[str, Any]:
        data = asdict(self)
        if not include_traceback:
            data.pop("last_traceback", None)
        return data


class HealthRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._modules: Dict[str, ModuleHealth] = {}

    def ensure(self, name: str, kind: str = "module", enabled: bool = True) -> ModuleHealth:
        with self._lock:
            health = self._modules.get(name)
            if health is None:
                health = ModuleHealth(name=name, kind=kind, enabled=enabled)
                if not enabled:
                    health.state = "disabled"
                    health.healthy = True
                self._modules[name] = health
            else:
                health.kind = kind or health.kind
                health.enabled = enabled
            return health

    def update(self, name: str, kind: str = "module", **fields: Any) -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            for key, value in fields.items():
                if key == "extra" and isinstance(value, dict):
                    health.extra.update(value)
                elif hasattr(health, key):
                    setattr(health, key, value)
                else:
                    health.extra[key] = value

    def mark_starting(self, name: str, kind: str = "module") -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            health.running = True
            health.healthy = False
            health.state = "starting"
            health.started_at = health.started_at or utc_now_iso()
            health.stopped_at = None

    def mark_stopped(self, name: str, kind: str = "module") -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            health.running = False
            health.state = "stopped" if health.enabled else "disabled"
            health.stopped_at = utc_now_iso()

    def record_ok(self, name: str, kind: str = "module", **extra: Any) -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            health.running = True
            health.healthy = True
            health.state = "running"
            health.last_ok_at = utc_now_iso()
            health.consecutive_errors = 0
            if extra:
                health.extra.update(extra)

    def record_error(
        self,
        name: str,
        exc: BaseException | str,
        kind: str = "module",
        state: str = "degraded",
        include_traceback: bool = True,
        **extra: Any,
    ) -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            health.running = True
            health.healthy = False
            health.state = state
            health.error_count += 1
            health.consecutive_errors += 1
            health.last_error_at = utc_now_iso()
            health.last_error = _short_error(exc)
            if include_traceback and isinstance(exc, BaseException):
                health.last_traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            if extra:
                health.extra.update(extra)

    def record_restart(self, name: str, kind: str = "module") -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            health.restart_count += 1
            health.state = "restarting"
            health.healthy = False

    def record_drop(self, name: str, count: int = 1, kind: str = "output") -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            health.dropped_count += count

    def set_queue(self, name: str, depth: int, maxsize: int, kind: str = "output") -> None:
        with self._lock:
            health = self.ensure(name, kind=kind)
            health.queue_depth = depth
            health.queue_maxsize = maxsize

    def snapshot(self, include_tracebacks: bool = False) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {name: health.public_dict(include_traceback=include_tracebacks) for name, health in sorted(self._modules.items())}

    def overall_status(self) -> str:
        with self._lock:
            enabled = [m for m in self._modules.values() if m.enabled]
            if not enabled:
                return "unknown"
            if all(m.healthy and m.state in {"running", "disabled"} for m in enabled):
                return "ok"
            return "degraded"


class TareState:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.requested = False
        self.active = False
        self.source: Optional[str] = None
        self.requested_at: Optional[str] = None
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_duration_s: Optional[float] = None
        self.last_offset: Optional[float] = None
        self.count = 0

    def request(self, source: str = "unknown") -> None:
        with self._lock:
            self.requested = True
            self.source = source
            self.requested_at = utc_now_iso()
            self.last_error = None

    def start(self, source: Optional[str] = None) -> None:
        with self._lock:
            self.requested = False
            self.active = True
            if source:
                self.source = source
            self.started_at = utc_now_iso()
            self.finished_at = None
            self.last_error = None

    def finish(self, offset: Optional[float] = None) -> None:
        with self._lock:
            finished = utc_now_iso()
            self.finished_at = finished
            self.active = False
            self.requested = False
            self.count += 1
            if offset is not None:
                self.last_offset = float(offset)
            if self.started_at:
                try:
                    start = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
                    end = datetime.fromisoformat(finished.replace("Z", "+00:00"))
                    self.last_duration_s = max(0.0, (end - start).total_seconds())
                except ValueError:
                    self.last_duration_s = None

    def fail(self, exc: BaseException | str) -> None:
        with self._lock:
            self.active = False
            self.requested = False
            self.finished_at = utc_now_iso()
            self.last_error = _short_error(exc)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "requested": self.requested,
                "active": self.active,
                "source": self.source,
                "requested_at": self.requested_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "last_error": self.last_error,
                "last_duration_s": self.last_duration_s,
                "last_offset": self.last_offset,
                "count": self.count,
            }


class CircuitBreaker:
    def __init__(self, name: str, health: Optional[HealthRegistry] = None, failure_threshold: int = 5, reset_timeout_s: float = 5.0, max_reset_timeout_s: float = 60.0, kind: str = "output") -> None:
        self.name = name
        self.health = health
        self.failure_threshold = max(1, int(failure_threshold))
        self.reset_timeout_s = max(0.1, float(reset_timeout_s))
        self.max_reset_timeout_s = max(self.reset_timeout_s, float(max_reset_timeout_s))
        self.kind = kind
        self._lock = threading.RLock()
        self._state = "closed"
        self._failure_count = 0
        self._opened_at: Optional[float] = None
        self._current_timeout_s = self.reset_timeout_s

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow(self) -> bool:
        with self._lock:
            if self._state != "open":
                return True
            assert self._opened_at is not None
            if time.monotonic() - self._opened_at >= self._current_timeout_s:
                self._state = "half_open"
                if self.health:
                    self.health.update(self.name, kind=self.kind, state="half_open")
                return True
            if self.health:
                self.health.update(self.name, kind=self.kind, state="circuit_open", healthy=False)
            return False

    def record_success(self) -> None:
        with self._lock:
            self._state = "closed"
            self._failure_count = 0
            self._opened_at = None
            self._current_timeout_s = self.reset_timeout_s

    def record_failure(self, exc: Optional[BaseException | str] = None) -> None:
        with self._lock:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold or self._state == "half_open":
                self._state = "open"
                self._opened_at = time.monotonic()
                self._current_timeout_s = min(self.max_reset_timeout_s, self._current_timeout_s * 2.0)
                if self.health:
                    self.health.record_error(self.name, exc or "circuit breaker opened", kind=self.kind, state="circuit_open")


class DropOldestQueue:
    def __init__(self, maxsize: int, name: str, health: Optional[HealthRegistry] = None, kind: str = "output") -> None:
        self.maxsize = max(1, int(maxsize))
        self.name = name
        self.health = health
        self.kind = kind
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=self.maxsize)
        self._dropped = 0
        self._lock = threading.Lock()
        if self.health:
            self.health.set_queue(self.name, 0, self.maxsize, kind=self.kind)

    @property
    def dropped(self) -> int:
        with self._lock:
            return self._dropped

    def qsize(self) -> int:
        return self._queue.qsize()

    def put(self, item: Any) -> bool:
        dropped_now = 0
        with self._lock:
            while True:
                try:
                    self._queue.put_nowait(item)
                    break
                except queue.Full:
                    try:
                        self._queue.get_nowait()
                        self._queue.task_done()
                        self._dropped += 1
                        dropped_now += 1
                    except queue.Empty:
                        continue
        if self.health:
            if dropped_now:
                self.health.record_drop(self.name, dropped_now, kind=self.kind)
            self.health.set_queue(self.name, self.qsize(), self.maxsize, kind=self.kind)
        return True

    def get(self, timeout: Optional[float] = None) -> Any:
        item = self._queue.get(timeout=timeout)
        if self.health:
            self.health.set_queue(self.name, self.qsize(), self.maxsize, kind=self.kind)
        return item

    def task_done(self) -> None:
        self._queue.task_done()


class SafeWorker:
    def __init__(self, name: str, target: Callable[[threading.Event], Any], health: HealthRegistry, stop_event: Optional[threading.Event] = None, kind: str = "module", restart: bool = True, min_restart_delay_s: float = 0.5, max_restart_delay_s: float = 30.0, logger: Optional[logging.Logger] = None) -> None:
        self.name = name
        self.target = target
        self.health = health
        self.stop_event = stop_event or threading.Event()
        self.kind = kind
        self.restart = restart
        self.min_restart_delay_s = max(0.0, float(min_restart_delay_s))
        self.max_restart_delay_s = max(self.min_restart_delay_s, float(max_restart_delay_s))
        self.logger = logger or LOGGER
        self.thread = threading.Thread(target=self._run, name=name, daemon=True)

    def start(self) -> "SafeWorker":
        self.thread.start()
        return self

    def stop(self, timeout: Optional[float] = None) -> None:
        self.stop_event.set()
        self.thread.join(timeout=timeout)

    def _sleep_or_stop(self, seconds: float) -> bool:
        return self.stop_event.wait(max(0.0, seconds))

    def _run(self) -> None:
        delay = self.min_restart_delay_s
        self.health.mark_starting(self.name, kind=self.kind)
        while not self.stop_event.is_set():
            try:
                self.health.record_ok(self.name, kind=self.kind)
                self.target(self.stop_event)
                if self.stop_event.is_set():
                    break
                raise RuntimeError("worker returned unexpectedly")
            except BaseException as exc:
                self.health.record_error(self.name, exc, kind=self.kind)
                self.logger.exception("worker %s failed", self.name)
                if not self.restart:
                    break
                self.health.record_restart(self.name, kind=self.kind)
                if self._sleep_or_stop(delay):
                    break
                delay = min(self.max_restart_delay_s, max(self.min_restart_delay_s, delay * 2.0))
        self.health.mark_stopped(self.name, kind=self.kind)


class SafeLoopWorker(SafeWorker):
    def __init__(self, name: str, iteration: Callable[[], Any], health: HealthRegistry, stop_event: Optional[threading.Event] = None, kind: str = "module", interval_s: float = 0.0, error_pause_s: float = 0.2, circuit_breaker: Optional[CircuitBreaker] = None, logger: Optional[logging.Logger] = None) -> None:
        self.iteration = iteration
        self.interval_s = max(0.0, float(interval_s))
        self.error_pause_s = max(0.0, float(error_pause_s))
        self.circuit_breaker = circuit_breaker
        super().__init__(name=name, target=self._loop, health=health, stop_event=stop_event, kind=kind, restart=False, logger=logger)

    def _loop(self, stop_event: threading.Event) -> None:
        self.health.record_ok(self.name, kind=self.kind)
        next_run = time.monotonic()
        while not stop_event.is_set():
            now = time.monotonic()
            if self.interval_s and now < next_run:
                if stop_event.wait(next_run - now):
                    break
            if self.circuit_breaker and not self.circuit_breaker.allow():
                self.health.update(self.name, kind=self.kind, state="circuit_open", healthy=False)
                stop_event.wait(min(1.0, max(0.1, self.circuit_breaker.reset_timeout_s)))
                next_run = time.monotonic() + self.interval_s
                continue
            try:
                self.iteration()
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()
                self.health.record_ok(self.name, kind=self.kind)
            except BaseException as exc:
                self.health.record_error(self.name, exc, kind=self.kind)
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure(exc)
                self.logger.exception("loop %s iteration failed", self.name)
                if self.error_pause_s and stop_event.wait(self.error_pause_s):
                    break
            next_run = time.monotonic() + self.interval_s
