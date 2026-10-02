from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from .safe_runtime import HealthRegistry, utc_now_iso

LOGGER = logging.getLogger(__name__)


class DRDYTimeoutError(TimeoutError):
    """Raised when ADS1220 DRDY does not become active before timeout."""


@dataclass
class ADS1220BootConfig:
    startup_delay_s: float = 8.0
    init_retries: int = 8
    init_retry_delay_s: float = 1.0
    init_retry_backoff: float = 1.5
    max_init_retry_delay_s: float = 8.0
    drdy_active_low: bool = True
    drdy_startup_timeout_s: float = 3.0
    drdy_read_timeout_s: float = 1.0
    drdy_poll_interval_s: float = 0.002
    drdy_consecutive_timeout_limit: int = 3
    delay_after_reset_s: float = 0.20
    discard_first_samples_after_init: int = 3
    fail_service_on_ads_startup: bool = False

    @classmethod
    def from_mapping(cls, cfg: Mapping[str, Any]) -> "ADS1220BootConfig":
        boot = cfg.get("boot_guard", {}) if isinstance(cfg.get("boot_guard", {}), Mapping) else {}
        drdy = cfg.get("drdy", {}) if isinstance(cfg.get("drdy", {}), Mapping) else {}

        def get(name: str, default: Any) -> Any:
            if name in boot:
                return boot[name]
            if name in drdy:
                return drdy[name]
            if name in cfg:
                return cfg[name]
            return default

        return cls(
            startup_delay_s=float(get("startup_delay_s", cls.startup_delay_s)),
            init_retries=int(get("init_retries", cls.init_retries)),
            init_retry_delay_s=float(get("init_retry_delay_s", cls.init_retry_delay_s)),
            init_retry_backoff=float(get("init_retry_backoff", cls.init_retry_backoff)),
            max_init_retry_delay_s=float(get("max_init_retry_delay_s", cls.max_init_retry_delay_s)),
            drdy_active_low=bool(get("active_low", get("drdy_active_low", cls.drdy_active_low))),
            drdy_startup_timeout_s=float(get("startup_timeout_s", get("drdy_startup_timeout_s", cls.drdy_startup_timeout_s))),
            drdy_read_timeout_s=float(get("read_timeout_s", get("drdy_read_timeout_s", cls.drdy_read_timeout_s))),
            drdy_poll_interval_s=float(get("poll_interval_s", get("drdy_poll_interval_s", cls.drdy_poll_interval_s))),
            drdy_consecutive_timeout_limit=int(get("consecutive_timeout_limit", get("drdy_consecutive_timeout_limit", cls.drdy_consecutive_timeout_limit))),
            delay_after_reset_s=float(get("delay_after_reset_s", cls.delay_after_reset_s)),
            discard_first_samples_after_init=int(get("discard_first_samples_after_init", cls.discard_first_samples_after_init)),
            fail_service_on_ads_startup=bool(get("fail_service_on_ads_startup", cls.fail_service_on_ads_startup)),
        )


class ADS1220BootGuard:
    """Cold-boot/DRDY protection for the ADS1220 sensor worker.

    It does not know the existing ADS1220 driver implementation. Instead, pass
    callbacks from your current code: init_once(), reset_once(), start_conversion(),
    read_drdy_level() and read_sample_once().
    """

    def __init__(
        self,
        config: ADS1220BootConfig,
        health: Optional[HealthRegistry] = None,
        module_name: str = "sensor-ads1220",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.health = health
        self.module_name = module_name
        self.logger = logger or LOGGER
        self._consecutive_drdy_timeouts = 0
        self._reinit_count = 0
        self._cold_start_delay_done = False

    @property
    def consecutive_drdy_timeouts(self) -> int:
        return self._consecutive_drdy_timeouts

    @property
    def reinit_count(self) -> int:
        return self._reinit_count

    def _health_ok(self, **extra: Any) -> None:
        if self.health:
            self.health.record_ok(self.module_name, kind="sensor", **extra)

    def _health_error(self, exc: BaseException | str, **extra: Any) -> None:
        if self.health:
            self.health.record_error(self.module_name, exc, kind="sensor", state="degraded", **extra)

    def _sleep_or_stop(self, seconds: float, stop_event: Any = None) -> bool:
        if seconds <= 0:
            return False
        if stop_event is not None and hasattr(stop_event, "wait"):
            return bool(stop_event.wait(seconds))
        time.sleep(seconds)
        return False

    def sleep_before_first_init(self, stop_event: Any = None) -> bool:
        """Delay only once per guard instance before first ADS init."""
        if self._cold_start_delay_done:
            return False
        self._cold_start_delay_done = True
        delay = max(0.0, self.config.startup_delay_s)
        if delay:
            if self.health:
                self.health.update(
                    self.module_name,
                    kind="sensor",
                    state="starting",
                    healthy=False,
                    extra={"ads_boot_wait_s": delay, "ads_boot_wait_started_at": utc_now_iso()},
                )
            return self._sleep_or_stop(delay, stop_event)
        return False

    def is_drdy_active(self, level: Any) -> bool:
        # ADS1220 DRDY is typically active-low. Keep this configurable.
        if self.config.drdy_active_low:
            return level in (0, False, "0", "LOW", "low")
        return level in (1, True, "1", "HIGH", "high")

    def wait_for_drdy(
        self,
        read_drdy_level: Callable[[], Any],
        timeout_s: Optional[float] = None,
        stop_event: Any = None,
        purpose: str = "read",
        raise_on_timeout: bool = True,
    ) -> bool:
        timeout = self.config.drdy_read_timeout_s if timeout_s is None else float(timeout_s)
        deadline = time.monotonic() + max(0.0, timeout)
        last_level: Any = None
        read_errors = 0
        while time.monotonic() <= deadline:
            if stop_event is not None and hasattr(stop_event, "is_set") and stop_event.is_set():
                return False
            try:
                last_level = read_drdy_level()
                if self.is_drdy_active(last_level):
                    self._consecutive_drdy_timeouts = 0
                    self._health_ok(
                        ads_drdy_ready=True,
                        ads_drdy_last_level=last_level,
                        ads_drdy_last_ok_at=utc_now_iso(),
                        ads_drdy_purpose=purpose,
                    )
                    return True
            except Exception as exc:
                read_errors += 1
                self._health_error(exc, ads_drdy_gpio_read_errors=read_errors, ads_drdy_purpose=purpose)
            self._sleep_or_stop(max(0.0005, self.config.drdy_poll_interval_s), stop_event)

        self._consecutive_drdy_timeouts += 1
        err = DRDYTimeoutError(
            f"ADS1220 DRDY timeout during {purpose}: timeout_s={timeout}, last_level={last_level}, consecutive={self._consecutive_drdy_timeouts}"
        )
        self._health_error(
            err,
            ads_drdy_ready=False,
            ads_drdy_last_level=last_level,
            ads_drdy_last_timeout_at=utc_now_iso(),
            ads_drdy_consecutive_timeouts=self._consecutive_drdy_timeouts,
            ads_drdy_purpose=purpose,
        )
        if raise_on_timeout:
            raise err
        return False

    def init_with_retry(
        self,
        init_once: Callable[[], Any],
        read_drdy_level: Optional[Callable[[], Any]] = None,
        start_conversion: Optional[Callable[[Any], None]] = None,
        reset_once: Optional[Callable[[], None]] = None,
        discard_sample_once: Optional[Callable[[Any], Any]] = None,
        stop_event: Any = None,
    ) -> Any:
        """Initialize ADS1220 with cold-start delay, retries and DRDY verification.

        Returns the driver object from init_once(). If fail_service_on_ads_startup
        is false and all attempts fail, returns None instead of killing the full app.
        """
        if self.sleep_before_first_init(stop_event):
            return None

        delay = max(0.0, self.config.init_retry_delay_s)
        last_exc: Optional[BaseException] = None
        attempts = max(1, int(self.config.init_retries))

        for attempt in range(1, attempts + 1):
            if stop_event is not None and hasattr(stop_event, "is_set") and stop_event.is_set():
                return None
            try:
                if self.health:
                    self.health.update(
                        self.module_name,
                        kind="sensor",
                        state="starting",
                        healthy=False,
                        extra={"ads_init_attempt": attempt, "ads_init_attempts": attempts},
                    )

                if reset_once is not None:
                    reset_once()
                    self._sleep_or_stop(self.config.delay_after_reset_s, stop_event)

                driver = init_once()

                if start_conversion is not None:
                    start_conversion(driver)

                if read_drdy_level is not None:
                    self.wait_for_drdy(
                        read_drdy_level,
                        timeout_s=self.config.drdy_startup_timeout_s,
                        stop_event=stop_event,
                        purpose="startup",
                        raise_on_timeout=True,
                    )

                if discard_sample_once is not None:
                    for _ in range(max(0, self.config.discard_first_samples_after_init)):
                        discard_sample_once(driver)

                self._consecutive_drdy_timeouts = 0
                self._health_ok(
                    ads_init_ok=True,
                    ads_init_attempt=attempt,
                    ads_reinit_count=self._reinit_count,
                    ads_drdy_ready=True if read_drdy_level is not None else None,
                )
                return driver
            except Exception as exc:
                last_exc = exc
                self._health_error(
                    exc,
                    ads_init_ok=False,
                    ads_init_attempt=attempt,
                    ads_init_attempts=attempts,
                    ads_reinit_count=self._reinit_count,
                )
                self.logger.warning("ADS1220 init attempt %s/%s failed: %s", attempt, attempts, exc)
                if attempt < attempts:
                    if self._sleep_or_stop(delay, stop_event):
                        return None
                    delay = min(self.config.max_init_retry_delay_s, max(0.1, delay * max(1.0, self.config.init_retry_backoff)))

        if self.config.fail_service_on_ads_startup and last_exc is not None:
            raise last_exc
        self._health_error(last_exc or "ADS1220 init failed", ads_disabled_after_boot_retries=True)
        return None

    def read_sample_with_drdy_recovery(
        self,
        read_sample_once: Callable[[], Any],
        read_drdy_level: Callable[[], Any],
        recover_once: Optional[Callable[[], None]] = None,
        stop_event: Any = None,
    ) -> Any:
        """Read one sample, recover on repeated DRDY timeouts, and keep worker alive.

        Returns None when the sample should be skipped.
        """
        ready = self.wait_for_drdy(
            read_drdy_level,
            timeout_s=self.config.drdy_read_timeout_s,
            stop_event=stop_event,
            purpose="read",
            raise_on_timeout=False,
        )
        if not ready:
            if self._consecutive_drdy_timeouts >= max(1, self.config.drdy_consecutive_timeout_limit):
                if recover_once is not None:
                    try:
                        recover_once()
                        self._reinit_count += 1
                        self._consecutive_drdy_timeouts = 0
                        self._health_ok(
                            ads_recovered_after_drdy_timeout=True,
                            ads_reinit_count=self._reinit_count,
                            ads_drdy_recovered_at=utc_now_iso(),
                        )
                    except Exception as exc:
                        self._health_error(exc, ads_recovery_failed=True, ads_reinit_count=self._reinit_count)
                else:
                    self._health_error("DRDY timeout and no recover_once callback configured", ads_recovery_missing=True)
            return None

        try:
            sample = read_sample_once()
            self._health_ok(
                ads_last_sample_at=utc_now_iso(),
                ads_drdy_consecutive_timeouts=self._consecutive_drdy_timeouts,
                ads_reinit_count=self._reinit_count,
            )
            return sample
        except Exception as exc:
            self._health_error(exc, ads_sample_read_failed=True)
            return None
