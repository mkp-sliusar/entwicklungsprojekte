from __future__ import annotations

from collections import deque
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
import logging
import os
import signal
import sys
import threading
import time
from statistics import median
from typing import Any
from datetime import datetime, timezone
import yaml

from core.data_store import DataStore, RateMeter, SensorSample
from core.frequency import DominantFrequencyEstimator
from core.publisher_worker import DestinationWorker
from core.rate_loop import RateLoop
from sensors.ads1220 import ADS1220
from sensors.adxl345 import ADXL345
from sensors.mpu9250 import MPU9250
from services.influx_writer import InfluxWriter
from services.mqtt_client import MQTTClient
from services.system_monitor import SystemMonitor
from services.websocket_server import LiveWebSocketServer
from services import tare_api
from sensors.pisugar import PiSugar
from sensors.x728 import X728


WEBSOCKET_STRAIN_CHANNEL = "NiE+080_HSN-u-_DU.E+080DU_HSN-u-"
WEBSOCKET_VIBRATION_CHANNEL = "NiE+233_HSN-m-_BU.E+233BU_HSN-m-"
WEBSOCKET_BATTERY_CHANNEL = "battery_percent"


DEFAULT_CONFIG: dict[str, Any] = {
    "runtime": {
        "log_level": "INFO",
        "stop_timeout_s": 3.0,
    },
    "device": {
        "id": "bridge01",
        "location": "lab",
    },
    "sensors": {
        "mpu9250": {
            "enabled": False,
            "retry_interval_s": 5.0,
            "i2c_bus": 1,
            "address": 0x68,
            "sample_rate_hz": 100,
            "dlpf_hz": 184,
            "accel_scale_g": 2,
            "vibration_highpass_alpha": 0.05,
            "roll_pitch_median_window": 1,
            "frequency": {
                "enabled": True,
                "fft_size": 256,
                "update_rate_hz": 5,
                "min_frequency_hz": 0.5,
            },
        },
        "adxl345": {
            "enabled": True,
            "retry_interval_s": 5.0,
            "i2c_bus": 1,
            "address": 0x53,
            "sample_rate_hz": 200,
            "accel_scale_g": 16,
            "full_resolution": True,
            "vibration_highpass_alpha": 0.05,
            "roll_pitch_median_window": 1,
            "frequency": {
                "enabled": True,
                "fft_size": 256,
                "update_rate_hz": 5,
                "min_frequency_hz": 0.5,
            },
        },
        "ads1220": {
            "enabled": True,
            "retry_interval_s": 5.0,
            "sample_rate_hz": 100,
            "spi_bus": 0,
            "spi_device": 0,
            "spi_speed_hz": 4000000,
            "spi_mode": 1,
            "drdy_pin": 17,
            "data_rate_sps": 330,
            "continuous_conversion": True,
            "start_each_read": False,
            "registers": {
                "reg0": 0x0E,
                "reg1": 0x84,
                "reg2": 0x00,
                "reg3": 0x00,
            },
            "vref": 3.3,
            "gain": 128.0,
            "zero_raw": 0.0,
            "raw_per_gram": 185.0,
            "gauge_factor": 2.0,
            "young_modulus_mpa": 3500.0,
            "tare_file": "tare.dat",
            "tare_samples": 10,
            "tare_interval_s": 0.0,
            "filter_alpha": 0.05,
            "auto_tare_on_startup": True,
            "auto_tare_delay_s": 2.0,
            "read_timeout_s": 1.0,
            "use_gpio_wait": True,
        },
        "x728": {
            "enabled": False,
            "retry_interval_s": 5.0,
            "auto_detect": True,
            "i2c_bus": 1,
            "address": 0x36,
            "sample_rate_hz": 1,
        },
        "pisugar": {
            "enabled": False,
            "retry_interval_s": 5.0,
            "auto_detect": True,
            "i2c_bus": 1,
            "address": 0x57,
            "sample_rate_hz": 1,
        },
    },
    "buttons": {
        "tare": {
            "enabled": True,
            "gpio_pin": 27,
            "poll_rate_hz": 50,
            "debounce_s": 0.10,
            "pull_up": True,
        }
    },
    "services": {
        "system_monitor": {
            "enabled": True,
            "sample_rate_hz": 1,
            "check_internet": True,
            "internet_timeout_s": 0.2,
        },
        "tare_api": {
            "enabled": True,
            "host": "0.0.0.0",
            "port": 8080,
        },
    },
    "destinations": {
        "influxdb": {
            "enabled": False,
            "url": "http://localhost:8086",
            "org": "MKP",
            "bucket": "MKP",
            "token": "",
            "timeout_ms": 5000,
            "batch_size": 500,
            "flush_interval_ms": 1000,
            "payload": "per_sensor",
            "mode": "aggregate",
            "rate_hz": 10,
            "queue_size": 10000,
            "sensors": {
                "adxl345": {"enabled": True, "mode": "aggregate", "rate_hz": 10},
                "ads1220": {"enabled": True, "mode": "aggregate", "rate_hz": 10},
                "system": {"enabled": True, "mode": "latest", "rate_hz": 1},
                "x728": {"enabled": True, "mode": "latest", "rate_hz": 1},
                "pisugar": {"enabled": True, "mode": "latest", "rate_hz": 1},
            },
        },
        "mqtt_external": {
            "enabled": False,
            "host": "localhost",
            "port": 1883,
            "username": "",
            "password": "",
            "topic": "test-monitoring/demonstrator",
            "append_sensor_to_topic": False,
            "qos": 0,
            "retain": False,
            "payload": "per_sensor",
            "mode": "aggregate",
            "rate_hz": 10,
            "queue_size": 5000,
            "sensors": {
                "adxl345": {"enabled": True, "mode": "aggregate", "rate_hz": 10},
                "ads1220": {"enabled": True, "mode": "aggregate", "rate_hz": 10},
                "system": {"enabled": False},
                "x728": {"enabled": True, "mode": "latest", "rate_hz": 1},
                "pisugar": {"enabled": True, "mode": "latest", "rate_hz": 1},
            },
        },
        "mqtt_local": {
            "enabled": False,
            "host": "localhost",
            "port": 1883,
            "username": "",
            "password": "",
            "topic": "live/bridge01",
            "append_sensor_to_topic": False,
            "qos": 0,
            "retain": False,
            "payload": "snapshot",
            "rate_hz": 1,
            "queue_size": 1000,
        },
        "websocket": {
            "enabled": False,
            "host": "0.0.0.0",
            "port": 8765,
            "payload": "snapshot",
            "rate_hz": 10,
            "queue_size": 2000,
            "battery_percent_median_window": 5,
        },
    },
}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def normalize_legacy_config(raw: dict[str, Any]) -> dict[str, Any]:
    raw = deepcopy(raw or {})

    if "sensors" not in raw:
        raw["sensors"] = {}
    if "mpu9250" in raw:
        raw["sensors"].setdefault("mpu9250", {}).update(raw.pop("mpu9250") or {})
    if "ads1220" in raw:
        raw["sensors"].setdefault("ads1220", {}).update(raw.pop("ads1220") or {})

    if "destinations" not in raw:
        raw["destinations"] = {}
    for old_key in ("influxdb", "mqtt_external", "mqtt_local", "websocket"):
        if old_key in raw:
            raw["destinations"].setdefault(old_key, {}).update(raw.pop(old_key) or {})

    if "hall" in raw:
        raw.pop("hall")

    return raw


def load_config(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    normalized = normalize_legacy_config(raw)
    return deep_merge(DEFAULT_CONFIG, normalized)


def parse_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 0)
    return int(value)


def publish_websocket_channels(
    websocket: LiveWebSocketServer,
    sensor: str,
    payload: dict[str, Any],
    timestamp_s: float,
    battery_percent_history: dict[str, deque[float]],
    battery_percent_median_window: int,
) -> None:
    iso_timestamp = datetime.fromtimestamp(timestamp_s, timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")

    if sensor == "ads1220" and "strain_mm_m" in payload:
        websocket.publish_sensor_channel(
            iso_timestamp=iso_timestamp,
            channel=WEBSOCKET_STRAIN_CHANNEL,
            unit="mm/m",
            physical_quantity="Strain",
            value=float(payload["strain_mm_m"]),
        )

    if sensor in {"mpu9250", "adxl345"} and "vibration_z_ms2" in payload:
        websocket.publish_sensor_channel(
            iso_timestamp=iso_timestamp,
            channel=WEBSOCKET_VIBRATION_CHANNEL,
            unit="m/s²",
            physical_quantity="Vertical vibration acceleration",
            value=float(payload["vibration_z_ms2"]),
        )

    if sensor in {"pisugar", "x728"} and "battery_percent" in payload:
        history = battery_percent_history.setdefault(
            sensor,
            deque(maxlen=max(int(battery_percent_median_window), 1)),
        )
        history.append(max(0.0, min(float(payload["battery_percent"]), 100.0)))
        websocket.publish_sensor_channel(
            iso_timestamp=iso_timestamp,
            channel=WEBSOCKET_BATTERY_CHANNEL,
            unit="%",
            physical_quantity="Battery charge level",
            value=float(median(history)),
        )


class BridgeApplication:
    def __init__(self, config: dict[str, Any]):
        self.cfg = config
        self.device_id = str(config["device"]["id"])
        self.stop_event = threading.Event()
        self.store = DataStore()
        self.loops: list[RateLoop] = []
        self.destinations: dict[str, DestinationWorker] = {}
        self.mqtt_clients: dict[str, MQTTClient] = {}
        self.influx: InfluxWriter | None = None
        self.websocket: LiveWebSocketServer | None = None
        self.ads: ADS1220 | None = None
        self.adxl: ADXL345 | None = None
        self.x728: X728 | None = None
        self.pisugar: PiSugar | None = None
        self.mpu: MPU9250 | None = None
        self._tare_thread: threading.Thread | None = None
        self._gpio = None
        self.log = logging.getLogger("bridge")

    def start(self) -> None:
        self._setup_sensors()
        self._setup_destinations()
        self._setup_tare_api()
        self._setup_button()

        for worker in self.destinations.values():
            worker.start()
        for loop in self.loops:
            loop.start()

        self.log.info("Sensor service started")

    def stop(self) -> None:
        self.stop_event.set()
        timeout = float(self.cfg["runtime"].get("stop_timeout_s", 3.0))
        for loop in self.loops:
            loop.join(timeout=timeout)
        for worker in self.destinations.values():
            worker.join(timeout=timeout)

        for client in self.mqtt_clients.values():
            client.close()
        if self.influx:
            self.influx.close()
        if self.websocket:
            self.websocket.close()
        if self.mpu:
            self.mpu.close()
        if self.adxl:
            self.adxl.close()
        if self.ads:
            self.ads.close()
        if self.x728:
            self.x728.close()
        if self.pisugar:
            self.pisugar.close()
        if self._gpio:
            try:
                self._gpio.cleanup()
            except Exception:
                pass

    def request_stop(self, *_args) -> None:
        self.stop_event.set()

    def status(self) -> dict[str, Any]:
        status: dict[str, Any] = {
            "destinations": {name: worker.stats() for name, worker in self.destinations.items()},
            "mqtt": {
                name: {
                    "connected": int(client.connected),
                    "last_error": client.last_error,
                }
                for name, client in self.mqtt_clients.items()
            },
        }

        if self.ads:
            tare_enabled = bool(self.ads.tare_enabled)
            status["tare"] = {
                "running": int(self.ads.tare_running),
                "enabled": int(tare_enabled),
                "mode": "tared" if tare_enabled else "raw",
                "status": "TARED" if tare_enabled else "RAW",
                "status_code": int(tare_enabled),
                "raw_values": int(not tare_enabled),
                "zero_raw": float(self.ads.zero_raw),
                "filter_alpha": float(self.ads.filter_alpha),
            }
        status["ups"] = {
            "x728_detected": bool(self.x728),
            "pisugar_detected": bool(self.pisugar),
        }
        return status

    def ws_command(self, command: str, payload: dict[str, Any] | None = None) -> None:
        payload = payload or {}
        if command == "tare":
            self.request_tare()
        elif command == "filter":
            if "alpha" in payload:
                self.set_filter_alpha(float(payload["alpha"]))

    def request_tare(self, ensure_tared: bool = False) -> bool:
        ads = self.ads
        if not ads:
            self.log.warning("Tare request rejected: ADS1220 unavailable")
            return False
        if ads.tare_running:
            self.log.warning("Tare request rejected: tare already running")
            return False
        if ensure_tared and ads.tare_enabled:
            self.log.info("Tare ensure request ignored: mode already tared")
            self.publish_tare_status(True)
            if self.websocket:
                self.websocket.publish_tare_status(True)
            return True

        def runner():
            self.log.info("Tare started")
            try:
                zero = ads.tare()
            except Exception:
                self.log.exception("Tare failed")
                return
            enabled = bool(ads.tare_enabled)
            self.log.info(
                "Tare completed: mode=%s zero_raw=%s",
                "tared" if enabled else "raw",
                zero,
            )
            self.publish_tare_status(enabled)
            if self.websocket:
                self.websocket.publish_tare_done(zero, enabled)

        self._tare_thread = threading.Thread(target=runner, name="tare", daemon=True)
        self._tare_thread.start()
        return True

    def publish_tare_status(self, enabled: bool) -> None:
        client = self.mqtt_clients.get("mqtt_local")
        if not client:
            return
        client.publish_json(
            {
                "device": self.device_id,
                "type": "tare_status",
                "sensor": "tare",
                "tare_status": "TARED" if enabled else "RAW",
                "tare_status_code": int(enabled),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            topic=f"{client.topic}/tare",
            retain_override=True,
        )

    def set_filter_alpha(self, alpha: float) -> None:
        if self.ads:
            self.ads.set_filter_alpha(alpha)
        tare_api.filter_alpha = float(alpha)

    def publish_sample(self, sample: SensorSample) -> None:
        self.store.update(sample)
        for worker in self.destinations.values():
            worker.submit(sample)

    def _add_retrying_sensor_loop(
        self,
        name: str,
        rate_hz: float,
        retry_interval_s: float,
        connect: Callable[[], Any],
        read: Callable[[Any], None],
        get_sensor: Callable[[], Any],
        set_sensor: Callable[[Any], None],
        close_sensor: Callable[[Any], None],
        on_connected: Callable[[Any], None] | None = None,
    ) -> None:
        retry_interval_s = max(float(retry_interval_s), 0.1)
        next_retry = 0.0

        def run() -> None:
            nonlocal next_retry

            sensor = get_sensor()
            if sensor is None:
                wait_s = next_retry - time.monotonic()
                if wait_s > 0 and self.stop_event.wait(wait_s):
                    return

                try:
                    sensor = connect()
                except Exception as exc:
                    next_retry = time.monotonic() + retry_interval_s
                    self.log.warning(
                        "%s unavailable; retrying in %.1fs: %s",
                        name,
                        retry_interval_s,
                        exc,
                    )
                    return

                set_sensor(sensor)
                next_retry = 0.0
                self.log.info("%s connected", name)
                if on_connected:
                    try:
                        on_connected(sensor)
                    except Exception as exc:
                        self.log.warning("%s startup action failed: %s", name, exc)
                return

            try:
                read(sensor)
            except Exception as exc:
                next_retry = time.monotonic() + retry_interval_s
                self.log.warning(
                    "%s read failed; reconnecting in %.1fs: %s",
                    name,
                    retry_interval_s,
                    exc,
                )
                try:
                    close_sensor(sensor)
                except Exception as close_exc:
                    self.log.warning("%s close failed: %s", name, close_exc)
                if get_sensor() is sensor:
                    set_sensor(None)

        self.loops.append(RateLoop(name, rate_hz, run, self.stop_event))

    def _setup_sensors(self) -> None:
        sensor_cfg = self.cfg.get("sensors", {})

        adxl_cfg = sensor_cfg.get("adxl345", {})
        if adxl_cfg.get("enabled", False):
            sample_rate = float(adxl_cfg.get("sample_rate_hz", 200.0))
            retry_interval_s = float(adxl_cfg.get("retry_interval_s", 5.0))
            bus_id = int(adxl_cfg.get("i2c_bus", 1))
            address = parse_int(adxl_cfg.get("address", 0x53))
            meter = RateMeter()
            freq_cfg = adxl_cfg.get("frequency", {}) or {}
            freq_estimator = None
            if freq_cfg.get("enabled", True):
                freq_estimator = DominantFrequencyEstimator(
                    sample_rate_hz=sample_rate,
                    fft_size=int(freq_cfg.get("fft_size", 256)),
                    update_rate_hz=float(freq_cfg.get("update_rate_hz", 5)),
                    min_frequency_hz=float(freq_cfg.get("min_frequency_hz", 0.5)),
                )

            def connect_adxl(
                *,
                bus_id: int = bus_id,
                address: int = address,
                sample_rate: float = sample_rate,
                adxl_cfg: dict[str, Any] = adxl_cfg,
            ) -> ADXL345:
                return ADXL345(
                    bus_id=bus_id,
                    address=address,
                    sample_rate_hz=sample_rate,
                    accel_scale_g=int(adxl_cfg.get("accel_scale_g", 16)),
                    full_resolution=bool(adxl_cfg.get("full_resolution", True)),
                    vibration_highpass_alpha=float(
                        adxl_cfg.get("vibration_highpass_alpha", 0.05)
                    ),
                    roll_pitch_median_window=int(
                        adxl_cfg.get("roll_pitch_median_window", 1)
                    ),
                )

            def read_adxl(
                sensor: ADXL345,
                *,
                sample_rate: float = sample_rate,
                meter: RateMeter = meter,
                freq_estimator: DominantFrequencyEstimator | None = freq_estimator,
            ) -> None:
                data = sensor.read()
                ts = float(data.pop("timestamp", time.time()))
                data["sample_rate_hz"] = meter.tick(ts)
                data["configured_rate_hz"] = sample_rate
                data["sensor_data_rate_hz"] = sensor.data_rate_hz
                if freq_estimator:
                    data["frequency_hz"] = freq_estimator.update(
                        float(data.get("vibration_ms2", 0.0)), ts
                    )
                self.publish_sample(SensorSample("adxl345", ts, data))

            self._add_retrying_sensor_loop(
                "sensor-adxl345",
                sample_rate,
                retry_interval_s,
                connect_adxl,
                read_adxl,
                lambda: self.adxl,
                lambda sensor: setattr(self, "adxl", sensor),
                lambda sensor: sensor.close(),
            )

        mpu_cfg = sensor_cfg.get("mpu9250", {})
        if mpu_cfg.get("enabled", True):
            sample_rate = float(mpu_cfg.get("sample_rate_hz", 100.0))
            retry_interval_s = float(mpu_cfg.get("retry_interval_s", 5.0))
            bus_id = int(mpu_cfg.get("i2c_bus", 1))
            address = parse_int(mpu_cfg.get("address", 0x68))
            meter = RateMeter()
            freq_cfg = mpu_cfg.get("frequency", {}) or {}
            freq_estimator = None
            if freq_cfg.get("enabled", True):
                freq_estimator = DominantFrequencyEstimator(
                    sample_rate_hz=sample_rate,
                    fft_size=int(freq_cfg.get("fft_size", 256)),
                    update_rate_hz=float(freq_cfg.get("update_rate_hz", 5)),
                    min_frequency_hz=float(freq_cfg.get("min_frequency_hz", 0.5)),
                )

            def connect_mpu(
                *,
                bus_id: int = bus_id,
                address: int = address,
                sample_rate: float = sample_rate,
                mpu_cfg: dict[str, Any] = mpu_cfg,
            ) -> MPU9250:
                return MPU9250(
                    bus_id=bus_id,
                    address=address,
                    sample_rate_hz=sample_rate,
                    dlpf_hz=int(mpu_cfg.get("dlpf_hz", 184)),
                    accel_scale_g=int(mpu_cfg.get("accel_scale_g", 2)),
                    vibration_highpass_alpha=float(mpu_cfg.get("vibration_highpass_alpha", 0.05)),
                    roll_pitch_median_window=int(mpu_cfg.get("roll_pitch_median_window", 1)),
                )

            def read_mpu(
                sensor: MPU9250,
                *,
                sample_rate: float = sample_rate,
                meter: RateMeter = meter,
                freq_estimator: DominantFrequencyEstimator | None = freq_estimator,
            ) -> None:
                data = sensor.read()
                ts = float(data.pop("timestamp", time.time()))
                data["sample_rate_hz"] = meter.tick(ts)
                data["configured_rate_hz"] = sample_rate
                if freq_estimator:
                    data["frequency_hz"] = freq_estimator.update(float(data.get("vibration_ms2", 0.0)), ts)
                self.publish_sample(SensorSample("mpu9250", ts, data))

            self._add_retrying_sensor_loop(
                "sensor-mpu9250",
                sample_rate,
                retry_interval_s,
                connect_mpu,
                read_mpu,
                lambda: self.mpu,
                lambda sensor: setattr(self, "mpu", sensor),
                lambda sensor: sensor.close(),
            )

        ads_cfg = sensor_cfg.get("ads1220", {})
        if ads_cfg.get("enabled", True):
            registers = ads_cfg.get("registers", {}) or {}
            sample_rate = float(ads_cfg.get("sample_rate_hz", 100.0))
            data_rate_sps = ads_cfg.get("data_rate_sps")
            retry_interval_s = float(ads_cfg.get("retry_interval_s", 5.0))
            tare_api.filter_alpha = float(ads_cfg.get("filter_alpha", 0.05))

            def connect_ads(ads_cfg: dict[str, Any] = ads_cfg) -> ADS1220:
                return ADS1220(
                    spi_bus=int(ads_cfg.get("spi_bus", 0)),
                    spi_device=int(ads_cfg.get("spi_device", 0)),
                    spi_speed_hz=int(ads_cfg.get("spi_speed_hz", 4000000)),
                    spi_mode=int(ads_cfg.get("spi_mode", 1)),
                    drdy_pin=int(ads_cfg.get("drdy_pin", 17)),
                    reg0=parse_int(registers.get("reg0", 0x0E)),
                    reg1=parse_int(registers.get("reg1", 0x84)),
                    reg2=parse_int(registers.get("reg2", 0x00)),
                    reg3=parse_int(registers.get("reg3", 0x00)),
                    data_rate_sps=int(data_rate_sps) if data_rate_sps is not None else None,
                    continuous_conversion=bool(ads_cfg.get("continuous_conversion", True)),
                    start_each_read=bool(ads_cfg.get("start_each_read", False)),
                    vref=float(ads_cfg.get("vref", 3.3)),
                    gain=float(ads_cfg.get("gain", 128.0)),
                    zero_raw=float(ads_cfg.get("zero_raw", 0.0)),
                    raw_per_gram=float(ads_cfg.get("raw_per_gram", 185.0)),
                    gauge_factor=float(ads_cfg.get("gauge_factor", 2.0)),
                    young_modulus_mpa=float(ads_cfg.get("young_modulus_mpa", 3500.0)),
                    tare_file=str(ads_cfg.get("tare_file", "tare.dat")),
                    tare_samples=int(ads_cfg.get("tare_samples", 10)),
                    tare_interval_s=float(ads_cfg.get("tare_interval_s", 0.0)),
                    filter_alpha=float(ads_cfg.get("filter_alpha", 0.05)),
                    use_gpio_wait=bool(ads_cfg.get("use_gpio_wait", True)),
                    read_timeout_s=float(ads_cfg.get("read_timeout_s", 1.0)),
                )

            def start_ads_auto_tare(sensor: ADS1220) -> None:
                if not ads_cfg.get("auto_tare_on_startup", False):
                    if self.websocket:
                        self.websocket.publish_tare_status(sensor.tare_enabled)
                    return

                delay_s = max(float(ads_cfg.get("auto_tare_delay_s", 2.0)), 0.0)

                def tare_runner() -> None:
                    if self.stop_event.wait(delay_s) or self.ads is not sensor:
                        return
                    try:
                        zero_raw = sensor.tare()
                    except Exception as exc:
                        self.log.warning("ADS1220 startup tare failed: %s", exc)
                    else:
                        self.log.info("ADS1220 startup tare completed: zero_raw=%s", zero_raw)
                        self.publish_tare_status(sensor.tare_enabled)
                        if self.websocket:
                            self.websocket.publish_tare_status(sensor.tare_enabled)

                self._tare_thread = threading.Thread(
                    target=tare_runner,
                    name="ads1220-auto-tare",
                    daemon=True,
                )
                self._tare_thread.start()

            meter = RateMeter()

            def read_ads(
                sensor: ADS1220,
                *,
                sample_rate: float = sample_rate,
                meter: RateMeter = meter,
            ) -> None:
                data = sensor.read()
                if data.get("error"):
                    raise RuntimeError(str(data["error"]))
                ts = float(data.pop("timestamp", time.time()))
                data["sample_rate_hz"] = meter.tick(ts)
                data["configured_rate_hz"] = sample_rate
                data["filter_alpha"] = float(sensor.filter_alpha)
                tare_enabled = bool(sensor.tare_enabled)
                data["tare_enabled"] = int(tare_enabled)
                data["tare_mode"] = "tared" if tare_enabled else "raw"
                data["tare_status"] = "TARED" if tare_enabled else "RAW"
                data["tare_status_code"] = int(tare_enabled)
                data["raw_values"] = int(not tare_enabled)
                self.publish_sample(SensorSample("ads1220", ts, data))

            self._add_retrying_sensor_loop(
                "sensor-ads1220",
                sample_rate,
                retry_interval_s,
                connect_ads,
                read_ads,
                lambda: self.ads,
                lambda sensor: setattr(self, "ads", sensor),
                lambda sensor: sensor.close(),
                start_ads_auto_tare,
            )

        x728_cfg = sensor_cfg.get("x728", {})
        if x728_cfg.get("enabled", False):
            sample_rate = float(x728_cfg.get("sample_rate_hz", 1.0))
            retry_interval_s = float(x728_cfg.get("retry_interval_s", 5.0))
            bus_id = int(x728_cfg.get("i2c_bus", 1))
            address = parse_int(x728_cfg.get("address", 0x36))
            meter = RateMeter()

            def connect_x728(
                *,
                x728_cfg: dict[str, Any] = x728_cfg,
                bus_id: int = bus_id,
                address: int = address,
            ) -> X728:
                if x728_cfg.get("auto_detect", True) and not X728.detect(bus_id, address):
                    raise RuntimeError("not detected")
                return X728(bus_id=bus_id, address=address)

            def read_x728(
                sensor: X728,
                *,
                sample_rate: float = sample_rate,
                meter: RateMeter = meter,
            ) -> None:
                data = sensor.read()
                if data.get("error"):
                    raise RuntimeError(str(data["error"]))
                ts = float(data.pop("timestamp", time.time()))
                data["sample_rate_hz"] = meter.tick(ts)
                data["configured_rate_hz"] = sample_rate
                self.publish_sample(SensorSample("x728", ts, data))

            self._add_retrying_sensor_loop(
                "sensor-x728",
                sample_rate,
                retry_interval_s,
                connect_x728,
                read_x728,
                lambda: self.x728,
                lambda sensor: setattr(self, "x728", sensor),
                lambda sensor: sensor.close(),
            )

        pisugar_cfg = sensor_cfg.get("pisugar", {})
        if pisugar_cfg.get("enabled", False):
            sample_rate = float(pisugar_cfg.get("sample_rate_hz", 1.0))
            retry_interval_s = float(pisugar_cfg.get("retry_interval_s", 5.0))
            bus_id = int(pisugar_cfg.get("i2c_bus", 1))
            address = parse_int(pisugar_cfg.get("address", 0x57))
            meter = RateMeter()

            def connect_pisugar(
                *,
                pisugar_cfg: dict[str, Any] = pisugar_cfg,
                bus_id: int = bus_id,
                address: int = address,
            ) -> PiSugar:
                if pisugar_cfg.get("auto_detect", True) and not PiSugar.detect(bus_id, address):
                    raise RuntimeError("not detected")
                return PiSugar(bus_id=bus_id, address=address)

            def read_pisugar(
                sensor: PiSugar,
                *,
                sample_rate: float = sample_rate,
                meter: RateMeter = meter,
            ) -> None:
                data = sensor.read()
                if data.get("error"):
                    raise RuntimeError(str(data["error"]))
                ts = float(data.pop("timestamp", time.time()))
                data["sample_rate_hz"] = meter.tick(ts)
                data["configured_rate_hz"] = sample_rate
                self.publish_sample(SensorSample("pisugar", ts, data))

            self._add_retrying_sensor_loop(
                "sensor-pisugar",
                sample_rate,
                retry_interval_s,
                connect_pisugar,
                read_pisugar,
                lambda: self.pisugar,
                lambda sensor: setattr(self, "pisugar", sensor),
                lambda sensor: sensor.close(),
            )

        system_cfg = self.cfg.get("services", {}).get("system_monitor", {})
        if system_cfg.get("enabled", True):
            sample_rate = float(system_cfg.get("sample_rate_hz", 1.0))
            meter = RateMeter()

            def read_system(
                *,
                sample_rate: float = sample_rate,
                meter: RateMeter = meter,
                system_cfg: dict[str, Any] = system_cfg,
            ) -> None:
                data = SystemMonitor.get_metrics(
                    check_internet=bool(system_cfg.get("check_internet", True)),
                    internet_timeout_s=float(system_cfg.get("internet_timeout_s", 0.2)),
                )
                ts = time.time()
                data["sample_rate_hz"] = meter.tick(ts)
                data["configured_rate_hz"] = sample_rate
                self.publish_sample(SensorSample("system", ts, data))

            self.loops.append(RateLoop("sensor-system", sample_rate, read_system, self.stop_event))

    def _setup_destinations(self) -> None:
        destinations_cfg = self.cfg.get("destinations", {})

        influx_cfg = destinations_cfg.get("influxdb", {})
        if influx_cfg.get("enabled", False):
            self.influx = InfluxWriter(
                url=str(influx_cfg.get("url")),
                token=str(influx_cfg.get("token", "")),
                org=str(influx_cfg.get("org", "")),
                bucket=str(influx_cfg.get("bucket", "")),
                timeout_ms=int(influx_cfg.get("timeout_ms", 5000)),
                batch_size=int(influx_cfg.get("batch_size", 500)),
                flush_interval_ms=int(influx_cfg.get("flush_interval_ms", 1000)),
            )

            def publish_influx(sensor: str, payload: dict[str, Any], timestamp_s: float) -> None:
                assert self.influx is not None
                self.influx.write_payload(sensor, self.device_id, payload, timestamp_s)

            self.destinations["influxdb"] = DestinationWorker(
                "influxdb",
                influx_cfg,
                self.device_id,
                self.store,
                publish_influx,
                self.status,
                self.stop_event,
            )

        for name in ("mqtt_external", "mqtt_local"):
            mqtt_cfg = destinations_cfg.get(name, {})
            if not mqtt_cfg.get("enabled", False):
                continue

            client = MQTTClient(
                host=str(mqtt_cfg.get("host", "localhost")),
                port=int(mqtt_cfg.get("port", 1883)),
                username=str(mqtt_cfg.get("username", "")),
                password=str(mqtt_cfg.get("password", "")),
                topic=str(mqtt_cfg.get("topic", name)),
                client_id=str(mqtt_cfg.get("client_id", "")) or None,
                keepalive=int(mqtt_cfg.get("keepalive", 60)),
                qos=int(mqtt_cfg.get("qos", 0)),
                retain=bool(mqtt_cfg.get("retain", False)),
                max_queued_messages=int(mqtt_cfg.get("mqtt_queue_size", 5000)),
            )
            self.mqtt_clients[name] = client
            append_sensor = bool(mqtt_cfg.get("append_sensor_to_topic", False))
            base_topic = str(mqtt_cfg.get("topic", name)).rstrip("/")

            def make_publish(c: MQTTClient, append: bool, base: str):
                def publish_mqtt(sensor: str, payload: dict[str, Any], _timestamp_s: float) -> None:
                    topic = f"{base}/{sensor}" if append and sensor != "snapshot" else base
                    c.publish_json(payload, topic=topic)

                return publish_mqtt

            self.destinations[name] = DestinationWorker(
                name,
                mqtt_cfg,
                self.device_id,
                self.store,
                make_publish(client, append_sensor, base_topic),
                self.status,
                self.stop_event,
            )

        ws_cfg = destinations_cfg.get("websocket", {})
        if ws_cfg.get("enabled", False):
            self.websocket = LiveWebSocketServer(
                host=str(ws_cfg.get("host", "0.0.0.0")),
                port=int(ws_cfg.get("port", 8765)),
            )
            self.websocket.set_command_callback(self.ws_command)
            self.websocket.start()
            battery_percent_history: dict[str, deque[float]] = {}
            battery_percent_median_window = max(
                int(ws_cfg.get("battery_percent_median_window", 5)),
                1,
            )

            def publish_ws(sensor: str, payload: dict[str, Any], _timestamp_s: float) -> None:
                assert self.websocket is not None
                publish_websocket_channels(
                    self.websocket,
                    sensor,
                    payload,
                    _timestamp_s,
                    battery_percent_history,
                    battery_percent_median_window,
                )

            self.destinations["websocket"] = DestinationWorker(
                "websocket",
                ws_cfg,
                self.device_id,
                self.store,
                publish_ws,
                self.status,
                self.stop_event,
            )

    def _setup_tare_api(self) -> None:
        api_cfg = self.cfg.get("services", {}).get("tare_api", {})
        if not api_cfg.get("enabled", True):
            return

        tare_api.set_handlers(
            tare_callback=self.request_tare,
            filter_callback=self.set_filter_alpha,
            status_callback=self.status,
        )
        thread = threading.Thread(
            target=tare_api.start_server,
            kwargs={
                "host": str(api_cfg.get("host", "0.0.0.0")),
                "port": int(api_cfg.get("port", 8080)),
            },
            name="tare-api",
            daemon=True,
        )
        thread.start()

    def _setup_button(self) -> None:
        button_cfg = self.cfg.get("buttons", {}).get("tare", {})
        if not button_cfg.get("enabled", False):
            return

        try:
            import RPi.GPIO as GPIO
        except Exception:
            self.log.warning("RPi.GPIO unavailable, tare button disabled")
            return

        self._gpio = GPIO
        pin = int(button_cfg.get("gpio_pin", 27))
        pull_up = bool(button_cfg.get("pull_up", True))
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP if pull_up else GPIO.PUD_DOWN)

        last_state = GPIO.input(pin)
        last_event = 0.0
        debounce_s = float(button_cfg.get("debounce_s", 0.1))

        def poll_button() -> None:
            nonlocal last_state, last_event
            state = GPIO.input(pin)
            now = time.monotonic()
            rising = state == 1 and last_state == 0
            falling = state == 0 and last_state == 1
            trigger = falling if pull_up else rising
            if trigger and now - last_event >= debounce_s:
                last_event = now
                self.log.info("Physical tare button pressed")
                self.request_tare()
            last_state = state

        self.loops.append(
            RateLoop(
                "button-tare",
                float(button_cfg.get("poll_rate_hz", 50)),
                poll_button,
                self.stop_event,
            )
        )


def setup_logging(config: dict[str, Any]) -> None:
    level_name = str(config.get("runtime", {}).get("log_level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    config_path = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CONFIG", "config.yaml"))
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path
    config_path = config_path.resolve()

    os.chdir(config_path.parent)
    config = load_config(config_path)
    setup_logging(config)

    app = BridgeApplication(config)
    signal.signal(signal.SIGTERM, app.request_stop)
    signal.signal(signal.SIGINT, app.request_stop)

    try:
        app.start()
        while not app.stop_event.is_set():
            app.stop_event.wait(1.0)
    finally:
        app.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
