from __future__ import annotations

import math
from typing import Any

from core.data_store import SensorSample
from core.time_utils import utc_iso


def rms(values: list[float]) -> float:
    if not values:
        return 0.0
    return math.sqrt(sum(v * v for v in values) / len(values))


def _numeric_values(samples: list[SensorSample], key: str) -> list[float]:
    values: list[float] = []
    for sample in samples:
        value = sample.data.get(key)
        if isinstance(value, bool):
            values.append(float(int(value)))
        elif isinstance(value, (int, float)):
            values.append(float(value))
    return values


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def aggregate_samples(device_id: str, sensor: str, samples: list[SensorSample]) -> dict[str, Any]:
    if not samples:
        return {
            "device": device_id,
            "type": sensor,
            "sensor": sensor,
            "count": 0,
        }

    latest = samples[-1]
    first_ts = samples[0].timestamp
    last_ts = latest.timestamp
    duration_s = max(last_ts - first_ts, 0.0)
    sample_rate_hz = (len(samples) - 1) / duration_s if len(samples) > 1 and duration_s > 0 else 0.0

    payload: dict[str, Any] = {
        "device": device_id,
        "type": sensor,
        "sensor": sensor,
        "timestamp": utc_iso(last_ts),
        "mode": "aggregate",
        "count": len(samples),
        "window_s": duration_s,
        "sample_rate_hz": sample_rate_hz,
    }
    payload.update(latest.data)

    if sensor in {"mpu9250", "adxl345"}:
        vib = _numeric_values(samples, "vibration_ms2")
        vib_z = _numeric_values(samples, "vibration_z_ms2")
        temp = _numeric_values(samples, "temperature")
        roll = _numeric_values(samples, "roll_deg")
        pitch = _numeric_values(samples, "pitch_deg")

        payload["vibration_rms"] = rms(vib)
        payload["vibration_peak"] = max((abs(v) for v in vib), default=0.0)
        payload["vibration_z_rms"] = rms(vib_z)
        payload["vibration_z_peak"] = max((abs(v) for v in vib_z), default=0.0)
        if temp:
            payload["temperature_avg"] = _mean(temp)
        payload["roll_deg_avg"] = _mean(roll)
        payload["pitch_deg_avg"] = _mean(pitch)
        return payload

    if sensor == "ads1220":
        for field in (
            "mv",
            "mv_per_v",
            "mv_per_v_filtered",
            "strain_um_m",
            "strain_mm_m",
            "stress_mpa",
            "weight_g",
            "weight_kg",
        ):
            values = _numeric_values(samples, field)
            if values:
                payload[f"{field}_avg"] = _mean(values)
                payload[f"{field}_min"] = min(values)
                payload[f"{field}_max"] = max(values)
        return payload

    return payload


def build_snapshot_payload(
    device_id: str,
    snapshot: dict[str, Any],
    status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "device": device_id,
        "type": "snapshot",
        "timestamp": utc_iso(),
    }
    payload.update(snapshot)
    if status is not None:
        payload["status"] = status
    return payload
