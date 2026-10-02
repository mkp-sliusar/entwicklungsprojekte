from __future__ import annotations

import logging
from typing import Any

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions


class InfluxWriter:
    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        timeout_ms: int = 5000,
        batch_size: int = 500,
        flush_interval_ms: int = 1000,
    ):
        self.bucket = bucket
        self.org = org
        self.log = logging.getLogger("influxdb")

        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout_ms)
        self.write_api = self.client.write_api(
            write_options=WriteOptions(
                batch_size=int(batch_size),
                flush_interval=int(flush_interval_ms),
                jitter_interval=250,
                retry_interval=1000,
                max_retries=3,
                max_retry_delay=10000,
                exponential_base=2,
            )
        )

    def write_payload(
        self,
        measurement: str,
        device_id: str,
        payload: dict[str, Any],
        timestamp_s: float | None = None,
        tags: dict[str, Any] | None = None,
    ) -> None:
        point = Point(measurement).tag("device", device_id)
        if tags:
            for key, value in tags.items():
                if value is not None:
                    point = point.tag(str(key), str(value))

        for key, value in _flatten_payload(payload).items():
            if key in {"device", "type", "sensor", "timestamp"}:
                continue
            if value is None:
                continue
            if isinstance(value, bool):
                point = point.field(key, int(value))
            elif isinstance(value, int):
                point = point.field(key, int(value))
            elif isinstance(value, float):
                point = point.field(key, float(value))
            elif isinstance(value, str):
                point = point.field(key, value)

        if timestamp_s is not None:
            point = point.time(
                int(timestamp_s * 1_000_000_000),
                WritePrecision.NS,
            )

        try:
            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

        except Exception as e:
            self.log.warning(
                "Influx write failed: %s",
                e
            )

    # Backward-compatible helpers used by the old app.py.
    def write_mpu9250(
        self,
        device_id,
        roll_deg,
        pitch_deg,
        vibration_rms,
        vibration_peak,
        vibration_z_rms,
        vibration_z_peak,
        vibration_z,
        frequency_hz,
        temperature,
        sample_rate,
    ):
        self.write_payload(
            "mpu9250",
            device_id,
            {
                "roll_deg": roll_deg,
                "pitch_deg": pitch_deg,
                "vibration_rms": vibration_rms,
                "vibration_peak": vibration_peak,
                "vibration_z_rms": vibration_z_rms,
                "vibration_z_peak": vibration_z_peak,
                "vibration_z": vibration_z,
                "frequency_hz": frequency_hz,
                "temperature": temperature,
                "sample_rate": sample_rate,
            },
        )

    def write_ads1220(
        self,
        device_id,
        raw,
        raw_offset,
        mv,
        mv_per_v,
        mv_per_v_filtered,
        strain_um_m,
        strain_mm_m,
        stress_mpa,
        weight_g,
    ):
        self.write_payload(
            "ads1220",
            device_id,
            {
                "raw": raw,
                "raw_offset": raw_offset,
                "mv": mv,
                "mv_per_v": mv_per_v,
                "mv_per_v_filtered": mv_per_v_filtered,
                "strain_um_m": strain_um_m,
                "strain_mm_m": strain_mm_m,
                "stress_mpa": stress_mpa,
                "weight_g": weight_g,
            },
        )

    def close(self) -> None:
        try:
            self.write_api.flush()
        except Exception:
            self.log.exception("Influx flush failed")
        try:
            self.client.close()
        except Exception:
            self.log.exception("Influx close failed")


def _flatten_payload(payload: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in payload.items():
        name = f"{prefix}_{key}" if prefix else str(key)
        if isinstance(value, dict):
            result.update(_flatten_payload(value, name))
        else:
            result[name] = value
    return result
