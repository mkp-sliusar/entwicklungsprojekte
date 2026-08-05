from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any

from live_protocol import (
    LENGTH_DATA,
    split_id_byte,
    MID_TELEMETRY_AD_NEW,
    parse_telemetry_ad_new,
    parse_telemetry_ad_old,
)


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def battery_percent_from_voltage(v: Optional[float]) -> Optional[int]:
    if v is None:
        return None
    pct = (float(v) - 3.0) / (4.2 - 3.0) * 100.0
    pct = _clamp(pct, 0.0, 100.0)
    return int(round(pct))


def rssi_bars_from_rssi_raw(rssi_raw: Optional[int]) -> Optional[int]:
    if rssi_raw is None:
        return None
    r = int(rssi_raw)
    if r <= 50:
        return 4
    if r <= 65:
        return 3
    if r <= 80:
        return 2
    if r <= 90:
        return 1
    return 0


@dataclass
class StatusSnapshot:
    sensor_id: Optional[int] = None
    voltage_raw: Optional[int] = None
    voltage_v: Optional[float] = None
    battery_percent: Optional[int] = None
    rssi_raw: Optional[int] = None
    rssi_dbm: Optional[int] = None
    rssi_bars: Optional[int] = None
    temperature_raw: Optional[int] = None
    temperature_c: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "sensor_id": self.sensor_id,
            "voltage_raw": self.voltage_raw,
            "voltage_v": self.voltage_v,
            "battery_percent": self.battery_percent,
            "rssi_raw": self.rssi_raw,
            "rssi_dbm": self.rssi_dbm,
            "rssi_bars": self.rssi_bars,
            "temperature_raw": self.temperature_raw,
            "temperature_c": self.temperature_c,
        }


class DeviceStatus:
    def __init__(self) -> None:
        self._snap = StatusSnapshot()

    def snapshot(self) -> StatusSnapshot:
        return self._snap

    def reset(self) -> None:
        self._snap = StatusSnapshot()

    def feed_raw(self, raw: bytes) -> Optional[StatusSnapshot]:
        """Feed a single COBS-decoded 252-byte message payload.

        Returns the updated snapshot when this frame contained status fields.
        Returns None if the frame was not recognized.
        """
        if not raw or len(raw) != LENGTH_DATA:
            return None

        # determine sid/mid from id byte (last byte)
        id_byte = int(raw[-1])
        sid, mid = split_id_byte(id_byte)

        # --- IMU frame (MID == 12) : explicit handling first ---
        # Firmware sends IMU frames as 252-byte messages; temperature is at bytes 8..9 (int16 little-endian).
        if mid == 12 and len(raw) == 252:
            try:
                temp_raw = int.from_bytes(raw[8:10], "little", signed=True)
                # update snapshot fields we can extract
                self._snap.sensor_id = int(sid)
                self._snap.temperature_raw = int(temp_raw)
                # best-effort conversion: 1 LSB = 0.1 °C (adjust if documentation differs)
                self._snap.temperature_c = float(temp_raw) * 0.1
            except Exception:
                # leave unchanged on parse error
                pass
            return self._snap

        # Preferred path: use the provided AD-new parser (handles documented layout for telemetry AD messages).
        t_new = parse_telemetry_ad_new(raw)
        if t_new is not None:
            self._update_common(
                sensor_id=t_new.sensor_id,
                voltage_raw=t_new.voltage_raw,
                rssi_raw=t_new.rssi_raw,
            )
            # AD-new includes temperature bytes in 8..9 (int16 little-endian) for some fw variants.
            try:
                temp_raw = int.from_bytes(raw[8:10], "little", signed=True)
                # if temp_raw is valid, update temperature fields
                self._snap.temperature_raw = int(temp_raw)
                self._snap.temperature_c = float(temp_raw) * 0.1
            except Exception:
                pass
            return self._snap

        # Fallback: AD-old parser (legacy layout)
        t_old = parse_telemetry_ad_old(raw)
        if t_old is not None:
            self._update_common(
                sensor_id=t_old.sensor_id,
                voltage_raw=t_old.voltage_raw,
                rssi_raw=t_old.rssi_raw,
            )
            # Legacy AD typically does not contain temperature
            return self._snap

        # Some firmwares place voltage/rssi near the end — handle that last-resort case
        try:
            v_b = int(raw[249])
            r_b = int(raw[250])
            if v_b != 0 or r_b != 0:
                self._update_common(sensor_id=sid, voltage_raw=v_b, rssi_raw=r_b)
                return self._snap
        except Exception:
            pass

        return None

    def _update_common(self, *, sensor_id: int, voltage_raw: int, rssi_raw: int) -> None:
        self._snap.sensor_id = int(sensor_id)
        try:
            self._snap.voltage_raw = int(voltage_raw)
            self._snap.voltage_v = float(self._snap.voltage_raw) / 10.0
        except Exception:
            self._snap.voltage_raw = None
            self._snap.voltage_v = None

        self._snap.battery_percent = battery_percent_from_voltage(self._snap.voltage_v)

        try:
            self._snap.rssi_raw = int(rssi_raw)
            self._snap.rssi_dbm = -int(rssi_raw)
            self._snap.rssi_bars = rssi_bars_from_rssi_raw(int(rssi_raw))
        except Exception:
            self._snap.rssi_raw = None
            self._snap.rssi_dbm = None
            self._snap.rssi_bars = None
