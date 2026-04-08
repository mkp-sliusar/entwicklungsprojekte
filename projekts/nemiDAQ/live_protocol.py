from __future__ import annotations

"""Official i4M/nemione USB message protocol helpers.

This module is intentionally self-contained (no external 'cobs' dependency).

Protocol summary (see 2025-02-17 "Dokumentation der ... USB Datennachrichten"):

* All frames are COBS-encoded and delimited by a single 0x00 byte.
* id_byte layout: bits 4..7 = Sensor ID (SID), bits 0..3 = Message ID (MID).
* Config messages (MID=6) are 13 bytes (without COBS) and are sent from host -> device.
* Data messages are commonly 252 bytes (without COBS).
"""

from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np


# -----------------------------
# Message constants
# -----------------------------

LENGTH_DATA: int = 252
LENGTH_CONFIG: int = 13

MID_TELEMETRY_AD_OLD: int = 0
MID_INFO: int = 3
MID_CONFIG: int = 6
MID_TIMING: int = 7
MID_TELEMETRY_AD_NEW: int = 11


# -----------------------------
# UI presets (used by the app)
# -----------------------------

# Receiver RF bitrate codes (base config; firmware >= 01.12.2023)
RF_PRESETS: dict[str, int] = {
    "HS 2 Mbit/s": 1,
    "1 Mbit/s": 2,
    "LR 500 kbit/s": 3,
    "ULR 125 kbit/s": 4,
}

# AD ODR setting codes (Telemetry config + for interpreting sampling rates)
# Naming follows the documentation tables.
AF_ODR_PRESETS: dict[str, int] = {
    "Off": 0,
    "High Res / 9600": 1,
    "High Res / 6400": 2,
    "High Res / 4800": 3,
    "High Res / 3200": 4,
    "High Res / 2400": 5,
    "High Res / 1600": 6,
    "High Res / 800": 7,
    "High Res / 400": 8,
    "High Res / 200": 9,
    "High Res / 100": 10,
    "ULP / 100": 11,
    "ULP / 75": 12,
    "ULP / 50": 13,
    "ULP / 25": 14,
    "ULP / 1": 15,
}


# Gain-code mapping (used in channel_config_* and in tel_config_1/2)
GAIN_CODE_TO_GAIN_VALUE: dict[int, int] = {
    0: 0,
    1: 1,
    2: 4,
    3: 8,
    4: 16,
    5: 32,
    6: 64,
    7: 128,
}

GAIN_VALUE_TO_GAIN_CODE: dict[int, int] = {v: k for k, v in GAIN_CODE_TO_GAIN_VALUE.items() if v != 0}


# -----------------------------
# COBS codec (no external dep)
# -----------------------------


def cobs_encode(data: bytes) -> bytes:
    """COBS encode.

    Output does NOT include the 0x00 delimiter.
    """

    if not data:
        return b"\x01"

    out = bytearray()
    code_idx = 0
    out.append(0)  # placeholder
    code = 1

    for b in data:
        if b == 0:
            out[code_idx] = code
            code_idx = len(out)
            out.append(0)  # placeholder for next code
            code = 1
            continue

        out.append(b)
        code += 1
        if code == 0xFF:
            out[code_idx] = code
            code_idx = len(out)
            out.append(0)
            code = 1

    out[code_idx] = code
    return bytes(out)


def cobs_decode(data: bytes) -> bytes:
    """COBS decode.

    Expects a single COBS frame WITHOUT the 0x00 delimiter.
    """

    if not data:
        return b""

    out = bytearray()
    i = 0
    n = len(data)
    while i < n:
        code = data[i]
        if code == 0:
            raise ValueError("COBS decode: zero code")
        i += 1
        end = i + code - 1
        if end > n and code != 1:
            raise ValueError("COBS decode: code exceeds input")

        out.extend(data[i:end])
        i = end

        if code != 0xFF and i < n:
            out.append(0)

    return bytes(out)


# -----------------------------
# ID helpers
# -----------------------------


def make_id_byte(sensor_id: int, message_id: int) -> int:
    """id_byte layout (per i4M firmware): bits 0..3 MID, bits 4..7 SID."""

    return ((sensor_id & 0x0F) << 4) | (message_id & 0x0F)


def split_id_byte(id_byte: int) -> tuple[int, int]:
    """Return (sensor_id, message_id) from id_byte (bits 4..7 SID, bits 0..3 MID)."""

    mid = id_byte & 0x0F
    sid = (id_byte >> 4) & 0x0F
    return sid, mid


# -----------------------------
# Config message builders
# -----------------------------


def _build_config_payload(
    *,
    sleep_time: int,
    sample_time: int,
    word_2: int,
    byte_6: int,
    byte_7: int,
    byte_8: int,
    imu_acc_range: int,
    imu_gyr_range: int,
    imu_mag_range: int,
    sid: int,
) -> bytes:
    """Build 13-byte config payload (without COBS, without delimiter)."""

    msg = bytearray()
    msg += int(sleep_time & 0xFFFF).to_bytes(2, "little", signed=False)
    msg += int(sample_time & 0xFFFF).to_bytes(2, "little", signed=False)
    msg += int(word_2 & 0xFFFF).to_bytes(2, "little", signed=False)
    msg += bytes([
        int(byte_6) & 0xFF,
        int(byte_7) & 0xFF,
        int(byte_8) & 0xFF,
        int(imu_acc_range) & 0xFF,
        int(imu_gyr_range) & 0xFF,
        int(imu_mag_range) & 0xFF,
    ])
    msg += bytes([make_id_byte(sid, MID_CONFIG)])
    if len(msg) != LENGTH_CONFIG:
        raise ValueError(f"Config payload length mismatch: {len(msg)}")
    return bytes(msg)


def build_base_config_new(
    *,
    rf_speed_code: int,
    max_sensors_code: int,
    rf_channel: int,
) -> bytes:
    """Build base/receiver config for firmware >= 01.12.2023.

    Uses SID=0, MID=6.
    """

    payload = _build_config_payload(
        sleep_time=rf_speed_code,
        sample_time=max_sensors_code,
        word_2=0,
        byte_6=rf_channel,
        byte_7=251,
        byte_8=251,
        imu_acc_range=251,
        imu_gyr_range=251,
        imu_mag_range=251,
        sid=0,
    )
    return cobs_encode(payload) + b"\x00"


def build_base_config_old(*, rf_channel: int) -> bytes:
    """Build base/receiver config for firmware < 01.12.2023.

    Uses SID=0, MID=6.
    """

    payload = _build_config_payload(
        sleep_time=0,
        sample_time=0,
        word_2=0,
        byte_6=rf_channel,
        byte_7=251,
        byte_8=251,
        imu_acc_range=251,
        imu_gyr_range=251,
        imu_mag_range=251,
        sid=0,
    )
    return cobs_encode(payload) + b"\x00"


def build_telemetry_config(
    *,
    sid: int,
    sensor_mode: int,
    ad_odr_setting: int,
    gain_ch1_code: int,
    gain_ch2_code: int = 0,
    gain_ch3_code: int = 0,
    gain_ch4_code: int = 0,
    imu_odr_setting: int = 0,
    din_high_levels: Optional[Iterable[int]] = None,
    imu_acc_range: int = 0,
    imu_gyr_range: int = 0,
    imu_mag_range: int = 0,
    sleep_time: int = 0,
    sample_time: int = 0,
) -> bytes:
    """Build Telemetry-Module config (section "B" in the documentation).

    The receiver expects one such config per sensor node (SID=1..N).
    """

    # tel_config_0: 6x 2-bit fields for DIN1..DIN6 high-level
    lv = list(din_high_levels) if din_high_levels is not None else [0, 0, 0, 0, 0, 0]
    if len(lv) != 6:
        raise ValueError("din_high_levels must contain 6 entries")
    tel_config_0 = 0
    for i, v in enumerate(lv):
        if not (0 <= int(v) <= 3):
            raise ValueError("DIN high-level values must be 0..3")
        # DIN1 uses bits 11..10, DIN2 9..8, ..., DIN6 1..0
        shift = 10 - 2 * i
        tel_config_0 |= (int(v) & 0x03) << shift

    # Gains: doc uses a shared gain for channel 3/4 in tel_config_*.
    # If channel 4 differs, we still must pick a single value; prefer ch3.
    gain34_code = int(gain_ch3_code) & 0x07
    _ = gain_ch4_code  # unused but kept for signature symmetry

    tel_config_1 = ((int(gain_ch1_code) & 0x07) << 5) | ((int(gain_ch2_code) & 0x07) << 2) | ((gain34_code >> 1) & 0x03)
    tel_config_2 = ((gain34_code & 0x01) << 7) | ((int(ad_odr_setting) & 0x0F) << 3) | (int(imu_odr_setting) & 0x07)

    payload = _build_config_payload(
        sleep_time=sleep_time,
        sample_time=sample_time,
        word_2=tel_config_0,
        byte_6=sensor_mode,
        byte_7=tel_config_1,
        byte_8=tel_config_2,
        imu_acc_range=imu_acc_range,
        imu_gyr_range=imu_gyr_range,
        imu_mag_range=imu_mag_range,
        sid=sid,
    )
    return cobs_encode(payload) + b"\x00"


def send_frame(ser, frame: bytes) -> None:
    """Write a full frame (COBS + delimiter) to a pyserial Serial."""

    ser.write(frame)
    try:
        ser.flush()
    except Exception:
        # Some Serial implementations do not expose flush.
        pass


# -----------------------------
# Parsing / decoding helpers
# -----------------------------


def channel_gain_codes_from_channel_config(channel_config_1_2: int, channel_config_3_4: int) -> tuple[int, int, int, int]:
    """Return gain codes (0..7) for channels 1..4."""

    g1 = (int(channel_config_1_2) >> 3) & 0x07
    g2 = int(channel_config_1_2) & 0x07
    g3 = (int(channel_config_3_4) >> 3) & 0x07
    g4 = int(channel_config_3_4) & 0x07
    return g1, g2, g3, g4


def active_channels_from_gain_codes(gain_codes: tuple[int, int, int, int]) -> list[int]:
    return [i + 1 for i, gc in enumerate(gain_codes) if int(gc) != 0]


def decode_u24_be_offset_binary(u24_bytes: np.ndarray) -> np.ndarray:
    """Decode big-endian 24-bit offset-binary to signed int32.

    * u24_bytes: shape (N, 3), dtype uint8
    * returns int32 counts in range [-2^23, 2^23-1]
    """

    if u24_bytes.ndim != 2 or u24_bytes.shape[1] != 3:
        raise ValueError("u24_bytes must have shape (N, 3)")

    b0 = u24_bytes[:, 0].astype(np.uint32)
    b1 = u24_bytes[:, 1].astype(np.uint32)
    b2 = u24_bytes[:, 2].astype(np.uint32)
    raw = (b0 << 16) | (b1 << 8) | b2
    signed = raw.astype(np.int32) - (1 << 23)
    return signed


@dataclass(frozen=True)
class TelemetryADOld:
    """Legacy AD telemetry message (MID=0) used by older firmware.

    Layout based on legacy decoder in i4m_messages_handling.py.
    Provides the same convenience API as TelemetryADNew for plotting.
    """

    sensor_id: int
    message_id: int
    timestamp_us: int  # timestamp of the last contained sample (1 MHz)
    ch_1_2_config: int
    ch_3_4_config: int
    ch_samples: tuple[bytes, bytes, bytes, bytes]  # each 28 bytes = 7x uint32 little-endian
    voltage_raw: int
    rssi_raw: int

    @property
    def gain_codes(self) -> tuple[int, int, int, int]:
        return channel_gain_codes_from_channel_config(self.ch_1_2_config, self.ch_3_4_config)

    @property
    def active_channels(self) -> list[int]:
        return active_channels_from_gain_codes(self.gain_codes)

    def extract_channel_mvv(self, channel: int) -> tuple[np.ndarray, int]:
        """Return (mV/V samples, n_sample_sets) for legacy message.

        Legacy messages contain exactly 7 samples per enabled channel.
        """
        if channel not in (1, 2, 3, 4):
            raise ValueError("channel must be 1..4")

        g_codes = self.gain_codes
        gain_code = int(g_codes[channel - 1])
        gain = GAIN_CODE_TO_GAIN_VALUE.get(gain_code, 0)
        if gain == 0:
            return np.zeros(0, dtype=np.float64), 0

        # unpack 7 uint32 little-endian; ADC uses 24-bit offset-binary stored in u32
        import struct
        raw_u32 = np.array(struct.unpack("<7I", self.ch_samples[channel - 1]), dtype=np.uint32)
        raw24 = raw_u32 & np.uint32(0x00FFFFFF)
        counts = raw24.astype(np.int32) - (1 << 23)
        scale = 1000.0 / ((2 ** 23) * float(gain))
        return counts.astype(np.float64) * scale, int(raw_u32.size)
@dataclass(frozen=True)
class TelemetryADNew:
    sensor_id: int
    message_id: int
    timestamp_us: int  # timestamp of the last contained sample (1 MHz)
    ad_data: bytes  # 198 bytes
    ad_config: int
    channel_config_1_2: int
    channel_config_3_4: int
    voltage_raw: int
    rssi_raw: int

    @property
    def gain_codes(self) -> tuple[int, int, int, int]:
        return channel_gain_codes_from_channel_config(self.channel_config_1_2, self.channel_config_3_4)

    @property
    def active_channels(self) -> list[int]:
        return active_channels_from_gain_codes(self.gain_codes)

    def extract_channel_mvv(self, channel: int) -> tuple[np.ndarray, int]:
        """Return (mV/V samples, n_sample_sets).

        Channel numbering: 1..4.
        """

        if channel not in (1, 2, 3, 4):
            raise ValueError("channel must be 1..4")

        g_codes = self.gain_codes
        gain_code = int(g_codes[channel - 1])
        gain = GAIN_CODE_TO_GAIN_VALUE.get(gain_code, 0)
        active = self.active_channels
        if gain == 0 or channel not in active:
            return np.zeros(0, dtype=np.float64), 0

        n_ch = len(active)
        bytes_per_set = 3 * n_ch

        ad = self.ad_data
        # For 4 channels only 192 bytes are used; the remaining 6 are padding zeros.
        if n_ch == 4:
            ad = ad[:192]

        n_sets = len(ad) // bytes_per_set
        used = ad[: n_sets * bytes_per_set]
        a = np.frombuffer(used, dtype=np.uint8).reshape(n_sets, n_ch, 3)

        idx = active.index(channel)
        u24 = a[:, idx, :]
        counts = decode_u24_be_offset_binary(u24)
        scale = 1000.0 / ((2 ** 23) * float(gain))
        return counts.astype(np.float64) * scale, n_sets


def parse_message_id(raw: bytes) -> tuple[int, int]:
    """Return (sensor_id, message_id) from a decoded raw message."""

    if not raw:
        return 0, -1
    sid, mid = split_id_byte(raw[-1])
    return sid, mid


def parse_info_message(raw: bytes) -> Optional[str]:
    """Parse an info (MID=3) message to a python string."""

    if len(raw) != LENGTH_DATA:
        return None
    sid, mid = split_id_byte(raw[-1])
    if mid != MID_INFO:
        return None

    # Format: timestamp (0..3), then ASCII string (4..249), padded with 0.
    info_bytes = raw[4:250]
    info_bytes = info_bytes.split(b"\x00", 1)[0]
    try:
        return info_bytes.decode("ascii", errors="replace")
    except Exception:
        return None


def parse_telemetry_ad_new(raw: bytes) -> Optional[TelemetryADNew]:
    if len(raw) != LENGTH_DATA:
        return None

    sid, mid = split_id_byte(raw[-1])
    if mid != MID_TELEMETRY_AD_NEW:
        return None

    ts = int.from_bytes(raw[0:4], "little", signed=False)
    ad_data = raw[10:208]
    ad_config = raw[208]
    cc12 = raw[209]
    cc34 = raw[210]
    voltage = raw[249]
    rssi = raw[250]
    return TelemetryADNew(
        sensor_id=sid,
        message_id=mid,
        timestamp_us=ts,
        ad_data=ad_data,
        ad_config=int(ad_config),
        channel_config_1_2=int(cc12),
        channel_config_3_4=int(cc34),
        voltage_raw=int(voltage),
        rssi_raw=int(rssi),
    )


def _split_id_byte_legacy_compatible(id_byte: int) -> tuple[int, int]:
    """Some older firmware/tools used swapped SID/MID bit order.

    Prefer the documented layout (SID low nibble, MID high nibble), but if that
    does not match any known MID, try the swapped interpretation.
    """
    sid, mid = split_id_byte(id_byte)
    if mid in (MID_TELEMETRY_AD_OLD, MID_INFO, MID_CONFIG, MID_TIMING, MID_TELEMETRY_AD_NEW):
        return sid, mid
    # swapped interpretation
    sid2 = (id_byte >> 4) & 0x0F
    mid2 = id_byte & 0x0F
    return sid2, mid2


def parse_telemetry_ad_old(raw: bytes) -> Optional[TelemetryADOld]:
    """Parse legacy telemetry AD message (MID=0).

    This parser is tolerant to legacy swapped SID/MID bit ordering.
    """
    if len(raw) != LENGTH_DATA:
        return None

    sid, mid = _split_id_byte_legacy_compatible(raw[-1])
    if mid != MID_TELEMETRY_AD_OLD:
        return None

    import struct

    # timestamps: 7x uint32, then time_t1 (uint32) at bytes 28..32 (ignored here)
    ts7 = struct.unpack("<7I", raw[0:28])
    ts_last = int(ts7[-1])

    ch1 = raw[32:60]
    ch2 = raw[60:88]
    ch3 = raw[88:116]
    ch4 = raw[116:144]

    ch_1_2_config = int(raw[247])
    ch_3_4_config = int(raw[248])
    voltage = int(raw[249])
    rssi = int(raw[250])

    return TelemetryADOld(
        sensor_id=int(sid),
        message_id=int(mid),
        timestamp_us=ts_last,
        ch_1_2_config=ch_1_2_config,
        ch_3_4_config=ch_3_4_config,
        ch_samples=(ch1, ch2, ch3, ch4),
        voltage_raw=voltage,
        rssi_raw=rssi,
    )


# -----------------------------
# Receiver init/shutdown
# -----------------------------


def _read_frames_for_seconds(ser, seconds: float, max_bytes: int = 200_000) -> bytes:
    """Read raw bytes from serial for a limited time (best-effort)."""

    import time

    buf = bytearray()
    t0 = time.time()
    while (time.time() - t0) < seconds and len(buf) < max_bytes:
        chunk = ser.read(4096)
        if chunk:
            buf += chunk
        else:
            time.sleep(0.01)
    return bytes(buf)


def _extract_cobs_frames(stream_bytes: bytes) -> list[bytes]:
    """Split by 0x00 and return non-empty frame payloads (still COBS-encoded)."""

    if not stream_bytes:
        return []
    parts = stream_bytes.split(b"\x00")
    return [p for p in parts if p]


def detect_receiver_new_firmware(ser) -> Optional[bool]:
    """Heuristic detection of receiver firmware generation.

    Returns:
        True  -> likely >= 01.12.2023
        False -> likely <  01.12.2023
        None  -> unknown
    """

    # Read a short burst of info output.
    raw = _read_frames_for_seconds(ser, seconds=0.6)
    frames = _extract_cobs_frames(raw)

    infos: list[str] = []
    for f in frames[-80:]:
        try:
            dec = cobs_decode(f)
        except Exception:
            continue
        s = parse_info_message(dec)
        if s:
            infos.append(s)

    if not infos:
        return None

    blob = "\n".join(infos)

    # Explicit hint in the documentation.
    if "Firmware" in blob and "2023" in blob:
        # If the device explicitly prints a build date >= 2023-12-01, assume new.
        if "2023-12" in blob or "2024" in blob or "2025" in blob:
            return True

    return None


def init_receiver(
    ser,
    *,
    rf_channel: int,
    rf_speed_code: int,
    max_sensors_code: int,
    ad_odr_setting: int,
    gain_ch1_value: int,
    gain_ch2_value: int | None = None,
    selected_channel: int = 1,
    prefer_new_firmware: bool = True,
    # new IMU defaults forwarded into build_telemetry_config
    imu_odr_setting_default: int = 2,
    imu_acc_range_default: int = 2,
    imu_gyr_range_default: int = 2,
    imu_mag_range_default: int = 3,
) -> dict:
    """Initialize receiver and all sensor nodes according to the official documentation.

    This sends:
      1) base config (SID=0, MID=6)
      2) N per-sensor config messages (SID=1..N, MID=6)

    Returns a small dict with the actually used parameters.
    """

    import time

    rf_channel = int(rf_channel)
    rf_channel = max(0, min(80, rf_channel))
    rf_speed_code = int(rf_speed_code)
    max_sensors_code = int(max_sensors_code)
    ad_odr_setting = int(ad_odr_setting) & 0x0F

    # Gain selection: clamp to the supported set.
    gain_ch1_value = int(gain_ch1_value)
    allowed = np.array([1, 4, 8, 16, 32, 64, 128], dtype=np.int32)
    gain_ch1_value = int(allowed[np.argmin(np.abs(allowed - gain_ch1_value))])
    gain_ch1_code = int(GAIN_VALUE_TO_GAIN_CODE.get(gain_ch1_value, 7))

    if gain_ch2_value is None:
        gain_ch2_value = gain_ch1_value
    gain_ch2_value = int(allowed[np.argmin(np.abs(allowed - int(gain_ch2_value)))])
    gain_ch2_code = int(GAIN_VALUE_TO_GAIN_CODE.get(int(gain_ch2_value), 7))

    # Heuristic: if we can detect old firmware, switch; otherwise use prefer_new_firmware.
    fw_is_new = detect_receiver_new_firmware(ser)
    if fw_is_new is None:
        fw_is_new = bool(prefer_new_firmware)

    # Send base config.
    if fw_is_new:
        send_frame(
            ser,
            build_base_config_new(
                rf_speed_code=rf_speed_code,
                max_sensors_code=max_sensors_code,
                rf_channel=rf_channel,
            ),
        )
    else:
        send_frame(ser, build_base_config_old(rf_channel=rf_channel))
    time.sleep(0.60)

    # Determine how many per-sensor configs are required.
    if max_sensors_code == 1:
        n_nodes = 3
    elif max_sensors_code == 2:
        n_nodes = 8
    elif max_sensors_code == 3:
        n_nodes = 1
    else:
        n_nodes = 8

    # Send per-sensor configs. We configure telemetry modules.
    for sid in range(1, n_nodes + 1):
        cfg = build_telemetry_config(
            sid=sid,
            sensor_mode=1,  # continuous sampling
            ad_odr_setting=ad_odr_setting,
            # enable CH1+CH2
            gain_ch1_code=gain_ch1_code,
            gain_ch2_code=gain_ch2_code,
            gain_ch3_code=0,
            gain_ch4_code=0,
            # IMU settings (forwarded from UI/worker)
            imu_odr_setting=imu_odr_setting_default,
            imu_acc_range=imu_acc_range_default,
            imu_gyr_range=imu_gyr_range_default,
            imu_mag_range=imu_mag_range_default,
            din_high_levels=[0, 0, 0, 0, 0, 0],
            # even in continuous mode, keep sane timed-mode defaults (matches official app)
            sleep_time=0,
            sample_time=0,
        )
        send_frame(ser, cfg)
        # The official PC software waits ~500 ms between config messages.
        # Shorter delays can cause the receiver to miss or ignore configs on some FW/bridges.
        time.sleep(0.60)

    return {
        "rf_channel": rf_channel,
        "rf_speed_code": rf_speed_code,
        "max_sensors_code": max_sensors_code,
        "ad_odr_setting": ad_odr_setting,
        "gain_ch1_value": gain_ch1_value,
        "gain_ch1_code": gain_ch1_code,
        "gain_ch2_code": gain_ch2_code,
        "selected_channel": int(selected_channel),
        "firmware_new": bool(fw_is_new),
        "n_nodes": int(n_nodes),
    }


def stop_receiver(ser, *, max_sensors_code: int = 2) -> None:
    """Best-effort stop: put sensor nodes into sleep mode.

    The system will also stop streaming when the serial port is closed.
    """

    import time

    if max_sensors_code == 1:
        n_nodes = 3
    elif max_sensors_code == 2:
        n_nodes = 8
    elif max_sensors_code == 3:
        n_nodes = 1
    else:
        n_nodes = 8

    for sid in range(1, n_nodes + 1):
        cfg = build_telemetry_config(
            sid=sid,
            sensor_mode=0,  # sleep
            ad_odr_setting=0,
            gain_ch1_code=0,
            gain_ch2_code=0,
            gain_ch3_code=0,
            gain_ch4_code=0,
            imu_odr_setting=0,
            imu_acc_range=0,
            imu_gyr_range=0,
            imu_mag_range=0,
            sleep_time=0,
            sample_time=0,
        )
        try:
            send_frame(ser, cfg)
        except Exception:
            return
        time.sleep(0.01)


# Backwards-compat aliases (older app versions used these names)
send_cmd = send_frame
