from __future__ import annotations

import csv
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import serial
from serial.tools import list_ports

from PySide6 import QtCore, QtGui, QtWidgets
import pyqtgraph as pg

from device_status import DeviceStatus

from live_protocol import (
    split_id_byte,
    RF_PRESETS,
    AF_ODR_PRESETS,
    cobs_decode,
    init_receiver,
    parse_info_message,
    parse_telemetry_ad_new,
    parse_telemetry_ad_old,
    stop_receiver,
)
from nemidaq_theme import set_plot_theme
from trigger_recorder import AutoTriggerRecorder, TriggerConfig


class CobsStreamDecoder:
    """Streaming splitter for COBS-delimited (0x00) frames."""

    def __init__(self):
        self.buf = bytearray()
        self.bad_frames = 0
        self.good_frames = 0

    def feed(self, chunk: bytes) -> None:
        if chunk:
            self.buf.extend(chunk)
        # prevent unbounded growth
        if len(self.buf) > 1_000_000:
            del self.buf[:-100_000]

    def pop_frame(self) -> bytes | None:
        """Return next COBS frame payload (without delimiter) or None."""

        while True:
            try:
                pos = self.buf.index(0)
            except ValueError:
                return None
            frame = bytes(self.buf[:pos])
            del self.buf[: pos + 1]
            if not frame:
                # empty frame between delimiters
                continue
            self.good_frames += 1
            return frame


class BootTextSniffer:
    """Collect raw bytes to detect plain-ASCII boot/info text (not COBS framed)."""

    def __init__(self, max_len: int = 8192):
        self.buf = bytearray()
        self.max_len = int(max_len)

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        self.buf.extend(chunk)
        if len(self.buf) > self.max_len:
            del self.buf[: len(self.buf) - self.max_len]

    def find_ascii(self, needle: str) -> bool:
        try:
            txt = self.buf.decode("ascii", errors="ignore")
        except Exception:
            return False
        return needle in txt

    def pop_lines(self) -> list[str]:
        """Best-effort: return complete ASCII lines seen so far."""
        try:
            txt = self.buf.decode("ascii", errors="ignore")
        except Exception:
            return []
        if "\n" not in txt and "\r" not in txt:
            return []
        # normalize line breaks
        txt = txt.replace("\r\n", "\n").replace("\r", "\n")
        parts = txt.split("\n")
        # keep last partial line
        complete = parts[:-1]
        tail = parts[-1]
        self.buf = bytearray(tail.encode("ascii", errors="ignore"))
        return [ln.strip() for ln in complete if ln.strip()]


@dataclass
class _NodeState:
    prev_ts_u32: int | None = None  # timestamp of last sample (u32, 1 MHz)
    prev_y1: np.ndarray | None = None
    prev_y2: np.ndarray | None = None
    prev_n: int = 0

    seg_counter0: int | None = None
    seg_t0: float = 0.0

    dt_hist_us: deque = field(default_factory=lambda: deque(maxlen=200))
    dt_med_us: float = 660_000.0  # reasonable default (100 Hz * 66)

    last_emitted_time: float = 0.0


class SerialWorker(QtCore.QThread):
    samples = QtCore.Signal(object, object, object)  # t_s, y1, y2
    segment_break = QtCore.Signal()          # insert NaN break in plot
    status = QtCore.Signal(str)
    status_snapshot = QtCore.Signal(object)  # StatusSnapshot

    def __init__(
        self,
        port: str,
        baud: int,
        rf_channel: int,
        rf_speed_code: int,
        max_sensors_code: int,
        gain: float,
        sign: float,
        ad_odr_setting: int,
        node_select: int = 0,  # 0=Auto, 1..8 fixed
        imu_odr_setting: int = 0,
        imu_acc_range: int = 0,
        imu_gyr_range: int = 0,
        imu_mag_range: int = 0,
        prefer_newfw: bool = True,
        gap_s: float = 0.5,
        dt_abs_reset_s: float = 5.0,
        dt_reset_factor: float = 20.0,
        parent=None,
    ):
        super().__init__(parent)
        self.port = port
        self.baud = int(baud)
        self.rf_channel = int(rf_channel)
        self.rf_speed_code = int(rf_speed_code)
        self.max_sensors_code = int(max_sensors_code)
        self.gain = float(gain)
        self.sign = float(sign)
        self.ad_odr_setting = int(ad_odr_setting)

        # IMU configuration forwarded to init_receiver
        self.imu_odr_setting = int(imu_odr_setting)
        self.imu_acc_range = int(imu_acc_range)
        self.imu_gyr_range = int(imu_gyr_range)
        self.imu_mag_range = int(imu_mag_range)

        self.prefer_newfw = bool(prefer_newfw)

        self.node_select = int(node_select)

        self.gap_s = float(gap_s)
        self.dt_abs_reset_s = float(dt_abs_reset_s)
        self.dt_reset_factor = float(dt_reset_factor)

        self._stop = False

        # for UX warnings
        self._last_warn_t = 0.0

    def stop(self):
        self._stop = True

    def run(self):
        # per-node timing state (kept even when Auto node switches)
        states: dict[int, _NodeState] = {}

        # auto node selection
        active_node: int | None = None
        if 1 <= self.node_select <= 8:
            active_node = int(self.node_select)

        node_counts: dict[int, int] = {}

        decoder = CobsStreamDecoder()
        dev_status = DeviceStatus()
        last_status_emit_t: float = 0.0

        sniffer = BootTextSniffer()

        try:
            with serial.Serial(self.port, self.baud, timeout=0.2, write_timeout=1.0) as ser:
                self.status.emit(f"Opened {ser.port} @ {self.baud}. Connected. Waiting…")
                # Give the receiver a moment after opening the COM port (matches official app behavior)
                time.sleep(1.2)

                # IMPORTANT: wait until the receiver reports it's ready to accept config messages.
                # Per documentation, the receiver prints "Waiting for Config Messages to start …" and
                # only then should the host send configuration frames.
                ready = False
                try:
                    wait_deadline = time.monotonic() + 8.0
                    while time.monotonic() < wait_deadline and not ready:
                        chunk0 = ser.read(4096)
                        if chunk0:
                            sniffer.feed(chunk0)
                            decoder.feed(chunk0)

                        # Some firmware versions print plain ASCII boot/info lines without COBS framing.
                        # Detect readiness that way as well.
                        if sniffer.find_ascii("Waiting for Config Messages"):
                            ready = True
                            break

                        for ln in sniffer.pop_lines():
                            # Forward a small subset to status, helps debugging device boot state.
                            if any(k in ln for k in ("Waiting", "RF", "Channel", "Config", "Start", "Stop", "Error")):
                                self.status.emit(ln)
                        while True:
                            frame0 = decoder.pop_frame()
                            if frame0 is None:
                                break
                            try:
                                raw0 = cobs_decode(frame0)
                            except Exception:
                                continue
                            info0 = parse_info_message(raw0)
                            if info0:
                                self.status.emit(info0)
                                if "Waiting for Config Messages" in info0:
                                    ready = True
                                    break
                except Exception:
                    ready = False

                if not ready:
                    self.status.emit(
                        "Warning: receiver did not print 'Waiting for Config Messages…' before init; continuing anyway."
                    )

                # Optional: clear any remaining boot chatter right before we send configs.
                try:
                    ser.reset_input_buffer()
                except Exception:
                    pass

                self.status.emit("Initializing receiver…")
                params = init_receiver(
                    ser,
                    prefer_new_firmware=self.prefer_newfw,
                    rf_channel=self.rf_channel,
                    rf_speed_code=self.rf_speed_code,
                    max_sensors_code=self.max_sensors_code,
                    ad_odr_setting=self.ad_odr_setting,
                    gain_ch1_value=int(round(self.gain)),
                    gain_ch2_value=int(round(self.gain)),
                    selected_channel=1,
                    imu_odr_setting_default=self.imu_odr_setting,
                    imu_acc_range_default=self.imu_acc_range,
                    imu_gyr_range_default=self.imu_gyr_range,
                    imu_mag_range_default=self.imu_mag_range,
                )
                fw = "new" if params.get("firmware_new") else "old"
                self.status.emit(
                    f"Receiver initialized (fw={fw}, rf_ch={params.get('rf_channel')}, "
                    f"rf_speed={params.get('rf_speed_code')}, nodes={params.get('n_nodes')}). Streaming…"
                )

                # watchdog
                last_any_data_t = time.monotonic()
                last_selected_data_t = last_any_data_t

                # MID debug counters
                mid_counts: dict[int, int] = {}
                last_mid_report_t: float = time.monotonic()

                while not self._stop:
                    now = time.monotonic()

                    chunk = ser.read(4096)
                    if chunk:
                        sniffer.feed(chunk)
                    decoder.feed(chunk)

                    # If device emits raw ASCII lines (no COBS), surface them.
                    for ln in sniffer.pop_lines():
                        if any(k in ln for k in ("Waiting", "RF", "Channel", "Config", "Start", "Stop", "Error")):
                            self.status.emit(ln)

                    # drain frames
                    while True:
                        frame = decoder.pop_frame()
                        if frame is None:
                            break

                        try:
                            raw = cobs_decode(frame)
                        except Exception:
                            decoder.bad_frames += 1
                            # If this happens a lot, framing (0x00 delimiters) or COBS is wrong.
                            if decoder.bad_frames in (1, 5, 20) or decoder.bad_frames % 100 == 0:
                                self.status.emit(f"COBS decode failed (bad_frames={decoder.bad_frames}, frame_len={len(frame)})")
                            continue

                        # Extract device status (battery / RSSI / temperature) from 252-byte frames
                        snap = dev_status.feed_raw(raw)
                        if snap is not None:
                            # Throttle UI updates (avoid flooding)
                            if (now - last_status_emit_t) > 0.2:
                                self.status_snapshot.emit(snap)
                                last_status_emit_t = now

                        # Basic frame classification by length + id_byte.
                        # According to the documentation, decoded lengths are typically 13 (config) or 252 (data/info).
                        if len(raw) not in (13, 223, 252):
                            # 223 = base message (MID=10) per doc; we don't plot it but it's useful for debugging.
                            if len(raw) > 0:
                                sid_dbg, mid_dbg = split_id_byte(raw[-1])
                                self.status.emit(f"RX unexpected length={len(raw)} sid={sid_dbg} mid={mid_dbg}")
                            else:
                                self.status.emit("RX empty decoded frame")
                        else:
                            sid_dbg, mid_dbg = split_id_byte(raw[-1])
                            mid_counts[mid_dbg] = mid_counts.get(mid_dbg, 0) + 1
                            # Emit a short MID summary occasionally
                            if (now - last_mid_report_t) > 2.0 and sum(mid_counts.values()) > 0:
                                summary = ", ".join(f"{k}:{mid_counts[k]}" for k in sorted(mid_counts.keys()))
                                self.status.emit(f"RX MIDs: {summary}")
                                last_mid_report_t = now

                        # Info messages (ASCII)
                        info = parse_info_message(raw)
                        if info:
                            # Avoid spamming: only forward a subset of lines as status.
                            if any(k in info for k in ("Waiting", "RF", "Channel", "Config", "Start", "Stop", "Error")):
                                self.status.emit(info)
                            continue

                        # Debug: surface unknown message IDs during bring-up.
                        # This helps detect whether we're receiving *any* RF data at all.
                        try:
                            idb = int(raw[-1])
                            sid = idb & 0x0F
                            mid = (idb >> 4) & 0x0F
                        except Exception:
                            sid, mid = -1, -1

                        msg = parse_telemetry_ad_new(raw)
                        if msg is None:
                            msg = parse_telemetry_ad_old(raw)
                        if msg is None:
                            if 0 <= sid <= 15 and 0 <= mid <= 15:
                                # Only show occasionally to avoid spamming.
                                if now - self._last_warn_t > 2.0:
                                    self.status.emit(f"RX frame: SID={sid} MID={mid} len={len(raw)} (not parsed)")
                                    self._last_warn_t = now
                                    # any valid decoded frame counts as activity (prevents unnecessary reinit)
                                    last_any_data_t = now
                            continue

                        node_id = int(msg.sensor_id) if msg.sensor_id is not None else 0

                        # Extract channels in mV/V (scaled based on channel_config gains)
                        y1_mvv, n1 = msg.extract_channel_mvv(channel=1)
                        y2_mvv, n2 = msg.extract_channel_mvv(channel=2)

                        # Choose a primary channel for timing (prefer CH1, fallback to CH2)
                        if n1 > 0 and y1_mvv.size > 0:
                            n_sets = int(n1)
                            y_primary = y1_mvv
                        elif n2 > 0 and y2_mvv.size > 0:
                            n_sets = int(n2)
                            y_primary = y2_mvv
                        else:
                            continue

                        y1 = (y1_mvv * float(self.sign)) if (n1 > 0 and y1_mvv.size > 0) else None
                        y2 = (y2_mvv * float(self.sign)) if (n2 > 0 and y2_mvv.size > 0) else None
                        c = int(msg.timestamp_us) & 0xFFFFFFFF
                        n_curr = int(y_primary.shape[0])

                        last_any_data_t = now

                        # Auto node selection: pick the most frequent node observed early on
                        if active_node is None and node_id in range(1, 9):
                            node_counts[node_id] = node_counts.get(node_id, 0) + 1
                            total = sum(node_counts.values())
                            best = max(node_counts, key=node_counts.get)
                            if node_counts[best] >= 3 or total >= 10:
                                active_node = int(best)
                                self.status.emit(f"Auto node selected: Node {active_node}")
                                last_selected_data_t = now

                        st = states.setdefault(node_id, _NodeState())

                        # First packet for this node initializes its segment.
                        if st.prev_ts_u32 is None:
                            st.prev_ts_u32 = c
                            st.prev_y1 = (y1 if y1 is not None else y_primary)
                            st.prev_y2 = y2
                            st.prev_n = n_curr
                            st.seg_counter0 = c
                            st.seg_t0 = 0.0
                            st.last_emitted_time = 0.0
                            continue

                        if st.prev_y1 is None or st.seg_counter0 is None or st.prev_n <= 0:
                            st.prev_ts_u32 = c
                            st.prev_y1 = (y1 if y1 is not None else y_primary)
                            st.prev_y2 = y2
                            st.prev_n = n_curr
                            st.seg_counter0 = c
                            st.seg_t0 = 0.0
                            st.last_emitted_time = 0.0
                            continue

                        # dt between packet timestamps (mod 2^32) for THIS node
                        dt_us = float(((int(c) - int(st.prev_ts_u32)) & 0xFFFFFFFF))
                        dt_abs_reset_us = float(self.dt_abs_reset_s) * 1e6

                        # update median only with "small" dts
                        if 0 < dt_us < dt_abs_reset_us:
                            st.dt_hist_us.append(dt_us)
                            if len(st.dt_hist_us) >= 5:
                                st.dt_med_us = float(np.median(np.fromiter(st.dt_hist_us, dtype=np.float64)))

                        reset_thr = max(dt_abs_reset_us, float(self.dt_reset_factor) * float(st.dt_med_us))
                        # Treat sample-count changes as a segment break (configuration changed)
                        is_reset = (dt_us > reset_thr) or (n_curr != int(st.prev_n))

                        dt_prev_us = float(st.dt_med_us) if is_reset else dt_us
                        n_prev = int(st.prev_n)
                        dur_s = dt_prev_us * 1e-6
                        dt_s = (dur_s / n_prev) if (dur_s > 0 and n_prev > 0) else 0.01

                        base_us = float(((int(st.prev_ts_u32) - int(st.seg_counter0)) & 0xFFFFFFFF))
                        t_last_s = float(st.seg_t0) + base_us * 1e-6

                        # Timestamp is the *last* sample; place samples ending at t_last_s.
                        t_block = t_last_s + (np.arange(n_prev, dtype=np.float64) - (n_prev - 1)) * dt_s

                        emit_this = (active_node is not None and node_id == active_node)
                        if emit_this:
                            self.samples.emit(t_block, st.prev_y1, st.prev_y2)
                            st.last_emitted_time = float(t_block[-1])
                            last_selected_data_t = now

                            if is_reset:
                                self.segment_break.emit()

                        if is_reset:
                            # start new segment for THIS node
                            if emit_this:
                                st.seg_t0 = st.last_emitted_time + float(self.gap_s)
                            else:
                                # keep segments coherent even if not emitting
                                st.seg_t0 = t_last_s + float(self.gap_s)
                            st.seg_counter0 = c

                        # shift prev -> current for this node
                        st.prev_ts_u32 = c
                        st.prev_y1 = (y1 if y1 is not None else y_primary)
                        st.prev_y2 = y2
                        st.prev_n = n_curr

                    # full re-init if no telemetry data for some time
                    if now - last_any_data_t > 10.0:
                        self.status.emit("No telemetry data for 10s → reinit")
                        # Do NOT send STOP here; only stop when disconnecting/closing COM.
                        # Just clear buffers and resend configs (closer to official app behaviour).
                        try:
                            ser.reset_input_buffer()
                        except Exception:
                            pass

                        decoder.buf.clear()
                        states.clear()
                        dev_status.reset()
                        node_counts.clear()
                        if 1 <= self.node_select <= 8:
                            active_node = int(self.node_select)
                        else:
                            active_node = None
                        init_receiver(
                            ser,
                            prefer_new_firmware=self.prefer_newfw,
                            rf_channel=self.rf_channel,
                            rf_speed_code=self.rf_speed_code,
                            max_sensors_code=self.max_sensors_code,
                            ad_odr_setting=self.ad_odr_setting,
                            gain_ch1_value=int(round(self.gain)),
                            gain_ch2_value=int(round(self.gain)),
                            selected_channel=1,
                            imu_odr_setting_default=self.imu_odr_setting,
                            imu_acc_range_default=self.imu_acc_range,
                            imu_gyr_range_default=self.imu_gyr_range,
                            imu_mag_range_default=self.imu_mag_range,
                        )
                        last_any_data_t = time.monotonic()
                        last_selected_data_t = last_any_data_t
                    # warn if we see data but none for the selected node
                    if active_node is not None:
                        if now - last_selected_data_t > 5.0 and now - last_any_data_t < 1.0:
                            if now - self._last_warn_t > 5.0:
                                seen = ", ".join(str(k) for k in sorted(node_counts.keys())) or "(none)"
                                self.status.emit(
                                    f"No data for Node {active_node} for 5s. Seen nodes: {seen}. "
                                    f"Try Node=Auto or another node."
                                )
                                self._last_warn_t = now

                # best-effort flush last pending for selected node
                if active_node is not None and active_node in states:
                    st = states[active_node]
                    if st.prev_ts_u32 is not None and st.prev_y1 is not None and st.seg_counter0 is not None and st.prev_n > 0:
                        dt_prev_us = float(st.dt_med_us)
                        n_prev = int(st.prev_n)
                        dur_s = dt_prev_us * 1e-6
                        dt_s = (dur_s / n_prev) if (dur_s > 0 and n_prev > 0) else 0.01
                        base_us = float(((int(st.prev_ts_u32) - int(st.seg_counter0)) & 0xFFFFFFFF))
                        t_last_s = float(st.seg_t0) + base_us * 1e-6
                        t_block = t_last_s + (np.arange(n_prev, dtype=np.float64) - (n_prev - 1)) * dt_s
                        self.samples.emit(t_block, st.prev_y1, st.prev_y2)

                # ask receiver to stop (best-effort)
                try:
                    stop_receiver(ser, max_sensors_code=self.max_sensors_code)
                except Exception:
                    pass

        except Exception as e:
            self.status.emit(f"ERROR: {e!r}")


