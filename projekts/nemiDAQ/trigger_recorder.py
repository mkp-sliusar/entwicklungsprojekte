# trigger_recorder.py
from __future__ import annotations

import csv
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque, Optional, Tuple

import numpy as np


def _safe_slug(s: str, max_len: int = 64) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9_\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:max_len] if s else "trigger"


@dataclass
class TriggerConfig:
    """Auto trigger recording configuration.

    Trigger condition (robust to DC drift):
      |y_i(t) - median(pre_buffer_i)| >= threshold_mvv
    where i is channel 1 and/or channel 2 (whichever is enabled/available).

    Notes:
      - pre_s defines how much history is written before the trigger.
      - post_s defines how long we keep writing after the trigger moment.
      - cooldown_s prevents immediate re-triggering.
    """

    enabled: bool = False

    threshold_mvv: float = 0.02  # mV/V
    pre_s: float = 5.0
    post_s: float = 5.0
    cooldown_s: float = 5.0

    out_dir: str = "."
    filename_prefix: str = "trigger"

    min_pre_samples: int = 200  # arming requires enough history


class AutoTriggerRecorder:
    """Pre-buffer + triggered CSV writer for CH1/CH2.

    CSV format matches the official logger:

      Timestamp (ASCII); Timestamp (s); Timecounter (s); Ch 1; Ch 2; Ch 3; Ch 4

    The trigger CSV uses Timecounter (s) starting from 0 at the beginning
    of the pre-window (so the file spans [0 .. pre_s+post_s]).
    """

    def __init__(self, config: TriggerConfig):
        self.config = config

        # (t_stream_s, ch1_mvv, ch2_mvv) with NaN for missing channel
        self._buf: Deque[Tuple[float, float, float]] = deque()
        self._stream_start_dt: Optional[datetime] = None

        self._armed: bool = False
        self._recording: bool = False
        self._cooldown_until_t: float = -1e30

        # active event state
        self._event_path: Optional[str] = None
        self._event_fh = None
        self._writer: Optional[csv.writer] = None
        self._event_t0: float = 0.0
        self._event_end_t: float = 0.0
        self._last_written_t: float = -1e30

        self.last_event_path: Optional[str] = None
        self.last_event_time: Optional[datetime] = None

    # ---------------- public API ----------------
    def reset(self, stream_start_dt: datetime | None):
        self._buf.clear()
        self._stream_start_dt = stream_start_dt

        self._armed = False
        self._recording = False
        self._cooldown_until_t = -1e30

        self._event_path = None
        self._event_t0 = 0.0
        self._event_end_t = 0.0
        self._last_written_t = -1e30

        self.last_event_path = None
        self.last_event_time = None

        self._close_file()

    def set_config(self, config: TriggerConfig):
        self.config = config
        if not self.config.enabled and self._recording:
            self._close_file()
            self._recording = False

    @property
    def armed(self) -> bool:
        return self._armed

    @property
    def recording(self) -> bool:
        return self._recording

    @property
    def event_path(self) -> Optional[str]:
        return self._event_path

    def feed_block(self, t_s: np.ndarray, y1: np.ndarray | None, y2: np.ndarray | None = None) -> None:
        """Feed a block of samples for CH1 and/or CH2.

        - If a channel is not used, pass None or an empty array.
        - t_s must match the used channel arrays shape.
        """

        # Important: keep filling the pre-buffer even when disabled.
        # Otherwise enabling the checkbox and expecting an immediate trigger
        # can fail because we have zero history and are not armed yet.
        enabled = bool(self.config.enabled)

        t_s = np.asarray(t_s, dtype=np.float64)

        y1a = None
        if y1 is not None:
            y1a = np.asarray(y1, dtype=np.float64)
            if y1a.size == 0:
                y1a = None

        y2a = None
        if y2 is not None:
            y2a = np.asarray(y2, dtype=np.float64)
            if y2a.size == 0:
                y2a = None

        # Determine reference shape for validation
        ref = y1a if y1a is not None else y2a
        if ref is None:
            return
        if t_s.shape != ref.shape:
            raise ValueError("t_s and channel arrays must have the same shape")

        if y1a is None:
            y1a = np.full_like(ref, np.nan, dtype=np.float64)
        if y2a is None:
            y2a = np.full_like(ref, np.nan, dtype=np.float64)

        for ti, c1, c2 in zip(t_s, y1a, y2a):
            if not np.isfinite(ti):
                continue

            ti = float(ti)
            c1 = float(c1) if np.isfinite(c1) else float("nan")
            c2 = float(c2) if np.isfinite(c2) else float("nan")

            # append prebuffer and arm checks
            self._append_prebuffer(ti, c1, c2)

            # If trigger is currently disabled, we only maintain the pre-buffer.
            if not enabled:
                continue

            if not self._armed and len(self._buf) >= int(self.config.min_pre_samples):
                self._armed = True
                print(f"[TRIG] armed (prebuffer={len(self._buf)})", flush=True)

            if self._recording:
                self._write_if_new(ti, c1, c2)
                if ti >= self._event_end_t:
                    self._finish_event(t_now=ti)
                continue

            if not self._armed:
                continue
            if ti < self._cooldown_until_t:
                continue

            b1, b2 = self._baseline_medians()
            if b1 is None and b2 is None:
                continue

            thr = float(self.config.threshold_mvv)

            trig = False
            if b1 is not None and np.isfinite(c1) and abs(c1 - b1) >= thr:
                trig = True
            if b2 is not None and np.isfinite(c2) and abs(c2 - b2) >= thr:
                trig = True

            if trig:
                self._start_event(trigger_t=ti)

    # ---------------- internals ----------------
    def _append_prebuffer(self, t: float, y1: float, y2: float) -> None:
        self._buf.append((t, y1, y2))
        pre_s = max(0.1, float(self.config.pre_s))
        t_min = t - pre_s
        while self._buf and self._buf[0][0] < t_min:
            self._buf.popleft()

    def _baseline_medians(self) -> Tuple[Optional[float], Optional[float]]:
        if len(self._buf) < max(10, int(self.config.min_pre_samples * 0.5)):
            return None, None

        y1s = np.fromiter((v1 for _, v1, _ in self._buf if np.isfinite(v1)), dtype=np.float64)
        y2s = np.fromiter((v2 for _, _, v2 in self._buf if np.isfinite(v2)), dtype=np.float64)

        b1 = float(np.median(y1s)) if y1s.size > 0 else None
        b2 = float(np.median(y2s)) if y2s.size > 0 else None
        return b1, b2

    def _start_event(self, trigger_t: float) -> None:
        if not self._buf:
            return

        out_dir = self.config.out_dir or "."
        os.makedirs(out_dir, exist_ok=True)

        prefix = _safe_slug(self.config.filename_prefix)
        dt = self._stream_time_to_dt(trigger_t)
        ts = dt.strftime("%Y%m%d_%H%M%S") if dt else datetime.now().strftime("%Y%m%d_%H%M%S")

        fname = (
            f"{ts}_{prefix}"
            f"_pre{self.config.pre_s:.0f}s"
            f"_post{self.config.post_s:.0f}s"
            f"_thr{self.config.threshold_mvv:.4g}"
            f".csv"
        )
        path = os.path.join(out_dir, fname)

        self._open_file(path)
        print(f"[TRIG] start_event: {path}", flush=True)

        self._event_path = path
        self.last_event_path = path
        self.last_event_time = dt

        self._event_t0 = float(self._buf[0][0])
        self._event_end_t = float(trigger_t) + max(0.1, float(self.config.post_s))
        self._recording = True

        # Write prebuffer immediately
        for (ti, c1, c2) in list(self._buf):
            self._write_if_new(float(ti), float(c1), float(c2))

    def _finish_event(self, t_now: float) -> None:
        self._close_file()
        print(f"[TRIG] finish_event: {self._event_path or ''}", flush=True)
        self._recording = False
        self._cooldown_until_t = float(t_now) + max(0.0, float(self.config.cooldown_s))

    def _open_file(self, path: str) -> None:
        self._close_file()
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        self._event_fh = open(path, "w", newline="", encoding="utf-8")
        print(f"[TRIG] writing: {path}", flush=True)
        self._writer = csv.writer(self._event_fh, delimiter=";")
        self._writer.writerow([
            "Timestamp (ASCII)",
            "Timestamp (s)",
            "Timecounter (s)",
            "Ch 1",
            "Ch 2",
            "Ch 3",
            "Ch 4",
        ])
        self._last_written_t = -1e30

    def _close_file(self) -> None:
        try:
            if self._event_fh is not None:
                self._event_fh.flush()
                self._event_fh.close()
        except Exception:
            pass
        self._event_fh = None
        self._writer = None

    def _stream_time_to_dt(self, t_stream_s: float) -> Optional[datetime]:
        if self._stream_start_dt is None:
            return None
        return self._stream_start_dt + timedelta(seconds=float(t_stream_s))

    def _write_if_new(self, t_stream_s: float, y1: float, y2: float) -> None:
        if self._writer is None:
            return
        if t_stream_s <= self._last_written_t + 1e-12:
            return

        dt = self._stream_time_to_dt(t_stream_s)
        t_rel = float(t_stream_s - self._event_t0)

        if dt is not None:
            ts_ascii = dt.strftime("%d.%m.%Y %H:%M:%S.%f")
            ts_s = f"{dt.timestamp():.6f}"
        else:
            ts_ascii = ""
            ts_s = ""

        v1 = float(y1) if np.isfinite(y1) else 0.0
        v2 = float(y2) if np.isfinite(y2) else 0.0

        self._writer.writerow([
            ts_ascii,
            ts_s,
            f"{t_rel:.6f}",
            f"{v1:.9f}",
            f"{v2:.9f}",
            "0",
            "0",
        ])
        self._last_written_t = t_stream_s
