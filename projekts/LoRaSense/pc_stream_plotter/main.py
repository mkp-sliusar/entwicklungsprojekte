#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import socket
import struct
import threading
import time
import tkinter as tk
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from tkinter import ttk, messagebox, filedialog

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

MAGIC = b"MKPS"
PACKET_SIZE = 50
HEADER_STRUCT = struct.Struct("<4sHH")
PACKET_STRUCT = struct.Struct("<4sHHIIIHiffiffhH")
EXPECTED_VERSION = 1


@dataclass
class Sample:
    seq: int
    t_pc: float
    t_ms: int
    t_us: int
    flags: int
    dms_raw: int
    dms_mV: float
    dms_mV_per_V: float
    ain2_raw: int
    ain2_mV: float
    ain2_value: float
    temp_c: float

    def field_value(self, name: str) -> float:
        mapping = {
            "dms_raw": float(self.dms_raw),
            "dms_mV": self.dms_mV,
            "dms_mV_per_V": self.dms_mV_per_V,
            "ain2_raw": float(self.ain2_raw),
            "ain2_mV": self.ain2_mV,
            "ain2_value": self.ain2_value,
            "temp_C": self.temp_c,
        }
        return mapping[name]

    def as_csv_row(self) -> list:
        return [
            self.seq,
            f"{self.t_pc:.6f}",
            self.t_ms,
            self.t_us,
            self.flags,
            self.dms_raw,
            self._fmt(self.dms_mV),
            self._fmt(self.dms_mV_per_V),
            self.ain2_raw,
            self._fmt(self.ain2_mV),
            self._fmt(self.ain2_value),
            self._fmt(self.temp_c),
        ]

    @staticmethod
    def _fmt(v: float) -> str:
        return "" if isinstance(v, float) and math.isnan(v) else f"{v:.9g}"


class CsvLogger:
    def __init__(self) -> None:
        self.file = None
        self.writer = None
        self.path = None
        self.rows_written = 0

    @property
    def active(self) -> bool:
        return self.file is not None

    def start(self, filepath: str) -> None:
        self.stop()
        self.path = filepath
        self.file = open(filepath, "w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        self.writer.writerow([
            "seq", "pc_time_s", "t_ms", "t_us", "flags",
            "dms_raw", "dms_mV", "dms_mV_per_V",
            "ain2_raw", "ain2_mV", "ain2_value", "temp_C"
        ])
        self.file.flush()
        self.rows_written = 0

    def write_sample(self, sample: Sample) -> None:
        if not self.writer:
            return
        self.writer.writerow(sample.as_csv_row())
        self.rows_written += 1
        if self.rows_written % 25 == 0 and self.file:
            self.file.flush()

    def stop(self) -> None:
        if self.file:
            try:
                self.file.flush()
                self.file.close()
            except OSError:
                pass
        self.file = None
        self.writer = None
        self.path = None


class StreamReceiver:
    def __init__(self) -> None:
        self.sock = None
        self.thread = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.samples = deque(maxlen=10000)
        self.status = "disconnected"
        self.last_error = ""
        self.packet_count = 0
        self.byte_count = 0
        self.sample_callback = None

    def connect(self, host: str, port: int) -> None:
        self.disconnect()
        self.stop_event.clear()
        self.status = "connecting"
        self.thread = threading.Thread(target=self._run, args=(host, port), daemon=True)
        self.thread.start()

    def disconnect(self) -> None:
        self.stop_event.set()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None
        self.status = "disconnected"

    def _run(self, host: str, port: int) -> None:
        buf = bytearray()
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.settimeout(1.0)
            self.sock = sock
            self.status = "connected"

            while not self.stop_event.is_set():
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    continue

                if not chunk:
                    self.status = "server_closed"
                    break

                self.byte_count += len(chunk)
                buf.extend(chunk)

                while True:
                    if len(buf) < PACKET_SIZE:
                        break

                    if buf[:4] != MAGIC:
                        idx = buf.find(MAGIC, 1)
                        if idx == -1:
                            del buf[:-3]
                            break
                        del buf[:idx]
                        if len(buf) < PACKET_SIZE:
                            break

                    magic, version, pkt_bytes = HEADER_STRUCT.unpack_from(buf, 0)
                    if magic != MAGIC:
                        del buf[0]
                        continue
                    if version != EXPECTED_VERSION or pkt_bytes != PACKET_SIZE:
                        del buf[0]
                        continue
                    if len(buf) < pkt_bytes:
                        break

                    raw = bytes(buf[:pkt_bytes])
                    del buf[:pkt_bytes]
                    sample = self._parse_packet(raw)
                    with self.lock:
                        self.samples.append(sample)
                        self.packet_count += 1
                    if self.sample_callback:
                        try:
                            self.sample_callback(sample)
                        except Exception:
                            pass

        except Exception as exc:
            self.last_error = str(exc)
            self.status = "error"
        finally:
            if self.sock:
                try:
                    self.sock.close()
                except OSError:
                    pass
            self.sock = None

    def _parse_packet(self, raw: bytes) -> Sample:
        (
            magic,
            version,
            pkt_bytes,
            seq,
            t_ms,
            t_us,
            flags,
            dms_raw,
            dms_mV,
            dms_mV_per_V,
            ain2_raw,
            ain2_mV,
            ain2_value,
            temp_c_x100,
            reserved,
        ) = PACKET_STRUCT.unpack(raw)
        temp_c = float(temp_c_x100) / 100.0 if temp_c_x100 != -32768 else math.nan
        return Sample(
            seq=seq,
            t_pc=time.time(),
            t_ms=t_ms,
            t_us=t_us,
            flags=flags,
            dms_raw=dms_raw,
            dms_mV=dms_mV,
            dms_mV_per_V=dms_mV_per_V,
            ain2_raw=ain2_raw,
            ain2_mV=ain2_mV,
            ain2_value=ain2_value,
            temp_c=temp_c,
        )

    def snapshot(self):
        with self.lock:
            return list(self.samples)


class App(tk.Tk):
    FIELDS = [
        ("dms_raw", "DMS raw"),
        ("dms_mV", "DMS mV"),
        ("dms_mV_per_V", "DMS mV/V"),
        ("ain2_raw", "AIN2 raw"),
        ("ain2_mV", "AIN2 mV"),
        ("ain2_value", "AIN2 value"),
        ("temp_C", "Temperature °C"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.title("MKP LoRaSense PC Stream Receiver")
        self.geometry("1180x760")

        self.receiver = StreamReceiver()
        self.receiver.sample_callback = self.on_new_sample
        self.logger = CsvLogger()

        self.host_var = tk.StringVar(value="192.168.4.1")
        self.port_var = tk.StringVar(value="3333")
        self.window_sec_var = tk.StringVar(value="20")
        self.status_var = tk.StringVar(value="Disconnected")
        self.latest_var = tk.StringVar(value="-")
        self.rate_var = tk.StringVar(value="-")
        self.rate_actual_var = tk.StringVar(value="-")
        self.seq_var = tk.StringVar(value="-")
        self.flags_var = tk.StringVar(value="-")
        self.log_status_var = tk.StringVar(value="CSV logger stopped")
        self.log_path_var = tk.StringVar(value="-")

        self._rate_times = deque(maxlen=4000)
        self._build_ui()
        self.after(200, self.refresh_ui)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Host").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.host_var, width=18).grid(row=0, column=1, padx=6)
        ttk.Label(top, text="Port").grid(row=0, column=2, sticky="w")
        ttk.Entry(top, textvariable=self.port_var, width=8).grid(row=0, column=3, padx=6)
        ttk.Button(top, text="Connect", command=self.connect).grid(row=0, column=4, padx=6)
        ttk.Button(top, text="Disconnect", command=self.disconnect).grid(row=0, column=5, padx=6)
        ttk.Button(top, text="Start CSV log", command=self.start_logging).grid(row=0, column=6, padx=6)
        ttk.Button(top, text="Stop CSV log", command=self.stop_logging).grid(row=0, column=7, padx=6)

        ttk.Label(top, text="Plot").grid(row=1, column=0, sticky="w", pady=(10, 0))
        field_names = [label for _, label in self.FIELDS]
        self.field_combo = ttk.Combobox(top, values=field_names, state="readonly", width=20)
        self.field_combo.set("DMS mV/V")
        self.field_combo.grid(row=1, column=1, padx=6, pady=(10, 0))

        ttk.Label(top, text="Window, s").grid(row=1, column=2, sticky="w", pady=(10, 0))
        ttk.Entry(top, textvariable=self.window_sec_var, width=8).grid(row=1, column=3, padx=6, pady=(10, 0))

        info = ttk.Frame(self, padding=(10, 0, 10, 10))
        info.pack(fill="x")

        ttk.Label(info, text="Status:").grid(row=0, column=0, sticky="w")
        ttk.Label(info, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(4, 16))
        ttk.Label(info, text="Latest:").grid(row=0, column=2, sticky="w")
        ttk.Label(info, textvariable=self.latest_var).grid(row=0, column=3, sticky="w", padx=(4, 16))
        ttk.Label(info, text="Packets/s:").grid(row=0, column=4, sticky="w")
        ttk.Label(info, textvariable=self.rate_var).grid(row=0, column=5, sticky="w", padx=(4, 16))
        ttk.Label(info, text="Actual Hz:").grid(row=0, column=6, sticky="w")
        ttk.Label(info, textvariable=self.rate_actual_var).grid(row=0, column=7, sticky="w", padx=(4, 16))

        ttk.Label(info, text="Seq:").grid(row=1, column=0, sticky="w")
        ttk.Label(info, textvariable=self.seq_var).grid(row=1, column=1, sticky="w", padx=(4, 16))
        ttk.Label(info, text="Flags:").grid(row=1, column=2, sticky="w")
        ttk.Label(info, textvariable=self.flags_var).grid(row=1, column=3, sticky="w", padx=(4, 16))
        ttk.Label(info, text="Logger:").grid(row=1, column=4, sticky="w")
        ttk.Label(info, textvariable=self.log_status_var).grid(row=1, column=5, sticky="w", padx=(4, 16))
        ttk.Label(info, text="CSV:").grid(row=1, column=6, sticky="w")
        ttk.Label(info, textvariable=self.log_path_var).grid(row=1, column=7, sticky="w", padx=(4, 16))

        fig = Figure(figsize=(10, 5), dpi=100)
        self.ax = fig.add_subplot(111)
        self.ax.set_title("Live stream")
        self.ax.set_xlabel("Time, s")
        self.ax.set_ylabel("Value")
        self.ax.grid(True)

        self.canvas = FigureCanvasTkAgg(fig, master=self)
        self.canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

        self._last_packets = 0
        self._last_rate_time = time.time()

    def on_new_sample(self, sample: Sample) -> None:
        self._rate_times.append(sample.t_pc)
        if self.logger.active:
            self.logger.write_sample(sample)

    def selected_field_key(self) -> str:
        label = self.field_combo.get()
        for key, txt in self.FIELDS:
            if txt == label:
                return key
        return "dms_mV_per_V"

    def connect(self) -> None:
        host = self.host_var.get().strip()
        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            messagebox.showerror("Error", "Invalid port")
            return
        self.receiver.connect(host, port)

    def disconnect(self) -> None:
        self.receiver.disconnect()

    def start_logging(self) -> None:
        default_name = f"lorasense_stream_{time.strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = filedialog.asksaveasfilename(
            title="Save CSV log",
            defaultextension=".csv",
            initialfile=default_name,
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not filepath:
            return
        try:
            self.logger.start(filepath)
            self.log_status_var.set("CSV logger running")
            self.log_path_var.set(filepath)
        except Exception as exc:
            messagebox.showerror("Error", f"Cannot start CSV logger:\n{exc}")

    def stop_logging(self) -> None:
        self.logger.stop()
        self.log_status_var.set("CSV logger stopped")
        self.log_path_var.set("-")

    def compute_actual_hz(self) -> float:
        if len(self._rate_times) < 2:
            return 0.0
        now = time.time()
        while self._rate_times and self._rate_times[0] < now - 5.0:
            self._rate_times.popleft()
        if len(self._rate_times) < 2:
            return 0.0
        span = self._rate_times[-1] - self._rate_times[0]
        if span <= 0:
            return 0.0
        return (len(self._rate_times) - 1) / span

    def refresh_ui(self) -> None:
        status = self.receiver.status
        if status == "error" and self.receiver.last_error:
            self.status_var.set(f"Error: {self.receiver.last_error}")
        else:
            self.status_var.set(status.replace("_", " ").title())

        samples = self.receiver.snapshot()
        now = time.time()

        dt = max(now - self._last_rate_time, 1e-6)
        pps = (self.receiver.packet_count - self._last_packets) / dt
        self._last_packets = self.receiver.packet_count
        self._last_rate_time = now
        self.rate_var.set(f"{pps:.1f}")
        self.rate_actual_var.set(f"{self.compute_actual_hz():.1f}")

        if self.logger.active:
            self.log_status_var.set(f"CSV logger running ({self.logger.rows_written} rows)")
            self.log_path_var.set(str(self.logger.path))
        else:
            self.log_status_var.set("CSV logger stopped")
            if self.log_path_var.get() == "":
                self.log_path_var.set("-")

        if samples:
            last = samples[-1]
            key = self.selected_field_key()
            latest = last.field_value(key)
            self.latest_var.set(f"{latest:.6g}")
            self.seq_var.set(str(last.seq))
            self.flags_var.set(f"0x{last.flags:04X}")
        else:
            self.latest_var.set("-")
            self.seq_var.set("-")
            self.flags_var.set("-")

        self.redraw_plot(samples)
        self.after(200, self.refresh_ui)

    def redraw_plot(self, samples) -> None:
        self.ax.clear()
        self.ax.grid(True)

        if not samples:
            self.ax.set_title("Live stream")
            self.ax.set_xlabel("Time, s")
            self.ax.set_ylabel("Value")
            self.canvas.draw_idle()
            return

        try:
            window_sec = max(1.0, float(self.window_sec_var.get()))
        except ValueError:
            window_sec = 20.0

        t0 = samples[-1].t_pc
        field = self.selected_field_key()
        xs = []
        ys = []
        for s in samples:
            x = s.t_pc - t0
            if x < -window_sec:
                continue
            y = s.field_value(field)
            if isinstance(y, float) and math.isnan(y):
                continue
            xs.append(x)
            ys.append(y)

        label = dict(self.FIELDS)[field]
        self.ax.set_title(label)
        self.ax.set_xlabel("Time relative to latest sample, s")
        self.ax.set_ylabel(label)

        if xs and ys:
            self.ax.plot(xs, ys, linewidth=1.5)
            self.ax.set_xlim(min(xs), 0.0)
        self.canvas.draw_idle()

    def on_close(self) -> None:
        self.logger.stop()
        self.receiver.disconnect()
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
