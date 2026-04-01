#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import socket
import struct
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import pyqtgraph as pg
import requests
from PySide6 import QtCore, QtGui, QtWidgets

MAGIC = b"MKPS"
PACKET_SIZE = 50
PACKET_STRUCT = struct.Struct("<4sHHIIIHiffiffhH")
HTTP_TIMEOUT_S = 3
STREAM_STATUS_COLORS = {
    "connected": "#1b5e20",
    "connecting": "#8d6e00",
    "disconnected": "#424242",
}
FREQ_OPTIONS = ["5", "11.25", "20", "22.5", "40", "44", "45", "82.5", "90", "150", "175", "180", "250", "330", "350", "600", "660", "1000", "1200", "2000"]
WEG_LENGTH_OPTIONS_MM = [5, 10, 25, 50, 75, 100, 150, 200, 300, 500]
UNIT_OPTIONS = ["V", "mm", "um", "C", "%", "bar", "mA", "kN"]
PLOT_FIELDS = ["dms_display", "ain2_display", "temp_c"]


def _short(text: Any, max_len: int = 14) -> str:
    value = "-" if text is None else str(text).strip()
    if not value:
        return "-"
    return value if len(value) <= max_len else value[: max_len - 1] + "…"


@dataclass
class Sample:
    seq: int
    t_pc: float
    dms_mV_per_V: float
    ain2_value: float
    temp_c: float


@dataclass
class DmsSettings:
    enabled: bool = True
    frequency_hz: str = "1000"
    k_factor: float = 0.0


@dataclass
class Ain2Settings:
    enabled: bool = True
    frequency_hz: str = "300"
    mode: str = "weg"
    weg_length_mm: float = 100.0
    volt_display: str = "volts"
    scaling_unit: str = "mm"
    scale_min: float = 0.0
    scale_max: float = 10.0


class StreamReceiver:
    def __init__(self) -> None:
        self.samples: deque[Sample] = deque(maxlen=30000)
        self.running = False
        self.sock: socket.socket | None = None
        self.thread: threading.Thread | None = None
        self.status = "disconnected"

    def connect(self, host: str, port: int) -> None:
        self.stop()
        self.running = True
        self.status = "connecting"
        self.thread = threading.Thread(target=self._run, args=(host, port), daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.running = False
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
            self.sock = socket.create_connection((host, port), timeout=5)
            self.sock.settimeout(1.0)
            self.status = "connected"

            while self.running:
                try:
                    data = self.sock.recv(4096)
                except socket.timeout:
                    continue

                if not data:
                    self.status = "server_closed"
                    break

                buf.extend(data)

                while len(buf) >= PACKET_SIZE:
                    if buf[:4] != MAGIC:
                        idx = buf.find(MAGIC, 1)
                        if idx == -1:
                            del buf[:-3]
                            break
                        del buf[:idx]
                        if len(buf) < PACKET_SIZE:
                            break

                    raw = bytes(buf[:PACKET_SIZE])
                    del buf[:PACKET_SIZE]
                    unpacked = PACKET_STRUCT.unpack(raw)
                    temp_raw = unpacked[13]
                    temp_c = temp_raw / 100.0 if temp_raw != -32768 else float("nan")

                    self.samples.append(
                        Sample(
                            seq=unpacked[3],
                            t_pc=time.time(),
                            dms_mV_per_V=unpacked[9],
                            ain2_value=unpacked[12],
                            temp_c=temp_c,
                        )
                    )
        except Exception as exc:
            self.status = f"error: {exc}"
        finally:
            if self.sock:
                try:
                    self.sock.close()
                except OSError:
                    pass
            self.sock = None


class CsvLogger:
    def __init__(self) -> None:
        self.file: Any | None = None
        self.writer: csv.writer | None = None
        self.path = ""
        self.rows = 0

    def start(self, path: str) -> None:
        self.stop()
        self.file = open(path, "w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        self.writer.writerow(["pc_time_s", "seq", "dms_mV_per_V", "ain2_value", "temp_C"])
        self.path = path
        self.rows = 0

    def write(self, sample: Sample) -> None:
        if self.writer:
            self.writer.writerow([f"{sample.t_pc:.6f}", sample.seq, sample.dms_mV_per_V, sample.ain2_value, sample.temp_c])
            self.rows += 1
            if self.rows % 25 == 0 and self.file:
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
        self.path = ""
        self.rows = 0


class DmsSettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, settings: DmsSettings) -> None:
        super().__init__(parent)
        self.setWindowTitle("DMS settings")
        self.setModal(True)
        self.resize(420, 220)

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()
        layout.addLayout(form)

        self.enable_box = QtWidgets.QCheckBox("Enable DMS")
        self.enable_box.setChecked(settings.enabled)
        form.addRow(self.enable_box)

        self.freq_combo = QtWidgets.QComboBox()
        self.freq_combo.addItems(FREQ_OPTIONS)
        self.freq_combo.setCurrentText(settings.frequency_hz)
        form.addRow("Frequency, Hz", self.freq_combo)

        self.k_factor = QtWidgets.QDoubleSpinBox()
        self.k_factor.setDecimals(4)
        self.k_factor.setRange(0.0, 100.0)
        self.k_factor.setSingleStep(0.1)
        self.k_factor.setSpecialValueText("not set")
        self.k_factor.setValue(max(0.0, settings.k_factor))
        form.addRow("K-factor", self.k_factor)

        note = QtWidgets.QLabel(
            "K-factor = 0  -> display in mV/V\n"
            "K-factor > 0 -> display in um/m"
        )
        note.setStyleSheet("color:#666;")
        layout.addWidget(note)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def value(self) -> DmsSettings:
        return DmsSettings(
            enabled=self.enable_box.isChecked(),
            frequency_hz=self.freq_combo.currentText(),
            k_factor=float(self.k_factor.value()),
        )


class Ain2SettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, settings: Ain2Settings) -> None:
        super().__init__(parent)
        self.setWindowTitle("Channel 2 settings")
        self.setModal(True)
        self.resize(480, 360)

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()
        layout.addLayout(form)

        self.enable_box = QtWidgets.QCheckBox("Enable channel 2")
        self.enable_box.setChecked(settings.enabled)
        form.addRow(self.enable_box)

        self.freq_combo = QtWidgets.QComboBox()
        self.freq_combo.addItems(FREQ_OPTIONS)
        self.freq_combo.setCurrentText(settings.frequency_hz)
        form.addRow("Frequency, Hz", self.freq_combo)

        self.mode_combo = QtWidgets.QComboBox()
        self.mode_combo.addItems(["weg", "10v"])
        self.mode_combo.setCurrentText(settings.mode)
        form.addRow("Input type", self.mode_combo)

        self.weg_combo = QtWidgets.QComboBox()
        self.weg_combo.addItems([f"{int(v)} mm" for v in WEG_LENGTH_OPTIONS_MM])
        if settings.weg_length_mm in WEG_LENGTH_OPTIONS_MM:
            self.weg_combo.setCurrentText(f"{int(settings.weg_length_mm)} mm")
        form.addRow("Wegsensor length", self.weg_combo)

        self.display_combo = QtWidgets.QComboBox()
        self.display_combo.addItems(["volts", "scaled"])
        self.display_combo.setCurrentText(settings.volt_display)
        form.addRow("10 V display", self.display_combo)

        self.unit_combo = QtWidgets.QComboBox()
        self.unit_combo.addItems(UNIT_OPTIONS)
        self.unit_combo.setCurrentText(settings.scaling_unit)
        form.addRow("Scaled unit", self.unit_combo)

        self.scale_min = QtWidgets.QDoubleSpinBox()
        self.scale_min.setDecimals(3)
        self.scale_min.setRange(-1_000_000.0, 1_000_000.0)
        self.scale_min.setValue(settings.scale_min)
        form.addRow("Scale minimum", self.scale_min)

        self.scale_max = QtWidgets.QDoubleSpinBox()
        self.scale_max.setDecimals(3)
        self.scale_max.setRange(-1_000_000.0, 1_000_000.0)
        self.scale_max.setValue(settings.scale_max)
        form.addRow("Scale maximum", self.scale_max)

        self.mode_combo.currentTextChanged.connect(self._update_visibility)
        self.display_combo.currentTextChanged.connect(self._update_visibility)
        self._update_visibility()

        note = QtWidgets.QLabel(
            "Wegsensor -> value shown in selected length units (mm).\n"
            "10V + volts -> value shown in V.\n"
            "10V + scaled -> linear scaling 0..10 V -> selected engineering unit."
        )
        note.setStyleSheet("color:#666;")
        layout.addWidget(note)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _update_visibility(self) -> None:
        is_weg = self.mode_combo.currentText() == "weg"
        is_scaled = self.display_combo.currentText() == "scaled"
        self.weg_combo.setEnabled(is_weg)
        self.display_combo.setEnabled(not is_weg)
        self.unit_combo.setEnabled((not is_weg) and is_scaled)
        self.scale_min.setEnabled((not is_weg) and is_scaled)
        self.scale_max.setEnabled((not is_weg) and is_scaled)

    def _validate_and_accept(self) -> None:
        if self.mode_combo.currentText() == "10v" and self.display_combo.currentText() == "scaled":
            if math.isclose(self.scale_min.value(), self.scale_max.value(), rel_tol=0.0, abs_tol=1e-12):
                QtWidgets.QMessageBox.warning(self, "Invalid scaling", "Scale minimum and maximum must be different.")
                return
        self.accept()

    def value(self) -> Ain2Settings:
        weg_length = float(self.weg_combo.currentText().split()[0])
        return Ain2Settings(
            enabled=self.enable_box.isChecked(),
            frequency_hz=self.freq_combo.currentText(),
            mode=self.mode_combo.currentText(),
            weg_length_mm=weg_length,
            volt_display=self.display_combo.currentText(),
            scaling_unit=self.unit_combo.currentText(),
            scale_min=float(self.scale_min.value()),
            scale_max=float(self.scale_max.value()),
        )


class NetworkSettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, host: str, port: int, window_s: float) -> None:
        super().__init__(parent)
        self.setWindowTitle("Connection settings")
        self.setModal(True)
        self.resize(380, 210)

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()
        layout.addLayout(form)

        self.host_edit = QtWidgets.QLineEdit(host)
        self.port_edit = QtWidgets.QLineEdit(str(port))
        self.port_edit.setValidator(QtGui.QIntValidator(1, 65535, self))
        self.window_edit = QtWidgets.QDoubleSpinBox()
        self.window_edit.setDecimals(1)
        self.window_edit.setRange(1.0, 3600.0)
        self.window_edit.setSingleStep(1.0)
        self.window_edit.setValue(window_s)

        form.addRow("Host / IP", self.host_edit)
        form.addRow("Port", self.port_edit)
        form.addRow("Window, s", self.window_edit)

        note = QtWidgets.QLabel("Use the gear near Connect / Disconnect to edit stream target.")
        note.setWordWrap(True)
        note.setStyleSheet("color:#666;")
        layout.addWidget(note)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _validate_and_accept(self) -> None:
        host = self.host_edit.text().strip()
        port_txt = self.port_edit.text().strip()
        if not host:
            QtWidgets.QMessageBox.warning(self, "Invalid host", "Host / IP must not be empty.")
            return
        try:
            port = int(port_txt)
        except ValueError:
            QtWidgets.QMessageBox.warning(self, "Invalid port", "Port must be a valid integer.")
            return
        if not (1 <= port <= 65535):
            QtWidgets.QMessageBox.warning(self, "Invalid port", "Port must be between 1 and 65535.")
            return
        self.accept()

    def value(self) -> tuple[str, int, float]:
        return self.host_edit.text().strip(), int(self.port_edit.text().strip()), float(self.window_edit.value())


class App(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("LoRaSense PC Control")
        self.resize(1400, 900)

        self.settings_store = QtCore.QSettings("OpenAI", "LoRaSensePcControl")
        self.host = self.settings_store.value("network/host", "192.168.4.1", str)
        self.port = int(self.settings_store.value("network/port", 3333))
        self.window_s = float(self.settings_store.value("network/window_s", 20.0))

        self.receiver = StreamReceiver()
        self.logger = CsvLogger()
        self.last_logged_seq: int | None = None
        self.last_rate_seq: int | None = None
        self.rate_times: deque[float] = deque(maxlen=5000)
        self.dms_settings = self._load_dms_settings()
        self.ain2_settings = self._load_ain2_settings()
        self.remote_state: dict[str, Any] = {}

        self._build_ui()
        self._update_sensor_buttons()
        self.stream_mode.setText("Offline")
        self.stream_target.setText(f"{self.host}:{self.port}")

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.update_ui)
        self.timer.start(250)

    def _load_dms_settings(self) -> DmsSettings:
        return DmsSettings(
            enabled=self.settings_store.value("dms/enabled", True, bool),
            frequency_hz=self.settings_store.value("dms/frequency_hz", "1000", str),
            k_factor=float(self.settings_store.value("dms/k_factor", 0.0)),
        )

    def _load_ain2_settings(self) -> Ain2Settings:
        return Ain2Settings(
            enabled=self.settings_store.value("ain2/enabled", True, bool),
            frequency_hz=self.settings_store.value("ain2/frequency_hz", "300", str),
            mode=self.settings_store.value("ain2/mode", "weg", str),
            weg_length_mm=float(self.settings_store.value("ain2/weg_length_mm", 100.0)),
            volt_display=self.settings_store.value("ain2/volt_display", "volts", str),
            scaling_unit=self.settings_store.value("ain2/scaling_unit", "mm", str),
            scale_min=float(self.settings_store.value("ain2/scale_min", 0.0)),
            scale_max=float(self.settings_store.value("ain2/scale_max", 10.0)),
        )

    def _save_local_settings(self) -> None:
        self.settings_store.setValue("network/host", self.host)
        self.settings_store.setValue("network/port", str(self.port))
        self.settings_store.setValue("network/window_s", self.window_s)
        self.settings_store.setValue("dms/enabled", self.dms_settings.enabled)
        self.settings_store.setValue("dms/frequency_hz", self.dms_settings.frequency_hz)
        self.settings_store.setValue("dms/k_factor", self.dms_settings.k_factor)
        self.settings_store.setValue("ain2/enabled", self.ain2_settings.enabled)
        self.settings_store.setValue("ain2/frequency_hz", self.ain2_settings.frequency_hz)
        self.settings_store.setValue("ain2/mode", self.ain2_settings.mode)
        self.settings_store.setValue("ain2/weg_length_mm", self.ain2_settings.weg_length_mm)
        self.settings_store.setValue("ain2/volt_display", self.ain2_settings.volt_display)
        self.settings_store.setValue("ain2/scaling_unit", self.ain2_settings.scaling_unit)
        self.settings_store.setValue("ain2/scale_min", self.ain2_settings.scale_min)
        self.settings_store.setValue("ain2/scale_max", self.ain2_settings.scale_max)
        self.settings_store.sync()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        root.addLayout(self._build_top_bar())
        root.addLayout(self._build_status_bar())

        body = QtWidgets.QHBoxLayout()
        body.setSpacing(12)
        root.addLayout(body, 1)

        left = QtWidgets.QVBoxLayout()
        left.setSpacing(10)
        body.addLayout(left, 0)

        left.addWidget(self._build_stream_card())
        left.addWidget(self._build_sensor_card())
        left.addStretch(1)

        right = QtWidgets.QVBoxLayout()
        right.setSpacing(10)
        body.addLayout(right, 1)

        right.addLayout(self._build_plot_toolbar())

        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        self.plot1 = pg.PlotWidget()
        self.plot2 = pg.PlotWidget()
        for plot in (self.plot1, self.plot2):
            plot.setBackground("#111111")
            plot.showGrid(x=True, y=True, alpha=0.25)
        splitter.addWidget(self.plot1)
        splitter.addWidget(self.plot2)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 1)
        right.addWidget(splitter, 1)

    def _build_top_bar(self) -> QtWidgets.QGridLayout:
        top = QtWidgets.QGridLayout()
        top.setHorizontalSpacing(8)
        top.setColumnStretch(0, 0)
        top.setColumnStretch(1, 0)
        top.setColumnStretch(2, 0)
        top.setColumnStretch(3, 1)

        self.btn_connect = QtWidgets.QPushButton("Connect")
        self.btn_disconnect = QtWidgets.QPushButton("Disconnect")
        self.btn_network = QtWidgets.QPushButton("⚙")
        self.btn_network.setFixedWidth(42)
        self.btn_csv_start = QtWidgets.QPushButton("Start CSV")
        self.btn_csv_stop = QtWidgets.QPushButton("Stop CSV")

        button_height = 30
        button_width = 110
        for button in (self.btn_connect, self.btn_disconnect, self.btn_csv_start, self.btn_csv_stop):
            button.setFixedSize(button_width, button_height)

        self.btn_connect.clicked.connect(self.connect_stream)
        self.btn_disconnect.clicked.connect(self.disconnect_stream)
        self.btn_network.clicked.connect(self.open_network_settings)
        self.btn_csv_start.clicked.connect(self.start_csv)
        self.btn_csv_stop.clicked.connect(self.stop_csv)

        row = QtWidgets.QHBoxLayout()
        row.setSpacing(8)
        row.addWidget(self.btn_connect)
        row.addWidget(self.btn_disconnect)
        row.addWidget(self.btn_network)
        row.addWidget(self.btn_csv_start)
        row.addWidget(self.btn_csv_stop)
        row.addStretch(1)

        top.addLayout(row, 0, 0, 1, 4)
        return top

    def _build_status_bar(self) -> QtWidgets.QGridLayout:
        info = QtWidgets.QGridLayout()
        info.setHorizontalSpacing(16)

        self.status_label = QtWidgets.QLabel("disconnected")
        self.status_label.setStyleSheet("padding:4px 10px; border-radius:10px; background:#424242; color:white;")
        self.seq_label = QtWidgets.QLabel("-")
        self.hz_label = QtWidgets.QLabel("-")
        self.csv_label = QtWidgets.QLabel("-")

        info.addWidget(QtWidgets.QLabel("Status:"), 0, 0)
        info.addWidget(self.status_label, 0, 1)
        info.addWidget(QtWidgets.QLabel("Last seq:"), 0, 2)
        info.addWidget(self.seq_label, 0, 3)
        info.addWidget(QtWidgets.QLabel("Actual Hz:"), 0, 4)
        info.addWidget(self.hz_label, 0, 5)
        info.addWidget(QtWidgets.QLabel("CSV:"), 0, 6)
        info.addWidget(self.csv_label, 0, 7)
        info.setColumnStretch(7, 1)
        return info

    def _build_stream_card(self) -> QtWidgets.QGroupBox:
        box = QtWidgets.QGroupBox("Controller")
        layout = QtWidgets.QFormLayout(box)
        layout.setLabelAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        layout.setFormAlignment(QtCore.Qt.AlignTop)
        layout.setHorizontalSpacing(16)
        layout.setVerticalSpacing(8)

        self.stream_mode = QtWidgets.QLabel("-")
        self.stream_target = QtWidgets.QLabel("-")
        self.ctrl_firmware = QtWidgets.QLabel("-")

        layout.addRow("Connection", self.stream_mode)
        layout.addRow("Target", self.stream_target)
        layout.addRow("Firmware", self.ctrl_firmware)
        return box

    def _build_sensor_card(self) -> QtWidgets.QGroupBox:
        box = QtWidgets.QGroupBox("Sensors")
        box.setMinimumWidth(300)
        outer = QtWidgets.QVBoxLayout(box)
        outer.setSpacing(8)

        self.dms_value = QtWidgets.QLabel("-")
        self.ain2_value = QtWidgets.QLabel("-")
        self.temp_value = QtWidgets.QLabel("-")
        for value_label in (self.dms_value, self.ain2_value, self.temp_value):
            value_label.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            value_label.setMinimumWidth(170)
        self.dms_meta = QtWidgets.QLabel("-")
        self.ain2_meta = QtWidgets.QLabel("-")
        self.temp_meta = QtWidgets.QLabel("Temperature stream")

        outer.addWidget(self._sensor_row("DMS", self.dms_value, self.dms_meta, self.open_dms_settings, include_button=True))
        outer.addWidget(self._sensor_row("Channel 2", self.ain2_value, self.ain2_meta, self.open_ain2_settings, include_button=True))
        outer.addWidget(self._sensor_row("Temperature", self.temp_value, self.temp_meta, None, include_button=False))
        return box

    def _sensor_row(
        self,
        title: str,
        value_label: QtWidgets.QLabel,
        meta_label: QtWidgets.QLabel,
        callback: Any,
        *,
        include_button: bool,
    ) -> QtWidgets.QWidget:
        card = QtWidgets.QFrame()
        card.setFrameShape(QtWidgets.QFrame.StyledPanel)
        layout = QtWidgets.QVBoxLayout(card)
        layout.setContentsMargins(8, 8, 8, 8)

        top = QtWidgets.QHBoxLayout()
        title_label = QtWidgets.QLabel(f"<b>{title}</b>")
        top.addWidget(title_label)
        top.addStretch(1)
        if include_button:
            btn = QtWidgets.QPushButton("⚙")
            btn.setFixedWidth(40)
            btn.clicked.connect(callback)
            top.addWidget(btn)
        layout.addLayout(top)

        value_label.setStyleSheet("font-size:20px; font-weight:600; font-family:'Courier New', monospace;")
        layout.addWidget(value_label)
        meta_label.setWordWrap(True)
        meta_label.setStyleSheet("color:#666;")
        layout.addWidget(meta_label)
        return card

    def _build_plot_toolbar(self) -> QtWidgets.QGridLayout:
        select = QtWidgets.QGridLayout()
        self.combo1 = QtWidgets.QComboBox()
        self.combo2 = QtWidgets.QComboBox()
        self.combo1.addItems(PLOT_FIELDS)
        self.combo2.addItems(PLOT_FIELDS)
        self.combo1.setCurrentText("dms_display")
        self.combo2.setCurrentText("ain2_display")
        self.combo1.currentIndexChanged.connect(self.refresh_plots)
        self.combo2.currentIndexChanged.connect(self.refresh_plots)
        select.addWidget(QtWidgets.QLabel("Graph 1"), 0, 0)
        select.addWidget(self.combo1, 0, 1)
        select.addWidget(QtWidgets.QLabel("Graph 2"), 0, 2)
        select.addWidget(self.combo2, 0, 3)
        return select

    def _update_sensor_buttons(self) -> None:
        self.dms_meta.setText(
            f"{self.dms_settings.frequency_hz} Hz | "
            f"{'mV/V' if self.dms_settings.k_factor <= 0 else 'um/m'}"
        )
        ain2_tail = self._ain2_display_unit()
        self.ain2_meta.setText(f"{self.ain2_settings.frequency_hz} Hz | {self.ain2_settings.mode} | {ain2_tail}")

    def _ain2_display_unit(self) -> str:
        if self.ain2_settings.mode == "weg":
            return f"{self.ain2_settings.weg_length_mm:.0f} mm sensor"
        if self.ain2_settings.volt_display == "scaled":
            return self.ain2_settings.scaling_unit
        return "V"

    def open_dms_settings(self) -> None:
        dlg = DmsSettingsDialog(self, self.dms_settings)
        if dlg.exec() == QtWidgets.QDialog.Accepted:
            self.dms_settings = dlg.value()
            self._save_local_settings()
            self._update_sensor_buttons()
            self.refresh_plots()
            self.push_config_to_device(show_success=True)

    def open_ain2_settings(self) -> None:
        dlg = Ain2SettingsDialog(self, self.ain2_settings)
        if dlg.exec() == QtWidgets.QDialog.Accepted:
            self.ain2_settings = dlg.value()
            self._save_local_settings()
            self._update_sensor_buttons()
            self.refresh_plots()
            self.push_config_to_device(show_success=True)

    def open_network_settings(self) -> None:
        dlg = NetworkSettingsDialog(self, self.host, self.port, self.window_s)
        if dlg.exec() == QtWidgets.QDialog.Accepted:
            host, port, window_s = dlg.value()
            self.host = host
            self.port = port
            self.window_s = window_s
            self.stream_target.setText(f"{self.host}:{self.port}")
            self._save_local_settings()
            self.refresh_plots()

    def connect_stream(self) -> None:
        self._save_local_settings()
        self.receiver.connect(self.host, self.port)
        self.stream_mode.setText("TCP stream")
        self.stream_target.setText(f"{self.host}:{self.port}")

    def disconnect_stream(self) -> None:
        self.receiver.stop()
        self.stream_mode.setText("Offline")

    def start_csv(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Save CSV",
            f"lorasense_stream_{time.strftime('%Y%m%d_%H%M%S')}.csv",
            "CSV files (*.csv)",
        )
        if path:
            self.logger.start(path)
            self.csv_label.setText(path)
            self.last_logged_seq = None

    def stop_csv(self) -> None:
        self.logger.stop()
        self.csv_label.setText("-")
        self.last_logged_seq = None

    def read_config(self) -> None:
        cfg = self.fetch_config()
        if cfg is None:
            return
        self._apply_remote_config(cfg)
        self._save_local_settings()
        self._update_sensor_buttons()
        self.refresh_plots()

    def fetch_config(self) -> dict[str, Any] | None:
        try:
            response = requests.get(f"http://{self.host}/api/config", timeout=HTTP_TIMEOUT_S)
            response.raise_for_status()
            return dict(response.json())
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Error", f"Read config failed:\n{exc}")
            return None

    def _apply_remote_config(self, cfg: dict[str, Any]) -> None:
        self.dms_settings.enabled = bool(cfg.get("dms_enabled", self.dms_settings.enabled))
        self.dms_settings.frequency_hz = str(cfg.get("dms_hz", self.dms_settings.frequency_hz))
        self.dms_settings.k_factor = float(cfg.get("dms_k_factor", self.dms_settings.k_factor))

        self.ain2_settings.enabled = bool(cfg.get("ain2_enabled", self.ain2_settings.enabled))
        self.ain2_settings.frequency_hz = str(cfg.get("ain2_hz", self.ain2_settings.frequency_hz))
        remote_mode = str(cfg.get("ain2_mode", self.ain2_settings.mode)).lower()
        self.ain2_settings.mode = "10v" if remote_mode in {"10v", "volt"} else "weg"
        self.ain2_settings.weg_length_mm = float(cfg.get("ain2_length_mm", self.ain2_settings.weg_length_mm))
        self.ain2_settings.volt_display = str(cfg.get("ain2_volt_display", self.ain2_settings.volt_display))
        self.ain2_settings.scaling_unit = str(cfg.get("ain2_scaling_unit", self.ain2_settings.scaling_unit))
        self.ain2_settings.scale_min = float(cfg.get("ain2_scale_min", self.ain2_settings.scale_min))
        self.ain2_settings.scale_max = float(cfg.get("ain2_scale_max", self.ain2_settings.scale_max))

    def push_config_to_device(self, *, show_success: bool = False) -> None:
        payload: dict[str, Any] = {
            "dms_enabled": int(self.dms_settings.enabled),
            "dms_hz": int(float(self.dms_settings.frequency_hz)),
            "dms_k_factor": float(self.dms_settings.k_factor),
            "ain2_enabled": int(self.ain2_settings.enabled),
            "ain2_hz": int(float(self.ain2_settings.frequency_hz)),
            "ain2_mode": "volt" if self.ain2_settings.mode == "10v" else "pot",
            "ain2_length_mm": float(self.ain2_settings.weg_length_mm),
            "ain2_volt_display": self.ain2_settings.volt_display,
            "ain2_scaling_unit": self.ain2_settings.scaling_unit,
            "ain2_scale_min": float(self.ain2_settings.scale_min),
            "ain2_scale_max": float(self.ain2_settings.scale_max),
            "temp_enabled": 1,
        }
        try:
            response = requests.post(f"http://{self.host}/api/config", json=payload, timeout=HTTP_TIMEOUT_S)
            response.raise_for_status()
            if show_success:
                QtWidgets.QToolTip.showText(self.mapToGlobal(QtCore.QPoint(40, 40)), "Settings saved")
        except Exception as exc:
            QtWidgets.QMessageBox.warning(
                self,
                "Config warning",
                "Local UI settings were saved, but device config update failed.\n\n"
                f"{exc}",
            )

    def fetch_state(self) -> dict[str, Any]:
        try:
            response = requests.get(f"http://{self.host}/api/state", timeout=0.8)
            response.raise_for_status()
            data = response.json()
            self.remote_state = dict(data) if isinstance(data, dict) else {}
        except Exception:
            self.remote_state = {}
        return self.remote_state

    def actual_hz(self) -> float:
        if len(self.rate_times) < 2:
            return 0.0
        latest = self.rate_times[-1]
        while self.rate_times and self.rate_times[0] < latest - 5.0:
            self.rate_times.popleft()
        if len(self.rate_times) < 2:
            return 0.0
        dt = self.rate_times[-1] - self.rate_times[0]
        if dt <= 0:
            return 0.0
        return (len(self.rate_times) - 1) / dt

    def update_ui(self) -> None:
        state = self.fetch_state()
        status = self.receiver.status
        self.status_label.setText(status)
        color = STREAM_STATUS_COLORS.get(status, "#7b1f1f" if status.startswith("error") else "#455a64")
        self.status_label.setStyleSheet(f"padding:4px 10px; border-radius:10px; background:{color}; color:white;")

        board_value = state.get("board") or state.get("api") or state.get("device") or state.get("model")
        firmware_value = (
            state.get("firmware")
            or state.get("fw")
            or state.get("firmware_version")
            or state.get("version")
        )
        self.ctrl_firmware.setText(_short(firmware_value))

        samples = list(self.receiver.samples)
        if samples:
            last = samples[-1]
            self.seq_label.setText(str(last.seq))
            new_samples = self._new_samples_since(samples, self.last_rate_seq)
            for sample in new_samples:
                self.rate_times.append(sample.t_pc)
                self.last_rate_seq = sample.seq
            self._update_sensor_values(last)

            if self.logger.file:
                for sample in self._new_samples_since(samples, self.last_logged_seq):
                    self.logger.write(sample)
                    self.last_logged_seq = sample.seq
        else:
            self.seq_label.setText("-")
            self.dms_value.setText("-")
            self.ain2_value.setText("-")
            self.temp_value.setText("-")

        self.hz_label.setText(f"{self.actual_hz():.1f}")
        self.refresh_plots()

    def _new_samples_since(self, samples: list[Sample], last_seq: int | None) -> list[Sample]:
        if not samples:
            return []
        if last_seq is None:
            return samples[-1:]
        start_idx = None
        for index in range(len(samples) - 1, -1, -1):
            if samples[index].seq == last_seq:
                start_idx = index + 1
                break
        if start_idx is None:
            return samples
        return samples[start_idx:]

    def _update_sensor_values(self, sample: Sample) -> None:
        dms_value, dms_unit = self.compute_dms_display(sample)
        ain2_value, ain2_unit = self.compute_ain2_display(sample)
        self.dms_value.setText(self._fmt_value(dms_value, dms_unit))
        self.ain2_value.setText(self._fmt_value(ain2_value, ain2_unit))
        self.temp_value.setText(self._fmt_value(sample.temp_c, "C"))

    def compute_dms_display(self, sample: Sample) -> tuple[float, str]:
        if self.dms_settings.k_factor > 0:
            return (sample.dms_mV_per_V / self.dms_settings.k_factor) * 1_000_000.0, "um/m"
        return sample.dms_mV_per_V, "mV/V"

    def compute_ain2_display(self, sample: Sample) -> tuple[float, str]:
        if self.ain2_settings.mode == "weg":
            return sample.ain2_value, "mm"
        if self.ain2_settings.volt_display == "scaled":
            span = self.ain2_settings.scale_max - self.ain2_settings.scale_min
            value = self.ain2_settings.scale_min + (sample.ain2_value / 10.0) * span
            return value, self.ain2_settings.scaling_unit
        return sample.ain2_value, "V"

    def _fmt_value(self, value: float, unit: str) -> str:
        if isinstance(value, float) and math.isnan(value):
            return f"nan {unit}"
        if unit in {"mV/V", "V", "mm", "%", "bar", "kN", "C"}:
            return f"{value:.4f} {unit}"
        return f"{value:.2f} {unit}"

    def _field_value(self, sample: Sample, field: str) -> tuple[float, str]:
        if field == "dms_display":
            return self.compute_dms_display(sample)
        if field == "ain2_display":
            return self.compute_ain2_display(sample)
        return sample.temp_c, "C"

    def refresh_plots(self) -> None:
        samples = list(self.receiver.samples)
        self.draw_plot(self.plot1, samples, self.combo1.currentText())
        self.draw_plot(self.plot2, samples, self.combo2.currentText())

    def draw_plot(self, plot: pg.PlotWidget, samples: list[Sample], field: str) -> None:
        plot.clear()
        if not samples:
            plot.setTitle(field)
            return

        window_s = max(1.0, float(self.window_s))

        label_unit = self._field_value(samples[-1], field)[1]
        pretty_name = {
            "dms_display": "DMS",
            "ain2_display": "Channel 2",
            "temp_c": "Temperature",
        }.get(field, field)
        plot.setTitle(f"{pretty_name} [{label_unit}]")
        plot.setLabel("bottom", "Time relative to latest sample, s")
        plot.setLabel("left", label_unit)

        t0 = samples[-1].t_pc
        xs: list[float] = []
        ys: list[float] = []
        for sample in samples:
            x = sample.t_pc - t0
            if x < -window_s:
                continue
            y, _ = self._field_value(sample, field)
            if isinstance(y, float) and math.isnan(y):
                continue
            xs.append(x)
            ys.append(y)

        if xs:
            plot.plot(xs, ys, pen=pg.mkPen(width=1.8))

    def closeEvent(self, event: QtGui.QCloseEvent | QtCore.QEvent) -> None:  # type: ignore[name-defined]
        self._save_local_settings()
        self.logger.stop()
        self.receiver.stop()
        super().closeEvent(event)


def main() -> None:
    app = QtWidgets.QApplication([])
    pg.setConfigOptions(antialias=False)
    win = App()
    win.show()
    app.exec()


if __name__ == "__main__":
    main()
