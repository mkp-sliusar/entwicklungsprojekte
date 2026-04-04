#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import socket
import struct
import threading
import time
from datetime import datetime
from pathlib import Path
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


def _safe_plot_field(value: Any, fallback: str) -> str:
    return value if value in PLOT_FIELDS else fallback


def _short(text: Any, max_len: int = 14) -> str:
    value = "-" if text is None else str(text).strip()
    if not value:
        return "-"
    return value if len(value) <= max_len else value[: max_len - 1] + "…"


@dataclass
class Sample:
    seq: int
    t_pc: float
    t_ctrl: float
    flags: int
    dms_raw: int
    dms_mV: float
    dms_mV_per_V: float
    ain2_raw: int
    ain2_mV: float
    ain2_value: float
    temp_c: float

    @property
    def dms_on(self) -> bool:
        return bool(self.flags & (1 << 0))

    @property
    def dms_valid(self) -> bool:
        return bool(self.flags & (1 << 1))

    @property
    def ain2_on(self) -> bool:
        return bool(self.flags & (1 << 2))

    @property
    def ain2_valid(self) -> bool:
        return bool(self.flags & (1 << 3))

    @property
    def temp_on(self) -> bool:
        return bool(self.flags & (1 << 4))

    @property
    def temp_valid(self) -> bool:
        return bool(self.flags & (1 << 5))

    @property
    def ain2_mode_volt(self) -> bool:
        return bool(self.flags & (1 << 6))


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


@dataclass
class TempSettings:
    enabled: bool = True


@dataclass
class CsvLoggerSettings:
    time_format: str = "gantner_timecounter"
    include_seq: bool = True
    include_dms_display: bool = True
    include_dms_mvv: bool = False
    include_dms_mv: bool = False
    include_dms_raw: bool = False
    include_ain2_display: bool = True
    include_ain2_mv: bool = False
    include_ain2_raw: bool = False
    include_temp_display: bool = True


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
                    unpacked = PACKET_STRUCT.unpack(raw)
                    version = unpacked[1]
                    packet_bytes = unpacked[2]
                    if version != 1 or packet_bytes != PACKET_SIZE:
                        del buf[0]
                        continue
                    del buf[:PACKET_SIZE]

                    flags = int(unpacked[6])
                    temp_raw = int(unpacked[13])
                    temp_c = temp_raw / 100.0 if (flags & (1 << 5)) and temp_raw != -32768 else float("nan")
                    dms_raw = int(unpacked[7])
                    dms_mV = float(unpacked[8])
                    dms_mV_per_V = float(unpacked[9])
                    ain2_raw = int(unpacked[10])
                    ain2_mV = float(unpacked[11])
                    ain2_value = float(unpacked[12])
                    if not (flags & (1 << 1)):
                        dms_mV = float("nan")
                        dms_mV_per_V = float("nan")
                    if not (flags & (1 << 3)):
                        ain2_mV = float("nan")
                        ain2_value = float("nan")
                    t_ms = int(unpacked[4])
                    t_us = int(unpacked[5])
                    t_ctrl = t_ms / 1000.0 + t_us / 1_000_000.0

                    self.samples.append(
                        Sample(
                            seq=int(unpacked[3]),
                            t_pc=time.time(),
                            t_ctrl=t_ctrl,
                            flags=flags,
                            dms_raw=dms_raw,
                            dms_mV=dms_mV,
                            dms_mV_per_V=dms_mV_per_V,
                            ain2_raw=ain2_raw,
                            ain2_mV=ain2_mV,
                            ain2_value=ain2_value,
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
        self.headers: list[str] = []

    def start(self, path: str, headers: list[str]) -> None:
        self.stop()
        self.file = open(path, "w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        self.headers = list(headers)
        self.writer.writerow(self.headers)
        self.path = path
        self.rows = 0

    def write_row(self, row: list[Any]) -> None:
        if self.writer:
            self.writer.writerow(row)
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
        self.headers = []


class DecimalLineEdit(QtWidgets.QLineEdit):
    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        rx = QtCore.QRegularExpression(r"^\d*(?:[\.,]\d*)?$")
        self.setValidator(QtGui.QRegularExpressionValidator(rx, self))

    def set_float_value(self, value: float, decimals: int = 4) -> None:
        text = f"{value:.{decimals}f}".rstrip("0").rstrip(".")
        self.setText(text if text else "0")

    def float_value(self) -> float:
        text = self.text().strip().replace(",", ".")
        if not text:
            return 0.0
        return float(text)


class FlexibleDoubleSpinBox(QtWidgets.QDoubleSpinBox):
    def validate(self, text: str, pos: int) -> tuple[QtGui.QValidator.State, str, int]:
        normalized = text.replace(",", ".")
        return super().validate(normalized, pos)

    def valueFromText(self, text: str) -> float:
        normalized = text.strip().replace(",", ".")
        if not normalized:
            return 0.0
        try:
            return float(normalized)
        except ValueError:
            return super().valueFromText(normalized)

    def textFromValue(self, value: float) -> str:
        return f"{value:.{self.decimals()}f}".rstrip("0").rstrip(".") if self.decimals() > 0 else str(int(value))

    def fixup(self, text: str) -> str:
        return text.replace(",", ".")


class TempSettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, settings: TempSettings) -> None:
        super().__init__(parent)
        self.setWindowTitle("Temperature settings")
        self.setModal(True)
        self.resize(360, 150)

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()
        layout.addLayout(form)

        self.enable_box = QtWidgets.QCheckBox("Enable temperature sensor")
        self.enable_box.setChecked(settings.enabled)
        form.addRow(self.enable_box)

        note = QtWidgets.QLabel("Enable or disable the temperature stream from the controller.")
        note.setStyleSheet("color:#666;")
        note.setWordWrap(True)
        layout.addWidget(note)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def value(self) -> TempSettings:
        return TempSettings(enabled=self.enable_box.isChecked())


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

        self.k_factor = DecimalLineEdit()
        self.k_factor.setPlaceholderText("not set")
        self.k_factor.set_float_value(max(0.0, settings.k_factor), 4)
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
            k_factor=float(self.k_factor.float_value()),
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
        self.weg_label = form.labelForField(self.weg_combo)

        self.display_combo = QtWidgets.QComboBox()
        self.display_combo.addItems(["volts", "scaled"])
        self.display_combo.setCurrentText(settings.volt_display)
        form.addRow("10 V display", self.display_combo)
        self.display_label = form.labelForField(self.display_combo)

        self.unit_combo = QtWidgets.QComboBox()
        self.unit_combo.addItems(UNIT_OPTIONS)
        self.unit_combo.setCurrentText(settings.scaling_unit)
        form.addRow("Scaled unit", self.unit_combo)
        self.unit_label = form.labelForField(self.unit_combo)

        self.scale_min = QtWidgets.QDoubleSpinBox()
        self.scale_min.setDecimals(3)
        self.scale_min.setRange(-1_000_000.0, 1_000_000.0)
        self.scale_min.setValue(settings.scale_min)
        form.addRow("Scale minimum", self.scale_min)
        self.scale_min_label = form.labelForField(self.scale_min)

        self.scale_max = QtWidgets.QDoubleSpinBox()
        self.scale_max.setDecimals(3)
        self.scale_max.setRange(-1_000_000.0, 1_000_000.0)
        self.scale_max.setValue(settings.scale_max)
        form.addRow("Scale maximum", self.scale_max)
        self.scale_max_label = form.labelForField(self.scale_max)

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

    def _set_row_visible(self, label: QtWidgets.QWidget | None, field: QtWidgets.QWidget, visible: bool) -> None:
        if label is not None:
            label.setVisible(visible)
        field.setVisible(visible)

    def _update_visibility(self) -> None:
        is_weg = self.mode_combo.currentText() == "weg"
        is_scaled = self.display_combo.currentText() == "scaled"

        self._set_row_visible(self.weg_label, self.weg_combo, is_weg)
        self._set_row_visible(self.display_label, self.display_combo, not is_weg)
        self._set_row_visible(self.unit_label, self.unit_combo, (not is_weg) and is_scaled)
        self._set_row_visible(self.scale_min_label, self.scale_min, (not is_weg) and is_scaled)
        self._set_row_visible(self.scale_max_label, self.scale_max, (not is_weg) and is_scaled)

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


class CsvLoggerSettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, settings: CsvLoggerSettings, available: dict[str, bool]) -> None:
        super().__init__(parent)
        self.setWindowTitle("CSV logger settings")
        self.setModal(True)
        self.resize(520, 520)
        self.available = available

        layout = QtWidgets.QVBoxLayout(self)

        general_box = QtWidgets.QGroupBox("Time and general")
        general_form = QtWidgets.QFormLayout(general_box)
        self.time_format = QtWidgets.QComboBox()
        self.time_format.addItem("Gantner CSV DateTime", "gantner_timecounter")
        self.time_format.addItem("ISO 8601", "iso8601")
        self.time_format.addItem("Unix", "unix")
        idx = self.time_format.findData(settings.time_format)
        if idx >= 0:
            self.time_format.setCurrentIndex(idx)
        general_form.addRow("Time format", self.time_format)
        general_form.addRow(QtWidgets.QLabel("Gantner CSV DateTime writes local time as YYYY-MM-DD HH:MM:SS.ffffff for better Test.Viewer compatibility."))
        self.include_seq = QtWidgets.QCheckBox("Sequence")
        self.include_seq.setChecked(settings.include_seq)
        general_form.addRow(self.include_seq)
        layout.addWidget(general_box)

        self.dms_box = QtWidgets.QGroupBox("DMS")
        dms_layout = QtWidgets.QVBoxLayout(self.dms_box)
        self.cb_dms_display = QtWidgets.QCheckBox("Displayed value")
        self.cb_dms_display.setChecked(settings.include_dms_display)
        self.cb_dms_mvv = QtWidgets.QCheckBox("mV/V")
        self.cb_dms_mvv.setChecked(settings.include_dms_mvv)
        self.cb_dms_mv = QtWidgets.QCheckBox("mV")
        self.cb_dms_mv.setChecked(settings.include_dms_mv)
        self.cb_dms_raw = QtWidgets.QCheckBox("Raw")
        self.cb_dms_raw.setChecked(settings.include_dms_raw)
        for cb in (self.cb_dms_display, self.cb_dms_mvv, self.cb_dms_mv, self.cb_dms_raw):
            dms_layout.addWidget(cb)
        layout.addWidget(self.dms_box)

        self.ain2_box = QtWidgets.QGroupBox("Channel 2")
        ain2_layout = QtWidgets.QVBoxLayout(self.ain2_box)
        self.cb_ain2_display = QtWidgets.QCheckBox("Displayed value")
        self.cb_ain2_display.setChecked(settings.include_ain2_display)
        self.cb_ain2_mv = QtWidgets.QCheckBox("mV")
        self.cb_ain2_mv.setChecked(settings.include_ain2_mv)
        self.cb_ain2_raw = QtWidgets.QCheckBox("Raw")
        self.cb_ain2_raw.setChecked(settings.include_ain2_raw)
        for cb in (self.cb_ain2_display, self.cb_ain2_mv, self.cb_ain2_raw):
            ain2_layout.addWidget(cb)
        layout.addWidget(self.ain2_box)

        self.temp_box = QtWidgets.QGroupBox("Temperature")
        temp_layout = QtWidgets.QVBoxLayout(self.temp_box)
        self.cb_temp_display = QtWidgets.QCheckBox("Temperature value")
        self.cb_temp_display.setChecked(settings.include_temp_display)
        temp_layout.addWidget(self.cb_temp_display)
        layout.addWidget(self.temp_box)

        note = QtWidgets.QLabel(
            "Only valid controller signals are offered.\n"
            "Default time format = Gantner TimeCounter."
        )
        note.setStyleSheet("color:#666;")
        note.setWordWrap(True)
        layout.addWidget(note)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._apply_availability()

    def _apply_availability(self) -> None:
        dms_on = self.available.get("dms", False)
        ain2_on = self.available.get("ain2", False)
        temp_on = self.available.get("temp", False)
        self.dms_box.setVisible(dms_on)
        self.ain2_box.setVisible(ain2_on)
        self.temp_box.setVisible(temp_on)
        if not dms_on:
            for cb in (self.cb_dms_display, self.cb_dms_mvv, self.cb_dms_mv, self.cb_dms_raw):
                cb.setChecked(False)
        if not ain2_on:
            for cb in (self.cb_ain2_display, self.cb_ain2_mv, self.cb_ain2_raw):
                cb.setChecked(False)
        if not temp_on:
            self.cb_temp_display.setChecked(False)

    def value(self) -> CsvLoggerSettings:
        return CsvLoggerSettings(
            time_format=str(self.time_format.currentData()),
            include_seq=self.include_seq.isChecked(),
            include_dms_display=self.cb_dms_display.isChecked(),
            include_dms_mvv=self.cb_dms_mvv.isChecked(),
            include_dms_mv=self.cb_dms_mv.isChecked(),
            include_dms_raw=self.cb_dms_raw.isChecked(),
            include_ain2_display=self.cb_ain2_display.isChecked(),
            include_ain2_mv=self.cb_ain2_mv.isChecked(),
            include_ain2_raw=self.cb_ain2_raw.isChecked(),
            include_temp_display=self.cb_temp_display.isChecked(),
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
        self.plot1_field = _safe_plot_field(self.settings_store.value("ui/plot1_field", "dms_display", str), "dms_display")
        self.plot2_field = _safe_plot_field(self.settings_store.value("ui/plot2_field", "ain2_display", str), "ain2_display")
        self.last_csv_dir = self.settings_store.value("csv/last_dir", "", str)

        self.receiver = StreamReceiver()
        self.logger = CsvLogger()
        self.last_logged_seq: int | None = None
        self.last_rate_seq: int | None = None
        self.rate_times: deque[float] = deque(maxlen=5000)
        self.dms_settings = self._load_dms_settings()
        self.ain2_settings = self._load_ain2_settings()
        self.temp_settings = self._load_temp_settings()
        self.csv_settings = self._load_csv_settings()
        self.csv_specs: list[tuple[str, Any]] = []
        self.remote_state: dict[str, Any] = {}
        self.ctrl_time_anchor: float | None = None

        self._build_ui()
        geometry = self.settings_store.value("ui/geometry")
        if isinstance(geometry, QtCore.QByteArray) and not geometry.isEmpty():
            self.restoreGeometry(geometry)
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

    def _load_temp_settings(self) -> TempSettings:
        return TempSettings(
            enabled=self.settings_store.value("temp/enabled", True, bool),
        )

    def _load_csv_settings(self) -> CsvLoggerSettings:
        return CsvLoggerSettings(
            time_format=self.settings_store.value("csv/time_format", "gantner_timecounter", str),
            include_seq=self.settings_store.value("csv/include_seq", True, bool),
            include_dms_display=self.settings_store.value("csv/include_dms_display", True, bool),
            include_dms_mvv=self.settings_store.value("csv/include_dms_mvv", False, bool),
            include_dms_mv=self.settings_store.value("csv/include_dms_mv", False, bool),
            include_dms_raw=self.settings_store.value("csv/include_dms_raw", False, bool),
            include_ain2_display=self.settings_store.value("csv/include_ain2_display", True, bool),
            include_ain2_mv=self.settings_store.value("csv/include_ain2_mv", False, bool),
            include_ain2_raw=self.settings_store.value("csv/include_ain2_raw", False, bool),
            include_temp_display=self.settings_store.value("csv/include_temp_display", True, bool),
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
        self.settings_store.setValue("temp/enabled", self.temp_settings.enabled)
        self.settings_store.setValue("csv/time_format", self.csv_settings.time_format)
        self.settings_store.setValue("csv/include_seq", self.csv_settings.include_seq)
        self.settings_store.setValue("csv/include_dms_display", self.csv_settings.include_dms_display)
        self.settings_store.setValue("csv/include_dms_mvv", self.csv_settings.include_dms_mvv)
        self.settings_store.setValue("csv/include_dms_mv", self.csv_settings.include_dms_mv)
        self.settings_store.setValue("csv/include_dms_raw", self.csv_settings.include_dms_raw)
        self.settings_store.setValue("csv/include_ain2_display", self.csv_settings.include_ain2_display)
        self.settings_store.setValue("csv/include_ain2_mv", self.csv_settings.include_ain2_mv)
        self.settings_store.setValue("csv/include_ain2_raw", self.csv_settings.include_ain2_raw)
        self.settings_store.setValue("csv/include_temp_display", self.csv_settings.include_temp_display)
        self.settings_store.setValue("csv/last_dir", self.last_csv_dir)
        self.settings_store.setValue("ui/plot1_field", getattr(self, "plot1_field", "dms_display"))
        self.settings_store.setValue("ui/plot2_field", getattr(self, "plot2_field", "ain2_display"))
        self.settings_store.setValue("ui/geometry", self.saveGeometry())
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
        right.setSpacing(6)
        body.addLayout(right, 1)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        self.plot1_panel = self._build_plot_panel("Graph 1", "dms_display", 1)
        self.plot2_panel = self._build_plot_panel("Graph 2", "ain2_display", 2)
        splitter.addWidget(self.plot1_panel)
        splitter.addWidget(self.plot2_panel)
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
        self.btn_csv_settings = QtWidgets.QPushButton("⚙")
        self.btn_csv_settings.setFixedWidth(42)

        button_height = 30
        button_width = 110
        for button in (self.btn_connect, self.btn_disconnect, self.btn_csv_start, self.btn_csv_stop):
            button.setFixedSize(button_width, button_height)

        self.btn_connect.clicked.connect(self.connect_stream)
        self.btn_disconnect.clicked.connect(self.disconnect_stream)
        self.btn_network.clicked.connect(self.open_network_settings)
        self.btn_csv_start.clicked.connect(self.start_csv)
        self.btn_csv_stop.clicked.connect(self.stop_csv)
        self.btn_csv_settings.clicked.connect(self.open_csv_settings)

        row = QtWidgets.QHBoxLayout()
        row.setSpacing(8)
        row.addWidget(self.btn_connect)
        row.addWidget(self.btn_disconnect)
        row.addWidget(self.btn_network)
        row.addWidget(self.btn_csv_start)
        row.addWidget(self.btn_csv_stop)
        row.addWidget(self.btn_csv_settings)
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
        outer.addWidget(self._sensor_row("Temperature", self.temp_value, self.temp_meta, self.open_temp_settings, include_button=True))
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

    def _build_plot_panel(self, title: str, default_field: str, index: int) -> QtWidgets.QWidget:
        panel = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        header = QtWidgets.QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(6)

        title_label = QtWidgets.QLabel(title)
        title_label.setStyleSheet("font-weight:600;")
        header.addWidget(title_label)

        combo = QtWidgets.QComboBox()
        combo.addItems(PLOT_FIELDS)
        stored_field = self.plot1_field if index == 1 else self.plot2_field
        combo.setCurrentText(_safe_plot_field(stored_field, default_field))
        combo.setMaximumWidth(190)
        combo.currentTextChanged.connect(self._on_plot_selection_changed)
        combo.currentIndexChanged.connect(self.refresh_plots)
        header.addWidget(combo)

        header.addStretch(1)
        layout.addLayout(header)

        plot = pg.PlotWidget()
        plot.setBackground("#111111")
        plot.showGrid(x=True, y=True, alpha=0.25)
        layout.addWidget(plot, 1)

        if index == 1:
            self.combo1 = combo
            self.plot1 = plot
        else:
            self.combo2 = combo
            self.plot2 = plot
        return panel

    def _on_plot_selection_changed(self) -> None:
        self.plot1_field = self.combo1.currentText() if hasattr(self, "combo1") else "dms_display"
        self.plot2_field = self.combo2.currentText() if hasattr(self, "combo2") else "ain2_display"
        self._save_local_settings()

    def _update_sensor_buttons(self) -> None:
        self.dms_meta.setText(
            f"{self.dms_settings.frequency_hz} Hz | "
            f"{'mV/V' if self.dms_settings.k_factor <= 0 else 'um/m'}"
        )
        ain2_tail = self._ain2_display_unit()
        self.ain2_meta.setText(f"{self.ain2_settings.frequency_hz} Hz | {self.ain2_settings.mode} | {ain2_tail}")
        self.temp_meta.setText("enabled" if self.temp_settings.enabled else "off")

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

    def open_temp_settings(self) -> None:
        dlg = TempSettingsDialog(self, self.temp_settings)
        if dlg.exec() == QtWidgets.QDialog.Accepted:
            self.temp_settings = dlg.value()
            self._save_local_settings()
            self._update_sensor_buttons()
            self.refresh_plots()
            self.push_config_to_device(show_success=True)

    def open_csv_settings(self) -> None:
        dlg = CsvLoggerSettingsDialog(self, self.csv_settings, self._csv_available_sources())
        if dlg.exec() == QtWidgets.QDialog.Accepted:
            self.csv_settings = self._coerce_csv_settings(dlg.value())
            self._save_local_settings()

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

    def _csv_available_sources(self) -> dict[str, bool]:
        return {
            "dms": bool(self.dms_settings.enabled),
            "ain2": bool(self.ain2_settings.enabled),
            "temp": bool(self.temp_settings.enabled),
        }

    def _coerce_csv_settings(self, settings: CsvLoggerSettings) -> CsvLoggerSettings:
        available = self._csv_available_sources()
        if not available["dms"]:
            settings.include_dms_display = False
            settings.include_dms_mvv = False
            settings.include_dms_mv = False
            settings.include_dms_raw = False
        if not available["ain2"]:
            settings.include_ain2_display = False
            settings.include_ain2_mv = False
            settings.include_ain2_raw = False
        if not available["temp"]:
            settings.include_temp_display = False
        return settings

    @staticmethod
    def _header_from_unit(prefix: str, unit: str) -> str:
        safe = unit.replace("/", "_per_").replace(" ", "_").replace("%", "pct")
        return f"{prefix}_{safe}"

    @staticmethod
    def _format_time_for_csv(ts: float, fmt: str) -> str | float:
        if fmt == "unix":
            return round(ts, 6)
        local_tz = datetime.now().astimezone().tzinfo
        dt_local = datetime.fromtimestamp(ts, local_tz) if local_tz is not None else datetime.fromtimestamp(ts)
        if fmt == "iso8601":
            return dt_local.isoformat(timespec="milliseconds")
        if fmt == "gantner_timecounter":
            return dt_local.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt_local.microsecond:06d}"
        base = datetime(1899, 12, 30, tzinfo=local_tz) if local_tz is not None else datetime(1899, 12, 30)
        delta = dt_local - base
        return round(delta.total_seconds() / 86400.0, 12)

    def _absolute_sample_time(self, sample: Sample) -> float:
        ctrl_t = sample.t_ctrl
        if math.isfinite(ctrl_t) and ctrl_t > 0:
            if self.ctrl_time_anchor is None:
                self.ctrl_time_anchor = sample.t_pc - ctrl_t
            return self.ctrl_time_anchor + ctrl_t
        return sample.t_pc

    def _make_csv_specs(self) -> list[tuple[str, Any]]:
        settings = self._coerce_csv_settings(CsvLoggerSettings(**self.csv_settings.__dict__))
        specs: list[tuple[str, Any]] = []
        time_format = settings.time_format
        time_header = {
            "gantner_timecounter": "time_gantner_datetime",
            "iso8601": "time_iso8601",
            "unix": "time_unix",
        }.get(time_format, "time_gantner_timecounter")
        specs.append((time_header, lambda s, tf=time_format: self._format_time_for_csv(self._absolute_sample_time(s), tf)))
        if settings.include_seq:
            specs.append(("seq", lambda s: s.seq))

        if self.dms_settings.enabled:
            if settings.include_dms_display:
                unit = "um_per_m" if self.dms_settings.k_factor > 0 else "mV_per_V"
                specs.append((self._header_from_unit("dms_display", unit), lambda s: self.compute_dms_display(s)[0]))
            if settings.include_dms_mvv:
                specs.append(("dms_mV_per_V", lambda s: s.dms_mV_per_V))
            if settings.include_dms_mv:
                specs.append(("dms_mV", lambda s: s.dms_mV))
            if settings.include_dms_raw:
                specs.append(("dms_raw", lambda s: s.dms_raw))

        if self.ain2_settings.enabled:
            if settings.include_ain2_display:
                display_unit = "mm"
                if self.ain2_settings.mode != "weg":
                    display_unit = self.ain2_settings.scaling_unit if self.ain2_settings.volt_display == "scaled" else "V"
                specs.append((self._header_from_unit("ain2_display", display_unit), lambda s: self.compute_ain2_display(s)[0]))
            if settings.include_ain2_mv:
                specs.append(("ain2_mV", lambda s: s.ain2_mV))
            if settings.include_ain2_raw:
                specs.append(("ain2_raw", lambda s: s.ain2_raw))

        if self.temp_settings.enabled and settings.include_temp_display:
            specs.append(("temp_C", lambda s: s.temp_c))
        return specs

    def _build_csv_row(self, sample: Sample) -> list[Any]:
        return [getter(sample) for _, getter in self.csv_specs]

    def _wait_for_receiver_status(self, expected: str, timeout_s: float = 3.0) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            QtWidgets.QApplication.processEvents()
            if self.receiver.status == expected:
                return True
            if self.receiver.status.startswith("error") or self.receiver.status in {"server_closed", "disconnected"}:
                return False
            time.sleep(0.05)
        return self.receiver.status == expected

    def _set_controller_stream_mode(self, enabled: bool) -> None:
        response = requests.post(
            f"http://{self.host}/api/stream",
            json={"enabled": int(enabled)},
            timeout=HTTP_TIMEOUT_S,
        )
        response.raise_for_status()

    def connect_stream(self) -> None:
        self._save_local_settings()
        self.stream_mode.setText("Connecting socket...")
        self.stream_target.setText(f"{self.host}:{self.port}")
        self.ctrl_time_anchor = None
        self.receiver.connect(self.host, self.port)
        if not self._wait_for_receiver_status("connected", timeout_s=3.0):
            self.receiver.stop()
            self.stream_mode.setText("Offline")
            QtWidgets.QMessageBox.warning(
                self,
                "Connect failed",
                f"Socket connection failed.\n\nStatus: {self.receiver.status}",
            )
            return

        try:
            self.stream_mode.setText("Enabling PC stream...")
            self._set_controller_stream_mode(True)
            self.read_config()
            self.stream_mode.setText("PC stream")
            self.stream_target.setText(f"{self.host}:{self.port}")
        except Exception as exc:
            self.receiver.stop()
            self.stream_mode.setText("Offline")
            QtWidgets.QMessageBox.warning(
                self,
                "Stream enable failed",
                f"Socket connected, but enabling PC stream on controller failed.\n\n{exc}",
            )

    def disconnect_stream(self) -> None:
        warning_text: str | None = None
        if self.receiver.status == "connected":
            try:
                self.stream_mode.setText("Disabling PC stream...")
                self._set_controller_stream_mode(False)
            except Exception as exc:
                warning_text = f"Controller PC stream disable failed before socket close.\n\n{exc}"
        self.receiver.stop()
        self.ctrl_time_anchor = None
        self.stream_mode.setText("Offline")
        if warning_text:
            QtWidgets.QMessageBox.warning(self, "Disconnect warning", warning_text)

    def start_csv(self) -> None:
        self.csv_settings = self._coerce_csv_settings(self.csv_settings)
        self.csv_specs = self._make_csv_specs()
        if not self.csv_specs:
            QtWidgets.QMessageBox.warning(self, "CSV logger", "No valid CSV fields are selected.")
            return
        default_name = f"lorasense_stream_{time.strftime('%Y%m%d_%H%M%S')}.csv"
        suggested_path = default_name if not self.last_csv_dir else str(Path(self.last_csv_dir) / default_name)
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Save CSV",
            suggested_path,
            "CSV files (*.csv)",
        )
        if path:
            self.last_csv_dir = str(Path(path).expanduser().resolve().parent)
            self.logger.start(path, [header for header, _ in self.csv_specs])
            self.csv_label.setText(path)
            self.last_logged_seq = None
            self._save_local_settings()

    def stop_csv(self) -> None:
        self.logger.stop()
        self.csv_specs = []
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
        self.temp_settings.enabled = bool(cfg.get("temp_enabled", self.temp_settings.enabled))
        self.csv_settings = self._coerce_csv_settings(self.csv_settings)

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
            "temp_enabled": int(self.temp_settings.enabled),
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
                    self.logger.write_row(self._build_csv_row(sample))
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
        if self.temp_settings.enabled and sample.temp_on:
            self.temp_value.setText(self._fmt_value(sample.temp_c, "C"))
        else:
            self.temp_value.setText("off")

    def compute_dms_display(self, sample: Sample) -> tuple[float, str]:
        if not sample.dms_on or not sample.dms_valid or math.isnan(sample.dms_mV_per_V):
            return float("nan"), ("um/m" if self.dms_settings.k_factor > 0 else "mV/V")
        if self.dms_settings.k_factor > 0:
            return (sample.dms_mV_per_V / self.dms_settings.k_factor) * 1000.0, "um/m"
        return sample.dms_mV_per_V, "mV/V"

    def compute_ain2_display(self, sample: Sample) -> tuple[float, str]:
        unit = "mm" if self.ain2_settings.mode == "weg" else (self.ain2_settings.scaling_unit if self.ain2_settings.volt_display == "scaled" else "V")
        if not sample.ain2_on or not sample.ain2_valid or math.isnan(sample.ain2_value):
            return float("nan"), unit
        if self.ain2_settings.mode == "weg":
            return sample.ain2_value, "mm"
        if self.ain2_settings.volt_display == "scaled":
            span = self.ain2_settings.scale_max - self.ain2_settings.scale_min
            value = self.ain2_settings.scale_min + (sample.ain2_value / 10.0) * span
            return value, self.ain2_settings.scaling_unit
        return sample.ain2_value, "V"

    def _fmt_value(self, value: float, unit: str) -> str:
        if isinstance(value, float) and math.isnan(value):
            return f"-- {unit}"
        if unit in {"mV/V", "V", "mm", "%", "bar", "kN", "C"}:
            return f"{value:.4f} {unit}"
        return f"{value:.2f} {unit}"

    def _field_value(self, sample: Sample, field: str) -> tuple[float, str]:
        if field == "dms_display":
            return self.compute_dms_display(sample)
        if field == "ain2_display":
            return self.compute_ain2_display(sample)
        if not self.temp_settings.enabled:
            return float("nan"), "C"
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

        time_key = (lambda s: s.t_ctrl) if len(samples) >= 2 and samples[-1].t_ctrl > samples[0].t_ctrl else (lambda s: s.t_pc)
        t0 = time_key(samples[-1])
        xs: list[float] = []
        ys: list[float] = []
        for sample in samples:
            x = time_key(sample) - t0
            if x < -window_s:
                continue
            y, _ = self._field_value(sample, field)
            if isinstance(y, float) and math.isnan(y):
                continue
            xs.append(x)
            ys.append(y)

        if xs:
            plot.plot(xs, ys, pen=pg.mkPen(width=1.8))
            plot.enableAutoRange(axis=pg.ViewBox.XYAxes, enable=True)
            plot.autoRange()

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
