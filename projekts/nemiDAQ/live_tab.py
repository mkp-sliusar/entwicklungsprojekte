from __future__ import annotations

import csv
import os
from collections import deque
from datetime import datetime, timedelta

import numpy as np
from serial.tools import list_ports

from PySide6 import QtCore, QtGui, QtWidgets

from live_protocol import AF_ODR_PRESETS, RF_PRESETS
from trigger_recorder import AutoTriggerRecorder, TriggerConfig

from live_worker import SerialWorker
from live_plot import apply_live_plot_theme

class LiveTab(QtWidgets.QWidget):
    """
    Live stream tab:
      - COM port connect + receiver init
      - real-time plot
      - optional CSV recording (same column style as official)
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.worker: SerialWorker | None = None
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(50)
        self.timer.timeout.connect(self.redraw)

        # plot ring buffers
        self.window_seconds = 60.0
        self.t = deque()
        self.y1 = deque()
        self.y2 = deque()

        # live processing
        self._spike_filter_enabled: bool = True
        self._spike_thr_mvv: float = 0.05  # spike threshold (mV/V)
        # spike filter state per channel
        self._sf_prev1: float | None = None
        self._sf_prev2: float | None = None
        self._sf_run1: int = 0
        self._sf_run2: int = 0

        # per-channel offset
        self._offset_enabled: bool = False
        self._offset1_mvv: float = 0.0
        self._offset2_mvv: float = 0.0
        self._offset_sync_guard: bool = False
# recording
        self._rec_enabled = False
        self._rec_path: str | None = None  # auto-generated full file path
        self._rec_dir: str | None = None   # preferred output directory
        self._rec_fh = None
        self._rec_writer = None
        self._rec_start_dt: datetime | None = None

        # stream time origin (used for timestamps in CSV)
        self._stream_start_dt: datetime | None = None

        # auto-trigger recorder (event CSV)
        self._trig_cfg = TriggerConfig(
            enabled=False,
            threshold_mvv=0.02,
            pre_s=5.0,
            post_s=5.0,
            cooldown_s=5.0,
            out_dir="",  # defaulted to ./messdata when empty
            filename_prefix="trigger",
        )
        self._trig: AutoTriggerRecorder | None = None

        self._build_ui()
        self._build_plot()

        self.refresh_ports()
        self._sync_settings_from_main()

        # sync processing UI defaults
        try:
            self.cb_offset_main.setChecked(bool(self._offset_enabled))
        except Exception:
            pass
        self._update_offset_label()

    # ---------- UI ----------
    def _build_ui(self):
        # Main view: only Start/Stop + value + status + plot.
        layout = QtWidgets.QVBoxLayout(self)

        top = QtWidgets.QHBoxLayout()
        self.btn_start = QtWidgets.QPushButton("Start")
        self.btn_stop = QtWidgets.QPushButton("Stop")
        self.btn_stop.setEnabled(False)
        self.btn_start.clicked.connect(self.start_worker)
        self.btn_stop.clicked.connect(self.stop_worker)
        top.addWidget(self.btn_start)
        top.addWidget(self.btn_stop)

        self.cb_ch1 = QtWidgets.QCheckBox("CH1")
        self.cb_ch1.setChecked(True)
        self.cb_ch2 = QtWidgets.QCheckBox("CH2")
        self.cb_ch2.setChecked(False)
        top.addWidget(self.cb_ch1)
        top.addWidget(self.cb_ch2)
        self.cb_record_main = QtWidgets.QCheckBox("Record CSV")
        self.cb_record_main.setChecked(False)
        self.cb_record_main.setTristate(False)
        self.cb_record_main.toggled.connect(self._record_toggled)
        top.addWidget(self.cb_record_main)

        self.cb_trig_main = QtWidgets.QCheckBox("Auto trigger")
        self.cb_trig_main.setChecked(bool(self._trig_cfg.enabled))
        self.cb_trig_main.setTristate(False)
        self.cb_trig_main.toggled.connect(self._trigger_toggled)
        top.addWidget(self.cb_trig_main)
        # offset controls (main bar)
        self.cb_offset_main = QtWidgets.QCheckBox("Offset")
        self.cb_offset_main.setChecked(False)
        self.cb_offset_main.setTristate(False)
        self.cb_offset_main.toggled.connect(self._offset_main_toggled)
        top.addWidget(self.cb_offset_main)

        self.lbl_offset = QtWidgets.QLabel("off1=0.000000 off2=0.000000")
        top.addWidget(self.lbl_offset)
        self.lbl_value = QtWidgets.QLabel("mV/V: —")

        self.lbl_dev_status = QtWidgets.QLabel("Temp: — | RSSI: — | Batt: —")
        self.lbl_dev_status.setTextFormat(QtCore.Qt.PlainText)
        self.lbl_dev_status.setWordWrap(False)
        self.lbl_dev_status.setMinimumWidth(360)
        self.lbl_dev_status.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Preferred)

        font = self.lbl_value.font()
        font.setPointSize(14)
        self.lbl_value.setFont(font)
        top.addWidget(self.lbl_value)
        top.addWidget(self.lbl_dev_status)

        self.lbl_status = QtWidgets.QLabel("Idle")
        self.lbl_status.setTextFormat(QtCore.Qt.PlainText)
        self.lbl_status.setWordWrap(False)
        self.lbl_status.setMaximumWidth(520)
        self.lbl_status.setMinimumWidth(520)
        self.lbl_status.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Preferred)
        top.addWidget(self.lbl_status)
        top.addStretch(1)

        layout.addLayout(top)

        # plot container
        self.plot_container = QtWidgets.QFrame()
        self.plot_container.setFrameShape(QtWidgets.QFrame.StyledPanel)
        layout.addWidget(self.plot_container, 1)
        # Settings dialog UI moved to live_settings.py
        from live_settings import build_settings_dialog
        build_settings_dialog(self)

        # sync processing widgets (created in settings dialog)
        if hasattr(self, "cb_spike_filter"):
            self.cb_spike_filter.blockSignals(True)
            self.cb_spike_filter.setChecked(bool(self._spike_filter_enabled))
            self.cb_spike_filter.blockSignals(False)
            self.cb_spike_filter.toggled.connect(self._spike_filter_toggled)
        if hasattr(self, "sp_spike_thr"):
            self.sp_spike_thr.blockSignals(True)
            self.sp_spike_thr.setValue(float(self._spike_thr_mvv))
            self.sp_spike_thr.blockSignals(False)
            self.sp_spike_thr.valueChanged.connect(self._spike_thr_changed)

        if hasattr(self, "cb_offset"):
            self.cb_offset.blockSignals(True)
            self.cb_offset.setChecked(bool(self._offset_enabled))
            self.cb_offset.blockSignals(False)
            self.cb_offset.toggled.connect(self._offset_toggled)
        if hasattr(self, "sp_offset1") and hasattr(self, "sp_offset2"):
            self.sp_offset1.blockSignals(True)
            self.sp_offset2.blockSignals(True)
            try:
                self.sp_offset1.setValue(float(self._offset1_mvv))
                self.sp_offset2.setValue(float(self._offset2_mvv))
            finally:
                self.sp_offset1.blockSignals(False)
                self.sp_offset2.blockSignals(False)

            self.sp_offset1.valueChanged.connect(self._offset_value_changed)
            self.sp_offset2.valueChanged.connect(self._offset_value_changed)
        if hasattr(self, "btn_zero"):
            self.btn_zero.clicked.connect(self._offset_zero_clicked)

        self._update_offset_label()
    def _build_plot(self):
        # Plot UI moved to live_plot.py
        from live_plot import build_live_plot
        build_live_plot(self, container=self.plot_container, dark=True)

    # ---------- helpers ----------
    def _log(self, s: str):
        self.log.appendPlainText(s)


    def _main_window(self):
        # Returns the top-level AppMainWindow if available.
        w = self.window()
        if w is None:
            return None
        if hasattr(w, "set_dark_theme") and hasattr(w, "set_offline_visible"):
            return w
        return None

    def _sync_settings_from_main(self) -> None:
        mw = self._main_window()
        if mw is None:
            return
        if hasattr(self, "cb_ui_dark"):
            self.cb_ui_dark.blockSignals(True)
            try:
                self.cb_ui_dark.setChecked(bool(mw.is_dark_theme()))
            finally:
                self.cb_ui_dark.blockSignals(False)

        if hasattr(self, "cb_ui_decoder"):
            self.cb_ui_decoder.blockSignals(True)
            try:
                self.cb_ui_decoder.setChecked(bool(mw.is_offline_visible()))
            finally:
                self.cb_ui_decoder.blockSignals(False)

    def _ui_dark_toggled(self, checked: bool) -> None:
        mw = self._main_window()
        if mw is not None:
            mw.set_dark_theme(bool(checked))

    def _ui_decoder_toggled(self, checked: bool) -> None:
        mw = self._main_window()
        if mw is not None:
            mw.set_offline_visible(bool(checked))

    def _messdata_dir(self) -> str:
        """Return default output folder ./messdata (project root) and ensure it exists."""
        root = os.getcwd()
        d = os.path.join(root, "messdata")
        os.makedirs(d, exist_ok=True)
        return d

    def refresh_ports(self):
        ports = [p.device for p in list_ports.comports()]
        cur = self.cmb_port.currentText()
        self.cmb_port.blockSignals(True)
        try:
            self.cmb_port.clear()
            self.cmb_port.addItems(ports)
            if cur and cur in ports:
                self.cmb_port.setCurrentText(cur)
            elif ports:
                self.cmb_port.setCurrentIndex(0)
        finally:
            self.cmb_port.blockSignals(False)

        self._log(f"Ports: {', '.join(ports) if ports else '(none)'}")

    def toggle_settings(self):
        """Show/hide the settings dialog."""
        if getattr(self, "settings_dialog", None) is None:
            return
        if self.settings_dialog.isVisible():
            self.settings_dialog.hide()
            return
        # refresh ports when opening
        self.refresh_ports()
        # sync UI switches (theme / decoder tab)
        self._sync_settings_from_main()
        self.settings_dialog.show()
        self.settings_dialog.raise_()
        self.settings_dialog.activateWindow()

    def _net_changed(self, text: str):
        if text == "Custom":
            self.sp_rfbyte.setEnabled(True)
            return
        self.sp_rfbyte.setEnabled(False)
        self.sp_rfbyte.setValue(int(RF_PRESETS.get(text, RF_PRESETS["HS 2 Mbit/s"])))

    def _record_toggled(self, state):
        # Accept both Qt state int and bool from toggled().
        enabled = bool(state) if isinstance(state, bool) else (state == QtCore.Qt.Checked)

        # sync both checkboxes (main + settings dialog)
        if hasattr(self, "cb_record_main") and self.cb_record_main.isChecked() != enabled:
            self.cb_record_main.blockSignals(True)
            self.cb_record_main.setChecked(enabled)
            self.cb_record_main.blockSignals(False)
        if hasattr(self, "cb_record") and self.cb_record.isChecked() != enabled:
            self.cb_record.blockSignals(True)
            self.cb_record.setChecked(enabled)
            self.cb_record.blockSignals(False)

        self._rec_enabled = enabled
        print(f"[REC] enabled={enabled}", flush=True)
        if not enabled:
            self._close_recording()
        else:
            if self.worker is not None and self.worker.isRunning():
                self._open_recording()

    def _window_changed(self):
        self.window_seconds = float(self.sp_window.value())


    # ---------- live processing ----------
    def _spike_filter_toggled(self, checked: bool) -> None:
        enabled = bool(checked)
        # sync settings checkbox (settings dialog)
        if hasattr(self, "cb_spike_filter") and self.cb_spike_filter.isChecked() != enabled:
            self.cb_spike_filter.blockSignals(True)
            try:
                self.cb_spike_filter.setChecked(enabled)
            finally:
                self.cb_spike_filter.blockSignals(False)

        self._spike_filter_enabled = enabled
        self._log(f"Spike filter: {'ON' if enabled else 'OFF'} (thr={self._spike_thr_mvv:g} mV/V)")
    def _spike_thr_changed(self, *args) -> None:
        try:
            self._spike_thr_mvv = float(self.sp_spike_thr.value())
        except Exception:
            return
        self._log(f"Spike threshold: {self._spike_thr_mvv:g} mV/V")

    def _offset_main_toggled(self, checked: bool) -> None:
        # Main toolbar behavior:
        # - when enabled -> auto Zero and enable offset
        # - when disabled -> disable offset
        if self._offset_sync_guard:
            return
        enabled = bool(checked)

        self._offset_sync_guard = True
        try:
            # sync Settings checkbox
            if hasattr(self, "cb_offset") and self.cb_offset.isChecked() != enabled:
                self.cb_offset.setChecked(enabled)
        finally:
            self._offset_sync_guard = False

        if enabled:
            self._offset_zero_clicked()
        else:
            self._offset_enabled = False
            self._update_offset_label()
            self._log("Offset: OFF")

    def _offset_toggled(self, checked: bool) -> None:
        # Settings dialog checkbox behavior:
        # - when enabled -> auto Zero and enable offset (sync with main)
        # - when disabled -> disable offset
        if self._offset_sync_guard:
            return
        enabled = bool(checked)

        self._offset_sync_guard = True
        try:
            if hasattr(self, "cb_offset_main") and self.cb_offset_main.isChecked() != enabled:
                self.cb_offset_main.setChecked(enabled)
        finally:
            self._offset_sync_guard = False

        if enabled:
            self._offset_zero_clicked()
        else:
            self._offset_enabled = False
            self._update_offset_label()
            self._log("Offset: OFF")
    def _offset_value_changed(self, *args) -> None:
        try:
            self._offset1_mvv = float(self.sp_offset1.value())
            self._offset2_mvv = float(self.sp_offset2.value())
        except Exception:
            return
        self._update_offset_label()
    def _offset_zero_clicked(self) -> None:
        # Set per-channel offsets so the current displayed values become ~0.
        v1 = None
        v2 = None
        try:
            if len(self.y1) > 0 and np.isfinite(self.y1[-1]):
                v1 = float(self.y1[-1])
        except Exception:
            v1 = None
        try:
            if len(self.y2) > 0 and np.isfinite(self.y2[-1]):
                v2 = float(self.y2[-1])
        except Exception:
            v2 = None

        if v1 is not None:
            self._offset1_mvv = float(v1)
            if hasattr(self, "sp_offset1"):
                self.sp_offset1.blockSignals(True)
                try:
                    self.sp_offset1.setValue(self._offset1_mvv)
                finally:
                    self.sp_offset1.blockSignals(False)

        if v2 is not None:
            self._offset2_mvv = float(v2)
            if hasattr(self, "sp_offset2"):
                self.sp_offset2.blockSignals(True)
                try:
                    self.sp_offset2.setValue(self._offset2_mvv)
                finally:
                    self.sp_offset2.blockSignals(False)

        self._offset_enabled = True

        # sync checkboxes without recursion
        self._offset_sync_guard = True
        try:
            if hasattr(self, "cb_offset_main") and not self.cb_offset_main.isChecked():
                self.cb_offset_main.setChecked(True)
            if hasattr(self, "cb_offset") and not self.cb_offset.isChecked():
                self.cb_offset.setChecked(True)
        finally:
            self._offset_sync_guard = False

        self._update_offset_label()
        self._log(f"Offset zero set: ch1={self._offset1_mvv:+.6f} ch2={self._offset2_mvv:+.6f} mV/V")
    def _update_offset_label(self) -> None:
        if self._offset_enabled:
            s = f"off1={self._offset1_mvv:+.6f} off2={self._offset2_mvv:+.6f}"
        else:
            s = "off1=0.000000 off2=0.000000"
        try:
            self.lbl_offset.setText(s)
        except Exception:
            pass
    def _apply_spike_filter(self, y: np.ndarray, *, ch: int) -> np.ndarray:
        # Single-sample spike suppressor (no drift, no runaway).
        # If a sample deviates from the previous value by >thr AND the *next* sample returns
        # close to the previous value, treat it as a one-point spike and replace with prev.
        y = np.asarray(y, dtype=np.float64)
        if (not self._spike_filter_enabled) or y.size == 0:
            return y

        thr = float(self._spike_thr_mvv)
        if thr <= 0:
            return y

        if ch == 1:
            prev = self._sf_prev1
        else:
            prev = self._sf_prev2

        out = y.copy()
        for i in range(out.size):
            v = out[i]
            if not np.isfinite(v):
                continue
            if prev is None:
                prev = float(v)
                continue

            dv = abs(float(v) - float(prev))
            if dv > thr:
                # look ahead (within the current block) to decide if it's a single-point spike
                nxt = None
                if i + 1 < out.size:
                    nxt = out[i + 1]
                if nxt is not None and np.isfinite(nxt) and abs(float(nxt) - float(prev)) <= thr:
                    out[i] = float(prev)
                    # prev stays unchanged
                    continue

            prev = float(out[i])

        if ch == 1:
            self._sf_prev1 = prev
        else:
            self._sf_prev2 = prev

        return out
    def choose_record_path(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Select CSV output folder", "")
        if d:
            self._rec_dir = d
            # Force a new filename on next start after changing the folder.
            self._rec_path = None
            self.ed_path.setText(d)
            self._log(f"Record folder: {d}")

            # If already recording, reopen immediately into the new folder.
            if self._rec_enabled and self.worker is not None and self.worker.isRunning():
                self._close_recording()
                self._open_recording()


    # ---------- auto trigger ----------
    def choose_trigger_dir(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Select trigger output folder", "")
        if d:
            self._trig_cfg.out_dir = d
            self.ed_trig_dir.setText(d)
            self._apply_trigger_cfg()
            self._log(f"Trigger folder: {d}")

    def _trigger_toggled(self, state):
        # Accept both Qt state int and bool from toggled().
        enabled = bool(state) if isinstance(state, bool) else (state == QtCore.Qt.Checked)

        if hasattr(self, "cb_trig_main") and self.cb_trig_main.isChecked() != enabled:
            self.cb_trig_main.blockSignals(True)
            self.cb_trig_main.setChecked(enabled)
            self.cb_trig_main.blockSignals(False)
        if hasattr(self, "cb_trig") and self.cb_trig.isChecked() != enabled:
            self.cb_trig.blockSignals(True)
            self.cb_trig.setChecked(enabled)
            self.cb_trig.blockSignals(False)

        self._trig_cfg.enabled = enabled
        print(f"[TRIG] enabled={enabled}", flush=True)
        self._trigger_cfg_changed()

    def _trigger_cfg_changed(self, *args):
        # values from widgets
        try:
            self._trig_cfg.threshold_mvv = float(self.sp_trig_thr.value())
            self._trig_cfg.pre_s = float(self.sp_trig_pre.value())
            self._trig_cfg.post_s = float(self.sp_trig_post.value())
            self._trig_cfg.cooldown_s = float(self.sp_trig_cool.value())
            self._trig_cfg.filename_prefix = str(self.ed_trig_prefix.text()).strip() or "trigger"
            # out_dir is changed only via folder chooser
        except Exception:
            pass

        self._apply_trigger_cfg()

    def _apply_trigger_cfg(self):
        # Default output folder if none chosen.
        if not self._trig_cfg.out_dir:
            self._trig_cfg.out_dir = self._messdata_dir()
            print(f"[TRIG] default out_dir: {self._trig_cfg.out_dir}", flush=True)
            try:
                self.ed_trig_dir.setText(str(self._trig_cfg.out_dir))
            except Exception:
                pass

        if self._trig is not None:
            self._trig.set_config(self._trig_cfg)
        self._update_trigger_state_label()

    def _update_trigger_state_label(self):
        if not self._trig_cfg.enabled:
            self.lbl_trig_state.setText("Off")
            return

        if self._trig is None:
            self.lbl_trig_state.setText("Enabled (idle)")
            return

        if self._trig.recording:
            p = self._trig.event_path or ""
            self.lbl_trig_state.setText("Recording… " + (os.path.basename(p) if p else ""))
            return

        if self._trig.armed:
            self.lbl_trig_state.setText("Armed")
        else:
            self.lbl_trig_state.setText("Arming…")

    # ---------- recording ----------
    def _open_recording(self):
        if not self._rec_enabled:
            return

        # If the user did not choose a folder, default to ./messdata (project root).
        if not self._rec_path:
            out_dir = self._rec_dir or self._messdata_dir()
            os.makedirs(out_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._rec_path = os.path.join(out_dir, f"record_{ts}.csv")
            try:
                self.ed_path.setText(out_dir)
            except Exception:
                pass
            self._log(f"Record file: {self._rec_path}")
        if self._rec_fh is not None:
            return

        os.makedirs(os.path.dirname(self._rec_path) or ".", exist_ok=True)
        self._rec_fh = open(self._rec_path, "w", newline="", encoding="utf-8")
        self._rec_writer = csv.writer(self._rec_fh, delimiter=";")
        self._rec_writer.writerow([
            "Timestamp (ASCII)",
            "Timestamp (s)",
            "Timecounter (s)",
            "Ch 1",
            "Ch 2",
            "Ch 3",
            "Ch 4",
        ])
        self._rec_start_dt = self._stream_start_dt or datetime.now()
        self._log("Recording started.")
        print(f"[REC] started: {self._rec_path}", flush=True)

    def _close_recording(self):
        if self._rec_fh is not None:
            try:
                self._rec_fh.flush()
                self._rec_fh.close()
            except Exception:
                pass
        self._rec_fh = None
        self._rec_writer = None
        self._rec_start_dt = None
        self._log("Recording stopped.")
        if self._rec_path:
            print(f"[REC] stopped: {self._rec_path}", flush=True)

    def _write_samples_to_csv(self, t_s: np.ndarray, y1: np.ndarray, y2: np.ndarray | None):
        if self._rec_writer is None or self._rec_start_dt is None:
            return
        # Skip NaNs (used for plot gaps)
        y2_iter = ([float('nan')] * len(y1)) if (y2 is None or len(y2)==0) else y2
        for ti, v1, v2 in zip(t_s, y1, y2_iter):
            if not np.isfinite(ti) or (not np.isfinite(v1) and not np.isfinite(v2)):
                continue
            dt = self._rec_start_dt + timedelta(seconds=float(ti))
            self._rec_writer.writerow([
                dt.strftime("%d.%m.%Y %H:%M:%S.%f"),
                f"{dt.timestamp():.6f}",
                f"{float(ti):.6f}",
                f"{float(v1) if np.isfinite(v1) else 0.0:.9f}",
                f"{float(v2) if np.isfinite(v2) else 0.0:.9f}",
                "0",
                "0",
            ])

    # ---------- start/stop ----------
    @QtCore.Slot()
    def start_worker(self):
        if self.worker is not None:
            return
        prefer_newfw = bool(getattr(self, "cb_newfw", None) and self.cb_newfw.isChecked())
        port = self.cmb_port.currentText().strip()
        if not port:
            QtWidgets.QMessageBox.critical(self, "Error", "COM port not selected.")
            return

        baud = int(self.ed_baud.text())
        rf_channel = int(self.sp_rfch.value())

        rf_speed_code = int(self.sp_rfbyte.value())

        # Max nodes code: 1=3 sensors, 2=8 sensors, 3=1 sensor (low latency)
        mn = self.cmb_nodes.currentText().lower()
        if mn.startswith("3"):
            max_sensors_code = 1
        elif mn.startswith("1"):
            max_sensors_code = 3
        else:
            max_sensors_code = 2

        gain = float(self.sp_gain.value())
        sign = +1.0  # sign control removed in refactor

        node_select = 0  # node selector removed in refactor (auto)

        af_odr_label = self.cmb_odr.currentText().strip()
        ad_odr_setting = int(AF_ODR_PRESETS.get(af_odr_label, AF_ODR_PRESETS.get("High Res / 100", 10)))

        # clear buffers
        self.t.clear()
        self.y1.clear(); self.y2.clear()
        self.curve1.setData([], []); self.curve2.setData([], [])
        self.lbl_value.setText("mV/V: —")

        # stream time origin (used for timestamps in CSV)
        self._stream_start_dt = datetime.now()

        # (re)create trigger recorder
        self._trig = AutoTriggerRecorder(self._trig_cfg)
        self._trig.reset(self._stream_start_dt)
        self._update_trigger_state_label()

        # recording file open (optional)
        self._open_recording()

        # Read IMU settings from UI
        imu_enabled = bool(getattr(self, "chk_imu_enable", None) and self.chk_imu_enable.isChecked())
        imu_odr_setting = 0 if not imu_enabled else int(self.cmb_imu_odr.currentIndex())  # index maps 0..7
        imu_acc_range = 0 if not imu_enabled else int(self.sp_imu_acc.value())
        imu_gyr_range = 0 if not imu_enabled else int(self.sp_imu_gyr.value())
        imu_mag_range = 0 if not imu_enabled else int(self.sp_imu_mag.value())

        self.worker = SerialWorker(
            port=port,
            baud=baud,
            rf_channel=rf_channel,
            rf_speed_code=rf_speed_code,
            max_sensors_code=max_sensors_code,
            gain=gain,
            sign=sign,
            ad_odr_setting=ad_odr_setting,
            node_select=node_select,
            # new IMU args
            imu_odr_setting=imu_odr_setting,
            imu_acc_range=imu_acc_range,
            imu_gyr_range=imu_gyr_range,
            imu_mag_range=imu_mag_range,
            prefer_newfw=prefer_newfw,
            parent=self,
        )
        self.worker.samples.connect(self.on_samples)
        self.worker.segment_break.connect(self.on_segment_break)
        self.worker.status.connect(self.on_status)
        self.worker.status_snapshot.connect(self.on_device_status)
        self.worker.finished.connect(self.on_finished)
        self.worker.start()

        self.btn_start.setEnabled(False)
        if hasattr(self, 'cmb_odr'):
            self.cmb_odr.setEnabled(False)
        self.btn_stop.setEnabled(True)

        # lock UI (settings can be changed only before start)
        for w in (self.cb_ch1, self.cb_ch2):
            w.setEnabled(False)
        self.timer.start()

    @QtCore.Slot()
    def stop_worker(self):
        if self.worker is None:
            return
        # Request stop without blocking the UI thread.
        self.worker.stop()
        self.btn_stop.setEnabled(False)
        # Keep options locked until the worker fully stops (on_finished),
        # to avoid "first click ignored" while shutdown is still in progress.
        self.on_status("Stopping…")

    # ---------- worker signals ----------
    @QtCore.Slot(object, object, object)
    def on_samples(self, t_s: object, y1: object, y2: object):
        t_s = np.asarray(t_s, dtype=np.float64)
        y1 = np.asarray(y1, dtype=np.float64)
        y2 = np.asarray(y2, dtype=np.float64) if y2 is not None else None
        # spike filter
        y1 = self._apply_spike_filter(y1, ch=1)
        if y2 is not None:
            y2 = self._apply_spike_filter(y2, ch=2)

        # offset
        if self._offset_enabled:
            off1 = float(self._offset1_mvv)
            off2 = float(self._offset2_mvv)
            if np.isfinite(off1) and off1 != 0.0:
                y1 = y1 - off1
            if y2 is not None and np.isfinite(off2) and off2 != 0.0:
                y2 = y2 - off2

        # append to plot buffers
        if y2 is None or y2.size == 0:
            y2_iter = [float('nan')] * len(y1)
        else:
            y2_iter = y2

        for ti, v1, v2 in zip(t_s, y1, y2_iter):
            self.t.append(float(ti))
            self.y1.append(float(v1))
            self.y2.append(float(v2))
        # trim buffers to window
        self._trim_buffers()

        # display last value
        if len(y1) > 0 and np.isfinite(y1[-1]):
            self.lbl_value.setText(f"mV/V: {float(y1[-1]):.6f}")
        elif y2 is not None and len(y2) > 0 and np.isfinite(y2[-1]):
            self.lbl_value.setText(f"mV/V: {float(y2[-1]):.6f}")
        else:
            self.lbl_value.setText("mV/V: —")

        # write to CSV
        self._write_samples_to_csv(t_s, y1, y2)

        # auto trigger recorder
        if self._trig is not None:
            try:
                y1_tr = y1 if self.cb_ch1.isChecked() else None
                y2_tr = y2 if (y2 is not None and self.cb_ch2.isChecked()) else None
                self._trig.feed_block(t_s, y1_tr, y2_tr)
            except Exception as e:
                self._log(f"Trigger recorder error: {e!r}")
            self._update_trigger_state_label()

    @QtCore.Slot()
    def on_segment_break(self):
        # NaN point to break the polyline
        self.t.append(float("nan"))
        self.y1.append(float("nan")); self.y2.append(float("nan"))
        self._trim_buffers()

    @QtCore.Slot(str)
    def on_status(self, s: str):
        # sanitize control chars and cap length to avoid UI stretching
        if s is None:
            s = ""
        s_clean = "".join(ch for ch in str(s) if (ch >= " " or ch in "\t"))
        s_clean = s_clean.replace("\t", " ")
        if len(s_clean) > 240:
            s_clean = s_clean[:240] + "…"
        fm = self.lbl_status.fontMetrics()
        s_elided = fm.elidedText(s_clean, QtCore.Qt.TextElideMode.ElideRight, self.lbl_status.maximumWidth())
        self.lbl_status.setText(s_elided)
        self._log(s_clean)


    @QtCore.Slot(object)
    def on_device_status(self, snap: object):
        try:
            d = snap.as_dict() if hasattr(snap, "as_dict") else dict(snap)
        except Exception:
            d = {}

        temp = d.get("temperature_c", None)
        rssi_dbm = d.get("rssi_dbm", None)
        bars = d.get("rssi_bars", None)
        v = d.get("voltage_v", None)
        pct = d.get("battery_percent", None)

        temp_s = "—" if temp is None else f"{float(temp):.1f} °C"
        if rssi_dbm is None:
            rssi_s = "—"
        else:
            if bars is None:
                rssi_s = f"{int(rssi_dbm)} dBm"
            else:
                rssi_s = f"{int(rssi_dbm)} dBm ({int(bars)}/4)"

        if v is None:
            batt_s = "—"
        else:
            if pct is None:
                batt_s = f"{float(v):.1f} V"
            else:
                batt_s = f"{float(v):.1f} V ({int(pct)}%)"

        s = f"Temp: {temp_s} | RSSI: {rssi_s} | Batt: {batt_s}"
        fm = self.lbl_dev_status.fontMetrics()
        s_elided = fm.elidedText(s, QtCore.Qt.TextElideMode.ElideRight, self.lbl_dev_status.width())
        self.lbl_dev_status.setText(s_elided)

    @QtCore.Slot()
    def on_finished(self):
        self.timer.stop()
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)

        # unlock UI
        for w in (self.cb_ch1, self.cb_ch2):
            w.setEnabled(True)
        self.settings_dialog.setEnabled(True)
        if hasattr(self, 'cmb_odr'):
            self.cmb_odr.setEnabled(True)
        self.worker = None
        self._close_recording()

        # stop trigger recorder
        if self._trig is not None:
            self._trig.reset(None)
        self._stream_start_dt = None
        self._update_trigger_state_label()

    def _trim_buffers(self):
        """
        Keeps last window_seconds worth of finite samples.
        NaNs are kept only if they sit inside the kept range.
        """
        if not self.t:
            return

        # find last finite time as reference
        last_t = None
        for i in range(len(self.t) - 1, -1, -1):
            if np.isfinite(self.t[i]):
                last_t = self.t[i]
                break
        if last_t is None:
            # all NaNs -> clear
            self.t.clear()
            self.y1.clear(); self.y2.clear()
            return

        t_min = float(last_t) - float(self.window_seconds)

        # pop left while time is older than window OR NaN at left
        while self.t:
            t0 = self.t[0]
            if not np.isfinite(t0):
                self.t.popleft(); self.y1.popleft(); self.y2.popleft()
                continue
            if t0 < t_min:
                self.t.popleft(); self.y1.popleft(); self.y2.popleft()
                continue
            break

    # ---------- plot update ----------
    def redraw(self):
        if not self.t:
            return
        t_list = list(self.t)
        if self.cb_ch1.isChecked():
            self.curve1.setData(t_list, list(self.y1), connect="finite")
        else:
            self.curve1.setData([], [])
        if self.cb_ch2.isChecked():
            self.curve2.setData(t_list, list(self.y2), connect="finite")
        else:
            self.curve2.setData([], [])

        # set x range to last window
        last_t = None
        for i in range(len(self.t) - 1, -1, -1):
            if np.isfinite(self.t[i]):
                last_t = self.t[i]
                break
        if last_t is None:
            return
        self.plot.setXRange(float(last_t) - float(self.window_seconds), float(last_t), padding=0.01)

    # ---------- theme hook ----------
    def apply_theme(self, dark: bool) -> None:
        apply_live_plot_theme(self, dark=dark)