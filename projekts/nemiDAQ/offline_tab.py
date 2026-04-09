from __future__ import annotations

import numpy as np
from PySide6 import QtCore, QtWidgets
import pyqtgraph as pg

from offline_decoder import (
    N_SAMPLES,
    DecodedPacket,
    best_packet_window,
    best_packet_window_2ch,
    build_time_series_segmented_raw_2ch,
    corr_z,
    decode_bin_full,
    export_full_bin_csv,
    export_like_reference_csv,
    guess_start_datetime,
    hampel_filter,
    load_csv,
    make_plot_arrays_with_nans,
)
from nemidaq_theme import set_plot_theme


class OfflineTab(QtWidgets.QWidget):
    """Offline BIN decoder + CSV compare tab (2 channels + CSV transforms)."""

    def __init__(self, parent=None):
        super().__init__(parent)

        # paths
        self.bin_path: str | None = None
        self.csv_path: str | None = None

        # decoded packets
        self.packets: list[DecodedPacket] | None = None
        self.decode_info: dict | None = None

        # BIN RAW (no NaN)
        self.t_bin_raw: np.ndarray | None = None
        self.y1_bin_raw0: np.ndarray | None = None  # before filter
        self.y2_bin_raw0: np.ndarray | None = None
        self.y1_bin_raw: np.ndarray | None = None   # after filter
        self.y2_bin_raw: np.ndarray | None = None

        # BIN PLOT (with NaN gaps)
        self.t_bin_plot: np.ndarray | None = None
        self.y1_bin_plot: np.ndarray | None = None
        self.y2_bin_plot: np.ndarray | None = None

        self.reset_packet_idx: list[int] | None = None
        self.peak_mask1: np.ndarray | None = None
        self.peak_mask2: np.ndarray | None = None

        # CSV
        self.df_csv = None
        self.t_csv: np.ndarray | None = None
        self.y1_csv_orig: np.ndarray | None = None
        self.y2_csv_orig: np.ndarray | None = None
        self.y1_csv: np.ndarray | None = None  # transformed
        self.y2_csv: np.ndarray | None = None

        # compare (active channel)
        self.match: dict | None = None
        self.y_bin_cmp: np.ndarray | None = None
        self.err_cmp: np.ndarray | None = None

        self._auto_match_guard = False
        self._x_csv_on_bin: np.ndarray | None = None

        self._build_ui()
        self._build_plot()
        self._build_crosshair()

        self._log("Ready. Open BIN → Decode. Optionally open CSV and run Auto-match.")

    # ---------- UI ----------
    def _build_ui(self):
        layout = QtWidgets.QHBoxLayout(self)

        # left panel (scrollable)
        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QtWidgets.QFrame.StyledPanel)
        scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        scroll.setMaximumWidth(480)
        layout.addWidget(scroll)

        left = QtWidgets.QWidget()
        scroll.setWidget(left)

        form = QtWidgets.QVBoxLayout(left)

        # Files
        btn_bin = QtWidgets.QPushButton("Open BIN…")
        btn_bin.clicked.connect(self.open_bin)
        form.addWidget(btn_bin)

        btn_csv = QtWidgets.QPushButton("Open CSV…")
        btn_csv.clicked.connect(self.open_csv)
        form.addWidget(btn_csv)

        # CSV transforms
        g_csv = QtWidgets.QGroupBox("CSV transforms")
        gl_csv = QtWidgets.QVBoxLayout(g_csv)
        self.cb_invert_csv = QtWidgets.QCheckBox("Invert CSV (mirror Ch 1 + Ch 2)")
        self.cb_invert_csv.setChecked(False)
        self.cb_invert_csv.stateChanged.connect(self.apply_csv_transforms)

        self.cb_zero_csv = QtWidgets.QCheckBox("Zero offset (mean of first 1 s)")
        self.cb_zero_csv.setChecked(False)
        self.cb_zero_csv.stateChanged.connect(self.apply_csv_transforms)

        gl_csv.addWidget(self.cb_invert_csv)
        gl_csv.addWidget(self.cb_zero_csv)

        btn_save_csv = QtWidgets.QPushButton("Save transformed CSV…")
        btn_save_csv.clicked.connect(self.save_transformed_csv)
        gl_csv.addWidget(btn_save_csv)

        form.addWidget(g_csv)

        # Channel selection
        form.addWidget(QtWidgets.QLabel("Active channel:"))
        self.cmb_channel = QtWidgets.QComboBox()
        self.cmb_channel.addItems(["Ch 1", "Ch 2", "Both"])
        self.cmb_channel.currentIndexChanged.connect(self.update_compare_series)
        self.cmb_channel.currentIndexChanged.connect(self.redraw)
        form.addWidget(self.cmb_channel)

        form.addSpacing(10)

        # Decode actions
        row = QtWidgets.QHBoxLayout()
        btn_decode = QtWidgets.QPushButton("Decode BIN")
        btn_decode.clicked.connect(self.decode_bin)
        row.addWidget(btn_decode)

        self.sp_gain = QtWidgets.QDoubleSpinBox()
        self.sp_gain.setRange(1.0, 1024.0)
        self.sp_gain.setDecimals(1)
        self.sp_gain.setSingleStep(1.0)
        self.sp_gain.setValue(128.0)
        self.sp_gain.setToolTip("Gain used for mV/V scaling.")
        row.addWidget(QtWidgets.QLabel("Gain:"))
        row.addWidget(self.sp_gain)
        form.addLayout(row)

        btn_match = QtWidgets.QPushButton("Auto-match BIN ↔ CSV")
        btn_match.clicked.connect(self.auto_match)
        form.addWidget(btn_match)

        form.addSpacing(10)

        # Time segmentation group
        g_time = QtWidgets.QGroupBox("Time / reset segmentation")
        glt = QtWidgets.QFormLayout(g_time)

        self.sp_anchor = QtWidgets.QDoubleSpinBox()
        self.sp_anchor.setRange(0.0, 1.0)
        self.sp_anchor.setSingleStep(0.5)
        self.sp_anchor.setValue(0.0)
        self.sp_anchor.valueChanged.connect(self.rebuild_bin_time_axis)

        self.sp_gap = QtWidgets.QDoubleSpinBox()
        self.sp_gap.setRange(0.0, 10.0)
        self.sp_gap.setSingleStep(0.1)
        self.sp_gap.setValue(0.5)
        self.sp_gap.valueChanged.connect(self.rebuild_bin_time_axis)

        self.sp_reset_abs = QtWidgets.QDoubleSpinBox()
        self.sp_reset_abs.setRange(0.1, 60.0)
        self.sp_reset_abs.setSingleStep(0.5)
        self.sp_reset_abs.setValue(5.0)
        self.sp_reset_abs.valueChanged.connect(self.rebuild_bin_time_axis)

        self.sp_reset_factor = QtWidgets.QDoubleSpinBox()
        self.sp_reset_factor.setRange(2.0, 200.0)
        self.sp_reset_factor.setSingleStep(1.0)
        self.sp_reset_factor.setValue(20.0)
        self.sp_reset_factor.valueChanged.connect(self.rebuild_bin_time_axis)

        glt.addRow("anchor:", self.sp_anchor)
        glt.addRow("gap between segments (s):", self.sp_gap)
        glt.addRow("reset abs thr (s):", self.sp_reset_abs)
        glt.addRow("reset factor:", self.sp_reset_factor)
        form.addWidget(g_time)

        # Filter group
        g_filter = QtWidgets.QGroupBox("Spike filter (Hampel)")
        gl = QtWidgets.QFormLayout(g_filter)

        self.cb_filter = QtWidgets.QCheckBox("Enabled")
        self.cb_filter.setChecked(True)
        self.cb_filter.stateChanged.connect(self.apply_processing)

        self.sp_window = QtWidgets.QSpinBox()
        self.sp_window.setRange(3, 201)
        self.sp_window.setSingleStep(2)
        self.sp_window.setValue(11)
        self.sp_window.valueChanged.connect(self.apply_processing)

        self.sp_sigma = QtWidgets.QDoubleSpinBox()
        self.sp_sigma.setRange(1.0, 10.0)
        self.sp_sigma.setSingleStep(0.5)
        self.sp_sigma.setValue(3.0)
        self.sp_sigma.valueChanged.connect(self.apply_processing)

        gl.addRow(self.cb_filter)
        gl.addRow("Window:", self.sp_window)
        gl.addRow("Sigma:", self.sp_sigma)
        form.addWidget(g_filter)

        # Compare group
        g_cmp = QtWidgets.QGroupBox("Compare")
        gl2 = QtWidgets.QFormLayout(g_cmp)

        self.cb_use_match = QtWidgets.QCheckBox("Use match window (packet-based)")
        self.cb_use_match.setChecked(True)
        self.cb_use_match.stateChanged.connect(self.update_compare_series)

        self.sp_startpkt = QtWidgets.QSpinBox()
        self.sp_startpkt.setRange(0, 10_000_000)
        self.sp_startpkt.setValue(0)
        self.sp_startpkt.valueChanged.connect(self.manual_startpkt_changed)

        self.cb_sign = QtWidgets.QComboBox()
        self.cb_sign.addItems(["+1", "-1"])
        self.cb_sign.currentIndexChanged.connect(self.manual_sign_changed)

        gl2.addRow(self.cb_use_match)
        gl2.addRow("start_pkt:", self.sp_startpkt)
        gl2.addRow("sign:", self.cb_sign)
        form.addWidget(g_cmp)

        # View mode
        self.cmb_view = QtWidgets.QComboBox()
        self.cmb_view.addItems(["BIN (full)", "Overlay (mV/V)", "Overlay (z-score)", "Error (CSV-BIN)"])
        self.cmb_view.currentIndexChanged.connect(self.redraw)
        form.addWidget(QtWidgets.QLabel("Plot mode:"))
        form.addWidget(self.cmb_view)

        form.addSpacing(10)

        # Save
        btn_save_full = QtWidgets.QPushButton("Export full BIN → CSV…")
        btn_save_full.clicked.connect(self.save_full_export)
        form.addWidget(btn_save_full)

        btn_save_like = QtWidgets.QPushButton("Export like reference CSV…")
        btn_save_like.clicked.connect(self.save_like_reference)
        form.addWidget(btn_save_like)

        form.addStretch(1)

        # Log box
        self.log = QtWidgets.QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumBlockCount(3000)
        form.addWidget(self.log)

        # plot container
        self.plot_container = QtWidgets.QFrame()
        self.plot_container.setFrameShape(QtWidgets.QFrame.StyledPanel)
        layout.addWidget(self.plot_container, 1)

    def _build_plot(self):
        pl = QtWidgets.QVBoxLayout(self.plot_container)

        self.plot = pg.PlotWidget()
        self.plot.showGrid(x=True, y=True, alpha=0.20)
        self.plot.addLegend()
        pl.addWidget(self.plot)

        set_plot_theme(self.plot, dark=True)

        # pens
        self.pen_bin1 = pg.mkPen((0, 220, 0), width=2)                # green
        self.pen_bin2 = pg.mkPen((0, 140, 255), width=2)              # blue
        self.pen_csv1 = pg.mkPen((220, 0, 0), width=3, style=QtCore.Qt.DashLine)      # red dashed
        self.pen_csv2 = pg.mkPen((255, 180, 0), width=3, style=QtCore.Qt.DashLine)    # orange dashed
        self.pen_err1 = pg.mkPen((180, 180, 180), width=2)            # gray
        self.pen_err2 = pg.mkPen((160, 80, 255), width=2)             # purple

        self.curve_bin1 = self.plot.plot([], [], name="BIN Ch1", pen=self.pen_bin1)
        self.curve_bin2 = self.plot.plot([], [], name="BIN Ch2", pen=self.pen_bin2)
        self.curve_csv1 = self.plot.plot([], [], name="CSV Ch1", pen=self.pen_csv1)
        self.curve_csv2 = self.plot.plot([], [], name="CSV Ch2", pen=self.pen_csv2)
        self.curve_err1 = self.plot.plot([], [], name="Error Ch1", pen=self.pen_err1)
        self.curve_err2 = self.plot.plot([], [], name="Error Ch2", pen=self.pen_err2)

        # ROI region
        self.region = pg.LinearRegionItem(values=[0, 1], movable=True)
        self.region.setZValue(10)
        self.region.hide()
        self.plot.addItem(self.region)
        self.region.sigRegionChanged.connect(self.update_region_stats)

        self.lbl_stats = QtWidgets.QLabel(" ")
        pl.addWidget(self.lbl_stats)

    def _build_crosshair(self):
        self.vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen((180, 180, 180), width=1))
        self.hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen((180, 180, 180), width=1))
        self.plot.addItem(self.vline, ignoreBounds=True)
        self.plot.addItem(self.hline, ignoreBounds=True)
        self.proxy = pg.SignalProxy(self.plot.scene().sigMouseMoved, rateLimit=60, slot=self.on_mouse_moved)

    # ---------- logging ----------
    def _log(self, s: str):
        self.log.appendPlainText(s)

    # ---------- dialogs ----------
    def open_bin(self):
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Select BIN", "", "BIN (*.bin);;All files (*.*)")
        if p:
            self.bin_path = p
            self._log(f"BIN: {p}")

    def open_csv(self):
        p, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Select CSV", "", "CSV (*.csv);;All files (*.*)")
        if p:
            self.csv_path = p
            self._log(f"CSV: {p}")
            self.load_csv_now()

    # ---------- CSV transforms ----------
    @staticmethod
    def _mirror_minmax(y: np.ndarray) -> np.ndarray:
        y = np.asarray(y, dtype=np.float64)
        if len(y) == 0:
            return y
        mn = float(np.nanmin(y))
        mx = float(np.nanmax(y))
        return (mn + mx) - y

    @staticmethod
    def _zero_first_second(t: np.ndarray, y: np.ndarray) -> np.ndarray:
        t = np.asarray(t, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64)
        if len(y) == 0:
            return y
        m = t < 1.0
        if not np.any(m):
            # fallback: first 66 samples
            m = np.zeros(len(t), dtype=bool)
            m[: min(len(t), N_SAMPLES)] = True
        base = float(np.mean(y[m]))
        return y - base

    def apply_csv_transforms(self):
        if self.t_csv is None or self.y1_csv_orig is None or self.y2_csv_orig is None:
            return

        y1 = self.y1_csv_orig.copy()
        y2 = self.y2_csv_orig.copy()

        if self.cb_invert_csv.isChecked():
            y1 = self._mirror_minmax(y1)
            y2 = self._mirror_minmax(y2)

        if self.cb_zero_csv.isChecked():
            y1 = self._zero_first_second(self.t_csv, y1)
            y2 = self._zero_first_second(self.t_csv, y2)

        self.y1_csv = y1
        self.y2_csv = y2

        # match depends on CSV values
        self.match = None
        self._x_csv_on_bin = None

        self.ensure_match_silent()
        self.update_compare_series()
        self.redraw()

    def save_transformed_csv(self):
        if self.df_csv is None or self.t_csv is None or self.y1_csv is None or self.y2_csv is None:
            QtWidgets.QMessageBox.critical(self, "Error", "Load CSV first.")
            return

        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Save transformed CSV", "", "CSV (*.csv)")
        if not path:
            return

        try:
            df_out = self.df_csv.copy()
            if "Ch 1" in df_out.columns:
                df_out["Ch 1"] = np.asarray(self.y1_csv, dtype=np.float64)
            if "Ch 2" in df_out.columns:
                df_out["Ch 2"] = np.asarray(self.y2_csv, dtype=np.float64)
            else:
                # keep format compatible with exporter
                df_out["Ch 2"] = np.asarray(self.y2_csv, dtype=np.float64)

            df_out.to_csv(path, sep=";", index=False)
            self._log(f"Saved transformed CSV: {path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Save error", str(e))

    # ---------- core ----------
    def decode_bin(self):
        if not self.bin_path:
            QtWidgets.QMessageBox.critical(self, "Error", "BIN file not selected.")
            return
        try:
            gain = float(self.sp_gain.value())
            packets, info = decode_bin_full(self.bin_path, gain=gain)
            self.packets = packets
            self.decode_info = info

            # reset match (new BIN)
            self.match = None
            self.y_bin_cmp = None
            self.err_cmp = None
            self._x_csv_on_bin = None

            self._log("\n=== BIN decoded ===")
            self._log(f"outer_packets: {info['outer_packets']}")
            self._log(f"decoded_packets: {info['decoded_packets']}  bad: {info['bad_packets']}  bad_off:{info['bad_subframe_off']}")
            self._log(f"subframe_off(mode): {info['subframe_off']}")
            self._log(f"ptype_counts: {info['ptype_counts']}")
            self._log(f"dt_pkt_median_us: {info['dt_pkt_median_us']:.3f}")
            self._log(f"dt_sample_median_s: {info['dt_sample_median_s']:.9f}")
            self._log(f"gain={info['gain']:.1f}  scale={info['scale_mvv']:.12g} mV/V per count")
            self._log(f"ch2_decoded_packets: {info.get('ch2_decoded_packets', 0)}")

            self.rebuild_bin_time_axis()  # includes apply_processing()

            # silent match if possible
            self.ensure_match_silent()

            self.update_compare_series()
            self.redraw()

        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Decode error", str(e))

    def rebuild_bin_time_axis(self):
        if not self.packets:
            return

        anchor = float(self.sp_anchor.value())
        gap_s = float(self.sp_gap.value())
        reset_abs_s = float(self.sp_reset_abs.value())
        reset_factor = float(self.sp_reset_factor.value())

        t_raw, y1_raw0, y2_raw0, reset_pkt_idx, seg = build_time_series_segmented_raw_2ch(
            self.packets,
            anchor=anchor,
            gap_s=gap_s,
            dt_abs_reset_s=reset_abs_s,
            dt_reset_factor=reset_factor,
        )

        self.t_bin_raw = t_raw
        self.y1_bin_raw0 = y1_raw0
        self.y2_bin_raw0 = y2_raw0
        self.reset_packet_idx = reset_pkt_idx

        dur = float(np.nanmax(t_raw)) if len(t_raw) else 0.0
        self._log(
            f"Time segmented: segments={seg['segments']} resets={len(seg['resets'])} dt_med_us={seg['dt_med_us']:.1f} reset_thr_us={seg['reset_thr_us']:.1f}"
        )
        if seg["resets"]:
            self._log("First resets (pkt_idx, dt_us): " + ", ".join([f"{i}:{dt:.0f}" for i, dt in seg["resets"][:6]]))
        self._log(f"BIN samples(raw): {len(y1_raw0)} duration(segmented): {dur:.3f} s")

        self.apply_processing()

    def apply_processing(self):
        if self.t_bin_raw is None or self.y1_bin_raw0 is None or self.y2_bin_raw0 is None:
            return

        y1 = self.y1_bin_raw0.copy()
        y2 = self.y2_bin_raw0.copy()

        if self.cb_filter.isChecked():
            w = int(self.sp_window.value())
            s = float(self.sp_sigma.value())
            y1_f, mask1 = hampel_filter(y1, window=w, n_sigma=s)
            y2_f, mask2 = hampel_filter(y2, window=w, n_sigma=s)
            self.y1_bin_raw = y1_f
            self.y2_bin_raw = y2_f
            self.peak_mask1 = mask1
            self.peak_mask2 = mask2
            self._log(f"Hampel: window={w}, sigma={s} replaced_ch1={int(mask1.sum())} replaced_ch2={int(mask2.sum())}")
        else:
            self.y1_bin_raw = y1
            self.y2_bin_raw = y2
            self.peak_mask1 = np.zeros(len(y1), dtype=bool)
            self.peak_mask2 = np.zeros(len(y2), dtype=bool)

        # plot arrays with NaN breaks
        if self.reset_packet_idx is None:
            self.t_bin_plot = self.t_bin_raw
            self.y1_bin_plot = self.y1_bin_raw
            self.y2_bin_plot = self.y2_bin_raw
        else:
            self.t_bin_plot, self.y1_bin_plot = make_plot_arrays_with_nans(self.t_bin_raw, self.y1_bin_raw, self.reset_packet_idx)
            _, self.y2_bin_plot = make_plot_arrays_with_nans(self.t_bin_raw, self.y2_bin_raw, self.reset_packet_idx)

        self.update_compare_series()
        self.redraw()

    def load_csv_now(self):
        if not self.csv_path:
            return
        try:
            df, t, y1, y2 = load_csv(self.csv_path)
            self.df_csv = df
            self.t_csv = t
            self.y1_csv_orig = y1
            self.y2_csv_orig = y2

            self._log("\n=== CSV loaded ===")
            self._log(f"rows: {len(df)} dt_med={float(np.median(np.diff(t))):.9f} s")
            if "Ch 2" not in df.columns:
                self._log("Note: CSV has no 'Ch 2' column; using zeros.")

            self.apply_csv_transforms()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "CSV error", str(e))

    # ---------- match ----------
    def ensure_match_silent(self) -> bool:
        if self._auto_match_guard:
            return False
        if not self.cb_use_match.isChecked():
            return False
        if self.match and self.match.get("ok"):
            return True
        if not self.packets or self.y1_csv is None or self.y2_csv is None:
            return False
        if len(self.y1_csv) % N_SAMPLES != 0:
            return False

        self._auto_match_guard = True
        try:
            # Prefer 2ch matching when possible
            m = best_packet_window_2ch(self.packets, self.y1_csv, self.y2_csv)
            if not m.get("ok"):
                m = best_packet_window(self.packets, self.y1_csv)
            if not m.get("ok"):
                return False

            self.match = m

            # sync UI without recursion
            self.sp_startpkt.blockSignals(True)
            self.cb_sign.blockSignals(True)
            try:
                self.sp_startpkt.setValue(int(m["start_pkt"]))
                self.cb_sign.setCurrentIndex(0 if float(m["sign"]) > 0 else 1)
            finally:
                self.sp_startpkt.blockSignals(False)
                self.cb_sign.blockSignals(False)

            self._log(f"[auto-match] start_pkt={m['start_pkt']} W={m['W']} sign={m['sign']:+.0f} rmse={m['rmse']:.3e}")
            return True
        finally:
            self._auto_match_guard = False

    def auto_match(self):
        if not self.packets or self.y1_bin_raw is None:
            QtWidgets.QMessageBox.critical(self, "Error", "Decode BIN first.")
            return
        if self.y1_csv is None or self.y2_csv is None:
            QtWidgets.QMessageBox.critical(self, "Error", "Load CSV first.")
            return

        m = best_packet_window_2ch(self.packets, self.y1_csv, self.y2_csv)
        if not m.get("ok"):
            m = best_packet_window(self.packets, self.y1_csv)

        if not m.get("ok"):
            self._log(f"Auto-match failed: {m.get('reason')}")
            QtWidgets.QMessageBox.warning(self, "Match", f"Auto-match failed: {m.get('reason')}")
            return

        self.match = m
        self.sp_startpkt.setValue(int(m["start_pkt"]))
        self.cb_sign.setCurrentIndex(0 if float(m["sign"]) > 0 else 1)

        self._log("\n=== MATCH ===")
        self._log(f"start_pkt={m['start_pkt']} W={m['W']} sign={m['sign']:+.0f} rmse={m['rmse']:.9e}")

        self.update_compare_series()
        self.cmb_view.setCurrentText("Overlay (mV/V)")
        self.redraw()

    def manual_startpkt_changed(self):
        if self.match and self.match.get("ok"):
            self.match["start_pkt"] = int(self.sp_startpkt.value())
        self.update_compare_series()
        self.redraw()

    def manual_sign_changed(self):
        if self.match and self.match.get("ok"):
            self.match["sign"] = +1.0 if self.cb_sign.currentText() == "+1" else -1.0
        self.update_compare_series()
        self.redraw()

    # ---------- compare ----------
    def _active_csv(self) -> tuple[np.ndarray | None, np.ndarray | None]:
        if self.y1_csv is None or self.y2_csv is None:
            return None, None
        mode = self.cmb_channel.currentText()
        if mode == "Ch 1":
            return self.y1_csv, None
        if mode == "Ch 2":
            return self.y2_csv, None
        return self.y1_csv, self.y2_csv

    def update_compare_series(self):
        if self.t_csv is None or self.t_bin_raw is None or self.y1_bin_raw is None or self.y2_bin_raw is None:
            self.y_bin_cmp = None
            self.err_cmp = None
            self.region.hide()
            self.lbl_stats.setText(" ")
            return

        y1_csv, y2_csv = self._active_csv()
        if y1_csv is None and y2_csv is None:
            self.y_bin_cmp = None
            self.err_cmp = None
            self.region.hide()
            self.lbl_stats.setText(" ")
            return

        # active channel selection for compare/error
        if y2_csv is None:
            active = 1 if self.cmb_channel.currentText() == "Ch 1" else 2
        else:
            # Both: compare on Ch1 by default for the single error series (stats still shown in ROI for both)
            active = 1

        ycsv = y1_csv if active == 1 else y2_csv if y2_csv is not None else self.y2_csv

        use_match = self.cb_use_match.isChecked()
        y_on_csv = None

        if use_match and self.match and self.match.get("ok") and (len(ycsv) % N_SAMPLES == 0):
            start_pkt = int(self.match["start_pkt"])
            W = int(self.match["W"])
            sign = float(self.match["sign"])

            start_s = start_pkt * N_SAMPLES
            end_s = (start_pkt + W) * N_SAMPLES

            y_bin_src = self.y1_bin_raw if active == 1 else self.y2_bin_raw

            if end_s <= len(y_bin_src) and start_s >= 0:
                t_seg = self.t_bin_raw[start_s:end_s]
                y_seg = y_bin_src[start_s:end_s] * sign
                t_seg0 = t_seg - t_seg[0]
                y_on_csv = np.interp(self.t_csv, t_seg0, y_seg)
            else:
                self._log("Warning: start_pkt out of BIN range. Falling back to time-interp.")
                y_on_csv = None

        if y_on_csv is None:
            t0 = self.t_bin_raw - self.t_bin_raw[0]
            y_bin_src = self.y1_bin_raw if active == 1 else self.y2_bin_raw
            y_on_csv = np.interp(self.t_csv, t0, y_bin_src)

        self.y_bin_cmp = y_on_csv
        self.err_cmp = ycsv - y_on_csv

        self.region.show()
        self.region.setRegion([float(self.t_csv[0]), float(self.t_csv[-1])])
        self.update_region_stats()

        r = corr_z(ycsv, y_on_csv)
        rmse = float(np.sqrt(np.mean((ycsv - y_on_csv) ** 2)))
        self._log(f"Compare stats ({'Ch1' if active==1 else 'Ch2'}): corr(z)={r:+.4f} rmse={rmse:.9e}")

    # ---------- plotting ----------
    def _clear_curves(self):
        for c in [self.curve_bin1, self.curve_bin2, self.curve_csv1, self.curve_csv2, self.curve_err1, self.curve_err2]:
            c.setData([], [])

    def redraw(self):
        mode = self.cmb_view.currentText()
        chan = self.cmb_channel.currentText()

        self._clear_curves()

        # sign for display (so BIN matches CSV)
        sign_ui = +1.0 if self.cb_sign.currentText() == "+1" else -1.0

        if mode == "BIN (full)":
            if self.t_bin_plot is None or self.y1_bin_plot is None or self.y2_bin_plot is None:
                return

            self.plot.setLabel("bottom", "Time (s)")
            self.plot.setLabel("left", "mV/V")
            self.plot.setTitle("BIN: full decoded (with reset gaps)")

            if chan in ("Ch 1", "Both"):
                self.curve_bin1.setData(self.t_bin_plot, self.y1_bin_plot * sign_ui, connect="finite")
            if chan in ("Ch 2", "Both"):
                self.curve_bin2.setData(self.t_bin_plot, self.y2_bin_plot * sign_ui, connect="finite")

            self._x_csv_on_bin = None

            # CSV over full BIN with time-shift
            if self.t_csv is not None and self.y1_csv is not None and self.y2_csv is not None and self.t_bin_raw is not None:
                self.ensure_match_silent()

                start_pkt = int(self.sp_startpkt.value()) if self.cb_use_match.isChecked() else 0
                start_s = start_pkt * N_SAMPLES
                t0 = float(self.t_bin_raw[start_s]) if 0 <= start_s < len(self.t_bin_raw) else float(self.t_bin_raw[0])

                x_csv_on_bin = t0 + self.t_csv
                self._x_csv_on_bin = x_csv_on_bin

                if chan in ("Ch 1", "Both"):
                    self.curve_csv1.setData(x_csv_on_bin, self.y1_csv, connect="finite")
                if chan in ("Ch 2", "Both"):
                    self.curve_csv2.setData(x_csv_on_bin, self.y2_csv, connect="finite")

                self.region.show()
                self.region.setRegion([float(x_csv_on_bin[0]), float(x_csv_on_bin[-1])])
                self.update_region_stats()
            else:
                self.region.hide()

            self.plot.enableAutoRange()
            return

        if mode in ("Overlay (mV/V)", "Overlay (z-score)"):
            if self.t_csv is None or self.y1_csv is None or self.y2_csv is None:
                return

            x = self.t_csv

            def z(v: np.ndarray) -> np.ndarray:
                return (v - v.mean()) / (v.std() + 1e-12)

            if mode == "Overlay (z-score)":
                self.plot.setLabel("left", "z-score")
                y1c = z(self.y1_csv)
                y2c = z(self.y2_csv)
            else:
                self.plot.setLabel("left", "mV/V")
                y1c = self.y1_csv
                y2c = self.y2_csv

            # BIN on CSV axis
            if self.y_bin_cmp is None:
                # CSV only
                if chan in ("Ch 1", "Both"):
                    self.curve_csv1.setData(x, y1c, connect="finite")
                if chan in ("Ch 2", "Both"):
                    self.curve_csv2.setData(x, y2c, connect="finite")
                self.plot.setTitle("CSV only (no BIN)")
                self.plot.setLabel("bottom", "Timecounter (s)")
                self.plot.enableAutoRange()
                return

            # Compare series uses active channel; for Both we plot BIN for both channels via full interpolation
            t0 = self.t_bin_raw - self.t_bin_raw[0] if self.t_bin_raw is not None else None
            if t0 is None or self.y1_bin_raw is None or self.y2_bin_raw is None:
                return

            y1b = np.interp(x, t0, self.y1_bin_raw * sign_ui)
            y2b = np.interp(x, t0, self.y2_bin_raw * sign_ui)
            if mode == "Overlay (z-score)":
                y1b = z(y1b)
                y2b = z(y2b)

            if chan in ("Ch 1", "Both"):
                self.curve_csv1.setData(x, y1c, connect="finite")
                self.curve_bin1.setData(x, y1b, connect="finite")
            if chan in ("Ch 2", "Both"):
                self.curve_csv2.setData(x, y2c, connect="finite")
                self.curve_bin2.setData(x, y2b, connect="finite")

            self.plot.setLabel("bottom", "Timecounter (s)")
            self.plot.setTitle("CSV vs BIN (overlay)")
            self.plot.enableAutoRange()
            return

        if mode == "Error (CSV-BIN)":
            if self.t_csv is None or self.err_cmp is None:
                return
            # Error is shown for the active channel only
            self.curve_err1.setData(self.t_csv, self.err_cmp, connect="finite")
            self.plot.setLabel("bottom", "Timecounter (s)")
            self.plot.setLabel("left", "Δ (mV/V)")
            self.plot.setTitle("Error = CSV - BIN (active channel)")
            self.plot.enableAutoRange()
            return

    # ---------- crosshair / stats ----------
    def on_mouse_moved(self, evt):
        pos = evt[0]
        if not self.plot.sceneBoundingRect().contains(pos):
            return
        mp = self.plot.plotItem.vb.mapSceneToView(pos)
        self.vline.setPos(float(mp.x()))
        self.hline.setPos(float(mp.y()))

    def update_region_stats(self):
        mode = self.cmb_view.currentText()
        chan = self.cmb_channel.currentText()

        # For full BIN: compare CSV-on-BIN axis to BIN via interpolation
        if mode == "BIN (full)":
            if (
                self._x_csv_on_bin is None
                or self.t_bin_raw is None
                or self.y1_bin_raw is None
                or self.y2_bin_raw is None
                or self.y1_csv is None
                or self.y2_csv is None
            ):
                self.lbl_stats.setText(" ")
                return

            lo, hi = self.region.getRegion()
            lo = float(lo)
            hi = float(hi)

            x = self._x_csv_on_bin
            m = (x >= lo) & (x <= hi)
            if int(np.sum(m)) < 10:
                self.lbl_stats.setText("ROI: too few points")
                return

            sign_ui = +1.0 if self.cb_sign.currentText() == "+1" else -1.0

            parts = []
            if chan in ("Ch 1", "Both"):
                a1 = self.y1_csv[m]
                b1 = np.interp(x[m], self.t_bin_raw, self.y1_bin_raw * sign_ui)
                r1 = corr_z(a1, b1)
                rm1 = float(np.sqrt(np.mean((a1 - b1) ** 2)))
                parts.append(f"Ch1 corr(z)={r1:+.4f} rmse={rm1:.6e}")
            if chan in ("Ch 2", "Both"):
                a2 = self.y2_csv[m]
                b2 = np.interp(x[m], self.t_bin_raw, self.y2_bin_raw * sign_ui)
                r2 = corr_z(a2, b2)
                rm2 = float(np.sqrt(np.mean((a2 - b2) ** 2)))
                parts.append(f"Ch2 corr(z)={r2:+.4f} rmse={rm2:.6e}")

            self.lbl_stats.setText(f"ROI(full) [{lo:.3f}..{hi:.3f}] " + " | ".join(parts) + f" n={int(np.sum(m))}")
            return

        # Standard: overlay/error axis = t_csv
        if self.t_csv is None or self.y_bin_cmp is None:
            self.lbl_stats.setText(" ")
            return

        lo, hi = self.region.getRegion()
        lo = float(lo)
        hi = float(hi)
        m = (self.t_csv >= lo) & (self.t_csv <= hi)
        if np.sum(m) < 10:
            self.lbl_stats.setText("ROI: too few points")
            return

        # active channel only
        if self.cmb_channel.currentText() == "Ch 2" and self.y2_csv is not None:
            a = self.y2_csv[m]
        else:
            a = self.y1_csv[m] if self.y1_csv is not None else None

        if a is None:
            self.lbl_stats.setText(" ")
            return

        b = self.y_bin_cmp[m]
        r = corr_z(a, b)
        rmse = float(np.sqrt(np.mean((a - b) ** 2)))
        self.lbl_stats.setText(f"ROI [{lo:.3f}..{hi:.3f}] corr(z)={r:+.4f} rmse={rmse:.6e} n={int(np.sum(m))}")

    # ---------- export ----------
    def save_full_export(self):
        if self.t_bin_raw is None or self.y1_bin_raw is None or self.y2_bin_raw is None:
            QtWidgets.QMessageBox.critical(self, "Error", "No decoded BIN data.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Export full BIN → CSV", "", "CSV (*.csv)")
        if not path:
            return
        start_dt = guess_start_datetime(self.df_csv, self.csv_path, self.bin_path)
        try:
            export_full_bin_csv(path, self.t_bin_raw, self.y1_bin_raw, self.y2_bin_raw, start_dt)
            self._log(f"Saved: {path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Save error", str(e))

    def save_like_reference(self):
        if self.df_csv is None:
            QtWidgets.QMessageBox.critical(self, "Error", "Reference CSV required.")
            return
        if self.y_bin_cmp is None:
            QtWidgets.QMessageBox.critical(self, "Error", "No BIN↔CSV compare (match or interp needed).")
            return
        if len(self.df_csv) != len(self.y_bin_cmp):
            QtWidgets.QMessageBox.critical(self, "Error", "BIN-on-CSV length != CSV length.")
            return

        ch = 2 if self.cmb_channel.currentText() == "Ch 2" else 1

        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Export like reference CSV", "", "CSV (*.csv)")
        if not path:
            return
        try:
            export_like_reference_csv(path, self.df_csv, self.y_bin_cmp, channel=ch)
            self._log(f"Saved: {path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Save error", str(e))

    # ---------- theme hook ----------
    def apply_theme(self, dark: bool) -> None:
        set_plot_theme(self.plot, dark=dark)
