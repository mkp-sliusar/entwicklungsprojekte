from __future__ import annotations

from PySide6 import QtCore, QtGui, QtWidgets

from live_protocol import RF_PRESETS, AF_ODR_PRESETS


def build_settings_dialog(tab: QtWidgets.QWidget) -> None:
    """Create the Settings dialog UI and attach widgets to `tab`.

    This intentionally mutates `tab` by setting the same attribute names that
    were previously created inline in live_tab.py (minimizes refactor risk).
    """

    # Settings dialog (opens via hotkeys)
    tab.settings_dialog = QtWidgets.QDialog(tab)
    tab.settings_dialog.setWindowTitle("Settings")
    tab.settings_dialog.setModal(False)
    tab.settings_dialog.setMinimumWidth(560)

    dlg_layout = QtWidgets.QVBoxLayout(tab.settings_dialog)

    scroll_area = QtWidgets.QScrollArea()
    scroll_area.setWidgetResizable(True)
    dlg_layout.addWidget(scroll_area)

    scroll_widget = QtWidgets.QWidget()
    scroll_area.setWidget(scroll_widget)

    form = QtWidgets.QVBoxLayout(scroll_widget)

    # --- Receiver settings ---
    g_rx = QtWidgets.QGroupBox("Receiver")
    gl = QtWidgets.QFormLayout(g_rx)

    tab.cmb_port = QtWidgets.QComboBox()
    tab.cmb_port.setMinimumWidth(140)

    btn_refresh = QtWidgets.QPushButton("Refresh")
    btn_refresh.clicked.connect(tab.refresh_ports)

    row = QtWidgets.QHBoxLayout()
    row.addWidget(tab.cmb_port, 1)
    row.addWidget(btn_refresh)
    gl.addRow("COM port:", row)

    tab.ed_baud = QtWidgets.QLineEdit("115200")
    tab.ed_baud.setMaximumWidth(120)
    gl.addRow("Baud:", tab.ed_baud)

    tab.cb_newfw = QtWidgets.QCheckBox("Prefer new receiver FW")
    tab.cb_newfw.setChecked(True)
    gl.addRow("Firmware:", tab.cb_newfw)

    tab.cmb_net = QtWidgets.QComboBox()
    tab.cmb_net.addItems(list(RF_PRESETS.keys()) + ["Custom"])
    tab.cmb_net.setCurrentText("1 Mbit/s" if "1 Mbit/s" in RF_PRESETS else list(RF_PRESETS.keys())[0])
    tab.cmb_net.currentTextChanged.connect(tab._net_changed)
    gl.addRow("RF preset:", tab.cmb_net)

    tab.sp_rfbyte = QtWidgets.QSpinBox()
    tab.sp_rfbyte.setRange(0, 255)
    tab.sp_rfbyte.setValue(int(RF_PRESETS.get(tab.cmb_net.currentText(), RF_PRESETS["HS 2 Mbit/s"])))
    tab.sp_rfbyte.setEnabled(False)
    gl.addRow("RF speed code:", tab.sp_rfbyte)

    tab.sp_rfch = QtWidgets.QSpinBox()
    tab.sp_rfch.setRange(0, 125)
    tab.sp_rfch.setValue(10)
    gl.addRow("RF channel:", tab.sp_rfch)

    tab.cmb_nodes = QtWidgets.QComboBox()
    tab.cmb_nodes.addItems(["8 nodes", "3 nodes", "1 node"])
    tab.cmb_nodes.setCurrentIndex(0)
    gl.addRow("Nodes:", tab.cmb_nodes)

    tab.cmb_odr = QtWidgets.QComboBox()
    tab.cmb_odr.addItems(list(AF_ODR_PRESETS.keys()))
    tab.cmb_odr.setCurrentText("High Res / 100" if "High Res / 100" in AF_ODR_PRESETS else list(AF_ODR_PRESETS.keys())[0])
    gl.addRow("AD ODR:", tab.cmb_odr)

    tab.sp_gain = QtWidgets.QSpinBox()
    tab.sp_gain.setRange(1, 128)
    tab.sp_gain.setSingleStep(1)
    tab.sp_gain.setValue(128)
    gl.addRow("Gain:", tab.sp_gain)

    tab.sp_window = QtWidgets.QDoubleSpinBox()
    tab.sp_window.setRange(1.0, 3600.0)
    tab.sp_window.setSingleStep(1.0)
    tab.sp_window.setValue(float(getattr(tab, "window_seconds", 60.0)))
    tab.sp_window.valueChanged.connect(tab._window_changed)
    gl.addRow("Plot window (s):", tab.sp_window)

    form.addWidget(g_rx)


    # --- Live processing (host-side) ---
    g_proc = QtWidgets.QGroupBox("Live processing (host-side)")
    glp = QtWidgets.QFormLayout(g_proc)

    tab.cb_spike_filter = QtWidgets.QCheckBox("Enable spike filter (single peaks)")
    tab.cb_spike_filter.setChecked(True)
    glp.addRow(tab.cb_spike_filter)

    tab.sp_spike_thr = QtWidgets.QDoubleSpinBox()
    tab.sp_spike_thr.setRange(0.0, 1000.0)
    tab.sp_spike_thr.setDecimals(6)
    tab.sp_spike_thr.setSingleStep(0.01)
    tab.sp_spike_thr.setValue(0.01)
    glp.addRow("Spike threshold (mV/V):", tab.sp_spike_thr)

    tab.cb_offset = QtWidgets.QCheckBox("Apply offset (per channel)")
    tab.cb_offset.setChecked(False)
    glp.addRow(tab.cb_offset)

    row_off = QtWidgets.QHBoxLayout()

    tab.sp_offset1 = QtWidgets.QDoubleSpinBox()
    tab.sp_offset1.setRange(-1000.0, 1000.0)
    tab.sp_offset1.setDecimals(6)
    tab.sp_offset1.setSingleStep(0.001)
    tab.sp_offset1.setValue(0.0)
    row_off.addWidget(QtWidgets.QLabel("CH1:"), 0)
    row_off.addWidget(tab.sp_offset1, 1)

    tab.sp_offset2 = QtWidgets.QDoubleSpinBox()
    tab.sp_offset2.setRange(-1000.0, 1000.0)
    tab.sp_offset2.setDecimals(6)
    tab.sp_offset2.setSingleStep(0.001)
    tab.sp_offset2.setValue(0.0)
    row_off.addWidget(QtWidgets.QLabel("CH2:"), 0)
    row_off.addWidget(tab.sp_offset2, 1)

    tab.btn_zero = QtWidgets.QPushButton("Zero now")
    row_off.addWidget(tab.btn_zero)

    glp.addRow("Offsets (mV/V):", row_off)

    form.addWidget(g_proc)


    # --- IMU / sensor extras ---
    g_imu = QtWidgets.QGroupBox("IMU (device-side)")
    gli = QtWidgets.QFormLayout(g_imu)

    tab.chk_imu_enable = QtWidgets.QCheckBox("Enable IMU (send IMU config)")
    tab.chk_imu_enable.setChecked(False)
    gli.addRow(tab.chk_imu_enable)

    # IMU ODR preset (maps to imu_odr_setting 0..7)
    tab.cmb_imu_odr = QtWidgets.QComboBox()
    tab.cmb_imu_odr.addItems([
        "Off (0)",
        "IMU ODR 1 (1)",
        "IMU ODR 2 (2)  — 104 Hz (default)",
        "IMU ODR 3 (3)",
        "IMU ODR 4 (4)",
        "IMU ODR 5 (5)",
        "IMU ODR 6 (6)",
        "IMU ODR 7 (7)"
    ])
    # default aligns with existing official defaults (2)
    tab.cmb_imu_odr.setCurrentIndex(2)
    gli.addRow("IMU ODR:", tab.cmb_imu_odr)

    # Ranges for accelerometer/gyro/magnetometer (small ints)
    tab.sp_imu_acc = QtWidgets.QSpinBox()
    tab.sp_imu_acc.setRange(0, 7)
    tab.sp_imu_acc.setValue(2)
    gli.addRow("Acc range code:", tab.sp_imu_acc)

    tab.sp_imu_gyr = QtWidgets.QSpinBox()
    tab.sp_imu_gyr.setRange(0, 7)
    tab.sp_imu_gyr.setValue(2)
    gli.addRow("Gyro range code:", tab.sp_imu_gyr)

    tab.sp_imu_mag = QtWidgets.QSpinBox()
    tab.sp_imu_mag.setRange(0, 7)
    tab.sp_imu_mag.setValue(2)
    gli.addRow("Mag range code:", tab.sp_imu_mag)

    form.addWidget(g_imu)
    # --- UI settings ---
    g_ui = QtWidgets.QGroupBox("UI")
    glu = QtWidgets.QFormLayout(g_ui)

    tab.cb_ui_dark = QtWidgets.QCheckBox("Dark theme")
    tab.cb_ui_dark.setChecked(True)
    tab.cb_ui_dark.toggled.connect(tab._ui_dark_toggled)
    glu.addRow(tab.cb_ui_dark)

    tab.cb_ui_decoder = QtWidgets.QCheckBox("Show Offline decoder tab")
    tab.cb_ui_decoder.setChecked(False)
    tab.cb_ui_decoder.toggled.connect(tab._ui_decoder_toggled)
    glu.addRow(tab.cb_ui_decoder)

    form.addWidget(g_ui)

    # --- Recording ---
    g_rec = QtWidgets.QGroupBox("Recording")
    glr = QtWidgets.QFormLayout(g_rec)

    tab.cb_record = QtWidgets.QCheckBox("Record CSV")
    tab.cb_record.setChecked(False)
    tab.cb_record.setTristate(False)
    tab.cb_record.toggled.connect(tab._record_toggled)
    glr.addRow(tab.cb_record)

    tab.ed_path = QtWidgets.QLineEdit("")
    btn_path = QtWidgets.QPushButton("Choose folder")
    btn_path.clicked.connect(tab.choose_record_path)

    rowp = QtWidgets.QHBoxLayout()
    rowp.addWidget(tab.ed_path, 1)
    rowp.addWidget(btn_path)
    glr.addRow("CSV folder:", rowp)

    form.addWidget(g_rec)

    # --- Auto trigger ---
    g_trig = QtWidgets.QGroupBox("Auto trigger")
    glt = QtWidgets.QFormLayout(g_trig)

    tab.cb_trig = QtWidgets.QCheckBox("Auto trigger")
    tab.cb_trig.setChecked(bool(getattr(tab, "_trig_cfg", None) and tab._trig_cfg.enabled))
    tab.cb_trig.setTristate(False)
    tab.cb_trig.toggled.connect(tab._trigger_toggled)
    glt.addRow(tab.cb_trig)

    tab.ed_trig_dir = QtWidgets.QLineEdit(getattr(getattr(tab, "_trig_cfg", None), "out_dir", ""))
    btn_trig_dir = QtWidgets.QPushButton("Choose folder")
    btn_trig_dir.clicked.connect(tab.choose_trigger_dir)

    rowt = QtWidgets.QHBoxLayout()
    rowt.addWidget(tab.ed_trig_dir, 1)
    rowt.addWidget(btn_trig_dir)
    glt.addRow("Output folder:", rowt)

    tab.sp_trig_thr = QtWidgets.QDoubleSpinBox()
    tab.sp_trig_thr.setRange(0.0001, 1000.0)
    tab.sp_trig_thr.setDecimals(6)
    tab.sp_trig_thr.setValue(float(getattr(getattr(tab, "_trig_cfg", None), "threshold_mvv", 0.02)))
    tab.sp_trig_thr.valueChanged.connect(tab._trigger_cfg_changed)
    glt.addRow("Threshold (mV/V):", tab.sp_trig_thr)

    tab.sp_trig_pre = QtWidgets.QDoubleSpinBox()
    tab.sp_trig_pre.setRange(0.0, 600.0)
    tab.sp_trig_pre.setValue(float(getattr(getattr(tab, "_trig_cfg", None), "pre_s", 5.0)))
    tab.sp_trig_pre.valueChanged.connect(tab._trigger_cfg_changed)
    glt.addRow("Pre (s):", tab.sp_trig_pre)

    tab.sp_trig_post = QtWidgets.QDoubleSpinBox()
    tab.sp_trig_post.setRange(0.0, 600.0)
    tab.sp_trig_post.setValue(float(getattr(getattr(tab, "_trig_cfg", None), "post_s", 5.0)))
    tab.sp_trig_post.valueChanged.connect(tab._trigger_cfg_changed)
    glt.addRow("Post (s):", tab.sp_trig_post)

    tab.sp_trig_cool = QtWidgets.QDoubleSpinBox()
    tab.sp_trig_cool.setRange(0.0, 600.0)
    tab.sp_trig_cool.setValue(float(getattr(getattr(tab, "_trig_cfg", None), "cooldown_s", 5.0)))
    tab.sp_trig_cool.valueChanged.connect(tab._trigger_cfg_changed)
    glt.addRow("Cooldown (s):", tab.sp_trig_cool)

    tab.ed_trig_prefix = QtWidgets.QLineEdit(str(getattr(getattr(tab, "_trig_cfg", None), "filename_prefix", "trigger")))
    tab.ed_trig_prefix.textChanged.connect(tab._trigger_cfg_changed)
    glt.addRow("File prefix:", tab.ed_trig_prefix)

    tab.lbl_trig_state = QtWidgets.QLabel("Off")
    glt.addRow("State:", tab.lbl_trig_state)

    form.addWidget(g_trig)

    # --- Log ---
    tab.log = QtWidgets.QPlainTextEdit()
    tab.log.setReadOnly(True)
    tab.log.setMaximumBlockCount(2000)
    tab.log.setMinimumHeight(220)
    form.addWidget(tab.log)

    # Hotkeys
    QtGui.QShortcut(QtGui.QKeySequence("Ctrl+,"), tab, activated=tab.toggle_settings)
    QtGui.QShortcut(QtGui.QKeySequence("F10"), tab, activated=tab.toggle_settings)

    # Make sure settings dialog closes on ESC.
    QtGui.QShortcut(QtGui.QKeySequence("Escape"), tab.settings_dialog, activated=tab.settings_dialog.hide)

