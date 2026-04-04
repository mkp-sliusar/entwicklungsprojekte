#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import socket
import struct
import threading
import time
import ctypes
import sys
from datetime import datetime
from pathlib import Path
from collections import deque
from dataclasses import dataclass
from typing import Any

import pyqtgraph as pg
import requests
from PySide6 import QtCore, QtGui, QtSvg, QtWidgets


def _load_app_icon() -> QtGui.QIcon:
    base = Path(__file__).resolve().parent
    for name in ("lorasense_icon.ico", "lorasense_icon.png"):
        candidate = base / name
        if candidate.exists():
            icon = QtGui.QIcon(str(candidate))
            if not icon.isNull():
                return icon
    return QtGui.QIcon()


def _apply_window_icon(widget: QtWidgets.QWidget) -> None:
    icon = _load_app_icon()
    if not icon.isNull():
        widget.setWindowIcon(icon)


def _set_windows_app_id() -> None:
    if sys.platform.startswith("win"):
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("MKP.LoRaSense.PCControl")
        except Exception:
            pass

MAGIC = b"MKPS"
PACKET_SIZE = 50
PACKET_STRUCT = struct.Struct("<4sHHIIIHiffiffhH")
HTTP_TIMEOUT_S = 3
STREAM_STATUS_COLORS = {
    "connected": "#1b5e20",
    "connecting": "#8d6e00",
    "disconnected": "#424242",
}
FREQ_OPTIONS = ["5", "11.25", "20", "22.5", "40", "44", "45", "82.5", "90", "150", "175", "180", "250", "330", "350",
                "600", "660", "1000", "1200", "2000"]
WEG_LENGTH_OPTIONS_MM = [5, 10, 25, 50, 75, 100, 150, 200, 300, 500]
UNIT_OPTIONS = ["V", "mm", "um", "C", "%", "bar", "mA", "kN"]
PLOT_CHANNELS = (
    ("dms_display", "DMS"),
    ("ain2_display", "Kanal 2"),
    ("temp_c", "Temperatur"),
)
PLOT_FIELDS = [field for field, _ in PLOT_CHANNELS]
PLOT_FIELD_LABELS = {field: label for field, label in PLOT_CHANNELS}

LIGHT_THEME_STYLESHEET = """
QMainWindow, QWidget {
    background-color: #eef0f1;
    color: #111111;
}
QGroupBox {
    border: 1px solid #d0d3d6;
    border-radius: 10px;
    margin-top: 8px;
    padding-top: 8px;
    background-color: #eef0f1;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 4px;
    color: #1c1c1c;
}
QQFrame[property="card"] {
    background-color: #d6dbe0;
    border: 1px solid #c2c6ca;
    border-radius: 10px;
    padding: 6px;
}
QPushButton, QComboBox, QLineEdit, QDoubleSpinBox, QSpinBox {
    background-color: #f7f8f9;
    color: #111111;
    border: 1px solid #cfd3d7;
    border-radius: 10px;
    padding: 4px 8px;
}
QPushButton:hover, QComboBox:hover, QLineEdit:hover, QDoubleSpinBox:hover, QSpinBox:hover {
    background-color: #eceef0;
}
QPushButton:pressed {
    background-color: #dde1e4;
}
QLabel {
    color: #1a1a1a;
}
QSplitter::handle {
    background-color: #d0d3d6;
}
"""


MKP_LOGO_SVG_LIGHT = r"""<svg width="221" height="78" viewBox="0 0 221 78" fill="none" xmlns="http://www.w3.org/2000/svg">
<g id="Logo_final">
<path id="Combined-Shape" fill-rule="evenodd" clip-rule="evenodd" d="M143.581 56.5515L122.444 28.5025L143.857 0.0872574L218.45 0.0872574C219.858 0.0872574 221 1.22893 221 2.63726V54.0015C221 55.4099 219.858 56.5515 218.45 56.5515H143.581ZM130.809 56.5515H101.308L116.059 36.9769L130.809 56.5515ZM88.5362 56.5515H55.408L88.6461 12.4431V56.4058L88.5362 56.5515ZM131.085 0.0872574L98.8461 42.8699V0.0872574L131.085 0.0872574ZM42.6362 56.5515H10.358L42.7461 13.5711V56.4058L42.6362 56.5515ZM85.1851 0.0872574L52.9461 42.8699V0.0872574H85.1851ZM40.1351 0.0872574L0 53.3484L0 2.63726C0 1.22893 1.14167 0.0872574 2.55 0.0872574H40.1351Z" fill="#42C0EB"/>
<path id="MARX-KRONTAL-Partner" fill-rule="evenodd" clip-rule="evenodd" d="M0.303467 77.2076V67.5494C0.303467 67.2275 0.579413 66.9515 0.901351 66.9515H1.03932C1.29994 66.9515 1.4839 67.0895 1.60655 67.2735L5.43914 73.0377L9.27173 67.2735C9.39437 67.0742 9.59366 66.9515 9.83895 66.9515H9.97692C10.2989 66.9515 10.5748 67.2275 10.5748 67.5494V77.1922C10.5748 77.5295 10.2989 77.8054 9.97692 77.8054C9.63966 77.8054 9.36371 77.5142 9.36371 77.1922V69.1898L5.92971 74.2335C5.79174 74.4328 5.63843 74.5401 5.42381 74.5401C5.20918 74.5401 5.04055 74.4328 4.90257 74.2335L1.4839 69.2051V77.2076C1.4839 77.5448 1.22329 77.8054 0.88602 77.8054C0.564083 77.8054 0.303467 77.5448 0.303467 77.2076ZM13.5712 77.2535C13.5712 77.1616 13.5865 77.0542 13.6478 76.9316L18.0017 67.4115C18.155 67.0742 18.3849 66.8749 18.7682 66.8749H18.8295C19.1974 66.8749 19.4427 67.0742 19.5807 67.4115L23.9345 76.9163C23.9805 77.0236 24.0112 77.1309 24.0112 77.2229C24.0112 77.5448 23.7506 77.8054 23.4286 77.8054C23.1373 77.8054 22.938 77.6061 22.8307 77.3609L21.7116 74.8927H15.8248L14.7056 77.3915C14.5983 77.6521 14.399 77.8054 14.1231 77.8054C13.8165 77.8054 13.5712 77.5601 13.5712 77.2535ZM16.3 73.7889H21.2364L18.7682 68.3006L16.3 73.7889ZM27.0229 77.1922V67.6107C27.0229 67.2735 27.2988 66.9975 27.6208 66.9975H31.6373C32.9557 66.9975 34.0135 67.3961 34.6881 68.0707C35.2093 68.5919 35.5159 69.3431 35.5159 70.1862V70.2169C35.5159 71.9799 34.3201 73.007 32.6491 73.329L35.3166 76.763C35.4393 76.9009 35.5159 77.0389 35.5159 77.2076C35.5159 77.5295 35.2093 77.8054 34.9027 77.8054C34.6574 77.8054 34.4734 77.6675 34.3355 77.4835L31.3154 73.5589H28.234V77.1922C28.234 77.5295 27.958 77.8054 27.6208 77.8054C27.2988 77.8054 27.0229 77.5295 27.0229 77.1922ZM28.234 72.4705H31.53C33.1397 72.4705 34.2895 71.6426 34.2895 70.2629V70.2322C34.2895 68.9138 33.2777 68.1167 31.5453 68.1167H28.234V72.4705ZM38.2977 77.2382C38.2977 77.0849 38.359 76.9316 38.4663 76.809L42.0536 72.2558L38.6503 67.9633C38.543 67.8254 38.451 67.6874 38.451 67.5034C38.451 67.1968 38.7116 66.9209 39.0489 66.9209C39.2941 66.9209 39.4321 67.0435 39.5854 67.2275L42.8201 71.428L46.0088 67.2735C46.1775 67.0742 46.3308 66.9209 46.5914 66.9209C46.8827 66.9209 47.1433 67.1662 47.1433 67.4881C47.1433 67.6414 47.082 67.7947 46.9747 67.9174L43.556 72.2405L47.0973 76.763C47.2199 76.9009 47.2966 77.0389 47.2966 77.2076C47.2966 77.5295 47.036 77.8054 46.6987 77.8054C46.4534 77.8054 46.3154 77.6828 46.1621 77.4988L42.7741 73.099L39.4321 77.4528C39.2635 77.6521 39.1102 77.8054 38.8342 77.8054C38.5583 77.8054 38.2977 77.5602 38.2977 77.2382ZM55.8189 77.1922V67.5341C55.8189 67.1968 56.0948 66.9209 56.4168 66.9209C56.754 66.9209 57.03 67.1968 57.03 67.5341V73.4056L63.1468 67.1355C63.2848 67.0129 63.4227 66.9209 63.622 66.9209C63.944 66.9209 64.2046 67.2122 64.2046 67.5188C64.2046 67.6874 64.1279 67.8254 64.0053 67.948L60.2034 71.7193L64.2812 76.7476C64.3885 76.8856 64.4499 77.0083 64.4499 77.1922C64.4499 77.5142 64.1739 77.8054 63.8367 77.8054C63.6067 77.8054 63.4534 77.6828 63.3461 77.5295L59.3449 72.5471L57.03 74.862V77.1922C57.03 77.5295 56.754 77.8054 56.4168 77.8054C56.0948 77.8054 55.8189 77.5295 55.8189 77.1922ZM67.4616 77.1922V67.6107C67.4616 67.2735 67.7375 66.9975 68.0595 66.9975H72.076C73.3944 66.9975 74.4522 67.3961 75.1268 68.0707C75.648 68.5919 75.9546 69.3431 75.9546 70.1862V70.2169C75.9546 71.9799 74.7588 73.007 73.0878 73.329L75.7553 76.763C75.8779 76.9009 75.9546 77.0389 75.9546 77.2076C75.9546 77.5295 75.648 77.8054 75.3414 77.8054C75.0961 77.8054 74.9121 77.6675 74.7742 77.4835L71.7541 73.5589H68.6727V77.1922C68.6727 77.5295 68.3967 77.8054 68.0595 77.8054C67.7375 77.8054 67.4616 77.5295 67.4616 77.1922ZM68.6727 72.4705H71.9687C73.5784 72.4705 74.7282 71.6426 74.7282 70.2629V70.2322C74.7282 68.9138 73.7164 68.1167 71.984 68.1167H68.6727V72.4705ZM84.2246 77.9127C80.9593 77.9127 78.767 75.3526 78.767 72.3938V72.3632C78.767 69.4044 80.9899 66.8136 84.2553 66.8136C87.5207 66.8136 89.7129 69.3737 89.7129 72.3325V72.3632C89.7129 75.3219 87.49 77.9127 84.2246 77.9127ZM84.2553 76.7936C86.7081 76.7936 88.4558 74.8313 88.4558 72.3938V72.3632C88.4558 69.9256 86.6775 67.9327 84.2246 67.9327C81.7718 67.9327 80.0241 69.895 80.0241 72.3325V72.3632C80.0241 74.8007 81.8024 76.7936 84.2553 76.7936ZM92.9699 77.2076V67.5494C92.9699 67.2275 93.2458 66.9515 93.5678 66.9515H93.7364C94.0124 66.9515 94.181 67.0895 94.3496 67.3041L100.865 75.6132V67.5188C100.865 67.1968 101.126 66.9209 101.463 66.9209C101.785 66.9209 102.045 67.1968 102.045 67.5188V77.2076C102.045 77.5295 101.816 77.7748 101.494 77.7748H101.432C101.172 77.7748 100.988 77.6215 100.804 77.4068L94.1503 68.8985V77.2076C94.1503 77.5295 93.8897 77.8054 93.5525 77.8054C93.2305 77.8054 92.9699 77.5295 92.9699 77.2076ZM108.614 77.1922V68.1167H105.502C105.195 68.1167 104.935 67.8714 104.935 67.5648C104.935 67.2581 105.195 66.9975 105.502 66.9975H112.952C113.259 66.9975 113.52 67.2581 113.52 67.5648C113.52 67.8714 113.259 68.1167 112.952 68.1167H109.84V77.1922C109.84 77.5295 109.564 77.8054 109.227 77.8054C108.89 77.8054 108.614 77.5295 108.614 77.1922ZM114.354 77.2535C114.354 77.1616 114.37 77.0542 114.431 76.9316L118.785 67.4115C118.938 67.0742 119.168 66.8749 119.551 66.8749H119.613C119.981 66.8749 120.226 67.0742 120.364 67.4115L124.718 76.9163C124.764 77.0236 124.794 77.1309 124.794 77.2229C124.794 77.5448 124.534 77.8054 124.212 77.8054C123.92 77.8054 123.721 77.6061 123.614 77.3609L122.495 74.8927H116.608L115.489 77.3915C115.381 77.6521 115.182 77.8054 114.906 77.8054C114.6 77.8054 114.354 77.5601 114.354 77.2535ZM117.083 73.7889H122.02L119.551 68.3006L117.083 73.7889ZM127.806 77.1156V67.5341C127.806 67.1968 128.082 66.9209 128.404 66.9209C128.741 66.9209 129.017 67.1968 129.017 67.5341V76.6097H134.582C134.889 76.6097 135.134 76.8703 135.134 77.1769C135.134 77.4835 134.889 77.7288 134.582 77.7288H128.404C128.082 77.7288 127.806 77.4528 127.806 77.1156ZM143.288 77.1922V67.6107C143.288 67.2735 143.564 66.9975 143.886 66.9975H147.305C149.727 66.9975 151.321 68.2853 151.321 70.4009V70.4315C151.321 72.7464 149.39 73.9422 147.106 73.9422H144.499V77.1922C144.499 77.5295 144.223 77.8054 143.886 77.8054C143.564 77.8054 143.288 77.5295 143.288 77.1922ZM144.499 72.8384H147.152C148.93 72.8384 150.095 71.8879 150.095 70.4775V70.4469C150.095 68.9138 148.945 68.1167 147.213 68.1167H144.499V72.8384ZM152.493 77.2535C152.493 77.1616 152.509 77.0542 152.57 76.9316L156.924 67.4115C157.077 67.0742 157.307 66.8749 157.69 66.8749H157.752C158.12 66.8749 158.365 67.0742 158.503 67.4115L162.857 76.9163C162.903 77.0236 162.933 77.1309 162.933 77.2229C162.933 77.5448 162.673 77.8054 162.351 77.8054C162.06 77.8054 161.86 77.6061 161.753 77.3609L160.634 74.8927H154.747L153.628 77.3915C153.521 77.6521 153.321 77.8054 153.045 77.8054C152.739 77.8054 152.493 77.5601 152.493 77.2535ZM155.222 73.7889H160.159L157.69 68.3006L155.222 73.7889ZM165.945 77.1922V67.6107C165.945 67.2735 166.221 66.9975 166.543 66.9975H170.56C171.878 66.9975 172.936 67.3961 173.61 68.0707C174.132 68.5919 174.438 69.3431 174.438 70.1862V70.2169C174.438 71.9799 173.242 73.007 171.571 73.329L174.239 76.763C174.362 76.9009 174.438 77.0389 174.438 77.2076C174.438 77.5295 174.132 77.8054 173.825 77.8054C173.58 77.8054 173.396 77.6675 173.258 77.4835L170.238 73.5589H167.156V77.1922C167.156 77.5295 166.88 77.8054 166.543 77.8054C166.221 77.8054 165.945 77.5295 165.945 77.1922ZM167.156 72.4705H170.452C172.062 72.4705 173.212 71.6426 173.212 70.2629V70.2322C173.212 68.9138 172.2 68.1167 170.468 68.1167H167.156V72.4705ZM180.455 77.1922V68.1167H177.343C177.036 68.1167 176.775 67.8714 176.775 67.5648C176.775 67.2581 177.036 66.9975 177.343 66.9975H184.793C185.1 66.9975 185.36 67.2581 185.36 67.5648C185.36 67.8714 185.1 68.1167 184.793 68.1167H181.681V77.1922C181.681 77.5295 181.405 77.8054 181.068 77.8054C180.731 77.8054 180.455 77.5295 180.455 77.1922ZM188.249 77.2076V67.5494C188.249 67.2275 188.525 66.9515 188.847 66.9515H189.016C189.292 66.9515 189.461 67.0895 189.629 67.3041L196.145 75.6132V67.5188C196.145 67.1968 196.405 66.9209 196.742 66.9209C197.064 66.9209 197.325 67.1968 197.325 67.5188V77.2076C197.325 77.5295 197.095 77.7748 196.773 77.7748H196.712C196.451 77.7748 196.267 77.6215 196.083 77.4068L189.43 68.8985V77.2076C189.43 77.5295 189.169 77.8054 188.832 77.8054C188.51 77.8054 188.249 77.5295 188.249 77.2076ZM201.655 77.7288C201.333 77.7288 201.057 77.4528 201.057 77.1156V67.6107C201.057 67.2735 201.333 66.9975 201.655 66.9975H208.324C208.63 66.9975 208.876 67.2428 208.876 67.5494C208.876 67.856 208.63 68.1013 208.324 68.1013H202.268V71.7653H207.634C207.941 71.7653 208.186 72.0259 208.186 72.3172C208.186 72.6238 207.941 72.8691 207.634 72.8691H202.268V76.625H208.401C208.707 76.625 208.952 76.8703 208.952 77.1769C208.952 77.4835 208.707 77.7288 208.401 77.7288H201.655ZM212.025 77.1922V67.6107C212.025 67.2735 212.301 66.9975 212.623 66.9975H216.64C217.958 66.9975 219.016 67.3961 219.691 68.0707C220.212 68.5919 220.518 69.3431 220.518 70.1862V70.2169C220.518 71.9799 219.323 73.007 217.652 73.329L220.319 76.763C220.442 76.9009 220.518 77.0389 220.518 77.2076C220.518 77.5295 220.212 77.8054 219.905 77.8054C219.66 77.8054 219.476 77.6675 219.338 77.4835L216.318 73.5589H213.237V77.1922C213.237 77.5295 212.961 77.8054 212.623 77.8054C212.301 77.8054 212.025 77.5295 212.025 77.1922ZM213.237 72.4705H216.533C218.142 72.4705 219.292 71.6426 219.292 70.2629V70.2322C219.292 68.9138 218.28 68.1167 216.548 68.1167H213.237V72.4705Z" fill="#003B62"/>
</g>
</svg>

"""
MKP_LOGO_SVG_DARK = MKP_LOGO_SVG_LIGHT.replace("#003B62", "#EAF2F8")


def _render_svg_to_pixmap(svg_data: str, target_height: int) -> QtGui.QPixmap:
    renderer = QtSvg.QSvgRenderer(QtCore.QByteArray(svg_data.encode("utf-8")))
    if not renderer.isValid():
        return QtGui.QPixmap()
    default_size = renderer.defaultSize()
    if default_size.isEmpty() or default_size.height() <= 0:
        target_width = max(1, int(target_height * 2.8))
    else:
        target_width = max(1, int(default_size.width() * target_height / default_size.height()))
    pixmap = QtGui.QPixmap(target_width, target_height)
    pixmap.fill(QtCore.Qt.transparent)
    painter = QtGui.QPainter(pixmap)
    painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
    painter.setRenderHint(QtGui.QPainter.SmoothPixmapTransform, True)
    renderer.render(painter)
    painter.end()
    return pixmap


def _mkp_logo_pixmap(theme_name: str, target_height: int) -> QtGui.QPixmap:
    svg_data = MKP_LOGO_SVG_DARK if theme_name == "dark" else MKP_LOGO_SVG_LIGHT
    return _render_svg_to_pixmap(svg_data, target_height)


def _make_light_palette(style: QtWidgets.QStyle) -> QtGui.QPalette:
    palette = style.standardPalette()
    palette.setColor(QtGui.QPalette.Window, QtGui.QColor('#f6f7f8'))
    palette.setColor(QtGui.QPalette.WindowText, QtGui.QColor('#111111'))
    palette.setColor(QtGui.QPalette.Base, QtGui.QColor('#ffffff'))
    palette.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor('#eef0f1'))
    palette.setColor(QtGui.QPalette.ToolTipBase, QtGui.QColor('#ffffff'))
    palette.setColor(QtGui.QPalette.ToolTipText, QtGui.QColor('#111111'))
    palette.setColor(QtGui.QPalette.Text, QtGui.QColor('#111111'))
    palette.setColor(QtGui.QPalette.Button, QtGui.QColor('#f3f4f6'))
    palette.setColor(QtGui.QPalette.ButtonText, QtGui.QColor('#111111'))
    palette.setColor(QtGui.QPalette.BrightText, QtGui.QColor('#ffffff'))
    palette.setColor(QtGui.QPalette.Link, QtGui.QColor('#1f4d7a'))
    palette.setColor(QtGui.QPalette.Highlight, QtGui.QColor('#cfd8e3'))
    palette.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor('#111111'))
    palette.setColor(QtGui.QPalette.Mid, QtGui.QColor('#666666'))
    palette.setColor(QtGui.QPalette.Midlight, QtGui.QColor('#d8dadc'))
    palette.setColor(QtGui.QPalette.Dark, QtGui.QColor('#c2c5c8'))
    palette.setColor(QtGui.QPalette.Shadow, QtGui.QColor('#999999'))
    return palette


def _read_saved_theme_name() -> str:
    settings = QtCore.QSettings("OpenAI", "LoRaSensePcControl")
    theme_name = settings.value("ui/theme", "dark", str)
    return theme_name if theme_name in {"dark", "light"} else "dark"



def _build_splash_pixmap(theme_name: str, icon: QtGui.QIcon) -> QtGui.QPixmap:
    width, height = 560, 320
    pixmap = QtGui.QPixmap(width, height)
    pixmap.fill(QtCore.Qt.transparent)

    painter = QtGui.QPainter(pixmap)
    painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
    painter.setRenderHint(QtGui.QPainter.TextAntialiasing, True)
    painter.setRenderHint(QtGui.QPainter.SmoothPixmapTransform, True)

    if theme_name == "light":
        bg = QtGui.QColor("#f6f7f8")
        card = QtGui.QColor("#e3e5e7")
        border = QtGui.QColor("#c8ccd0")
        title = QtGui.QColor("#101418")
        subtitle = QtGui.QColor("#4e5b66")
        accent = QtGui.QColor("#1f4d7a")
    else:
        bg = QtGui.QColor("#101215")
        card = QtGui.QColor("#181c21")
        border = QtGui.QColor("#2a3138")
        title = QtGui.QColor("#f3f5f7")
        subtitle = QtGui.QColor("#9aa6b2")
        accent = QtGui.QColor("#6fb1ff")

    outer = QtCore.QRectF(0, 0, width, height)
    painter.setPen(QtCore.Qt.NoPen)
    painter.setBrush(bg)
    painter.drawRoundedRect(outer.adjusted(8, 8, -8, -8), 24, 24)

    card_rect = QtCore.QRectF(26, 26, width - 52, height - 52)
    painter.setPen(QtGui.QPen(border, 1.2))
    painter.setBrush(card)
    painter.drawRoundedRect(card_rect, 20, 20)

    logo_pixmap = _mkp_logo_pixmap(theme_name, 52)
    if not logo_pixmap.isNull():
        logo_x = int(card_rect.left()) + 28
        logo_y = int(card_rect.top()) + 28
        painter.drawPixmap(logo_x, logo_y, logo_pixmap)

    icon_size = 56
    icon_rect = QtCore.QRect(int(card_rect.right()) - icon_size - 28, int(card_rect.top()) + 26, icon_size, icon_size)
    if not icon.isNull():
        icon.paint(painter, icon_rect)
    else:
        painter.setPen(QtCore.Qt.NoPen)
        painter.setBrush(accent)
        painter.drawRoundedRect(QtCore.QRectF(icon_rect), 16, 16)
        painter.setPen(QtGui.QPen(QtGui.QColor("white")))
        font = QtGui.QFont("Segoe UI", 24, QtGui.QFont.Bold)
        painter.setFont(font)
        painter.drawText(icon_rect, QtCore.Qt.AlignCenter, "L")

    painter.setPen(title)
    title_font = QtGui.QFont("Segoe UI", 22, QtGui.QFont.Bold)
    painter.setFont(title_font)
    painter.drawText(
        QtCore.QRectF(card_rect.left() + 28, card_rect.top() + 102, card_rect.width() - 56, 38),
        QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter,
        "LoRaSense PC Control",
    )

    painter.setPen(subtitle)
    subtitle_font = QtGui.QFont("Segoe UI", 11)
    painter.setFont(subtitle_font)
    painter.drawText(
        QtCore.QRectF(card_rect.left() + 28, card_rect.top() + 140, card_rect.width() - 56, 24),
        QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter,
        "Initializing interface, theme, network and plots",
    )

    line_y = int(card_rect.top()) + 190
    painter.setPen(QtGui.QPen(border, 1))
    painter.drawLine(int(card_rect.left()) + 28, line_y, int(card_rect.right()) - 28, line_y)

    bar_rect = QtCore.QRectF(card_rect.left() + 28, card_rect.bottom() - 56, card_rect.width() - 56, 10)
    painter.setPen(QtCore.Qt.NoPen)
    painter.setBrush(border)
    painter.drawRoundedRect(bar_rect, 5, 5)

    progress_rect = QtCore.QRectF(bar_rect.left(), bar_rect.top(), bar_rect.width() * 0.62, bar_rect.height())
    painter.setBrush(accent)
    painter.drawRoundedRect(progress_rect, 5, 5)

    painter.setPen(subtitle)
    footer_font = QtGui.QFont("Segoe UI", 10)
    painter.setFont(footer_font)
    painter.drawText(
        QtCore.QRectF(card_rect.left() + 28, card_rect.bottom() - 34, card_rect.width() - 56, 20),
        QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter,
        "MKP · Measurement startup",
    )

    painter.end()
    return pixmap


def _create_splash(theme_name: str, icon: QtGui.QIcon) -> QtWidgets.QSplashScreen:
    splash = QtWidgets.QSplashScreen(_build_splash_pixmap(theme_name, icon))
    splash.setWindowFlags(
        QtCore.Qt.SplashScreen
        | QtCore.Qt.WindowStaysOnTopHint
        | QtCore.Qt.FramelessWindowHint
        | QtCore.Qt.NoDropShadowWindowHint
    )
    splash.setAttribute(QtCore.Qt.WA_TranslucentBackground, True)
    splash.setAttribute(QtCore.Qt.WA_ShowWithoutActivating, True)
    splash.setEnabled(False)
    if not icon.isNull():
        splash.setWindowIcon(icon)
    return splash


def _safe_plot_field(value: Any, fallback: str) -> str:
    return value if value in PLOT_FIELDS else fallback


def _plot_field_label(field: Any) -> str:
    return PLOT_FIELD_LABELS.get(str(field), str(field))


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
    tare_enabled: bool = False
    tare_value: float = 0.0
    spike_filter_enabled: bool = False
    spike_filter_strength: int = 4


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
    tare_enabled: bool = False
    tare_value: float = 0.0
    spike_filter_enabled: bool = False
    spike_filter_strength: int = 4


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


class MarqueeLabel(QtWidgets.QWidget):
    def __init__(self, text: str = "-", parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._text = text
        self._offset = 0
        self._step = 1
        self._gap = 40
        self._padding = 8
        self._text_width = 0
        self._single_width = 0
        self._timer = QtCore.QTimer(self)
        self._timer.setInterval(30)
        self._timer.timeout.connect(self._tick)
        self.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        self.setMinimumHeight(30)
        self._update_metrics()

    def setText(self, text: str) -> None:
        value = text if text and text.strip() else "-"
        if value == self._text:
            return
        self._text = value
        self._offset = 0
        self._update_metrics()
        self.update()

    def text(self) -> str:
        return self._text

    def _update_metrics(self) -> None:
        fm = self.fontMetrics()
        self._single_width = max(1, fm.horizontalAdvance(self._text))
        self._text_width = self._single_width + self._gap
        self._update_timer()

    def _update_timer(self) -> None:
        available = max(1, self.width() - (2 * self._padding))
        should_scroll = self._single_width > available
        if should_scroll:
            if not self._timer.isActive():
                self._timer.start()
        else:
            self._timer.stop()
            self._offset = 0

    def _tick(self) -> None:
        if self._text_width <= 0:
            return
        self._offset = (self._offset + self._step) % self._text_width
        self.update()

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._update_timer()

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:
        del event
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.TextAntialiasing, True)
        rect = self.rect().adjusted(self._padding, 0, -self._padding, 0)
        painter.setPen(self.palette().color(QtGui.QPalette.Text))
        fm = painter.fontMetrics()
        baseline = rect.y() + (rect.height() + fm.ascent() - fm.descent()) // 2
        available = max(1, rect.width())
        if self._single_width <= available:
            painter.drawText(rect, QtCore.Qt.AlignVCenter | QtCore.Qt.AlignLeft, self._text)
            return
        start_x = rect.x() - self._offset
        while start_x < rect.right() + self._text_width:
            painter.drawText(start_x, baseline, self._text)
            start_x += self._text_width


class TempSettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, settings: TempSettings) -> None:
        super().__init__(parent)
        _apply_window_icon(self)
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
    def __init__(
            self,
            parent: QtWidgets.QWidget,
            settings: DmsSettings,
            current_value: float,
            current_unit: str,
            selftest_value: str = "-",
            selftest_supported: bool = False,
    ) -> None:
        super().__init__(parent)
        _apply_window_icon(self)
        self._selftest_supported = selftest_supported
        self._initial_tare_enabled = settings.tare_enabled
        self._current_value = current_value
        self._current_unit = current_unit
        self._tare_value = settings.tare_value
        self.setWindowTitle("DMS settings")
        self.setModal(True)
        self.resize(460, 360)

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

        self.tare_box = QtWidgets.QCheckBox("Offset zero (tare current value)")
        self.tare_box.setChecked(settings.tare_enabled)
        form.addRow(self.tare_box)

        tare_hint = "--" if math.isnan(current_value) else f"Current: {current_value:.4f} {current_unit}"
        self.tare_hint = QtWidgets.QLabel(tare_hint)
        self.tare_hint.setStyleSheet("color:#666;")
        layout.addWidget(self.tare_hint)

        self.filter_box = QtWidgets.QCheckBox("Spike filter")
        self.filter_box.setChecked(settings.spike_filter_enabled)
        form.addRow(self.filter_box)

        filter_row = QtWidgets.QHBoxLayout()
        self.filter_strength = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.filter_strength.setRange(1, 10)
        self.filter_strength.setValue(max(1, min(10, int(settings.spike_filter_strength))))
        self.filter_label = QtWidgets.QLabel(str(self.filter_strength.value()))
        self.filter_reset = QtWidgets.QToolButton()
        self.filter_reset.setText("↺")
        self.filter_reset.setToolTip("Reset to optimal")
        filter_row.addWidget(self.filter_strength, 1)
        filter_row.addWidget(self.filter_label)
        filter_row.addWidget(self.filter_reset)
        form.addRow("Filter strength", filter_row)

        self.filter_box.toggled.connect(self._update_filter_state)
        self.filter_strength.valueChanged.connect(lambda v: self.filter_label.setText(str(v)))
        self.filter_reset.clicked.connect(lambda: self.filter_strength.setValue(4))
        self._update_filter_state()

        self.selftest_value_label = QtWidgets.QLabel(selftest_value if selftest_value else "-")
        self.selftest_value_label.setStyleSheet("font-family:'Courier New', monospace;")
        form.addRow("Selbsttest", self.selftest_value_label)

        if self._selftest_supported:
            self.selftest_button = QtWidgets.QPushButton("Run Selbsttest")
            form.addRow("", self.selftest_button)

        note = QtWidgets.QLabel(
            "K-factor = 0  -> display in mV/V\n"
            "K-factor > 0 -> display in um/m\n"
            "Spike filter removes isolated jumps. 4 is a good default.\n"
            "Selbsttest value is read from controller API."
        )
        note.setStyleSheet("color:#666;")
        layout.addWidget(note)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _update_filter_state(self) -> None:
        enabled = self.filter_box.isChecked()
        self.filter_strength.setEnabled(enabled)
        self.filter_label.setEnabled(enabled)
        self.filter_reset.setEnabled(enabled)

    def value(self) -> DmsSettings:
        tare_enabled = self.tare_box.isChecked()
        tare_value = self._tare_value if self._initial_tare_enabled and tare_enabled else 0.0
        if tare_enabled and not self._initial_tare_enabled and not math.isnan(self._current_value):
            tare_value = self._current_value
        return DmsSettings(
            enabled=self.enable_box.isChecked(),
            frequency_hz=self.freq_combo.currentText(),
            k_factor=float(self.k_factor.float_value()),
            tare_enabled=tare_enabled,
            tare_value=float(tare_value),
            spike_filter_enabled=self.filter_box.isChecked(),
            spike_filter_strength=int(self.filter_strength.value()),
        )


class Ain2SettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, settings: Ain2Settings, current_value: float,
                 current_unit: str) -> None:
        super().__init__(parent)
        _apply_window_icon(self)
        self._initial_tare_enabled = settings.tare_enabled
        self._current_value = current_value
        self._current_unit = current_unit
        self._tare_value = settings.tare_value
        self.setWindowTitle("Channel 2 settings")
        self.setModal(True)
        self.resize(480, 420)

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

        self.tare_box = QtWidgets.QCheckBox("Offset zero (tare current value)")
        self.tare_box.setChecked(settings.tare_enabled)
        form.addRow(self.tare_box)

        tare_hint = "--" if math.isnan(current_value) else f"Current: {current_value:.4f} {current_unit}"
        self.tare_hint = QtWidgets.QLabel(tare_hint)
        self.tare_hint.setStyleSheet("color:#666;")
        layout.addWidget(self.tare_hint)

        self.filter_box = QtWidgets.QCheckBox("Spike filter")
        self.filter_box.setChecked(settings.spike_filter_enabled)
        form.addRow(self.filter_box)

        filter_row = QtWidgets.QHBoxLayout()
        self.filter_strength = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.filter_strength.setRange(1, 10)
        self.filter_strength.setValue(max(1, min(10, int(settings.spike_filter_strength))))
        self.filter_label = QtWidgets.QLabel(str(self.filter_strength.value()))
        self.filter_reset = QtWidgets.QToolButton()
        self.filter_reset.setText("↺")
        self.filter_reset.setToolTip("Reset to optimal")
        filter_row.addWidget(self.filter_strength, 1)
        filter_row.addWidget(self.filter_label)
        filter_row.addWidget(self.filter_reset)
        form.addRow("Filter strength", filter_row)

        self.mode_combo.currentTextChanged.connect(self._update_visibility)
        self.display_combo.currentTextChanged.connect(self._update_visibility)
        self.filter_box.toggled.connect(self._update_filter_state)
        self.filter_strength.valueChanged.connect(lambda v: self.filter_label.setText(str(v)))
        self.filter_reset.clicked.connect(lambda: self.filter_strength.setValue(4))

        self._update_visibility()
        self._update_filter_state()

        note = QtWidgets.QLabel(
            "Wegsensor -> value shown in selected length units (mm).\n"
            "10V + volts -> value shown in V.\n"
            "10V + scaled -> linear scaling 0..10 V -> selected engineering unit.\n"
            "Spike filter removes isolated jumps. 4 is a good default."
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

    def _update_filter_state(self) -> None:
        enabled = self.filter_box.isChecked()
        self.filter_strength.setEnabled(enabled)
        self.filter_label.setEnabled(enabled)
        self.filter_reset.setEnabled(enabled)

    def _validate_and_accept(self) -> None:
        if self.mode_combo.currentText() == "10v" and self.display_combo.currentText() == "scaled":
            if math.isclose(self.scale_min.value(), self.scale_max.value(), rel_tol=0.0, abs_tol=1e-12):
                QtWidgets.QMessageBox.warning(self, "Invalid scaling", "Scale minimum and maximum must be different.")
                return
        self.accept()

    def value(self) -> Ain2Settings:
        weg_length = float(self.weg_combo.currentText().split()[0])
        tare_enabled = self.tare_box.isChecked()
        tare_value = self._tare_value if self._initial_tare_enabled and tare_enabled else 0.0
        if tare_enabled and not self._initial_tare_enabled and not math.isnan(self._current_value):
            tare_value = self._current_value
        return Ain2Settings(
            enabled=self.enable_box.isChecked(),
            frequency_hz=self.freq_combo.currentText(),
            mode=self.mode_combo.currentText(),
            weg_length_mm=weg_length,
            volt_display=self.display_combo.currentText(),
            scaling_unit=self.unit_combo.currentText(),
            scale_min=float(self.scale_min.value()),
            scale_max=float(self.scale_max.value()),
            tare_enabled=tare_enabled,
            tare_value=float(tare_value),
            spike_filter_enabled=self.filter_box.isChecked(),
            spike_filter_strength=int(self.filter_strength.value()),
        )


class CsvLoggerSettingsDialog(QtWidgets.QDialog):
    def __init__(
            self,
            parent: QtWidgets.QWidget,
            settings: CsvLoggerSettings,
            available: dict[str, bool],
    ) -> None:
        super().__init__(parent)
        _apply_window_icon(self)
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
        general_form.addRow(QtWidgets.QLabel(
            "Gantner CSV DateTime writes local time as YYYY-MM-DD HH:MM:SS.ffffff for better Test.Viewer compatibility."))
        self.include_seq = QtWidgets.QCheckBox("Sequence")
        self.include_seq.setChecked(settings.include_seq)
        general_form.addRow(self.include_seq)
        layout.addWidget(general_box)

        self.dms_box = QtWidgets.QGroupBox("DMS")
        dms_layout = QtWidgets.QVBoxLayout(self.dms_box)
        self.cb_dms_display = QtWidgets.QCheckBox("Displayed value")
        self.cb_dms_display.setChecked(settings.include_dms_display)
        self.cb_dms_mvv = QtWidgets.QCheckBox("Extra mV/V")
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
        dms_display_is_um = self.available.get("dms_display_is_um", False)
        self.dms_box.setVisible(dms_on)
        self.ain2_box.setVisible(ain2_on)
        self.temp_box.setVisible(temp_on)
        self.cb_dms_mvv.setEnabled(dms_display_is_um)
        if dms_display_is_um:
            self.cb_dms_mvv.setText("Extra mV/V")
        else:
            self.cb_dms_mvv.setText("Extra mV/V (not needed while DMS display = mV/V)")
            self.cb_dms_mvv.setChecked(False)
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
    def __init__(
            self,
            parent: QtWidgets.QWidget,
            host: str,
            port: int,
            window_s: float,
            oled_supported: bool = False,
    ) -> None:
        super().__init__(parent)
        _apply_window_icon(self)
        self._oled_supported = oled_supported
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

        action_box = QtWidgets.QGroupBox("Controller actions")
        action_layout = QtWidgets.QVBoxLayout(action_box)

        self.restart_button = QtWidgets.QPushButton("Restart controller")
        action_layout.addWidget(self.restart_button)

        if self._oled_supported:
            self.oled_button = QtWidgets.QPushButton("Activate OLED")
            action_layout.addWidget(self.oled_button)

        layout.addWidget(action_box)

        note = QtWidgets.QLabel(
            "Use these settings for IP, port and time window.\n"
            "Controller actions are available here as well."
        )
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
        _apply_window_icon(self)
        self.setWindowTitle("LoRaSense PC Control")
        self.resize(1400, 900)

        self.settings_store = QtCore.QSettings("OpenAI", "LoRaSensePcControl")
        app_instance = QtWidgets.QApplication.instance()
        self._default_palette = QtGui.QPalette(app_instance.palette()) if app_instance is not None else QtGui.QPalette()
        self._default_style_sheet = app_instance.styleSheet() if app_instance is not None else ""
        self.theme_name = self.settings_store.value("ui/theme", "dark", str)
        if self.theme_name not in {"dark", "light"}:
            self.theme_name = "dark"
        self.host = self.settings_store.value("network/host", "192.168.4.1", str)
        self.port = int(self.settings_store.value("network/port", 3333))
        self.window_s = float(self.settings_store.value("network/window_s", 20.0))
        self.plot1_field = _safe_plot_field(self.settings_store.value("ui/plot1_field", "dms_display", str),
                                            "dms_display")
        self.plot2_field = _safe_plot_field(self.settings_store.value("ui/plot2_field", "ain2_display", str),
                                            "ain2_display")
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
        self.pending_info_message = ""
        self.pending_info_until = 0.0
        self._dms_display_cache: dict[int, tuple[float, str]] = {}
        self._ain2_display_cache: dict[int, tuple[float, str]] = {}

        self._build_ui()
        self._reset_display_processing()
        geometry = self.settings_store.value("ui/geometry")
        if isinstance(geometry, QtCore.QByteArray) and not geometry.isEmpty():
            self.restoreGeometry(geometry)
        self._update_sensor_buttons()
        self.stream_mode.setText("Offline")
        self.stream_target.setText(f"{self.host}:{self.port}")

        self.apply_theme()

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.update_ui)
        self.timer.start(250)

    def _load_dms_settings(self) -> DmsSettings:
        return DmsSettings(
            enabled=self.settings_store.value("dms/enabled", True, bool),
            frequency_hz=self.settings_store.value("dms/frequency_hz", "1000", str),
            k_factor=float(self.settings_store.value("dms/k_factor", 0.0)),
            tare_enabled=self.settings_store.value("dms/tare_enabled", False, bool),
            tare_value=float(self.settings_store.value("dms/tare_value", 0.0)),
            spike_filter_enabled=self.settings_store.value("dms/spike_filter_enabled", False, bool),
            spike_filter_strength=int(self.settings_store.value("dms/spike_filter_strength", 4)),
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
            tare_enabled=self.settings_store.value("ain2/tare_enabled", False, bool),
            tare_value=float(self.settings_store.value("ain2/tare_value", 0.0)),
            spike_filter_enabled=self.settings_store.value("ain2/spike_filter_enabled", False, bool),
            spike_filter_strength=int(self.settings_store.value("ain2/spike_filter_strength", 4)),
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
        self.settings_store.setValue("dms/tare_enabled", self.dms_settings.tare_enabled)
        self.settings_store.setValue("dms/tare_value", self.dms_settings.tare_value)
        self.settings_store.setValue("dms/spike_filter_enabled", self.dms_settings.spike_filter_enabled)
        self.settings_store.setValue("dms/spike_filter_strength", self.dms_settings.spike_filter_strength)
        self.settings_store.setValue("ain2/enabled", self.ain2_settings.enabled)
        self.settings_store.setValue("ain2/frequency_hz", self.ain2_settings.frequency_hz)
        self.settings_store.setValue("ain2/mode", self.ain2_settings.mode)
        self.settings_store.setValue("ain2/weg_length_mm", self.ain2_settings.weg_length_mm)
        self.settings_store.setValue("ain2/volt_display", self.ain2_settings.volt_display)
        self.settings_store.setValue("ain2/scaling_unit", self.ain2_settings.scaling_unit)
        self.settings_store.setValue("ain2/scale_min", self.ain2_settings.scale_min)
        self.settings_store.setValue("ain2/scale_max", self.ain2_settings.scale_max)
        self.settings_store.setValue("ain2/tare_enabled", self.ain2_settings.tare_enabled)
        self.settings_store.setValue("ain2/tare_value", self.ain2_settings.tare_value)
        self.settings_store.setValue("ain2/spike_filter_enabled", self.ain2_settings.spike_filter_enabled)
        self.settings_store.setValue("ain2/spike_filter_strength", self.ain2_settings.spike_filter_strength)
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
        self.settings_store.setValue("ui/theme", getattr(self, "theme_name", "dark"))
        self.settings_store.setValue("ui/geometry", self.saveGeometry())
        self.settings_store.sync()

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(6)

        root.addLayout(self._build_header())

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

    def _build_header(self) -> QtWidgets.QGridLayout:
        top_bar = self._build_top_bar()
        status_bar = self._build_status_bar()

        header = QtWidgets.QGridLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setHorizontalSpacing(10)
        header.setVerticalSpacing(4)
        header.setColumnStretch(0, 1)
        header.setColumnStretch(1, 0)

        header.addLayout(top_bar, 0, 0)
        header.addLayout(status_bar, 1, 0)
        header.addWidget(self.logo_label, 0, 1, 2, 1, QtCore.Qt.AlignTop | QtCore.Qt.AlignRight)
        return header

    def _build_top_bar(self) -> QtWidgets.QHBoxLayout:
        top = QtWidgets.QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.setSpacing(8)

        self.btn_connect = QtWidgets.QPushButton("Connect")
        self.btn_disconnect = QtWidgets.QPushButton("Disconnect")
        self.btn_csv_start = QtWidgets.QPushButton("Start CSV")
        self.btn_csv_stop = QtWidgets.QPushButton("Stop CSV")
        self.btn_csv_settings = QtWidgets.QPushButton("⚙")
        self.btn_theme = QtWidgets.QPushButton()
        self.logo_label = QtWidgets.QLabel()
        self.logo_label.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignTop)
        self.logo_label.setMinimumWidth(180)
        self.logo_label.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)
        self._update_logo_pixmap()

        button_height = 30
        button_width = 110
        for button in (self.btn_connect, self.btn_disconnect, self.btn_csv_start, self.btn_csv_stop):
            button.setFixedSize(button_width, button_height)
        self.btn_csv_settings.setFixedSize(42, button_height)
        self.btn_theme.setFixedSize(42, button_height)

        self.btn_connect.clicked.connect(self.connect_stream)
        self.btn_disconnect.clicked.connect(self.disconnect_stream)
        self.btn_csv_start.clicked.connect(self.start_csv)
        self.btn_csv_stop.clicked.connect(self.stop_csv)
        self.btn_csv_settings.clicked.connect(self.open_csv_settings)
        self.btn_theme.clicked.connect(self.toggle_theme)

        top.addWidget(self.btn_connect)
        top.addWidget(self.btn_disconnect)
        top.addWidget(self.btn_csv_start)
        top.addWidget(self.btn_csv_stop)
        top.addWidget(self.btn_csv_settings)
        top.addWidget(self.btn_theme)
        top.addStretch(1)
        return top

    def _update_logo_pixmap(self) -> None:
        target_height = 46
        pixmap = _mkp_logo_pixmap(getattr(self, "theme_name", "dark"), target_height)
        if pixmap.isNull():
            self.logo_label.clear()
            self.logo_label.hide()
            return

        scaled = pixmap.scaledToHeight(target_height, QtCore.Qt.SmoothTransformation)
        self.logo_label.setPixmap(scaled)
        self.logo_label.show()

    def _build_status_bar(self) -> QtWidgets.QHBoxLayout:
        info = QtWidgets.QHBoxLayout()
        info.setContentsMargins(0, 0, 0, 0)
        info.setSpacing(10)

        self.status_label = QtWidgets.QLabel("disconnected")
        self.status_label.setStyleSheet("padding:4px 10px; border-radius:10px; background:#424242; color:white;")
        self.seq_label = QtWidgets.QLabel("-")
        self.hz_label = QtWidgets.QLabel("-")

        self.csv_label = MarqueeLabel("-")
        self.csv_label.setStyleSheet(
            "MarqueeLabel {"
            "border:1px solid #333; border-radius:10px; "
            "background:transparent; "
            "color:palette(text);"
            "}"
        )

        info.addWidget(QtWidgets.QLabel("Status:"))
        info.addWidget(self.status_label)
        info.addWidget(QtWidgets.QLabel("Last seq:"))
        info.addWidget(self.seq_label)
        info.addWidget(QtWidgets.QLabel("Actual Hz:"))
        info.addWidget(self.hz_label)
        info.addWidget(QtWidgets.QLabel("Info / CSV:"))
        info.addWidget(self.csv_label, 1)
        return info

    def _build_stream_card(self) -> QtWidgets.QGroupBox:
        box = QtWidgets.QGroupBox("Controller")
        outer = QtWidgets.QVBoxLayout(box)
        outer.setSpacing(8)

        top = QtWidgets.QHBoxLayout()
        title = QtWidgets.QLabel("<b>Controller</b>")
        top.addWidget(title)
        top.addStretch(1)

        self.btn_controller_settings = QtWidgets.QPushButton("⚙")
        self.btn_controller_settings.setFixedSize(40, 30)
        self.btn_controller_settings.clicked.connect(self.open_network_settings)
        top.addWidget(self.btn_controller_settings)
        outer.addLayout(top)

        card = QtWidgets.QFrame()
        card.setFrameShape(QtWidgets.QFrame.StyledPanel)
        layout = QtWidgets.QFormLayout(card)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setLabelAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        layout.setFormAlignment(QtCore.Qt.AlignTop)
        layout.setHorizontalSpacing(16)
        layout.setVerticalSpacing(8)

        self.stream_mode = QtWidgets.QLabel("-")
        self.stream_target = QtWidgets.QLabel("-")
        self.ctrl_firmware = QtWidgets.QLabel("-")

        for value_label in (self.stream_mode, self.stream_target, self.ctrl_firmware):
            value_label.setStyleSheet("color:palette(text); font-family:'Courier New', monospace;")

        layout.addRow("Connection", self.stream_mode)
        layout.addRow("Target", self.stream_target)
        layout.addRow("Firmware", self.ctrl_firmware)

        outer.addWidget(card)
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

        outer.addWidget(
            self._sensor_row("DMS", self.dms_value, self.dms_meta, self.open_dms_settings, include_button=True))
        outer.addWidget(self._sensor_row("Channel 2", self.ain2_value, self.ain2_meta, self.open_ain2_settings,
                                         include_button=True))
        outer.addWidget(self._sensor_row("Temperature", self.temp_value, self.temp_meta, self.open_temp_settings,
                                         include_button=True))
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
            btn.setFixedSize(40, 30)
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
        for field in PLOT_FIELDS:
            combo.addItem(_plot_field_label(field), field)
        stored_field = self.plot1_field if index == 1 else self.plot2_field
        current_field = _safe_plot_field(stored_field, default_field)
        current_index = combo.findData(current_field)
        if current_index >= 0:
            combo.setCurrentIndex(current_index)
        combo.setMaximumWidth(190)
        combo.currentIndexChanged.connect(self._on_plot_selection_changed)
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
        self.plot1_field = self.combo1.currentData() if hasattr(self, "combo1") else "dms_display"
        self.plot2_field = self.combo2.currentData() if hasattr(self, "combo2") else "ain2_display"
        self._save_local_settings()

    def toggle_theme(self) -> None:
        self.theme_name = "light" if self.theme_name == "dark" else "dark"
        self.apply_theme()
        self._save_local_settings()

    def apply_theme(self) -> None:
        app = QtWidgets.QApplication.instance()
        if app is None:
            return

        if self.theme_name == "light":
            app.setPalette(_make_light_palette(app.style()))
            app.setStyleSheet(LIGHT_THEME_STYLESHEET)
        else:
            app.setPalette(QtGui.QPalette(self._default_palette))
            app.setStyleSheet(self._default_style_sheet)

        if hasattr(self, "btn_theme"):
            is_dark = self.theme_name == "dark"
            self.btn_theme.setText("🌙" if is_dark else "☀️")
            self.btn_theme.setToolTip("Dark mode" if is_dark else "Light mode")
        if hasattr(self, "logo_label"):
            self._update_logo_pixmap()

        self._apply_plot_theme()
        self._apply_status_style(self.receiver.status)
        self.csv_label.setStyleSheet(
            "MarqueeLabel {"
            "border:1px solid #333; border-radius:10px; "
            "background:transparent; "
            "color:palette(text);"
            "}"
        )

    def _apply_status_style(self, status: str) -> None:
        status_text = {
            "connected": "Connected",
            "connecting": "Connecting",
            "disconnected": "Offline",
            "server_closed": "Closed",
        }.get(status, "Error" if status.startswith("error") else status)
        self.status_label.setText(status_text)
        color = STREAM_STATUS_COLORS.get(status, "#7b1f1f" if status.startswith("error") else "#455a64")
        self.status_label.setStyleSheet(
            f"padding:4px 10px; border-radius:10px; background:{color}; color:white;"
        )

    def _apply_plot_theme(self) -> None:
        light = self.theme_name == "light"
        background = "#ffffff" if light else "#111111"
        axis = "#111111" if light else "#9fb3c8"
        grid = (17, 17, 17, 55) if light else (255, 255, 255, 35)
        title_color = axis

        for plot in (getattr(self, "plot1", None), getattr(self, "plot2", None)):
            if plot is None:
                continue
            plot.setBackground(background)
            plot.showGrid(x=True, y=True, alpha=0.25)
            plot.getAxis("left").setTextPen(axis)
            plot.getAxis("bottom").setTextPen(axis)
            plot.getAxis("left").setPen(axis)
            plot.getAxis("bottom").setPen(axis)
            plot.getPlotItem().titleLabel.item.setDefaultTextColor(pg.mkColor(title_color))
            for axis_name in ("left", "bottom"):
                axis_item = plot.getAxis(axis_name)
                axis_item.setGrid(55 if light else 35)
            plot.getPlotItem().getViewBox().setBorder(pg.mkPen(None))

    def _extract_selftest_value(self) -> str:
        state = self.remote_state if isinstance(self.remote_state, dict) else {}
        candidates = [
            "selftest",
            "self_test",
            "selbsttest",
            "selftest_value",
            "self_test_value",
            "selbsttest_value",
        ]
        for key in candidates:
            if key in state:
                value = state.get(key)
                if value is None:
                    continue
                if isinstance(value, float):
                    if math.isnan(value):
                        continue
                    return f"{value:.6f}".rstrip("0").rstrip(".")
                return str(value)
        return "-"

    def _selftest_api_supported(self) -> bool:
        state = self.remote_state if isinstance(self.remote_state, dict) else {}
        candidates = [
            "selftest_run_supported",
            "self_test_run_supported",
            "selbsttest_run_supported",
            "selftest_supported",
            "self_test_supported",
            "selbsttest_supported",
        ]
        for key in candidates:
            if key in state:
                return bool(state.get(key))
        return False

    def _oled_api_supported(self) -> bool:
        state = self.remote_state if isinstance(self.remote_state, dict) else {}
        candidates = [
            "oled_activate_supported",
            "oled_supported",
            "display_activate_supported",
            "activate_oled_supported",
        ]
        for key in candidates:
            if key in state:
                return bool(state.get(key))
        return False

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

    def restart_controller(self) -> None:
        if not self._is_connected():
            QtWidgets.QMessageBox.information(self, "Controller restart", "Connect to controller first.")
            return
        try:
            response = requests.post(f"http://{self.host}/api/reset", timeout=HTTP_TIMEOUT_S)
            response.raise_for_status()
            self._set_info_message("Controller restart requested.")
        except Exception as exc:
            QtWidgets.QMessageBox.warning(
                self,
                "Controller restart",
                f"Restart request failed.\n\n{exc}",
            )

    def activate_controller_oled(self) -> None:
        if not self._is_connected():
            QtWidgets.QMessageBox.information(self, "OLED activation", "Connect to controller first.")
            return
        try:
            response = requests.post(f"http://{self.host}/api/oled", timeout=HTTP_TIMEOUT_S)
            if response.ok:
                self._set_info_message("OLED activation requested.")
                return
            raise requests.HTTPError(f"{response.status_code} {response.reason}")
        except Exception as exc:
            QtWidgets.QMessageBox.information(
                self,
                "OLED activation",
                "OLED activation API is not available yet on controller.\n\n"
                f"{exc}",
            )

    def open_dms_settings(self) -> None:
        current_value = float("nan")
        current_unit = "um/m" if self.dms_settings.k_factor > 0 else "mV/V"
        samples = list(self.receiver.samples)
        if samples:
            current_value, current_unit = self.compute_dms_display(samples[-1])
        dlg = DmsSettingsDialog(
            self,
            self.dms_settings,
            current_value,
            current_unit,
            selftest_value=self._extract_selftest_value(),
            selftest_supported=self._selftest_api_supported(),
        )
        if hasattr(dlg, "selftest_button"):
            dlg.selftest_button.clicked.connect(lambda: self.run_dms_selftest(dlg))
        if dlg.exec() == QtWidgets.QDialog.Accepted:
            self.dms_settings = dlg.value()
            self._reset_display_processing()
            self._rebuild_display_caches()
            self._save_local_settings()
            self._update_sensor_buttons()
            self.refresh_plots()
            self.push_config_to_device(show_success=True)

    def run_dms_selftest(self, dialog: DmsSettingsDialog | None = None) -> None:
        if not self._is_connected():
            QtWidgets.QMessageBox.information(self, "Selbsttest", "Connect to controller first.")
            return
        try:
            response = requests.post(f"http://{self.host}/api/selftest", timeout=HTTP_TIMEOUT_S)
            if response.ok:
                data = response.json() if response.content else {}
                if isinstance(data, dict):
                    self.remote_state.update(data)
                value = self._extract_selftest_value()
                if dialog is not None:
                    dialog.selftest_value_label.setText(value)
                self._set_info_message(f"Selbsttest finished: {value}")
                return
            raise requests.HTTPError(f"{response.status_code} {response.reason}")
        except Exception as exc:
            QtWidgets.QMessageBox.information(
                self,
                "Selbsttest",
                "Manual Selbsttest API is not available yet on controller.\n\n"
                f"{exc}",
            )

    def open_ain2_settings(self) -> None:
        current_value = float("nan")
        current_unit = "mm" if self.ain2_settings.mode == "weg" else (
            self.ain2_settings.scaling_unit if self.ain2_settings.volt_display == "scaled" else "V")
        samples = list(self.receiver.samples)
        if samples:
            current_value, current_unit = self.compute_ain2_display(samples[-1])
        dlg = Ain2SettingsDialog(self, self.ain2_settings, current_value, current_unit)
        if dlg.exec() == QtWidgets.QDialog.Accepted:
            self.ain2_settings = dlg.value()
            self._reset_display_processing()
            self._rebuild_display_caches()
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
        dlg = NetworkSettingsDialog(
            self,
            self.host,
            self.port,
            self.window_s,
            oled_supported=self._oled_api_supported(),
        )
        dlg.restart_button.clicked.connect(self.restart_controller)
        if hasattr(dlg, "oled_button"):
            dlg.oled_button.clicked.connect(self.activate_controller_oled)
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
            "dms_display_is_um": self.dms_settings.k_factor > 0,
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
        elif self.dms_settings.k_factor <= 0:
            settings.include_dms_mvv = False
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

    def _dms_display_uses_um_per_m(self) -> bool:
        return self.dms_settings.k_factor > 0

    def _make_csv_specs(self) -> list[tuple[str, Any]]:
        settings = self._coerce_csv_settings(CsvLoggerSettings(**self.csv_settings.__dict__))
        specs: list[tuple[str, Any]] = []
        time_format = settings.time_format
        time_header = {
            "gantner_timecounter": "time_gantner_datetime",
            "iso8601": "time_iso8601",
            "unix": "time_unix",
        }.get(time_format, "time_gantner_timecounter")
        specs.append(
            (time_header, lambda s, tf=time_format: self._format_time_for_csv(self._absolute_sample_time(s), tf)))
        if settings.include_seq:
            specs.append(("seq", lambda s: s.seq))

        if self.dms_settings.enabled:
            dms_display_is_um = self._dms_display_uses_um_per_m()
            if settings.include_dms_display:
                unit = "um_per_m" if dms_display_is_um else "mV_per_V"
                specs.append((self._header_from_unit("dms_display", unit), lambda s: self.compute_dms_display(s)[0]))
            if settings.include_dms_mvv and dms_display_is_um:
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
                specs.append(
                    (self._header_from_unit("ain2_display", display_unit), lambda s: self.compute_ain2_display(s)[0]))
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

    def _is_connected(self) -> bool:
        return self.receiver.status == "connected"

    def _set_info_message(self, message: str, duration_s: float = 6.0) -> None:
        self.pending_info_message = message.strip()
        self.pending_info_until = time.time() + max(0.0, duration_s) if self.pending_info_message else 0.0
        self._refresh_info_label()

    def _clear_info_message(self) -> None:
        self.pending_info_message = ""
        self.pending_info_until = 0.0

    def _refresh_info_label(self) -> None:
        if self.pending_info_message and self.pending_info_until > 0 and time.time() >= self.pending_info_until:
            self._clear_info_message()

        parts: list[str] = []
        if self.logger.path:
            parts.append(self.logger.path)
        if self.pending_info_message:
            parts.append(self.pending_info_message)
        self.csv_label.setText(" | ".join(parts) if parts else "-")

    def _set_controller_stream_mode(self, enabled: bool) -> None:
        response = requests.post(
            f"http://{self.host}/api/stream",
            json={"enabled": int(enabled)},
            timeout=HTTP_TIMEOUT_S,
        )
        response.raise_for_status()

    def connect_stream(self) -> None:
        self._clear_info_message()
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
        self._clear_info_message()
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
        self._clear_info_message()
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
            self._refresh_info_label()
            self.last_logged_seq = None
            self._save_local_settings()

    def stop_csv(self) -> None:
        self._clear_info_message()
        self.logger.stop()
        self.csv_specs = []
        self._refresh_info_label()
        self.last_logged_seq = None

    def read_config(self) -> None:
        self._clear_info_message()
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
        if not self._is_connected():
            if show_success:
                self._set_info_message("Settings saved locally. They will be sent after connect.")
            return
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
        if not self._is_connected():
            self.remote_state = {}
            return self.remote_state
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
        self._apply_status_style(status)

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
                self._cache_processed_sample(sample)
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
            if not self._is_connected():
                self.ctrl_firmware.setText("-")

        self.hz_label.setText(f"{self.actual_hz():.1f}")
        self._refresh_info_label()
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

    def _reset_display_processing(self) -> None:
        self._dms_display_cache.clear()
        self._ain2_display_cache.clear()
        self._dms_filter_state = {"last_raw": float("nan"), "last_filtered": float("nan"), "noise": 0.0,
                                  "pending_sign": 0}
        self._ain2_filter_state = {"last_raw": float("nan"), "last_filtered": float("nan"), "noise": 0.0,
                                   "pending_sign": 0}

    def _rebuild_display_caches(self) -> None:
        self._reset_display_processing()
        for sample in list(self.receiver.samples):
            self._cache_processed_sample(sample)

    def _base_dms_display(self, sample: Sample) -> tuple[float, str]:
        if not sample.dms_on or not sample.dms_valid or math.isnan(sample.dms_mV_per_V):
            return float("nan"), ("um/m" if self.dms_settings.k_factor > 0 else "mV/V")
        if self.dms_settings.k_factor > 0:
            return (sample.dms_mV_per_V / self.dms_settings.k_factor) * 1000.0, "um/m"
        return sample.dms_mV_per_V, "mV/V"

    def _base_ain2_display(self, sample: Sample) -> tuple[float, str]:
        unit = "mm" if self.ain2_settings.mode == "weg" else (
            self.ain2_settings.scaling_unit if self.ain2_settings.volt_display == "scaled" else "V")
        if not sample.ain2_on or not sample.ain2_valid or math.isnan(sample.ain2_value):
            return float("nan"), unit
        if self.ain2_settings.mode == "weg":
            return sample.ain2_value, "mm"
        if self.ain2_settings.volt_display == "scaled":
            span = self.ain2_settings.scale_max - self.ain2_settings.scale_min
            value = self.ain2_settings.scale_min + (sample.ain2_value / 10.0) * span
            return value, self.ain2_settings.scaling_unit
        return sample.ain2_value, "V"

    def _apply_spike_filter(self, value: float, state: dict[str, Any], enabled: bool, strength: int) -> float:
        if math.isnan(value):
            state["pending_sign"] = 0
            return float("nan")
        if not enabled:
            state["last_raw"] = value
            state["last_filtered"] = value
            state["pending_sign"] = 0
            return value
        last_filtered = state.get("last_filtered", float("nan"))
        last_raw = state.get("last_raw", float("nan"))
        if math.isnan(last_filtered) or math.isnan(last_raw):
            state["last_raw"] = value
            state["last_filtered"] = value
            state["noise"] = 0.0
            state["pending_sign"] = 0
            return value
        step = abs(value - last_raw)
        noise = state.get("noise", 0.0)
        noise = (noise * 0.85) + (step * 0.15)
        state["noise"] = noise
        threshold = max(0.0005, noise * (12.0 - max(1, min(10, strength))) + 0.001)
        delta = value - last_filtered
        if abs(delta) <= threshold:
            state["pending_sign"] = 0
            filtered = value
        else:
            sign = 1 if delta > 0 else -1
            if state.get("pending_sign", 0) == sign:
                state["pending_sign"] = 0
                filtered = value
            else:
                state["pending_sign"] = sign
                filtered = last_filtered
        state["last_raw"] = value
        state["last_filtered"] = filtered
        return filtered

    def _cache_processed_sample(self, sample: Sample) -> None:
        dms_value, dms_unit = self._base_dms_display(sample)
        dms_value = self._apply_spike_filter(
            dms_value,
            self._dms_filter_state,
            self.dms_settings.spike_filter_enabled,
            self.dms_settings.spike_filter_strength,
        )
        if not math.isnan(dms_value) and self.dms_settings.tare_enabled:
            dms_value -= self.dms_settings.tare_value
        self._dms_display_cache[sample.seq] = (dms_value, dms_unit)

        ain2_value, ain2_unit = self._base_ain2_display(sample)
        ain2_value = self._apply_spike_filter(
            ain2_value,
            self._ain2_filter_state,
            self.ain2_settings.spike_filter_enabled,
            self.ain2_settings.spike_filter_strength,
        )
        if not math.isnan(ain2_value) and self.ain2_settings.tare_enabled:
            ain2_value -= self.ain2_settings.tare_value
        self._ain2_display_cache[sample.seq] = (ain2_value, ain2_unit)

    def compute_dms_display(self, sample: Sample) -> tuple[float, str]:
        cached = self._dms_display_cache.get(sample.seq)
        if cached is not None:
            return cached
        self._cache_processed_sample(sample)
        return self._dms_display_cache.get(sample.seq, (float("nan"), "mV/V"))

    def compute_ain2_display(self, sample: Sample) -> tuple[float, str]:
        cached = self._ain2_display_cache.get(sample.seq)
        if cached is not None:
            return cached
        self._cache_processed_sample(sample)
        return self._ain2_display_cache.get(sample.seq, (float("nan"), "V"))

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
        self.draw_plot(self.plot1, samples, self.combo1.currentData())
        self.draw_plot(self.plot2, samples, self.combo2.currentData())

    def draw_plot(self, plot: pg.PlotWidget, samples: list[Sample], field: str) -> None:
        plot.clear()
        if not samples:
            plot.setTitle(_plot_field_label(field))
            return

        window_s = max(1.0, float(self.window_s))

        label_unit = self._field_value(samples[-1], field)[1]
        pretty_name = _plot_field_label(field)
        plot.setTitle(f"{pretty_name} [{label_unit}]")
        plot.setLabel("bottom", "Time relative to latest sample, s")
        plot.setLabel("left", label_unit)

        time_key = (lambda s: s.t_ctrl) if len(samples) >= 2 and samples[-1].t_ctrl > samples[0].t_ctrl else (
            lambda s: s.t_pc)
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
            pen_color = '#1f4d7a' if self.theme_name == 'light' else None
            plot.plot(xs, ys, pen=pg.mkPen(color=pen_color, width=1.8) if pen_color else pg.mkPen(width=1.8))
            plot.enableAutoRange(axis=pg.ViewBox.XYAxes, enable=True)
            plot.autoRange()

    def closeEvent(self, event: QtGui.QCloseEvent | QtCore.QEvent) -> None:  # type: ignore[name-defined]
        self._save_local_settings()
        self.logger.stop()
        self.receiver.stop()
        super().closeEvent(event)


def main() -> None:
    _set_windows_app_id()

    theme_name = _read_saved_theme_name()
    app = QtWidgets.QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(True)

    app_icon = _load_app_icon()
    if not app_icon.isNull():
        app.setWindowIcon(app_icon)

    pg.setConfigOptions(antialias=False)

    splash = _create_splash(theme_name, app_icon)
    splash.show()
    splash.showMessage(
        "Preparing workspace…",
        QtCore.Qt.AlignBottom | QtCore.Qt.AlignHCenter,
        QtGui.QColor("#dfe6ee") if theme_name == "dark" else QtGui.QColor("#3c4954"),
    )
    app.processEvents()

    win = App()
    win.setVisible(False)
    if hasattr(win, "apply_theme"):
        win.apply_theme()

    def _finish_startup() -> None:
        splash.finish(win)
        win.show()
        win.raise_()
        win.activateWindow()

    QtCore.QTimer.singleShot(120, _finish_startup)
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
