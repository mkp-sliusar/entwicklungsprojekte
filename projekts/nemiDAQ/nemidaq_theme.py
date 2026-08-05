from __future__ import annotations

from PySide6 import QtGui, QtWidgets
import pyqtgraph as pg
from PySide6.QtGui import QAction




DARK_QSS = """
QWidget { background-color: #1e1e1e; color: #e0e0e0; font-size: 12px; }
QFrame { background-color: #1e1e1e; }
QPushButton { background-color: #2b2b2b; border: 1px solid #3a3a3a; padding: 6px; }
QPushButton:hover { background-color: #353535; }
QPushButton:disabled { color: #888; }
QGroupBox { border: 1px solid #3a3a3a; margin-top: 10px; padding: 6px; }
QGroupBox:title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 4px; }
QPlainTextEdit { background-color: #111; color: #ddd; border: 1px solid #333; }
QLineEdit { background-color: #111; color: #ddd; border: 1px solid #333; padding: 2px; }
QComboBox {
    background-color: #2b2b2b; border: 1px solid #3a3a3a; padding: 2px;
    selection-background-color: #444;
}
QSpinBox, QDoubleSpinBox {
    background-color: #2b2b2b; border: 1px solid #3a3a3a;
    padding: 2px 34px 2px 6px; /* ensure arrows are clickable */
    selection-background-color: #444;
}
QCheckBox { padding: 2px; }
QTabWidget::pane { border: 1px solid #333; }
QTabBar::tab { background: #2b2b2b; padding: 6px; border: 1px solid #333; }
QTabBar::tab:selected { background: #353535; }
"""

LIGHT_QSS = """
QWidget { background-color: #f5f5f5; color: #111; font-size: 12px; }
QFrame { background-color: #f5f5f5; }
QPushButton { background-color: #ffffff; border: 1px solid #cfcfcf; padding: 6px; }
QPushButton:hover { background-color: #f0f0f0; }
QPushButton:disabled { color: #888; }
QGroupBox { border: 1px solid #cfcfcf; margin-top: 10px; padding: 6px; }
QGroupBox:title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 4px; }
QPlainTextEdit { background-color: #ffffff; color: #111; border: 1px solid #cfcfcf; }
QLineEdit { background-color: #ffffff; color: #111; border: 1px solid #cfcfcf; padding: 2px; }
QComboBox {
    background-color: #ffffff; border: 1px solid #cfcfcf; padding: 2px;
    selection-background-color: #d8eaff;
}
QSpinBox, QDoubleSpinBox {
    background-color: #ffffff; border: 1px solid #cfcfcf;
    padding: 2px 34px 2px 6px; /* ensure arrows are clickable */
    selection-background-color: #d8eaff;
}
QCheckBox { padding: 2px; }
QTabWidget::pane { border: 1px solid #cfcfcf; }
QTabBar::tab { background: #ffffff; padding: 6px; border: 1px solid #cfcfcf; }
QTabBar::tab:selected { background: #eaeaea; }
"""


def apply_theme(app: QtWidgets.QApplication, dark: bool) -> None:
    """
    Applies a global QSS + pyqtgraph default colors.
    Per-PlotWidget fine-tuning can still be done inside tabs.
    """
    if dark:
        app.setStyleSheet(DARK_QSS)
        pg.setConfigOption("background", (0, 0, 0))
        pg.setConfigOption("foreground", (220, 220, 220))
    else:
        app.setStyleSheet(LIGHT_QSS)
        pg.setConfigOption("background", (255, 255, 255))
        pg.setConfigOption("foreground", (20, 20, 20))

    pg.setConfigOptions(antialias=True)


def set_plot_theme(plot: pg.PlotWidget, dark: bool) -> None:
    # explicit plot background (ensures already-created widgets update)
    plot.setBackground((0, 0, 0) if dark else (255, 255, 255))

    # grid / labels will follow pg foreground, but left axis SI prefix should stay off
    plot.getAxis("left").enableAutoSIPrefix(False)
    plot.getAxis("bottom").enableAutoSIPrefix(False)
