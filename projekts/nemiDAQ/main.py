from __future__ import annotations

import os
import sys

from PySide6 import QtCore, QtWidgets
from PySide6.QtGui import QIcon

from live_tab import LiveTab
from nemidaq_theme import apply_theme
from offline_tab import OfflineTab


class AppMainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()

        # App icon
        icon_path = os.path.join(os.path.dirname(__file__), "resources", "nemidaq.ico")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        self.setWindowTitle("nemiDAQ: Live Plotter")
        self.resize(1500, 860)

        self._dark = True

        # tabs
        self.tabs = QtWidgets.QTabWidget()
        self.live_tab = LiveTab(self)
        self.offline_tab = OfflineTab(self)

        self._live_idx = self.tabs.addTab(self.live_tab, "Live")
        self._offline_idx = self.tabs.addTab(self.offline_tab, "Offline decoder")

        # Hide decoder tab by default (can be enabled in Settings)
        try:
            self.tabs.setTabVisible(self._offline_idx, False)
        except Exception:
            # Fallback if per-tab visibility isn't supported
            pass

        self.setCentralWidget(self.tabs)

        # apply theme
        self._apply_theme()

    def is_dark_theme(self) -> bool:
        return bool(self._dark)

    @QtCore.Slot(bool)
    def set_dark_theme(self, enabled: bool) -> None:
        self._dark = bool(enabled)
        self._apply_theme()

    def is_offline_visible(self) -> bool:
        try:
            return bool(self.tabs.isTabVisible(self._offline_idx))
        except Exception:
            return True

    @QtCore.Slot(bool)
    def set_offline_visible(self, visible: bool) -> None:
        visible = bool(visible)
        try:
            self.tabs.setTabVisible(self._offline_idx, visible)
            if not visible and self.tabs.currentIndex() == self._offline_idx:
                self.tabs.setCurrentIndex(self._live_idx)
        except Exception:
            pass

    def _apply_theme(self) -> None:
        app = QtWidgets.QApplication.instance()
        if app is not None:
            apply_theme(app, dark=self._dark)

        # per-tab plot updates
        self.live_tab.apply_theme(self._dark)
        self.offline_tab.apply_theme(self._dark)


def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")

    icon_path = os.path.join(os.path.dirname(__file__), "resources", "nemidaq.ico")
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))

    win = AppMainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
