from __future__ import annotations

from PySide6 import QtWidgets
import pyqtgraph as pg

from nemidaq_theme import set_plot_theme


def build_live_plot(tab, *, container: QtWidgets.QWidget, dark: bool = True) -> None:
    """Build the live plot into `container` and attach plot objects to `tab`.

    Mutates `tab` by setting: plot, pen1, pen2, curve1, curve2.
    """
    pl = QtWidgets.QVBoxLayout(container)

    tab.plot = pg.PlotWidget()
    tab.plot.showGrid(x=True, y=True, alpha=0.20)
    tab.plot.setLabel("bottom", "Timecounter", "s")
    tab.plot.setLabel("left", "mV/V")

    # ===== ADD LEGEND =====
    tab.plot.addLegend(offset=(10, 10))

    pl.addWidget(tab.plot)

    set_plot_theme(tab.plot, dark=bool(dark))

    tab.pen1 = pg.mkPen((0, 220, 0), width=2)
    tab.pen2 = pg.mkPen((220, 0, 0), width=2)

    tab.curve1 = tab.plot.plot([], [], pen=tab.pen1, name="Channel 1")
    tab.curve2 = tab.plot.plot([], [], pen=tab.pen2, name="Channel 2")


def apply_live_plot_theme(tab, *, dark: bool) -> None:
    if hasattr(tab, "plot"):
        set_plot_theme(tab.plot, dark=bool(dark))