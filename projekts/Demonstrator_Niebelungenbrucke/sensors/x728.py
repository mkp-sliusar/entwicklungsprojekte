from __future__ import annotations

import time
from typing import Any

from smbus2 import SMBus


class X728:
    VCELL_REGISTER = 0x02
    SOC_REGISTER = 0x04

    def __init__(self, bus_id: int = 1, address: int = 0x36):
        self.bus = SMBus(int(bus_id))
        self.address = int(address)

    @classmethod
    def detect(cls, bus_id: int = 1, address: int = 0x36) -> bool:
        try:
            with SMBus(int(bus_id)) as bus:
                bus.read_word_data(int(address), cls.VCELL_REGISTER)
                bus.read_word_data(int(address), cls.SOC_REGISTER)
            return True
        except Exception:
            return False

    @staticmethod
    def _swap_word(value: int) -> int:
        return ((value & 0xFF) << 8) | (value >> 8)

    def read(self) -> dict[str, Any]:
        timestamp = time.time()
        try:
            raw_voltage = self.bus.read_word_data(self.address, self.VCELL_REGISTER)
            raw_soc = self.bus.read_word_data(self.address, self.SOC_REGISTER)
        except Exception:
            return {
                "error": "x728_error",
                "timestamp": timestamp,
            }

        voltage_word = self._swap_word(raw_voltage)
        soc_word = self._swap_word(raw_soc)
        battery_voltage_v = voltage_word * 1.25 / 1000.0 / 16.0
        battery_percent = max(0.0, min(soc_word / 256.0, 100.0))

        return {
            "battery_voltage_v": battery_voltage_v,
            "battery_percent": battery_percent,
            "timestamp": timestamp,
        }

    def close(self) -> None:
        try:
            self.bus.close()
        except Exception:
            pass