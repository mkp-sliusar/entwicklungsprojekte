from __future__ import annotations

import time
from typing import Any

from smbus2 import SMBus


class PiSugar:
    STATUS_REGISTER = 0x02
    BATTERY_VOLTAGE_HIGH_REGISTER = 0x22
    BATTERY_VOLTAGE_LOW_REGISTER = 0x23
    BATTERY_PERCENT_REGISTER = 0x2A

    def __init__(self, bus_id: int = 1, address: int = 0x57):
        self.bus = SMBus(int(bus_id))
        self.address = int(address)

    @classmethod
    def detect(cls, bus_id: int = 1, address: int = 0x57) -> bool:
        try:
            with SMBus(int(bus_id)) as bus:
                bus.read_byte_data(int(address), cls.STATUS_REGISTER)
                bus.read_byte_data(int(address), cls.BATTERY_PERCENT_REGISTER)
            return True
        except Exception:
            return False

    def read(self) -> dict[str, Any]:
        timestamp = time.time()
        try:
            status = self.bus.read_byte_data(self.address, self.STATUS_REGISTER)
            voltage_high = self.bus.read_byte_data(
                self.address,
                self.BATTERY_VOLTAGE_HIGH_REGISTER,
            )
            voltage_low = self.bus.read_byte_data(
                self.address,
                self.BATTERY_VOLTAGE_LOW_REGISTER,
            )
            battery_percent = self.bus.read_byte_data(
                self.address,
                self.BATTERY_PERCENT_REGISTER,
            )
        except Exception:
            return {
                "error": "pisugar_error",
                "timestamp": timestamp,
            }

        battery_voltage_mv = (voltage_high << 8) | voltage_low
        return {
            "battery_voltage_v": battery_voltage_mv / 1000.0,
            "battery_percent": max(0.0, min(float(battery_percent), 100.0)),
            "external_power": bool(status & 0x80),
            "charging_enabled": bool(status & 0x40),
            "output_enabled": bool(status & 0x04),
            "timestamp": timestamp,
        }

    def close(self) -> None:
        try:
            self.bus.close()
        except Exception:
            pass