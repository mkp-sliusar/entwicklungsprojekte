from __future__ import annotations

from collections import deque
import math
import time

from smbus2 import SMBus


class ADXL345:
    DEVICE_ID = 0x00
    DEVICE_ID_VALUE = 0xE5
    BW_RATE = 0x2C
    POWER_CTL = 0x2D
    DATA_FORMAT = 0x31
    DATA_X0 = 0x32

    DATA_RATE_CODES = {
        6: 0x06,
        12: 0x07,
        25: 0x08,
        50: 0x09,
        100: 0x0A,
        200: 0x0B,
        400: 0x0C,
        800: 0x0D,
        1600: 0x0E,
        3200: 0x0F,
    }
    RANGE_CODES = {
        2: 0x00,
        4: 0x01,
        8: 0x02,
        16: 0x03,
    }

    def __init__(
        self,
        bus_id: int = 1,
        address: int = 0x53,
        sample_rate_hz: float = 200.0,
        accel_scale_g: int = 16,
        full_resolution: bool = True,
        vibration_highpass_alpha: float = 0.05,
        roll_pitch_median_window: int = 1,
    ):
        self.address = int(address)
        self.bus = SMBus(int(bus_id))
        try:
            identity = self.bus.read_byte_data(self.address, self.DEVICE_ID)
            if identity != self.DEVICE_ID_VALUE:
                raise RuntimeError(
                    f"unexpected ADXL345 DEVID 0x{identity:02x} "
                    f"at I2C address 0x{self.address:02x}"
                )

            requested_rate = float(sample_rate_hz)
            self.data_rate_hz = float(
                min(self.DATA_RATE_CODES, key=lambda rate: abs(rate - requested_rate))
            )
            accel_scale_g = int(accel_scale_g)
            if accel_scale_g not in self.RANGE_CODES:
                accel_scale_g = 16
            self.full_resolution = bool(full_resolution)
            self.accel_scale_g = accel_scale_g
            self.accel_lsb_per_g = 256.0 if self.full_resolution else 256.0 / accel_scale_g

            format_reg = self.RANGE_CODES[accel_scale_g]
            if self.full_resolution:
                format_reg |= 0x08
            self.bus.write_byte_data(self.address, self.DATA_FORMAT, format_reg)
            self.bus.write_byte_data(
                self.address,
                self.BW_RATE,
                self.DATA_RATE_CODES[int(self.data_rate_hz)],
            )
            self.bus.write_byte_data(self.address, self.POWER_CTL, 0x08)
            time.sleep(0.01)
        except Exception:
            self.bus.close()
            raise

        self.vibration_highpass_alpha = float(vibration_highpass_alpha)
        self.az_lp = 1.0
        median_window = max(int(roll_pitch_median_window), 1)
        self.roll_buffer = deque(maxlen=median_window)
        self.pitch_buffer = deque(maxlen=median_window)

    @staticmethod
    def _word(low: int, high: int) -> int:
        value = (int(high) << 8) | int(low)
        if value > 32767:
            value -= 65536
        return value

    @staticmethod
    def _median(values: deque[float]) -> float:
        ordered = sorted(values)
        return ordered[len(ordered) // 2]

    def read(self) -> dict[str, float]:
        block = self.bus.read_i2c_block_data(self.address, self.DATA_X0, 6)
        ax = self._word(block[0], block[1]) / self.accel_lsb_per_g
        ay = self._word(block[2], block[3]) / self.accel_lsb_per_g
        az = self._word(block[4], block[5]) / self.accel_lsb_per_g

        alpha = max(0.0, min(self.vibration_highpass_alpha, 1.0))
        self.az_lp = ((1.0 - alpha) * self.az_lp) + (alpha * az)
        vibration_z_ms2 = (az - self.az_lp) * 9.81

        roll_deg = math.degrees(math.atan2(ay, -az))
        pitch_deg = math.degrees(math.atan2(-ax, math.sqrt((ay * ay) + (az * az))))
        self.roll_buffer.append(roll_deg)
        self.pitch_buffer.append(pitch_deg)

        acc_mag_g = math.sqrt((ax * ax) + (ay * ay) + (az * az))
        vibration_ms2 = abs(acc_mag_g - 1.0) * 9.81

        return {
            "ax": ax,
            "ay": ay,
            "az": az,
            "roll_deg": self._median(self.roll_buffer),
            "pitch_deg": self._median(self.pitch_buffer),
            "acc_mag_g": acc_mag_g,
            "vibration_ms2": vibration_ms2,
            "vibration_z_ms2": vibration_z_ms2,
            "timestamp": time.time(),
        }

    def close(self) -> None:
        try:
            self.bus.close()
        except Exception:
            pass