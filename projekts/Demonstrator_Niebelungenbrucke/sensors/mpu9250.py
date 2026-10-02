from __future__ import annotations

from collections import deque
import math
import time

from smbus2 import SMBus


class MPU9250:
    WHO_AM_I = 0x75
    MPU9250_ID = 0x71
    PWR_MGMT_1 = 0x6B
    CONFIG = 0x1A
    SMPLRT_DIV = 0x19
    ACCEL_CONFIG = 0x1C
    ACCEL_XOUT_H = 0x3B

    DLPF_MAP = {
        250: 0x00,
        184: 0x01,
        92: 0x02,
        41: 0x03,
        20: 0x04,
        10: 0x05,
        5: 0x06,
    }

    ACCEL_SCALE_MAP = {
        2: (0x00, 16384.0),
        4: (0x08, 8192.0),
        8: (0x10, 4096.0),
        16: (0x18, 2048.0),
    }

    def __init__(
        self,
        bus_id: int = 1,
        address: int = 0x68,
        sample_rate_hz: float = 100.0,
        dlpf_hz: int = 184,
        accel_scale_g: int = 2,
        vibration_highpass_alpha: float = 0.05,
        roll_pitch_median_window: int = 1,
    ):
        self.address = int(address)
        self.bus = SMBus(int(bus_id))
        try:
            identity = self.bus.read_byte_data(self.address, self.WHO_AM_I)
            if identity != self.MPU9250_ID:
                raise RuntimeError(
                    f"unexpected MPU9250 WHO_AM_I 0x{identity:02x} "
                    f"at I2C address 0x{self.address:02x}"
                )
        except Exception:
            self.bus.close()
            raise

        self.sample_rate_hz = float(sample_rate_hz)
        self.vibration_highpass_alpha = float(vibration_highpass_alpha)
        self.az_lp = 1.0

        accel_scale_g = int(accel_scale_g)
        accel_reg, self.accel_lsb_per_g = self.ACCEL_SCALE_MAP.get(accel_scale_g, self.ACCEL_SCALE_MAP[2])

        median_window = max(int(roll_pitch_median_window), 1)
        self.roll_buffer = deque(maxlen=median_window)
        self.pitch_buffer = deque(maxlen=median_window)

        self.bus.write_byte_data(self.address, self.PWR_MGMT_1, 0x00)
        time.sleep(0.05)

        dlpf_cfg = self.DLPF_MAP.get(int(dlpf_hz), self.DLPF_MAP[184])
        self.bus.write_byte_data(self.address, self.CONFIG, dlpf_cfg)

        # With DLPF enabled, internal output rate is 1000 Hz. SMPLRT_DIV=1 gives 500 Hz.
        divider = int(round(1000.0 / max(self.sample_rate_hz, 1.0)) - 1)
        divider = max(0, min(255, divider))
        self.bus.write_byte_data(self.address, self.SMPLRT_DIV, divider)
        self.bus.write_byte_data(self.address, self.ACCEL_CONFIG, accel_reg)

    @staticmethod
    def _word(high: int, low: int) -> int:
        value = (int(high) << 8) | int(low)
        if value > 32767:
            value -= 65536
        return value

    @staticmethod
    def _median(values: deque[float]) -> float:
        ordered = sorted(values)
        return ordered[len(ordered) // 2]

    def read(self) -> dict[str, float]:
        block = self.bus.read_i2c_block_data(self.address, self.ACCEL_XOUT_H, 14)

        ax = self._word(block[0], block[1]) / self.accel_lsb_per_g
        ay = self._word(block[2], block[3]) / self.accel_lsb_per_g
        az = self._word(block[4], block[5]) / self.accel_lsb_per_g
        temp_raw = self._word(block[6], block[7])

        temperature = (temp_raw / 333.87) + 21.0

        alpha = max(0.0, min(self.vibration_highpass_alpha, 1.0))
        self.az_lp = ((1.0 - alpha) * self.az_lp) + (alpha * az)
        az_hp = az - self.az_lp
        vibration_z_ms2 = az_hp * 9.81

        roll_deg = math.degrees(math.atan2(ay, az))
        pitch_deg = math.degrees(math.atan2(-ax, math.sqrt((ay * ay) + (az * az))))

        self.roll_buffer.append(roll_deg)
        self.pitch_buffer.append(pitch_deg)
        roll_deg = self._median(self.roll_buffer)
        pitch_deg = self._median(self.pitch_buffer)

        acc_mag_g = math.sqrt((ax * ax) + (ay * ay) + (az * az))
        vibration_g = abs(acc_mag_g - 1.0)
        vibration_ms2 = vibration_g * 9.81

        return {
            "ax": ax,
            "ay": ay,
            "az": az,
            "roll_deg": roll_deg,
            "pitch_deg": pitch_deg,
            "acc_mag_g": acc_mag_g,
            "vibration_ms2": vibration_ms2,
            "vibration_z_ms2": vibration_z_ms2,
            "temperature": temperature,
            "timestamp": time.time(),
        }

    def close(self) -> None:
        try:
            self.bus.close()
        except Exception:
            pass


if __name__ == "__main__":
    sensor = MPU9250(sample_rate_hz=100)
    while True:
        d = sensor.read()
        print(
            f"Roll={d['roll_deg']:.2f} Pitch={d['pitch_deg']:.2f} "
            f"Vib={d['vibration_ms2']:.3f} T={d['temperature']:.1f}"
        )
        time.sleep(0.1)
