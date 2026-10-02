from __future__ import annotations

import os
import threading
import time
from typing import Any

import RPi.GPIO as GPIO
import spidev


class ADS1220:
    CMD_RESET = 0x06
    CMD_START = 0x08
    CMD_RDATA = 0x10
    CMD_WREG_BASE = 0x40

    ADS_FS = 1 << 23

    DATA_RATE_CODES = {
        20: 0,
        45: 1,
        90: 2,
        175: 3,
        330: 4,
        600: 5,
        1000: 6,
        2000: 7,
    }

    def __init__(
        self,
        spi_bus: int = 0,
        spi_device: int = 0,
        spi_speed_hz: int = 4_000_000,
        spi_mode: int = 1,
        drdy_pin: int = 17,
        reg0: int = 0x0E,
        reg1: int = 0x84,
        reg2: int = 0x00,
        reg3: int = 0x00,
        data_rate_sps: int | None = None,
        continuous_conversion: bool = True,
        start_each_read: bool = False,
        vref: float = 3.3,
        gain: float = 128.0,
        zero_raw: float = 0.0,
        raw_per_gram: float = 185.0,
        gauge_factor: float = 2.0,
        young_modulus_mpa: float = 3500.0,
        tare_file: str = "tare.dat",
        tare_samples: int = 10,
        tare_interval_s: float = 0.0,
        filter_alpha: float = 0.1,
        use_gpio_wait: bool = True,
        read_timeout_s: float = 1.0,
    ):
        self.drdy_pin = int(drdy_pin)
        self.reg0 = _int_value(reg0)
        self.reg1 = _int_value(reg1)
        self.reg2 = _int_value(reg2)
        self.reg3 = _int_value(reg3)
        self.continuous_conversion = bool(continuous_conversion)
        self.start_each_read = bool(start_each_read)
        self.vref = float(vref)
        self.gain = float(gain)
        self.base_zero_raw = float(zero_raw)
        self.raw_per_gram = float(raw_per_gram)
        self.gauge_factor = float(gauge_factor)
        self.young_modulus_mpa = float(young_modulus_mpa)
        self.tare_file = str(tare_file)
        self.tare_samples = int(tare_samples)
        self.tare_interval_s = float(tare_interval_s)
        self.filter_alpha = float(filter_alpha)
        self.use_gpio_wait = bool(use_gpio_wait)
        self.read_timeout_s = float(read_timeout_s)

        self.io_lock = threading.RLock()
        self.tare_lock = threading.Lock()
        self.tare_running = False
        self.tare_enabled = False
        self.mv_per_v_filtered: float | None = None

        self.spi = spidev.SpiDev()
        self.spi.open(int(spi_bus), int(spi_device))
        self.spi.max_speed_hz = int(spi_speed_hz)
        self.spi.mode = int(spi_mode)

        GPIO.setmode(GPIO.BCM)
        GPIO.setup(self.drdy_pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

        self.zero_raw = self.load_tare()

        if data_rate_sps is not None:
            self.set_data_rate_sps(int(data_rate_sps))
        self.set_continuous_conversion(self.continuous_conversion)
        self.configure()

    def set_data_rate_sps(self, data_rate_sps: int) -> None:
        code = self.DATA_RATE_CODES.get(int(data_rate_sps))
        if code is None:
            allowed = ", ".join(str(k) for k in sorted(self.DATA_RATE_CODES))
            raise ValueError(f"Unsupported ADS1220 data_rate_sps={data_rate_sps}. Allowed: {allowed}")
        self.reg1 = (self.reg1 & 0x1F) | (code << 5)

    def set_continuous_conversion(self, enabled: bool) -> None:
        self.continuous_conversion = bool(enabled)
        if enabled:
            self.reg1 |= 0x04
        else:
            self.reg1 &= ~0x04

    def configure(self) -> None:
        with self.io_lock:
            self.spi.xfer2([self.CMD_RESET])

            time.sleep(0.1)

            self.spi.xfer2([
                self.CMD_WREG_BASE | 0x03,
                self.reg0,
                self.reg1,
                self.reg2,
                self.reg3,
            ])

            time.sleep(0.05)

            self.spi.xfer2([self.CMD_START])

            time.sleep(0.2)

    def recover(self) -> None:
        try:
            self.spi.xfer2([self.CMD_RESET])
        except Exception:
            pass

        time.sleep(0.2)

        self.configure()

        time.sleep(0.2)

    def wait_drdy(self, timeout: float | None = None) -> None:
        timeout = self.read_timeout_s if timeout is None else float(timeout)
        if GPIO.input(self.drdy_pin) == 0:
            return

        if self.use_gpio_wait and hasattr(GPIO, "wait_for_edge"):
            result = GPIO.wait_for_edge(self.drdy_pin, GPIO.FALLING, timeout=int(timeout * 1000))
            if result is None:
                self.recover()
                raise TimeoutError("ADS1220 DRDY timeout")
            return

        t0 = time.monotonic()
        while GPIO.input(self.drdy_pin):
            if time.monotonic() - t0 > timeout:
                self.recover()
                raise TimeoutError("ADS1220 DRDY timeout")
            time.sleep(0.00005)

    def read_raw(self) -> int:
        with self.io_lock:
            try:
                return self._read_raw_unlocked()

            except TimeoutError:
                self.recover()
                raise

            except Exception:
                raise

    def _read_raw_unlocked(self) -> int:
        if self.start_each_read or not self.continuous_conversion:
            self.spi.xfer2([self.CMD_START])

        self.wait_drdy()
        r = self.spi.xfer2([self.CMD_RDATA, 0xFF, 0xFF, 0xFF])
        raw = (r[1] << 16) | (r[2] << 8) | r[3]
        if raw & 0x800000:
            raw -= 0x1000000
        return raw

    def load_tare(self) -> float:
        try:
            if os.path.exists(self.tare_file):
                with open(self.tare_file, "r", encoding="utf-8") as f:
                    return float(f.read().strip())
        except Exception:
            pass
        return self.base_zero_raw

    def save_tare(self) -> None:
        with open(self.tare_file, "w", encoding="utf-8") as f:
            f.write(str(self.zero_raw))

    def set_filter_alpha(self, alpha: float) -> None:
        self.filter_alpha = max(0.0, min(float(alpha), 1.0))

    def filter_mv_per_v(self, mv_per_v: float) -> float:
        if self.mv_per_v_filtered is None:
            self.mv_per_v_filtered = mv_per_v
        else:
            self.mv_per_v_filtered += self.filter_alpha * (mv_per_v - self.mv_per_v_filtered)
        return self.mv_per_v_filtered

    def tare(self) -> float:
        with self.tare_lock:
            self.tare_running = True
            try:
                with self.io_lock:
                    if self.tare_enabled:
                        self.zero_raw = self.base_zero_raw
                        self.save_tare()
                        self.tare_enabled = False
                        return self.zero_raw

                    samples: list[int] = []
                    for _ in range(max(self.tare_samples, 1)):
                        samples.append(self._read_raw_unlocked())
                        if self.tare_interval_s > 0:
                            time.sleep(self.tare_interval_s)

                    self.zero_raw = sum(samples) / len(samples)
                    self.save_tare()
                    self.tare_enabled = True
                    return self.zero_raw
            finally:
                self.tare_running = False

    def tare_async(self) -> None:
        self.tare()

    def read(self) -> dict[str, Any]:
        try:
            with self.io_lock:
                raw = self._read_raw_unlocked()
                raw_offset = float(raw - self.zero_raw if self.tare_enabled else raw)

        except TimeoutError:
            return {
                "error": "drdy_timeout",
                "timestamp": time.time(),
            }

        except Exception:
            return {
                "error": "ads_error",
                "timestamp": time.time(),
            }

        volts = raw_offset * self.vref / (self.gain * self.ADS_FS)
        mv = volts * 1000.0
        mv_per_v = mv / self.vref
        mv_per_v_filtered = self.filter_mv_per_v(mv_per_v)
        v_per_v = volts / self.vref
        weight_g = raw_offset / self.raw_per_gram if self.raw_per_gram != 0 else 0.0
        strain_um_m = (v_per_v / self.gauge_factor) * 1_000_000.0 if self.gauge_factor != 0 else 0.0
        strain_mm_m = strain_um_m / 1000.0
        stress_mpa = strain_mm_m * self.young_modulus_mpa / 1000.0

        return {
            "raw": raw,
            "raw_offset": int(round(raw_offset)),
            "mv": mv,
            "mv_per_v": mv_per_v,
            "mv_per_v_filtered": mv_per_v_filtered,
            "strain_um_m": strain_um_m,
            "strain_mm_m": strain_mm_m,
            "stress_mpa": stress_mpa,
            "weight_g": weight_g,
            "weight_kg": weight_g / 1000.0,
            "timestamp": time.time(),
        }

    def close(self) -> None:
        try:
            self.spi.close()
        except Exception:
            pass


def _int_value(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 0)
    return int(value)


if __name__ == "__main__":
    ads = ADS1220()
    while True:
        data = ads.read()
        print(
            f"RAW={data['raw']} OFFSET={data['raw_offset']} "
            f"mV={data['mv']:.6f} mV/V={data['mv_per_v']:.6f} "
            f"mm/m={data['strain_mm_m']:.4f} MPa={data['stress_mpa']:.2f}"
        )
        time.sleep(0.5)
