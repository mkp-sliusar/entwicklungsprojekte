from __future__ import annotations

import socket
import time

import psutil


class SystemMonitor:
    start_time = time.time()

    @staticmethod
    def get_metrics(check_internet: bool = True, internet_timeout_s: float = 0.2):
        internet = 0
        if check_internet:
            try:
                with socket.create_connection(("8.8.8.8", 53), timeout=float(internet_timeout_s)):
                    internet = 1
            except Exception:
                internet = 0

        net = psutil.net_io_counters()
        return {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "ram_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage("/").percent,
            "cpu_temp": SystemMonitor.get_cpu_temp(),
            "internet": internet,
            "uptime": int(time.time() - SystemMonitor.start_time),
            "rx_bytes": net.bytes_recv,
            "tx_bytes": net.bytes_sent,
            "cpu_freq": SystemMonitor.get_cpu_freq(),
        }

    @staticmethod
    def get_cpu_temp():
        try:
            with open("/sys/class/thermal/thermal_zone0/temp", "r", encoding="utf-8") as f:
                return float(f.read()) / 1000.0
        except Exception:
            return 0.0

    @staticmethod
    def get_cpu_freq():
        try:
            freq = psutil.cpu_freq()
            if freq:
                return freq.current
        except Exception:
            pass
        return 0.0
