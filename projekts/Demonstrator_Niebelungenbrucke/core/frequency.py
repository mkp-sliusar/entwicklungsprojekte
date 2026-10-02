from __future__ import annotations

from collections import deque
import time

try:
    import numpy as np
except Exception:  # pragma: no cover - numpy is optional at runtime
    np = None


class DominantFrequencyEstimator:
    def __init__(
        self,
        sample_rate_hz: float,
        fft_size: int = 256,
        update_rate_hz: float = 5.0,
        min_frequency_hz: float = 0.5,
    ):
        self.sample_rate_hz = float(sample_rate_hz)
        self.fft_size = int(fft_size)
        self.update_period_s = 1.0 / float(update_rate_hz) if update_rate_hz > 0 else 1.0
        self.min_frequency_hz = float(min_frequency_hz)
        self.values: deque[float] = deque(maxlen=self.fft_size)
        self.timestamps: deque[float] = deque(maxlen=self.fft_size)
        self.last_frequency_hz = 0.0
        self.next_update = time.monotonic()

    def update(self, value: float, timestamp_s: float | None = None) -> float:
        timestamp_s = time.time() if timestamp_s is None else timestamp_s
        self.values.append(float(value))
        self.timestamps.append(float(timestamp_s))

        now = time.monotonic()
        if now < self.next_update:
            return self.last_frequency_hz
        self.next_update = now + self.update_period_s

        if np is None or len(self.values) < self.fft_size:
            return self.last_frequency_hz

        duration = self.timestamps[-1] - self.timestamps[0]
        if duration <= 0:
            sample_rate = self.sample_rate_hz
        else:
            sample_rate = (len(self.timestamps) - 1) / duration

        if sample_rate <= 0:
            return self.last_frequency_hz

        signal = np.array(self.values, dtype=float)
        signal = signal - np.mean(signal)
        window = np.hanning(len(signal))
        spectrum = np.fft.rfft(signal * window)
        freqs = np.fft.rfftfreq(len(signal), d=1.0 / sample_rate)

        if len(freqs) <= 1:
            return self.last_frequency_hz

        min_index = int(np.searchsorted(freqs, self.min_frequency_hz))
        min_index = max(min_index, 1)
        if min_index >= len(freqs):
            return self.last_frequency_hz

        idx = int(np.argmax(np.abs(spectrum[min_index:])) + min_index)
        self.last_frequency_hz = float(freqs[idx])
        return self.last_frequency_hz
