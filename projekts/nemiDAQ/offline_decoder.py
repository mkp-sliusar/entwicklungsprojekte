from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


# =========================
# BIN format constants
# =========================
END_MARK = b"\x0C\x38"
PACKET_SIZE = 254

TYPE_BYTES = (0x04, 0x05)
PAD_PATTERN = b"\x01" * 5

COUNTER_OFF = 1          # u32 LE in subframe
DATA_OFF = 11            # Ch1 block start
DATA_LEN = 198           # 66 * 3 (u24)
N_SAMPLES = DATA_LEN // 3

# If BIN contains multiple channels, they are expected to be stored consecutively.
CH2_OFF = DATA_OFF + DATA_LEN


# =========================
# Data structures
# =========================
@dataclass
class DecodedPacket:
    ptype: int
    counter_us: int
    ch1_mvv: np.ndarray  # (66,)
    ch2_mvv: np.ndarray  # (66,)


# =========================
# Helpers: decode
# =========================
def extract_outer_packets_by_endmark(blob: bytes) -> list[bytes]:
    ends = [m.start() for m in re.finditer(re.escape(END_MARK), blob)]
    packets: list[bytes] = []
    seen = set()
    for pos in ends:
        end = pos + len(END_MARK)
        start = end - PACKET_SIZE
        if start < 0:
            continue
        if start in seen:
            continue
        pkt = blob[start:end]
        if len(pkt) == PACKET_SIZE and pkt[-2:] == END_MARK:
            seen.add(start)
            packets.append(pkt)
    return packets


def find_subframe_offset(outer_pkt: bytes) -> int | None:
    n = len(outer_pkt)
    for i in range(0, n - 16):
        if outer_pkt[i] in TYPE_BYTES and outer_pkt[i + 5:i + 10] == PAD_PATTERN:
            return i
    return None


def decode_u24_be_offset_binary(data: bytes) -> np.ndarray:
    """66*3 bytes: u24 big-endian, offset-binary center at 0x800000 (signed = raw - 2^23)."""
    u = np.frombuffer(data, dtype=np.uint8).reshape(-1, 3).astype(np.uint32)
    raw = (u[:, 0] << 16) | (u[:, 1] << 8) | u[:, 2]
    signed = raw.astype(np.int64) - (1 << 23)
    return signed.astype(np.int32)


def scale_mvv(gain: float) -> float:
    """Count -> mV/V scale for 24-bit offset-binary centered at 0x800000."""
    gain = float(gain)
    if gain <= 0:
        gain = 1.0
    return 1000.0 / ((2**23) * gain)


def decode_bin_full(bin_path: str, gain: float = 128.0) -> tuple[list[DecodedPacket], dict]:
    """Robust BIN decode.

    Notes on channels:
      - Ch1 is always decoded.
      - If the frame has enough bytes for Ch2 (DATA_LEN more bytes after Ch1), it is decoded.
      - Otherwise Ch2 is returned as zeros.
    """
    blob = open(bin_path, "rb").read()
    outers = extract_outer_packets_by_endmark(blob)
    if not outers:
        raise RuntimeError("No 254B packets found by END_MARK 0C 38.")

    offs = [find_subframe_offset(p) for p in outers]
    offs_valid = [o for o in offs if o is not None]
    if not offs_valid:
        raise RuntimeError("Subframe pattern not found (0x04/0x05 + 0x01*5).")

    # stable modal offset
    sub_off = int(pd.Series(offs_valid).mode().iloc[0])

    decoded: list[DecodedPacket] = []
    bad = 0
    bad_off = 0
    SCALE_MVV = scale_mvv(gain)

    have_ch2 = 0

    for outer in outers:
        o = find_subframe_offset(outer)
        if o is None:
            bad += 1
            continue
        if o != sub_off:
            bad_off += 1
            continue

        sf = outer[o:]
        if len(sf) < (DATA_OFF + DATA_LEN + 2):
            bad += 1
            continue
        if sf[-2:] != END_MARK:
            bad += 1
            continue

        ptype = sf[0]
        if ptype not in TYPE_BYTES:
            bad += 1
            continue

        counter_us = int.from_bytes(sf[COUNTER_OFF:COUNTER_OFF + 4], "little", signed=False)

        # ---- Ch1 ----
        data1 = sf[DATA_OFF:DATA_OFF + DATA_LEN]
        if len(data1) != DATA_LEN:
            bad += 1
            continue
        signed1 = decode_u24_be_offset_binary(data1)
        ch1 = signed1.astype(np.float64) * SCALE_MVV

        # ---- Ch2 (optional) ----
        ch2 = np.zeros(N_SAMPLES, dtype=np.float64)
        if len(sf) >= (CH2_OFF + DATA_LEN + 2):
            data2 = sf[CH2_OFF:CH2_OFF + DATA_LEN]
            if len(data2) == DATA_LEN:
                signed2 = decode_u24_be_offset_binary(data2)
                ch2 = signed2.astype(np.float64) * SCALE_MVV
                have_ch2 += 1

        decoded.append(DecodedPacket(ptype=ptype, counter_us=counter_us, ch1_mvv=ch1, ch2_mvv=ch2))

    if not decoded:
        raise RuntimeError("Subframes found, but none passed validation.")

    # dt by modulo 2^32
    counters = np.array([p.counter_us for p in decoded], dtype=np.uint32)
    dt_mod = ((counters[1:].astype(np.uint64) - counters[:-1].astype(np.uint64)) & 0xFFFFFFFF).astype(np.float64)
    small = dt_mod[dt_mod < 5_000_000]  # <5s
    dt_pkt_med_us = float(np.median(small)) if len(small) else float(np.median(dt_mod))

    info = {
        "outer_packets": len(outers),
        "decoded_packets": len(decoded),
        "bad_packets": bad,
        "bad_subframe_off": bad_off,
        "subframe_off": sub_off,
        "ptype_counts": dict(pd.Series([p.ptype for p in decoded]).value_counts()),
        "dt_pkt_median_us": dt_pkt_med_us,
        "dt_sample_median_s": (dt_pkt_med_us / N_SAMPLES) * 1e-6,
        "gain": float(gain),
        "scale_mvv": float(SCALE_MVV),
        "ch2_decoded_packets": int(have_ch2),
    }
    return decoded, info


# =========================
# Time build with reset segmentation (NO unwrap to +4294s)
# =========================
def _build_time_core(
    counters_u32: np.ndarray,
    anchor: float,
    gap_s: float,
    dt_abs_reset_s: float,
    dt_reset_factor: float,
) -> tuple[np.ndarray, list[int], dict, np.ndarray]:
    """Internal helper: compute segmented time vector and reset indices."""
    counters = counters_u32.astype(np.uint32)

    # modulo dt between packets (u32 us)
    dt_mod = ((counters[1:].astype(np.uint64) - counters[:-1].astype(np.uint64)) & 0xFFFFFFFF).astype(np.float64)

    dt_abs_reset_us = float(dt_abs_reset_s) * 1e6
    small = dt_mod[dt_mod < dt_abs_reset_us]
    dt_med = float(np.median(small)) if len(small) else float(np.median(dt_mod))
    reset_thr = max(dt_abs_reset_us, dt_reset_factor * dt_med)

    # dt_to_next for each packet (last = dt_med)
    dt_to_next = np.concatenate([dt_mod, [dt_med]])
    dt_to_next = np.where(dt_to_next > reset_thr, dt_med, dt_to_next)

    # reset boundaries by packets
    reset_packet_idx = [0]
    resets = []
    for i in range(1, len(counters)):
        if dt_mod[i - 1] > reset_thr:
            reset_packet_idx.append(i)
            resets.append((i, float(dt_mod[i - 1])))

    # build time by segments
    t_out = np.empty(len(counters) * N_SAMPLES, dtype=np.float64)

    seg_t0 = 0.0
    seg_counter0 = counters[0]

    reset_set = set(reset_packet_idx[1:])  # without 0

    for i in range(len(counters)):
        if i in reset_set:
            prev_last = t_out[i * N_SAMPLES - 1]
            seg_t0 = float(prev_last) + float(gap_s)
            seg_counter0 = counters[i]

        delta_us = float(((np.uint64(counters[i]) - np.uint64(seg_counter0)) & 0xFFFFFFFF))
        base_s = seg_t0 + delta_us * 1e-6

        dur_s = float(dt_to_next[i]) * 1e-6
        if dur_s <= 0:
            dur_s = dt_med * 1e-6
        dt_s = dur_s / N_SAMPLES

        idx0 = i * N_SAMPLES
        idx1 = idx0 + N_SAMPLES
        jj = (np.arange(N_SAMPLES, dtype=np.float64) + float(anchor)) * dt_s
        t_out[idx0:idx1] = base_s + jj

    info = {
        "dt_med_us": float(dt_med),
        "reset_thr_us": float(reset_thr),
        "resets": resets,
        "segments": len(reset_packet_idx),
    }
    return t_out, reset_packet_idx, info, dt_to_next


def build_time_series_segmented_raw(
    packets: list[DecodedPacket],
    anchor: float = 0.0,
    gap_s: float = 0.5,
    dt_abs_reset_s: float = 5.0,
    dt_reset_factor: float = 20.0,
) -> tuple[np.ndarray, np.ndarray, list[int], dict]:
    """Backwards-compatible: returns time + Ch1 only."""
    t, ch1, _ch2, reset_idx, info = build_time_series_segmented_raw_2ch(
        packets,
        anchor=anchor,
        gap_s=gap_s,
        dt_abs_reset_s=dt_abs_reset_s,
        dt_reset_factor=dt_reset_factor,
    )
    return t, ch1, reset_idx, info


def build_time_series_segmented_raw_2ch(
    packets: list[DecodedPacket],
    anchor: float = 0.0,
    gap_s: float = 0.5,
    dt_abs_reset_s: float = 5.0,
    dt_reset_factor: float = 20.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[int], dict]:
    """Returns segmented time axis and two channel arrays (NaN-free).

    Returns:
      t_raw, ch1_raw, ch2_raw: len = len(packets)*66
      reset_packet_idx: list of packet indices (segment starts), first = 0
      info: diagnostics
    """
    if not packets:
        raise RuntimeError("No packets.")

    counters = np.array([p.counter_us for p in packets], dtype=np.uint32)
    t_out, reset_packet_idx, info, _dt_to_next = _build_time_core(
        counters,
        anchor=float(anchor),
        gap_s=float(gap_s),
        dt_abs_reset_s=float(dt_abs_reset_s),
        dt_reset_factor=float(dt_reset_factor),
    )

    ch1 = np.empty(len(packets) * N_SAMPLES, dtype=np.float64)
    ch2 = np.empty(len(packets) * N_SAMPLES, dtype=np.float64)
    for i, pkt in enumerate(packets):
        idx0 = i * N_SAMPLES
        idx1 = idx0 + N_SAMPLES
        ch1[idx0:idx1] = pkt.ch1_mvv
        ch2[idx0:idx1] = pkt.ch2_mvv

    return t_out, ch1, ch2, reset_packet_idx, info


def make_plot_arrays_with_nans(
    t_raw: np.ndarray, y_raw: np.ndarray, reset_packet_idx: list[int]
) -> tuple[np.ndarray, np.ndarray]:
    """Build t_plot,y_plot with NaN breaks between segments."""
    if len(reset_packet_idx) <= 1:
        return t_raw, y_raw

    boundaries = [i * N_SAMPLES for i in reset_packet_idx[1:]]  # without 0

    t_list = []
    y_list = []
    last = 0
    for b in boundaries:
        t_list.append(t_raw[last:b])
        y_list.append(y_raw[last:b])
        t_list.append(np.array([np.nan], dtype=np.float64))
        y_list.append(np.array([np.nan], dtype=np.float64))
        last = b
    t_list.append(t_raw[last:])
    y_list.append(y_raw[last:])

    return np.concatenate(t_list), np.concatenate(y_list)


# =========================
# Filters
# =========================
def hampel_filter(y: np.ndarray, window: int = 11, n_sigma: float = 3.0) -> tuple[np.ndarray, np.ndarray]:
    if window < 3:
        window = 3
    if window % 2 == 0:
        window += 1

    s = pd.Series(np.asarray(y, dtype=np.float64))
    med = s.rolling(window=window, center=True, min_periods=1).median()
    diff = (s - med).abs()
    mad = diff.rolling(window=window, center=True, min_periods=1).median()
    sigma = 1.4826 * mad
    thr = n_sigma * sigma

    out = s.copy()
    mask = diff > thr
    out[mask] = med[mask]
    return out.to_numpy(np.float64), mask.to_numpy(bool)


# =========================
# CSV helpers
# =========================
def load_csv(csv_path: str) -> tuple[pd.DataFrame, np.ndarray, np.ndarray, np.ndarray]:
    df = pd.read_csv(csv_path, sep=";", engine="python")
    df = df.rename(columns=lambda c: c.strip())

    if "Timecounter (s)" not in df.columns or "Ch 1" not in df.columns:
        raise RuntimeError("CSV must contain 'Timecounter (s)' and 'Ch 1'.")

    t = pd.to_numeric(df["Timecounter (s)"], errors="coerce").to_numpy(np.float64)
    y1 = pd.to_numeric(df["Ch 1"], errors="coerce").to_numpy(np.float64)

    if "Ch 2" in df.columns:
        y2 = pd.to_numeric(df["Ch 2"], errors="coerce").to_numpy(np.float64)
    else:
        y2 = np.zeros(len(df), dtype=np.float64)

    m = np.isfinite(t) & np.isfinite(y1) & np.isfinite(y2)
    df2 = df.loc[m].copy()
    t = t[m]
    y1 = y1[m]
    y2 = y2[m]
    t = t - t[0]

    return df2, t, y1, y2


def guess_start_datetime(df_csv: pd.DataFrame | None, csv_path: str | None, bin_path: str | None) -> datetime | None:
    if df_csv is not None and "Timestamp (ASCII)" in df_csv.columns and len(df_csv) > 0:
        v = str(df_csv["Timestamp (ASCII)"].iloc[0])
        try:
            return datetime.strptime(v, "%d.%m.%Y %H:%M:%S.%f")
        except Exception:
            pass

    for p in [csv_path, bin_path]:
        if not p:
            continue
        name = os.path.basename(p)
        m = re.search(r"(\d{14})", name)
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            except Exception:
                pass
    return None


# =========================
# Match logic
# =========================
def best_packet_window(packets: list[DecodedPacket], y_csv: np.ndarray) -> dict:
    """Backwards-compatible: match using only Ch1."""
    y_csv = np.asarray(y_csv, dtype=np.float64)
    if len(y_csv) % N_SAMPLES != 0:
        return {"ok": False, "reason": "len(csv) is not a multiple of 66"}

    W = len(y_csv) // N_SAMPLES
    P = len(packets)
    if P < W:
        return {"ok": False, "reason": f"not enough packets in BIN: {P} < {W}"}

    mat = np.vstack([p.ch1_mvv for p in packets]).astype(np.float64)  # (P,66)

    best = None
    for start in range(0, P - W + 1):
        seg = mat[start:start + W].reshape(-1)
        for sign in (+1.0, -1.0):
            pred = seg * sign
            rmse = float(np.sqrt(np.mean((pred - y_csv) ** 2)))
            if best is None or rmse < best["rmse"]:
                best = {"ok": True, "start_pkt": start, "W": W, "sign": sign, "rmse": rmse}
    return best


def best_packet_window_2ch(packets: list[DecodedPacket], y1_csv: np.ndarray, y2_csv: np.ndarray) -> dict:
    """Match using Ch1+Ch2 together (single start_pkt and a single sign applied to both)."""
    y1_csv = np.asarray(y1_csv, dtype=np.float64)
    y2_csv = np.asarray(y2_csv, dtype=np.float64)

    if len(y1_csv) != len(y2_csv):
        return {"ok": False, "reason": "CSV channel lengths differ"}
    if len(y1_csv) % N_SAMPLES != 0:
        return {"ok": False, "reason": "len(csv) is not a multiple of 66"}

    W = len(y1_csv) // N_SAMPLES
    P = len(packets)
    if P < W:
        return {"ok": False, "reason": f"not enough packets in BIN: {P} < {W}"}

    m1 = np.vstack([p.ch1_mvv for p in packets]).astype(np.float64)
    m2 = np.vstack([p.ch2_mvv for p in packets]).astype(np.float64)

    best = None
    for start in range(0, P - W + 1):
        seg1 = m1[start:start + W].reshape(-1)
        seg2 = m2[start:start + W].reshape(-1)
        for sign in (+1.0, -1.0):
            d1 = (seg1 * sign) - y1_csv
            d2 = (seg2 * sign) - y2_csv
            rmse = float(np.sqrt(np.mean(np.concatenate([d1, d2]) ** 2)))
            if best is None or rmse < best["rmse"]:
                best = {"ok": True, "start_pkt": start, "W": W, "sign": sign, "rmse": rmse}
    return best


def corr_z(a, b) -> float:
    a = np.asarray(a, dtype=np.float64)
    b = np.asarray(b, dtype=np.float64)
    if len(a) < 50 or len(b) < 50:
        return float("nan")
    a = (a - a.mean()) / (a.std() + 1e-12)
    b = (b - b.mean()) / (b.std() + 1e-12)
    return float(np.corrcoef(a, b)[0, 1])


# =========================
# Export
# =========================
def export_full_bin_csv(path: str, t: np.ndarray, ch1: np.ndarray, ch2: np.ndarray, start_dt: datetime | None):
    t = np.asarray(t, dtype=np.float64)
    ch1 = np.asarray(ch1, dtype=np.float64)
    ch2 = np.asarray(ch2, dtype=np.float64)

    if len(t) != len(ch1) or len(t) != len(ch2):
        raise RuntimeError("export_full_bin_csv: length mismatch")

    if start_dt is not None:
        ts_dt = [start_dt + timedelta(seconds=float(x)) for x in t]
        ts_ascii = [d.strftime("%d.%m.%Y %H:%M:%S.%f") for d in ts_dt]
        ts_s = np.array([d.timestamp() for d in ts_dt], dtype=np.float64)
    else:
        ts_ascii = [""] * len(t)
        ts_s = np.full(len(t), np.nan, dtype=np.float64)

    df = pd.DataFrame({
        "Timestamp (ASCII)": ts_ascii,
        "Timestamp (s)": ts_s,
        "Timecounter (s)": t,
        "Ch 1": ch1,
        "Ch 2": ch2,
        "Ch 3": np.zeros(len(t), dtype=np.float64),
        "Ch 4": np.zeros(len(t), dtype=np.float64),
    })
    df.to_csv(path, sep=";", index=False)


def export_like_reference_csv(path: str, df_ref: pd.DataFrame, y_new: np.ndarray, channel: int = 1):
    """Export a single channel into a reference CSV (keeps other columns unchanged)."""
    df_out = df_ref.copy()
    col = f"Ch {int(channel)}"
    if col not in df_out.columns:
        raise RuntimeError(f"Reference CSV does not have '{col}'.")
    if len(df_out) != len(y_new):
        raise RuntimeError("Signal length != reference CSV length.")
    df_out[col] = np.asarray(y_new, dtype=np.float64)
    df_out.to_csv(path, sep=";", index=False)
