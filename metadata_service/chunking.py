"""File chunking: fixed-size and content-defined (FastCDC).

Both chunkers return a list of (offset, length) boundaries for a file on disk.

Fixed-size chunking cuts every `size` bytes. It is fast, but inserting a
single byte near the start of a file shifts every later boundary, so none of
the following chunks deduplicate against the previous version.

FastCDC (Xia et al., USENIX ATC'16) cuts where a rolling "gear" hash of the
last bytes matches a mask, so boundaries depend on the *content* and re-align
right after an edit. It uses normalized chunking: a stricter mask (more bits)
before the average size and a looser one after it, which narrows the
chunk-size distribution.

The gear hash is h_i = (h_{i-1} << 1) + GEAR[b_i]. Its low 32 bits depend
only on the last 32 bytes, h_i = sum_{j<32} GEAR[b_{i-j}] << j (mod 2^32),
which we compute for the whole file at once with numpy (log2(32) = 5
shift-and-add passes), instead of a slow per-byte Python loop.
"""
import os
from concurrent.futures import ThreadPoolExecutor

import numpy as np

MiB = 1024 * 1024

FIXED_SIZE = int(os.getenv("FIXED_CHUNK_SIZE", 4 * MiB))
CDC_MIN = int(os.getenv("CDC_MIN_SIZE", 1 * MiB))
CDC_AVG = int(os.getenv("CDC_AVG_SIZE", 4 * MiB))
CDC_MAX = int(os.getenv("CDC_MAX_SIZE", 16 * MiB))

_SEGMENT = 16 * MiB  # bytes hashed per numpy pass, bounds memory use
_WINDOW = 32
_THREADS = int(os.getenv("CDC_THREADS", min(8, os.cpu_count() or 1)))  # numpy releases the GIL

# Deterministic gear table (must never change, or boundaries change).
GEAR = np.random.default_rng(0x5EED_CDC).integers(0, 2**32, size=256, dtype=np.uint64).astype(np.uint32)


def fixed_chunks(path: str, size: int = FIXED_SIZE) -> list[tuple[int, int]]:
    total = os.path.getsize(path)
    return [(off, min(size, total - off)) for off in range(0, total, size)] or [(0, 0)]


def _gear_hashes(buf: np.ndarray) -> np.ndarray:
    h = np.take(GEAR, buf)
    tmp = np.empty_like(h)
    w = 1
    while w < _WINDOW:
        # h[i] += h[i-w] << w, using the values from before this pass
        np.left_shift(h[:-w], np.uint32(w), out=tmp[:-w])
        h[w:] += tmp[:-w]
        w *= 2
    return h


def _masks(avg: int) -> tuple[int, int]:
    bits = avg.bit_length() - 1
    mask_s = (1 << (bits + 2)) - 1  # harder to match before the average size
    mask_l = (1 << (bits - 2)) - 1  # easier to match after it
    return mask_s, mask_l


def cdc_chunks(path: str, min_size: int = CDC_MIN, avg_size: int = CDC_AVG,
               max_size: int = CDC_MAX) -> list[tuple[int, int]]:
    total = os.path.getsize(path)
    if total <= min_size:  # can only ever be one chunk, no need to hash
        return [(0, total)]
    mask_s, mask_l = (np.uint32(m) for m in _masks(avg_size))

    # 1. candidate cut positions for both masks over the whole file
    data = np.memmap(path, dtype=np.uint8, mode="r")

    def candidates(seg: int) -> tuple[np.ndarray, np.ndarray]:
        lead = min(seg, _WINDOW - 1)  # re-include the previous bytes of the window
        h = _gear_hashes(np.asarray(data[seg - lead: seg + _SEGMENT]))[lead:]
        return np.flatnonzero((h & mask_s) == 0) + seg, np.flatnonzero((h & mask_l) == 0) + seg

    with ThreadPoolExecutor(_THREADS) as ex:
        cands = list(ex.map(candidates, range(0, total, _SEGMENT)))
    del data
    pos_s = np.concatenate([s for s, _ in cands])
    pos_l = np.concatenate([l for _, l in cands])

    # 2. walk the file choosing the first valid cut in each chunk
    out, start = [], 0
    while start < total:
        if total - start <= min_size:
            cut = total
        else:
            lo, mid, hi = start + min_size, min(start + avg_size, total), min(start + max_size, total)
            i = np.searchsorted(pos_s, lo)
            if i < len(pos_s) and pos_s[i] < mid:
                cut = int(pos_s[i]) + 1
            else:
                j = np.searchsorted(pos_l, mid)
                cut = int(pos_l[j]) + 1 if j < len(pos_l) and pos_l[j] < hi else hi
        out.append((start, cut - start))
        start = cut
    return out


def chunk_file(path: str, method: str) -> list[tuple[int, int]]:
    if method == "fixed":
        return fixed_chunks(path)
    if method == "cdc":
        return cdc_chunks(path)
    raise ValueError(f"unknown chunking method {method!r} (use 'fixed' or 'cdc')")
