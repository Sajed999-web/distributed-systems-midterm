"""Generate the two benchmark workloads.

  small  : many small files (default 5000 files of 1-50 KB, JSON and text).
           About 10 % are exact copies of other files, as happens with real
           config/log collections, so deduplication has something to find.
  large  : a few large files (default 10 files, 100 MB - 1 GB) that look like
           videos/backups (random, incompressible) and disk images (random
           data mixed with zero-filled regions). disk_base_v2.img is
           disk_base.img with a few bytes inserted and overwritten, and
           dataset_01_copy.bin is an exact copy of dataset_01.bin, to show
           deduplication and the fixed-size vs content-defined chunking difference.

Everything is seeded, so the same command always produces the same bytes.

Examples:
    python scripts/generate_datasets.py small
    python scripts/generate_datasets.py large
    python scripts/generate_datasets.py large --scale 0.1   # 10x smaller, for quick tests
"""
import argparse
import json
import random
import shutil
from pathlib import Path

MB = 1024 * 1024
BLOCK = 4 * MB

WORDS = ("storage node chunk replica erasure parity hash ring object bucket metadata repair scrub "
         "heartbeat quorum latency throughput cluster distributed system file version key value "
         "client server request response failure recovery placement virtual consistent").split()

LARGE_FILES = [  # (name, size in MB, kind)
    ("video_01.mp4", 100, "random"),
    ("video_02.mp4", 150, "random"),
    ("video_03.mp4", 200, "random"),
    ("video_04.mp4", 300, "random"),
    ("backup_01.tar", 400, "random"),
    ("backup_02.tar", 500, "random"),
    ("disk_base.img", 1000, "disk"),
    ("disk_base_v2.img", 1000, "edit-of:disk_base.img"),
    ("dataset_01.bin", 250, "random"),
    ("dataset_01_copy.bin", 250, "copy-of:dataset_01.bin"),
]


def _text(rng: random.Random, size: int) -> bytes:
    out, n = [], 0
    while n < size:
        line = " ".join(rng.choices(WORDS, k=rng.randint(6, 16))) + ".\n"
        out.append(line)
        n += len(line)
    return "".join(out).encode()[:size]


def _json(rng: random.Random, size: int) -> bytes:
    records, n = [], 2
    while n < size:
        rec = {"id": rng.randint(1, 10**9), "name": rng.choice(WORDS) + "-" + rng.choice(WORDS),
               "value": round(rng.random() * 1000, 3), "tags": rng.sample(WORDS, 3),
               "active": rng.random() < 0.5, "ts": f"2026-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"}
        s = json.dumps(rec)
        records.append(s)
        n += len(s) + 2
    return ("[" + ",\n".join(records) + "]").encode()


def gen_small(out: Path, count: int, min_kb: int, max_kb: int, dup_ratio: float, seed: int) -> None:
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    total = 0
    for i in range(count):
        sub = out / f"dir{i % 50:02d}"
        sub.mkdir(exist_ok=True)
        if written and rng.random() < dup_ratio:
            src = rng.choice(written)
            dst = sub / f"copy_{i:05d}{src.suffix}"
            shutil.copyfile(src, dst)
        else:
            size = rng.randint(min_kb * 1024, max_kb * 1024)
            if rng.random() < 0.5:
                dst, data = sub / f"record_{i:05d}.json", _json(rng, size)
            else:
                dst, data = sub / f"log_{i:05d}.txt", _text(rng, size)
            dst.write_bytes(data)
            written.append(dst)
        total += dst.stat().st_size
    print(f"small workload: {count} files, {total / MB:.1f} MB in {out}")


def _random_file(path: Path, size: int, rng: random.Random, zero_fraction: float = 0.0) -> None:
    with open(path, "wb") as f:
        left = size
        while left:
            n = min(BLOCK, left)
            f.write(bytes(n) if rng.random() < zero_fraction else rng.randbytes(n))
            left -= n


def _edited_copy(src: Path, dst: Path, rng: random.Random) -> None:
    """Copy src with three small insertions and one in-place overwrite."""
    size = src.stat().st_size
    inserts = sorted(rng.sample(range(1, size), 3))
    overwrite_at = rng.randrange(size - 4096)
    with open(src, "rb") as fi, open(dst, "wb") as fo:
        pos = 0
        for cut in inserts + [size]:
            while pos < cut:
                data = fi.read(min(BLOCK, cut - pos))
                if pos <= overwrite_at < pos + len(data):  # flip a 4 KB region
                    k = overwrite_at - pos
                    data = data[:k] + rng.randbytes(min(4096, len(data) - k)) + data[k + 4096:]
                fo.write(data)
                pos += len(data)
            if cut < size:
                fo.write(b"<<inserted %d bytes of new data>>" % cut)


def gen_large(out: Path, scale: float, seed: int) -> None:
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    for name, size_mb, kind in LARGE_FILES:
        path = out / name
        size = int(size_mb * MB * scale)
        if kind.startswith("copy-of:"):
            shutil.copyfile(out / kind.split(":")[1], path)
        elif kind.startswith("edit-of:"):
            _edited_copy(out / kind.split(":")[1], path, rng)
        else:
            _random_file(path, size, rng, zero_fraction=0.3 if kind == "disk" else 0.0)
        print(f"  {name:22s} {path.stat().st_size / MB:8.1f} MB  ({kind})")
    print(f"large workload: {len(LARGE_FILES)} files in {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("workload", choices=["small", "large", "both"])
    ap.add_argument("--out", default="datasets")
    ap.add_argument("--count", type=int, default=5000, help="small: number of files")
    ap.add_argument("--min-kb", type=int, default=1)
    ap.add_argument("--max-kb", type=int, default=50)
    ap.add_argument("--dup-ratio", type=float, default=0.10, help="small: fraction of exact duplicates")
    ap.add_argument("--scale", type=float, default=1.0, help="large: multiply every file size")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    if a.workload in ("small", "both"):
        gen_small(Path(a.out) / "small", a.count, a.min_kb, a.max_kb, a.dup_ratio, a.seed)
    if a.workload in ("large", "both"):
        gen_large(Path(a.out) / "large", a.scale, a.seed)


if __name__ == "__main__":
    main()
