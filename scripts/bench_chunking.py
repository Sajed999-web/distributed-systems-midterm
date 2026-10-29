"""Compare fixed-size chunking with content-defined chunking (FastCDC).

For each workload directory and each method this measures the number of
chunks, the average chunk size, the chunking + hashing throughput and how many
bytes deduplicate (bytes that are in a chunk already seen elsewhere).

    python scripts/bench_chunking.py datasets/small datasets/large

Writes benchmarks/chunking_comparison.csv and benchmarks/chunking_comparison.png.
"""
import argparse
import csv
import hashlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from metadata_service.chunking import cdc_chunks, fixed_chunks  # noqa: E402

OUT = Path("benchmarks")
MB = 1024 * 1024


def run(directory: Path, method: str, report: bool = True) -> dict:
    files = sorted(p for p in directory.rglob("*") if p.is_file())
    chunker = fixed_chunks if method == "fixed" else cdc_chunks
    seen: set[str] = set()
    logical = unique = chunks = 0
    t0 = time.time()
    for p in files:
        bounds = chunker(str(p))
        with open(p, "rb") as f:
            for off, ln in bounds:
                f.seek(off)
                h = hashlib.sha256(f.read(ln)).hexdigest()
                chunks += 1
                logical += ln
                if h not in seen:
                    seen.add(h)
                    unique += ln
    secs = time.time() - t0
    row = {
        "workload": directory.name, "method": method, "files": len(files), "logical_MB": round(logical / MB, 1),
        "chunks": chunks, "avg_chunk_KB": round(logical / chunks / 1024, 1),
        "unique_MB": round(unique / MB, 1), "dedup_saving_pct": round(100 * (1 - unique / logical), 2),
        "throughput_MBps": round(logical / MB / secs, 1), "seconds": round(secs, 2),
    }
    if report:
        print(row, flush=True)
    return row


def plot(rows) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    workloads = list(dict.fromkeys(r["workload"] for r in rows))
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, metric, label in ((axes[0], "dedup_saving_pct", "Deduplication saving (%)"),
                              (axes[1], "throughput_MBps", "Chunking + SHA-256 throughput (MB/s)")):
        for i, method in enumerate(("fixed", "cdc")):
            vals = [next(r[metric] for r in rows if r["workload"] == w and r["method"] == method) for w in workloads]
            bars = ax.bar([x + (i - 0.5) * 0.38 for x in range(len(workloads))], vals, 0.38,
                          label="fixed-size (4 MiB)" if method == "fixed" else "FastCDC (avg 4 MiB)",
                          color="#2a78d6" if method == "fixed" else "#1f9e75")
            ax.bar_label(bars, fmt="%.1f")
        ax.set_xticks(range(len(workloads)), [f"{w} workload" for w in workloads])
        ax.set_title(label)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(OUT / "chunking_comparison.png", dpi=150)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    a = ap.parse_args()
    rows = []
    for d in a.dirs:
        run(Path(d), "fixed", report=False)  # untimed warm-up: the first pass pays one-off file-open/cache costs
        rows += [run(Path(d), m) for m in ("fixed", "cdc")]
    OUT.mkdir(exist_ok=True)
    with open(OUT / "chunking_comparison.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    plot(rows)
    print(f"-> {OUT / 'chunking_comparison.csv'}, {OUT / 'chunking_comparison.png'}")


if __name__ == "__main__":
    main()
