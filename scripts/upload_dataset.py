"""Upload every file in a directory to a bucket and report throughput + dedup.

    python scripts/upload_dataset.py datasets/small --bucket small-fixed --chunking fixed
    python scripts/upload_dataset.py datasets/large --bucket large-cdc --chunking cdc --workers 2
"""
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx


def file_stream(path: Path, block: int = 1024 * 1024):
    with open(path, "rb") as f:
        while data := f.read(block):
            yield data


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("directory")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--chunking", choices=["fixed", "cdc"], default="fixed")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--url", default="http://localhost:8100")
    ap.add_argument("--out", help="write per-file results as JSON to this path")
    a = ap.parse_args()

    root = Path(a.directory)
    files = sorted(p for p in root.rglob("*") if p.is_file())
    client = httpx.Client(base_url=a.url, timeout=600)
    client.put(f"/buckets/{a.bucket}").raise_for_status()
    before = client.get("/stats").json()

    def upload(p: Path) -> dict:
        key = p.relative_to(root).as_posix()
        r = client.put(f"/objects/{a.bucket}/{key}", params={"chunking": a.chunking},
                       content=file_stream(p), headers={"Content-Length": str(p.stat().st_size)})
        r.raise_for_status()
        return r.json()

    t0 = time.time()
    results, done = [], 0
    with ThreadPoolExecutor(a.workers) as ex:
        for res in ex.map(upload, files):
            results.append(res)
            done += 1
            if done % max(1, len(files) // 20) == 0 or done == len(files):
                print(f"  {done}/{len(files)} uploaded", flush=True)
    secs = time.time() - t0
    after = client.get("/stats").json()

    size = sum(r["size"] for r in results)
    summary = {
        "directory": str(root), "bucket": a.bucket, "chunking": a.chunking, "files": len(results),
        "logical_bytes": size, "seconds": round(secs, 2),
        "throughput_MBps": round(size / 2**20 / secs, 2), "files_per_s": round(len(results) / secs, 1),
        "chunks": sum(r["chunks"] for r in results),
        "new_chunks": sum(r["new_chunks"] for r in results),
        "deduplicated_chunks": sum(r["deduplicated_chunks"] for r in results),
        "new_unique_bytes": after["unique_bytes"] - before["unique_bytes"],
        "new_physical_bytes": after["physical_bytes"] - before["physical_bytes"],
    }
    summary["dedup_saving_percent"] = round(100 * (1 - summary["new_unique_bytes"] / size), 2) if size else 0
    print(json.dumps(summary, indent=2))
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text(json.dumps({"summary": summary, "files": results}, indent=1, default=str))


if __name__ == "__main__":
    main()
