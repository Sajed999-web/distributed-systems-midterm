"""Replication vs erasure coding: storage overhead and repair traffic on both workloads.

For every (workload, scheme) pair the cluster is RESET (PostgreSQL and node
volumes are deleted, so deduplication cannot reuse chunks from another run),
then:
  1. the workload is uploaded into a bucket with that redundancy policy
  2. storage overhead = physical bytes on the nodes / unique data bytes
  3. node4 is killed and its disk volume deleted (a lost disk)
  4. the automatic repair job is timed; its traffic (bytes read from healthy
     nodes, bytes written to new nodes) is compared with the bytes node4 held
  5. sample objects are downloaded and verified
  6. node4 comes back with an empty disk

!! This deletes all data stored in the cluster. !!

    python scripts/bench_redundancy.py                       # small + large, rep3 + ec42
    python scripts/bench_redundancy.py --workloads small --schemes rep3 ec42 ec22

Writes benchmarks/redundancy_comparison.csv/.json/.png.
"""
import argparse
import csv
import hashlib
import json
import random
import subprocess
import sys
import time
from pathlib import Path

import httpx

API = "http://localhost:8100"
OUT = Path("benchmarks")
MB = 1024 * 1024
PROJECT = Path.cwd().name.lower()
NODES = ["node1", "node2", "node3", "node4"]
SCHEMES = {
    "rep3": {"redundancy": "replication", "replicas": 3},
    "ec42": {"redundancy": "ec", "k": 4, "m": 2},
    "ec22": {"redundancy": "ec", "k": 2, "m": 2},
}
WORKERS = {"small": 16, "large": 4}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def sh(cmd: str, check: bool = True) -> None:
    subprocess.run(cmd, shell=True, check=check, capture_output=True)


def wait_for(cond, timeout: float, what: str):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = cond()
            if v:
                return v
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise TimeoutError(what)


def reset_cluster(c: httpx.Client) -> None:
    services = "postgres metadata " + " ".join(NODES)
    sh(f"docker compose rm -sf {services}")
    sh("docker volume rm " + " ".join(f"{PROJECT}_{v}-data" for v in ["pg"] + NODES), check=False)
    sh(f"docker compose up -d {services}")
    wait_for(lambda: c.get("/health").json()["ring"] == NODES, 120, "cluster did not come up")


def run(c: httpx.Client, workload: str, scheme: str) -> dict:
    log(f"--- {workload} / {scheme}: resetting cluster")
    reset_cluster(c)
    bucket = f"{workload}-{scheme}"
    log(f"create bucket {bucket}: {c.put(f'/buckets/{bucket}', params=SCHEMES[scheme]).json()}")

    log(f"uploading datasets/{workload} ...")
    up_json = OUT / f"upload_{bucket}.json"
    subprocess.run([sys.executable, "scripts/upload_dataset.py", f"datasets/{workload}", "--bucket", bucket,
                    "--workers", str(WORKERS[workload]), "--out", str(up_json)], check=True,
                   stdout=subprocess.DEVNULL)
    upload = json.loads(up_json.read_text())["summary"]
    log(f"uploaded {upload['files']} files, {upload['logical_bytes'] / MB:.0f} MB in {upload['seconds']} s "
        f"({upload['throughput_MBps']} MB/s)")
    s = c.get("/stats", params={"bucket": bucket}).json()
    on_node4 = next((n for n in s["per_node"] if n["node_id"] == "node4"), {"bytes": 0, "blobs": 0})

    log("killing node4 and deleting its disk")
    t_fail = time.time()
    sh("docker compose rm -sf node4")
    sh(f"docker volume rm {PROJECT}_node4-data")

    def job_done():
        jobs = [j for j in c.get("/repair/jobs").json() if "node4 failed" in j["reason"]]
        return jobs[0] if jobs and jobs[0]["status"] != "running" else None

    job = wait_for(job_done, 3600, "repair did not finish")
    detected_after = (time.time() - t_fail) - job["seconds"]
    log(f"repair job {job['status']}: {job['chunks_done']}/{job['chunks_total']} chunks in {job['seconds']:.1f} s, "
        f"read {job['bytes_read'] / MB:.1f} MB, wrote {job['bytes_written'] / MB:.1f} MB")
    degraded = c.get("/repair/status").json()["degraded_chunks"]

    files = json.loads(up_json.read_text())["files"]
    sample = random.Random(0).sample(files, min(len(files), 50 if workload == "small" else 3))
    ok = sum(hashlib.sha256(c.get(f"/objects/{bucket}/{f['key']}").content).hexdigest() == f["sha256"]
             for f in sample)
    log(f"verified {ok}/{len(sample)} sampled objects after repair")

    sh("docker compose up -d node4")  # replacement node with an empty disk
    wait_for(lambda: "node4" in c.get("/health").json()["alive"], 60, "node4 did not rejoin")

    lost = on_node4["bytes"]
    return {
        "workload": workload, "scheme": scheme, "policy": c.get("/buckets").json()[0]["policy"],
        "files": upload["files"], "logical_MB": round(upload["logical_bytes"] / MB, 1),
        "unique_MB": round(s["unique_bytes"] / MB, 1), "physical_MB": round(s["physical_bytes"] / MB, 1),
        "stored_blobs": s["stored_blobs"], "storage_overhead": s["redundancy_overhead"],
        "upload_MBps": upload["throughput_MBps"], "upload_files_per_s": upload["files_per_s"],
        "lost_on_node4_MB": round(lost / MB, 1), "lost_blobs": on_node4["blobs"],
        "repair_chunks": job["chunks_done"], "repair_failed": job["chunks_failed"],
        "repair_read_MB": round(job["bytes_read"] / MB, 1), "repair_written_MB": round(job["bytes_written"] / MB, 1),
        "repair_traffic_MB": round((job["bytes_read"] + job["bytes_written"]) / MB, 1),
        "repair_read_per_lost_byte": round(job["bytes_read"] / lost, 2) if lost else None,
        "repair_seconds": round(job["seconds"], 1), "detection_seconds": round(detected_after, 1),
        "degraded_after_repair": degraded, "verified": f"{ok}/{len(sample)}",
    }


def plot(rows) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    workloads = list(dict.fromkeys(r["workload"] for r in rows))
    schemes = list(dict.fromkeys(r["scheme"] for r in rows))
    colors = {"rep3": "#2a78d6", "ec42": "#1f9e75", "ec22": "#eb6834"}
    metrics = [("storage_overhead", "Storage overhead (stored / unique bytes)", "x%.2f"),
               ("repair_traffic_MB", "Repair traffic after losing node4 (MB read + written)", "%.0f"),
               ("repair_read_per_lost_byte", "Bytes read per byte lost", "%.2f")]
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.8))
    width = 0.8 / len(schemes)
    for ax, (metric, title, fmt) in zip(axes, metrics):
        for i, sc in enumerate(schemes):
            vals = [next((r[metric] for r in rows if r["workload"] == w and r["scheme"] == sc), 0) or 0
                    for w in workloads]
            label = next(r["policy"] for r in rows if r["scheme"] == sc)
            bars = ax.bar([x + (i - (len(schemes) - 1) / 2) * width for x in range(len(workloads))], vals, width,
                          label=label, color=colors.get(sc, "#888"))
            ax.bar_label(bars, fmt=fmt, fontsize=9)
        ax.set_xticks(range(len(workloads)), [f"{w} workload" for w in workloads])
        ax.set_title(title, fontsize=11)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False, fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT / "redundancy_comparison.png", dpi=150)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workloads", nargs="+", default=["small", "large"])
    ap.add_argument("--schemes", nargs="+", default=["rep3", "ec42"], choices=list(SCHEMES))
    a = ap.parse_args()
    OUT.mkdir(exist_ok=True)
    c = httpx.Client(base_url=API, timeout=3600)
    rows = []
    for w in a.workloads:
        for sc in a.schemes:
            rows.append(run(c, w, sc))
            log(json.dumps(rows[-1]))
            (OUT / "redundancy_comparison.json").write_text(json.dumps(rows, indent=2))
    with open(OUT / "redundancy_comparison.csv", "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=rows[0].keys())
        wr.writeheader()
        wr.writerows(rows)
    plot(rows)
    log(f"-> {OUT / 'redundancy_comparison.csv'}, {OUT / 'redundancy_comparison.png'}")


if __name__ == "__main__":
    main()
