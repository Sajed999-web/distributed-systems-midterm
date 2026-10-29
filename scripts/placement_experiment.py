"""Placement experiment: how much data moves when a node is added or removed?

Part A (offline simulation, no cluster needed)
    100 000 synthetic chunk hashes are placed with N = 3 replicas on 4 nodes,
    then a 5th node is added / the 4th node is removed. For several numbers of
    virtual nodes we compare consistent hashing with naive modulo hashing
    (node = hash mod #nodes) and also report how evenly the ring spreads load.

Part B (live cluster, --live)
    Uses the chunks actually stored in PostgreSQL:
      1. asks the metadata service to predict the movement for +node5 / -node4
      2. starts node5 (docker compose --profile scale up -d node5), runs a
         rebalance and measures the replicas that really moved
      3. decommissions node5, rebalances again (data moves back off node5)
      4. stops node5

Results are written to benchmarks/placement_*.csv|json|png.

    python scripts/placement_experiment.py           # part A only
    python scripts/placement_experiment.py --live    # part A + part B
"""
import argparse
import csv
import hashlib
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from metadata_service.ring import HashRing  # noqa: E402

OUT = Path("benchmarks")
API = "http://localhost:8100"
N = 3


def mod_targets(h: str, nodes: list[str], n: int) -> set[str]:
    i = int(h[:16], 16) % len(nodes)
    return {nodes[(i + k) % len(nodes)] for k in range(n)}


def moved_fraction(hashes, before, after, vnodes, n) -> tuple[float, float]:
    r1, r2 = HashRing(before, vnodes), HashRing(after, vnodes)
    total = len(hashes) * n
    ch = sum(len(set(r2.get_nodes(h, n)) - set(r1.get_nodes(h, n))) for h in hashes)
    md = sum(len(mod_targets(h, after, n) - mod_targets(h, before, n)) for h in hashes)
    return 100 * ch / total, 100 * md / total


def part_a(num_chunks: int) -> None:
    hashes = [hashlib.sha256(f"chunk-{i}".encode()).hexdigest() for i in range(num_chunks)]
    four = [f"node{i}" for i in range(1, 5)]
    scenarios = {"add node5 (4->5)": (four, four + ["node5"]), "remove node4 (4->3)": (four, four[:3])}
    vnode_counts = (1, 8, 32, 128, 256)
    rows = []
    print(f"\nPart A - simulation with {num_chunks} chunks, N={N} replicas", flush=True)

    # load balance depends only on the 4-node "before" ring: one ring per vnode count
    print("computing load balance of the 4-node ring ...", flush=True)
    imbalance = {}
    for v in vnode_counts:
        ring = HashRing(four, v)
        load = Counter(ring.get_nodes(h, 1)[0] for h in hashes)
        imbalance[v] = max(load.values()) / min(load.values())

    print(f"{'scenario':22s} {'vnodes':>6s} {'consistent %':>13s} {'modulo %':>9s} {'ideal %':>8s} {'load max/min':>13s}",
          flush=True)
    for name, (before, after) in scenarios.items():
        print(f"-- scenario: {name} ...", flush=True)
        ideal = 100 * abs(len(after) - len(before)) / max(len(after), len(before))
        for v in vnode_counts:
            ch, md = moved_fraction(hashes, before, after, v, N)
            rows.append({"scenario": name, "vnodes": v, "consistent_pct_moved": round(ch, 2),
                         "modulo_pct_moved": round(md, 2), "ideal_pct": round(ideal, 2),
                         "load_imbalance_max_over_min": round(imbalance[v], 3)})
            print(f"{name:22s} {v:6d} {ch:13.2f} {md:9.2f} {ideal:8.2f} {imbalance[v]:13.2f}", flush=True)
    OUT.mkdir(exist_ok=True)
    with open(OUT / "placement_simulation.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    _plot(rows)
    print(f"-> {OUT / 'placement_simulation.csv'}, {OUT / 'placement_simulation.png'}")


def _plot(rows) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, scen in zip(axes, ("add node5 (4->5)", "remove node4 (4->3)")):
        rs = [r for r in rows if r["scenario"] == scen and r["vnodes"] == 256][0]
        bars = ax.bar(["consistent hashing\n(256 vnodes)", "modulo hashing", "ideal (minimum)"],
                      [rs["consistent_pct_moved"], rs["modulo_pct_moved"], rs["ideal_pct"]],
                      color=["#2a78d6", "#d95f43", "#9aa0a6"])
        ax.bar_label(bars, fmt="%.1f%%")
        ax.set_title(f"Replicas moved when we {scen}")
        ax.set_ylabel("% of chunk replicas moved")
        ax.set_ylim(0, 100)
        ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(OUT / "placement_simulation.png", dpi=150)


def sh(cmd: str) -> None:
    print(f"$ {cmd}")
    subprocess.run(cmd, shell=True, check=True)


def wait_ring(c: httpx.Client, expect: set[str], timeout: float = 30) -> None:
    t0 = time.time()
    while set(c.get("/health").json()["ring"]) != expect:
        if time.time() - t0 > timeout:
            raise TimeoutError(f"ring never became {expect}")
        time.sleep(1)


def timed_rebalance(c: httpx.Client, label: str) -> dict:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] rebalance {label}: started", flush=True)
    r = c.post("/admin/rebalance").json()
    print(f"[{time.strftime('%H:%M:%S')}] rebalance {label}: finished in {time.time() - t0:.1f} s, "
          f"{r.get('replicas_copied', 0)} replicas moved ({r.get('percent_replicas_moved')}%), "
          f"{r.get('replicas_removed', 0)} old replicas removed", flush=True)
    return r


def part_b() -> None:
    c = httpx.Client(base_url=API, timeout=3600)
    results = {}
    print("\nPart B - live cluster")
    stats = c.get("/stats").json()
    print(f"stored: {stats['unique_chunks']} unique chunks, {stats['stored_blobs']} replicas")
    for label, params in (("predict_add_node5", {"add": "node5"}), ("predict_remove_node4", {"remove": "node4"})):
        r = c.get("/placement-simulation", params=params).json()
        results[label] = r
        print(f"{label:22s}: consistent {r['consistent_hashing']['percent_moved']}%  "
              f"modulo {r['modulo_hashing']['percent_moved']}%  ideal {r['ideal_percent']}%")

    sh("docker compose --profile scale up -d node5")
    wait_ring(c, {f"node{i}" for i in range(1, 6)})
    results["rebalance_after_add_node5"] = r = timed_rebalance(c, "after adding node5")
    print("rebalance after adding node5   :", r)
    results["stats_with_node5"] = c.get("/stats").json()["per_node"]

    c.post("/nodes/node5/decommission")
    wait_ring(c, {f"node{i}" for i in range(1, 5)})
    results["rebalance_after_removing_node5"] = r = timed_rebalance(c, "after removing node5")
    print("rebalance after removing node5 :", r)
    sh("docker compose --profile scale stop node5")
    t0 = time.time()  # wait until node5 is dead before re-activating it, so it can't re-enter the ring
    while any(n["node_id"] == "node5" and n["status"] == "alive" for n in c.get("/nodes").json()):
        if time.time() - t0 > 60:
            raise TimeoutError("node5 never reported dead")
        time.sleep(1)
    c.post("/nodes/node5/activate")  # so it re-joins if it is started again
    results["stats_after"] = c.get("/stats").json()["per_node"]

    (OUT / "placement_live.json").write_text(json.dumps(results, indent=2, default=str))
    print(f"-> {OUT / 'placement_live.json'}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks", type=int, default=100_000)
    ap.add_argument("--live", action="store_true")
    a = ap.parse_args()
    part_a(a.chunks)
    if a.live:
        part_b()


if __name__ == "__main__":
    main()
