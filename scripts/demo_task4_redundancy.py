"""Task 4 demo: replication with quorums, erasure coding, failure detection + repair, scrubbing.

Run with the cluster up (docker compose up -d --build):
    python scripts/demo_task4_redundancy.py

It stops node4 for about a minute and starts it again at the end.
"""
import hashlib
import random
import subprocess
import time

import httpx

API = "http://localhost:8100"
NODE_URL = {f"node{i}": f"http://localhost:800{i}" for i in range(1, 6)}
MB = 1024 * 1024


def step(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 74 - len(title)), flush=True)


def sh(cmd: str) -> None:
    print(f"$ {cmd}", flush=True)
    subprocess.run(cmd, shell=True, check=True, capture_output=True)


def wait_for(cond, timeout: float, what: str):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = cond()
        if v:
            return v
        time.sleep(1)
    raise TimeoutError(what)


def show_layout(c: httpx.Client, bucket: str, key: str) -> None:
    for ch in c.get(f"/meta/{bucket}/{key}").json()["chunk_list"]:
        if ch["scheme"] == "ec":
            where = "  ".join(f"f{f['idx']}->{f['node']}" for f in ch["fragments"])
            print(f"  chunk #{ch['idx']} {ch['chunk_hash'][:12]}  EC {ch['k']}+{ch['n'] - ch['k']}:  {where}")
        else:
            print(f"  chunk #{ch['idx']} {ch['chunk_hash'][:12]}  {ch['n']} replicas: {ch['replicas']}")


def verify(c: httpx.Client, bucket: str, key: str, expected: str) -> str:
    t0 = time.time()
    r = c.get(f"/objects/{bucket}/{key}")
    ok = r.status_code == 200 and hashlib.sha256(r.content).hexdigest() == expected
    return f"GET {bucket}/{key}: HTTP {r.status_code}, {len(r.content) / MB:.0f} MB, sha256 ok={ok}, {time.time() - t0:.2f}s"


def main() -> None:
    c = httpx.Client(base_url=API, timeout=300)
    rng = random.Random(4)
    for b, k in (("rep3", "video.bin"), ("rep3", "during-failure.bin"), ("ec42", "video.bin")):
        c.delete(f"/objects/{b}/{k}")  # start clean so the demo can be re-run

    step("1. Buckets with different redundancy policies")
    print(c.put("/buckets/rep3", params={"redundancy": "replication", "replicas": 3}).json())
    print(c.put("/buckets/ec42", params={"redundancy": "ec", "k": 4, "m": 2}).json())

    step("2. Upload the same 16 MB file to both buckets (4 chunks of 4 MiB)")
    data = rng.randbytes(16 * MB)
    sha = hashlib.sha256(data).hexdigest()
    for b in ("rep3", "ec42"):
        data_b = data if b == "rep3" else data[::-1]  # different bytes, so dedup does not share chunks
        r = c.put(f"/objects/{b}/video.bin", content=data_b).json()
        print(f"{b}: policy={r['policy']}  chunks={r['chunks']}  copies written={r['copies_written']}  "
              f"bytes written to nodes={r['bytes_uploaded_to_nodes'] / MB:.1f} MB")
        show_layout(c, b, "video.bin")
    sha_ec = hashlib.sha256(data[::-1]).hexdigest()

    step("3. Storage overhead per bucket")
    for b in ("rep3", "ec42"):
        s = c.get("/stats", params={"bucket": b}).json()
        print(f"{b}: unique data {s['unique_bytes'] / MB:.1f} MB -> stored {s['physical_bytes'] / MB:.1f} MB "
              f"(overhead x{s['redundancy_overhead']})")

    step("4. Node failure: stop node4")
    sh("docker stop node4")

    step("5. Quorum write while node4 is down but not yet detected (W=2 of N=3)")
    r = c.put("/objects/rep3/during-failure.bin", content=rng.randbytes(8 * MB)).json()
    print(f"upload succeeded: chunks={r['chunks']}, copies written={r['copies_written']} "
          f"(< {r['chunks'] * 3} because writes to node4 failed, but each chunk reached W=2)")

    step("6. Degraded reads while node4 is down")
    print(verify(c, "rep3", "video.bin", sha))
    print(verify(c, "ec42", "video.bin", sha_ec), "(rebuilt from the remaining fragments)")

    step("7. Failure detection (missed heartbeats) and automatic repair job")
    wait_for(lambda: any(n["node_id"] == "node4" and n["status"] == "dead" for n in c.get("/nodes").json()),
             40, "node4 not detected")
    print("metadata service marked node4 DEAD:",
          [(n["node_id"], n["status"], n["seconds_since_heartbeat"]) for n in c.get("/nodes").json()
           if n["node_id"] != "node5"])

    def finished_job():
        jobs = [j for j in c.get("/repair/jobs").json() if "node4 failed" in j["reason"]]
        return jobs[0] if jobs and jobs[0]["status"] != "running" else None

    job = wait_for(finished_job, 600, "repair job did not finish")
    print(f"repair job #{job['id']} ({job['reason']}): {job['status']} in {job['seconds']:.1f}s, "
          f"{job['chunks_done']}/{job['chunks_total']} chunks repaired, {job['blobs_written']} copies written, "
          f"read {job['bytes_read'] / MB:.1f} MB, wrote {job['bytes_written'] / MB:.1f} MB")
    print("degraded after repair:", {k: v for k, v in c.get("/repair/status").json().items() if k != "examples"})
    print("layout after repair:")
    show_layout(c, "rep3", "video.bin")
    show_layout(c, "ec42", "video.bin")

    step("8. Bring node4 back")
    sh("docker start node4")
    wait_for(lambda: "node4" in c.get("/health").json()["alive"], 30, "node4 did not come back")
    print("alive:", c.get("/health").json()["alive"])

    step("9. Bit rot: corrupt one replica and one EC fragment on disk, then scrub")
    rep_chunk = c.get("/meta/rep3/video.bin").json()["chunk_list"][0]
    ec_chunk = c.get("/meta/ec42/video.bin").json()["chunk_list"][0]
    victim_rep = rep_chunk["replicas"][0]
    victim_frag = ec_chunk["fragments"][1]
    print("corrupt", httpx.post(f"{NODE_URL[victim_rep]}/admin/corrupt/{rep_chunk['chunk_hash']}").json(),
          "on", victim_rep)
    print("corrupt", httpx.post(f"{NODE_URL[victim_frag['node']]}/admin/corrupt/{victim_frag['blob']}").json(),
          "on", victim_frag["node"])
    res = c.post("/admin/scrub").json()
    print(f"scrub run {res['scrub_run']}: checked {res['checked']} blobs in {res['seconds']}s, "
          f"corrupt={res['corrupt']}, missing={res['missing']}, repair={res['repair']}")
    for node, r in res["nodes"].items():
        print(f"  {node}: {r}")
    print("layout after scrub + repair (the bad copies were replaced from healthy ones):")
    show_layout(c, "rep3", "video.bin")
    show_layout(c, "ec42", "video.bin")
    print(verify(c, "rep3", "video.bin", sha))
    print(verify(c, "ec42", "video.bin", sha_ec))


if __name__ == "__main__":
    main()
