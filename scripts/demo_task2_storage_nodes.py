"""Task 2 demo: exercises the 4 running storage nodes end to end.

Run with the cluster up (docker compose up -d --build):
    python scripts/demo_task2_storage_nodes.py
"""
import hashlib
import os
import time

import httpx

NODES = {f"node{i}": f"http://localhost:800{i}" for i in range(1, 5)}
METADATA = "http://localhost:8100"


def step(title: str) -> None:
    print(f"\n=== {title} " + "=" * (70 - len(title)))


def main() -> None:
    c = httpx.Client(timeout=5)

    step("1. Health of every storage node")
    for name, url in NODES.items():
        j = c.get(f"{url}/health").json()
        print(f"{name}: status={j['status']} chunks={j['chunk_count']} "
              f"free={j['free_bytes'] / 2**30:.1f} GiB")

    step("2. PUT one chunk per node, addressed by its SHA-256")
    chunks = {}
    for name, url in NODES.items():
        data = os.urandom(64 * 1024)
        digest = hashlib.sha256(data).hexdigest()
        r = c.put(f"{url}/chunks/{digest}", content=data)
        chunks[name] = (digest, data)
        print(f"{name}: PUT {digest[:16]}...  -> {r.status_code} {r.json()}")

    step("3. PUT the same chunk again (idempotent, stored once)")
    digest, data = chunks["node1"]
    r = c.put(f"{NODES['node1']}/chunks/{digest}", content=data)
    print(f"node1: PUT {digest[:16]}... -> {r.status_code} created={r.json()['created']}")

    step("4. HEAD + GET with hash verification")
    for name, (digest, data) in chunks.items():
        head = c.head(f"{NODES[name]}/chunks/{digest}")
        got = c.get(f"{NODES[name]}/chunks/{digest}")
        ok = hashlib.sha256(got.content).hexdigest() == digest and got.content == data
        print(f"{name}: HEAD {head.status_code} size={head.headers.get('content-length')} | "
              f"GET {got.status_code} verified={ok}")

    step("5. PUT with wrong hash is rejected")
    r = c.put(f"{NODES['node2']}/chunks/{hashlib.sha256(b'expected').hexdigest()}", content=b"other bytes")
    print(f"node2: -> {r.status_code} {r.json()}")

    step("6. Bit-rot simulation: corrupt a chunk on disk, then read it")
    digest, _ = chunks["node3"]
    print("corrupt:", c.post(f"{NODES['node3']}/admin/corrupt/{digest}").json())
    r = c.get(f"{NODES['node3']}/chunks/{digest}")
    print(f"node3: GET {digest[:16]}... -> {r.status_code} {r.json()}")

    step("7. DELETE a chunk")
    digest, _ = chunks["node4"]
    print(f"node4: DELETE -> {c.delete(f'{NODES['node4']}/chunks/{digest}').status_code}, "
          f"HEAD afterwards -> {c.head(f'{NODES['node4']}/chunks/{digest}').status_code}")

    step("8. Heartbeats received by the metadata service")
    time.sleep(3.5)  # let one heartbeat cycle report the new counts
    for n in c.get(f"{METADATA}/nodes").json():
        print(f"{n['node_id']}: {n['status']:5s} last_hb={n['seconds_since_heartbeat']}s ago "
              f"chunks={n['chunk_count']} used={n['used_bytes']} B "
              f"free={n['free_bytes'] / 2**30:.1f} GiB  url={n['url']}")


if __name__ == "__main__":
    main()
