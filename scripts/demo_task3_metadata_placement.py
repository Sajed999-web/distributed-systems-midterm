"""Task 3 demo: chunking, metadata in PostgreSQL, ring placement, deduplication.

Run with the cluster up (docker compose up -d --build):
    python scripts/demo_task3_metadata_placement.py
"""
import hashlib
import os
import random

import httpx

API = "http://localhost:8100"
NODES = {f"node{i}": f"http://localhost:800{i}" for i in range(1, 5)}
MB = 1024 * 1024


def step(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 74 - len(title)))


def mb(n: int) -> str:
    return f"{n / MB:.2f} MB"


def main() -> None:
    c = httpx.Client(base_url=API, timeout=120)
    rng = random.Random(7)
    for key in ("demo/videos/clip.bin", "demo/backup/clip-copy.bin", "cmp-fixed/doc.bin", "cmp-cdc/doc.bin"):
        c.delete(f"/objects/{key}")  # start clean so the demo can be re-run

    step("1. Cluster membership and hash ring")
    for n in c.get("/nodes").json():
        print(f"{n['node_id']}: {n['status']:5s} in_ring={n['in_ring']} state={n['state']}")
    print("ring:", c.get("/health").json()["ring"])

    step("2. Create bucket 'demo' and upload a 20 MB file with FIXED 4 MB chunks")
    c.put("/buckets/demo")
    original = rng.randbytes(20 * MB)
    r = c.put("/objects/demo/videos/clip.bin", params={"chunking": "fixed"}, content=original).json()
    print({k: r[k] for k in ("key", "version", "size", "chunking", "chunks", "new_chunks", "deduplicated_chunks")})

    step("3. Object metadata stored in PostgreSQL (ordered chunk list + replicas)")
    meta = c.get("/meta/demo/videos/clip.bin").json()
    print(f"bucket={meta['bucket']} key={meta['key']} version={meta['version']} size={meta['size']} "
          f"sha256={meta['sha256'][:16]}... created={meta['created_at']}")
    for ch in meta["chunk_list"]:
        print(f"  #{ch['idx']}  {ch['chunk_hash'][:16]}...  offset={ch['offset']:>9}  "
              f"size={ch['size']:>8}  refcount={ch['refcount']}  -> {ch['replicas']}")

    step("4. Ring placement of the first chunk (N = 3 distinct nodes, primary first)")
    print(c.get(f"/placement/{meta['chunk_list'][0]['chunk_hash']}").json())

    step("5. Download and verify")
    data = c.get("/objects/demo/videos/clip.bin").content
    print(f"downloaded {mb(len(data))}, sha256 matches: {hashlib.sha256(data).hexdigest() == meta['sha256']}")

    step("6. Deduplication: upload the SAME bytes under a different key")
    s0 = c.get("/stats").json()
    r = c.put("/objects/demo/backup/clip-copy.bin", params={"chunking": "fixed"}, content=original).json()
    s1 = c.get("/stats").json()
    print(f"chunks={r['chunks']} new={r['new_chunks']} deduplicated={r['deduplicated_chunks']} "
          f"bytes sent to nodes={r['bytes_uploaded_to_nodes']}")
    print(f"logical bytes {mb(s0['logical_bytes'])} -> {mb(s1['logical_bytes'])}, "
          f"physical bytes {mb(s0['physical_bytes'])} -> {mb(s1['physical_bytes'])} (unchanged)")
    first = c.get("/meta/demo/videos/clip.bin").json()["chunk_list"][0]
    print(f"refcount of chunk #0 is now {first['refcount']}")

    step("7. Reference counting: delete one copy, then the other")
    first_hash, replicas = first["chunk_hash"], first["replicas"]
    r = c.delete("/objects/demo/backup/clip-copy.bin").json()
    print("delete copy    :", r)
    print("  refcount of chunk #0:", c.get("/meta/demo/videos/clip.bin").json()["chunk_list"][0]["refcount"])
    print("  HEAD chunk #0 on its nodes:",
          {n: httpx.head(f"{NODES[n]}/chunks/{first_hash}").status_code for n in replicas})
    r = c.delete("/objects/demo/videos/clip.bin").json()
    print("delete original:", r)
    print("  HEAD chunk #0 on its nodes:",
          {n: httpx.head(f"{NODES[n]}/chunks/{first_hash}").status_code for n in replicas}, "(404 = removed)")

    step("8. Fixed-size vs content-defined chunking: insert 100 bytes near the start")
    doc = rng.randbytes(20 * MB)
    edited = doc[:5000] + os.urandom(100) + doc[5000:]
    for method in ("fixed", "cdc"):
        c.put(f"/buckets/cmp-{method}")
        v1 = c.put(f"/objects/cmp-{method}/doc.bin", params={"chunking": method}, content=doc).json()
        v2 = c.put(f"/objects/cmp-{method}/doc.bin", params={"chunking": method}, content=edited).json()
        print(f"{method:5s}: v1 chunks={v1['chunks']}  v2 chunks={v2['chunks']}  "
              f"v2 NEW chunks={v2['new_chunks']}  v2 reused={v2['deduplicated_chunks']}  "
              f"(v2 is version {v2['version']})")

    step("9. Cluster storage statistics")
    s = c.get("/stats").json()
    print({k: s[k] for k in ("objects", "logical_bytes", "unique_chunks", "unique_bytes",
                             "stored_blobs", "physical_bytes", "dedup_ratio", "storage_overhead")})
    for n in s["per_node"]:
        print(f"  {n['node_id']}: {n['blobs']} chunks, {mb(n['bytes'])}")


if __name__ == "__main__":
    main()
