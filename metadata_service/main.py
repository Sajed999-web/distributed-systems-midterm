"""Metadata & placement service.

Responsibilities
  * node registry: receives heartbeats, tracks liveness, builds the hash ring (cluster.py)
  * write path:  spool upload -> split into chunks (fixed or FastCDC) ->
                 SHA-256 each chunk -> deduplicate (refcount) -> store new
                 chunks with the bucket's redundancy policy (redundancy.py):
                 N-way replication with quorum writes, or Reed-Solomon k+m
  * read path:   ordered chunk list from PostgreSQL -> read each chunk with a
                 read quorum / from any k fragments -> verify SHA-256 -> stream
  * delete path: decrement refcounts, garbage-collect chunks that reach 0
  * failure handling: repair jobs and the background scrubber (repair.py)
  * placement experiments (simulate adding/removing a node) and rebalancing
"""
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from collections import Counter
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from metadata_service import redundancy, repair
from metadata_service.chunking import chunk_file
from metadata_service.cluster import (HEARTBEAT_TIMEOUT, VNODES, cluster, io_pool, monitor_loop, node_delete,
                                      node_put, task_pool)
from metadata_service.db import init_db, pool
from metadata_service.redundancy import Policy
from metadata_service.ring import HashRing

DEFAULT_REPLICAS = int(os.getenv("REPLICATION_FACTOR", "3"))
SPOOL_DIR = os.getenv("SPOOL_DIR", tempfile.gettempdir())
DEFAULT_CHUNKING = os.getenv("DEFAULT_CHUNKING", "fixed")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [metadata] %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("metadata")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    threading.Thread(target=monitor_loop, daemon=True).start()
    repair.start()
    yield


app = FastAPI(title="Metadata & placement service", lifespan=lifespan)


# ============================================================ nodes
class Heartbeat(BaseModel):
    node_id: str
    url: str
    chunk_count: int
    used_bytes: int
    free_bytes: int
    total_bytes: int


@app.post("/nodes/heartbeat")
def heartbeat(hb: Heartbeat):
    with pool.connection() as conn:
        conn.execute(
            """INSERT INTO nodes (node_id, url, chunk_count, used_bytes, free_bytes, total_bytes, last_heartbeat)
               VALUES (%(node_id)s, %(url)s, %(chunk_count)s, %(used_bytes)s, %(free_bytes)s, %(total_bytes)s, now())
               ON CONFLICT (node_id) DO UPDATE SET url = EXCLUDED.url, chunk_count = EXCLUDED.chunk_count,
                 used_bytes = EXCLUDED.used_bytes, free_bytes = EXCLUDED.free_bytes,
                 total_bytes = EXCLUDED.total_bytes, last_heartbeat = now()""",
            hb.model_dump(),
        )
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT n.*, EXTRACT(EPOCH FROM now() - n.last_heartbeat)::float AS age,
                      (SELECT count(*) FROM chunk_replicas r WHERE r.node_id = n.node_id) AS replicas_in_metadata,
                      (SELECT count(*) FROM chunk_fragments f WHERE f.node_id = n.node_id) AS fragments_in_metadata
               FROM nodes n ORDER BY node_id"""
        ).fetchall()
    return [{
        "node_id": r["node_id"], "url": r["url"], "state": r["state"],
        "status": "alive" if r["age"] is not None and r["age"] <= HEARTBEAT_TIMEOUT else "dead",
        "in_ring": r["node_id"] in cluster.ring.nodes,
        "seconds_since_heartbeat": round(r["age"], 1) if r["age"] is not None else None,
        "chunk_count": r["chunk_count"],
        "chunks_in_metadata": r["replicas_in_metadata"] + r["fragments_in_metadata"],
        "replicas_in_metadata": r["replicas_in_metadata"], "fragments_in_metadata": r["fragments_in_metadata"],
        "used_bytes": r["used_bytes"], "free_bytes": r["free_bytes"], "total_bytes": r["total_bytes"],
    } for r in rows]


def _set_state(node_id: str, state: str):
    with pool.connection() as conn:
        if conn.execute("UPDATE nodes SET state = %s WHERE node_id = %s", (state, node_id)).rowcount == 0:
            raise HTTPException(404, "unknown node")
    cluster.refresh()
    return {"node_id": node_id, "state": state, "ring": sorted(cluster.ring.nodes)}


@app.post("/nodes/{node_id}/decommission")
def decommission(node_id: str):
    """Remove a node from the ring (its data is moved off by /admin/rebalance)."""
    return _set_state(node_id, "decommissioned")


@app.post("/nodes/{node_id}/activate")
def activate(node_id: str):
    return _set_state(node_id, "active")


# ============================================================ buckets
@app.put("/buckets/{name}", status_code=201)
def create_bucket(name: str,
                  redundancy: str = Query("replication", pattern="^(replication|ec)$"),
                  replicas: int = Query(DEFAULT_REPLICAS, ge=1, le=10),
                  k: int = Query(4, ge=1, le=16), m: int = Query(2, ge=1, le=16)):
    """Create a bucket with its redundancy policy: N-way replication or erasure coding k+m."""
    with pool.connection() as conn:
        conn.execute("""INSERT INTO buckets (name, redundancy, replicas, ec_k, ec_m) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING""", (name, redundancy, replicas, k, m))
        b = conn.execute("SELECT * FROM buckets WHERE name = %s", (name,)).fetchone()
    w, r = redundancy_quorums(b)
    return {"bucket": name, "policy": Policy.from_bucket(b).describe(), "write_quorum": w, "read_quorum": r}


def redundancy_quorums(b: dict) -> tuple[int, int]:
    if b["redundancy"] == "ec":
        return b["ec_k"] + 1, b["ec_k"]
    return redundancy.quorums(b["replicas"])


@app.get("/buckets")
def list_buckets():
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT b.*, count(o.id) AS objects, coalesce(sum(o.size), 0) AS bytes
               FROM buckets b LEFT JOIN objects o ON o.bucket = b.name GROUP BY b.name ORDER BY b.name"""
        ).fetchall()
    return [{**r, "policy": Policy.from_bucket(r).describe()} for r in rows]


# ============================================================ write path
def ingest(bucket: str, key: str, path: str, size: int, sha: str, content_type: str | None,
           method: str, tags: dict) -> dict:
    t0 = time.time()
    bounds = chunk_file(path, method)
    pieces = []  # (idx, hash, offset, length)
    with open(path, "rb") as f:
        for i, (off, ln) in enumerate(bounds):
            f.seek(off)
            pieces.append((i, hashlib.sha256(f.read(ln)).hexdigest(), off, ln))
    t_chunk = time.time() - t0
    refs = Counter(h for _, h, _, _ in pieces)
    where = {h: (off, ln) for _, h, off, ln in pieces}

    with pool.connection() as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{bucket}/{key}",))
        b = conn.execute("SELECT * FROM buckets WHERE name = %s", (bucket,)).fetchone()
        if not b:
            raise HTTPException(404, f"bucket {bucket!r} does not exist")
        policy = Policy.from_bucket(b)

        # Deduplication: increment the refcount of every chunk; only chunks that
        # are new (or have lost all their copies) need to be sent to storage nodes.
        # An existing chunk keeps the redundancy scheme it was first stored with.
        to_upload = []
        for h in sorted(refs):  # fixed order avoids deadlocks between uploads
            p = policy if where[h][1] > 0 else Policy("replication", DEFAULT_REPLICAS, 1)
            row = conn.execute(
                """INSERT INTO chunks (hash, size, refcount, scheme, n, k) VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (hash) DO UPDATE SET refcount = chunks.refcount + EXCLUDED.refcount
                   RETURNING (xmax = 0) AS inserted, scheme, n, k""",
                (h, where[h][1], refs[h], p.scheme, p.n, p.k),
            ).fetchone()
            if row["inserted"] or not conn.execute(
                    """SELECT 1 FROM chunk_replicas WHERE hash = %(h)s
                       UNION ALL SELECT 1 FROM chunk_fragments WHERE hash = %(h)s LIMIT 1""", {"h": h}).fetchone():
                to_upload.append((h, Policy(row["scheme"], row["n"], row["k"])))

        def upload(item) -> list[redundancy.Placement]:
            h, p = item
            off, ln = where[h]
            with open(path, "rb") as f:
                f.seek(off)
                return redundancy.store(h, f.read(ln), p)

        try:
            placed = dict(zip([h for h, _ in to_upload], task_pool.map(upload, to_upload)))
        except IOError as e:
            raise HTTPException(503, str(e))
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO chunk_replicas (hash, node_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            [(h, p.node) for h, ps in placed.items() for p in ps if p.idx is None])
            cur.executemany("""INSERT INTO chunk_fragments (hash, idx, blob_hash, size, node_id) VALUES (%s, %s, %s, %s, %s)
                               ON CONFLICT DO NOTHING""",
                            [(h, p.idx, p.blob_hash, p.size, p.node) for h, ps in placed.items() for p in ps
                             if p.idx is not None])

        version = conn.execute("SELECT coalesce(max(version), 0) + 1 AS v FROM objects WHERE bucket = %s AND key = %s",
                               (bucket, key)).fetchone()["v"]
        obj = conn.execute(
            """INSERT INTO objects (bucket, key, version, size, sha256, content_type, chunking, chunk_count, tags)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id, created_at""",
            (bucket, key, version, size, sha, content_type, method, len(pieces), json.dumps(tags)),
        ).fetchone()
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO object_chunks (object_id, idx, chunk_hash, "offset", size) VALUES (%s, %s, %s, %s, %s)',
                            [(obj["id"], i, h, off, ln) for i, h, off, ln in pieces])

    return {
        "bucket": bucket, "key": key, "version": version, "size": size, "sha256": sha,
        "policy": policy.describe(), "chunking": method, "chunks": len(pieces), "unique_chunks": len(refs),
        "new_chunks": len(to_upload), "deduplicated_chunks": len(refs) - len(to_upload),
        "bytes_uploaded_to_nodes": sum(p.size for ps in placed.values() for p in ps),
        "copies_written": sum(len(ps) for ps in placed.values()),
        "chunking_seconds": round(t_chunk, 3), "total_seconds": round(time.time() - t0, 3),
        "created_at": obj["created_at"],
    }


@app.put("/objects/{bucket}/{key:path}", status_code=201)
async def put_object(bucket: str, key: str, request: Request,
                     chunking: str = Query(DEFAULT_CHUNKING, pattern="^(fixed|cdc)$"),
                     tags: str = Query("{}", description="JSON object of user-defined tags")):
    try:
        tag_dict = json.loads(tags)
    except json.JSONDecodeError:
        raise HTTPException(400, "tags must be a JSON object")
    sha, size = hashlib.sha256(), 0
    fd, path = tempfile.mkstemp(dir=SPOOL_DIR, prefix="upload-")
    try:
        with os.fdopen(fd, "wb") as f:
            async for block in request.stream():
                f.write(block)
                sha.update(block)
                size += len(block)
        return await run_in_threadpool(ingest, bucket, key, path, size, sha.hexdigest(),
                                       request.headers.get("content-type"), chunking, tag_dict)
    finally:
        os.remove(path)


# ============================================================ read path
def _find_object(conn, bucket: str, key: str, version: int | None) -> dict:
    sql = "SELECT * FROM objects WHERE bucket = %s AND key = %s"
    args = [bucket, key]
    if version is not None:
        sql += " AND version = %s"
        args.append(version)
    obj = conn.execute(sql + " ORDER BY version DESC LIMIT 1", args).fetchone()
    if not obj:
        raise HTTPException(404, "object not found")
    return obj


def _chunk_list(conn, object_id: int) -> list[dict]:
    return conn.execute(
        """SELECT oc.idx, oc.chunk_hash, oc."offset", oc.size, c.refcount, c.scheme, c.n, c.k,
                  coalesce((SELECT array_agg(r.node_id ORDER BY r.node_id) FROM chunk_replicas r
                            WHERE r.hash = oc.chunk_hash), '{}') AS replicas,
                  coalesce((SELECT json_agg(json_build_object('idx', f.idx, 'blob', f.blob_hash, 'node', f.node_id)
                                            ORDER BY f.idx)
                            FROM chunk_fragments f WHERE f.hash = oc.chunk_hash), '[]') AS fragments
           FROM object_chunks oc JOIN chunks c ON c.hash = oc.chunk_hash
           WHERE oc.object_id = %s ORDER BY oc.idx""",
        (object_id,),
    ).fetchall()


@app.get("/objects/{bucket}/{key:path}")
def get_object(bucket: str, key: str, version: int | None = None):
    with pool.connection() as conn:
        obj = _find_object(conn, bucket, key, version)
        chunks = _chunk_list(conn, obj["id"])

    def stream():
        for c in chunks:
            yield redundancy.read(c)

    return StreamingResponse(stream(), media_type=obj["content_type"] or "application/octet-stream",
                             headers={"Content-Length": str(obj["size"]), "ETag": obj["sha256"],
                                      "X-Object-Version": str(obj["version"])})


@app.get("/meta/{bucket}/{key:path}")
def object_metadata(bucket: str, key: str, version: int | None = None):
    with pool.connection() as conn:
        obj = _find_object(conn, bucket, key, version)
        obj["chunk_list"] = _chunk_list(conn, obj["id"])
        obj["versions"] = [r["version"] for r in conn.execute(
            "SELECT version FROM objects WHERE bucket = %s AND key = %s ORDER BY version", (bucket, key))]
    return obj


@app.get("/objects/{bucket}")
def list_objects(bucket: str, prefix: str = "", limit: int = 1000):
    with pool.connection() as conn:
        return conn.execute(
            """SELECT DISTINCT ON (key) key, version, size, sha256, chunking, chunk_count, created_at, tags
               FROM objects WHERE bucket = %s AND key LIKE %s ORDER BY key, version DESC LIMIT %s""",
            (bucket, prefix.replace("%", r"\%").replace("_", r"\_") + "%", limit),
        ).fetchall()


# ============================================================ delete path
@app.delete("/objects/{bucket}/{key:path}")
def delete_object(bucket: str, key: str, version: int | None = None):
    """Delete one version (or all versions) and garbage-collect unreferenced chunks."""
    with pool.connection() as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{bucket}/{key}",))
        sql, args = "SELECT id FROM objects WHERE bucket = %s AND key = %s", [bucket, key]
        if version is not None:
            sql += " AND version = %s"
            args.append(version)
        ids = [r["id"] for r in conn.execute(sql, args)]
        if not ids:
            raise HTTPException(404, "object not found")
        refs = conn.execute("SELECT chunk_hash, count(*) AS n FROM object_chunks WHERE object_id = ANY(%s) "
                            "GROUP BY chunk_hash ORDER BY chunk_hash", (ids,)).fetchall()
        conn.execute("DELETE FROM objects WHERE id = ANY(%s)", (ids,))
        garbage = []
        for r in refs:
            left = conn.execute("UPDATE chunks SET refcount = refcount - %s WHERE hash = %s RETURNING refcount",
                                (r["n"], r["chunk_hash"])).fetchone()["refcount"]
            if left <= 0:
                garbage.append(r["chunk_hash"])
        blobs = conn.execute(
            """SELECT hash AS blob, node_id FROM chunk_replicas WHERE hash = ANY(%(g)s)
               UNION ALL SELECT blob_hash, node_id FROM chunk_fragments WHERE hash = ANY(%(g)s)""",
            {"g": garbage}).fetchall()
        # delete from nodes while the chunk rows are still locked, so a concurrent
        # upload of the same content cannot be lost
        list(io_pool.map(lambda r: node_delete(r["node_id"], r["blob"]), blobs))
        conn.execute("DELETE FROM chunks WHERE hash = ANY(%s)", (garbage,))
    return {"deleted_versions": len(ids), "chunk_references_released": sum(r["n"] for r in refs),
            "chunks_garbage_collected": len(garbage), "blobs_deleted": len(blobs)}


# ============================================================ stats
@app.get("/stats")
def stats(bucket: str | None = None):
    """Logical vs unique vs physical bytes (optionally for the chunks of one bucket)."""
    obj_filter = "WHERE bucket = %(b)s" if bucket else ""
    chunk_filter = ("WHERE hash IN (SELECT oc.chunk_hash FROM object_chunks oc JOIN objects o ON o.id = oc.object_id "
                    "WHERE o.bucket = %(b)s)") if bucket else ""
    args = {"b": bucket}
    with pool.connection() as conn:
        o = conn.execute(f"SELECT count(*) AS objects, coalesce(sum(size), 0) AS logical_bytes FROM objects {obj_filter}",
                         args).fetchone()
        c = conn.execute(f"SELECT count(*) AS unique_chunks, coalesce(sum(size), 0) AS unique_bytes FROM chunks {chunk_filter}",
                         args).fetchone()
        p = conn.execute(f"""
            WITH ch AS (SELECT hash, size FROM chunks {chunk_filter}),
                 copies AS (SELECT r.node_id, ch.size FROM chunk_replicas r JOIN ch USING (hash)
                            UNION ALL SELECT f.node_id, f.size FROM chunk_fragments f JOIN ch USING (hash))
            SELECT count(*) AS stored_blobs, coalesce(sum(size), 0) AS physical_bytes FROM copies""", args).fetchone()
        per_node = conn.execute(f"""
            WITH ch AS (SELECT hash, size FROM chunks {chunk_filter}),
                 copies AS (SELECT r.node_id, ch.size, 'replica' AS kind FROM chunk_replicas r JOIN ch USING (hash)
                            UNION ALL SELECT f.node_id, f.size, 'fragment' FROM chunk_fragments f JOIN ch USING (hash))
            SELECT node_id, count(*) AS blobs, count(*) FILTER (WHERE kind = 'replica') AS replicas,
                   count(*) FILTER (WHERE kind = 'fragment') AS fragments, sum(size) AS bytes
            FROM copies GROUP BY node_id ORDER BY node_id""", args).fetchall()
        by_scheme = conn.execute(f"SELECT scheme, count(*) AS chunks, sum(size) AS bytes FROM chunks {chunk_filter} "
                                 "GROUP BY scheme", args).fetchall()
    return {
        "bucket": bucket, **o, **c, **p,
        "dedup_ratio": round(o["logical_bytes"] / c["unique_bytes"], 3) if c["unique_bytes"] else None,
        "redundancy_overhead": round(p["physical_bytes"] / c["unique_bytes"], 3) if c["unique_bytes"] else None,
        "storage_overhead": round(p["physical_bytes"] / o["logical_bytes"], 3) if o["logical_bytes"] else None,
        "by_scheme": by_scheme, "per_node": per_node,
    }


# ============================================================ repair & scrub
@app.get("/repair/status")
def repair_status():
    s = repair.degraded_summary()
    with pool.connection() as conn:
        s["running_jobs"] = conn.execute("SELECT * FROM repair_jobs WHERE status = 'running' ORDER BY id").fetchall()
    return s


@app.get("/repair/jobs")
def repair_jobs(limit: int = 20):
    with pool.connection() as conn:
        return conn.execute("SELECT *, EXTRACT(EPOCH FROM coalesce(finished_at, now()) - started_at)::float AS seconds "
                            "FROM repair_jobs ORDER BY id DESC LIMIT %s", (limit,)).fetchall()


@app.post("/admin/repair")
def trigger_repair(wait: bool = True):
    """Find all degraded chunks and repair them now."""
    if wait:
        return repair.run_job("manual")
    repair.request_repair("manual")
    return {"queued": True}


@app.post("/admin/scrub")
def trigger_scrub():
    """Re-read and verify every blob on every live node, repair what is corrupt or missing."""
    return repair.scrub()


@app.get("/scrub/runs")
def scrub_runs(limit: int = 10):
    with pool.connection() as conn:
        return conn.execute("SELECT * FROM scrub_runs ORDER BY id DESC LIMIT %s", (limit,)).fetchall()


# ============================================================ placement
@app.get("/placement/{chunk_hash}")
def placement(chunk_hash: str):
    with pool.connection() as conn:
        actual = [r["node_id"] for r in conn.execute(
            "SELECT node_id FROM chunk_replicas WHERE hash = %s ORDER BY node_id", (chunk_hash,))]
        frags = conn.execute("SELECT idx, node_id FROM chunk_fragments WHERE hash = %s ORDER BY idx",
                             (chunk_hash,)).fetchall()
    return {"hash": chunk_hash, "ring_targets": cluster.targets(chunk_hash, DEFAULT_REPLICAS),
            "stored_on": actual, "fragments": frags}


def _mod_targets(h: str, nodes: list[str]) -> set[str]:
    i = int(h[:16], 16) % len(nodes)
    return {nodes[(i + k) % len(nodes)] for k in range(min(DEFAULT_REPLICAS, len(nodes)))}


@app.get("/placement-simulation")
def placement_simulation(add: list[str] = Query([]), remove: list[str] = Query([])):
    """Percentage of stored chunk replicas that would move if nodes were added/removed."""
    with pool.connection() as conn:
        hashes = [r["hash"] for r in conn.execute("SELECT hash FROM chunks WHERE scheme = 'replication'")]
    before = sorted(cluster.ring.nodes)
    after = sorted((set(before) | set(add)) - set(remove))
    old, new = HashRing(before, VNODES), HashRing(after, VNODES)
    n = min(DEFAULT_REPLICAS, len(after))
    moved = changed = mod_moved = 0
    for h in hashes:
        a, b = set(old.get_nodes(h, n)), set(new.get_nodes(h, n))
        moved += len(b - a)
        changed += a != b
        mod_moved += len(_mod_targets(h, after) - _mod_targets(h, before))
    total = len(hashes) * n or 1
    return {
        "nodes_before": before, "nodes_after": after, "chunks": len(hashes), "replicas": len(hashes) * n,
        "consistent_hashing": {"replicas_moved": moved, "percent_moved": round(100 * moved / total, 2),
                               "chunks_with_changed_placement": changed},
        "modulo_hashing": {"replicas_moved": mod_moved, "percent_moved": round(100 * mod_moved / total, 2)},
        "ideal_percent": round(100 * abs(len(after) - len(before)) / max(len(after), len(before)), 2),
    }


@app.post("/admin/rebalance")
def rebalance(dry_run: bool = False):
    """Move replicas so that every replicated chunk lives exactly on its ring targets."""
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT c.hash, c.size, c.n, coalesce(array_agg(r.node_id) FILTER (WHERE r.node_id IS NOT NULL), '{}') AS replicas
               FROM chunks c LEFT JOIN chunk_replicas r USING (hash) WHERE c.scheme = 'replication'
               GROUP BY c.hash, c.size, c.n"""
        ).fetchall()
    stats, stats_lock = Counter(), threading.Lock()

    def count(**kw) -> None:
        with stats_lock:
            stats.update(kw)

    def fix(row) -> None:
        h, current = row["hash"], set(row["replicas"])
        desired = set(cluster.targets(h, row["n"]))
        missing, extra = desired - current, current - desired
        count(chunks_checked=1, chunks_moved=int(bool(missing or extra)), target_replicas=len(desired))
        if dry_run:
            count(replicas_to_copy=len(missing), replicas_to_remove=len(extra))
            return
        if missing:
            data = redundancy.read({"chunk_hash": h, "scheme": "replication", "replicas": list(current),
                                    "n": 1, "fragments": []})
            for n in missing:
                node_put(n, h, data)
                with pool.connection() as conn:
                    conn.execute("INSERT INTO chunk_replicas VALUES (%s, %s) ON CONFLICT DO NOTHING", (h, n))
                count(replicas_copied=1, bytes_moved=row["size"])
        for n in extra:
            if n in cluster.alive:
                node_delete(n, h)
            with pool.connection() as conn:
                conn.execute("DELETE FROM chunk_replicas WHERE hash = %s AND node_id = %s", (h, n))
            count(replicas_removed=1)

    t0 = time.time()
    for err in task_pool.map(lambda r: _safe(fix, r), rows):
        if err:
            stats["errors"] += 1
    total = stats.pop("target_replicas", 0) or 1
    moved = stats["replicas_to_copy"] if dry_run else stats["replicas_copied"]
    return {**stats, "dry_run": dry_run, "ring": sorted(cluster.ring.nodes),
            "percent_replicas_moved": round(100 * moved / total, 2), "seconds": round(time.time() - t0, 2)}


def _safe(fn, *args):
    try:
        fn(*args)
    except Exception as e:  # reported in the summary, logged for debugging
        log.warning("rebalance: %s", e)
        return e
    return None


@app.get("/health")
def health():
    return {"status": "ok", "ring": sorted(cluster.ring.nodes), "alive": sorted(cluster.alive)}
