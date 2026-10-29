"""Failure handling: repair jobs and the background scrubber.

Repair
    A chunk is *degraded* when fewer of its copies are on live nodes than its
    policy asks for (replication: fewer than N live replicas, erasure coding:
    fewer than k+m live fragments). A repair job finds every degraded chunk and
    rebuilds the missing copies on other live nodes:
      replication  copy the chunk from a healthy replica to new ring targets
      EC           read any k fragments, decode, re-encode and write only the
                   missing fragments
    Jobs are started when a node is declared dead (missed heartbeats), when a
    read or the scrubber finds a bad copy, every REPAIR_SCAN_INTERVAL seconds
    as a safety net, or manually (POST /admin/repair). Progress and repair
    traffic (bytes read from healthy nodes / bytes written) are stored in the
    repair_jobs table.

Scrubbing
    Every SCRUB_INTERVAL seconds each live node re-reads and re-hashes all of
    its blobs (POST /scrub on the node; corrupt blobs are quarantined). The
    result is compared with PostgreSQL: copies that are corrupt or missing are
    dropped from the metadata and rebuilt from a healthy copy. Blobs on a node
    that no metadata refers to (orphans, e.g. left behind by a failed upload or
    by a node that was declared dead and came back) are deleted once they have
    been orphans for ORPHAN_GRACE seconds.
"""
import json
import logging
import os
import queue
import threading
import time
from collections import Counter

import httpx

from metadata_service import erasure, redundancy
from metadata_service.cluster import cluster, node_delete, node_get, node_put, node_scrub, task_pool
from metadata_service.db import pool

REPAIR_DELAY = float(os.getenv("REPAIR_DELAY", "0"))  # extra wait after a node is declared dead
REPAIR_SCAN_INTERVAL = float(os.getenv("REPAIR_SCAN_INTERVAL", "60"))
SCRUB_INTERVAL = float(os.getenv("SCRUB_INTERVAL", "300"))
ORPHAN_GRACE = float(os.getenv("ORPHAN_GRACE", "120"))

log = logging.getLogger("metadata")

_jobs: "queue.Queue[str]" = queue.Queue()
_orphan_seen: dict[tuple[str, str], float] = {}  # (node, blob) -> first time seen as orphan


class Unrecoverable(Exception):
    pass


# ------------------------------------------------------------ degraded chunks
DEGRADED_SQL = """
SELECT c.hash, c.scheme, c.n, c.k, c.size FROM chunks c
WHERE (c.scheme = 'replication'
       AND (SELECT count(*) FROM chunk_replicas r WHERE r.hash = c.hash AND r.node_id = ANY(%(alive)s))
           < LEAST(c.n, %(ring)s))
   OR (c.scheme = 'ec'
       AND (SELECT count(*) FROM chunk_fragments f WHERE f.hash = c.hash AND f.node_id = ANY(%(alive)s)) < c.n)
"""


def degraded_chunks(conn) -> list[dict]:
    alive = sorted(cluster.alive)
    return conn.execute(DEGRADED_SQL, {"alive": alive, "ring": len(cluster.ring.nodes)}).fetchall()


def degraded_summary() -> dict:
    with pool.connection() as conn:
        chunks = degraded_chunks(conn)
        hashes = [c["hash"] for c in chunks]
        objects = conn.execute(
            """SELECT DISTINCT o.bucket, o.key, o.version FROM objects o JOIN object_chunks oc ON oc.object_id = o.id
               WHERE oc.chunk_hash = ANY(%s) ORDER BY o.bucket, o.key LIMIT 100""", (hashes,)).fetchall()
        n_objects = conn.execute(
            "SELECT count(DISTINCT object_id) AS n FROM object_chunks WHERE chunk_hash = ANY(%s)", (hashes,)
        ).fetchone()["n"]
    return {"degraded_chunks": len(chunks), "degraded_objects": n_objects, "examples": objects,
            "by_scheme": dict(Counter(c["scheme"] for c in chunks))}


# ------------------------------------------------------------ repairing one chunk
def repair_chunk(h: str) -> dict:
    """Rebuild missing copies of one chunk. Returns traffic counters."""
    with pool.connection() as conn, conn.transaction():
        # lock the chunk so it cannot be garbage-collected or repaired twice meanwhile
        c = conn.execute("SELECT * FROM chunks WHERE hash = %s FOR UPDATE SKIP LOCKED", (h,)).fetchone()
        if c is None:
            return {"skipped": 1}
        if c["scheme"] == "ec":
            return _repair_ec(conn, c)
        return _repair_replicated(conn, c)


def _repair_replicated(conn, c: dict) -> dict:
    h = c["hash"]
    rows = [r["node_id"] for r in conn.execute("SELECT node_id FROM chunk_replicas WHERE hash = %s", (h,))]
    live = [n for n in rows if n in cluster.alive]
    stale = [n for n in rows if n not in cluster.alive]
    want = min(c["n"], len(cluster.ring.nodes))
    stats = Counter()
    if len(live) < want:
        data = None
        for node in live:
            try:
                data = node_get(node, h)
                break
            except (httpx.HTTPError, IOError):
                conn.execute("DELETE FROM chunk_replicas WHERE hash = %s AND node_id = %s", (h, node))
                live.remove(node)
        if data is None:
            raise Unrecoverable(f"chunk {h[:12]}: no healthy replica left")
        stats["bytes_read"] += len(data)
        for node in [n for n in cluster.ring_order(h) if n not in live][: want - len(live)]:
            node_put(node, h, data)
            conn.execute("INSERT INTO chunk_replicas (hash, node_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (h, node))
            stats["bytes_written"] += len(data)
            stats["blobs_written"] += 1
    if stale:  # copies on dead nodes are considered lost
        conn.execute("DELETE FROM chunk_replicas WHERE hash = %s AND node_id = ANY(%s)", (h, stale))
    return stats


def _repair_ec(conn, c: dict) -> dict:
    h, n, k = c["hash"], c["n"], c["k"]
    frags = {r["idx"]: r for r in conn.execute("SELECT * FROM chunk_fragments WHERE hash = %s", (h,))}
    live = {i: f for i, f in frags.items() if f["node_id"] in cluster.alive}
    missing = [i for i in range(n) if i not in live]
    stats = Counter()
    if not missing:
        return stats
    blobs = []
    for f in sorted(live.values(), key=lambda f: f["idx"]):
        if len(blobs) == k:
            break
        try:
            blobs.append(node_get(f["node_id"], f["blob_hash"]))
        except (httpx.HTTPError, IOError):
            missing.append(f["idx"])
            del live[f["idx"]]
    if len(blobs) < k:
        raise Unrecoverable(f"chunk {h[:12]}: only {len(blobs)} of k={k} fragments left")
    stats["bytes_read"] += sum(len(b) for b in blobs)
    new = {f.index: f for f in erasure.encode(h, erasure.decode(blobs), k, n - k)}
    load = Counter(f["node_id"] for f in live.values())
    order = cluster.ring_order(h)
    for i in sorted(missing):
        node = min(order, key=lambda x: (load[x], order.index(x)))  # node holding fewest fragments of this chunk
        node_put(node, new[i].blob_hash, new[i].blob)
        load[node] += 1
        conn.execute(
            """INSERT INTO chunk_fragments (hash, idx, blob_hash, size, node_id) VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (hash, idx) DO UPDATE SET node_id = EXCLUDED.node_id, blob_hash = EXCLUDED.blob_hash""",
            (h, i, new[i].blob_hash, len(new[i].blob), node))
        stats["bytes_written"] += len(new[i].blob)
        stats["blobs_written"] += 1
    return stats


# ------------------------------------------------------------ repair jobs
def request_repair(reason: str) -> None:
    _jobs.put(reason)


def _on_node_dead(node: str) -> None:
    threading.Timer(REPAIR_DELAY, request_repair, args=(f"node {node} failed (missed heartbeats)",)).start()


def _report_bad(h: str, node: str, idx: int | None) -> None:
    """Read repair: a live node returned a missing/corrupt copy -> drop it and rebuild."""
    if node not in cluster.alive:
        return  # dead nodes are handled by the node-failure repair
    with pool.connection() as conn:
        if idx is None:
            conn.execute("DELETE FROM chunk_replicas WHERE hash = %s AND node_id = %s", (h, node))
        else:
            conn.execute("DELETE FROM chunk_fragments WHERE hash = %s AND idx = %s AND node_id = %s", (h, idx, node))
    log.warning("read repair: bad copy of %s on %s", h[:12], node)
    request_repair(f"read repair: bad copy of chunk {h[:12]} on {node}")


def run_job(reason: str, only_if_needed: bool = False) -> dict | None:
    with pool.connection() as conn:
        todo = degraded_chunks(conn)
        if not todo and only_if_needed:
            return None
        job = conn.execute("INSERT INTO repair_jobs (reason, chunks_total) VALUES (%s, %s) RETURNING id",
                           (reason, len(todo))).fetchone()["id"]
    log.warning("repair job %d started (%s): %d degraded chunks", job, reason, len(todo))
    t0 = time.time()
    total, lock, last_flush = Counter(), threading.Lock(), [0.0]

    def flush(force: bool = False) -> None:
        if not force and time.time() - last_flush[0] < 1:
            return
        last_flush[0] = time.time()
        with pool.connection() as conn:
            conn.execute(
                """UPDATE repair_jobs SET chunks_done = %s, chunks_failed = %s, blobs_written = %s,
                   bytes_read = %s, bytes_written = %s WHERE id = %s""",
                (total["done"], total["failed"], total["blobs_written"], total["bytes_read"],
                 total["bytes_written"], job))

    def one(c: dict) -> None:
        try:
            s = repair_chunk(c["hash"])
            with lock:
                total.update(s)
                total["done"] += 1
        except Exception as e:  # unrecoverable or node error: counted, logged, retried by the next scan
            log.error("repair of %s failed: %s", c["hash"][:12], e)
            with lock:
                total["failed"] += 1
        with lock:
            flush()

    list(task_pool.map(one, todo))
    flush(force=True)
    status = "done" if total["failed"] == 0 else "failed"
    detail = {"seconds": round(time.time() - t0, 2), "by_scheme": dict(Counter(c["scheme"] for c in todo)),
              "skipped": total["skipped"]}
    with pool.connection() as conn:
        conn.execute("UPDATE repair_jobs SET status = %s, finished_at = now(), detail = %s WHERE id = %s",
                     (status, json.dumps(detail), job))
    log.warning("repair job %d %s in %.1fs: %d repaired, %d failed, read %d B, wrote %d B", job, status,
                time.time() - t0, total["done"], total["failed"], total["bytes_read"], total["bytes_written"])
    return {"job_id": job, "status": status}


def _worker() -> None:
    while True:
        reasons = [_jobs.get()]
        while not _jobs.empty():  # coalesce triggers that arrived meanwhile into one job
            reasons.append(_jobs.get_nowait())
        try:
            run_job("; ".join(dict.fromkeys(reasons)))
        except Exception:
            log.exception("repair job crashed")


def _periodic_scan() -> None:
    while True:
        time.sleep(REPAIR_SCAN_INTERVAL)
        try:
            run_job("periodic scan", only_if_needed=True)
        except Exception:
            log.exception("periodic repair scan failed")


# ------------------------------------------------------------ scrubbing
_scrub_lock = threading.Lock()


def scrub() -> dict:
    """Verify every blob on every live node, then repair what is corrupt or missing."""
    if not _scrub_lock.acquire(blocking=False):
        return {"status": "a scrub is already running"}
    try:
        with pool.connection() as conn:
            run = conn.execute("INSERT INTO scrub_runs DEFAULT VALUES RETURNING id").fetchone()["id"]
        t0, report, totals = time.time(), {}, Counter()
        for node in sorted(cluster.alive):
            # what the metadata says this node holds -- read BEFORE scanning the node (see module doc)
            with pool.connection() as conn:
                replicas = {r["hash"] for r in conn.execute("SELECT hash FROM chunk_replicas WHERE node_id = %s", (node,))}
                fragments = {r["blob_hash"]: (r["hash"], r["idx"]) for r in conn.execute(
                    "SELECT hash, idx, blob_hash FROM chunk_fragments WHERE node_id = %s", (node,))}
            try:
                res = node_scrub(node)
            except httpx.HTTPError as e:
                report[node] = {"error": str(e)}
                continue
            present, corrupt = set(res["present"]), set(res["corrupt"])
            expected = replicas | set(fragments)
            corrupt &= expected
            missing = expected - present - corrupt
            bad = missing | corrupt
            with pool.connection() as conn:
                for b in bad:
                    if b in replicas:
                        conn.execute("DELETE FROM chunk_replicas WHERE hash = %s AND node_id = %s", (b, node))
                    else:
                        h, idx = fragments[b]
                        conn.execute("DELETE FROM chunk_fragments WHERE hash = %s AND idx = %s", (h, idx))
            orphans_removed = 0
            now = time.time()
            for b in present - expected:
                first = _orphan_seen.setdefault((node, b), now)
                if now - first >= ORPHAN_GRACE:
                    node_delete(node, b)
                    _orphan_seen.pop((node, b), None)
                    orphans_removed += 1
            report[node] = {"checked": res["checked"], "corrupt": len(corrupt), "missing": len(missing),
                            "orphans_pending": len(present - expected) - orphans_removed,
                            "orphans_removed": orphans_removed, "node_seconds": res["seconds"]}
            totals.update(checked=res["checked"], corrupt=len(corrupt), missing=len(missing),
                          orphans_removed=orphans_removed)
        repair = None
        if totals["corrupt"] or totals["missing"]:
            repair = run_job(f"scrub run {run}: {totals['corrupt']} corrupt, {totals['missing']} missing copies")
        with pool.connection() as conn:
            conn.execute(
                """UPDATE scrub_runs SET finished_at = now(), blobs_checked = %s, corrupt = %s, missing = %s,
                   orphans_removed = %s, detail = %s WHERE id = %s""",
                (totals["checked"], totals["corrupt"], totals["missing"], totals["orphans_removed"],
                 json.dumps(report), run))
        log.info("scrub run %d: %s", run, dict(totals))
        return {"scrub_run": run, "seconds": round(time.time() - t0, 2), **totals, "nodes": report, "repair": repair}
    finally:
        _scrub_lock.release()


def _periodic_scrub() -> None:
    while True:
        time.sleep(SCRUB_INTERVAL)
        try:
            scrub()
        except Exception:
            log.exception("scrub failed")


def start() -> None:
    cluster.on_node_dead.append(_on_node_dead)
    redundancy.report_bad = _report_bad
    for target in (_worker, _periodic_scan, _periodic_scrub):
        threading.Thread(target=target, daemon=True).start()
