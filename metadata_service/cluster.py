"""Cluster membership (heartbeats -> alive nodes -> hash ring) and the HTTP client for storage nodes."""
import hashlib
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import httpx

from metadata_service.db import pool
from metadata_service.ring import DEFAULT_VNODES, HashRing

HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "10"))
VNODES = int(os.getenv("VNODES", DEFAULT_VNODES))

log = logging.getLogger("metadata")

http = httpx.Client(timeout=httpx.Timeout(60.0, connect=2.0), limits=httpx.Limits(max_connections=200, max_keepalive_connections=50))
io_pool = ThreadPoolExecutor(max_workers=64)    # leaf calls to storage nodes
task_pool = ThreadPoolExecutor(max_workers=16)  # per-chunk tasks that wait on io_pool


class Cluster:
    """In-memory view of the nodes, refreshed from PostgreSQL every second."""

    def __init__(self):
        self.lock = threading.Lock()
        self.urls: dict[str, str] = {}
        self.alive: set[str] = set()
        self.ring = HashRing(vnodes=VNODES)
        self.on_node_dead: list[Callable[[str], None]] = []
        self.suspect: dict[str, float] = {}  # node -> time of its last connection failure

    def refresh(self) -> None:
        with pool.connection() as conn:
            rows = conn.execute(
                "SELECT node_id, url, state, EXTRACT(EPOCH FROM now() - last_heartbeat) AS age FROM nodes"
            ).fetchall()
        urls = {r["node_id"]: r["url"] for r in rows}
        alive = {r["node_id"] for r in rows if r["age"] is not None and r["age"] <= HEARTBEAT_TIMEOUT}
        members = {r["node_id"] for r in rows if r["node_id"] in alive and r["state"] == "active"}
        with self.lock:
            died = self.alive - alive
            for n in alive - self.alive:
                log.info("node %s is ALIVE", n)
            for n in died:
                log.warning("node %s is DEAD (no heartbeat for > %ss)", n, HEARTBEAT_TIMEOUT)
            self.urls, self.alive = urls, alive
            if members != self.ring.nodes:
                self.ring = HashRing(sorted(members), vnodes=VNODES)
                log.info("ring rebuilt: %s", sorted(members))
        for n in died:
            for callback in self.on_node_dead:
                callback(n)

    def targets(self, h: str, n: int) -> list[str]:
        return self.ring.get_nodes(h, n)

    def ring_order(self, h: str) -> list[str]:
        """Every ring member, in clockwise order from the chunk's position."""
        return self.ring.get_nodes(h, len(self.ring.nodes))

    def url(self, node: str) -> str:
        return self.urls[node]

    def usable(self, node: str) -> bool:
        """Alive, and no connection failure since a heartbeat timeout ago.

        A node that has just crashed is still "alive" until its heartbeats are
        missed; marking it suspect on the first refused connection keeps reads
        from waiting on it again and again in the meantime."""
        return node in self.alive and time.time() - self.suspect.get(node, 0) > HEARTBEAT_TIMEOUT


cluster = Cluster()


def monitor_loop() -> None:
    while True:
        try:
            cluster.refresh()
        except Exception:
            log.exception("cluster refresh failed")
        time.sleep(1)


# ------------------------------------------------------------ node client
def _call(node: str, method: str, path: str, **kw) -> httpx.Response:
    try:
        return http.request(method, f"{cluster.url(node)}{path}", **kw)
    except httpx.TransportError:
        cluster.suspect[node] = time.time()
        raise


def node_put(node: str, h: str, data: bytes) -> None:
    _call(node, "PUT", f"/chunks/{h}", content=data).raise_for_status()


def node_get(node: str, h: str) -> bytes:
    """GET a blob and verify it; raises httpx.HTTPStatusError (404 missing / 409 corrupt) or IOError."""
    r = _call(node, "GET", f"/chunks/{h}")
    r.raise_for_status()
    if hashlib.sha256(r.content).hexdigest() != h:
        raise IOError(f"blob {h} from {node} failed verification")
    return r.content


def node_has(node: str, h: str) -> bool:
    return _call(node, "HEAD", f"/chunks/{h}").status_code == 200


def node_delete(node: str, h: str) -> None:
    try:
        http.delete(f"{cluster.url(node)}/chunks/{h}")
    except httpx.HTTPError as e:
        log.warning("delete %s on %s failed: %s", h[:12], node, e)


def node_scrub(node: str) -> dict:
    r = http.post(f"{cluster.url(node)}/scrub", timeout=3600)
    r.raise_for_status()
    return r.json()
