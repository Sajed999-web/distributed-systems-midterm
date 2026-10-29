"""Redundancy schemes: N-way replication with quorums, and Reed-Solomon erasure coding.

Replication (default N = 3)
    write: send the chunk to the N ring targets in parallel; succeed when at
           least W nodes acknowledge.
    read:  fetch the chunk from one live replica and confirm it with HEAD on
           others until R replicas agree.
    W and R default to a majority (W = N//2 + 1, R = N - W + 1), so W + R > N:
    every read quorum overlaps every write quorum in at least one node.
    Replicas that failed during a write are re-created later by the repair job.

Erasure coding (k data + m parity, e.g. 4+2)
    write: encode into k+m fragments, place fragment i on the i-th node in ring
           order (wrapping around if there are fewer than k+m nodes) and
           succeed when at least k+1 fragments are stored.
    read:  fetch any k fragments (data fragments first, which needs no
           decoding), rebuild and verify the chunk.

A replica or fragment that turns out to be missing or corrupt during a read is
reported through `report_bad`, so the repair module can drop it and rebuild it
(read repair).
"""
import logging
import os
from concurrent.futures import as_completed
from dataclasses import dataclass
from typing import Callable

import httpx

from metadata_service import erasure
from metadata_service.cluster import cluster, io_pool, node_get, node_has, node_put

log = logging.getLogger("metadata")

READ_QUORUM_ENV = os.getenv("READ_QUORUM")
WRITE_QUORUM_ENV = os.getenv("WRITE_QUORUM")

# set by the repair module: report_bad(chunk_hash, node, fragment_index_or_None)
report_bad: Callable[[str, str, int | None], None] = lambda h, node, idx: None


@dataclass(frozen=True)
class Policy:
    scheme: str  # "replication" | "ec"
    n: int       # replicas, or k+m fragments
    k: int       # fragments needed to read (1 for replication)

    @classmethod
    def from_bucket(cls, b: dict) -> "Policy":
        if b["redundancy"] == "ec":
            return cls("ec", b["ec_k"] + b["ec_m"], b["ec_k"])
        return cls("replication", b["replicas"], 1)

    def describe(self) -> str:
        return f"EC {self.k}+{self.n - self.k}" if self.scheme == "ec" else f"replication N={self.n}"


def quorums(n: int) -> tuple[int, int]:
    """(W, R) for N replicas, from the environment if valid, else majority quorums."""
    w = n // 2 + 1
    r = n - w + 1
    if WRITE_QUORUM_ENV and READ_QUORUM_ENV:
        ew, er = int(WRITE_QUORUM_ENV), int(READ_QUORUM_ENV)
        if 1 <= ew <= n and 1 <= er <= n and ew + er > n:
            w, r = ew, er
    return w, r


@dataclass
class Placement:
    """One stored copy: a full replica (idx None) or an EC fragment."""
    idx: int | None
    blob_hash: str
    size: int
    node: str


def _put_all(jobs: list[tuple[str, str, bytes]]) -> list[bool]:
    futures = [io_pool.submit(node_put, node, h, data) for node, h, data in jobs]
    ok = []
    for (node, h, _), f in zip(jobs, futures):
        try:
            f.result()
            ok.append(True)
        except (httpx.HTTPError, KeyError) as e:
            log.warning("put %s on %s failed: %s", h[:12], node, e)
            ok.append(False)
    return ok


# ------------------------------------------------------------ write
def store(h: str, data: bytes, policy: Policy) -> list[Placement]:
    if policy.scheme == "ec" and data:
        return _store_ec(h, data, policy)
    return _store_replicated(h, data, policy.n)


def _store_replicated(h: str, data: bytes, n: int) -> list[Placement]:
    w, _ = quorums(n)
    targets = cluster.targets(h, n)
    if len(targets) < w:
        raise IOError(f"only {len(targets)} live nodes, write quorum W={w}")
    ok = _put_all([(node, h, data) for node in targets])
    stored = [Placement(None, h, len(data), node) for node, good in zip(targets, ok) if good]
    if len(stored) < w:
        raise IOError(f"chunk {h[:12]} stored on {len(stored)} nodes, write quorum W={w} not reached")
    return stored


def _store_ec(h: str, data: bytes, policy: Policy) -> list[Placement]:
    order = cluster.ring_order(h)
    if not order:
        raise IOError("no live nodes")
    frags = erasure.encode(h, data, policy.k, policy.n - policy.k)
    nodes = [order[i % len(order)] for i in range(len(frags))]
    ok = _put_all([(node, f.blob_hash, f.blob) for node, f in zip(nodes, frags)])
    stored = [Placement(f.index, f.blob_hash, len(f.blob), node) for f, node, good in zip(frags, nodes, ok) if good]
    if len(stored) < policy.k + 1:
        raise IOError(f"chunk {h[:12]}: only {len(stored)} of {len(frags)} fragments stored (need k+1)")
    return stored


# ------------------------------------------------------------ read
def read(chunk: dict) -> bytes:
    """Read a chunk described by a row with scheme, n, k, replicas[], fragments[]."""
    if chunk["scheme"] == "ec" and chunk["fragments"]:
        return _read_ec(chunk["chunk_hash"], chunk["k"], chunk["fragments"])
    return _read_replicated(chunk["chunk_hash"], chunk["replicas"], chunk["n"])


def _read_replicated(h: str, replicas: list[str], n: int) -> bytes:
    _, r = quorums(n)
    live = [x for x in replicas if cluster.usable(x)]
    if len(live) < r:
        raise IOError(f"chunk {h[:12]}: {len(live)} live replicas, read quorum R={r} not reachable")
    data, source, errors = None, None, []
    for node in live:  # 1) the data, from the first replica that returns a verified copy
        try:
            data, source = node_get(node, h), node
            break
        except (httpx.HTTPError, IOError) as e:
            errors.append(f"{node}: {e}")
            report_bad(h, node, None)
    if data is None:
        raise IOError(f"no healthy replica for chunk {h[:12]}: {errors}")
    confirmed = 1  # 2) confirm the copy exists on R-1 other replicas
    others = [x for x in live if x != source]
    if r > 1:
        futures = {io_pool.submit(node_has, node, h): node for node in others}
        for f in as_completed(futures):
            try:
                good = f.result()
            except httpx.HTTPError:
                good = False
            if good:
                confirmed += 1
            else:
                report_bad(h, futures[f], None)
    if confirmed < r:
        raise IOError(f"chunk {h[:12]}: read quorum R={r} not met ({confirmed} replicas)")
    return data


def _read_ec(h: str, k: int, fragments: list[dict]) -> bytes:
    # data fragments on live nodes first (no decoding needed), then parity
    candidates = sorted(fragments, key=lambda f: (not cluster.usable(f["node"]), f["idx"]))
    blobs: list[bytes] = []
    pending = iter(candidates)
    while len(blobs) < k:
        batch = [f for _, f in zip(range(k - len(blobs)), pending)]
        if not batch:
            raise IOError(f"chunk {h[:12]}: only {len(blobs)} of k={k} fragments readable")
        for f, fut in [(f, io_pool.submit(node_get, f["node"], f["blob"])) for f in batch]:
            try:
                blobs.append(fut.result())
            except (httpx.HTTPError, IOError, KeyError):
                report_bad(h, f["node"], f["idx"])
    return erasure.decode(blobs)
