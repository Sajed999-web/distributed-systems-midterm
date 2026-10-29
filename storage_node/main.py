"""Storage node service: PUT / GET / HEAD / DELETE content-addressed chunks.

Configuration (environment variables):
    NODE_ID                  unique node name, e.g. node1
    NODE_URL                 URL other services use to reach this node
    DATA_DIR                 local directory where chunks are stored
    METADATA_URL             metadata service base URL ("" disables heartbeats)
    HEARTBEAT_INTERVAL       seconds between heartbeats (default 3)
    ENABLE_FAULT_INJECTION   "1" exposes POST /admin/corrupt/{hash} for demos
"""
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from prometheus_client import CollectorRegistry, Counter, Gauge, make_asgi_app

from storage_node.store import (
    ChunkStore,
    CorruptChunkError,
    HashMismatchError,
    InvalidHashError,
)

NODE_ID = os.getenv("NODE_ID", "node1")
NODE_URL = os.getenv("NODE_URL", f"http://{NODE_ID}:8000")
DATA_DIR = os.getenv("DATA_DIR", "/data")
METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "3"))
FAULT_INJECTION = os.getenv("ENABLE_FAULT_INJECTION", "0") == "1"

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{NODE_ID}] %(levelname)s %(message)s")
log = logging.getLogger("storage_node")

store = ChunkStore(DATA_DIR)
started_at = time.time()

REGISTRY = CollectorRegistry()
CHUNK_OPS = Counter("chunk_ops_total", "Chunk operations", ["node", "op", "result"], registry=REGISTRY)
CHUNKS = Gauge("node_chunks", "Chunks stored on this node", ["node"], registry=REGISTRY)
USED = Gauge("node_used_bytes", "Bytes of chunk data stored", ["node"], registry=REGISTRY)
FREE = Gauge("node_free_bytes", "Free disk bytes", ["node"], registry=REGISTRY)


def stats() -> dict:
    du = store.disk_usage()
    CHUNKS.labels(NODE_ID).set(store.chunk_count)
    USED.labels(NODE_ID).set(store.used_bytes)
    FREE.labels(NODE_ID).set(du["free_bytes"])
    return {
        "node_id": NODE_ID,
        "url": NODE_URL,
        "chunk_count": store.chunk_count,
        "used_bytes": store.used_bytes,
        **du,
    }


async def heartbeat_loop() -> None:
    async with httpx.AsyncClient(timeout=2.0) as client:
        while True:
            try:
                r = await client.post(f"{METADATA_URL}/nodes/heartbeat", json=stats())
                r.raise_for_status()
            except httpx.HTTPError as e:
                log.warning("heartbeat to %s failed: %s", METADATA_URL, e)
            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("starting: %d chunks / %d bytes in %s", store.chunk_count, store.used_bytes, DATA_DIR)
    task = asyncio.create_task(heartbeat_loop()) if METADATA_URL else None
    yield
    if task:
        task.cancel()


app = FastAPI(title=f"Storage node {NODE_ID}", lifespan=lifespan)
app.mount("/metrics", make_asgi_app(REGISTRY))


def _check(h: str) -> None:
    try:
        store._path(h)
    except InvalidHashError:
        raise HTTPException(400, "chunk id must be a lowercase hex SHA-256 digest")


@app.put("/chunks/{h}", status_code=201)
async def put_chunk(h: str, request: Request, response: Response):
    _check(h)
    data = await request.body()
    try:
        created = await run_in_threadpool(store.put, h, data)
    except HashMismatchError:
        CHUNK_OPS.labels(NODE_ID, "put", "hash_mismatch").inc()
        raise HTTPException(400, "content does not match SHA-256 in URL")
    CHUNK_OPS.labels(NODE_ID, "put", "created" if created else "exists").inc()
    if not created:
        response.status_code = 200
    return {"hash": h, "size": len(data), "created": created, "node_id": NODE_ID}


@app.get("/chunks/{h}")
async def get_chunk(h: str):
    _check(h)
    try:
        data = await run_in_threadpool(store.get, h)
    except FileNotFoundError:
        CHUNK_OPS.labels(NODE_ID, "get", "not_found").inc()
        raise HTTPException(404, "chunk not found")
    except CorruptChunkError:
        CHUNK_OPS.labels(NODE_ID, "get", "corrupt").inc()
        log.error("chunk %s failed verification and was quarantined", h)
        raise HTTPException(409, "chunk failed SHA-256 verification (corrupt)")
    CHUNK_OPS.labels(NODE_ID, "get", "ok").inc()
    return Response(data, media_type="application/octet-stream", headers={"X-Chunk-Hash": h})


@app.head("/chunks/{h}")
async def head_chunk(h: str):
    _check(h)
    size = store.size(h)
    if size is None:
        return Response(status_code=404)
    return Response(headers={"Content-Length": str(size), "X-Chunk-Hash": h})


@app.delete("/chunks/{h}", status_code=204)
async def delete_chunk(h: str):
    _check(h)
    if not await run_in_threadpool(store.delete, h):
        raise HTTPException(404, "chunk not found")
    CHUNK_OPS.labels(NODE_ID, "delete", "ok").inc()
    return Response(status_code=204)


@app.get("/chunks")
async def list_chunks():
    hashes = await run_in_threadpool(store.list)
    return {"node_id": NODE_ID, "count": len(hashes), "chunks": hashes}


def _scrub() -> dict:
    t0 = time.time()
    present, corrupt = [], []
    for h in store.list():
        try:
            store.get(h)  # re-reads and re-hashes; corrupt chunks are quarantined
            present.append(h)
        except CorruptChunkError:
            corrupt.append(h)
            log.error("scrub: chunk %s failed verification and was quarantined", h)
        except FileNotFoundError:
            pass  # deleted while scrubbing
    CHUNK_OPS.labels(NODE_ID, "scrub", "corrupt").inc(len(corrupt))
    return {"node_id": NODE_ID, "checked": len(present) + len(corrupt), "corrupt": corrupt,
            "present": present, "seconds": round(time.time() - t0, 2)}


@app.post("/scrub")
async def scrub():
    """Re-read every chunk on this node, verify its SHA-256 and quarantine bit rot."""
    return await run_in_threadpool(_scrub)


@app.get("/health")
async def health():
    return {"status": "ok", "uptime_s": round(time.time() - started_at, 1), **stats()}


if FAULT_INJECTION:

    @app.post("/admin/corrupt/{h}")
    async def corrupt_chunk(h: str):
        _check(h)
        if store.size(h) is None:
            raise HTTPException(404, "chunk not found")
        store.corrupt(h)
        log.warning("FAULT INJECTION: corrupted chunk %s", h)
        return {"corrupted": h}
