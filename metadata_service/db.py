"""PostgreSQL schema and connection pool for object metadata."""
import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dfs:dfs@postgres:5432/dfs")

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id        TEXT PRIMARY KEY,
    url            TEXT NOT NULL,
    state          TEXT NOT NULL DEFAULT 'active',     -- active | decommissioned
    chunk_count    BIGINT,
    used_bytes     BIGINT,
    free_bytes     BIGINT,
    total_bytes    BIGINT,
    last_heartbeat TIMESTAMPTZ,
    joined_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS buckets (
    name       TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS objects (
    id           BIGSERIAL PRIMARY KEY,
    bucket       TEXT NOT NULL REFERENCES buckets(name),
    key          TEXT NOT NULL,
    version      INT  NOT NULL,
    size         BIGINT NOT NULL,
    sha256       TEXT NOT NULL,                      -- hash of the whole object (ETag)
    content_type TEXT,
    chunking     TEXT NOT NULL,                      -- fixed | cdc
    chunk_count  INT NOT NULL,
    tags         JSONB NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bucket, key, version)
);
CREATE INDEX IF NOT EXISTS objects_bucket_key ON objects (bucket, key text_pattern_ops);

-- ordered list of chunk hashes that make up each object version
CREATE TABLE IF NOT EXISTS object_chunks (
    object_id  BIGINT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    idx        INT    NOT NULL,
    chunk_hash TEXT   NOT NULL,
    "offset"   BIGINT NOT NULL,
    size       INT    NOT NULL,
    PRIMARY KEY (object_id, idx)
);

-- one row per unique chunk; refcount = number of object_chunks rows using it
CREATE TABLE IF NOT EXISTS chunks (
    hash       TEXT PRIMARY KEY,
    size       INT  NOT NULL,
    refcount   INT  NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- which nodes currently hold a copy of each chunk
CREATE TABLE IF NOT EXISTS chunk_replicas (
    hash    TEXT NOT NULL REFERENCES chunks(hash) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    PRIMARY KEY (hash, node_id)
);
CREATE INDEX IF NOT EXISTS chunk_replicas_node ON chunk_replicas (node_id);

-- Task 4: redundancy policy per bucket and per chunk ---------------------------
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS redundancy TEXT NOT NULL DEFAULT 'replication';  -- replication | ec
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS replicas   INT  NOT NULL DEFAULT 3;              -- N
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS ec_k       INT  NOT NULL DEFAULT 4;
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS ec_m       INT  NOT NULL DEFAULT 2;

ALTER TABLE chunks ADD COLUMN IF NOT EXISTS scheme TEXT NOT NULL DEFAULT 'replication';       -- how it is stored
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS n      INT  NOT NULL DEFAULT 3;  -- replicas, or k+m fragments
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS k      INT  NOT NULL DEFAULT 1;  -- fragments needed to rebuild

-- erasure-coded chunks: one row per fragment (k data + m parity)
CREATE TABLE IF NOT EXISTS chunk_fragments (
    hash      TEXT NOT NULL REFERENCES chunks(hash) ON DELETE CASCADE,
    idx       INT  NOT NULL,
    blob_hash TEXT NOT NULL,
    size      INT  NOT NULL,
    node_id   TEXT NOT NULL,
    PRIMARY KEY (hash, idx)
);
CREATE INDEX IF NOT EXISTS chunk_fragments_node ON chunk_fragments (node_id);

CREATE TABLE IF NOT EXISTS repair_jobs (
    id             BIGSERIAL PRIMARY KEY,
    reason         TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'running',   -- running | done | failed
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    chunks_total   INT NOT NULL DEFAULT 0,
    chunks_done    INT NOT NULL DEFAULT 0,
    chunks_failed  INT NOT NULL DEFAULT 0,
    blobs_written  INT NOT NULL DEFAULT 0,
    bytes_read     BIGINT NOT NULL DEFAULT 0,    -- repair traffic: bytes fetched from healthy nodes
    bytes_written  BIGINT NOT NULL DEFAULT 0,    -- repair traffic: bytes written to new nodes
    detail         JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS scrub_runs (
    id             BIGSERIAL PRIMARY KEY,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    blobs_checked  BIGINT NOT NULL DEFAULT 0,
    corrupt        INT NOT NULL DEFAULT 0,
    missing        INT NOT NULL DEFAULT 0,
    orphans_removed INT NOT NULL DEFAULT 0,
    detail         JSONB NOT NULL DEFAULT '{}'
);
"""

pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=20, open=False,
                      kwargs={"row_factory": dict_row, "autocommit": False})


def init_db() -> None:
    pool.open(wait=True, timeout=60)
    with pool.connection() as conn:
        conn.execute(SCHEMA)
