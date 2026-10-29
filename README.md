# Distributed Object Storage Framework

Files are split into content-addressed chunks (SHA-256), placed on 4 storage nodes with a consistent-hashing ring,
kept available with replication (N=3, quorum W=2/R=2) or Reed-Solomon erasure coding (k+m, e.g. 4+2), deduplicated
with reference counting, repaired automatically after node failures and scrubbed for bit rot.

## Project structure

| Path | Content |
|---|---|
| `storage_node/` | FastAPI storage node: PUT/GET/HEAD/DELETE chunks, SHA-256 check on every read, heartbeat, `/scrub` |
| `metadata_service/` | FastAPI metadata & placement service: chunking (fixed / FastCDC), hash ring, PostgreSQL metadata, dedup, replication + erasure coding, repair jobs, scrubber |
| `docker-compose.yml` | PostgreSQL, metadata service, node1–node4 (+ optional node5), Prometheus, Grafana, Kafka, RabbitMQ |
| `prometheus.yml` | Prometheus scrape config for the storage nodes |
| `tests/` | pytest unit tests |
| `scripts/` | dataset generator, upload tool, demos, benchmarks, flowchart generator |
| `benchmarks/` | benchmark results (CSV / JSON / PNG / logs) |
| `docs/report/` | report notes per task; `docs/flowcharts/` write, read and failure/repair flowcharts |
| `requirements.txt` | Python packages for the scripts and tests (each service has its own `requirements.txt`) |

## Start the cluster

Requires Docker Desktop.

```powershell
docker compose up -d --build
docker compose ps
```

The metadata service API is at http://localhost:8100/docs and the storage nodes at http://localhost:8001–8004.
To add a fifth node: `docker compose --profile scale up -d node5`.

## Python environment (for the scripts and tests)

```powershell
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Datasets

The datasets are **not included** in this submission because they are about **4.3 GB**. Regenerate them with:

```powershell
.\venv\Scripts\python.exe scripts\generate_datasets.py small   # 5000 files of 1-50 KB (JSON / text), ~124 MB
.\venv\Scripts\python.exe scripts\generate_datasets.py large   # 10 files of 100 MB - 1 GB, ~4.15 GB
```

The generator is seeded (`--seed 42` by default), so it reproduces exactly the same bytes that were used for the
benchmarks. Use `--scale 0.1` on the large workload for a quick, smaller version.

## Tests

```powershell
.\venv\Scripts\python.exe -m pytest -v
```

## Scripts (run with the cluster up)

| Script | Purpose |
|---|---|
| `scripts/demo_task2_storage_nodes.py` | storage node demo: PUT/GET/HEAD/DELETE, hash mismatch, corruption, heartbeats |
| `scripts/demo_task3_metadata_placement.py` | chunking, PostgreSQL metadata, ring placement, deduplication, refcount GC |
| `scripts/demo_task4_redundancy.py` | replication vs EC, quorum write, node failure + automatic repair, scrubbing |
| `scripts/upload_dataset.py <dir> --bucket B [--chunking fixed\|cdc]` | upload a folder, report throughput and dedup |
| `scripts/bench_chunking.py datasets/small datasets/large` | fixed-size vs FastCDC (no cluster needed) |
| `scripts/placement_experiment.py [--live]` | data movement when adding/removing a node |
| `scripts/bench_redundancy.py` | replication vs erasure coding: overhead and repair traffic (**wipes the cluster data**) |
| `scripts/make_flowcharts.py` | redraws `docs/flowcharts/*.png` |

Example: `.\venv\Scripts\python.exe scripts\demo_task4_redundancy.py`
