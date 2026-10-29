# Task 2 â€“ Storage Node Module

## Report paragraph

In this phase we built the storage node service, the lowest layer of the framework. Each node is a FastAPI
application (`storage_node/main.py`) that exposes four chunk operations: `PUT /chunks/{sha256}` stores a chunk,
`GET /chunks/{sha256}` returns it, `HEAD /chunks/{sha256}` reports whether it exists and how large it is, and
`DELETE /chunks/{sha256}` removes it. Chunks are content-addressed: the identifier of every chunk is the SHA-256
hash of its bytes. The node recomputes this hash on upload and rejects any body that does not match the hash in
the URL (HTTP 400). Chunks are saved on the node's local disk under a two-level directory fan-out
(`/data/chunks/ab/cd/<hash>`). Each chunk is first written to a temporary file, fsynced, and then atomically
renamed, so a crash cannot leave a partial chunk behind. Because the name is derived from the content, writing the
same chunk twice is idempotent and it is stored only once. Every read re-hashes the data. If the bytes no longer
match their name (bit rot or tampering), the node refuses to serve them (HTTP 409) and moves the file to a
quarantine directory so that the repair process (Task 4) can restore a healthy copy. Each node runs a background
task that sends a heartbeat to the metadata service every 3 seconds. The heartbeat carries the node id, URL,
chunk count, bytes used, and free and total disk space. The metadata service marks a node *dead* when no
heartbeat arrives within 10 seconds. Four nodes (`node1`â€“`node4`) run in separate Docker containers built from
the same image. Each has its own named volume and its own host port (8001â€“8004). The nodes also export
Prometheus metrics (`/metrics/`) for chunk operations, chunk count and disk usage.

## Files produced in this phase

| File | Purpose |
|---|---|
| `storage_node/store.py` | Content-addressed on-disk chunk store (atomic write, verify-on-read, quarantine) |
| `storage_node/main.py` | FastAPI service: PUT/GET/HEAD/DELETE chunks, list, health, heartbeat loop, metrics |
| `storage_node/Dockerfile`, `storage_node/requirements.txt` | Container image for a storage node |
| `metadata_service/main.py` | Heartbeat receiver, node liveness (`GET /nodes`) â€“ extended in Task 3 |
| `metadata_service/Dockerfile`, `metadata_service/requirements.txt` | Container image for the metadata service |
| `docker-compose.yml` | metadata + node1..node4 (separate containers and volumes) |
| `prometheus.yml` | Scrapes the 4 storage nodes |
| `tests/test_storage_node.py`, `pytest.ini` | 7 unit tests (round trip, idempotency, hash mismatch, corruption, delete, list) |
| `scripts/demo_task2_storage_nodes.py` | End-to-end demo against the running cluster |

## How to reproduce

```powershell
docker compose up -d --build metadata node1 node2 node3 node4
docker compose ps
.\venv\Scripts\python.exe -m pytest -v tests/test_storage_node.py
.\venv\Scripts\python.exe scripts/demo_task2_storage_nodes.py
docker stop node4 ; Start-Sleep 12 ; curl.exe -s http://localhost:8100/nodes ; docker start node4
```

## Screenshot guide (step by step)

Use the same setup as in the Task 3 guide (`docs/report/task3_metadata_placement.md`, "Before you start"): PowerShell
full screen in the project folder, the cluster running, **Windows + PrtScn** to take a picture, then move it to
`docs\screenshots` and rename it as shown. Type `cls` before each one.

#### Screenshot 2-01: Docker Desktop with the storage nodes
- **Open** Docker Desktop, click **Containers** on the left, and expand the **midterm_distributed** group.
- **You should see:** `node1`, `node2`, `node3`, `node4` and `metadata` with a green dot (running) and their ports
  8001–8004 and 8100.
- **Save as:** `task2_01_docker_desktop.png`

#### Screenshot 2-02: the unit tests pass
- **Type:** `cls; .\venv\Scripts\python.exe -m pytest -v tests/test_storage_node.py`
- **You should see:** 7 `PASSED` lines and `7 passed`.
- **Save as:** `task2_02_unit_tests.png`

#### Screenshot 2-03: the storage node demo
- **Type:** `cls; .\venv\Scripts\python.exe scripts\demo_task2_storage_nodes.py`
- **You should see:** steps 1–8: health of the 4 nodes, PUT `201`, the second PUT `200 created=False`, HEAD/GET
  `verified=True`, the wrong hash rejected with `400`, the corrupted chunk rejected with `409`, DELETE `204` and then
  HEAD `404`, and the heartbeats (all nodes `alive`). If it does not fit on one screen, take two screenshots
  (`task2_03a_demo.png`, `task2_03b_demo.png`).
- **Save as:** `task2_03_demo.png`

#### Screenshot 2-04: the storage node API (Swagger)
- **Open** <http://localhost:8001/docs> in the browser and press **F11**.
- **You should see:** `PUT /chunks/{h}`, `GET /chunks/{h}`, `HEAD /chunks/{h}`, `DELETE /chunks/{h}`,
  `GET /chunks`, `GET /health`.
- **Save as:** `task2_04_node_swagger.png`

#### Screenshot 2-05: the node detected the corrupted chunk
- Take this right after 2-03.
- **Type:** `cls; docker logs node3 --tail 15`
- **You should see:** the red line `ERROR chunk … failed verification and was quarantined`, the `409 Conflict`
  request, and the heartbeat POSTs every 3 seconds.
- **Save as:** `task2_05_corruption_log.png`

#### Screenshots 2-06 and 2-07: heartbeats and failure detection
- **Open** <http://localhost:8100/nodes> in the browser. **You should see** every node with `"status": "alive"` and
  `seconds_since_heartbeat` below 3. **Save as:** `task2_06_heartbeats_alive.png`
- **In PowerShell type:** `docker stop node4`, wait **15 seconds**, then refresh the browser page (**F5**).
  **You should see** `node4` with `"status": "dead"` and `seconds_since_heartbeat` above 10.
  **Save as:** `task2_07_node4_dead.png`
- **Afterwards type:** `docker start node4`. Refresh after a few seconds: node4 is `alive` again.

#### Screenshot 2-08 (optional): Prometheus collects the node metrics
- **Type:** `docker compose up -d prometheus`, then open <http://localhost:9090/targets>.
- **You should see:** the `storage-nodes` job with 4 targets in state **UP**.
- **Save as:** `task2_08_prometheus.png`
