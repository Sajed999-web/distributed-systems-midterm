# Task 3 – Metadata and Placement Module

## 3.1 Report text (copy into the Word document)

### Overview
In this phase we turned the metadata service into the "brain" of the framework. Clients upload whole files to it.
The service splits each file into chunks, computes the SHA-256 hash of every chunk, removes duplicates, decides which
storage nodes receive each chunk, and records everything in PostgreSQL. On a download it reads the ordered chunk list
from PostgreSQL, fetches each chunk from a live replica, verifies its hash again, and streams the file back. The
service is written with FastAPI (`metadata_service/main.py`) and runs in its own container next to a PostgreSQL 16
container and the four storage nodes from Task 2.

### Chunking: fixed-size and content-defined (FastCDC)
Two chunking methods are implemented in `metadata_service/chunking.py`, and the client chooses one per upload
(`?chunking=fixed` or `?chunking=cdc`).
**Fixed-size chunking** cuts the file every 4 MiB. It is very fast, but inserting a single byte near the start of a
file shifts every later boundary, so none of the following chunks match the previous version.
**Content-defined chunking** uses the FastCDC algorithm. A rolling "gear" hash is computed over the last 32 bytes at
every position, and a chunk boundary is placed where the hash matches a bit mask, so boundaries depend on the
*content* and not on the position. After an insertion the boundaries re-align and the rest of the file deduplicates.
We use FastCDC's normalized chunking: a stricter mask before the 4 MiB average and a looser one after it, with a
minimum chunk size of 1 MiB and a maximum of 16 MiB. A plain Python byte loop would run at only a few MB/s, so the
gear hash is computed with numpy: the low 32 bits of the hash are an exact sum of shifted table values, which takes
five vectorised shift-and-add passes. Segments of the file are processed in parallel threads.

### Metadata in PostgreSQL
The schema (`metadata_service/db.py`) has six tables:

| Table | Content |
|---|---|
| `nodes` | node id, URL, state (active / decommissioned), chunk count, used / free / total bytes, last heartbeat |
| `buckets` | bucket name, creation date |
| `objects` | bucket, key, **version**, size, SHA-256 of the whole object, content type, chunking method, number of chunks, user tags (JSONB), **creation date** |
| `object_chunks` | the **ordered list of chunk hashes** of every object version (index, hash, offset, size) |
| `chunks` | one row per *unique* chunk: hash, size, **reference count** |
| `chunk_replicas` | which nodes hold a copy of each chunk |

Uploading to an existing key creates a new version, and the previous versions stay readable.

### Consistent hashing ring with virtual nodes
`metadata_service/ring.py` implements a consistent-hashing ring. Each physical node is placed on a 64-bit ring at 256
pseudo-random positions (virtual nodes), computed as SHA-256(`"node#i"`). A chunk is placed at the position given by
the first 64 bits of its own hash, and its N = 3 replicas go to the first three *distinct* physical nodes found walking
clockwise. Lookups use binary search (`bisect`), so they cost O(log V). The ring contains only nodes that are alive
(heartbeat within 10 s) and not decommissioned. It is rebuilt automatically when a node joins or leaves.

### Deduplication with reference counting
Inside one database transaction, every chunk of a new upload is inserted with
`INSERT … ON CONFLICT (hash) DO UPDATE SET refcount = refcount + 1`. Only chunks that did not exist before are sent to
the storage nodes; chunks that already exist cost nothing except the reference-count increment. Deleting an object
decrements the counts. When a count reaches zero, the chunk is deleted from every node that holds it and its row is
removed, all while the row is still locked, so a concurrent upload of the same content cannot be lost. Identical
chunks are therefore stored once, and deleted only when no object references them any more.

### Placement test (adding / removing a node)
We measured how many chunk replicas have to move when the cluster changes size, in two ways:

1. **Simulation** with 100,000 chunk hashes and N = 3 replicas (`scripts/placement_experiment.py`), comparing
   consistent hashing with naive modulo hashing (`node = hash mod #nodes`):

| Scenario | vnodes | Consistent hashing | Modulo hashing | Ideal (minimum) | Load max/min |
|---|---|---|---|---|---|
| add node5 (4→5) | 1 | 19.94 % | 40.10 % | 20 % | 7.55 |
| add node5 (4→5) | 32 | 19.26 % | 40.10 % | 20 % | 1.40 |
| add node5 (4→5) | 256 | **20.19 %** | **40.10 %** | 20 % | 1.19 |
| remove node4 (4→3) | 1 | 31.41 % | 24.95 % | 25 % | 7.55 |
| remove node4 (4→3) | 256 | **24.18 %** | 24.95 % | 25 % | 1.19 |

   With 256 virtual nodes, adding a node moves only the minimum possible ~20 % of the replicas, while modulo
   hashing moves twice as much. Virtual nodes are also what keep the load even: with a single position per node
   the busiest node holds 7.5× more data than the least busy one, and with 256 vnodes only 1.19×. *(When a node is
   removed, modulo hashing also looks ideal in this setup. This is a side effect of N = 3 replicas on 3 remaining
   nodes: every chunk must then be on all three nodes, so any method moves exactly node4's share.)*

2. **Live test on the running cluster**, using the 4,570 unique chunks (13,710 replicas) of the small workload: we
   started `node5`, ran a rebalance, then decommissioned `node5` and rebalanced again.

| Step | Predicted moved | Actually moved | Bytes moved | Time |
|---|---|---|---|---|
| add node5 (4→5) | 20.36 % | **2,791 replicas = 20.36 %** | 112.8 MB | 25.3 s |
| remove node5 (5→4) | – | **2,791 replicas = 20.36 %** | 112.8 MB | 29.3 s |

   The measured movement matched the prediction exactly. After both moves, 200 randomly chosen objects were
   downloaded and all 200 passed SHA-256 verification.

### Fixed-size vs content-defined chunking on both workloads
(`scripts/bench_chunking.py`, both workloads produced by `scripts/generate_datasets.py`)

| Workload | Method | Files | Size | Chunks | Avg chunk | Dedup saving | Chunking + SHA-256 speed |
|---|---|---|---|---|---|---|---|
| small (5000 × 1–50 KB) | fixed | 5000 | 124 MB | 5000 | 25.5 KB | 8.97 % | 60 MB/s |
| small (5000 × 1–50 KB) | FastCDC | 5000 | 124 MB | 5000 | 25.5 KB | 8.97 % | 74 MB/s |
| large (10 × 100 MB–1 GB) | fixed | 10 | 4150 MB | 1040 | 4086 KB | 17.3 % | 375 MB/s |
| large (10 × 100 MB–1 GB) | FastCDC | 10 | 4150 MB | 820 | 5182 KB | **29.5 %** | 115 MB/s |

For small files both methods behave the same, because every file is smaller than the minimum chunk size and becomes
exactly one chunk. The 9 % saving comes from the 10 % of files that are exact duplicates. (The speed difference on
small files comes from per-file open costs on Windows, not from the chunker.) For large files FastCDC saves much more
space (29.5 % vs 17.3 %): `disk_base_v2.img` is `disk_base.img` with three small insertions, and FastCDC re-aligns
after each insertion, whereas fixed-size chunking only matches the part before the first insertion. The price is
speed: FastCDC is about 3× slower than fixed-size chunking, which only hashes. The demo showed the same effect on a
single file: after inserting 100 bytes into a 20 MB file, fixed-size chunking had to store 6 new chunks (all of them)
and FastCDC only 1.

### Note on upload speed from Windows
Inside the Docker network one small-object upload takes about 23 ms (one chunk, three replicas, plus the PostgreSQL
transaction). Uploading from the Windows host goes through Docker Desktop's port forwarding, which adds 10–40 ms per
request, so the small workload uploaded at about 53 files/s from the host.

## 3.2 Files produced in this phase

| File | Purpose |
|---|---|
| `metadata_service/chunking.py` | fixed-size and FastCDC chunkers (vectorised gear hash) |
| `metadata_service/ring.py` | consistent-hashing ring with virtual nodes |
| `metadata_service/db.py` | PostgreSQL schema and connection pool |
| `metadata_service/main.py` | heartbeats, ring, write/read/delete paths, dedup, stats, placement simulation, rebalance |
| `metadata_service/requirements.txt`, `metadata_service/Dockerfile` | container image |
| `docker-compose.yml` | adds `postgres` and the optional `node5` (profile `scale`) |
| `tests/test_chunking_ring.py` | 6 unit tests: chunk coverage, CDC limits, CDC re-alignment, ring determinism, balance, minimal movement |
| `scripts/generate_datasets.py` | creates both workloads (small: 5000 files, large: 10 files / 4.15 GB) |
| `scripts/upload_dataset.py` | uploads a folder to a bucket and reports throughput and dedup |
| `scripts/demo_task3_metadata_placement.py` | end-to-end demo of this phase |
| `scripts/placement_experiment.py` | placement simulation (+ live add/remove with `--live`) |
| `scripts/bench_chunking.py` | fixed vs FastCDC on both workloads |
| `benchmarks/placement_simulation.csv/.png`, `benchmarks/placement_live.json` | placement results |
| `benchmarks/chunking_comparison.csv/.png` | chunking results |
| `benchmarks/upload_small_fixed.json` | upload results of the small workload |

---

## 3.3 Screenshot guide (step by step)

### Before you start (do this once)
1. Open **Docker Desktop** and wait until it says *Engine running* (bottom left).
2. Open **PowerShell** in the project folder: in File Explorer open `C:\Users\sajed\Documents\midterm_distributed`,
   click the address bar, type `powershell` and press **Enter**.
3. Make the PowerShell window **full screen** (double-click its title bar) and make the text readable
   (**Ctrl + mouse wheel** to zoom).
4. Create a folder for the screenshots (only once):
   ```powershell
   mkdir docs\screenshots -Force
   ```
5. Make sure the cluster is running:
   ```powershell
   docker compose up -d postgres metadata node1 node2 node3 node4
   ```

### How to take a full-screen screenshot
Press **Windows key + PrtScn** together. The screen flashes, and the picture is saved in
`C:\Users\sajed\Pictures\Screenshots`. Move it to `docs\screenshots` and **rename it to the name given below**. With
those names I can put every picture in the right place in the Word report automatically.

> Tip: clear the PowerShell window before each screenshot with `cls`, so only the relevant output is visible.

---

#### Screenshot 3-01: the containers are running
- **Type:**
  ```powershell
  cls; docker compose ps
  ```
- **You should see:** `postgres` with `(healthy)`, plus `metadata`, `node1`, `node2`, `node3` and `node4`, all `Up`.
- **Save as:** `task3_01_containers.png`

#### Screenshot 3-02: the unit tests pass
- **Type:**
  ```powershell
  cls; .\venv\Scripts\python.exe -m pytest -v tests/test_chunking_ring.py
  ```
- **You should see:** 6 green `PASSED` lines and `6 passed` at the bottom.
- **Save as:** `task3_02_unit_tests.png`

#### Screenshots 3-03 and 3-04: the Task 3 demo
- **Type:**
  ```powershell
  cls; .\venv\Scripts\python.exe scripts\demo_task3_metadata_placement.py
  ```
- The output is long, so take **two** screenshots:
  - Scroll up until you see **`=== 1. Cluster membership`** at the top. Steps 1–5 should be visible: the ring, the
    5 chunks with their `refcount` and the 3 nodes each one is stored on, the placement of the first chunk, and
    `sha256 matches: True`. **Save as:** `task3_03_demo_part1.png`
  - Scroll down so steps 6–9 are visible: deduplication (`new=0 deduplicated=5`, physical bytes *unchanged*,
    refcount 2), deleting the copy and then the original (`404 = removed`), fixed vs CDC
    (`fixed … NEW chunks=6`, `cdc … NEW chunks=1`), and the statistics. **Save as:** `task3_04_demo_part2.png`

#### Screenshot 3-05: the metadata stored in PostgreSQL
- **Type** (copy all three lines at once):
  ```powershell
  cls; docker exec postgres psql -U dfs -d dfs -c "\dt"
  docker exec postgres psql -U dfs -d dfs -c "SELECT bucket, key, version, size, chunking, chunk_count, created_at FROM objects ORDER BY id DESC LIMIT 6;"
  docker exec postgres psql -U dfs -d dfs -c "SELECT o.key, o.version, oc.idx, left(oc.chunk_hash,16) AS chunk, oc.size, c.refcount FROM objects o JOIN object_chunks oc ON oc.object_id=o.id JOIN chunks c ON c.hash=oc.chunk_hash WHERE o.bucket='cmp-cdc' ORDER BY o.version, oc.idx;"
  ```
- **You should see:** the 6 tables (`buckets`, `chunk_replicas`, `chunks`, `nodes`, `object_chunks`, `objects`); the
  latest objects with their version, size, chunking method and creation date; and the ordered chunk list of
  `doc.bin` versions 1 and 2, where the chunks shared by both versions have `refcount = 2`.
- **Save as:** `task3_05_postgres_metadata.png`

#### Screenshot 3-06: the metadata service API (Swagger)
- **Open** in Chrome or Edge: <http://localhost:8100/docs>
- **You should see:** the list of endpoints (`/nodes/heartbeat`, `/buckets`, `/objects/{bucket}/{key}`,
  `/meta/...`, `/stats`, `/placement/...`, `/placement-simulation`, `/admin/rebalance`).
- Press **F11** for full screen, take the screenshot, then press **F11** again.
- **Save as:** `task3_06_swagger.png`

#### Screenshot 3-07: the two workloads
- **Type:**
  ```powershell
  cls; (Get-ChildItem datasets\small -Recurse -File | Measure-Object Length -Sum) | Select-Object Count, @{n='MB';e={[math]::Round($_.Sum/1MB,1)}}
  Get-ChildItem datasets\large | Select-Object Name, @{n='MB';e={[math]::Round($_.Length/1MB)}}
  ```
- **You should see:** `Count 5000` and about 124 MB for the small workload, and the 10 large files with their sizes
  (100 MB to 1000 MB).
- **Save as:** `task3_07_datasets.png`

#### Screenshot 3-08: uploading the small workload (deduplication across buckets)
- **Type** (this takes about 1.5 minutes):
  ```powershell
  cls; .\venv\Scripts\python.exe scripts\upload_dataset.py datasets\small --bucket small-cdc --chunking cdc
  ```
- **You should see:** the progress lines, then a summary with `"files": 5000`. `new_chunks` is close to **0** and
  `dedup_saving_percent` close to **100**: every file was already stored in the `small-fixed` bucket earlier, so
  nothing new has to be written. That is deduplication across buckets.
- **Save as:** `task3_08_upload_small.png`

#### Screenshot 3-09: fixed vs FastCDC benchmark (table)
- **Type** (takes about 1.5 minutes):
  ```powershell
  cls; .\venv\Scripts\python.exe scripts\bench_chunking.py datasets\small datasets\large
  ```
- **You should see:** 4 result lines. On the `large` workload `dedup_saving_pct` is about 17.3 for `fixed` and about
  29.5 for `cdc`.
- **Save as:** `task3_09_chunking_benchmark.png`

#### Screenshot 3-10: fixed vs FastCDC chart
- **Type:**
  ```powershell
  start benchmarks\chunking_comparison.png
  ```
- The chart opens in the Photos app. Maximise the window, then take the screenshot.
- **Save as:** `task3_10_chunking_chart.png`

#### Screenshot 3-11: placement simulation (table)
- **Type:**
  ```powershell
  cls; .\venv\Scripts\python.exe scripts\placement_experiment.py
  ```
- **You should see:** the table. For `add node5` with 256 vnodes, consistent hashing is about 20 % and modulo about
  40 %.
- **Save as:** `task3_11_placement_table.png`

#### Screenshot 3-12: placement simulation chart
- **Type:**
  ```powershell
  start benchmarks\placement_simulation.png
  ```
- **Save as:** `task3_12_placement_chart.png`

#### Screenshot 3-13: live test, adding and removing a real node
- **Type** (takes about 1 minute; it starts `node5`, moves data to it, then moves the data back and stops it):
  ```powershell
  cls; .\venv\Scripts\python.exe scripts\placement_experiment.py --chunks 10000 --live
  ```
- **You should see** under **`Part B - live cluster`**: the prediction (`consistent ~20 %`, `modulo ~40 %`), then
  `rebalance after adding node5: finished in … s, 2791 replicas moved (20.36%)`, and the same when node5 is removed.
- **Save as:** `task3_13_live_add_remove.png`
- ⚠️ This overwrites `benchmarks\placement_simulation.csv` with the 10,000-chunk version. Afterwards run
  `.\venv\Scripts\python.exe scripts\placement_experiment.py` once more (without `--live`) to restore the
  100,000-chunk results.

#### Screenshot 3-14: the metadata service saw node5 join and leave
- Take this **right after 3-13**. The log only goes back to the last time the metadata container was restarted.
- **Type:**
  ```powershell
  cls; docker logs metadata --since 10m 2>&1 | Select-String "ring rebuilt|ALIVE|DEAD"
  ```
- **You should see:** `node node5 is ALIVE`, `ring rebuilt: ['node1', … 'node5']`, then
  `ring rebuilt: ['node1', 'node2', 'node3', 'node4']` and `node node5 is DEAD`.
- **Save as:** `task3_14_ring_log.png`

#### Screenshot 3-15: chunk distribution across the nodes
- **Open** in the browser: <http://localhost:8100/stats> (tick **"Pretty-print"** if Chrome shows that option).
- **You should see:** `objects`, `logical_bytes`, `unique_chunks`, `physical_bytes`, `dedup_ratio`, and under
  `per_node` the chunk count on each of the 4 nodes, which should be roughly equal.
- **Save as:** `task3_15_stats.png`

### Checklist
`task3_01_containers.png`, `task3_02_unit_tests.png`, `task3_03_demo_part1.png`, `task3_04_demo_part2.png`,
`task3_05_postgres_metadata.png`, `task3_06_swagger.png`, `task3_07_datasets.png`, `task3_08_upload_small.png`,
`task3_09_chunking_benchmark.png`, `task3_10_chunking_chart.png`, `task3_11_placement_table.png`,
`task3_12_placement_chart.png`, `task3_13_live_add_remove.png`, `task3_14_ring_log.png`, `task3_15_stats.png`
