# Task 4 – Redundancy and Recovery

## 4.1 Report text (copy into the Word document)

### Overview
In this phase we made the storage layer fault tolerant. Every bucket now has a **redundancy policy**: N-way
**replication** with quorum writes and reads, or Reed-Solomon **erasure coding** with k data and m parity fragments.
The metadata service detects failed nodes from missing heartbeats and automatically starts a **repair job** that
rebuilds the lost copies on the remaining nodes. A background **scrubber** periodically re-reads every stored blob,
detects bit rot, and repairs it from a healthy copy. The code is in three new modules of the metadata service:
`redundancy.py` (write and read for both schemes), `erasure.py` (Reed-Solomon with zfec) and `repair.py` (repair
jobs and the scrubber). The node registry and node client moved to `cluster.py`.

### Replication with quorum writes and reads
A replicated chunk is stored on the N distinct nodes chosen by the consistent-hashing ring (N = 3 by default,
configurable per bucket with `PUT /buckets/{name}?redundancy=replication&replicas=N`). The write and read quorums
default to majorities, **W = ⌊N/2⌋ + 1** and **R = N − W + 1**, so **W + R > N** (for N = 3: W = 2, R = 2), and both can be
set in `docker-compose.yml`.
*Write:* the chunk is sent to all N nodes in parallel and the upload succeeds as soon as at least W of them confirm.
A replica that could not be written leaves the chunk "degraded", and the repair job completes it later.
*Read:* the chunk is read from one replica and its SHA-256 is checked. HEAD requests to the other replicas then
confirm that at least R replicas hold it. Because W + R > N, every read quorum overlaps every write quorum in at
least one node.
A node that refuses a connection is marked *suspect*, so reads stop waiting on a node that has just crashed but has
not yet missed enough heartbeats to be declared dead.

### Erasure coding (Reed-Solomon k + m)
A bucket created with `?redundancy=ec&k=4&m=2` stores each chunk as **k = 4 data fragments plus m = 2 parity
fragments** computed with the zfec Reed-Solomon library. **Any 4 of the 6 fragments rebuild the chunk**, so up to 2
fragments can be lost, at a storage cost of only (k+m)/k = **1.5×** instead of 3× for replication. Fragment *i* goes to
the *i*-th node in ring order from the chunk's position. With our 4 nodes and 6 fragments, two nodes hold two
fragments each, which still guarantees that any single node failure is survivable (at most 2 fragments lost = m).
Each fragment is stored as its own content-addressed blob with a 42-byte header containing the chunk hash, fragment
index, k, m and padding. The header makes every fragment unique to its chunk. Without it, two chunks with an
identical quarter (for example zero-filled regions of a disk image) would share a fragment, and garbage-collecting
one chunk would destroy the other's data. zfec is *systematic*: fragments 0..k−1 are the original data, so a healthy
read needs no decoding, and decoding is needed only when a parity fragment has to stand in for a lost one. A write
succeeds when at least k + 1 fragments are stored. The policy is chosen per bucket, and buckets with different
policies coexist in the same cluster.

### Failure detection and automatic repair
Nodes send a heartbeat every 3 seconds. A monitor thread in the metadata service checks every second and declares
a node **dead** when its last heartbeat is more than 10 seconds old. It removes the node from the hash ring (new data
goes elsewhere) and starts a **repair job**. A chunk is *degraded* when fewer of its copies are on live nodes than its
policy requires. The job finds every degraded chunk and locks each one (`FOR UPDATE SKIP LOCKED`, so the chunk
cannot be garbage-collected or repaired twice at the same time).
For **replication** it reads the chunk from a healthy replica (verified) and writes it to the next ring node.
For **erasure coding** it reads any k fragments, decodes the chunk, re-encodes it and writes **only the missing
fragments** to the nodes holding the fewest fragments of that chunk.
Progress and traffic (chunks done, bytes read from healthy nodes, bytes written) are stored in the `repair_jobs`
table and exposed by `GET /repair/jobs` and `GET /repair/status`, which also reports the number of degraded objects.
Repair jobs are also started by *read repair* (a read that meets a missing or corrupt copy reports it), by a periodic
scan every 60 seconds as a safety net, and manually with `POST /admin/repair`.

### Background scrubber (bit-rot detection)
Every 5 minutes (or on `POST /admin/scrub`) the scrubber asks every live node to **re-read and re-hash all of its
blobs** (`POST /scrub` on the node). Blobs whose bytes no longer match their SHA-256 name are quarantined by the node.
The metadata service compares the result with PostgreSQL: copies that are corrupt or missing are dropped from the
metadata and rebuilt from a healthy copy by a repair job. Blobs that no metadata refers to (orphans left by a failed
upload, or old copies on a node that was declared dead and came back) are deleted once they have stayed orphans for
2 minutes. The grace period protects uploads that are still in progress.

### Demo results (`scripts/demo_task4_redundancy.py`)
- A 16 MB file stored with replication took **48 MB** on the nodes (12 copies), and with EC 4+2 **24 MB** (24
  fragments of 1 MB).
- With node4 stopped but not yet detected, an 8 MB upload still **succeeded with W = 2**: 4 copies were written
  instead of 6.
- With node4 down, both files were downloaded correctly. The EC file was rebuilt from the remaining fragments.
- About 12 s after the failure, node4 was declared dead and the repair job ran automatically; afterwards
  `degraded_chunks = 0`.
- We corrupted one replica and one EC fragment on disk. The scrub checked 17,104 blobs in 2.8 s, found **2 corrupt**
  copies and repaired both, and the files downloaded correctly afterwards.

### Replication vs erasure coding on both workloads
(`scripts/bench_redundancy.py`: for each run the cluster was reset, the workload uploaded, then node4 was killed and
its disk deleted; the automatic repair job was measured and sampled objects were verified.)

| Workload | Scheme | Unique data | Stored on nodes | Overhead | Lost with node4 | Repair read | Repair written | Read per lost byte | Repair time | Verified |
|---|---|---|---|---|---|---|---|---|---|---|
| small (5000 files) | replication N=3 | 113.3 MB | 339.8 MB | **3.00×** | 81.8 MB | 81.8 MB | 81.8 MB | **1.00** | 25.1 s | 50/50 |
| small (5000 files) | EC 4+2 | 113.3 MB | 171.0 MB | **1.51×** | 43.0 MB | 114.0 MB | 43.0 MB | **2.65** | 99.2 s | 50/50 |
| large (10 files, 4.15 GB) | replication N=3 | 3432 MB | 10292 MB | **3.00×** | 2400 MB | 2404 MB | 2404 MB | **1.00** | 32.5 s | 3/3 |
| large (10 files, 4.15 GB) | EC 4+2 | 3432 MB | 5148 MB | **1.50×** | 1315 MB | 3432 MB | 1315 MB | **2.61** | 61.3 s | 3/3 |

Erasure coding **halves the storage cost** (1.5× instead of 3×) on both workloads, but **repair is more expensive per
lost byte**. Replication rebuilds a lost replica by copying exactly one chunk (1 byte read per byte lost). EC must
read k = 4 fragments, i.e. the whole chunk, to rebuild 1–2 missing fragments (about 2.6 bytes read per byte lost),
and it also spends CPU on decoding and re-encoding. Because EC stores less data, the *total* repair traffic was about
the same for both schemes (small: 157 MB vs 164 MB; large: 4.75 GB vs 4.81 GB), but EC repair took longer: 2.0× on
the large workload and 4× on the small one, where every 1–50 KB file is its own chunk with 6 tiny fragments, so the
per-request cost dominates. Node failures were detected 10–12 s after the kill in every run, every repair finished
with 0 degraded chunks, and all sampled objects downloaded correctly afterwards. Upload throughput from the host was
similar for large files (41.8 vs 39.5 MB/s) but lower for small files with EC (32 vs 51 files/s), again because of the
6 fragment writes per file. Conclusion: replication is the better choice for many small, hot files (cheap, fast
repair and writes); erasure coding is the better choice for large, cold files where the 50 % storage saving matters
most.

## 4.2 Files produced in this phase

| File | Purpose |
|---|---|
| `metadata_service/erasure.py` | Reed-Solomon encode / decode with zfec, self-describing fragment header |
| `metadata_service/redundancy.py` | replication with W/R quorums, EC write/read, policy per bucket, read repair hook |
| `metadata_service/repair.py` | degraded-chunk detection, repair jobs (traffic + progress), scrubber, orphan GC |
| `metadata_service/cluster.py` | node registry, hash ring, failure detection callback, node client |
| `metadata_service/db.py` | new columns (bucket policy, chunk scheme) and tables `chunk_fragments`, `repair_jobs`, `scrub_runs` |
| `metadata_service/main.py` | bucket policy API, `/repair/status`, `/repair/jobs`, `/admin/repair`, `/admin/scrub`, `/scrub/runs` |
| `storage_node/main.py` | new `POST /scrub` endpoint (re-hash everything, quarantine bit rot) |
| `docker-compose.yml` | `WRITE_QUORUM`, `READ_QUORUM`, `REPAIR_*`, `SCRUB_INTERVAL`, `ORPHAN_GRACE` |
| `tests/test_erasure_quorum.py` | 13 tests: every 2-fragment loss is recoverable, < k fails, 1.5× overhead, no fragment collisions, corruption detected, W + R > N |
| `tests/test_storage_node.py` | + scrub test |
| `scripts/demo_task4_redundancy.py` | end-to-end demo of this phase |
| `scripts/bench_redundancy.py` | replication vs EC benchmark (overhead + repair traffic) on both workloads |
| `scripts/make_flowcharts.py` | draws the three flowcharts |
| `docs/flowcharts/write_path.png`, `read_path.png`, `failure_repair_path.png` | flowcharts for the report |
| `benchmarks/redundancy_comparison.csv/.json/.png`, `benchmarks/redundancy_run.log` | benchmark results |

---

## 4.3 Screenshot guide (step by step)

Same setup as before: PowerShell full screen in the project folder, **Windows + PrtScn** to take a full-screen
picture, then move it from `Pictures\Screenshots` to `docs\screenshots` and **rename it** as shown. Type `cls` before
each command.

First make sure everything is running and up to date:
```powershell
docker compose up -d --build postgres metadata node1 node2 node3 node4
```

#### Screenshot 4-01: the unit tests pass
- **Type:**
  ```powershell
  cls; .\venv\Scripts\python.exe -m pytest -v tests/test_erasure_quorum.py tests/test_storage_node.py
  ```
- **You should see:** 21 `PASSED` lines, including `test_any_k_of_k_plus_m_fragments_rebuild_the_chunk`,
  `test_majority_quorums_overlap` and `test_scrub_finds_and_quarantines_bit_rot`.
- **Save as:** `task4_01_unit_tests.png`

#### Screenshots 4-02, 4-03 and 4-04: the Task 4 demo
- **Type** (takes about 1 minute; it stops node4 and starts it again by itself):
  ```powershell
  cls; .\venv\Scripts\python.exe scripts\demo_task4_redundancy.py
  ```
- The output is long, so take **three** screenshots:
  - **Steps 1–3**: the two buckets (`replication N=3, write_quorum 2, read_quorum 2` and `EC 4+2`), the chunk layout
    (3 replicas per chunk vs fragments `f0->node1 … f5->node3`), and the overhead (`x3.0` vs `x1.5`).
    **Save as:** `task4_02_demo_policies.png`
  - **Steps 4–7**: node4 stopped, the upload that still succeeded with W = 2, both degraded reads
    `sha256 ok=True`, node4 marked `dead`, and the repair job line (`done …, chunks repaired`) with
    `degraded_chunks: 0`. **Save as:** `task4_03_demo_failure_repair.png`
  - **Steps 8–9**: node4 back, the two corrupted blobs, the scrub result (`corrupt=2`), and both files verified
    again. **Save as:** `task4_04_demo_scrub.png`

#### Screenshot 4-05: the metadata service log of failure detection and repair
- Take this **right after the demo**.
- **Type:**
  ```powershell
  cls; docker logs metadata --since 10m 2>&1 | Select-String "DEAD|ALIVE|ring rebuilt|repair job|scrub run|read repair"
  ```
- **You should see:** `node node4 is DEAD`, `ring rebuilt: ['node1', 'node2', 'node3']`,
  `repair job N started (node node4 failed …)`, `repair job N done …`, `node node4 is ALIVE`, and the scrub-run and
  repair lines.
- **Save as:** `task4_05_failure_log.png`

#### Screenshot 4-06: repair jobs and fragments in PostgreSQL
- **Type** (both lines at once):
  ```powershell
  cls; docker exec postgres psql -U dfs -d dfs -c "SELECT id, left(reason,45) AS reason, status, chunks_done AS done, chunks_total AS total, blobs_written, round(bytes_read/1048576.0,1) AS read_mb, round(bytes_written/1048576.0,1) AS written_mb FROM repair_jobs ORDER BY id DESC LIMIT 6;"
  docker exec postgres psql -U dfs -d dfs -c "SELECT left(f.hash,12) AS chunk, f.idx, f.node_id, f.size FROM chunk_fragments f JOIN object_chunks oc ON oc.chunk_hash=f.hash JOIN objects o ON o.id=oc.object_id WHERE o.bucket='ec42' AND oc.idx=0 ORDER BY f.idx;"
  ```
- **You should see:** the latest repair jobs (node failure and scrub) with their traffic, and the 6 fragments
  (idx 0–5) of one EC chunk with the node holding each one.
- **Save as:** `task4_06_postgres_repair.png`

#### Screenshot 4-07: repair status in the browser
- **Open** <http://localhost:8100/repair/jobs> (tick **Pretty-print** if shown).
- **You should see:** the list of repair jobs with `reason`, `status`, `chunks_done`, `bytes_read`, `bytes_written`
  and `seconds`.
- **Save as:** `task4_07_repair_jobs_api.png`

#### Screenshot 4-08: the new API endpoints (Swagger)
- **Open** <http://localhost:8100/docs>, press **F11**, and scroll so that `PUT /buckets/{name}` (with the
  `redundancy`, `replicas`, `k`, `m` parameters), `/repair/status`, `/repair/jobs`, `/admin/repair`,
  `/admin/scrub` and `/scrub/runs` are visible. Click `PUT /buckets/{name}` once to open it and show its parameters.
- **Save as:** `task4_08_swagger.png`

#### Screenshot 4-09: failure seen by the cluster (optional but nice)
- **Type:** `docker stop node4`, wait **15 seconds**, then:
  - open **Docker Desktop → Containers**: `node4` is grey (stopped);
  - open <http://localhost:8100/nodes>: node4 has `"status": "dead"`;
  - open <http://localhost:8100/repair/jobs>: the newest job says `node node4 failed`.
- Put Docker Desktop and the browser side by side (drag one to the left edge and one to the right edge of the
  screen), then take the screenshot.
- **Afterwards type:** `docker start node4`
- **Save as:** `task4_09_node_failure.png`

#### Screenshot 4-10: replication vs erasure coding results (table)
The benchmark takes about 30 minutes and wipes the cluster, so you don't need to run it again. Show its saved
results instead:
- **Type:**
  ```powershell
  cls; Import-Csv benchmarks\redundancy_comparison.csv | Format-Table workload, scheme, logical_MB, unique_MB, physical_MB, storage_overhead, lost_on_node4_MB, repair_read_MB, repair_written_MB, repair_read_per_lost_byte, repair_seconds, verified -AutoSize
  ```
- **Save as:** `task4_10_benchmark_table.png`

#### Screenshot 4-11: replication vs erasure coding chart
- **Type:** `start benchmarks\redundancy_comparison.png`, then maximise the window.
- **Save as:** `task4_11_benchmark_chart.png`

#### Screenshot 4-12: the benchmark log
- **Type:** `cls; Get-Content benchmarks\redundancy_run.log | Select-String -NotMatch '^\[.*\] \{'`
- **You should see:** each run (reset, upload, node4 killed, repair job done, objects verified) with timestamps.
- **Save as:** `task4_12_benchmark_log.png`

> The three flowcharts (`docs/flowcharts/*.png`) are images that go into the report directly. You don't need to
> screenshot them.

### Checklist
`task4_01_unit_tests.png`, `task4_02_demo_policies.png`, `task4_03_demo_failure_repair.png`,
`task4_04_demo_scrub.png`, `task4_05_failure_log.png`, `task4_06_postgres_repair.png`,
`task4_07_repair_jobs_api.png`, `task4_08_swagger.png`, `task4_09_node_failure.png` (optional),
`task4_10_benchmark_table.png`, `task4_11_benchmark_chart.png`, `task4_12_benchmark_log.png`
