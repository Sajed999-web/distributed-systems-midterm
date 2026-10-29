"""Draw the report flowcharts (write path, read path, failure & repair path).

    python scripts/make_flowcharts.py      ->  docs/flowcharts/*.png

Plain matplotlib, so no Graphviz / Mermaid installation is needed.
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Polygon  # noqa: E402

OUT = Path("docs/flowcharts")

STYLE = {  # kind -> (fill, edge)
    "start": ("#d7f0e3", "#1f9e75"),
    "end": ("#d7f0e3", "#1f9e75"),
    "process": ("#e3eefb", "#2a78d6"),
    "node": ("#ece6f7", "#7356b8"),      # work done on a storage node
    "db": ("#fdf0dc", "#c98a1c"),        # PostgreSQL
    "decision": ("#fff6d6", "#b58a00"),
    "error": ("#fbe1dd", "#c8442e"),
}
INK = "#1f2328"


class Chart:
    def __init__(self, title: str, width: float, height: float):
        self.fig, self.ax = plt.subplots(figsize=(width, height))
        self.ax.set_xlim(0, width)
        self.ax.set_ylim(-height, 0)
        self.ax.axis("off")
        self.ax.set_title(title, fontsize=15, fontweight="bold", color=INK, loc="left", pad=12)
        self.boxes: dict[str, tuple[float, float, float, float]] = {}

    def box(self, key, x, y, text, kind="process", w=3.4, h=0.78):
        fill, edge = STYLE[kind]
        if kind == "decision":
            w, h = max(w, 3.0), max(h, 1.15)
            self.ax.add_patch(Polygon([(x, y + h / 2), (x + w / 2, y), (x, y - h / 2), (x - w / 2, y)],
                                      closed=True, fc=fill, ec=edge, lw=1.6, zorder=2))
        else:
            rounding = 0.38 if kind in ("start", "end") else 0.08
            self.ax.add_patch(FancyBboxPatch((x - w / 2, y - h / 2), w, h,
                                             boxstyle=f"round,pad=0,rounding_size={rounding}",
                                             fc=fill, ec=edge, lw=1.6, zorder=2))
        self.ax.text(x, y, text, ha="center", va="center", fontsize=9.2, color=INK, zorder=3, linespacing=1.25)
        self.boxes[key] = (x, y, w, h)

    def _anchor(self, key, side):
        x, y, w, h = self.boxes[key]
        return {"top": (x, y + h / 2), "bottom": (x, y - h / 2),
                "left": (x - w / 2, y), "right": (x + w / 2, y)}[side]

    def arrow(self, a, b, sa="bottom", sb="top", label=None, via=(), label_at=0, color="#57606a"):
        pts = [self._anchor(a, sa), *via, self._anchor(b, sb)]
        for p, q in zip(pts[:-2], pts[1:-1]):
            self.ax.plot([p[0], q[0]], [p[1], q[1]], color=color, lw=1.4, zorder=1, solid_capstyle="round")
        self.ax.add_patch(FancyArrowPatch(pts[-2], pts[-1], arrowstyle="-|>", mutation_scale=14,
                                          color=color, lw=1.4, zorder=1, shrinkA=0, shrinkB=1))
        if label:
            p, q = pts[label_at], pts[label_at + 1]
            self.ax.text((p[0] + q[0]) / 2, (p[1] + q[1]) / 2, label, ha="center", va="center", fontsize=8.6,
                         fontweight="bold", color=color, zorder=4,
                         bbox=dict(fc="white", ec="none", pad=1.5))

    def legend(self, x, y):
        items = [("process", "metadata service"), ("node", "storage node"), ("db", "PostgreSQL"),
                 ("decision", "decision"), ("error", "error / failure")]
        for i, (kind, text) in enumerate(items):
            fill, edge = STYLE[kind]
            self.ax.add_patch(FancyBboxPatch((x, y - i * 0.38 - 0.13), 0.42, 0.26,
                                             boxstyle="round,pad=0,rounding_size=0.05", fc=fill, ec=edge, lw=1.2))
            self.ax.text(x + 0.55, y - i * 0.38, text, va="center", fontsize=8.5, color=INK)

    def save(self, name):
        OUT.mkdir(parents=True, exist_ok=True)
        self.fig.savefig(OUT / name, dpi=170, bbox_inches="tight", facecolor="white")
        plt.close(self.fig)
        print("->", OUT / name)


def write_path():
    c = Chart("Write path  (PUT /objects/{bucket}/{key})", 12.5, 17.2)
    X = 5.0
    c.box("start", X, -0.6, "Client uploads a file\nPUT /objects/{bucket}/{key}", "start")
    c.box("spool", X, -1.8, "Stream the body to a spool file\nand compute the SHA-256 of the object")
    c.box("split", X, -3.0, "Split into chunks\nfixed 4 MiB  or  FastCDC (avg 4 MiB)")
    c.box("hash", X, -4.2, "SHA-256 of every chunk\n= its content address (chunk ID)")
    c.box("txn", X, -5.4, "BEGIN transaction, lock the key,\nload the bucket's redundancy policy", "db")
    c.box("upsert", X, -6.6, "For each unique chunk: INSERT ... ON CONFLICT\nrefcount = refcount + 1", "db", w=4.0)
    c.box("new", X, -8.0, "Chunk already\nstored?", "decision")
    c.box("dedup", 10.3, -8.0, "Deduplicated:\nnothing is sent\nto the storage nodes", "process", w=2.6, h=1.0)
    c.box("policy", X, -9.6, "Bucket policy?", "decision")
    c.box("rep", 2.4, -11.2, "Ring: N = 3 distinct nodes\nPUT the chunk to all 3\nin parallel", "process", w=3.2, h=1.0)
    c.box("ec", 7.6, -11.2, "Reed-Solomon encode into\nk + m fragments (e.g. 4+2);\nPUT fragment i to i-th ring node",
          "process", w=3.4, h=1.0)
    c.box("nodechk", X, -12.7, "Each storage node re-hashes the body, rejects a mismatch (400)\n"
                               "and writes atomically: temp file -> fsync -> rename", "node", w=6.6)
    c.box("quorum", X, -14.1, "Enough copies?\nrep: >= W = 2 acks\nEC: >= k + 1 fragments", "decision", w=3.6, h=1.45)
    c.box("fail", 1.3, -14.1, "ROLLBACK\nHTTP 503", "error", w=1.9)
    c.box("record", X, -15.5, "Record replicas / fragments, insert object version\n"
                              "+ ordered chunk list, COMMIT", "db", w=4.6)
    c.box("end", X, -16.6, "201 Created  (missing copies are rebuilt later by the repair job)", "end", w=5.6)
    c.legend(9.5, -0.5)

    for a, b in [("start", "spool"), ("spool", "split"), ("split", "hash"), ("hash", "txn"), ("txn", "upsert"),
                 ("upsert", "new")]:
        c.arrow(a, b)
    c.arrow("new", "dedup", "right", "left", label="yes")
    c.arrow("new", "policy", label="no (new chunk)")
    c.arrow("policy", "rep", "left", "top", label="replication", via=[(2.4, -9.6)])
    c.arrow("policy", "ec", "right", "top", label="erasure coding", via=[(7.6, -9.6)])
    c.arrow("rep", "nodechk", "bottom", "top", via=[(2.4, -12.05), (X, -12.05)])
    c.arrow("ec", "nodechk", "bottom", "top", via=[(7.6, -12.05), (X, -12.05)])
    c.arrow("nodechk", "quorum")
    c.arrow("quorum", "fail", "left", "right", label="no")
    c.arrow("quorum", "record", label="yes")
    c.arrow("dedup", "record", "bottom", "right", via=[(10.3, -15.5)])
    c.arrow("record", "end")
    c.save("write_path.png")


def read_path():
    c = Chart("Read path  (GET /objects/{bucket}/{key})", 13.4, 16.4)
    X = 6.2
    c.box("start", X, -0.6, "Client downloads a file\nGET /objects/{bucket}/{key}[?version=]", "start", w=3.6)
    c.box("lookup", X, -1.8, "Look up the object version and its\nordered chunk list (+ replicas / fragments)", "db",
          w=4.2)
    c.box("exists", X, -3.2, "Object found?", "decision")
    c.box("404", 10.0, -3.2, "HTTP 404", "error", w=1.8)
    c.box("next", X, -4.6, "Take the next chunk in order")
    c.box("scheme", X, -6.0, "Chunk stored\nwith?", "decision")
    # replication branch (left)
    c.box("rget", 3.0, -7.6, "GET from the first usable\nreplica", "process", w=3.0)
    c.box("rver", 3.0, -8.9, "Node re-hashes the chunk:\nreturns 409 if corrupt", "node", w=3.0)
    c.box("rok", 3.0, -10.35, "Valid copy?", "decision", w=2.6)
    c.box("rbad", 1.25, -12.0, "Read repair:\nreport bad copy,\ntry next replica", "error", w=1.8, h=1.05)
    c.box("rhead", 3.5, -12.0, "HEAD other replicas\nuntil R = 2 agree", "process", w=2.3)
    # EC branch (right)
    c.box("eget", 9.4, -7.6, "GET k fragments in parallel\n(data fragments first)", "process", w=3.2)
    c.box("ever", 9.4, -8.9, "Nodes re-hash each fragment", "node", w=3.2)
    c.box("eok", 9.4, -10.35, "k valid\nfragments?", "decision", w=2.6)
    c.box("ebad", 11.75, -12.0, "Read repair:\nreport bad fragment,\nfetch a parity one", "error", w=2.1, h=1.05)
    c.box("edec", 8.8, -12.0, "Reed-Solomon decode\nif parity was used", "process", w=2.5)
    c.box("verify", X, -13.4, "Verify SHA-256 of the chunk,\nstream it to the client", w=3.4)
    c.box("more", X, -14.75, "More chunks?", "decision")
    c.box("end", X, -16.0, "200 OK: the whole file is streamed", "end", w=3.6)
    c.legend(10.6, -0.4)

    c.arrow("start", "lookup")
    c.arrow("lookup", "exists")
    c.arrow("exists", "404", "right", "left", label="no")
    c.arrow("exists", "next", label="yes")
    c.arrow("next", "scheme")
    c.arrow("scheme", "rget", "left", "top", label="replication", via=[(3.0, -6.0)])
    c.arrow("scheme", "eget", "right", "top", label="erasure coding", via=[(9.4, -6.0)])
    c.arrow("rget", "rver")
    c.arrow("rver", "rok")
    c.arrow("rok", "rbad", "left", "top", label="no", via=[(1.25, -10.35)])
    c.arrow("rbad", "rget", "left", "left", via=[(0.12, -12.0), (0.12, -7.6)])
    c.arrow("rok", "rhead", "bottom", "top", label="yes", via=[(3.0, -11.3), (3.5, -11.3)])
    c.arrow("eget", "ever")
    c.arrow("ever", "eok")
    c.arrow("eok", "ebad", "right", "top", label="no", via=[(11.75, -10.35)])
    c.arrow("ebad", "eget", "bottom", "right", via=[(11.75, -12.7), (12.95, -12.7), (12.95, -7.6)])
    c.arrow("eok", "edec", "bottom", "top", label="yes", via=[(9.4, -11.3), (8.8, -11.3)])
    c.arrow("rhead", "verify", "bottom", "top", via=[(3.5, -12.75), (X, -12.75)])
    c.arrow("edec", "verify", "bottom", "top", via=[(8.8, -12.75), (X, -12.75)])
    c.arrow("verify", "more")
    c.arrow("more", "next", "right", "right", label="yes", via=[(13.25, -14.75), (13.25, -4.6)], label_at=0)
    c.arrow("more", "end", label="no")
    c.save("read_path.png")


def failure_repair_path():
    c = Chart("Failure detection & repair path", 13.5, 17.6)
    # three triggers
    c.box("hb", 2.3, -0.7, "Every node sends a heartbeat\nevery 3 s (id, free space,\nchunk count)", "node", w=3.3, h=1.0)
    c.box("mon", 2.3, -2.1, "Monitor (every 1 s):\nlast heartbeat > 10 s ago?", "decision", w=3.5, h=1.3)
    c.box("dead", 2.3, -3.75, "Mark node DEAD,\nremove it from the ring", "error", w=3.1)
    c.box("scrub", 6.75, -0.7, "Scrubber (every 5 min):\neach node re-reads and re-hashes\nall its blobs", "node",
          w=3.5, h=1.0)
    c.box("quar", 6.75, -2.1, "Corrupt blobs are quarantined;\ncompare with the metadata", "node", w=3.5)
    c.box("drop", 6.75, -3.75, "Drop corrupt / missing copies\nfrom the metadata", "db", w=3.4)
    c.box("other", 11.4, -0.7, "Other triggers:\nread repair, periodic scan\n(60 s), POST /admin/repair",
          "process", w=3.3, h=1.0)
    c.box("job", 6.75, -5.4, "Start a repair job (logged in repair_jobs)", "db", w=4.6)
    c.box("find", 6.75, -6.65, "Find degraded chunks: live replicas < N\nor live fragments < k + m", "db", w=4.6)
    c.box("lock", 6.75, -7.95, "Take the next degraded chunk\n(row lock: FOR UPDATE SKIP LOCKED)", "db", w=4.6)
    c.box("enough", 6.75, -9.4, "Enough healthy\ncopies left?", "decision", w=3.2, h=1.25)
    c.box("lost", 11.6, -9.4, "Unrecoverable:\ncount as failed,\nlog an error", "error", w=2.6, h=1.0)
    c.box("scheme", 6.75, -10.95, "Scheme?", "decision", w=2.8)
    c.box("rcopy", 3.2, -12.6, "Read the chunk from a healthy\nreplica (verified) and write it\n"
                               "to the next ring node(s)", "process", w=3.5, h=1.05)
    c.box("ecrb", 10.3, -12.6, "Read any k fragments, decode,\nre-encode, write ONLY the missing\n"
                               "fragments to least-loaded nodes", "process", w=3.7, h=1.05)
    c.box("update", 6.75, -14.1, "Update chunk_replicas / chunk_fragments,\nadd bytes read / written to the job",
          "db", w=4.6)
    c.box("more", 6.75, -15.45, "More degraded\nchunks?", "decision", w=3.0)
    c.box("done", 6.75, -16.75, "Job done: degraded chunks = 0, data back to full redundancy", "end", w=5.6)

    c.arrow("hb", "mon")
    c.arrow("mon", "dead", label="yes")
    c.arrow("mon", "hb", "left", "left", label="no", via=[(0.15, -2.1), (0.15, -0.7)], label_at=0)
    c.arrow("scrub", "quar")
    c.arrow("quar", "drop")
    c.arrow("dead", "job", "bottom", "left", via=[(2.3, -5.4)])
    c.arrow("drop", "job")
    c.arrow("other", "job", "bottom", "right", via=[(11.4, -5.4)])
    c.arrow("job", "find")
    c.arrow("find", "lock")
    c.arrow("lock", "enough")
    c.arrow("enough", "lost", "right", "left", label="no")
    c.arrow("enough", "scheme", label="yes")
    c.arrow("scheme", "rcopy", "left", "top", label="replication", via=[(3.2, -10.95)])
    c.arrow("scheme", "ecrb", "right", "top", label="erasure coding", via=[(10.3, -10.95)])
    c.arrow("rcopy", "update", "bottom", "top", via=[(3.2, -13.45), (6.75, -13.45)])
    c.arrow("ecrb", "update", "bottom", "top", via=[(10.3, -13.45), (6.75, -13.45)])
    c.arrow("lost", "more", "right", "right", via=[(13.3, -9.4), (13.3, -15.45)])
    c.arrow("update", "more")
    c.arrow("more", "lock", "left", "left", label="yes", via=[(1.0, -15.45), (1.0, -7.95)], label_at=0)
    c.arrow("more", "done", label="no")
    c.save("failure_repair_path.png")


if __name__ == "__main__":
    write_path()
    read_path()
    failure_repair_path()
