"""Content-addressed chunk store on the node's local disk.

Every chunk is stored under the SHA-256 hash of its own bytes:
    <root>/chunks/ab/cd/abcd...  (two-level fan-out keeps directories small)

Writes go to a temp file first and are then atomically renamed, so a crash
never leaves a half-written chunk under a valid hash. Reads re-hash the data
and refuse to return anything whose hash does not match its name; such chunks
are moved to <root>/quarantine so they can be repaired from another replica.
"""
import hashlib
import os
import re
import shutil
import tempfile
import threading
from pathlib import Path

HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class InvalidHashError(ValueError):
    """The name is not a valid lowercase hex SHA-256 digest."""


class HashMismatchError(ValueError):
    """The uploaded bytes do not hash to the name they were sent under."""


class CorruptChunkError(Exception):
    """The stored bytes no longer match their hash (bit rot / tampering)."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ChunkStore:
    def __init__(self, root: str | os.PathLike):
        self.root = Path(root)
        self.chunks_dir = self.root / "chunks"
        self.quarantine_dir = self.root / "quarantine"
        self.tmp_dir = self.root / "tmp"
        for d in (self.chunks_dir, self.quarantine_dir, self.tmp_dir):
            d.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.chunk_count, self.used_bytes = self._scan()

    # ---------------------------------------------------------------- helpers
    def _scan(self) -> tuple[int, int]:
        count = size = 0
        for p in self.chunks_dir.glob("*/*/*"):
            if p.is_file():
                count += 1
                size += p.stat().st_size
        return count, size

    def _path(self, h: str) -> Path:
        if not HASH_RE.match(h):
            raise InvalidHashError(h)
        return self.chunks_dir / h[:2] / h[2:4] / h

    # ------------------------------------------------------------- operations
    def put(self, h: str, data: bytes) -> bool:
        """Store a chunk. Returns True if newly written, False if already present."""
        path = self._path(h)
        if sha256_hex(data) != h:
            raise HashMismatchError(h)
        if path.exists():
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.tmp_dir)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        with self._lock:
            if path.exists():  # a concurrent PUT of the same chunk won the race
                os.remove(tmp)
                return False
            os.replace(tmp, path)
            self.chunk_count += 1
            self.used_bytes += len(data)
        return True

    def get(self, h: str) -> bytes:
        """Read a chunk and verify it. Raises FileNotFoundError / CorruptChunkError."""
        path = self._path(h)
        data = path.read_bytes()
        if sha256_hex(data) != h:
            self._quarantine(h, path, len(data))
            raise CorruptChunkError(h)
        return data

    def size(self, h: str) -> int | None:
        path = self._path(h)
        return path.stat().st_size if path.exists() else None

    def delete(self, h: str) -> bool:
        path = self._path(h)
        with self._lock:
            try:
                size = path.stat().st_size
                path.unlink()
            except FileNotFoundError:
                return False
            self.chunk_count -= 1
            self.used_bytes -= size
        return True

    def list(self) -> list[str]:
        return [p.name for p in self.chunks_dir.glob("*/*/*") if p.is_file()]

    def _quarantine(self, h: str, path: Path, size: int) -> None:
        with self._lock:
            try:
                shutil.move(str(path), str(self.quarantine_dir / h))
            except FileNotFoundError:
                return
            self.chunk_count -= 1
            self.used_bytes -= size

    def corrupt(self, h: str) -> None:
        """Fault injection: flip one byte in place to simulate bit rot."""
        path = self._path(h)
        with open(path, "r+b") as f:
            b = f.read(1)
            f.seek(0)
            f.write(bytes([b[0] ^ 0xFF]) if b else b"\x00")

    def disk_usage(self) -> dict:
        du = shutil.disk_usage(self.root)
        return {"total_bytes": du.total, "free_bytes": du.free}
