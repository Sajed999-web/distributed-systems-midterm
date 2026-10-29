"""Unit tests for chunking (fixed / FastCDC) and the consistent-hashing ring."""
import hashlib
import os
from collections import Counter

import pytest

from metadata_service.chunking import cdc_chunks, fixed_chunks
from metadata_service.ring import HashRing

KiB = 1024


def _write(tmp_path, name, data):
    p = tmp_path / name
    p.write_bytes(data)
    return str(p)


def _hashes(path, bounds):
    data = open(path, "rb").read()
    return [hashlib.sha256(data[o:o + n]).hexdigest() for o, n in bounds]


@pytest.mark.parametrize("chunker", ["fixed", "cdc"])
def test_chunks_cover_file_exactly(tmp_path, chunker):
    path = _write(tmp_path, "f", os.urandom(1_000_003))
    bounds = (fixed_chunks(path, 64 * KiB) if chunker == "fixed"
              else cdc_chunks(path, 16 * KiB, 64 * KiB, 256 * KiB))
    assert bounds[0][0] == 0
    assert all(a[0] + a[1] == b[0] for a, b in zip(bounds, bounds[1:]))
    assert sum(n for _, n in bounds) == 1_000_003


def test_cdc_respects_size_limits(tmp_path):
    path = _write(tmp_path, "f", os.urandom(8 * 1024 * KiB))
    bounds = cdc_chunks(path, 16 * KiB, 64 * KiB, 256 * KiB)
    sizes = [n for _, n in bounds[:-1]]
    assert min(sizes) >= 16 * KiB and max(sizes) <= 256 * KiB
    assert 32 * KiB < sum(sizes) / len(sizes) < 128 * KiB  # close to the target average


def test_cdc_realigns_after_insertion_but_fixed_does_not(tmp_path):
    base = os.urandom(4 * 1024 * KiB)
    edited = base[:1000] + b"INSERTED BYTES" + base[1000:]
    p1, p2 = _write(tmp_path, "v1", base), _write(tmp_path, "v2", edited)

    def shared(chunker):
        a, b = set(_hashes(p1, chunker(p1))), _hashes(p2, chunker(p2))
        return sum(h in a for h in b) / len(b)

    assert shared(lambda p: fixed_chunks(p, 64 * KiB)) < 0.05
    assert shared(lambda p: cdc_chunks(p, 16 * KiB, 64 * KiB, 256 * KiB)) > 0.9


def test_ring_returns_distinct_nodes_deterministically():
    ring = HashRing(["n1", "n2", "n3", "n4"])
    h = hashlib.sha256(b"x").hexdigest()
    nodes = ring.get_nodes(h, 3)
    assert len(set(nodes)) == 3 and nodes == HashRing(["n4", "n3", "n2", "n1"]).get_nodes(h, 3)


def test_ring_balance_and_minimal_movement():
    hashes = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(20000)]
    ring4 = HashRing([f"n{i}" for i in range(1, 5)])
    load = Counter(ring4.get_nodes(h, 1)[0] for h in hashes)
    assert max(load.values()) / min(load.values()) < 1.3  # virtual nodes even out the load

    ring5 = HashRing([f"n{i}" for i in range(1, 6)])
    moved = sum(ring4.get_nodes(h, 1) != ring5.get_nodes(h, 1) for h in hashes) / len(hashes)
    assert 0.15 < moved < 0.25  # ~1/5 of the data moves to the new node
    # and everything that moved went to the new node
    assert all(ring5.get_nodes(h, 1) == ["n5"] for h in hashes if ring4.get_nodes(h, 1) != ring5.get_nodes(h, 1))
