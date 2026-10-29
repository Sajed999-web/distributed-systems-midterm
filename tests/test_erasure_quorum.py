"""Unit tests for Reed-Solomon erasure coding and quorum sizes."""
import hashlib
import itertools
import os

import pytest

from metadata_service import erasure
from metadata_service.redundancy import quorums


def _h(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("size", [1, 3, 1000, 4 * 1024 * 1024 + 7])
def test_any_k_of_k_plus_m_fragments_rebuild_the_chunk(size):
    data = os.urandom(size)
    frags = erasure.encode(_h(data), data, 4, 2)
    assert len(frags) == 6
    for subset in itertools.combinations(frags, 4):  # every way of losing 2 fragments
        assert erasure.decode([f.blob for f in subset]) == data


def test_fewer_than_k_fragments_fail():
    data = os.urandom(10_000)
    frags = erasure.encode(_h(data), data, 4, 2)
    with pytest.raises(ValueError):
        erasure.decode([f.blob for f in frags[:3]])


def test_fragment_overhead_is_about_k_plus_m_over_k():
    data = os.urandom(4 * 1024 * 1024)
    frags = erasure.encode(_h(data), data, 4, 2)
    assert sum(len(f.blob) for f in frags) / len(data) == pytest.approx(1.5, abs=0.001)


def test_fragments_of_different_chunks_never_collide():
    # two chunks whose first quarter is identical (e.g. zero-filled disk regions)
    a = bytes(1000) + os.urandom(3000)
    b = bytes(1000) + os.urandom(3000)
    fa, fb = erasure.encode(_h(a), a, 4, 2), erasure.encode(_h(b), b, 4, 2)
    assert fa[0].blob[erasure.HEADER.size:] == fb[0].blob[erasure.HEADER.size:]  # same payload...
    assert fa[0].blob_hash != fb[0].blob_hash                                    # ...but distinct blobs


def test_corrupted_fragment_is_detected():
    data = os.urandom(5000)
    frags = erasure.encode(_h(data), data, 2, 2)
    bad = bytearray(frags[0].blob)
    bad[-1] ^= 0xFF
    with pytest.raises(ValueError):
        erasure.decode([bytes(bad), frags[1].blob])


@pytest.mark.parametrize("n", [1, 2, 3, 4, 5])
def test_majority_quorums_overlap(n):
    w, r = quorums(n)
    assert w + r > n and 1 <= w <= n and 1 <= r <= n
    assert quorums(3) == (2, 2)
