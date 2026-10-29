"""Unit tests for the storage node service (run: pytest tests/test_storage_node.py)."""
import hashlib
import importlib
import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("METADATA_URL", "")  # no heartbeats in unit tests
    monkeypatch.setenv("ENABLE_FAULT_INJECTION", "1")
    import storage_node.main as main
    main = importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def h(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_put_get_roundtrip(client):
    data = b"hello distributed storage"
    r = client.put(f"/chunks/{h(data)}", content=data)
    assert r.status_code == 201 and r.json()["created"] is True
    r = client.get(f"/chunks/{h(data)}")
    assert r.status_code == 200 and r.content == data


def test_put_is_idempotent(client):
    data = b"same chunk twice"
    assert client.put(f"/chunks/{h(data)}", content=data).status_code == 201
    r = client.put(f"/chunks/{h(data)}", content=data)
    assert r.status_code == 200 and r.json()["created"] is False
    assert client.get("/health").json()["chunk_count"] == 1


def test_put_rejects_hash_mismatch(client):
    r = client.put(f"/chunks/{h(b'original')}", content=b"tampered")
    assert r.status_code == 400


def test_invalid_hash_format(client):
    assert client.get("/chunks/not-a-hash").status_code == 400


def test_head_and_delete(client):
    data = os.urandom(4096)
    client.put(f"/chunks/{h(data)}", content=data)
    r = client.head(f"/chunks/{h(data)}")
    assert r.status_code == 200 and r.headers["content-length"] == "4096"
    assert client.delete(f"/chunks/{h(data)}").status_code == 204
    assert client.head(f"/chunks/{h(data)}").status_code == 404
    assert client.delete(f"/chunks/{h(data)}").status_code == 404


def test_corrupted_chunk_is_rejected_and_quarantined(client):
    data = b"this chunk will rot"
    client.put(f"/chunks/{h(data)}", content=data)
    assert client.post(f"/admin/corrupt/{h(data)}").status_code == 200
    assert client.get(f"/chunks/{h(data)}").status_code == 409
    # quarantined: no longer served or counted
    assert client.get(f"/chunks/{h(data)}").status_code == 404
    assert client.get("/health").json()["chunk_count"] == 0


def test_list_chunks(client):
    blobs = [os.urandom(100) for _ in range(5)]
    for b in blobs:
        client.put(f"/chunks/{h(b)}", content=b)
    listed = client.get("/chunks").json()
    assert listed["count"] == 5 and set(listed["chunks"]) == {h(b) for b in blobs}


def test_scrub_finds_and_quarantines_bit_rot(client):
    blobs = [os.urandom(500) for _ in range(4)]
    for b in blobs:
        client.put(f"/chunks/{h(b)}", content=b)
    client.post(f"/admin/corrupt/{h(blobs[2])}")
    res = client.post("/scrub").json()
    assert res["checked"] == 4 and res["corrupt"] == [h(blobs[2])]
    assert set(res["present"]) == {h(b) for i, b in enumerate(blobs) if i != 2}
    assert client.post("/scrub").json()["corrupt"] == []  # quarantined, no longer served
