"""Reed-Solomon erasure coding of chunks (zfec).

A chunk is split into k equal data fragments plus m parity fragments; ANY k of
the k+m fragments are enough to rebuild the chunk. zfec is systematic: the
first k fragments are the plain data, so a healthy read needs no decoding.

Every fragment is stored on the storage nodes as its own content-addressed
blob (named by the SHA-256 of the blob). The blob starts with a small header
(chunk hash, fragment index, k, m, padding), which makes each blob unique to
its chunk -- two chunks can never share a fragment blob, so deleting one
chunk can never destroy another chunk's data -- and self-describing.
"""
import hashlib
import struct

from zfec.easyfec import Decoder, Encoder

MAGIC = b"RSF1"
HEADER = struct.Struct(">4s32sBBBI")  # magic, chunk sha256, index, k, m, padlen


class Fragment:
    __slots__ = ("index", "blob", "blob_hash", "payload_size")

    def __init__(self, index: int, blob: bytes):
        self.index = index
        self.blob = blob
        self.blob_hash = hashlib.sha256(blob).hexdigest()
        self.payload_size = len(blob) - HEADER.size


def encode(chunk_hash: str, data: bytes, k: int, m: int) -> list[Fragment]:
    shares = Encoder(k, k + m).encode(data)
    padlen = len(shares[0]) * k - len(data)
    return [Fragment(i, HEADER.pack(MAGIC, bytes.fromhex(chunk_hash), i, k, m, padlen) + bytes(s))
            for i, s in enumerate(shares)]


def parse(blob: bytes) -> tuple[str, int, int, int, int, bytes]:
    magic, h, idx, k, m, padlen = HEADER.unpack_from(blob)
    if magic != MAGIC:
        raise ValueError("not an erasure-coded fragment")
    return h.hex(), idx, k, m, padlen, blob[HEADER.size:]


def decode(blobs: list[bytes]) -> bytes:
    """Rebuild a chunk from any k fragment blobs of it."""
    parsed = [parse(b) for b in blobs]
    chunk_hash, _, k, m, padlen, _ = parsed[0]
    if len(parsed) < k:
        raise ValueError(f"need {k} fragments, got {len(parsed)}")
    parsed = sorted(parsed, key=lambda p: p[1])[:k]
    data = Decoder(k, k + m).decode([p[5] for p in parsed], [p[1] for p in parsed], padlen)
    if hashlib.sha256(data).hexdigest() != chunk_hash:
        raise ValueError("decoded chunk failed SHA-256 verification")
    return data
