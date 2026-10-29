"""Consistent hashing ring with virtual nodes.

Every physical node is placed on a 64-bit ring at `vnodes` pseudo-random
positions (SHA-256 of "<node_id>#<i>"). A chunk is mapped to the ring at the
position given by the first 64 bits of its own SHA-256 hash and is stored on
the first N *distinct* physical nodes found walking clockwise.

When a node joins it takes over only the arcs just before its own virtual
nodes, so roughly 1/(nodes) of the data moves; when it leaves, only the data it
held moves to its successors. Virtual nodes spread each node over many small
arcs, which evens out the load and spreads the moved data across all nodes.
"""
import bisect
import hashlib

DEFAULT_VNODES = 256


def _pos(s: str) -> int:
    return int.from_bytes(hashlib.sha256(s.encode()).digest()[:8], "big")


class HashRing:
    def __init__(self, nodes=(), vnodes: int = DEFAULT_VNODES):
        self.vnodes = vnodes
        self.nodes: set[str] = set()
        self._keys: list[int] = []
        self._owners: list[str] = []
        for n in nodes:
            self.add(n)

    def _rebuild(self, points: list[tuple[int, str]]) -> None:
        points.sort()
        self._keys = [p for p, _ in points]
        self._owners = [n for _, n in points]

    def add(self, node: str) -> None:
        if node in self.nodes:
            return
        self.nodes.add(node)
        points = list(zip(self._keys, self._owners))
        points += [(_pos(f"{node}#{i}"), node) for i in range(self.vnodes)]
        self._rebuild(points)

    def remove(self, node: str) -> None:
        self.nodes.discard(node)
        self._rebuild([(p, n) for p, n in zip(self._keys, self._owners) if n != node])

    def get_nodes(self, chunk_hash: str, n: int) -> list[str]:
        """The n distinct nodes responsible for a chunk, primary first."""
        if not self._keys:
            return []
        n = min(n, len(self.nodes))
        start = bisect.bisect_right(self._keys, int(chunk_hash[:16], 16))  # O(log V) lookup
        out: list[str] = []
        for i in range(len(self._keys)):
            owner = self._owners[(start + i) % len(self._keys)]
            if owner not in out:
                out.append(owner)
                if len(out) == n:
                    break
        return out
