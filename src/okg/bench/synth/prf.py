"""Deterministic HMAC-based PRF — the #754 pattern, standalone.

Every random choice in the synth core flows through one keyed PRF so that
``(config, seed, world, pack)`` yields byte-identical artifacts across runs
and platforms, and distinct seeds yield disjoint draw streams. This is a
deliberate copy of the pattern in
``okg.bench.injection.prf`` — the synth core must not
import substrate code (independence rule), so it carries its own ~40 lines
rather than the import.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Sequence, TypeVar

T = TypeVar("T")


class SynthPrf:
    """Keyed PRF namespaced by draw path; stateless between calls."""

    def __init__(self, seed: str) -> None:
        if not seed:
            raise ValueError("SynthPrf seed must be non-empty")
        self._key = hashlib.sha256(f"okg.bench.synth:{seed}".encode("utf-8")).digest()

    def _digest(self, *path: str) -> bytes:
        message = "\x1f".join(path).encode("utf-8")
        return hmac.new(self._key, message, hashlib.sha256).digest()

    def uniform(self, *path: str) -> float:
        """Deterministic float in [0, 1) for the draw path."""
        raw = int.from_bytes(self._digest(*path)[:8], "big")
        return raw / float(1 << 64)

    def randint(self, low: int, high: int, *path: str) -> int:
        """Deterministic integer in [low, high] inclusive."""
        if high < low:
            raise ValueError(f"randint bounds inverted: [{low}, {high}]")
        span = high - low + 1
        return low + int(self.uniform(*path) * span) % span

    def choice(self, items: Sequence[T], *path: str) -> T:
        if not items:
            raise ValueError(f"PRF choice from empty sequence at {'/'.join(path)}")
        return items[self.randint(0, len(items) - 1, *path)]

    def shuffled(self, items: Sequence[T], *path: str) -> list[T]:
        """Deterministic permutation (sort by per-item PRF key)."""
        return [
            item
            for _, item in sorted(
                ((self._digest(*path, str(index)), item) for index, item in enumerate(items)),
                key=lambda pair: pair[0],
            )
        ]

    def token(self, *path: str, length: int = 12) -> str:
        """Deterministic lowercase hex token (canaries, ids)."""
        return self._digest(*path).hex()[:length]
