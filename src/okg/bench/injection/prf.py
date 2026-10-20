"""Seeded keyed PRF — the injection framework's only randomness source.

All selection and parameter choice derive from HMAC-SHA256 over
``(seed, corpus tree digest)`` keyed by a caller-supplied tag: no global
RNG state, no wall clock (req.bench.determinism). Identical pinned inputs
therefore reproduce identical choices, and any two distinct tags are
independent draws.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Sequence, TypeVar

from okg.bench.injection.types import InjectionError

_T = TypeVar("_T")


class KeyedPRF:
    """Deterministic pseudo-random draws keyed by (seed, tree digest, tag)."""

    def __init__(self, seed: int, tree_digest: str) -> None:
        material = f"okg.injection.prf.v1:{seed}:{tree_digest}".encode("utf-8")
        self._key = hashlib.sha256(material).digest()

    def _digest(self, tag: str) -> bytes:
        return hmac.new(self._key, tag.encode("utf-8"), hashlib.sha256).digest()

    def uniform(self, tag: str) -> float:
        """Uniform float in [0, 1)."""
        return int.from_bytes(self._digest(tag)[:8], "big") / 2**64

    def randint(self, tag: str, lo: int, hi: int) -> int:
        """Uniform integer in [lo, hi] (inclusive)."""
        if hi < lo:
            raise InjectionError(f"randint bounds inverted: [{lo}, {hi}]")
        width = hi - lo + 1
        return lo + int.from_bytes(self._digest(tag)[:8], "big") % width

    def choice(self, tag: str, options: Sequence[_T]) -> _T:
        """Deterministic choice from a non-empty sequence."""
        if not options:
            raise InjectionError(f"choice from empty sequence (tag {tag!r})")
        return options[int.from_bytes(self._digest(tag)[:8], "big") % len(options)]
