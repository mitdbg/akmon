"""Deterministic out-of-band epoch authority for restore scenarios."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib


@dataclass
class FakeEpochAuthority:
    epoch: int = 17
    available: bool = True

    def rotate(self) -> int:
        if not self.available:
            raise RuntimeError("epoch authority unavailable")
        self.epoch += 1
        return self.epoch

    def lease_digest(self) -> str:
        return "sha256:" + hashlib.sha256(f"epoch:{self.epoch}".encode()).hexdigest()


__all__ = ["FakeEpochAuthority"]
