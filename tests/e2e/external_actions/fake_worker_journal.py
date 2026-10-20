"""Append-only deterministic worker-I/O journal."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from okg.substrate.external_actions.signing import domain_digest


@dataclass
class FakeWorkerJournal:
    entries: list[dict[str, Any]] = field(default_factory=list)

    def append(self, phase: str, **fields: Any) -> str:
        record = {"phase": phase, **fields}
        digest = domain_digest("okg-fake-worker-journal-v1", record)
        self.entries.append({**record, "digest": digest})
        return digest

    @property
    def digest(self) -> str:
        return domain_digest("okg-fake-worker-journal-chain-v1", self.entries)

    def latest(self, phase: str) -> Mapping[str, Any] | None:
        return next((entry for entry in reversed(self.entries) if entry["phase"] == phase), None)

    def snapshot(self) -> dict[str, Any]:
        return {"entries": list(self.entries), "digest": self.digest}


__all__ = ["FakeWorkerJournal"]
