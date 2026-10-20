"""Out-of-band worker registry used by the restore proof."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FakeWorkerControlPlane:
    active_workers: set[str] = field(default_factory=set)
    fenced_workers: set[str] = field(default_factory=set)

    def register(self, worker_id: str) -> None:
        if worker_id in self.fenced_workers:
            raise RuntimeError("fenced worker cannot resume")
        self.active_workers.add(worker_id)

    def fence_epoch(self, worker_ids: set[str]) -> None:
        self.fenced_workers.update(worker_ids)
        self.active_workers.difference_update(worker_ids)

    def can_resume(self, worker_id: str) -> bool:
        return worker_id in self.active_workers and worker_id not in self.fenced_workers


__all__ = ["FakeWorkerControlPlane"]
