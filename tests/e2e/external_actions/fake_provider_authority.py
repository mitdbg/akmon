"""Out-of-band provider grant registry used by the restore proof."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FakeProviderAuthority:
    active_grants: set[str] = field(default_factory=set)
    fenced_grants: set[str] = field(default_factory=set)

    def grant(self, grant_id: str) -> None:
        if grant_id in self.fenced_grants:
            raise RuntimeError("fenced provider grant cannot resume")
        self.active_grants.add(grant_id)

    def fence_epoch(self, grant_ids: set[str]) -> None:
        self.fenced_grants.update(grant_ids)
        self.active_grants.difference_update(grant_ids)

    def can_use(self, grant_id: str) -> bool:
        return grant_id in self.active_grants and grant_id not in self.fenced_grants


__all__ = ["FakeProviderAuthority"]
