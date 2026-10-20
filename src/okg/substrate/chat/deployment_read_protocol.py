"""Typed discover/inspect/read/poll protocol over deployment lifecycle state."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from okg.substrate.contracts.deployment_product.projections import (
    DeploymentReadStatus,
    ReadbackProjectionAuthority,
    load_deployment_read_status,
)


@dataclass(frozen=True, slots=True)
class ReadLoopEvent:
    step: Literal["discover", "inspect", "read", "poll"]
    operation_id: str
    plan_digest: str
    schema_version: str


@dataclass(frozen=True, slots=True)
class ReadLoopRecord:
    events: tuple[ReadLoopEvent, ...]

    def to_json(self) -> dict[str, Any]:
        return {"events": [asdict(event) for event in self.events]}


class DeploymentReadProtocol:
    """Reconnect to one existing operation without prompting or inventing state."""

    def __init__(
        self,
        state_file: Path,
        *,
        authority: ReadbackProjectionAuthority | None = None,
    ) -> None:
        self._state_file = state_file
        self._authority = authority

    def discover(self) -> DeploymentReadStatus:
        return self._status()

    def inspect(self) -> DeploymentReadStatus:
        return self._status()

    def read(self) -> DeploymentReadStatus:
        return self._status()

    def poll(self) -> DeploymentReadStatus:
        return self._status()

    def _status(self) -> DeploymentReadStatus:
        return load_deployment_read_status(
            self._state_file,
            authority=self._authority,
        )

    def self_play(self) -> ReadLoopRecord:
        events = []
        expected_identity: tuple[str, str] | None = None
        for step in ("discover", "inspect", "read", "poll"):
            status = getattr(self, step)()
            identity = (status.operation_id, status.plan_digest)
            if expected_identity is None:
                expected_identity = identity
            elif identity != expected_identity:
                raise ValueError("read protocol changed operation or plan identity")
            events.append(
                ReadLoopEvent(
                    step=step,
                    operation_id=status.operation_id,
                    plan_digest=status.plan_digest,
                    schema_version=status.schema_version,
                )
            )
        return ReadLoopRecord(events=tuple(events))


__all__ = ["DeploymentReadProtocol", "ReadLoopEvent", "ReadLoopRecord"]
