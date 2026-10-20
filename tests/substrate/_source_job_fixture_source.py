"""Fixture connectors for the source-job (one process per source) tests.

They are importable by module name, so a ``spawn`` source child loads them
through the registry exactly as it loads a production adapter. Each is
written against the public ``okg.deployment`` SDK.
"""
from __future__ import annotations

import os
import time

from okg.deployment import Checkpoint, ConnectorAdapter, ConnectorRun, NodeFact


class _DocConnector:
    """Emits ``count`` document facts; optionally sleeps first."""

    profile = "discovery_crawl"

    def __init__(self, *, prefix: str, count: int = 1, sleep_seconds: float = 0.0) -> None:
        self.name = f"source_job_fixture_{prefix}"
        self.prefix = prefix
        self.count = count
        self.sleep_seconds = sleep_seconds

    def run(self, ctx) -> ConnectorRun:
        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)
        facts = tuple(
            NodeFact(
                node_id=f"{self.prefix}:{index}",
                subtype="document",
                attrs={"text": f"{self.prefix} {index}"},
                source_record_id={"id": f"{self.prefix}:{index}"},
                source_revision={"content_hash": f"{self.prefix}-{index}"},
            )
            for index in range(self.count)
        )
        return ConnectorRun(
            facts=facts,
            completed_scope=False,
            next_checkpoint=Checkpoint(next_cursor={"prefix": self.prefix}),
            run_mode=ctx.mode,
        )


class DocFixtureSource(ConnectorAdapter):
    profile = _DocConnector.profile

    def __init__(self, *, prefix: str, count: int = 1, sleep_seconds: float = 0.0) -> None:
        super().__init__(
            _DocConnector(prefix=prefix, count=count, sleep_seconds=sleep_seconds)
        )


class _CrashConnector(_DocConnector):
    """Dies mid-run without unwinding, like a segfault or an OOM kill."""

    def __init__(self, *, prefix: str, exit_status: int = 9) -> None:
        super().__init__(prefix=prefix)
        self.exit_status = exit_status

    def run(self, ctx) -> ConnectorRun:
        os._exit(self.exit_status)


class CrashFixtureSource(ConnectorAdapter):
    profile = _DocConnector.profile

    def __init__(self, *, prefix: str, exit_status: int = 9) -> None:
        super().__init__(_CrashConnector(prefix=prefix, exit_status=exit_status))


class _MemoryHogConnector(_DocConnector):
    """Holds ``megabytes`` of touched memory for ``hold_seconds``."""

    def __init__(self, *, prefix: str, megabytes: int, hold_seconds: float) -> None:
        super().__init__(prefix=prefix)
        self.megabytes = megabytes
        self.hold_seconds = hold_seconds

    def run(self, ctx) -> ConnectorRun:
        block = bytearray(self.megabytes * 1024 * 1024)
        for offset in range(0, len(block), 4096):
            block[offset] = 1
        time.sleep(self.hold_seconds)
        del block
        return super().run(ctx)


class MemoryHogFixtureSource(ConnectorAdapter):
    profile = _DocConnector.profile

    def __init__(self, *, prefix: str, megabytes: int, hold_seconds: float) -> None:
        super().__init__(
            _MemoryHogConnector(
                prefix=prefix, megabytes=megabytes, hold_seconds=hold_seconds
            )
        )
