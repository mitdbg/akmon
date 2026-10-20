"""Seed the blocked incomplete checkpoint okg cut before issue #3342.

Before #3342 a partial ingest failure froze a blocked ``repair_required``
checkpoint. Now it publishes an incomplete generation, and the freeze is cut
only when a failed source's undo did not finish. Tests of the recovery path
(the repair contract, promotion, supersede, and the deployments upgraded
with such a checkpoint still on disk) need that checkpoint. The freeze path
is unchanged production code; forcing the publish decision off reproduces
exactly what the old rule wrote.
"""
from __future__ import annotations

import pytest

from okg.substrate.ingest import published_completeness


def freeze_partial_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``run_ingest`` freeze a partial failure the pre-#3342 way."""
    monkeypatch.setattr(
        published_completeness,
        "partial_failure_may_publish",
        lambda _results: False,
    )
