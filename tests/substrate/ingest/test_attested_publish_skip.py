from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.ingest import service as ingest_service
from okg.substrate.ingest.service import IngestOptions


def test_attested_deployment_is_always_pending_before_database_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: SimpleNamespace(checkpoint_attestors=[object()]),
    )

    def unexpected_pool(_dsn: str):
        raise AssertionError("attested skip invalidation must not need a database probe")

    monkeypatch.setattr("okg.substrate.db.pool.get_pool", unexpected_pool)

    assert ingest_service._deployment_has_pending_publisher_work(
        IngestOptions(
            dsn="postgresql://unused",
            deployment="attested",
            deployment_dir=Path("/unused/attested"),
        )
    ) is True


def test_attested_no_work_layer_still_drives_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: True,
    )
    options = IngestOptions(
        dsn="postgresql://unused",
        deployment="attested",
        deployment_dir=Path("/unused/attested"),
    )
    zero_work = [{
        "nodes_appended": 0,
        "edges_appended": 0,
        "retract_n": 0,
        "retract_e": 0,
    }]

    assert ingest_service._should_skip_layer_publish(options, zero_work) is False
