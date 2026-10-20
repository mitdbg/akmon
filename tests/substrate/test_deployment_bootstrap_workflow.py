from __future__ import annotations

from contextlib import nullcontext
import sys
import types
from pathlib import Path

import psycopg
import pytest

import okg.substrate.catalog.modules_apply as modules_apply
import okg.substrate.catalog.ownership as ownership
import okg.substrate.graph_store as graph_store
import okg.substrate.ingest as ingest
import okg.substrate.ingest.pii_posture as pii_posture
import okg.substrate.nomos.rollout as nomos_rollout
import okg.substrate.publish_proof as publish_proof
from okg.substrate.deployment_bootstrap import workflow


def test_completed_scope_deletion_bootstrap_threads_policy_and_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "deployment.yaml").write_text(
        "name: bootstrap-proof\n",
        encoding="utf-8",
    )
    (staged / "source_registry.bootstrap.yaml").write_text(
        """
sources:
  crawl:
    module: completed_scope_bootstrap_source
    class: CompletedScopeSource
    ownership_id: bootstrap-proof.crawl
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [path]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
""".lstrip(),
        encoding="utf-8",
    )
    source_module = types.ModuleType("completed_scope_bootstrap_source")

    class CompletedScopeSource:
        profile = "discovery_crawl"

    source_module.CompletedScopeSource = CompletedScopeSource
    monkeypatch.setitem(
        sys.modules,
        "completed_scope_bootstrap_source",
        source_module,
    )
    captured: dict[str, object] = {}

    class CapturingRunner:
        def __init__(self, _dsn: str, **kwargs: object) -> None:
            captured.update(kwargs)

        def run(self, _adapter: object, **_kwargs: object) -> object:
            return types.SimpleNamespace(as_dict=lambda: {"status": "OK"})

    class FakeGraph:
        def __init__(self, count: int) -> None:
            self.count = count

        def query(self, _sql: str) -> list[dict[str, int]]:
            return [{"n": self.count}]

    class FakeStore:
        calls = 0

        @classmethod
        def connect(cls, *_args: object, **_kwargs: object) -> "FakeStore":
            cls.calls += 1
            return cls()

        def checkout(self) -> FakeGraph:
            return FakeGraph(0)

        def checkout_generation(self, _generation_id: str) -> FakeGraph:
            return FakeGraph(1)

        def close(self) -> None:
            return None

    class FakePublisher:
        def __init__(
            self,
            _dsn: str,
            _deployment: str,
            **_publisher_kwargs: object,
        ) -> None:
            pass

        def cycle(self) -> dict[str, object]:
            return {
                "status": "published",
                "generation_id": "gen:completed-scope-proof",
                "catalog_version_id": 1,
            }

    monkeypatch.setattr(workflow, "_doctor_connectivity_check", lambda _dsn: {})
    monkeypatch.setattr(graph_store, "ChronosGraphStore", FakeStore)
    monkeypatch.setattr(ingest, "SourceRunner", CapturingRunner)
    monkeypatch.setattr(
        "okg.substrate.publisher.factory.build_deployment_publisher",
        lambda **_kwargs: FakePublisher("postgres://unused", "bootstrap-proof"),
    )
    catalog_identity = object()
    monkeypatch.setattr(
        ownership,
        "catalog_identity_for",
        lambda **_kwargs: catalog_identity,
    )
    monkeypatch.setattr(
        ownership,
        "ensure_catalog_write_allowed",
        lambda _conn, *, identity: (
            None if identity is catalog_identity else pytest.fail("wrong identity")
        ),
    )
    monkeypatch.setattr(
        modules_apply,
        "apply_composed_catalog",
        lambda _conn, _composed, *, identity: (
            1 if identity is catalog_identity else pytest.fail("wrong identity")
        ),
    )
    monkeypatch.setattr(
        pii_posture,
        "resolve_deployment_source_pii_posture",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        nomos_rollout,
        "runtime_enforce_mode_dsn_keys",
        lambda _path: set(),
    )
    monkeypatch.setattr(
        publish_proof,
        "dlq_count",
        lambda _dsn: (0, 0),
    )
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: nullcontext(object()),
    )
    # #3003: the proof's source runs and publish hold the deployment's
    # source-sync signal; stub the guard and record that it was taken.
    guarded: list[dict[str, object]] = []

    def record_guard(**kwargs: object) -> object:
        guarded.append(kwargs)
        return nullcontext()

    monkeypatch.setattr(
        "okg.substrate.runtime.source_sync_priority.ingest_run_guard",
        record_guard,
    )

    proof = workflow._run_real_publish_proof(
        dsn="postgres://unused",
        deployment_name="bootstrap-proof",
        staged=staged,
        composed=object(),
    )

    assert proof["generation_id"] == "gen:completed-scope-proof"
    assert [(g["deployment"], g["hold"]) for g in guarded] == [
        ("bootstrap-proof", True),
    ]
    assert captured["deletion_semantics"] == "missing_from_completed_scope"
    assert captured["registry_binding_fingerprint"]
