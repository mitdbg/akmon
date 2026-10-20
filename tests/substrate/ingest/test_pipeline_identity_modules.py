from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest


def test_pipeline_identity_hashes_live_transform_modules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.ingest import runner as runner_module

    runner = object.__new__(runner_module.SourceRunner)
    runner.canonicalizers = ()
    runner.registry_binding_fingerprint = "registry-binding"
    runner.source_name = "pipeline-identity-test"
    runner._volatile_keys_cache = None
    runner._resolve_registered_pii_posture_if_available = lambda: "pii-posture"
    runner._load_volatile_fields = lambda: {}
    runner._active_nomos_policy_fingerprint = lambda: "nomos-policy"

    observed: list[object] = []

    def record_identity(module: object) -> str:
        observed.append(module)
        return str(getattr(module, "__name__", "unknown"))

    monkeypatch.setattr(runner_module, "module_code_identity", record_identity)
    monkeypatch.setattr(
        runner_module.PipelineIdentitySnapshot,
        "build",
        SimpleNamespace,
    )

    adapter = SimpleNamespace()
    runner._resolve_pipeline_identity_snapshot(adapter)

    expected_names = (
        "okg.substrate.ingest.runner",
        "okg.substrate.ingest.sanitize",
        "okg.substrate.ingest.pii_posture",
        "okg.substrate.ingest.pii_sanitize",
        "okg.substrate.nomos.pii_classification",
        "okg.substrate.nomos.read_masking",
        "okg.substrate.alias.pseudonymize",
        "okg.substrate.canonicalize.base",
        "okg.substrate.nomos.source_lineage",
        "okg.substrate.fact_equivalence",
    )
    assert observed == [sys.modules[name] for name in expected_names]
