"""deployment-publish-proof: provision fails closed on unparseable catalog output.

Hermetic (no Postgres): exercises ``_apply_catalog`` directly. A zero-exit
``catalog load --apply`` whose JSON does not parse (or which emits nothing)
must NOT be coerced to an empty ``{}`` payload — an empty payload silently
yields a null ``catalog_version_id``, which provision would then treat as a
(failed) success. The step must refuse loudly instead.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from okg.substrate.cli import provision as provision_mod
from okg.substrate.cli import catalog as catalog_mod
from okg.substrate.errors import SubstrateError


@pytest.fixture(autouse=True)
def _no_source_sync_signal_without_a_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3003: provision holds the source-sync signal on the database across
    its source runs and final publish; these tests stub the database."""
    from contextlib import nullcontext

    monkeypatch.setattr(
        "okg.substrate.runtime.source_sync_priority.ingest_run_guard",
        lambda **_kwargs: nullcontext(),
    )


def _patch_catalog_load(monkeypatch, *, stdout: str, rc: int = 0) -> None:
    """Make the wrapped catalog-load handler print ``stdout`` and return ``rc``.

    ``_apply_catalog`` captures the handler's stdout, so the stub writes to the
    current (redirected) stdout to mimic the real handler.
    """
    provision_mod._load_main_helpers()

    def fake_cmd_catalog_load(ns, *, as_json: bool) -> int:
        print(stdout, end="")
        return rc

    monkeypatch.setattr(
        "okg.substrate.cli.catalog._cmd_catalog_load",
        fake_cmd_catalog_load,
    )


def test_apply_catalog_refuses_unparseable_zero_exit_output(monkeypatch) -> None:
    _patch_catalog_load(monkeypatch, stdout="not json at all {{{", rc=0)
    with pytest.raises(SubstrateError, match="unparseable"):
        provision_mod._apply_catalog("dep-dir", dsn="postgres://x/y")


def test_apply_catalog_refuses_empty_zero_exit_output(monkeypatch) -> None:
    _patch_catalog_load(monkeypatch, stdout="", rc=0)
    with pytest.raises(SubstrateError, match="no catalog payload"):
        provision_mod._apply_catalog("dep-dir", dsn="postgres://x/y")


def test_apply_catalog_refuses_non_object_zero_exit_output(monkeypatch) -> None:
    # Valid JSON, but not an object → no catalog_version_id can be read.
    _patch_catalog_load(monkeypatch, stdout='["a", "list"]', rc=0)
    with pytest.raises(SubstrateError, match="no catalog payload"):
        provision_mod._apply_catalog("dep-dir", dsn="postgres://x/y")


def test_apply_catalog_returns_parsed_object_on_clean_output(monkeypatch) -> None:
    _patch_catalog_load(
        monkeypatch,
        stdout='{"catalog_version_id": 7, "subtype_count": 12}',
        rc=0,
    )
    out = provision_mod._apply_catalog("dep-dir", dsn="postgres://x/y")
    assert out == {"catalog_version_id": 7, "subtype_count": 12}


def test_ingest_timing_evidence_retains_existing_source_and_stage_measurements() -> None:
    report = {
        "timing_status": "available",
        "timings_ms": {
            "total": 1250.0,
            "source_wall": 900.0,
            "publish_wall": 300.0,
        },
        "layer_timings": [
            {
                "layer": 1,
                "sources": ["git-files"],
                "source_wall_ms": 900.0,
                "publish_wall_ms": 300.0,
            }
        ],
        "results": [
            {
                "name": "git-files",
                "status": "OK",
                "elapsed_s": 0.9,
                "timing_status": "available",
                "timings_ms": {
                    "adapter_run": 250.0,
                    "source_runner": 600.0,
                    "total": 900.0,
                },
                "source_stats": {"operator_private_path": "/not/retained"},
            }
        ],
    }

    assert provision_mod._ingest_timing_evidence(report) == {
        "timing_status": "available",
        "timings_ms": report["timings_ms"],
        "layer_timings": report["layer_timings"],
        "source_timings": [
            {
                "name": "git-files",
                "status": "OK",
                "stage": None,
                "elapsed_s": 0.9,
                "timing_status": "available",
                "timings_ms": report["results"][0]["timings_ms"],
            }
        ],
    }


def test_ingest_timing_evidence_includes_publish_once_wall_and_stage_timings() -> None:
    report = {
        "timing_status": "available",
        "timings_ms": {
            "total": 100.0,
            "source_wall": 80.0,
            "source_reported_total": 75.0,
            "publish_wall": 0.0,
            "publish_reported_total": 0.0,
            "orchestration_overhead": 20.0,
        },
        "layer_timings": [{
            "index": 1,
            "sources": ["pact"],
            "source_wall_ms": 80.0,
            "source_reported_total_ms": 75.0,
            "publish_wall_ms": 0.0,
            "publish_reported_ms": 0.0,
            "publish_status": "disabled",
        }],
        "results": [],
    }
    final = {
        "status": "published",
        "wall_ms": 55.0,
        "reported_ms": 50.0,
        "timings_ms": {"phase_a": 20.0, "phase_b": 30.0, "total": 50.0},
    }

    evidence = provision_mod._ingest_timing_evidence(
        report,
        final_publish_timing=final,
        pipeline_total_ms=160.0,
    )

    assert evidence["timings_ms"] == {
        "total": 160.0,
        "source_wall": 80.0,
        "source_reported_total": 75.0,
        "publish_wall": 55.0,
        "publish_reported_total": 50.0,
        "orchestration_overhead": 25.0,
    }
    assert evidence["layer_timings"][-1] == {
        "index": "final",
        "sources": [],
        "source_wall_ms": 0.0,
        "source_reported_total_ms": 0.0,
        "publish_wall_ms": 55.0,
        "publish_reported_ms": 50.0,
        "publish_status": "published",
        "publish_timings_ms": final["timings_ms"],
    }
    assert evidence["final_publish_timing"] == final


@pytest.mark.parametrize(
    "publish",
    [
        {"status": "published", "timings_ms": {"phase_a": 1.0, "total": 1.0}},
        {
            "status": "published",
            "total_duration_ms": float("nan"),
            "timings_ms": {"phase_a": 1.0, "total": 1.0},
        },
        {
            "status": "published",
            "total_duration_ms": 1.0,
            "timings_ms": {"total": 1.0},
        },
    ],
)
def test_final_publish_timing_evidence_refuses_missing_or_nonfinite_stages(
    publish: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="omitted finite total and stage"):
        provision_mod._final_publish_timing_evidence(publish, wall_ms=2.0)


class _QueryResult:
    def __init__(self, row) -> None:
        self._row = row

    def fetchone(self):
        return self._row


class _CatalogClaimConnection:
    def __init__(self, *, generation_count: int) -> None:
        self.generation_count = generation_count

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, statement: str):
        if "database_owner" in statement:
            return _QueryResult(("fixture-staging",))
        if "graph_generations" in statement:
            return _QueryResult((self.generation_count,))
        raise AssertionError(f"unexpected query: {statement}")


def _patch_sealed_clone_claim(
    tmp_path,
    monkeypatch,
    *,
    generation_count: int,
) -> list[dict]:
    provision_mod._load_main_helpers()
    state_root = tmp_path / "state"
    state_root.mkdir()
    rebind = state_root / "clone-rebind.json"
    rebind.write_text(
        json.dumps({
            "schema": "okg.environment_clone_rebind/v1",
            "ok": True,
            "apply": True,
            "ordinary_sessions_read_only": False,
            "source_backup_digest": "sha256:backup",
            "target": {
                "environment_id": "fixture-staging",
                "graph_deployment_name": "fixture-staging",
                "database": "fixture_database",
            },
        }),
        encoding="utf-8",
    )
    rebind.chmod(0o600)
    environment = SimpleNamespace(
        id="fixture-staging",
        graph_deployment_name="fixture-staging",
        tier="staging",
        readiness=SimpleNamespace(requires_clone_rebind=True),
        runtime=SimpleNamespace(state_root=state_root),
        database=SimpleNamespace(name="fixture_database"),
    )
    claims: list[dict] = []
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(tmp_path / "env.yaml"))
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(tmp_path / "release"))
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: environment,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(deployment_name="fixture-staging"),
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda _manifest: SimpleNamespace(deployment_name="fixture-staging"),
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.catalog_identity_for",
        lambda **_kwargs: SimpleNamespace(
            deployment_name="fixture-staging",
            writer_fingerprint="sha256:fixture-writer",
        ),
    )

    def _claim(_conn, *, identity, force):
        claims.append({"identity": identity, "force": force})
        return {
            "ok": False,
            "status": "latest_incompatible",
            "deployment_name": "fixture-staging",
            "owner": {
                "owner_fingerprint": identity.writer_fingerprint,
            },
        }

    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.claim_catalog_ownership",
        _claim,
    )
    monkeypatch.setattr(
        "psycopg.connect",
        lambda *_args, **_kwargs: _CatalogClaimConnection(
            generation_count=generation_count,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.generation_bootstrap.formal_bootstrap_generation_inventory",
        lambda _conn: {
            "ok": True,
            "canonical": True,
            "generation_zero_count": 1,
            "non_bootstrap_count": generation_count,
        },
    )
    return claims


def test_sealed_rebound_staging_claims_catalog_before_load(
    tmp_path,
    monkeypatch,
) -> None:
    claims = _patch_sealed_clone_claim(
        tmp_path,
        monkeypatch,
        generation_count=0,
    )

    result = provision_mod._claim_rebound_staging_catalog_ownership(
        "fixture-staging",
        deployment_dir=tmp_path / "deployment",
        dsn="postgres://fixture/staging",
    )

    assert result == {
        "step": "catalog_ownership",
        "status": "claimed_pending_catalog_load",
        "deployment_name": "fixture-staging",
        "source_backup_digest": "sha256:backup",
    }
    assert len(claims) == 1
    assert claims[0]["force"] is True


def test_rebound_catalog_claim_is_unavailable_outside_sealed_launcher(
    tmp_path,
    monkeypatch,
) -> None:
    provision_mod._load_main_helpers()
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    monkeypatch.delenv("OKG_RELEASE_ROOT", raising=False)

    assert provision_mod._claim_rebound_staging_catalog_ownership(
        "fixture-staging",
        deployment_dir=tmp_path / "deployment",
        dsn="postgres://fixture/staging",
    ) is None


def test_sealed_rebound_staging_refuses_catalog_claim_after_generation(
    tmp_path,
    monkeypatch,
) -> None:
    claims = _patch_sealed_clone_claim(
        tmp_path,
        monkeypatch,
        generation_count=1,
    )

    with pytest.raises(
        SubstrateError,
        match="requires zero non-bootstrap generations",
    ):
        provision_mod._claim_rebound_staging_catalog_ownership(
            "fixture-staging",
            deployment_dir=tmp_path / "deployment",
            dsn="postgres://fixture/staging",
        )

    assert claims == []


@pytest.mark.parametrize("as_json", [True, False])
def test_catalog_load_reports_stdlib_exception_without_masking_it(
    monkeypatch,
    capsys,
    as_json: bool,
) -> None:
    def missing_catalog(_args):
        raise FileNotFoundError("missing ontology.yaml")

    # `_cmd_catalog_load` resolves through `_resolve_catalog_target`,
    # which also accepts an environment id (#1303).
    monkeypatch.setattr(catalog_mod, "_resolve_catalog_target", missing_catalog)

    rc = catalog_mod._cmd_catalog_load(
        type("Args", (), {"deployment": "missing"})(),
        as_json=as_json,
    )

    assert rc == 1
    stderr = capsys.readouterr().err
    assert "missing ontology.yaml" in stderr
    assert "Traceback" not in stderr
    if as_json:
        payload = json.loads(stderr)
        assert payload["error"]["code"] == "substrate_error"
        assert payload["error"]["message"] == "missing ontology.yaml"
        assert payload["error"]["data"]["exception_type"] == "FileNotFoundError"
    else:
        assert "error [substrate_error]" in stderr
        assert "FileNotFoundError" in stderr


def test_generic_error_normalization_preserves_typed_extension_behavior(
    capsys,
) -> None:
    err = SubstrateError("vector missing")
    err.code = "extension_missing"

    assert provision_mod._emit_error(err, as_json=True) == 2
    assert json.loads(capsys.readouterr().err)["error"]["code"] == (
        "extension_missing"
    )

def _patch_dlq_clean(monkeypatch) -> None:
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn: (0, {}),
    )


def _patch_observed_generation(
    monkeypatch, *, generation_id, node_count, edge_count
) -> None:
    """Stub the observed already-current generation's live row counts."""
    monkeypatch.setattr(
        provision_mod,
        "_published_generation_row_counts",
        lambda dsn, *, deployment_name: (generation_id, node_count, edge_count),
    )


def _patch_exact_generation(
    monkeypatch, *, generation_id, node_count, edge_count
) -> None:
    """Stub one generation-bound predecessor lookup."""
    expected_generation_id = generation_id

    def exact_counts(dsn, *, generation_id, deployment_name):
        assert generation_id == expected_generation_id
        return (generation_id, node_count, edge_count)

    monkeypatch.setattr(
        provision_mod,
        "_generation_row_counts",
        exact_counts,
    )


# Issue #1291: a healthy publish receipt also proves capacity — the measured
# advisory-lock hold must clear the configured headroom against the hard
# statement timeout. Payloads standing for a HEALTHY publish carry one.
_HEALTHY_LOCK_TIMING_DETAILS = {
    "schema": "okg.publisher.timing_details.v1",
    "lock": {
        "coordination_mode": "dbos",
        "acquire_ms": 4.0,
        "wait_ms": 4.0,
        "hold_ms": 12_000.0,
        "hard_timeout_ms": 900_000,
    },
}


def test_provision_publish_proof_skips_row_delta_for_explicit_noop(
    monkeypatch,
) -> None:
    """A publisher no-op (explicit ``noop: True``) skips the positive-row-delta
    assertion but still verifies the DLQ is bounded."""
    _patch_dlq_clean(monkeypatch)
    # No-op publish (unchanged repo dedups): published but nodes_added == 0 with
    # the publisher's explicit no-op signal set.
    noop = {
        "status": "published",
        "generation_id": 5,
        "nodes_added": 0,
        "edges_added": 0,
        "noop": True,
        "timing_details": _HEALTHY_LOCK_TIMING_DETAILS,
    }
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", noop, deployment_name="okg-workspace"
    )
    assert proof["ok"] is True


def test_provision_publish_proof_fails_when_dbos_queue_state_is_unavailable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn: (0, {"runtime_unavailable": 1}),
    )
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y",
        {
            "status": "published",
            "generation_id": 5,
            "nodes_added": 0,
            "edges_added": 0,
            "noop": True,
        },
        deployment_name="okg-workspace",
    )

    assert proof["ok"] is False
    assert "could not establish healthy DBOS queue state" in proof["reason"]


def test_provision_publish_proof_no_publish_verifies_current_generation_rows(
    monkeypatch,
) -> None:
    """The no-publish fallback (idempotent re-provision) must observe the
    already-current generation's live row counts, not synthesize a published
    status. A populated current generation passes."""
    _patch_dlq_clean(monkeypatch)
    _patch_observed_generation(
        monkeypatch, generation_id="gen-current", node_count=12, edge_count=7
    )
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", {}, deployment_name="okg-workspace"
    )
    assert proof["ok"] is True
    assert proof["generation_id"] == "gen-current"
    # Issue #1291: no publish ran, so the lock-hold capacity bar does not
    # apply -- and the exemption is stated by this path, not inferred from a
    # receipt that happens to lack a lock section.
    assert proof["lock_headroom"]["status"] == "not_applicable"


def test_provision_publish_proof_no_publish_fails_when_no_generation(
    monkeypatch,
) -> None:
    """No publish this run AND no published generation in the DB → fail loud,
    not a synthesized empty-safe pass."""
    _patch_dlq_clean(monkeypatch)
    _patch_observed_generation(
        monkeypatch, generation_id=None, node_count=None, edge_count=None
    )
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", {}, deployment_name="okg-workspace"
    )
    assert proof["ok"] is False
    assert "no published generation" in proof["reason"]


def test_provision_publish_proof_no_publish_fails_on_empty_current_generation(
    monkeypatch,
) -> None:
    """An already-current generation that holds NO rows must fail the gate — the
    graph is not actually populated, which the old synthesized status masked."""
    _patch_dlq_clean(monkeypatch)
    _patch_observed_generation(
        monkeypatch, generation_id="gen-empty", node_count=0, edge_count=0
    )
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", {}, deployment_name="okg-workspace"
    )
    assert proof["ok"] is False
    assert "no live node/edge rows" in proof["reason"]


def test_provision_publish_proof_fails_blocked_publish(monkeypatch) -> None:
    _patch_dlq_clean(monkeypatch)
    blocked = {"status": "blocked", "generation_id": 9}
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", blocked, deployment_name="okg-workspace"
    )
    assert proof["ok"] is False
    assert "was not published" in proof["reason"]


def test_provision_publish_proof_requires_row_delta_on_fresh_publish(
    monkeypatch,
) -> None:
    _patch_dlq_clean(monkeypatch)
    # Fresh published generation that added rows → ok.
    fresh = {
        "status": "published",
        "generation_id": 11,
        "nodes_added": 4,
        "edges_added": 3,
        "timing_details": _HEALTHY_LOCK_TIMING_DETAILS,
    }
    assert provision_mod._provision_publish_proof(
        "postgres://x/y", fresh, deployment_name="okg-workspace"
    )["ok"]


def test_provision_publish_proof_accepts_one_sided_delta_on_populated_replay(
    monkeypatch,
) -> None:
    _patch_dlq_clean(monkeypatch)
    replay = {
        "status": "published",
        "generation_id": "gen-next",
        "predecessor_generation_id": "gen-prior",
        "nodes_added": 0,
        "edges_added": 4,
        "timing_details": _HEALTHY_LOCK_TIMING_DETAILS,
    }
    _patch_exact_generation(
        monkeypatch, generation_id="gen-prior", node_count=12, edge_count=7
    )

    proof = provision_mod._provision_publish_proof(
        "postgres://x/y",
        replay,
        deployment_name="okg-ci",
    )

    assert proof["ok"] is True
    assert proof["predecessor_generation_id"] == "gen-prior"
    assert proof["predecessor_nodes_live"] == 12
    assert proof["predecessor_edges_live"] == 7


def test_provision_publish_proof_refuses_one_sided_delta_without_populated_prior(
    monkeypatch,
) -> None:
    _patch_dlq_clean(monkeypatch)
    replay = {
        "status": "published",
        "generation_id": "gen-next",
        "nodes_added": 0,
        "edges_added": 4,
    }

    no_prior = provision_mod._provision_publish_proof(
        "postgres://x/y",
        replay,
        deployment_name="okg-ci",
    )
    replay["predecessor_generation_id"] = "gen-empty"
    _patch_exact_generation(
        monkeypatch, generation_id="gen-empty", node_count=0, edge_count=0
    )
    empty_predecessor = provision_mod._provision_publish_proof(
        "postgres://x/y",
        replay,
        deployment_name="okg-ci",
    )

    assert no_prior["ok"] is False
    assert empty_predecessor["ok"] is False
    assert "did not add live node and edge rows" in no_prior["reason"]


def _patch_generations(monkeypatch, counts: dict) -> list:
    """Stub generation-bound row counts; unknown ids read as absent."""
    observed: list = []

    def exact_counts(dsn, *, generation_id, deployment_name):
        observed.append(generation_id)
        if generation_id not in counts:
            return (None, None, None)
        return (generation_id, *counts[generation_id])

    monkeypatch.setattr(provision_mod, "_generation_row_counts", exact_counts)
    return observed


def _zero_delta_replay() -> dict:
    return {
        "status": "published",
        "generation_id": "gen-next",
        "predecessor_generation_id": "gen-prior",
        "nodes_added": 0,
        "edges_added": 0,
        "timing_details": _HEALTHY_LOCK_TIMING_DETAILS,
    }


def test_provision_publish_proof_accepts_observed_zero_delta_redeploy(
    monkeypatch,
) -> None:
    """#2490: re-provisioning a deployment that already has data can publish
    a version that adds no rows. It passes when the previous version and
    the new version are both read back with rows, and says so."""
    _patch_dlq_clean(monkeypatch)
    observed = _patch_generations(
        monkeypatch, {"gen-prior": (12, 7), "gen-next": (12, 7)},
    )

    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", _zero_delta_replay(), deployment_name="okg-ci",
    )

    assert proof["ok"] is True, proof
    assert proof["no_rows_added"] is True
    assert "added no rows" in proof["reason"]
    assert "catalog" not in str(proof.get("fix_hint") or "")
    assert set(observed) == {"gen-prior", "gen-next"}


@pytest.mark.parametrize(
    "counts",
    [
        pytest.param({"gen-next": (12, 7)}, id="predecessor-not-found"),
        pytest.param(
            {"gen-prior": (0, 0), "gen-next": (12, 7)}, id="empty-predecessor",
        ),
        pytest.param(
            {"gen-prior": (12, 7), "gen-next": (0, 0)}, id="empty-new-version",
        ),
        pytest.param({"gen-prior": (12, 7)}, id="new-version-not-found"),
    ],
)
def test_provision_publish_proof_refuses_unobserved_zero_delta(
    monkeypatch, counts,
) -> None:
    """The zero-delta acceptance needs both versions observed with rows."""
    _patch_dlq_clean(monkeypatch)
    _patch_generations(monkeypatch, counts)

    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", _zero_delta_replay(), deployment_name="okg-ci",
    )

    assert proof["ok"] is False
    assert "did not add live node and edge rows" in proof["reason"]


def test_provision_publish_proof_refuses_zero_delta_without_predecessor(
    monkeypatch,
) -> None:
    """A first install that published nothing stays refused."""
    _patch_dlq_clean(monkeypatch)
    observed = _patch_generations(monkeypatch, {"gen-next": (12, 7)})
    replay = _zero_delta_replay()
    replay.pop("predecessor_generation_id")

    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", replay, deployment_name="okg-ci",
    )

    assert proof["ok"] is False
    assert "did not add live node and edge rows" in proof["reason"]
    assert observed == []


@pytest.mark.parametrize(
    ("nodes_added", "edges_added"),
    [
        (-1, 4),
        (4, -1),
    ],
)
def test_provision_publish_proof_refuses_negative_delta_on_populated_replay(
    monkeypatch,
    nodes_added: int,
    edges_added: int,
) -> None:
    _patch_dlq_clean(monkeypatch)
    replay = {
        "status": "published",
        "generation_id": "gen-next",
        "predecessor_generation_id": "gen-prior",
        "nodes_added": nodes_added,
        "edges_added": edges_added,
    }
    _patch_exact_generation(
        monkeypatch, generation_id="gen-prior", node_count=12, edge_count=7
    )

    proof = provision_mod._provision_publish_proof(
        "postgres://x/y",
        replay,
        deployment_name="okg-ci",
    )

    assert proof["ok"] is False
    assert "could not read publish row delta" in proof["reason"]


@pytest.mark.parametrize(
    ("nodes_added", "edges_added"),
    [
        (0.5, 4),
        (0, 4.2),
        (True, 0),
        (0, False),
        ("++0", 4),
        ("²", 4),
    ],
)
def test_provision_publish_proof_refuses_non_integer_delta_on_populated_replay(
    monkeypatch,
    nodes_added,
    edges_added,
) -> None:
    _patch_dlq_clean(monkeypatch)
    _patch_exact_generation(
        monkeypatch, generation_id="gen-prior", node_count=12, edge_count=7
    )
    replay = {
        "status": "published",
        "generation_id": "gen-next",
        "predecessor_generation_id": "gen-prior",
        "nodes_added": nodes_added,
        "edges_added": edges_added,
    }

    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", replay, deployment_name="okg-ci"
    )

    assert proof["ok"] is False
    assert "could not read publish row delta" in proof["reason"]


def test_provision_publish_proof_uses_recorded_predecessor_not_latest(
    monkeypatch,
) -> None:
    """A concurrent head advance cannot change predecessor evidence."""
    _patch_dlq_clean(monkeypatch)
    observed: list[str | int] = []

    def exact_counts(dsn, *, generation_id, deployment_name):
        observed.append(generation_id)
        return (generation_id, 12, 7)

    monkeypatch.setattr(
        provision_mod,
        "_generation_row_counts",
        exact_counts,
    )
    replay = {
        "status": "published",
        "generation_id": "gen-next",
        "predecessor_generation_id": "gen-recorded-predecessor",
        "nodes_added": 0,
        "edges_added": 4,
        "timing_details": _HEALTHY_LOCK_TIMING_DETAILS,
    }

    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", replay, deployment_name="okg-ci"
    )

    assert proof["ok"] is True
    assert observed == ["gen-recorded-predecessor"]


def test_provision_publish_proof_fails_fresh_zero_row_publish_without_noop(
    monkeypatch,
) -> None:
    """The adversarial-review finding: a freshly *published* payload with 0
    nodes / 0 edges and NO explicit ``noop`` signal must FAIL the gate. The old
    zero-delta clause wrongly exempted it; only the explicit no-op signal
    exempts the row-delta assertion now."""
    _patch_dlq_clean(monkeypatch)
    fresh_empty = {
        "status": "published",
        "generation_id": 13,
        "nodes_added": 0,
        "edges_added": 0,
        # NOTE: no "noop" key — this is a genuinely empty fresh publish.
    }
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", fresh_empty, deployment_name="okg-workspace"
    )
    assert proof["ok"] is False
    assert "did not add live node and edge rows" in proof["reason"]


# ---------------------------------------------------------------------------
# Packaged-install repeat: a published 0/0 addition delta over an already
# populated, already-current graph. Only the packaged installer supplies the
# replay context; every other caller keeps the strict refusal above.
# ---------------------------------------------------------------------------


def _authorized_row(name: str, **overrides) -> dict:
    row = {
        "name": name,
        "status": "OK",
        "admission_id": f"admission-{name}",
        "admission_status": "accepted",
        "admission_verdict": {
            "producer_policy": {
                "terminal_state_authorized": True,
                "reason": "authoritative_replay",
                "completed_scope": True,
            },
            # Content that must never be copied into a verdict or receipt.
            "unpublished_prior_admissions_sample": ["private-admission-id"],
        },
        "sync_scope": {"root": "/private/operator/data"},
        "source_stats": {"fetched_path": "/private/operator/data/file"},
    }
    row.update(overrides)
    return row


def _replay_publish(**overrides) -> dict:
    publish = {
        "status": "published",
        "generation_id": "gen-next",
        "predecessor_generation_id": "gen-prior",
        "nodes_added": 0,
        "edges_added": 0,
        "nodes_updated": 0,
        "nodes_retracted": 0,
        "edges_updated": 0,
        "edges_retracted": 0,
        "node_count": 12,
        "edge_count": 7,
        "timing_details": _HEALTHY_LOCK_TIMING_DETAILS,
    }
    publish.update(overrides)
    return publish


def _replay_context(results=None, ordered=None, predecessor="unset") -> dict:
    if predecessor == "unset":
        predecessor = {"generation_id": "gen-prior", "node_count": 12, "edge_count": 7}
    return {
        "ordered": ["alpha", "beta"] if ordered is None else ordered,
        "results": (
            [_authorized_row("alpha"), _authorized_row("beta")]
            if results is None
            else results
        ),
        "predecessor": predecessor,
    }


def _replay_proof(monkeypatch, publish=None, context="default", *, prior=(12, 7)):
    _patch_dlq_clean(monkeypatch)
    _patch_exact_generation(
        monkeypatch, generation_id="gen-prior", node_count=prior[0], edge_count=prior[1]
    )
    return provision_mod._provision_publish_proof(
        "postgres://x/y",
        _replay_publish() if publish is None else publish,
        deployment_name="s14-archi",
        replay_context=_replay_context() if context == "default" else context,
    )


def test_packaged_repeat_replay_passes_only_with_full_evidence(monkeypatch) -> None:
    proof = _replay_proof(monkeypatch)
    assert proof["ok"] is True
    assert proof["caught_up_replay"] is True
    assert proof["generation_id"] == "gen-next"
    assert proof["predecessor_generation_id"] == "gen-prior"
    assert (proof["predecessor_nodes_live"], proof["predecessor_edges_live"]) == (12, 7)
    assert (proof["node_count"], proof["edge_count"]) == (12, 7)
    assert proof["replay_sources"] == ["alpha", "beta"]
    assert (proof["nodes_updated"], proof["nodes_retracted"]) == (0, 0)
    assert (proof["edges_updated"], proof["edges_retracted"]) == (0, 0)
    # Names, ids and counts only: no ingest row content reaches the verdict.
    assert set(proof) == {
        "ok", "status", "generation_id", "nodes_added", "edges_added",
        "nodes_changed", "edges_changed", "caught_up_replay", "dlq_count",
        "queue_depth", "lock_headroom", "predecessor_generation_id",
        "predecessor_nodes_live", "predecessor_edges_live", "node_count",
        "edge_count", "nodes_updated", "nodes_retracted", "edges_updated",
        "edges_retracted", "replay_sources",
    }
    assert "private" not in json.dumps(proof)


def test_packaged_repeat_replay_is_unavailable_without_context(monkeypatch) -> None:
    """Without a replay context the same 0/0 payload is refused unless
    ordinary provision observes both versions with rows (#2490); here
    neither version is found."""
    _patch_dlq_clean(monkeypatch)
    _patch_generations(monkeypatch, {})
    proof = provision_mod._provision_publish_proof(
        "postgres://x/y", _replay_publish(), deployment_name="s14-archi"
    )
    assert proof["ok"] is False
    assert "did not add live node and edge rows" in proof["reason"]


@pytest.mark.parametrize(
    "damage, expected",
    [
        ("missing_verdict", "unproven: beta"),
        ("pending", "unproven: beta"),
        ("unauthorized", "unproven: beta"),
        ("failed", "unproven: beta"),
        ("absent", "unproven: beta"),
        ("bootstrap_shape", "unproven: beta"),
        ("malformed_ordered", "no well-formed requested source list"),
    ],
)
def test_packaged_repeat_replay_refuses_unproven_sources(monkeypatch, damage, expected) -> None:
    alpha = _authorized_row("alpha")
    if damage == "missing_verdict":
        rows = [alpha, _authorized_row("beta", admission_verdict={})]
    elif damage == "pending":
        rows = [alpha, _authorized_row("beta", admission_status="pending")]
    elif damage == "unauthorized":
        rows = [alpha, _authorized_row("beta", admission_verdict={
            "producer_policy": {"terminal_state_authorized": False, "reason": "partial"}})]
    elif damage == "failed":
        rows = [alpha, _authorized_row("beta", status="FAIL")]
    elif damage == "absent":
        rows = [alpha]
    elif damage == "bootstrap_shape":
        # Idempotent bootstrap replay rows carry the flag at the top level and a
        # "published" envelope status; that shape is deliberately unproven.
        rows = [alpha, _authorized_row("beta", admission_status="published",
                                       admission_verdict={"terminal_state_authorized": True})]
    else:
        rows = [alpha, _authorized_row("beta")]
    context = _replay_context(results=rows, ordered=["alpha", "beta", "alpha"]
                              if damage == "malformed_ordered" else None)
    proof = _replay_proof(monkeypatch, context=context)
    assert proof["ok"] is False
    assert proof["caught_up_replay"] is False
    assert "accepted authorized zero-output terminal state" in proof["reason"]
    assert expected in proof["reason"]
    assert (proof["nodes_updated"], proof["edges_retracted"]) == (0, 0)
    assert "private" not in json.dumps(proof)


def test_packaged_repeat_replay_refuses_predecessor_drift(monkeypatch) -> None:
    proof = _replay_proof(monkeypatch, publish=_replay_publish(predecessor_generation_id="gen-other"))
    assert proof["ok"] is False
    assert "not the generation observed before refresh" in proof["reason"]
    assert proof["observed_predecessor_generation_id"] == "gen-prior"

    proof = _replay_proof(monkeypatch, publish=_replay_publish(predecessor_generation_id=None))
    assert proof["ok"] is False
    assert "not the generation observed before refresh" in proof["reason"]


def test_packaged_repeat_replay_refuses_without_populated_predecessor(monkeypatch) -> None:
    no_observation = _replay_proof(monkeypatch, context=_replay_context(predecessor=None))
    assert no_observation["ok"] is False
    assert "no populated generation was observed" in no_observation["reason"]

    empty = _replay_proof(monkeypatch, prior=(0, 0))
    assert empty["ok"] is False
    assert "not a populated published generation" in empty["reason"]

    unreadable = _replay_proof(monkeypatch, prior=(None, None))
    assert unreadable["ok"] is False
    assert "not a populated published generation" in unreadable["reason"]


def test_packaged_repeat_replay_refuses_changed_live_counts(monkeypatch) -> None:
    grew = _replay_proof(monkeypatch, publish=_replay_publish(node_count=13))
    assert grew["ok"] is False
    assert "changed live counts" in grew["reason"]
    assert (grew["node_count"], grew["edge_count"]) == (13, 7)

    shrank = _replay_proof(monkeypatch, publish=_replay_publish(edge_count=6))
    assert shrank["ok"] is False
    assert "changed live counts" in shrank["reason"]

    missing = _replay_proof(monkeypatch, publish=_replay_publish(node_count=None))
    assert missing["ok"] is False
    assert "exact live node and edge counts" in missing["reason"]

    boolean = _replay_proof(monkeypatch, publish=_replay_publish(node_count=True, edge_count=True))
    assert boolean["ok"] is False
    assert "exact live node and edge counts" in boolean["reason"]


def test_packaged_repeat_replay_keeps_shared_gate_checks(monkeypatch) -> None:
    blocked = _replay_proof(monkeypatch, publish=_replay_publish(status="blocked"))
    assert blocked["ok"] is False

    _patch_exact_generation(monkeypatch, generation_id="gen-prior", node_count=12, edge_count=7)
    monkeypatch.setattr("okg.substrate.publish_proof.dlq_count", lambda dsn: (3, {"dead_letter": 3}))
    dead = provision_mod._provision_publish_proof(
        "postgres://x/y", _replay_publish(), deployment_name="s14-archi",
        replay_context=_replay_context(),
    )
    assert dead["ok"] is False
    assert "dead-letter" in dead["reason"]

    no_headroom = _replay_proof(monkeypatch, publish=_replay_publish(timing_details=None))
    assert no_headroom["ok"] is False


def test_packaged_repeat_replay_does_not_bypass_positive_delta_paths(monkeypatch) -> None:
    """A positive or one-sided delta never enters the replay arm even when a
    context is present: those keep their existing strict and predecessor rules."""
    positive = _replay_proof(monkeypatch, publish=_replay_publish(nodes_added=3, edges_added=2))
    assert positive["ok"] is True
    assert positive["caught_up_replay"] is False
    assert "replay_sources" not in positive

    one_sided = _replay_proof(monkeypatch, publish=_replay_publish(nodes_added=0, edges_added=4))
    assert one_sided["ok"] is True
    assert "replay_sources" not in one_sided


def test_source_admission_summaries_copy_names_enums_and_ids_only() -> None:
    rows = [
        _authorized_row("alpha"),
        {"name": "beta", "status": "FAIL", "admission_status": "rejected", "run_mode": "reconcile",
         "nodes_appended": 4, "edges_appended": 0, "retract_n": 1, "retract_e": [1],
         "admission_verdict": {"producer_policy": {"terminal_state_authorized": False,
                                                   "reason": {"nested": "mapping"}}}},
        "not-a-row",
        {"name": ["list"], "admission_verdict": "text"},
    ]
    summaries = provision_mod._source_admission_summaries(rows)
    assert summaries == [
        {"name": "alpha", "status": "OK", "run_mode": None, "admission_id": "admission-alpha",
         "admission_status": "accepted", "nodes_appended": None, "edges_appended": None,
         "nodes_retracted": None, "edges_retracted": None, "terminal_state_authorized": True,
         "zero_output_reason": "authoritative_replay", "completed_scope": True},
        {"name": "beta", "status": "FAIL", "run_mode": "reconcile", "admission_id": None,
         "admission_status": "rejected", "nodes_appended": 4, "edges_appended": 0,
         "nodes_retracted": 1, "edges_retracted": None, "terminal_state_authorized": False,
         "zero_output_reason": None, "completed_scope": None},
        {"name": None, "status": None, "run_mode": None, "admission_id": None,
         "admission_status": None, "nodes_appended": None, "edges_appended": None,
         "nodes_retracted": None, "edges_retracted": None,
         "terminal_state_authorized": None, "zero_output_reason": None, "completed_scope": None},
    ]
    assert "private" not in json.dumps(summaries)
    assert provision_mod._source_admission_summaries(None) == []


# ---- final publish failure keeps the publisher's reason ---------------------
#
# `_publish` reports a refusal as a dict (`status: fail`, `error`,
# `error_data`, `fix_hint`) instead of raising. Before
# `provision-publish-error`, provision asked that dict for timing evidence and
# reported only "final publish timing evidence is unavailable", dropping the
# publisher's reason (release authority, framework provenance, catalog
# ownership, ...).

_PUBLISHER_FAILURE = {
    "label": "final",
    "status": "fail",
    "code": "substrate_error",
    "error": "repository provenance inputs are not committed",
    "error_data": {
        "repository": "/tmp/okg",
        "paths": ["M pact/changes/x/pact.yaml", "?? stray.pem"],
    },
    "fix_hint": "commit or remove the listed paths, then restart the worker",
}


def test_final_publish_failure_carries_publisher_fields_verbatim() -> None:
    err = provision_mod._final_publish_failure("demo", dict(_PUBLISHER_FAILURE))

    assert isinstance(err, SubstrateError)
    assert "repository provenance inputs are not committed" in str(err)
    assert "status='fail'" in str(err)
    assert err.data["error"] == _PUBLISHER_FAILURE["error"]
    assert err.data["error_data"] == _PUBLISHER_FAILURE["error_data"]
    assert err.data["fix_hint"] == _PUBLISHER_FAILURE["fix_hint"]
    assert err.data["error_code"] == "substrate_error"
    assert err.data["publish_status"] == "fail"
    assert err.fix_hint == _PUBLISHER_FAILURE["fix_hint"]


def test_final_publish_failure_without_hint_points_at_error_fields() -> None:
    err = provision_mod._final_publish_failure(
        "demo", {"status": "fail", "generation_id": None},
        steps=[{"step": "catalog", "catalog_version_id": 3}],
    )

    assert "status='fail'" in str(err)
    assert err.data["error"] is None
    assert err.data["steps"] == [{"step": "catalog", "catalog_version_id": 3}]
    assert "error_data" in err.fix_hint


def _stub_provision_pipeline(monkeypatch, tmp_path, *, final_publish: dict) -> dict:
    """Stub every step of `_cmd_provision` before the final publish so the
    test drives the real command path with a publisher that refuses."""
    calls: dict = {"publish": []}
    deployment_dir = tmp_path / "deployments" / "demo"
    deployment_dir.mkdir(parents=True)

    provision_mod._load_main_helpers()
    monkeypatch.setattr(provision_mod, "_resolve_dsn", lambda _args: "postgres://x/y")
    monkeypatch.setattr(
        "okg.substrate.db.migrate.ensure_required_extensions",
        lambda _dsn: {"created": [], "already_present": []},
    )
    monkeypatch.setattr(
        "okg.substrate.db.migrate.migrate",
        lambda _dsn, deployment_name: SimpleNamespace(statements_executed=0),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _name: deployment_dir,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path", lambda _dir: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: SimpleNamespace(deployment_name="demo", sync_root=None),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.sync_root.require_resolved_sync_root",
        lambda _manifest, command: None,
    )
    monkeypatch.setattr(
        provision_mod, "_claim_rebound_staging_catalog_ownership",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        provision_mod, "_apply_catalog",
        lambda _dep, dsn: {"catalog_version_id": 3, "subtype_count": 1},
    )
    monkeypatch.setattr(provision_mod, "_seed_nomos_baseline", lambda *_a, **_k: None)
    monkeypatch.setattr(
        "okg.substrate.ingest.service.run_ingest",
        lambda _options: SimpleNamespace(
            any_failed=False, report={"publishes": [], "results": []},
        ),
    )

    def _fake_publish(options, label, *_a, **_k):
        calls["publish"].append((options.deployment, label))
        return dict(final_publish)

    monkeypatch.setattr("okg.substrate.ingest.service._publish", _fake_publish)
    return calls


def _error_payload(captured) -> dict:
    for stream in (captured.err, captured.out):
        for line in reversed(stream.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                payload = json.loads(line)
                if "error" in payload:
                    return payload["error"]
    raise AssertionError(f"no JSON error envelope; out={captured.out!r} err={captured.err!r}")


def test_provision_final_publish_failure_carries_publisher_error(
    monkeypatch, tmp_path, capsys,
) -> None:
    calls = _stub_provision_pipeline(
        monkeypatch, tmp_path, final_publish=_PUBLISHER_FAILURE,
    )
    args = SimpleNamespace(
        deployment="demo", include=["git_files"], readiness_sources=False,
        no_predecessors=False, source_workers=None, publish_once=True,
        dsn=None,
    )

    rc = provision_mod._cmd_provision(args, as_json=True)

    assert rc != 0
    assert calls["publish"] == [("demo", "final")]
    error = _error_payload(capsys.readouterr())
    assert "repository provenance inputs are not committed" in error["message"]
    assert "timing evidence" not in error["message"]
    assert error["data"]["error"] == _PUBLISHER_FAILURE["error"]
    assert error["data"]["error_data"] == _PUBLISHER_FAILURE["error_data"]
    assert error["data"]["fix_hint"] == _PUBLISHER_FAILURE["fix_hint"]
    assert error["fix_hint"] == _PUBLISHER_FAILURE["fix_hint"]
    assert [step["step"] for step in error["data"]["steps"]] == [
        "extensions", "migrate", "catalog",
    ]


def test_provision_blocked_final_publish_reaches_publish_proof(
    monkeypatch, tmp_path, capsys,
) -> None:
    """A cycle blocked by an invariant is a real generation with timings
    and a structured reason; it must keep reaching the publish proof
    instead of being flattened into the refusal error."""
    blocked = {
        "label": "final",
        "status": "blocked",
        "generation_id": "gen-blocked",
        "blocked_reason": {"reason": "cross_source_error_invariant_failed"},
        "total_duration_ms": 12.0,
        "timings_ms": {"total": 12.0, "phase_b": 4.0},
    }
    _stub_provision_pipeline(monkeypatch, tmp_path, final_publish=blocked)
    monkeypatch.setattr(
        "okg.substrate.db.management.bootstrap_preflight",
        lambda _dsn, deployment_name: {
            "ok": True, "database": "demo", "summary": {},
        },
    )
    proofs: list[dict] = []

    def _proof(_dsn, last_publish, *, deployment_name, source_failure_reasons):
        proofs.append(dict(last_publish))
        return {"ok": False, "reason": "publish generation gen-blocked was not published"}

    monkeypatch.setattr(provision_mod, "_provision_publish_proof", _proof)
    args = SimpleNamespace(
        deployment="demo", include=None, readiness_sources=False,
        no_predecessors=False, source_workers=None, publish_once=True,
        dsn=None,
    )

    rc = provision_mod._cmd_provision(args, as_json=True)

    assert rc != 0
    assert proofs and proofs[0]["status"] == "blocked"
    out = capsys.readouterr().out
    payload = json.loads(
        [line for line in out.splitlines() if line.strip().startswith("{")][-1],
    )
    assert payload["ok"] is False
    assert payload["generation_id"] == "gen-blocked"
    assert payload["publish_proof"]["reason"].endswith("was not published")


def test_provision_final_publish_success_still_requires_timing_evidence(
    monkeypatch, tmp_path, capsys,
) -> None:
    """A publish that reports `published` but omits its timings is still a
    refusal: the timing-evidence rule now applies only to real publishes."""
    _stub_provision_pipeline(
        monkeypatch, tmp_path,
        final_publish={"status": "published", "generation_id": "gen-1"},
    )
    args = SimpleNamespace(
        deployment="demo", include=None, readiness_sources=False,
        no_predecessors=False, source_workers=None, publish_once=True,
        dsn=None,
    )

    rc = provision_mod._cmd_provision(args, as_json=True)

    assert rc != 0
    error = _error_payload(capsys.readouterr())
    assert "timing evidence" in error["message"]


def test_provision_publish_once_holds_the_source_sync_signal_to_the_end(
    monkeypatch, tmp_path, capsys,
) -> None:
    """#3003: `--publish-once` publishes after `run_ingest` returns, so
    provision holds the deployment's source-sync signal itself, from the
    source runs through the final publish."""
    from contextlib import contextmanager

    calls = _stub_provision_pipeline(
        monkeypatch, tmp_path,
        final_publish={"status": "published", "generation_id": "gen-1"},
    )
    events: list[str] = []

    @contextmanager
    def guard(**kwargs):
        events.append(f"enter:{kwargs['deployment']}:{kwargs['hold']}")
        yield 1
        events.append("exit")

    monkeypatch.setattr(
        "okg.substrate.runtime.source_sync_priority.ingest_run_guard", guard,
    )

    def fake_run_ingest(_options):
        events.append("run_ingest")
        return SimpleNamespace(any_failed=False, report={"publishes": [], "results": []})

    def fake_publish(options, label, *_a, **_k):
        events.append(f"publish:{label}")
        calls["publish"].append((options.deployment, label))
        return {"status": "published", "generation_id": "gen-1"}

    monkeypatch.setattr("okg.substrate.ingest.service.run_ingest", fake_run_ingest)
    monkeypatch.setattr("okg.substrate.ingest.service._publish", fake_publish)
    args = SimpleNamespace(
        deployment="demo", include=None, readiness_sources=False,
        no_predecessors=False, source_workers=None, publish_once=True,
        dsn=None,
    )

    provision_mod._cmd_provision(args, as_json=True)
    capsys.readouterr()

    assert events[:4] == ["enter:demo:True", "run_ingest", "publish:final", "exit"]


# ---- #1889: a retry after a crash must publish the work the crash staged ----


@pytest.mark.parametrize(
    ("publish_once", "stage_only", "expected"),
    [(True, False, True), (False, False, False), (True, True, False)],
)
def test_provision_asks_ingest_to_republish_staged_work_only_when_publishing_once(
    monkeypatch, tmp_path, capsys, publish_once: bool, stage_only: bool,
    expected: bool,
) -> None:
    """`--publish-once` turns ingest's own publishes off; the guarded
    pre-source republish must still run, but never for a stage-only run."""
    _stub_provision_pipeline(
        monkeypatch, tmp_path, final_publish={"status": "fail"},
    )
    monkeypatch.setattr(
        "okg.substrate.distributions.packaged_release.current_verified_release",
        lambda _dir: object(),
    )
    seen: list = []

    def _run_ingest(options):
        seen.append(options)
        return SimpleNamespace(any_failed=True, report={"publishes": [], "results": []})

    monkeypatch.setattr("okg.substrate.ingest.service.run_ingest", _run_ingest)
    # Nothing after ingest is under test here; keep it off the database.
    monkeypatch.setattr(
        provision_mod, "_latest_published_generation", lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.bootstrap_preflight",
        lambda _dsn, deployment_name: {"ok": True, "database": "demo", "summary": {}},
    )
    monkeypatch.setattr(
        provision_mod, "_provision_publish_proof",
        lambda *_a, **_k: {"ok": False, "reason": "stubbed"},
    )
    args = SimpleNamespace(
        deployment="demo", include=None, readiness_sources=False,
        no_predecessors=False, source_workers=None, publish_once=publish_once,
        dsn=None, _packaged_stage_only=stage_only,
    )

    provision_mod._cmd_provision(args, as_json=True)
    capsys.readouterr()

    assert len(seen) == 1
    assert seen[0].redrive_staged_before_sources is expected
    assert seen[0].publish is (not publish_once and not stage_only)


_NO_WORK_SKIP = {
    "label": "commit",
    "status": "skipped",
    "publish_skipped": True,
    "reason": "no_source_work",
}


def test_judged_publish_prefers_crash_retry_republish_over_trailing_skip() -> None:
    """A retry republishes the staged work, then its own sources add nothing."""
    redrive = {
        "label": "staged-source-redrive", "status": "published",
        "generation_id": "g1",
    }
    skip = dict(_NO_WORK_SKIP)
    assert provision_mod._judged_publish([redrive, skip]) is redrive


@pytest.mark.parametrize(
    "earlier",
    [
        pytest.param([], id="only-skip"),
        pytest.param([{"status": "fail", "error": "refused"}], id="refused"),
        pytest.param(
            [
                {"status": "published", "generation_id": "g1"},
                {"status": "blocked", "generation_id": "g2"},
            ],
            id="blocked-after-published",
        ),
        pytest.param(
            [{"status": "skipped", "publish_skipped": True, "reason": "other"}],
            id="other-skip",
        ),
        # A real publish earlier in the run must not wave through a later
        # refusal or an unknown skip: these guard the "every earlier entry
        # published" rule itself.
        pytest.param(
            [
                {"status": "published", "generation_id": "g1"},
                {"status": "fail", "error": "refused"},
            ],
            id="refused-after-published",
        ),
        pytest.param(
            [
                {"status": "published", "generation_id": "g1"},
                {"status": "skipped", "publish_skipped": True, "reason": "other"},
            ],
            id="other-skip-after-published",
        ),
    ],
)
def test_judged_publish_keeps_last_entry_unless_every_earlier_one_published(
    earlier: list,
) -> None:
    """Fail closed: a refused, blocked, or unknown entry is never waved past."""
    last = dict(_NO_WORK_SKIP)
    assert provision_mod._judged_publish([*earlier, last]) is last


def test_judged_publish_keeps_a_last_entry_that_is_not_the_skip() -> None:
    first = {"status": "published", "generation_id": "g1"}
    blocked = {"status": "blocked", "generation_id": "g2"}
    assert provision_mod._judged_publish([first, blocked]) is blocked
    assert provision_mod._judged_publish([]) == {}
