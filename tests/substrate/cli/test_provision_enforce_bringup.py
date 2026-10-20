"""fix-provision-enforce-bringup: provision cold-starts enforce-mode deployments.

End-to-end (postgres-gated): a fixture deployment whose manifest ships
``nomos.runtime_enforcement: enforce`` provisions from an empty database to a
published generation with zero ``nomos.default_deny.no_active_bundle``
rejections, because ``okg provision`` now seeds the same idempotent Nomos
baseline ``okg up`` runs (``deployment_upgrade._phase_nomos_baseline``).
Also pins:

- per-source failure reasons reach the provision summary (human + --json),
  including the empty-publish refusal (req.provision.surface-reasons);
- the documented single-operator read path works right after provision —
  ``okg search`` with no Nomos env overrides presents the manifest's
  operator read principal, while undeclared subjects stay denied
  (req.provision.cli-read-subject; design recorded in
  ``pact/changes/fix-provision-enforce-bringup/design.md``);
- a DSN whose database does not exist fails with the literal ``createdb``
  command, not a raw psycopg traceback (req.provision.cold-start-hint).

The fixture deployment is staged by the bootstrap plan/inspect/draft loop
(reusing ``test_publish_proof_uniform_gate._stage_fixture_deployment``), then
its manifest gains an audit-clean enforce-mode ``nomos`` block and each source
an explicit ``source_policy``.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.dsn_form import dsn_with_dbname
from tests.substrate._deployment_repo import commit_deployment_tree
from tests.substrate.cli.test_publish_proof_uniform_gate import (
    _json_out,
    _stage_fixture_deployment,
)

DEFAULT_DENY_REASON = "nomos.default_deny.no_active_bundle"

_SOURCE_POLICY = {
    "sensitivity": "internal",
    "data_classification": "internal",
    "store_raw": True,
    "credential_ref_policy": "none",
    "live_call_allowed": False,
    "exportability": "internal",
    "retention": "normal",
    "provenance": "local_filesystem",
    "privacy_obligations": ["audit"],
}


def test_provision_seed_uses_public_policy_graph_and_refuses_unrooted_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import provision as provision_mod
    from okg.substrate.errors import SubstrateError
    import okg.substrate.nomos as nomos
    import okg.substrate.nomos.baseline as baseline

    deployment_dir = tmp_path / "source-bound"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "source-bound",
            "source_registry": "source_registry.yaml",
            "nomos": {"runtime_enforcement": "audit"},
        }),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    policy_graph = {
        "policy_bundle_id": "nomos:rollout:baseline:source-bound",
        "rules": [{"rule_id": "exact-source-bindings"}],
    }
    spec = {
        "required": True,
        "manifest": {"nomos": {"runtime_enforcement": "audit"}},
        "source_names": ["source-bound.alpha"],
        "source_policies": {"source-bound.alpha": {}},
        "runtime_source_names": ["source-bound.alpha"],
        "policy_graph": policy_graph,
    }
    captured: dict[str, object] = {}

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

        def commit(self) -> None:
            pass

    monkeypatch.setattr(baseline, "deployment_baseline_spec", lambda **kwargs: spec)
    monkeypatch.setattr(
        nomos,
        "audit_deployment_policy",
        lambda *args, **kwargs: {"ok": True, "source_policies": {}},
    )
    monkeypatch.setattr(
        nomos,
        "seed_deployment_baseline_nomos_state",
        lambda _conn, **kwargs: captured.update(kwargs) or {"ok": True},
    )
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: Connection())

    result = provision_mod._seed_nomos_baseline(
        "source-bound",
        deployment_dir,
        dsn="postgres://unused",
    )

    assert result["ok"] is True
    assert captured["policy_graph"] is policy_graph

    (deployment_dir / "deployment.yaml").unlink()
    monkeypatch.setattr(provision_mod, "SubstrateError", SubstrateError)
    with pytest.raises(SubstrateError, match="cannot resolve.*source_binding_policy"):
        provision_mod._seed_nomos_baseline(
            "source-bound",
            deployment_dir,
            dsn="postgres://unused",
            raw_manifest={
                "nomos": {
                    "runtime_enforcement": "audit",
                    "source_binding_policy": "source-bindings.policy.json",
                },
            },
        )


def _nomos_enforce_block() -> dict:
    """Audit-clean enforce-mode nomos block (dogfood_workspace class)."""
    return {
        "runtime_enforcement": "enforce",
        "deployment_posture": {
            "deployment_class": "dogfood_workspace",
            "read_principals": ["human_operator", "okg_service"],
            "write_principals": [
                "human_operator", "okg_service", "source_adapter",
            ],
            "mcp_write_default": "review",
            "export_policy": "internal_only",
            "secret_handling": "secret_ref_required",
            "live_call_default": "review",
            "allowed_source_sensitivities": ["internal", "local_private"],
            "allowed_data_classifications": ["internal", "private"],
            "allowed_exportability": ["internal", "no_export"],
            "privacy_obligations": ["audit", "secret_ref_required"],
            "readiness_profiles": ["dogfood"],
        },
    }


def _enable_enforce_mode(target: Path) -> None:
    """Add the enforce nomos block + per-source source_policy to a staged
    fixture deployment."""
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text()) or {}
    manifest["nomos"] = _nomos_enforce_block()
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")

    registry_path = target / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text()) or {}
    for entry in (registry.get("sources") or {}).values():
        entry["source_policy"] = dict(_SOURCE_POLICY)
    registry_path.write_text(yaml.safe_dump(registry), encoding="utf-8")
    # The publisher reads the committed tree (repository provenance, #1879),
    # so the enforce-mode edits are committed before any provision reads them.
    commit_deployment_tree(target, "enable enforce mode")


def _stage_enforce_deployment(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
    *,
    name: str,
) -> Path:
    target = _stage_fixture_deployment(tmp_path, capsys, name=name)
    _enable_enforce_mode(target)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    # The manifest must decide the enforcement mode, and the CLI read path
    # must see no explicit subject.
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    for var in ("OKG_NOMOS_SUBJECT", "OKG_PRINCIPAL", "OKG_MCP_NOMOS_SUBJECT"):
        monkeypatch.delenv(var, raising=False)
    return target


def _claim_ownership(target: Path, dsn: str, capsys) -> None:
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", dsn, "--force",
    ]) == 0
    capsys.readouterr()


def _provision(name: str, dsn: str, capsys) -> tuple[int, dict]:
    rc = main([
        "provision", "--deployment", name, "--dsn", dsn, "--json",
    ])
    return rc, _json_out(capsys)


def _baseline_activation_state(dsn: str, name: str) -> tuple[int, str | None]:
    """(total activation rows, active policy fingerprint) for a deployment."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.nomos_policy_activations "
            "WHERE deployment_name = %s",
            (name,),
        )
        total = int(cur.fetchone()[0])
        cur.execute(
            "SELECT policy_fingerprint FROM okg.nomos_policy_activations "
            "WHERE deployment_name = %s AND status = 'active' "
            "ORDER BY activated_at DESC LIMIT 1",
            (name,),
        )
        row = cur.fetchone()
        return total, (row[0] if row else None)


def _default_deny_rejections(dsn: str, name: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.nomos_decisions "
            "WHERE deployment_name = %s AND reason_code = %s",
            (name, DEFAULT_DENY_REASON),
        )
        return int(cur.fetchone()[0])


# ---------------------------------------------------------------------------
# req.provision.nomos-baseline — enforce-mode cold start end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.postgres
def test_provision_enforce_cold_start_seeds_baseline_and_publishes(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """A fresh enforce-mode deployment provisions to a populated published
    generation with zero default-deny rejections; a second provision does not
    churn the baseline bundle (same fingerprint, no new activation)."""
    name = f"enf{tmp_path.name.lower().replace('-', '')}"
    target = _stage_enforce_deployment(tmp_path, capsys, monkeypatch, name=name)
    _claim_ownership(target, clean_runtime, capsys)

    rc, out = _provision(name, clean_runtime, capsys)

    assert rc == 0, out
    assert out["ok"] is True
    assert [s["step"] for s in out["steps"]] == [
        "extensions", "migrate", "catalog", "nomos_baseline", "ingest",
    ]
    baseline = next(s for s in out["steps"] if s["step"] == "nomos_baseline")
    assert baseline["ok"] is True
    assert baseline["activated"] is True
    assert baseline["policy_bundle_id"] == f"nomos:rollout:baseline:{name}"
    assert baseline["policy_fingerprint"]
    # req.readux.mcp-subject-default: the bring-up output names the MCP
    # subject contract so non-default setups are discoverable.
    hint = baseline["mcp_subject_hint"]
    assert "subject:mcp_client" in hint
    assert "OKG_MCP_NOMOS_SUBJECT" in hint

    ingest = next(s for s in out["steps"] if s["step"] == "ingest")
    assert ingest["any_failed"] is False
    assert "failed_sources" not in ingest

    # A real, populated published generation (the uniform gate).
    proof = out["publish_proof"]
    assert proof["ok"] is True, proof
    assert proof["status"] == "published"
    assert proof["nodes_added"] > 0
    assert proof["edges_added"] > 0

    # Zero nomos.default_deny.no_active_bundle rejections anywhere in the run.
    assert _default_deny_rejections(clean_runtime, name) == 0

    total_1, fingerprint_1 = _baseline_activation_state(clean_runtime, name)
    assert total_1 == 1
    assert fingerprint_1 == baseline["policy_fingerprint"]

    # Second provision: the baseline step runs again but re-activates
    # nothing (idempotent seed) — same bundle fingerprint, no new
    # activation row. (The fixture sources are not incremental, so the
    # re-run's ingest outcome is not asserted here; the churn contract is.)
    _rc2, out2 = _provision(name, clean_runtime, capsys)
    baseline_2 = next(
        s for s in out2["steps"] if s["step"] == "nomos_baseline"
    )
    assert baseline_2["activated"] is False
    assert baseline_2["policy_fingerprint"] == fingerprint_1
    total_2, fingerprint_2 = _baseline_activation_state(clean_runtime, name)
    assert (total_2, fingerprint_2) == (total_1, fingerprint_1)


@pytest.mark.postgres
@pytest.mark.parametrize("reference_kind", ["directory", "manifest"])
def test_ingest_path_reference_uses_manifest_name_for_admission(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
    record_property,
    reference_kind: str,
) -> None:
    """A path-addressed inline ingest reuses the name-scoped Nomos baseline."""
    name = f"path{tmp_path.name.lower().replace('-', '')}"
    target = _stage_enforce_deployment(tmp_path, capsys, monkeypatch, name=name)
    _claim_ownership(target, clean_runtime, capsys)

    # Establish the active baseline and a first real published generation using
    # the normal name-addressed provisioning path.
    rc, provision = _provision(name, clean_runtime, capsys)
    assert rc == 0, provision
    assert provision["publish_proof"]["status"] == "published"

    # Force source work for the path-addressed run so this test proves a new
    # publish, not only that a no-op command returned successfully.
    from tests.substrate.cli.test_publish_proof_uniform_gate import (
        _advance_fixture_revisions,
    )

    _advance_fixture_revisions(target)
    deployment_reference = (
        target if reference_kind == "directory" else target / "deployment.yaml"
    )
    rc = main([
        "--json", "ingest", "--deployment", str(deployment_reference),
        "--dsn", clean_runtime, "--mode", "reconcile", "--inline",
    ])
    ingest = _json_out(capsys)
    assert rc == 0, ingest
    assert ingest["deployment"] == name
    publish = next(
        item for item in ingest["publishes"]
        if item.get("status") in {"published", "blocked"}
    )
    assert publish["status"] == "published", publish
    assert publish["generation_id"] != provision["generation_id"], publish
    record_property("generation_id", str(publish["generation_id"]))
    record_property(
        "pact_requirement", "req.issue-976.canonical-admission-identity",
    )

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        )
        graph = store.checkout_generation(str(publish["generation_id"]))
        assert graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes",
        )[0]["n"] > 0
        assert graph.query(
            "SELECT count(*) AS n FROM okg.graph_edges",
        )[0]["n"] > 0
        cur.execute(
            """
            SELECT count(*)
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
               AND action = 'source.emit'
               AND resource->>'scope' = %s
            """,
            (name, f"deployment:{name}"),
        )
        assert int(cur.fetchone()[0]) > 0
        # Neither filesystem spelling may become a second Nomos identity.
        # Check both the deployment column and the resource scope because the
        # admission and audit paths persist those values independently.
        for path_reference in (target, target / "deployment.yaml"):
            raw_reference = str(path_reference)
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.nomos_decisions
                 WHERE deployment_name = %s
                    OR resource->>'scope' = %s
                """,
                (raw_reference, f"deployment:{raw_reference}"),
            )
            assert int(cur.fetchone()[0]) == 0


@pytest.mark.postgres
def test_ingest_manifest_path_with_nomos_disabled_keeps_name_identity(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """Path addressing preserves the pre-existing Nomos-disabled behavior."""
    name = f"plain{tmp_path.name.lower().replace('-', '')}"
    target = _stage_fixture_deployment(tmp_path, capsys, name=name)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    assert deployment_nomos_enforcement_mode(name) == "off"
    _claim_ownership(target, clean_runtime, capsys)

    rc, provision = _provision(name, clean_runtime, capsys)
    assert rc == 0, provision
    assert provision["publish_proof"]["status"] == "published"

    from tests.substrate.cli.test_publish_proof_uniform_gate import (
        _advance_fixture_revisions,
    )

    _advance_fixture_revisions(target)
    manifest_path = target / "deployment.yaml"
    rc = main([
        "--json", "ingest", "--deployment", str(manifest_path),
        "--dsn", clean_runtime, "--mode", "reconcile", "--inline",
    ])
    ingest = _json_out(capsys)
    assert rc == 0, ingest
    assert ingest["deployment"] == name
    publish = next(
        item for item in ingest["publishes"]
        if item.get("status") in {"published", "blocked"}
    )
    assert publish["status"] == "published", publish
    assert publish["generation_id"] != provision["generation_id"], publish


# ---------------------------------------------------------------------------
# req.provision.surface-reasons — denial reasons reach the summary
# ---------------------------------------------------------------------------


@pytest.mark.postgres
def test_reasons_surface(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The pre-fix cold-start failure (enforce mode, no active bundle) now
    names each denied source and the distinct reason codes in --json, and the
    empty-publish refusal states why the run was empty."""
    name = f"deny{tmp_path.name.lower().replace('-', '')}"
    target = _stage_enforce_deployment(tmp_path, capsys, monkeypatch, name=name)
    _claim_ownership(target, clean_runtime, capsys)

    # Simulate the pre-fix boot: skip the baseline seed so the enforce-mode
    # runtime default-denies every source admission.
    from okg.substrate.cli import provision as provision_mod

    monkeypatch.setattr(
        provision_mod, "_seed_nomos_baseline", lambda *a, **k: None,
    )

    rc, out = _provision(name, clean_runtime, capsys)

    assert rc == 1
    assert out["ok"] is False
    ingest = next(s for s in out["steps"] if s["step"] == "ingest")
    assert ingest["any_failed"] is True
    # Each failing source is named with its reason code — not a bare
    # any_failed: True.
    failed = ingest["failed_sources"]
    assert failed, out
    assert all(entry["name"] for entry in failed)
    # The default-deny reason code reaches every failing source's payload
    # (as admission_reject_reason at the admission boundary, or embedded in
    # the error for a pre-run source.ingest boundary denial).
    assert all(
        DEFAULT_DENY_REASON in (
            str(entry.get("admission_reject_reason") or "")
            + str(entry.get("error") or "")
        )
        for entry in failed
    ), failed
    assert any(DEFAULT_DENY_REASON in r for r in ingest["failure_reasons"])
    # Also surfaced at the top of the summary.
    assert out["failed_sources"] == failed
    assert any(DEFAULT_DENY_REASON in r for r in out["failure_reasons"])
    # The empty-publish refusal states WHY the run was empty.
    proof = out["publish_proof"]
    assert proof["ok"] is False
    assert DEFAULT_DENY_REASON in proof["reason"]
    assert any(
        DEFAULT_DENY_REASON in r for r in proof["source_failure_reasons"]
    )


def test_failed_source_summaries_collapse_to_distinct_reasons() -> None:
    """Unit: the summary builders name failing sources and dedup reasons."""
    from okg.substrate.cli.provision import (
        _distinct_failure_reasons,
        _failed_source_summaries,
    )

    report = {
        "results": [
            {"name": "ok_source", "status": "OK"},
            {
                "name": "denied_a",
                "status": "FAIL",
                "stage": "admission_finalize",
                "error": f"source admission rejected: {DEFAULT_DENY_REASON}",
                "admission_reject_reason": DEFAULT_DENY_REASON,
            },
            {
                "name": "denied_b",
                "status": "FAIL",
                "stage": "admission_finalize",
                "error": f"source admission rejected: {DEFAULT_DENY_REASON}",
                "admission_reject_reason": DEFAULT_DENY_REASON,
            },
            {
                "name": "crashed",
                "status": "FAIL",
                "stage": "run",
                "error": "ValueError: boom",
            },
        ],
    }

    failed = _failed_source_summaries(report)
    assert [entry["name"] for entry in failed] == [
        "denied_a", "denied_b", "crashed",
    ]
    assert failed[0]["admission_reject_reason"] == DEFAULT_DENY_REASON
    assert failed[2]["error"] == "ValueError: boom"
    # Distinct reason codes, admission reason preferred over raw error.
    assert _distinct_failure_reasons(failed) == [
        DEFAULT_DENY_REASON,
        "ValueError: boom",
    ]


def test_publish_proof_refusal_states_denial_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unit: the no-publish refusal names the per-source denial reasons."""
    from okg.substrate.cli import provision as provision_mod

    monkeypatch.setattr(
        provision_mod,
        "_published_generation_row_counts",
        lambda dsn, *, deployment_name: (None, None, None),
    )

    proof = provision_mod._provision_publish_proof(
        "postgres://unused/unused",
        {},
        deployment_name="dep",
        source_failure_reasons=[DEFAULT_DENY_REASON],
    )

    assert proof["ok"] is False
    assert DEFAULT_DENY_REASON in proof["reason"]
    assert proof["source_failure_reasons"] == [DEFAULT_DENY_REASON]
    assert "failed_sources" in proof["fix_hint"]

    # Without reasons the refusal keeps its original wording.
    bare = provision_mod._provision_publish_proof(
        "postgres://unused/unused",
        {},
        deployment_name="dep",
    )
    assert bare["ok"] is False
    assert "source_failure_reasons" not in bare


# ---------------------------------------------------------------------------
# req.provision.cli-read-subject — search works right after provision
# ---------------------------------------------------------------------------


def _search(deployment: str, dsn: str, monkeypatch, capsys, **overrides):
    from okg.substrate.cli import _read_side
    from okg.substrate.cli.dsn_authority import ReadDsnResolution

    monkeypatch.setattr(
        _read_side,
        "resolve_read_dsn",
        lambda _args, *, surface: ReadDsnResolution(
            dsn=dsn,
            deployment=deployment,
            authority={"mode": "test", "surface": surface},
            target=None,
        ),
    )
    args = SimpleNamespace(
        deployment=deployment,
        dsn=None,
        generation_id=None,
        query="policy",
        method="lexical",
        subtype=None,
        attr_filter=None,
        select=None,
        limit=10,
        offset=0,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    exit_code = _read_side.cmd_search(args, as_json=True)
    captured = capsys.readouterr()
    if captured.out.strip():
        # Success path: one indented JSON document on stdout.
        payload = json.loads(captured.out)
    else:
        # Error path: a single-line JSON error envelope on stderr.
        payload = json.loads(captured.err.strip().splitlines()[-1])
    return exit_code, payload


@pytest.mark.postgres
def test_search_after_provision(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """With no OKG_NOMOS_SUBJECT/OKG_PRINCIPAL exported, okg search resolves
    the manifest's operator read principal and returns rows right after an
    enforce-mode provision; an undeclared subject is still denied."""
    name = f"srch{tmp_path.name.lower().replace('-', '')}"
    target = _stage_enforce_deployment(tmp_path, capsys, monkeypatch, name=name)
    _claim_ownership(target, clean_runtime, capsys)
    rc, out = _provision(name, clean_runtime, capsys)
    assert rc == 0, out

    exit_code, payload = _search(name, clean_runtime, monkeypatch, capsys)

    assert exit_code == 0, payload
    assert payload["nomos_effect"] == "allow"
    assert payload["results"], payload

    # The boundary decision is auditable: it records the presented subject,
    # how it was resolved, and the OS user behind the terminal.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT subject, context FROM okg.nomos_decisions "
            "WHERE deployment_name = %s "
            "  AND resource->>'resource_kind' = 'cli_search_boundary' "
            "ORDER BY decision_row_id DESC LIMIT 1",
            (name,),
        )
        subject, context = cur.fetchone()
    assert subject == "subject:human_operator"
    assert context["subject_source"] == "manifest_read_principal"
    if os.environ.get("USER"):
        assert context["os_user"] == os.environ["USER"]

    # Fail-closed: an explicitly presented subject outside the manifest's
    # read principals is still denied.
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:stranger")
    denied_code, denied_payload = _search(
        name, clean_runtime, monkeypatch, capsys,
    )
    assert denied_code == 1
    assert denied_payload["error"]["code"] == "nomos_runtime_denied"
    assert (
        denied_payload["error"]["data"]["surface"] == "graph.read.cli_search"
    )


def test_resolve_cli_read_subject_env_override_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli._read_side import _resolve_cli_read_subject

    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "alice")
    subject, meta = _resolve_cli_read_subject(
        deployment="anything", default_kind="cli_search",
    )
    assert subject == "subject:alice"
    assert meta["source"] == "env_override"

    monkeypatch.delenv("OKG_NOMOS_SUBJECT")
    monkeypatch.setenv("OKG_PRINCIPAL", "subject:bob")
    subject, meta = _resolve_cli_read_subject(
        deployment="anything", default_kind="cli_search",
    )
    assert subject == "subject:bob"
    assert meta["source"] == "env_override"


def _write_manifest_with_principals(
    tmp_path: Path, name: str, read_principals: list[str] | None,
) -> Path:
    root = tmp_path / "deployments"
    dep = root / name
    dep.mkdir(parents=True)
    manifest: dict = {"name": name}
    if read_principals is not None:
        manifest["nomos"] = {
            "runtime_enforcement": "enforce",
            "deployment_posture": {"read_principals": read_principals},
        }
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(manifest), encoding="utf-8",
    )
    return root


def test_resolve_cli_read_subject_prefers_human_operator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli._read_side import _resolve_cli_read_subject

    for var in ("OKG_NOMOS_SUBJECT", "OKG_PRINCIPAL"):
        monkeypatch.delenv(var, raising=False)
    root = _write_manifest_with_principals(
        tmp_path, "dep_ho", ["agent", "human_operator", "okg_service"],
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    subject, meta = _resolve_cli_read_subject(
        deployment="dep_ho", default_kind="cli_search",
    )
    assert subject == "subject:human_operator"
    assert meta["source"] == "manifest_read_principal"
    assert meta["principal"] == "human_operator"


def test_resolve_cli_read_subject_falls_back_to_first_declared(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli._read_side import _resolve_cli_read_subject

    for var in ("OKG_NOMOS_SUBJECT", "OKG_PRINCIPAL"):
        monkeypatch.delenv(var, raising=False)
    root = _write_manifest_with_principals(
        tmp_path, "dep_ci", ["ci_runner", "okg_service"],
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    subject, meta = _resolve_cli_read_subject(
        deployment="dep_ci", default_kind="cli_search",
    )
    assert subject == "subject:ci_runner"
    assert meta["principal"] == "ci_runner"


def test_resolve_cli_read_subject_legacy_fallback_without_principals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No declared read principals -> unchanged legacy behavior ($USER),
    which an enforce baseline still denies (fail-closed)."""
    from okg.substrate.cli._read_side import _resolve_cli_read_subject

    for var in ("OKG_NOMOS_SUBJECT", "OKG_PRINCIPAL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("USER", "zed")

    # Manifest exists but declares no read principals.
    root = _write_manifest_with_principals(tmp_path, "dep_none", None)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    subject, meta = _resolve_cli_read_subject(
        deployment="dep_none", default_kind="cli_search",
    )
    assert subject == "subject:zed"
    assert meta["source"] == "os_user_fallback"

    # No manifest at all.
    subject, meta = _resolve_cli_read_subject(
        deployment="no_such_deployment_zzz", default_kind="cli_search",
    )
    assert subject == "subject:zed"
    assert meta["source"] == "os_user_fallback"


# ---------------------------------------------------------------------------
# req.provision.cold-start-hint — missing database names the createdb fix
# ---------------------------------------------------------------------------


def _missing_db_dsn(base_dsn: str, dbname: str) -> str:
    """The base DSN pointed at ``dbname``, kept in URL form.

    ``make_conninfo`` would return a keyword/value string, which the DSN
    resolvers refuse before provision can reach the missing database.
    """
    return dsn_with_dbname(base_dsn, dbname)


@pytest.mark.postgres
def test_missing_db_hint(
    tmp_path: Path,
    okg_migrated: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """Provision against a nonexistent database exits nonzero with the
    literal createdb command; no raw psycopg traceback reaches the operator."""
    from okg.substrate.cli._db_bootstrap_errors import createdb_command

    name = "missdep"
    root = tmp_path / "deployments"
    (root / name).mkdir(parents=True)
    (root / name / "deployment.yaml").write_text(
        yaml.safe_dump({"name": name}), encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    missing_db = f"okg_test_provbringup_missing_{tmp_path.name.lower()}"
    missing_db = "".join(
        ch if ch.isalnum() or ch == "_" else "_" for ch in missing_db
    )
    dsn = _missing_db_dsn(okg_migrated, missing_db)

    rc = main(["provision", "--deployment", name, "--dsn", dsn, "--json"])
    captured = capsys.readouterr()

    assert rc != 0
    assert "Traceback" not in captured.err
    assert "Traceback" not in captured.out
    err = json.loads(captured.err.strip().splitlines()[-1])["error"]
    assert err["code"] == "database_missing"
    assert missing_db in err["message"]
    expected_command = createdb_command(dsn)
    assert missing_db in expected_command
    assert expected_command in err["fix_hint"]
    assert err["fix_hint"].count("createdb") == 1


def test_missing_database_classifier_unit() -> None:
    from okg.substrate.cli._db_bootstrap_errors import (
        classify_missing_database_error,
        createdb_command,
        missing_database_name,
    )
    from okg.substrate.errors import SubstrateError

    connect_failure = psycopg.OperationalError(
        'connection failed: connection to server at "127.0.0.1", port 5433 '
        'failed: FATAL:  database "okg_absent" does not exist',
    )
    assert missing_database_name(connect_failure) == "okg_absent"

    # Server-side InvalidCatalogName without a parsable name classifies as
    # missing (empty name -> caller falls back to the DSN's dbname).
    invalid_catalog = psycopg.errors.InvalidCatalogName("nope")
    assert missing_database_name(invalid_catalog) == ""

    # Unrelated failures never classify (no wrong hint on auth/refused).
    assert missing_database_name(
        psycopg.OperationalError("connection refused"),
    ) is None
    assert missing_database_name(ValueError("boom")) is None

    dsn = "postgres://postgres:okg@localhost:5433/okg_absent"
    assert createdb_command(dsn) == (
        "createdb -h localhost -p 5433 -U postgres okg_absent"
    )
    assert createdb_command("dbname=okg_absent") == "createdb okg_absent"

    err = classify_missing_database_error(
        connect_failure, dsn=dsn, surface="okg provision",
    )
    assert isinstance(err, SubstrateError)
    assert err.code == "database_missing"
    assert "okg_absent" in err.message
    assert "createdb -h localhost -p 5433 -U postgres okg_absent" in (
        err.fix_hint or ""
    )

    # InvalidCatalogName without a name in the message falls back to the
    # DSN's dbname for both the message and the command.
    fallback = classify_missing_database_error(
        invalid_catalog, dsn=dsn, surface="okg init",
    )
    assert isinstance(fallback, SubstrateError)
    assert "okg_absent" in fallback.message

    assert classify_missing_database_error(
        ValueError("boom"), dsn=dsn, surface="okg provision",
    ) is None
