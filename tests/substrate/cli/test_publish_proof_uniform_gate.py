"""deployment-publish-proof (req.proof.uniform-gate): postgres real-publish gate.

Mirrors ``test_bootstrap_verify_runs_real_publish``: drives a real source run +
catalog apply + Publisher cycle against a clean ephemeral runtime, but through
``okg provision`` and the ``okg up`` smoke-publish phase instead of bootstrap
verify. Each asserts the SAME bar the bootstrap proof enforces — a published
(not blocked) generation with a positive nodes_live/edges_live row delta and the
DLQ in bounds — proving the uniform gate threads through both live-claiming
paths.

The deployment under test is a self-contained fixture deployment authored by the
bootstrap plan/inspect/draft loop (so it needs no live sources, declares no
sync_root, and ingests deterministic fixtures). ``ingest`` reads
``<dir>/source_registry.yaml``, so the bootstrap fixture registry is installed
there with its fixture paths repointed at the staged ``{"nodes": [...]}``
fixtures.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
import yaml

from okg.substrate import deployment_upgrade as du
from okg.substrate.cli.main import main
from okg.substrate.deployment_upgrade import UpgradeOptions

from .._deployment_repo import commit_deployment_tree

pytestmark = pytest.mark.postgres


def _write_fixture(tmp_path: Path) -> tuple[Path, Path]:
    dep = tmp_path / "dep"
    (dep / "fixtures").mkdir(parents=True)
    brief = tmp_path / "brief.md"
    brief.write_text(
        "Track records and policy documents.\n"
        "Which records mention conflicting policy?\n",
        encoding="utf-8",
    )
    (dep / "fixtures" / "records.json").write_text(
        json.dumps([
            {"id": "REC-1", "summary": "Primary record",
             "description": "This record cites policy A and a consistency issue."},
            {"id": "REC-2", "summary": "Second record",
             "description": "This record cites policy B and a different date."},
        ]),
        encoding="utf-8",
    )
    (dep / "fixtures" / "corpus").mkdir()
    (dep / "fixtures" / "corpus" / "policy.md").write_text(
        "# Policy Conflict Note\n\nPolicy A and policy B disagree.",
        encoding="utf-8",
    )
    return dep, brief


def _stage_fixture_deployment(
    tmp_path: Path,
    capsys,
    *,
    name: str,
) -> Path:
    """Author a self-contained fixture deployment and return its dir.

    Runs the bootstrap plan/inspect/draft loop, then copies the staged tree to a
    ``deployments/<name>/`` root and installs the bootstrap fixture registry as
    the deployment's ``source_registry.yaml`` (repointed at the staged
    transformed fixtures so a plain ingest can read them).
    """
    dep, brief = _write_fixture(tmp_path)
    for step in (
        ["plan", "--source", "records=fixtures/records.json",
         "--source", "docs=fixtures/corpus", "--brief", str(brief),
         "--name", name],
        ["inspect-source"],
        ["draft"],
    ):
        rc = main([
            "--json", "deployment", "bootstrap", step[0],
            "--deployment", str(dep), *step[1:], "--apply",
        ])
        capsys.readouterr()
        assert rc == 0, step

    staged = dep / ".okg_bootstrap/staged"
    deploy_root = tmp_path / "deployments"
    target = deploy_root / name
    shutil.copytree(staged, target)
    reg = yaml.safe_load((target / "source_registry.bootstrap.yaml").read_text())
    for sid, entry in reg["sources"].items():
        entry["params"] = {
            "fixture_path": str(target / "fixtures" / f"{sid}.json"),
        }
    (target / "source_registry.yaml").write_text(yaml.safe_dump(reg))
    # The publisher refuses a deployment outside a committed git tree
    # (repository provenance, #1879), so the fixture is committed before
    # any claim or provision reads it.
    commit_deployment_tree(target, "uniform-gate fixture deployment")
    return target


def _json_out(capsys) -> dict:
    out = capsys.readouterr()
    text = (out.out or out.err).strip().splitlines()[-1]
    return json.loads(text)


def _advance_fixture_revisions(target: Path) -> None:
    """Make the staged fixture represent real source work on the next ingest.

    The idempotent-upgrade contract correctly skips byte-identical fixtures as
    ``no_source_work``.  A new source revision alone is not enough either: the
    source runner skips a fact whose only difference from the live row is its
    source revision (#2076, #2308).  So this helper changes real content -- the
    ``title`` or ``label`` attribute of every record, finding and document
    node -- and advances each node's source revision the way a real upstream
    edit would.  Chunk nodes keep their content, because a chunk's identity is
    the digest of its text.
    """
    fixture = target / "fixtures" / "records.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    changed_content = 0
    for node in payload["nodes"]:
        attrs = node.get("attrs")
        if isinstance(attrs, dict):
            for key in ("title", "label"):
                if isinstance(attrs.get(key), str):
                    attrs[key] = f"{attrs[key]} (revised)"
                    changed_content += 1
        revision = node.get("source_revision")
        if not isinstance(revision, dict):
            continue
        previous = str(revision.get("content_sha256") or "")
        revision["content_sha256"] = hashlib.sha256(
            f"{previous}:changed".encode()
        ).hexdigest()
    # Fail loudly if the fixture shape drifts and nothing real changes; a
    # revision-only advance would silently turn every caller into a no-op run.
    assert changed_content > 0, "fixture has no title/label attribute to change"
    fixture.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    # The advanced fixture must be committed too: a dirty deployment tree is
    # refused by the provenance guard before the publish could even block.
    commit_deployment_tree(target, "advance fixture revisions")


@pytest.mark.postgres
def test_provision_publish_proof_asserts_real_published_generation(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """`okg provision` terminates at the uniform gate: a published (not blocked)
    generation with a positive node/edge row delta and DLQ in bounds, surfaced
    via the publish_proof block."""
    name = f"prov{tmp_path.name.lower().replace('-', '')}"
    target = _stage_fixture_deployment(tmp_path, capsys, name=name)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))

    # Claim writable catalog ownership for this fixture deployment on the shared
    # ephemeral DB (which already carries the substrate's own catalog history).
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", clean_runtime, "--force",
    ]) == 0
    capsys.readouterr()

    rc = main([
        "provision", "--deployment", name, "--dsn", clean_runtime, "--json",
    ])
    out = _json_out(capsys)

    assert rc == 0, out
    assert out["ok"] is True
    # A real published-not-blocked generation, pinned.
    generation_id = out["generation_id"]
    assert generation_id is not None
    assert str(generation_id).startswith("okg:"), generation_id
    assert out["catalog_version_id"] is not None

    proof = out["publish_proof"]
    assert proof["ok"] is True
    assert proof["status"] == "published"
    assert proof["generation_id"] == generation_id
    assert proof["nodes_added"] > 0
    assert proof["edges_added"] > 0
    assert proof["dlq_count"] == 0
    # Decision 23a: under the lock-hold bar there is no capacity warning.
    assert "capacity_warning" not in proof
    assert "capacity_warning" not in out

    ingest = next(s for s in out["steps"] if s["step"] == "ingest")
    assert ingest["any_failed"] is False
    assert ingest["generation_id"] == generation_id


@pytest.mark.postgres
def test_provision_by_folder_path_publishes_under_the_manifest_name(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """`okg provision --deployment <folder>` publishes, and every row it writes
    is keyed by the manifest name, never by the folder path.

    Before the fix provision handed the raw path to every step: source progress
    and admission rows were written under the path, and the publish refused
    with ``catalog_writer_scope_conflict`` because its catalog identity carried
    the manifest name. The deployments root is pointed at an empty folder, so
    the name reaches the deployment only through the folder given.
    """
    import psycopg

    name = f"dpath{tmp_path.name.lower().replace('-', '')}"
    target = _stage_fixture_deployment(tmp_path, capsys, name=name)
    empty_root = tmp_path / "empty-deployments-root"
    empty_root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(empty_root))

    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", clean_runtime, "--force",
    ]) == 0
    capsys.readouterr()

    rc = main([
        "provision", "--deployment", str(target),
        "--dsn", clean_runtime, "--json",
    ])
    out = _json_out(capsys)

    assert rc == 0, out
    assert out["ok"] is True
    proof = out["publish_proof"]
    assert proof["status"] == "published", proof
    assert proof["nodes_added"] > 0
    assert proof["edges_added"] > 0
    assert proof["dlq_count"] == 0
    generation_id = out["generation_id"]
    assert str(generation_id).startswith(f"okg:{name}:"), generation_id

    assert main([
        "generation", "describe", str(generation_id),
        "--dsn", clean_runtime, "--json",
    ]) == 0
    described = _json_out(capsys)
    assert described["deployment_name"] == name, described
    assert described["status"] == "published", described

    path_forms = [str(target), str(target.resolve())]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        by_name = conn.execute(
            "SELECT count(*) FROM okg.source_progress WHERE deployment = %s",
            (name,),
        ).fetchone()[0]
        by_path = conn.execute(
            "SELECT"
            " (SELECT count(*) FROM okg.source_progress"
            "   WHERE deployment = ANY(%(p)s))"
            " + (SELECT count(*) FROM okg.admission_envelopes"
            "   WHERE deployment_name = ANY(%(p)s))"
            " + (SELECT count(*) FROM okg.producer_profiles"
            "   WHERE deployment_name = ANY(%(p)s))",
            {"p": path_forms},
        ).fetchone()[0]
    assert by_name > 0
    assert by_path == 0


def _stage_over_bar_deployment(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
    *,
    prefix: str,
) -> str:
    """A fixture deployment whose manifest lowers the lock-hold bar to 9 ms.

    ``publisher.lock_hold_headroom_factor: 100000`` against the 900 s
    statement timeout makes any real publish hold the lock past the bar; the
    reader honours a receipt's factor because it is stricter than its own.
    """
    name = f"{prefix}{tmp_path.name.lower().replace('-', '')}"
    target = _stage_fixture_deployment(tmp_path, capsys, name=name)
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text()) or {}
    publisher = manifest.get("publisher")
    if not isinstance(publisher, dict):
        publisher = {}
    publisher["lock_hold_headroom_factor"] = 100000.0
    manifest["publisher"] = publisher
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    commit_deployment_tree(target, "lower the lock-hold capacity bar")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", clean_runtime, "--force",
    ]) == 0
    capsys.readouterr()
    return name


@pytest.mark.postgres
def test_provision_exits_capacity_warning_for_a_committed_publish_over_the_bar(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """Decision 23a: a committed publish whose lock hold passed the bar exits
    8, not 1, and the JSON names the hold, the bar and how it was derived."""
    from okg.substrate.cli.run_summary import EXIT_CAPACITY_WARNING

    name = _stage_over_bar_deployment(
        tmp_path, clean_runtime, monkeypatch, capsys, prefix="capw",
    )
    rc = main([
        "provision", "--deployment", name, "--dsn", clean_runtime, "--json",
    ])
    captured = capsys.readouterr()
    out = json.loads(captured.out.strip().splitlines()[-1])
    warning_lines = [
        line for line in captured.err.splitlines()
        if line.startswith("capacity warning")
    ]
    assert warning_lines == [
        "capacity warning (okg provision, exit 8): "
        + out["capacity_warning"]["message"]
    ], captured.err

    assert rc == EXIT_CAPACITY_WARNING == 8, out
    assert out["ok"] is True
    assert out["status"] == "capacity_warning"
    assert out["exit_code"] == 8
    proof = out["publish_proof"]
    assert proof["ok"] is True
    assert proof["status"] == "published"
    assert out["generation_id"] is not None
    assert proof["generation_id"] == out["generation_id"]
    assert proof["nodes_added"] > 0
    assert proof["edges_added"] > 0
    assert proof["dlq_count"] == 0
    warning = out["capacity_warning"]
    assert warning == proof["capacity_warning"]
    assert warning["max_hold_ms"] == 9.0
    assert warning["hold_ms"] > warning["max_hold_ms"]
    assert "held the lock" in warning["message"]
    assert "against a 0.009s bar" in warning["message"]
    assert "headroom factor 100000.0" in warning["bar_derivation"]
    assert "running statement timeout 900.0s" in warning["bar_derivation"]


@pytest.mark.postgres
def test_provision_over_the_bar_still_exits_1_on_a_real_proof_failure(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The warning never hides a failed check: dead-letter work fails the
    proof and provision exits 1 as before, even with the hold over the bar."""
    name = _stage_over_bar_deployment(
        tmp_path, clean_runtime, monkeypatch, capsys, prefix="capf",
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (1, {}),
    )
    rc = main([
        "provision", "--deployment", name, "--dsn", clean_runtime, "--json",
    ])
    out = _json_out(capsys)

    assert rc == 1, out
    assert out["ok"] is False
    assert "status" not in out
    proof = out["publish_proof"]
    assert proof["ok"] is False
    assert proof["reason"] == "publish verification found dead-letter runtime work"
    assert "capacity_warning" not in out


@pytest.mark.postgres
def test_up_smoke_publish_gate_asserts_real_published_generation(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The `okg up` smoke-publish phase applies the uniform gate: it drives a
    real publish and asserts a published-not-blocked generation with a positive
    node/edge row delta and DLQ in bounds (the publish_proof block), instead of
    trusting the publish command's exit code."""
    name = f"up{tmp_path.name.lower().replace('-', '')}"
    target = _stage_fixture_deployment(tmp_path, capsys, name=name)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))

    # Migrate + claim ownership + load the catalog (no publish yet), so the
    # smoke publish is the first real generation.
    assert main([
        "--json", "migrate", "--dsn", clean_runtime,
        "--deployment", name, "--apply",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", clean_runtime, "--force",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json", "catalog", "load", "--deployment", str(target),
        "--dsn", clean_runtime, "--apply",
    ]) == 0
    capsys.readouterr()

    # Drive the upgrade runner's smoke-publish phase against the real DB.
    monkeypatch.setattr(
        du, "_resolve_target",
        lambda options: {"dsn": clean_runtime, "target": {"database": "db"}},
    )
    ctx = du._build_context(
        UpgradeOptions(
            deployment=name,
            dsn=clean_runtime,
            profile="prod",
            state_file=tmp_path / "up-state.json",
        ),
        command_runner=None,
    )
    phase = du._phase_smoke_publish(ctx)

    assert phase["ok"] is True, phase
    proof = phase["publish_proof"]
    assert proof["status"] == "published"
    assert str(proof["generation_id"]).startswith("okg:"), proof
    assert proof["nodes_added"] > 0
    assert proof["edges_added"] > 0
    assert proof["dlq_count"] == 0


@pytest.mark.postgres
def test_up_smoke_publish_gate_fails_a_blocked_generation(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """A re-publish of the already-published fixtures produces a BLOCKED
    generation (narrowing violations against the now-populated graph); the
    uniform gate must FAIL the smoke phase rather than report success on the
    zero-exit-or-not publish command."""
    name = f"blk{tmp_path.name.lower().replace('-', '')}"
    target = _stage_fixture_deployment(tmp_path, capsys, name=name)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))

    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", clean_runtime, "--force",
    ]) == 0
    capsys.readouterr()
    # provision publishes generation 1 from the fixtures.
    rc = main([
        "provision", "--deployment", name, "--dsn", clean_runtime, "--json",
    ])
    first = _json_out(capsys)
    assert rc == 0, first
    assert first["ok"] is True

    # Change fixture content so the idempotent ingest does real source work.
    # A byte-identical or revision-only fixture is correctly accepted as a
    # validated no-op and therefore cannot exercise the blocked-generation gate.
    _advance_fixture_revisions(target)

    # The second smoke publish blocks on narrowing/subtype violations against
    # the now-populated graph.
    monkeypatch.setattr(
        du, "_resolve_target",
        lambda options: {"dsn": clean_runtime, "target": {"database": "db"}},
    )
    ctx = du._build_context(
        UpgradeOptions(
            deployment=name,
            dsn=clean_runtime,
            profile="prod",
            state_file=tmp_path / "up-state.json",
        ),
        command_runner=None,
    )
    phase = du._phase_smoke_publish(ctx)
    # The publish command exits non-zero on a blocked generation; the gate
    # reports the failure instead of a false success.
    assert phase["ok"] is False, phase


@pytest.mark.postgres
def test_dlq_count_treats_absent_dbos_runtime_as_not_provisioned(
    clean_runtime: str,
) -> None:
    """#634 regression: on a database where DBOS was never launched
    (`dbos.workflow_status` absent — every CLI cold-start), the health view
    emits a sentinel `unavailable` row that made the fail-closed gate refuse
    ALL cold-start publish proofs. Provable absence of the runtime means no
    queue can hold work: the proof must annotate (dbos_not_provisioned) and
    pass, while a runtime that exists-but-unreadable still refuses."""
    import psycopg

    from okg.substrate.publish_proof import dlq_count

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    dead_letter, queue_depth = dlq_count(clean_runtime)
    assert dead_letter == 0
    assert "dbos_not_provisioned" in queue_depth
    assert not any(
        key.startswith("runtime_") and count
        for key, count in queue_depth.items()
    ), queue_depth


_AWAITING_SOURCE_REGISTRY = """\
sources:
  notes:
    enabled_by_default: false
    module: okg.substrate.library.sources.notes_dir
    class: NotesDirSource
    ownership_id: ghost2331.notes
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields:
    - path
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params:
      notes_dir: null
"""


@pytest.mark.postgres
def test_init_empty_plan_refuses_another_deployments_generation(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2331, review of PR #2412. With no runnable source, init's
    first publish must be proven by a generation of THIS deployment. On a
    migrated database provision's unscoped fallback already answers with the
    `bootstrap` deployment's generation 0, so a publish that produced nothing
    must still fail init. Real database reads; only the okg children, the
    migration and the Nomos seed are stubbed."""
    import subprocess as subprocess_mod

    from okg.substrate.cli import init as init_mod
    from okg.substrate.cli import provision as provision_mod
    from okg.substrate.db import migrate as migrate_mod

    name = "ghost2331"
    # The trap is real on this database: the unscoped helper finds a
    # generation for a deployment that never published, with no counts.
    trap = provision_mod._published_generation_row_counts(
        clean_runtime, deployment_name=name,
    )
    assert trap[0] is not None and trap[1:] == (None, None), trap
    assert init_mod._deployment_published_generation(
        clean_runtime, deployment_name=name,
    ) is None

    dep_dir = tmp_path / name
    dep_dir.mkdir()
    (dep_dir / "deployment.yaml").write_text(
        f"name: {name}\nsource_registry: source_registry.yaml\n",
    )
    (dep_dir / "source_registry.yaml").write_text(_AWAITING_SOURCE_REGISTRY)

    monkeypatch.setattr(
        migrate_mod, "ensure_required_extensions", lambda *a, **k: {},
    )
    monkeypatch.setattr(migrate_mod, "migrate", lambda *a, **k: None)
    monkeypatch.setattr(
        init_mod,
        "_seed_init_nomos_baseline_if_required",
        lambda **_kwargs: None,
    )
    ran: list[str] = []
    real_run = subprocess_mod.run

    def _publishes_nothing(cmd, *args, **kwargs):
        if not (isinstance(cmd, list) and "okg.substrate.cli.main" in cmd):
            return real_run(cmd, *args, **kwargs)
        verb = cmd[cmd.index("okg.substrate.cli.main") + 1]
        ran.append(verb)
        stdout = ""
        if verb == "run":
            # The run claims another deployment's version (#2422): the
            # proof must read it, scoped to this deployment, and refuse it.
            stdout = json.dumps({"run_summary": {
                "outcome": "published", "generation_id": str(trap[0]),
            }})
        return subprocess_mod.CompletedProcess(
            args=cmd, returncode=0, stdout=stdout, stderr="",
        )

    monkeypatch.setattr(subprocess_mod, "run", _publishes_nothing)

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name=name,
        deployment_dir=dep_dir,
        answers={"postgres_dsn": clean_runtime},
    )

    assert ran == ["catalog", "run"]
    assert outcome["ok"] is False, outcome
    assert outcome["stage"] == "publish_proof"
    proof = outcome["details"]["publish_proof"]
    assert proof["generation_id"] == str(trap[0])
    assert (proof["node_count"], proof["edge_count"]) == (None, None)
