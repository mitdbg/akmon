"""End-to-end publish gate (#829): provision -> multi-source sync -> live nodes.

Three consecutive regressions (#822 forbidden ``command`` fact attr, #826
zero-output re-sync rejection, #828 manifest-continuity refusal) each passed
unit tests while breaking every real downstream publish, because the existing
per-PR publish proof stages one narrow fixture, never re-syncs unchanged
content, and resolves claim + publish in a single process. This gate widens
all three axes against a five-source fixture deployment:

  * ``records`` / ``docs`` — bootstrap-authored record sources; the records
    fixture carries ``command``/``client`` keys top-level AND nested (the
    #822 escape shape; the forbidden-key scan recurses).
  * ``doc_chunks`` — document/chunk shape (extraction module vocabulary:
    ``document`` + ``document_chunk`` + ``contains`` edges).
  * ``repo_files`` — a REAL temporary git repository driven through
    GitFilesSource (git_graph module vocabulary).
  * ``concept_records`` — alias shape (``concept`` nodes, whose subtype
    declares ``okg_alias_fields``; concepts module).

plus one deterministic enricher (DeclarativeLinker with a word_boundary
rule emitting a catalog-legal ``references`` edge document_chunk->document).

Assertion bar (issue #829): per-run admission from the durable
``okg.admission_envelopes`` records (never a CLI aggregate flag alone), a
published-not-blocked generation with positive live node/edge row deltas,
poison-pill read-back on a live node, a derived enricher edge actually
present, DLQ empty, doctor clean, an unchanged re-sync accepted through the
``authoritative_replay`` verdict (#826 guard), and a fresh-process
publish-side manifest-continuity admission of the unchanged committed
manifest (#828 admit-path guard).
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main

from .._deployment_repo import commit_deployment_tree

pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[3]

# Sentinel values for the #822 poison-pill read-back.
_COMMAND_SENTINEL = "uv run okg provision --deployment e2e --json"
_CLIENT_SENTINEL = "e2e-gate-client"
# Word-boundary token linking a chunk's text to the target document's key.
_LINK_TOKEN = "POLHUB1"


def _json_out(capsys) -> dict:
    out = capsys.readouterr()
    text = (out.out or out.err).strip().splitlines()[-1]
    return json.loads(text)


def _write_sample_sources(tmp_path: Path) -> tuple[Path, Path]:
    dep = tmp_path / "dep"
    (dep / "fixtures").mkdir(parents=True)
    brief = tmp_path / "brief.md"
    brief.write_text(
        "Track operational records and policy documents.\n"
        "Which records mention conflicting policy?\n",
        encoding="utf-8",
    )
    (dep / "fixtures" / "records.json").write_text(
        json.dumps([
            {"id": "REC-1", "summary": "Primary record",
             "description": "This record cites policy A and an issue."},
            {"id": "REC-2", "summary": "Second record",
             "description": "This record cites policy B and a date."},
        ]),
        encoding="utf-8",
    )
    (dep / "fixtures" / "corpus").mkdir()
    (dep / "fixtures" / "corpus" / "policy.md").write_text(
        "# Policy Conflict Note\n\nPolicy A and policy B disagree.",
        encoding="utf-8",
    )
    return dep, brief


def _inject_command_poison_pill(target: Path) -> str:
    """Add top-level AND nested `command`/`client` keys to a records fact.

    #822 regression shape: these keys are legitimate graph DATA; the
    FactBatch forbidden-key scan (which recurses to any depth) must admit
    them in fact data while still refusing them on control surfaces.
    Returns the poisoned node_id for the live read-back assertion.
    """
    fixture = target / "fixtures" / "records.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    node = payload["nodes"][0]
    attrs = node.setdefault("attrs", {})
    attrs["command"] = _COMMAND_SENTINEL
    attrs["client"] = _CLIENT_SENTINEL
    attrs["invocation"] = {
        "command": _COMMAND_SENTINEL,
        "clients": [_CLIENT_SENTINEL],
    }
    fixture.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return str(node["node_id"])


def _write_doc_chunk_fixture(target: Path) -> None:
    """Document/chunk-shaped source (extraction module vocabulary)."""
    doc_a = "doc:e2e:guides/policy-hub.md"
    doc_b = "doc:e2e:guides/rollout.md"
    chunk = "chunk:e2e0000000001"
    (target / "fixtures" / "doc_chunks.json").write_text(json.dumps({
        "nodes": [
            {"node_id": doc_a, "subtype": "document",
             "attrs": {"label": "Policy hub", "path": "guides/policy-hub.md",
                       "doc_key": _LINK_TOKEN, "format": "markdown"},
             "source_record_id": {"path": "guides/policy-hub.md"},
             "source_revision": {"content_sha256": "a" * 64}},
            {"node_id": doc_b, "subtype": "document",
             "attrs": {"label": "Rollout guide", "path": "guides/rollout.md",
                       "doc_key": "ROLLOUT9", "format": "markdown"},
             "source_record_id": {"path": "guides/rollout.md"},
             "source_revision": {"content_sha256": "b" * 64}},
            {"node_id": chunk, "subtype": "document_chunk",
             "attrs": {"label": "Rollout intro",
                       "text": f"The rollout depends on {_LINK_TOKEN} "
                               "for the canonical policy wording."},
             "source_record_id": {"path": "guides/rollout.md", "chunk": 0},
             "source_revision": {"content_sha256": "b" * 64}},
        ],
        "edges": [
            {"src": doc_b, "dst": chunk, "edge_type": "contains",
             "attrs": {},
             "source_record_id": {"path": "guides/rollout.md", "chunk": 0},
             "source_revision": {"content_sha256": "b" * 64}},
        ],
    }, sort_keys=True), encoding="utf-8")


def _write_concept_fixture(target: Path) -> None:
    """Alias-shaped source: `concept` nodes (okg_alias_fields subtype)."""
    (target / "fixtures" / "concept_records.json").write_text(json.dumps({
        "nodes": [
            {"node_id": "concept:policy-hub", "subtype": "concept",
             "attrs": {"label": "Policy hub", "name": "Policy hub",
                       "slug": "policy-hub",
                       "aliases": ["policy hub", _LINK_TOKEN]},
             "source_record_id": {"id": "concept:policy-hub"},
             "source_revision": {"v": 1}},
            {"node_id": "concept:rollout", "subtype": "concept",
             "attrs": {"label": "Rollout", "name": "Rollout",
                       "slug": "rollout", "aliases": ["rollout"]},
             "source_record_id": {"id": "concept:rollout"},
             "source_revision": {"v": 1}},
        ],
        "edges": [],
    }, sort_keys=True), encoding="utf-8")


def _init_git_repo(target: Path) -> Path:
    """A real temporary git repository for the git-shaped source."""
    repo = target / "gitrepo"
    (repo / "src").mkdir(parents=True)
    (repo / "README.md").write_text(
        "# e2e gate repo\n", encoding="utf-8"
    )
    (repo / "src" / "app.py").write_text(
        "print('e2e publish gate')\n", encoding="utf-8"
    )
    commit_deployment_tree(repo, "e2e fixture commit")
    return repo


def _commit_deployment_repo(target: Path) -> None:
    """Bind the synthetic deployment inputs to one exact clean revision.

    The publisher derives repository provenance from this revision and
    refuses any UNCOMMITTED deployment input, so the whole tree is added --
    including the nested `gitrepo` the git-shaped source reads, which is
    recorded as a gitlink and so pins that repository's own revision too.
    """
    commit_deployment_tree(target, "e2e deployment fixture")


def _stage_multi_source_deployment(tmp_path: Path, capsys, *, name: str) -> Path:
    """Author the five-source fixture deployment with enricher + poison pill.

    Starts from the bootstrap plan/inspect/draft loop (records + docs), then
    widens the staged tree: poison-pill attrs, a document/chunk fixture
    source, an alias-shaped concept fixture source, a real git repository
    source, and a deterministic DeclarativeLinker enricher.
    """
    import shutil

    dep, brief = _write_sample_sources(tmp_path)
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

    # Bootstrap sources run from the staged fact-shaped fixtures.
    reg = yaml.safe_load((target / "source_registry.bootstrap.yaml").read_text())
    for sid, entry in reg["sources"].items():
        entry["params"] = {
            "fixture_path": str(target / "fixtures" / f"{sid}.json"),
        }

    poisoned_node_id = _inject_command_poison_pill(target)
    (target / ".poisoned_node_id").write_text(poisoned_node_id)

    _write_doc_chunk_fixture(target)
    _write_concept_fixture(target)
    repo = _init_git_repo(target)

    fixture_profile = {
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["source_id", "record_id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "schedule": {"reconcile_interval": "24h"},
    }
    reg["sources"]["doc_chunks"] = {
        "module": "okg.substrate.library.sources.bootstrap_fixture",
        "class": "BootstrapFixtureSource",
        "ownership_id": f"{name}.doc_chunks",
        "params": {"fixture_path": str(target / "fixtures" / "doc_chunks.json")},
        **fixture_profile,
    }
    reg["sources"]["concept_records"] = {
        "module": "okg.substrate.library.sources.bootstrap_fixture",
        "class": "BootstrapFixtureSource",
        "ownership_id": f"{name}.concept_records",
        "params": {
            "fixture_path": str(target / "fixtures" / "concept_records.json"),
        },
        **fixture_profile,
    }
    reg["sources"]["repo_files"] = {
        "module": "okg.substrate.library.sources.git_graph_files",
        "class": "GitFilesSource",
        "ownership_id": f"{name}.repo_files",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["repo", "path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "schedule": {"reconcile_interval": "24h"},
        "params": {"repo": name, "repo_root": str(repo)},
    }
    (target / "source_registry.yaml").write_text(yaml.safe_dump(reg))

    # Deterministic enricher: word_boundary rule linking a chunk's text to
    # the document whose doc_key token appears in it, emitted as the
    # catalog-legal `references` narrowing (document_chunk -> document).
    (target / "cross_links.yaml").write_text(yaml.safe_dump({
        "rules": [{
            "id": "chunk_text_references_document_key",
            "src": {"subtype": "document_chunk", "field": "text"},
            "dst": {"subtype": "document", "field": "doc_key"},
            "matcher": "word_boundary",
            "edge": "references",
            "bidirectional": False,
        }],
    }))

    manifest = yaml.safe_load((target / "deployment.yaml").read_text())
    modules = list(manifest.get("modules") or [])
    if "concepts" not in modules:
        modules.append("concepts")
    manifest["modules"] = modules
    manifest["enrichers"] = [{
        "class": "okg.substrate.library.linkers.declarative.DeclarativeLinker",
        "config": {"rules_files": [str(target / "cross_links.yaml")]},
    }]
    (target / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    _commit_deployment_repo(target)
    return target


def _claim_and_provision(
    target: Path, dsn: str, capsys, *, name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> dict:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", dsn, "--force",
    ]) == 0
    capsys.readouterr()
    rc = main([
        "provision", "--deployment", name, "--dsn", dsn, "--json",
    ])
    out = _json_out(capsys)
    assert rc == 0, out
    return out


def _admission_rows(dsn: str, name: str) -> list[dict]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source, run_id::text, status,
                   verdict #>> '{producer_policy,reason}',
                   verdict #>> '{producer_policy,terminal_state_authorized}',
                   verdict #>> '{producer_policy,status}'
              FROM okg.admission_envelopes
             WHERE deployment_name = %s AND source IS NOT NULL
             ORDER BY created_at
            """,
            (name,),
        )
        return [
            {
                "source": row[0], "run_id": row[1], "status": row[2],
                "reason": row[3], "terminal_state_authorized": row[4],
                "policy_status": row[5],
            }
            for row in cur.fetchall()
        ]


def _live_rows(dsn: str, query: str, params: dict | None = None) -> list[dict]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return store.checkout().query(query, params or {})


@pytest.mark.postgres
def test_multi_source_provision_publishes_live_nodes(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """Full cycle: five-source provision publishes live rows, admits every
    source run, keeps the poison-pill attrs on a live node, emits the
    deterministic enricher edge, and leaves DLQ + doctor clean."""
    name = f"e2ea{tmp_path.name.lower().replace('-', '')[-8:]}"
    target = _stage_multi_source_deployment(tmp_path, capsys, name=name)
    out = _claim_and_provision(
        target, clean_runtime, capsys, name=name, monkeypatch=monkeypatch,
    )

    assert out["ok"] is True
    proof = out["publish_proof"]
    assert proof["ok"] is True
    assert proof["status"] == "published"
    assert proof["nodes_added"] > 0
    assert proof["edges_added"] > 0
    assert proof["dlq_count"] == 0

    # Every source ran and was admitted — from the durable per-run records,
    # not the CLI aggregate. All five sources must be present.
    rows = _admission_rows(clean_runtime, name)
    sources_seen = {row["source"] for row in rows}
    expected = {
        f"{name}.records", f"{name}.docs", f"{name}.doc_chunks",
        f"{name}.concept_records", f"{name}.repo_files",
    }
    missing = {
        source for source in expected
        if not any(source in seen for seen in sources_seen)
    }
    assert not missing, (missing, sources_seen)
    rejected = [row for row in rows if row["status"] == "rejected"]
    assert rejected == [], rejected

    # #822 read-back: the command/client poison pill survived to a LIVE node.
    poisoned = (target / ".poisoned_node_id").read_text().strip()
    live = _live_rows(
        clean_runtime,
        "SELECT node_id, attrs FROM okg.graph_nodes WHERE node_id = :nid",
        {"nid": poisoned},
    )
    assert live, f"poisoned node {poisoned} not live"
    attrs = live[0]["attrs"]
    if isinstance(attrs, str):
        attrs = json.loads(attrs)
    assert attrs.get("command") == _COMMAND_SENTINEL, attrs
    assert attrs.get("client") == _CLIENT_SENTINEL, attrs
    assert attrs.get("invocation", {}).get("command") == _COMMAND_SENTINEL, attrs

    # The deterministic enricher actually emitted its derived edge — a rule
    # whose subtypes were silently skipped would leave this empty.
    derived = _live_rows(
        clean_runtime,
        """
        SELECT src, dst FROM okg.graph_edges
         WHERE edge_type = 'references'
           AND provenance = 'derived_deterministic'
        """,
    )
    assert derived, "DeclarativeLinker emitted no derived references edge"

    # Distinct shapes actually live: chunk, git file, concept.
    for subtype in ("document_chunk", "source_file", "concept"):
        rows = _live_rows(
            clean_runtime,
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE subtype = :st",
            {"st": subtype},
        )
        assert rows[0]["n"] > 0, f"no live {subtype} nodes"

    # Doctor clean against the provisioned database.
    rc = main(["--json", "doctor", "--dsn", clean_runtime])
    doctor = _json_out(capsys)
    assert rc == 0, doctor


@pytest.mark.postgres
def test_unchanged_resync_is_accepted_idempotent_noop(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """#826 guard: an unchanged reconcile re-sync is ACCEPTED, with at least
    one run classified by the authoritative_replay verdict itself (non-empty
    record set, zero appended output) — no_source_work / unchanged skips do
    not count, and nothing is rejected."""
    name = f"e2eb{tmp_path.name.lower().replace('-', '')[-8:]}"
    target = _stage_multi_source_deployment(tmp_path, capsys, name=name)
    _claim_and_provision(
        target, clean_runtime, capsys, name=name, monkeypatch=monkeypatch,
    )
    first_wave = {row["run_id"] for row in _admission_rows(clean_runtime, name)}

    # Defeat the change-probe short-circuit: with record-set authority
    # stamped by the first sync, the probe skip would classify the unchanged
    # re-sync as no_change before the authoritative_replay verdict ever ran.
    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    rc = main([
        "--json", "ingest", "--deployment", name, "--dsn", clean_runtime,
        "--mode", "reconcile", "--inline",
    ])
    out = _json_out(capsys)
    assert rc == 0, out

    second_wave = [
        row for row in _admission_rows(clean_runtime, name)
        if row["run_id"] not in first_wave
    ]
    assert second_wave, "re-sync produced no admission envelopes"
    rejected = [row for row in second_wave if row["status"] == "rejected"]
    assert rejected == [], rejected
    replays = [
        row for row in second_wave
        if row["reason"] == "authoritative_replay"
        and row["terminal_state_authorized"] == "true"
    ]
    assert replays, second_wave


@pytest.mark.postgres
def test_armed_continuity_admits_unchanged_manifest(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """#828 admit-path guard: with the ownership row armed at claim time, a
    FRESH-process publish-side resolution of the byte-identical committed
    manifest (by-name resolution under OKG_DEPLOYMENTS_DIR, env-interpolated
    manifest values) admits instead of refusing.

    Covered asymmetry axes: process boundary, working directory,
    path-vs-name deployment resolution, env-interpolated manifest values.
    #828's true trigger is undiagnosed; this pins the admit path across
    those axes."""
    name = f"e2ec{tmp_path.name.lower().replace('-', '')[-8:]}"
    target = _stage_multi_source_deployment(tmp_path, capsys, name=name)

    # The env-interpolation axis must actually be present in the committed
    # manifest (the #828 cooper shape) — if bootstrap ever stops writing
    # `${...}` values, this guard's claimed coverage would silently vanish.
    manifest_text = (target / "deployment.yaml").read_text(encoding="utf-8")
    assert "${" in manifest_text, (
        "fixture manifest carries no env-interpolated value; the #828 "
        "asymmetry axis is gone"
    )

    # Claim in subprocess A: relative-path deployment resolution from the
    # deployments root as working directory.
    env = {
        **os.environ,
        "OKG_DSN": clean_runtime,
        "PYTHONPATH": str(_REPO_ROOT / "src"),
    }
    env.pop("OKG_DEPLOYMENTS_DIR", None)
    claim = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "catalog", "ownership", "claim",
         "--deployment", name, "--dsn", clean_runtime, "--force"],
        cwd=target.parent, env=env,
        capture_output=True, text=True, timeout=600,
    )
    assert claim.returncode == 0, (claim.stdout, claim.stderr)

    # The ownership row is ARMED: a non-null manifest fingerprint recorded.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT manifest_fingerprint FROM okg.deployment_write_ownership "
            " WHERE deployment_name = %s",
            (name,),
        )
        row = cur.fetchone()
    assert row is not None and row[0], (
        "ownership row is not armed with a manifest fingerprint"
    )
    armed_fp = row[0]

    # Provision in THIS process (a third resolution path; publishes gen 1).
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    rc = main([
        "provision", "--deployment", name, "--dsn", clean_runtime, "--json",
    ])
    out = _json_out(capsys)
    assert rc == 0, out

    # Publish-side resolution in FRESH subprocess B: by-name resolution
    # under worker-style env. Publisher construction runs
    # resolve_catalog_for_publish -> _enforce_manifest_continuity; a
    # claim-side/publish-side fingerprint divergence for this unchanged
    # committed manifest refuses here and fails the gate.
    env_b = {
        **env,
        "OKG_DEPLOYMENTS_DIR": str(target.parent),
    }
    run_once = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "--json", "run", "--once", "--inline",
         "--deployment", name, "--dsn", clean_runtime, "--apply"],
        cwd=tmp_path, env=env_b,
        capture_output=True, text=True, timeout=600,
    )
    combined = run_once.stdout + run_once.stderr
    assert run_once.returncode == 0, combined
    assert "catalog_ownership_mismatch" not in combined, combined
    assert "publish refused" not in combined, combined

    # The armed fingerprint is unchanged — nothing silently re-stamped a
    # divergent identity to make the admit pass.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT manifest_fingerprint FROM okg.deployment_write_ownership "
            " WHERE deployment_name = %s",
            (name,),
        )
        row = cur.fetchone()
    assert row is not None and row[0] == armed_fp


# ---------------------------------------------------------------------------
# Publishing from a deployment directory that is NOT a Git working tree (#1955)
# ---------------------------------------------------------------------------
#
# `okg install` writes new deployments under the documented user-state root
# (`~/.local/share/okg/deployments`), which is never a Git checkout. Before
# #1955 the first publish from such a deployment always refused with
# `deployment release authority is unavailable`: the publisher derived
# repository provenance by running `git rev-parse --show-toplevel` on the
# deployment directory.
#
# The two tests below pin both halves of the fix:
#
#   * Without the declaration, a non-Git deployment still refuses exactly as
#     it did before. The escape hatch is never an automatic fallback that
#     triggers because Git happens to be absent.
#   * With `release.kind: standalone` declared in `deployment.yaml`, the same
#     deployment provisions and publishes, and the published generation
#     records `repository_provenance.status == "unavailable"` -- the same
#     shape the packaged-release branch already records -- so a reader can
#     tell that no repository authority backed this generation.
#
# They live in this file because the `e2e-publish-gate` lane step is their
# per-PR owner (tests/qa_lanes.yaml); they are publish-gate proofs and they
# reuse this file's fixture helpers directly.
#
# Deliberately, and unlike every other fixture here, they stage the deployment
# under `tmp_path` and never call `commit_deployment_tree`. The absence of a
# repository is the condition under test.


_STANDALONE_RELEASE = {"channel": "dev", "kind": "standalone"}


def _stage_uncommitted_deployment(
    tmp_path: Path,
    capsys,
    *,
    name: str,
    standalone: bool,
) -> Path:
    """Author a small bootstrap fixture deployment outside any Git repository.

    ``standalone`` selects whether ``deployment.yaml`` carries the explicit
    ``release.kind: standalone`` declaration. Everything else is identical, so
    the two tests differ only in that one manifest key.
    """
    import shutil

    dep, brief = _write_sample_sources(tmp_path)
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
    target = tmp_path / "deployments" / name
    shutil.copytree(staged, target)

    registry_path = target / "source_registry.bootstrap.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    for source_id, entry in registry["sources"].items():
        entry["params"] = {
            "fixture_path": str(target / "fixtures" / f"{source_id}.json"),
        }
    (target / "source_registry.yaml").write_text(
        yaml.safe_dump(registry), encoding="utf-8",
    )

    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["release"] = (
        dict(_STANDALONE_RELEASE) if standalone else {"channel": "dev"}
    )
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")

    # No commit_deployment_tree() call: the point of these tests is a
    # deployment directory that is not inside any Git repository.
    assert not (target / ".git").exists()
    return target


def _claim_uncommitted(target: Path, dsn: str, capsys, monkeypatch) -> None:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", dsn, "--force",
    ]) == 0
    capsys.readouterr()


def _outside_git_name(tmp_path: Path) -> str:
    return f"e2epog{tmp_path.name.lower().replace('-', '')[-7:]}"


@pytest.mark.postgres
def test_undeclared_non_git_deployment_still_refuses_to_publish(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The refusal is unchanged when nothing declares the standalone mode.

    This is the guard on the fix: repository provenance must never be waived
    just because ``git rev-parse`` failed. Only an explicit declaration in the
    deployment waives it.
    """
    name = _outside_git_name(tmp_path)
    target = _stage_uncommitted_deployment(
        tmp_path, capsys, name=name, standalone=False,
    )
    _claim_uncommitted(target, clean_runtime, capsys, monkeypatch)

    rc = main(["provision", "--deployment", name, "--dsn", clean_runtime, "--json"])
    out = _json_out(capsys)

    assert rc != 0, out
    proof = out["publish_proof"]
    assert proof["ok"] is False, proof
    assert proof["publish_error"] == "deployment release authority is unavailable", proof
    # Pin the CAUSE, not just the message: this must be the missing-repository
    # refusal, not some other deployment-authority failure wearing the same
    # sentence.
    assert proof["publish_error_data"]["operation"] == "resolve_deployment_repository", proof
    assert proof["publish_error_data"]["authority_role"] == "deployment", proof


@pytest.mark.postgres
def test_declared_standalone_deployment_publishes_outside_git(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """A declared standalone deployment publishes with provenance unavailable."""
    name = _outside_git_name(tmp_path)
    target = _stage_uncommitted_deployment(
        tmp_path, capsys, name=name, standalone=True,
    )
    _claim_uncommitted(target, clean_runtime, capsys, monkeypatch)

    rc = main(["provision", "--deployment", name, "--dsn", clean_runtime, "--json"])
    out = _json_out(capsys)
    assert rc == 0, out

    proof = out["publish_proof"]
    assert proof["ok"] is True, proof
    assert proof["status"] == "published", proof
    assert proof["nodes_added"] > 0, proof
    # Nothing was diverted to the failure queue to get this publish through.
    assert proof["dlq_count"] == 0, proof

    # The generation records that no repository authority backed it, in the
    # same shape the packaged-release branch records.
    generation_id = proof["generation_id"]
    assert main([
        "--json", "generation", "describe", str(generation_id),
        "--dsn", clean_runtime,
    ]) == 0
    described = _json_out(capsys)
    provenance = described["repository_provenance"]
    assert provenance["status"] == "unavailable", described
    assert provenance["core_revision"] is None, described
    assert provenance["deployment_revision"] is None, described
    assert provenance["deployment_tree_sha"] is None, described
    # A standalone generation is not a packaged one: the reader must not be
    # able to confuse the two even though both record provenance as
    # unavailable.
    assert "packaged_release" not in described, described
    assert described["status"] == "published", described

    # The published rows are actually readable from the live graph.
    live = _live_rows(clean_runtime, "SELECT count(*) AS n FROM okg.graph_nodes")
    assert live[0]["n"] > 0, live

    # The deployment is healthy after the standalone publish.
    assert main(["--json", "doctor", "--dsn", clean_runtime]) == 0


# ---------------------------------------------------------------------------
# Serving a standalone deployment: worker, MCP server, release report (#1962)
# ---------------------------------------------------------------------------
#
# #1955 (above) made a standalone deployment PUBLISH. It still could not be
# SERVED: `okg runtime worker`, `okg mcp-serve` and `okg deployment release`
# each reach `current_release_artifact` through `build_release_report`, which
# had a packaged branch and no standalone one, so all three fell through to
# `_resolve_repo_authorities` and exited with "deployment release authority is
# unavailable" -- the same refusal #1955 removed from the publish path.
#
# These rows are here, in the `e2e-publish-gate` lane file, for the same two
# reasons the #1955 rows are: that lane step is their per-PR blocking owner,
# and they reuse `_stage_uncommitted_deployment` directly.
#
# They deliberately drive real processes rather than asserting on the gate
# function. The claim being proven is "nothing downstream of the release gate
# needs Git either", and only a real `okg mcp-serve` that answers a request
# and a real `okg rt worker` that gets past startup can measure that.

_RELEASE_AUTHORITY_REFUSAL = "deployment release authority is unavailable"

# Hard caps on every wait for a child, chosen far below the lane step's 600s
# process cap. A child that hangs must fail ITS OWN row, with the child's
# output in the assertion, rather than sit until the cap and kill the whole
# lane -- which produces exit 124, no JUnit, and no indication of which of the
# lane's four files was responsible. Locally these waits resolve in ~3s and
# ~9s, so the caps are roughly an order of magnitude of headroom, not a
# throttle.
_MCP_ANSWER_TIMEOUT = 60.0
_WORKER_RUN_TIMEOUT = 90.0
_OUTPUT_TAIL_LINES = 40


def _tail(text: str) -> str:
    lines = (text or "").splitlines()
    shown = lines[-_OUTPUT_TAIL_LINES:]
    prefix = (
        f"(last {_OUTPUT_TAIL_LINES} of {len(lines)} lines)\n"
        if len(lines) > _OUTPUT_TAIL_LINES else ""
    )
    return prefix + "\n".join(shown)


def _enable_runtime(target: Path) -> None:
    """Turn the DBOS runtime on for a staged fixture deployment.

    `okg runtime worker` refuses on `runtime.enabled` BEFORE it reaches the
    release contract. Without this the worker rows would assert on the wrong
    gate and would pass even if #1962 were reverted.
    """
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    runtime = dict(manifest.get("runtime") or {})
    runtime["enabled"] = True
    manifest["runtime"] = runtime
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")


def _serve_env(target: Path, dsn: str) -> dict[str, str]:
    """Environment for a child `okg` process serving ``target``."""
    env = dict(os.environ)
    env["OKG_DEPLOYMENTS_DIR"] = str(target.parent)
    env["OKG_DSN"] = dsn
    env.pop("OKG_ENVIRONMENT_DESCRIPTOR", None)
    return env


def _reap(proc: "subprocess.Popen") -> None:
    """Terminate and reap a session-isolated child, whatever it is doing.

    `start_new_session=True` (below) is required by the #703 lint: the child
    leads its own session so it can never signal the pytest-xdist worker
    group. The cost is that signals no longer propagate from the worker
    either, so a failed assertion would otherwise leak a live
    `okg mcp-serve` or `okg runtime worker` -- and the worker holds a
    Postgres connection. Every spawn below is therefore reaped from a
    `finally`.

    The whole process GROUP is signalled, not just the leader: a worker
    starts DBOS children of its own, and killing only the leader would
    orphan them. That is safe here precisely because the group is the
    child's own, which is the property `start_new_session=True` buys.
    """
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (OSError, AttributeError):
        proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, AttributeError):
            proc.kill()
        proc.wait(timeout=15)


def _mcp_stdio_session(target: Path, name: str, dsn: str) -> tuple[int, dict, str]:
    """Start a real `okg mcp-serve` over stdio and complete one exchange.

    Returns (returncode, the `initialize` result, stderr). A server that
    answers `initialize` has passed the release gate inside its own process,
    which is what makes this a measurement rather than a re-reading of the
    gate function.
    """
    # `stdin` is a pipe this function owns and writes the request to, NOT the
    # worker's inherited fd 0 -- which under xdist is the execnet control
    # channel, and which `start_new_session=True` plus an explicit stdin
    # keeps the child away from (#703).
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "okg.substrate.cli.main", "mcp-serve",
            "--deployment", name, "--dsn", dsn, "--transport", "stdio",
        ],
        cwd=_REPO_ROOT,
        env=_serve_env(target, dsn),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    request = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "okg-e2e-1962", "version": "0"},
        },
    })
    try:
        try:
            stdout, stderr = proc.communicate(
                request + "\n", timeout=_MCP_ANSWER_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            _reap(proc)
            stdout, stderr = proc.communicate()
            raise AssertionError(
                "okg mcp-serve did not answer initialize within "
                f"{_MCP_ANSWER_TIMEOUT:.0f}s and was killed.\n"
                f"stderr:\n{_tail(stderr)}\nstdout:\n{_tail(stdout)}"
            ) from None
    finally:
        _reap(proc)
    result: dict = {}
    for line in stdout.splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if isinstance(message, dict) and message.get("id") == 1:
            result = message
            break
    return proc.returncode, result, stderr


def _run_worker_start(target: Path, name: str, dsn: str) -> tuple[int, str]:
    """Start a real `okg runtime worker` and let it run to completion.

    The stop is DRIVEN, not waited for. `okg runtime worker --operator` is the
    CLI's own bounded mode -- "suppress recurring schedules and supervised
    services, drain queued deployment work, and exit" -- and
    `--max-wait-seconds 5` caps its drain. So the child is asked to terminate
    from the moment it starts, rather than being left on the job loop (which
    never ends) and killed by a timeout.

    `_WORKER_RUN_TIMEOUT` is the backstop for that contract, not the mechanism:
    if the child has not exited by then it has failed to honour `--operator`,
    and the row says so with the child's output instead of waiting for the
    lane's process cap.

    Either way this measures the only thing #1962 is about: whether startup
    gets PAST the release gate. A release refusal exits before any of it.
    """
    # DEVNULL, not inherited: the worker must not hold the xdist control
    # channel on fd 0, and `start_new_session=True` keeps its (and DBOS's)
    # signals out of the worker group entirely (#703).
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "okg.substrate.cli.main", "--json",
            "runtime", "worker", "--deployment", name, "--dsn", dsn,
            "--operator", "--max-wait-seconds", "5",
        ],
        cwd=_REPO_ROOT,
        env=_serve_env(target, dsn),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        try:
            output = proc.communicate(timeout=_WORKER_RUN_TIMEOUT)[0]
            return proc.returncode, output
        except subprocess.TimeoutExpired:
            # NOT reported as "still running, so it got past the gates". That
            # reading turned a hang into a pass, and the row that waited for
            # it would sit until the lane's process cap. `--operator` is a
            # bounded drain-and-exit mode, so not exiting is a defect in
            # itself and fails here with the child's own output.
            _reap(proc)
            output = proc.communicate()[0]
            raise AssertionError(
                "okg runtime worker --operator did not exit within "
                f"{_WORKER_RUN_TIMEOUT:.0f}s and was killed; "
                "--operator is drain-and-exit and must terminate on its "
                f"own.\noutput:\n{_tail(output)}"
            ) from None
    finally:
        _reap(proc)


@pytest.mark.postgres
def test_declared_standalone_deployment_can_be_served_outside_git(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """#1962: publish, then actually serve the same standalone deployment."""
    name = _outside_git_name(tmp_path)
    target = _stage_uncommitted_deployment(
        tmp_path, capsys, name=name, standalone=True,
    )
    _enable_runtime(target)
    _claim_uncommitted(target, clean_runtime, capsys, monkeypatch)
    assert main([
        "provision", "--deployment", name, "--dsn", clean_runtime, "--json",
    ]) == 0
    published = _json_out(capsys)["publish_proof"]
    assert published["status"] == "published", published

    # (1) `okg deployment release` -- the report both other surfaces build.
    assert main([
        "--json", "deployment", "release", name, "--dsn", clean_runtime,
    ]) == 0
    report = _json_out(capsys)
    assert report["ok"] is True, report
    # The identity is explicit about WHY it carries no revision: a bare
    # `code_revision: None` is also what a packaged deployment reports.
    assert report["current"]["code_revision"] is None, report
    assert report["current"]["release_kind"] == "standalone", report
    assert "packaged_release" not in report["current"], report
    # Waiving the repository half waived nothing else.
    assert report["current"]["schema_checksum"], report
    assert report["current"]["catalog_fingerprint"], report

    # (2) `okg mcp-serve` -- a real child process that answers a real request.
    returncode, answer, stderr = _mcp_stdio_session(target, name, clean_runtime)
    assert _RELEASE_AUTHORITY_REFUSAL not in stderr, stderr
    assert "result" in answer, (returncode, answer, stderr)
    assert answer["result"]["serverInfo"], answer

    # (3) `okg rt worker` -- a real child process that gets past startup.
    worker_rc, worker_output = _run_worker_start(target, name, clean_runtime)
    assert _RELEASE_AUTHORITY_REFUSAL not in worker_output, worker_output
    assert "release authority" not in worker_output, worker_output
    # Not merely "did not refuse": the DBOS runtime really came up against
    # this standalone deployment and drained cleanly, so nothing downstream
    # of the release gate needs a Git checkout either.
    assert worker_rc == 0, (worker_rc, worker_output)
    assert "DBOS successfully shut down" in worker_output, worker_output

    # (4) One read of the graph these surfaces serve.
    live = _live_rows(clean_runtime, "SELECT count(*) AS n FROM okg.graph_nodes")
    assert live[0]["n"] > 0, live


@pytest.mark.postgres
def test_undeclared_non_git_deployment_still_refuses_to_serve(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The guard, on the serving half.

    Repository provenance must never be waived just because `git` failed.
    Without the declaration, every one of the three surfaces still refuses --
    otherwise #1962 would have turned an explicit escape hatch into an
    automatic fallback for any deployment that happens to sit outside Git.
    """
    name = _outside_git_name(tmp_path)
    target = _stage_uncommitted_deployment(
        tmp_path, capsys, name=name, standalone=False,
    )
    _enable_runtime(target)
    _claim_uncommitted(target, clean_runtime, capsys, monkeypatch)

    assert main([
        "--json", "deployment", "release", name, "--dsn", clean_runtime,
    ]) != 0
    refusal = _json_out(capsys)
    assert _RELEASE_AUTHORITY_REFUSAL in json.dumps(refusal), refusal

    _returncode, answer, stderr = _mcp_stdio_session(target, name, clean_runtime)
    assert _RELEASE_AUTHORITY_REFUSAL in (stderr + json.dumps(answer)), (
        answer, stderr,
    )

    worker_rc, worker_output = _run_worker_start(target, name, clean_runtime)
    assert worker_rc != 0, worker_output
    assert _RELEASE_AUTHORITY_REFUSAL in worker_output, worker_output
