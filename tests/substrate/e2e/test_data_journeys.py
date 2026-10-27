"""Data journeys on a real deployment: stored rows and exit codes, not text.

Escaped bugs this month passed tests that checked printed text or fixtures,
or that ran only at night:

* #2308: every ingest wrote a new full version of every unchanged record
  (124 GB of duplicate history on okg-workspace). The current rows looked
  right; only the history tables grew.
* #2346: ``okg run`` exited 0 when the publish was blocked.
* #2472: a publish printed "No change" (closed by PR #2627; asserted here).
* #2502 and #2622 are open and pinned below as strict expected failures.

Each journey provisions a small real deployment against a real PostgreSQL
database (no mocks) and asserts on what an operator or a script relies on:
row-version counts in the history tables, the published generation read at
that generation, and the process exit code.

The deployment is the bootstrap fixture the ``e2e-publish-gate`` lane uses
(two record sources authored by ``okg deployment bootstrap``) plus one real
temporary git repository read by ``GitFilesSource``. That git source stamps
one repository-wide ``{tree_sha, head_sha}`` revision on every file record,
the shape that made #2308 rewrite every record on okg-workspace.

An open bug is pinned with ``xfail(strict=True, raises=KnownBug)``.
``KnownBug`` is raised only after the stored state and the output show a
known symptom, so the test fails outright if it breaks any other way (any
``AssertionError`` is a real failure). A pinned test turns red as a strict
XPASS only when every issue it names is fixed. The doctor test names two
(#2502 and #2622): with only one of them fixed, the other's codes remain,
so it still reports the known failure rather than an ``AssertionError``.
"""
from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main

from .._deployment_repo import GIT_ENV, HERMETIC_GIT, commit_deployment_tree

pytestmark = pytest.mark.postgres

# The per-record lineage envelope. It moves with the source revision, so it
# is left out when deciding whether a record's content changed (#2308).
_LINEAGE_ATTR = "nomos_source_lineage"

# Scheduled refresh: the poll interval declared on the git source, and how
# long one refresh may take to publish before the journey gives up.
_POLL_INTERVAL_S = 2
_REFRESH_TIMEOUT_S = 90.0
_WORKER_STOP_TIMEOUT_S = 60.0


class KnownBug(Exception):
    """Raised only when the stored state and output show an open issue."""


# ---------------------------------------------------------------------------
# deployment
# ---------------------------------------------------------------------------


def _json_out(capsys) -> dict:
    out = capsys.readouterr()
    text = (out.out or out.err).strip().splitlines()[-1]
    return json.loads(text)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        [*HERMETIC_GIT, *args], cwd=repo, env={**os.environ, **GIT_ENV},
        stdin=subprocess.DEVNULL, check=True, capture_output=True, text=True,
    ).stdout.strip()


def _name(prefix: str) -> str:
    """A fresh deployment name.

    Runtime rows such as the worker's acknowledgement are keyed by
    deployment and outlive the per-test reset, so a name reused by a later
    run would read an earlier run's rows.
    """
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _repo(target: Path) -> Path:
    """The git repository the ``repo_files`` source reads.

    It lives outside the deployment tree, as on a real install, so a commit
    there leaves the deployment's own revision alone.
    """
    return target.parent.parent / "repo"


def _stage_deployment(tmp_path: Path, capsys, *, name: str) -> Path:
    """Bootstrap two record sources, add a git source, commit the tree."""
    authoring = tmp_path / "authoring"
    (authoring / "fixtures" / "corpus").mkdir(parents=True)
    brief = tmp_path / "brief.md"
    brief.write_text(
        "Track operational records and policy documents.\n"
        "Which records mention conflicting policy?\n",
        encoding="utf-8",
    )
    (authoring / "fixtures" / "records.json").write_text(json.dumps([
        {"id": "REC-1", "summary": "Primary record",
         "description": "This record cites policy A and an issue."},
        {"id": "REC-2", "summary": "Second record",
         "description": "This record cites policy B and a date."},
    ]), encoding="utf-8")
    (authoring / "fixtures" / "corpus" / "policy.md").write_text(
        "# Policy Conflict Note\n\nPolicy A and policy B disagree.",
        encoding="utf-8",
    )
    for step in (
        ["plan", "--source", "records=fixtures/records.json",
         "--source", "docs=fixtures/corpus", "--brief", str(brief),
         "--name", name],
        ["inspect-source"],
        ["draft"],
    ):
        rc = main([
            "--json", "deployment", "bootstrap", step[0],
            "--deployment", str(authoring), *step[1:], "--apply",
        ])
        capsys.readouterr()
        assert rc == 0, step

    target = tmp_path / "deployments" / name
    shutil.copytree(authoring / ".okg_bootstrap" / "staged", target)
    registry = yaml.safe_load(
        (target / "source_registry.bootstrap.yaml").read_text(encoding="utf-8")
    )
    for source_id, entry in registry["sources"].items():
        entry["params"] = {
            "fixture_path": str(target / "fixtures" / f"{source_id}.json"),
        }

    repo = _repo(target)
    (repo / "src").mkdir(parents=True)
    (repo / "README.md").write_text("# journey repo\n", encoding="utf-8")
    (repo / "src" / "app.py").write_text("print('journey')\n", encoding="utf-8")
    (repo / "src" / "util.py").write_text(
        "def helper():\n    return 1\n", encoding="utf-8",
    )
    commit_deployment_tree(repo, "journey repo")
    registry["sources"]["repo_files"] = {
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
    (target / "source_registry.yaml").write_text(
        yaml.safe_dump(registry), encoding="utf-8",
    )
    # The publisher refuses uncommitted deployment inputs.
    commit_deployment_tree(target, "journey deployment")
    return target


def _claim(target: Path, dsn: str, capsys) -> None:
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", dsn, "--force",
    ]) == 0
    capsys.readouterr()


def _provision(target: Path, dsn: str, capsys) -> dict:
    rc = main(["provision", "--deployment", target.name, "--dsn", dsn, "--json"])
    out = _json_out(capsys)
    assert rc == 0, json.dumps(out, indent=1, default=str)[:6000]
    assert out["publish_proof"]["status"] == "published", out
    return out


def _claim_and_provision(
    target: Path, dsn: str, capsys, monkeypatch: pytest.MonkeyPatch,
) -> dict:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    _claim(target, dsn, capsys)
    return _provision(target, dsn, capsys)


def _ingest(name: str, dsn: str, capsys, *extra: str) -> dict:
    """``okg ingest``: read the sources and, unless told not to, publish."""
    rc = main([
        "--json", "ingest", "--deployment", name, "--dsn", dsn, "--inline",
        *extra,
    ])
    out = _json_out(capsys)
    assert rc == 0, json.dumps(out, indent=1, default=str)[:6000]
    return out


def _source_stats(ingest_out: dict, source: str) -> dict:
    for result in ingest_out["results"]:
        if result["name"] == source:
            return dict(result.get("source_stats") or {})
    raise AssertionError(f"ingest reported no result for {source}: {ingest_out}")


def _child_env(target: Path, dsn: str) -> dict[str, str]:
    env = {**os.environ, "OKG_DEPLOYMENTS_DIR": str(target.parent), "OKG_DSN": dsn}
    env.pop("OKG_ENVIRONMENT_DESCRIPTOR", None)
    return env


# One `okg` child (a publish cycle or a doctor run on this small deployment)
# takes a few seconds; 120s leaves room for a slow runner and still fails
# well inside the lane's 25-minute job limit.
_OKG_CALL_TIMEOUT_S = 120


def _okg(args: list[str], *, target: Path, dsn: str, cwd: Path) -> subprocess.CompletedProcess:
    """Run the real ``okg`` entry point in a fresh process, exit code included."""
    return subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main", *args],
        cwd=cwd, env=_child_env(target, dsn), stdin=subprocess.DEVNULL,
        capture_output=True, text=True, timeout=_OKG_CALL_TIMEOUT_S,
    )


# ---------------------------------------------------------------------------
# stored state
# ---------------------------------------------------------------------------


def _physical_table(conn, table: str) -> str:
    row = conn.execute(
        "SELECT physical_table FROM _chronos_branch_tables "
        "WHERE backend = 'interval' AND table_name = %s",
        (table,),
    ).fetchone()
    assert row is not None, f"no interval history table for {table}"
    return str(row[0])


def _history(dsn: str) -> dict:
    """Every stored row version, per history table, and the duplicates.

    ``versions`` counts all rows, closed and open. ``duplicates`` lists each
    record holding more versions than it has distinct contents (lineage
    left out): the query #2308 used to find ~30 versions per record whose
    content never changed. A correct ingest leaves it empty.
    """
    versions: dict[str, int] = {}
    duplicates: dict[str, list] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        nodes = _physical_table(conn, "okg.graph_nodes")
        edges = _physical_table(conn, "okg.graph_edges")
        queries = {
            "okg.graph_nodes": (
                "SELECT node_id, count(*), count(DISTINCT "
                "(subtype, attrs - %(lineage)s, deleted)) "
                f'FROM "{nodes}" GROUP BY node_id'
            ),
            "okg.graph_edges": (
                "SELECT edge_id::text, count(*), count(DISTINCT "
                "(src, dst, edge_type, attrs - %(lineage)s, provenance, "
                f'confidence, deleted)) FROM "{edges}" GROUP BY edge_id'
            ),
        }
        for table, sql in queries.items():
            rows = conn.execute(sql, {"lineage": _LINEAGE_ATTR}).fetchall()
            versions[table] = sum(int(row[1]) for row in rows)
            duplicates[table] = sorted(
                (str(key), int(n), int(distinct))
                for key, n, distinct in rows
                if int(n) > int(distinct)
            )
    return {"versions": versions, "duplicates": duplicates}


def _newest_node_attrs(dsn: str, node_id: str) -> dict:
    """The newest stored attributes of one node, from its history table."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        table = _physical_table(conn, "okg.graph_nodes")
        rows = conn.execute(
            f'SELECT attrs FROM "{table}" WHERE node_id = %s '
            "AND NOT deleted ORDER BY live_lo DESC LIMIT 1",
            (node_id,),
        ).fetchall()
    assert rows, f"node {node_id} was never stored"
    attrs = rows[0][0]
    return json.loads(attrs) if isinstance(attrs, str) else dict(attrs)


def _published_content(dsn: str, name: str) -> dict:
    """The latest published generation and a digest of what it holds.

    Read pinned to that generation, the way a reader sees it, so rows a
    later unpublished cycle wrote cannot hide in the digest.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published = store.list_generations(deployment_name=name)
        assert published, f"deployment {name} has no published generation"
        latest = published[-1].checkpoint_id
        view = store.checkout_generation(latest)
        nodes = view.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes ORDER BY node_id"
        )
        edges = view.query(
            "SELECT src, dst, edge_type, attrs FROM okg.graph_edges "
            "ORDER BY src, dst, edge_type"
        )
    digest = hashlib.sha256(
        json.dumps([nodes, edges], sort_keys=True, default=str).encode()
    ).hexdigest()
    return {
        "generations": len(published),
        "checkpoint": latest,
        "node_ids": sorted(str(row["node_id"]) for row in nodes),
        "edges": len(edges),
        "attrs": {str(row["node_id"]): row["attrs"] for row in nodes},
        "digest": digest,
    }


def _generation_statuses(dsn: str, name: str) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoints = store.list_generations(deployment_name=name, status=None)
    counts: dict[str, int] = {}
    for checkpoint in checkpoints:
        status = str((checkpoint.metadata or {}).get("status"))
        counts[status] = counts.get(status, 0) + 1
    return counts


def _admissions(dsn: str, name: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT source, status, coalesce(verdict ->> 'error', '') "
            "FROM okg.admission_envelopes "
            "WHERE deployment_name = %s ORDER BY created_at",
            (name,),
        ).fetchall()
    return [(str(a), str(b), str(c)) for a, b, c in rows]


# ---------------------------------------------------------------------------
# 1. an unchanged re-ingest changes nothing
# ---------------------------------------------------------------------------


def test_unchanged_reingest_writes_no_new_row_versions(
    tmp_path: Path, clean_runtime: str, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """#2308: re-reading unchanged records stores no new row version.

    Three runs after the first provision, each publishing:

    1. ``okg ingest`` over byte-identical sources.
    2. A full re-read of byte-identical sources (``--mode reconcile
       --reset-cursor``, the authoritative replay a scheduled reconcile or
       an operator repair runs): every record is emitted again.
    3. The same full re-read after the git repository gets a commit that
       changes no file. That moves the repository-wide ``{tree_sha,
       head_sha}`` revision stamped on every file record: the okg-workspace
       shape where #2308 stored ~30 versions per record.

    After steps 1 and 2 no history table gains a row and the published
    content is unchanged. After step 3 only the branch node, whose
    ``head_commit_sha`` really changed, gains a version, and no record holds
    more versions than it has distinct contents.
    """
    name = _name("jdre")
    target = _stage_deployment(tmp_path, capsys, name=name)
    _claim_and_provision(target, clean_runtime, capsys, monkeypatch)
    before = _history(clean_runtime)
    content = _published_content(clean_runtime, name)
    assert content["node_ids"] and content["edges"] > 0, content
    assert all(n > 0 for n in before["versions"].values()), before
    no_duplicates = dict.fromkeys(before["duplicates"], [])
    assert before["duplicates"] == no_duplicates, before

    # 1. Byte-identical re-ingest.
    _ingest(name, clean_runtime, capsys)
    assert _history(clean_runtime) == before
    assert _published_content(clean_runtime, name)["digest"] == content["digest"]

    # 2. Byte-identical full re-read.
    full_reread = ("--mode", "reconcile", "--reset-cursor")
    out = _ingest(name, clean_runtime, capsys, *full_reread)
    # Non-vacuity: every file record really was emitted again.
    assert _source_stats(out, "repo_files")["files_emitted"] == 3, out
    assert _history(clean_runtime) == before
    assert _published_content(clean_runtime, name)["digest"] == content["digest"]

    # 3. The repository head moves; no file changes.
    repo = _repo(target)
    branch_node = f"branch:{name}:main"
    old_head = _git(repo, "rev-parse", "HEAD")
    assert _newest_node_attrs(clean_runtime, branch_node)["head_commit_sha"] == old_head
    _git(repo, "commit", "--allow-empty", "-m", "no file changes")
    new_head = _git(repo, "rev-parse", "HEAD")

    out = _ingest(name, clean_runtime, capsys, *full_reread)
    assert _source_stats(out, "repo_files")["files_emitted"] == 3, out
    assert _newest_node_attrs(clean_runtime, branch_node)["head_commit_sha"] == new_head
    after = _history(clean_runtime)
    assert after["duplicates"] == no_duplicates, (
        "records whose content did not change gained row versions "
        f"(record, versions, distinct contents): {after['duplicates']}"
    )
    # The branch node is the one record whose content changed: one new node
    # version, and nothing else.
    expected = dict(before["versions"])
    expected["okg.graph_nodes"] += 1
    assert after["versions"] == expected, (before["versions"], after["versions"])
    moved = _published_content(clean_runtime, name)
    assert moved["generations"] > content["generations"], moved
    assert moved["attrs"][branch_node]["head_commit_sha"] == new_head


def test_unchanged_reprovision_adds_no_version(
    tmp_path: Path, clean_runtime: str, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """An unchanged ``okg provision`` adds no version.

    #2305 reported an unchanged re-provision refused on the workspace-graph
    profile. That does not reproduce on this deployment, so this test guards
    the general contract, not #2305's workspace-profile symptom: the
    re-provision is accepted, stores no new node or edge version, and leaves
    the published content unchanged. (The attribute-interval retention half of
    this test went with the interval table, PACT lane-s-storage-drop-copies.)
    """
    name = _name("jdrp")
    target = _stage_deployment(tmp_path, capsys, name=name)
    _claim_and_provision(target, clean_runtime, capsys, monkeypatch)
    _git(_repo(target), "commit", "--allow-empty", "-m", "no file changes")
    _ingest(name, clean_runtime, capsys, "--mode", "reconcile", "--reset-cursor")

    history = _history(clean_runtime)
    content = _published_content(clean_runtime, name)

    _provision(target, clean_runtime, capsys)
    reprovisioned = _history(clean_runtime)
    assert reprovisioned["duplicates"] == history["duplicates"], reprovisioned
    for table in ("okg.graph_nodes", "okg.graph_edges"):
        assert reprovisioned["versions"][table] == history["versions"][table], table
    assert _published_content(clean_runtime, name)["digest"] == content["digest"]


# ---------------------------------------------------------------------------
# 2. a scheduled refresh works
# ---------------------------------------------------------------------------


def _enable_poll_refresh(target: Path) -> None:
    """Turn on the runtime and a poll schedule for the git source."""
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["runtime"] = {**(manifest.get("runtime") or {}), "enabled": True}
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    registry_path = target / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["sources"]["repo_files"]["sync"] = {
        "watch": {
            "roots": [str(_repo(target))],
            "include_globs": ["**/*"],
            "exclude_globs": [".git/**", "**/.git/**"],
        },
        "triggers": ["poll", "manual", "reconcile"],
        "default_event_mode": "scope_complete",
        "reconcile_mode": "scope_complete",
        "poll": {"probe": "change_probe", "interval_seconds": _POLL_INTERVAL_S},
    }
    registry_path.write_text(yaml.safe_dump(registry), encoding="utf-8")


def _edit_repo_file(target: Path, text: str) -> str:
    """Commit a new ``src/app.py``; return its blob id."""
    repo = _repo(target)
    (repo / "src" / "app.py").write_text(text, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "edit app.py")
    return _git(repo, "rev-parse", "HEAD:src/app.py")


def _wait_until_published(
    dsn: str, name: str, blob: str, worker: subprocess.Popen,
) -> float:
    """Seconds until the published generation holds ``blob`` for app.py."""
    node_id = f"file:{name}:src/app.py"
    started = time.monotonic()
    while time.monotonic() - started < _REFRESH_TIMEOUT_S:
        attrs = _published_content(dsn, name)["attrs"].get(node_id) or {}
        if attrs.get("git_blob_sha") == blob:
            return time.monotonic() - started
        assert worker.poll() is None, f"the worker exited early: {worker.returncode}"
        time.sleep(1.0)
    raise AssertionError(
        f"no poll published blob {blob} of src/app.py within "
        f"{_REFRESH_TIMEOUT_S:.0f}s"
    )


def _stop(worker: subprocess.Popen) -> None:
    """Stop the worker and its children (it leads its own process group)."""
    if worker.poll() is not None:
        return
    os.killpg(os.getpgid(worker.pid), signal.SIGTERM)
    try:
        worker.wait(timeout=_WORKER_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(worker.pid), signal.SIGKILL)
        worker.wait(timeout=15)
        raise AssertionError(
            f"the worker ignored SIGTERM for {_WORKER_STOP_TIMEOUT_S:.0f}s"
        ) from None


_WORKER_LOG_TAIL_LINES = 60


@contextlib.contextmanager
def _worker_log_on_failure(tmp_path: Path) -> Iterator[None]:
    """Append the tail of the worker's log to any assertion failure.

    A CI failure then shows what the worker was doing, not only that a
    poll never published. ``KnownBug`` passes through untouched.
    """
    try:
        yield
    except AssertionError as exc:
        log = tmp_path / "worker.log"
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines() if log.exists() else []
        tail = "\n".join(lines[-_WORKER_LOG_TAIL_LINES:]) or "(no worker log)"
        raise AssertionError(
            f"{exc}\n--- last {_WORKER_LOG_TAIL_LINES} lines of worker.log ---\n{tail}"
        ) from exc


def _run_worker_through_edits(
    tmp_path: Path, dsn: str, capsys, monkeypatch: pytest.MonkeyPatch,
    *, name: str, edits: int,
) -> tuple[Path, int]:
    """Provision with a scheduled refresh, run a real worker, stop it.

    With the worker running, ``edits`` successive commits to the repository
    must each be published by a poll. Returns the deployment directory and
    the published-generation count before the worker started.
    """
    target = _stage_deployment(tmp_path, capsys, name=name)
    _enable_poll_refresh(target)
    commit_deployment_tree(target, "enable a scheduled refresh")
    _claim_and_provision(target, dsn, capsys, monkeypatch)
    generations = _published_content(dsn, name)["generations"]

    worker_log = tmp_path / "worker.log"
    with worker_log.open("w", encoding="utf-8") as sink:
        worker = subprocess.Popen(
            [sys.executable, "-m", "okg.substrate.cli.main", "runtime", "worker",
             "--deployment", name, "--dsn", dsn],
            cwd=tmp_path, env=_child_env(target, dsn),
            stdin=subprocess.DEVNULL, stdout=sink, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            for index in range(edits):
                blob = _edit_repo_file(target, f"print('refresh {index}')\n")
                _wait_until_published(dsn, name, blob, worker)
        finally:
            _stop(worker)
    return target, generations


def test_scheduled_refresh_publishes_each_poll(
    tmp_path: Path, clean_runtime: str, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """A real ``okg runtime worker`` refreshes on schedule (#2502 item 1).

    The git source declares a poll every two seconds and the runtime is on.
    With the worker running, a committed edit to the repository is
    published by a poll, and so is a second edit made after that: a later
    poll is accepted too. No admission is rejected, and the worker stops on
    SIGTERM.
    """
    name = _name("jdrf")
    with _worker_log_on_failure(tmp_path):
        _target, generations = _run_worker_through_edits(
            tmp_path, clean_runtime, capsys, monkeypatch, name=name, edits=2,
        )
        assert _published_content(clean_runtime, name)["generations"] >= generations + 2
        rejected = [a for a in _admissions(clean_runtime, name) if a[1] == "rejected"]
        assert rejected == [], rejected


# Every doctor error the worker journey is known to leave today, with the
# open issue that owns it. `deployment_apply_not_converged` is keyed by the
# reason of each stage it says is needed; any other stage reason, like any
# other code, is not known and fails the test outright.
_KNOWN_DOCTOR_CODES: dict[str, str] = {
    "deployment_doctor_never_ran": "#2622",
    "publisher_workflow_never_ran": "#2622",
}
_KNOWN_APPLY_REASONS: dict[str, str] = {
    "worker_ack_absent": "#2622",
    "worker_ack_fingerprint_drift": "#2502",
    # A hand-started worker's process identity never matches the supervisor
    # unit apply would install; doctor now names that case (#2502).
    "worker_ack_identity_mismatch": "#2502",
    # The source_supervision stage: reported before any worker runs and,
    # on some fresh-database runs, still after it stops (#2622).
    "source_added_or_changed": "#2622",
}


def _raise_for_red_doctor(errors: list[dict]) -> None:
    """Raise KnownBug only when every red code maps to an open issue.

    Any unknown code or stage reason raises AssertionError instead, which
    ``xfail(raises=KnownBug)`` reports as a plain failure.
    """
    known: list[str] = []
    unknown: list[str] = []
    for error in errors:
        code = str(error.get("code"))
        if code == "deployment_apply_not_converged":
            actions = (error.get("details") or {}).get("actions") or []
            reasons = [str(a.get("reason")) for a in actions if a.get("needed")]
            if not reasons:
                unknown.append(f"{code}(no needed stage)")
            for reason in reasons:
                issue = _KNOWN_APPLY_REASONS.get(reason)
                bucket = known if issue else unknown
                bucket.append(f"{code}:{reason}" + (f" {issue}" if issue else ""))
        elif code in _KNOWN_DOCTOR_CODES:
            known.append(f"{code} {_KNOWN_DOCTOR_CODES[code]}")
        else:
            unknown.append(code)
    if unknown or not known:
        raise AssertionError(
            f"doctor red after the worker stopped; unknown: {sorted(unknown)}; "
            f"known: {sorted(known)}"
        )
    raise KnownBug(f"doctor red after the worker stopped: {sorted(known)}")


def test_red_doctor_classifier_accepts_only_known_codes() -> None:
    """The doctor pin cannot absorb a new failure: unknown codes fail."""
    ack_absent = {
        "code": "deployment_apply_not_converged",
        "details": {"actions": [
            {"needed": False, "reason": "converged", "stage": "catalog_apply"},
            {"needed": True, "reason": "worker_ack_absent", "stage": "worker_reload"},
        ]},
    }
    drift = {
        "code": "deployment_apply_not_converged",
        "details": {"actions": [{
            "needed": True, "reason": "worker_ack_fingerprint_drift",
            "stage": "worker_reload",
        }]},
    }
    never_ran = [
        {"code": "deployment_doctor_never_ran"},
        {"code": "publisher_workflow_never_ran"},
    ]
    source_changed = {
        "code": "deployment_apply_not_converged",
        "details": {"actions": [
            {"needed": True, "reason": "source_added_or_changed",
             "stage": "source_supervision"},
            {"needed": True, "reason": "worker_ack_absent", "stage": "worker_reload"},
        ]},
    }
    for known in (
        [ack_absent, *never_ran], [drift, never_ran[0]], [source_changed, *never_ran],
    ):
        with pytest.raises(KnownBug):
            _raise_for_red_doctor(known)
    other_stage = {
        "code": "deployment_apply_not_converged",
        "details": {"actions": [
            {"needed": True, "reason": "catalog_drifted", "stage": "catalog_apply"},
            {"needed": True, "reason": "worker_ack_absent", "stage": "worker_reload"},
        ]},
    }
    for bad in (
        [ack_absent, {"code": "queue_depth_probe_failed"}],
        [other_stage, *never_ran],
        [{"code": "deployment_apply_not_converged", "details": {"actions": []}}],
        [],
    ):
        with pytest.raises(AssertionError):
            _raise_for_red_doctor(bad)


@pytest.mark.xfail(
    strict=True, raises=KnownBug,
    reason="#2502 and #2622: okg doctor stays red after a scheduled-refresh "
    "worker stops (worker_ack_fingerprint_drift or worker_ack_absent, "
    "and never-ran workflow checks)",
)
def test_doctor_is_ok_after_the_refresh_worker_stops(
    tmp_path: Path, clean_runtime: str, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """#2502 closing condition: stopping the worker leaves doctor ok.

    One poll publishes one edit, the worker stops, and ``okg doctor`` must
    exit 0. Today it exits 1 with codes owned by #2502 and #2622; any other
    code fails this test outright. It turns red as a strict XPASS only when
    both issues are fixed: with #2502 alone fixed, the #2622 codes remain
    and it still reports the known failure.
    """
    name = _name("jddr")
    with _worker_log_on_failure(tmp_path):
        target, _generations = _run_worker_through_edits(
            tmp_path, clean_runtime, capsys, monkeypatch, name=name, edits=1,
        )
        doctor = _okg(
            ["--json", "doctor", "--dsn", clean_runtime, "--deployment", name],
            target=target, dsn=clean_runtime, cwd=tmp_path,
        )
        if doctor.returncode != 0:
            report = json.loads(doctor.stdout.strip().splitlines()[-1])
            _raise_for_red_doctor([dict(e) for e in report.get("errors") or []])



# ---------------------------------------------------------------------------
# 3. the exit code matches the outcome
# ---------------------------------------------------------------------------


def _add_record(target: Path) -> str:
    """Add one record to the records fixture; return its node id."""
    fixture = target / "fixtures" / "records.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    extra = copy.deepcopy(payload["nodes"][0])
    extra["node_id"] = f"{extra['node_id']}-added"
    extra["source_record_id"] = {"record": "added"}
    payload["nodes"].append(extra)
    fixture.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return str(extra["node_id"])


def _set_blocking_invariant(target: Path, dsn: str, capsys, *, present: bool) -> None:
    """Load, or remove, an error-severity invariant that always has a violator."""
    path = target / "invariants" / "journey_block.yaml"
    if present:
        path.parent.mkdir(exist_ok=True)
        path.write_text(yaml.safe_dump({
            "invariants": [{
                "name": "journey_always_violated",
                "severity": "error",
                "scope": "cross",
                "timeout_ms": 5000,
                "description": "Always returns one row, so every publish blocks.",
                "sql": "SELECT 1 AS violator",
            }],
        }), encoding="utf-8")
    else:
        path.unlink()
    commit_deployment_tree(target, f"blocking invariant present={present}")
    _claim(target, dsn, capsys)
    rc = main([
        "--json", "catalog", "load", "--deployment", target.name,
        "--dsn", dsn, "--apply",
    ])
    out = _json_out(capsys)
    assert rc == 0, out


def test_run_exit_code_and_summary_match_the_publish_outcome(
    tmp_path: Path, clean_runtime: str, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """#2346 and #2472: ``okg run --once --apply`` reports what happened.

    1. Nothing new: exit 0.
    2. A new record is read (``okg ingest --no-publish``) and an
       error-severity invariant that always has a violator is loaded. The
       cycle is blocked: the process exits 3 (#2346 made it exit 0), a
       blocked generation is stored, and the published generation does not
       move.
    3. The invariant is removed. The next cycle publishes the waiting
       record: exit 0, one more published generation, and it holds the
       record. Its summary must name that new version (#2472, fixed by
       PR #2627; it used to print "No change").
    """
    name = _name("jdex")
    target = _stage_deployment(tmp_path, capsys, name=name)
    _claim_and_provision(target, clean_runtime, capsys, monkeypatch)
    run_once = [
        "run", "--once", "--inline", "--deployment", name,
        "--dsn", clean_runtime, "--apply",
    ]

    # 1. Nothing new.
    idle = _okg(run_once, target=target, dsn=clean_runtime, cwd=tmp_path)
    assert idle.returncode == 0, (idle.stdout, idle.stderr)

    # 2. Blocked.
    published = _published_content(clean_runtime, name)
    added = _add_record(target)
    commit_deployment_tree(target, "add a record")
    _ingest(name, clean_runtime, capsys, "--no-publish")
    _set_blocking_invariant(target, clean_runtime, capsys, present=True)
    blocked = _okg(run_once, target=target, dsn=clean_runtime, cwd=tmp_path)
    assert _generation_statuses(clean_runtime, name).get("blocked") == 1, (
        blocked.stdout, blocked.stderr,
    )
    assert _published_content(clean_runtime, name) == published
    assert blocked.returncode == 3, (
        f"okg run exited {blocked.returncode} although the publish was "
        f"blocked:\n{blocked.stdout}\n{blocked.stderr}"
    )

    # 3. Unblocked: the waiting record is published.
    _set_blocking_invariant(target, clean_runtime, capsys, present=False)
    unblocked = _okg(run_once, target=target, dsn=clean_runtime, cwd=tmp_path)
    assert unblocked.returncode == 0, (unblocked.stdout, unblocked.stderr)
    now = _published_content(clean_runtime, name)
    assert now["generations"] == published["generations"] + 1, now
    assert set(now["node_ids"]) - set(published["node_ids"]) == {added}
    first_line = (unblocked.stdout.strip().splitlines() or [""])[0]
    assert first_line == f"Published graph version {now['checkpoint']}", unblocked.stdout
