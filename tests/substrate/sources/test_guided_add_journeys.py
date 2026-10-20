"""Real-publish journeys for `okg deployment add-source` (issue #1802).

Each test owns one fresh database. The flow itself shells out to
`okg deployment lint`, `okg catalog load`, `okg ingest` and `okg search`,
so what is observed here is the same path a user runs. The GitHub journey
uses a deterministic fake `gh` executable on PATH: it proves the flow,
retry, edit and removal behaviour, not GitHub authentication.
"""
from __future__ import annotations

import json
import os
import pickle
import shutil
import stat
import subprocess
import textwrap
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from okg.substrate.cli import main as cli
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.sources.guided_add import read_ledger
from tests.pytest.admission_envelope_reads import fetch_admission_envelope


REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def private_dsn(okg_dsn: str):
    """Own one fresh database; never touch the shared test DB."""
    options = conninfo_to_dict(okg_dsn)
    options.pop("dbname", None)

    def target(name: str) -> str:
        uri = "postgresql:///" + quote(name, safe="")
        if options:
            uri += "?" + urlencode(options, quote_via=quote)
        return uri

    name = "okg_test_addsource_" + uuid.uuid4().hex
    admin, dsn = target("postgres"), target(name)
    with psycopg.connect(admin, autocommit=True, connect_timeout=15) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    from okg.substrate.db import migrate as migration

    original = dict(migration.REQUIRED_EXTENSIONS)
    migration.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        migration.ensure_required_extensions(dsn)
        migration.migrate(dsn)
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        migration.REQUIRED_EXTENSIONS.clear()
        migration.REQUIRED_EXTENSIONS.update(original)
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)),
            )


@pytest.fixture
def deployment(tmp_path: Path, private_dsn: str, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("addsrc", deployments_root=root).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text(encoding="utf-8"))
    manifest["release"] = {"channel": "dev", "pin": {"code_repository": str(REPO_ROOT)}}
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8",
    )
    for arguments in (
        ["init"], ["add", "."],
        ["-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-q", "-m", "scaffold"],
    ):
        subprocess.run(["git", "-C", str(dep), *arguments], check=True, capture_output=True)
    monkeypatch.setenv("OKG_DSN", private_dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    return dep


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, dict]:
    rc = cli.main([*argv, "--json"])
    out = capsys.readouterr()
    text = out.out.strip() or out.err.strip()
    return rc, json.loads(text.splitlines()[-1]) if text else {}


def _ingest(dep: Path, name: str, *extra: str, expect_rc: int = 0) -> dict:
    proc = subprocess.run(
        ["uv", "run", "okg", "ingest", "--deployment", str(dep), "--include", name,
         "--no-predecessors", "--inline", *extra, "--json"],
        capture_output=True, text=True, cwd=str(REPO_ROOT), check=False,
    )
    assert proc.returncode == expect_rc, proc.stdout[-2000:] + proc.stderr[-2000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _live_nodes(dsn: str, generation_id: str, subtype: str) -> dict[str, dict]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph = store.checkout_generation(generation_id)
        rows = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes WHERE subtype = %(subtype)s",
            {"subtype": subtype},
        )
    return {row["node_id"]: dict(row["attrs"] or {}) for row in rows}


def _registry(dep: Path) -> dict:
    return yaml.safe_load((dep / "source_registry.yaml").read_text(encoding="utf-8"))


def _last_generation(report: dict) -> str:
    body = report.get("report") if isinstance(report.get("report"), dict) else report
    publish = body["publishes"][-1]
    assert publish["status"] == "published", publish
    return str(publish["generation_id"])


# --------------------------------------------------------------------------
# Local folder journey
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_local_folder_journey_publishes_edits_and_removals(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], record_property,
) -> None:
    folder = tmp_path / "notes"
    folder.mkdir()
    (folder / "plan.md").write_text("# Weekly plan\n\nShip the guided flow.\n")
    (folder / "retro.md").write_text("# Retro\n\nWhat went well last week.\n")
    before = (deployment / "source_registry.yaml").read_bytes()

    # 1. Preview writes nothing.
    rc, plan = _run(capsys, [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={folder}",
    ])
    assert rc == 0 and plan["ok"] is True and plan["apply"] is False
    assert {r["subtype"] for r in plan["preview"]["records"]} >= {"document"}
    assert plan["preview"]["scope"]["files_matched"] == 2
    assert plan["decisions"][0]["add"] == ["extraction"]
    assert plan["behaviour"]["removal"].startswith("A record that is missing")
    assert (deployment / "source_registry.yaml").read_bytes() == before
    assert not (deployment / "extractors.yaml").exists()

    # 2. Confirm once: save, validate, catalog, import, publish, sample.
    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={folder}", "--apply", "--yes",
    ])
    assert rc == 0, outcome
    assert outcome["status"] == "published", outcome
    assert [s["step"] for s in outcome["steps"]] == [
        "save", "validate", "catalog", "claim", "policy", "import", "publish",
        "sample",
    ]
    gen1 = outcome["generation_id"]
    assert outcome["counts"]["records_added"] >= 2
    sample = outcome["sample"]
    assert sample["subtype"] in ("document", "document_chunk")
    assert sample["source"] == "addsrc.notes"
    assert sample["origin"]["source"] == "addsrc.notes"
    assert sample["origin"]["record"]["path"] in ("plan.md", "retro.md")
    assert sample["generation_id"] == gen1
    record_property("published_generation", gen1)
    record_property("sample", json.dumps(sample))

    registry = _registry(deployment)
    entry = registry["sources"]["notes"]
    assert registry["admission_contract"] == "strict_v1"
    assert entry["enabled_by_default"] is True
    assert entry["ownership_id"] == "addsrc.notes"
    assert entry["params"]["repo_root"] == str(folder)
    assert "extraction" in yaml.safe_load(
        (deployment / "deployment.yaml").read_text(encoding="utf-8"),
    )["modules"]
    assert (deployment / "extractors.yaml").is_file()
    docs = _live_nodes(private_dsn, gen1, "document")
    assert set(docs) == {"doc:notes:plan.md", "doc:notes:retro.md"}
    sha_before = docs["doc:notes:plan.md"]["content_sha256"]

    # 3. An edit updates the same record.
    (folder / "plan.md").write_text("# Weekly plan\n\nShip the guided flow. Then rest.\n")
    gen2 = _last_generation(_ingest(deployment, "notes"))
    docs = _live_nodes(private_dsn, gen2, "document")
    assert set(docs) == {"doc:notes:plan.md", "doc:notes:retro.md"}
    assert docs["doc:notes:plan.md"]["content_sha256"] != sha_before

    # 4. An incomplete read removes nothing. For a source that already
    #    published, a healthy empty incomplete run is an accepted no-op
    #    (#2074 (a), pact s18-empty-run-keeps-prior-state): no version is
    #    published (the run reports only a skipped commit with no source
    #    work), and the last published version still carries the unseen
    #    record.
    (folder / "retro.md").unlink()
    report = _ingest(deployment, "notes", "--mode", "scope_incomplete")
    body = report.get("report") if isinstance(report.get("report"), dict) else report
    for entry in body["publishes"]:
        assert entry.get("status") == "skipped", entry
        assert entry.get("publish_skipped") is True, entry
        assert entry.get("reason") == "no_source_work", entry
    policy = body["results"][0]["admission_verdict"]["producer_policy"]
    assert policy["reason"] == "incomplete_scope_noop"
    assert set(_live_nodes(private_dsn, gen2, "document")) == {
        "doc:notes:plan.md", "doc:notes:retro.md",
    }

    # 5. A complete scan removes it.
    gen4 = _last_generation(_ingest(deployment, "notes"))
    assert set(_live_nodes(private_dsn, gen4, "document")) == {"doc:notes:plan.md"}
    record_property("removal_generation", gen4)


@pytest.mark.postgres
def test_guided_add_source_child_payload_is_plain_data(
    tmp_path: Path, deployment: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Guided add writes a strict_v1 registry, whose entries are frozen into
    read-only mapping views. The start payload of each real source child
    must still pickle, and must rebuild the strict entry in the child.

    The flow imports through an `okg ingest` subprocess, so the payload is
    captured from a second, in-process `okg ingest` of the saved source.
    """
    from okg.substrate.ingest import source_jobs
    from okg.substrate.sources.registry import source_entry_from_description

    captured: list[dict] = []
    real_job = source_jobs.SpawnedSourceJob

    class RecordingJob(real_job):
        def __init__(self, *, payload, **kwargs) -> None:
            # What the spawn start method does with the arguments.
            captured.append(pickle.loads(pickle.dumps(payload)))
            super().__init__(payload=payload, **kwargs)

    folder = tmp_path / "notes"
    folder.mkdir()
    (folder / "plan.md").write_text("# Weekly plan\n\nShip the guided flow.\n")
    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={folder}", "--apply", "--yes",
    ])
    assert rc == 0 and outcome["status"] == "published", outcome
    (folder / "plan.md").write_text("# Weekly plan\n\nShip it, then rest.\n")
    monkeypatch.setattr(source_jobs, "SpawnedSourceJob", RecordingJob)

    rc, report = _run(capsys, [
        "ingest", "--deployment", str(deployment), "--include", "notes",
        "--no-predecessors", "--inline",
    ])

    assert rc == 0, report
    _last_generation(report)
    by_name = {payload["name"]: payload for payload in captured}
    assert "notes" in by_name, sorted(by_name)
    rebuilt = source_entry_from_description(by_name["notes"]["entry"])
    assert rebuilt.admission_contract == "strict_v1"
    assert rebuilt.ownership_id == "addsrc.notes"
    assert rebuilt.raw["params"]["repo_root"] == str(folder)


@pytest.mark.postgres
def test_guided_add_publishes_when_ownership_was_claimed_before_the_flow(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], record_property,
) -> None:
    """The real operator order: the database is CLAIMED before add-source runs.

    `okg install` applies the catalog, which records a catalog-ownership claim
    bound to the deployment manifest as it was BEFORE any source existed.
    Saving a source definition rewrites `source_registry`, which is part of
    that manifest identity, so on dev the publish refused with
    `drifted_blocks=['source_registry']` and every retry re-drifted what the
    operator had just re-claimed (#2213). Every other journey in this file
    starts from an unclaimed database and therefore never saw it.

    This asserts the acceptance the loop broke: one command, no hand claim,
    a published generation with records in it.
    """
    folder = tmp_path / "labnotes"
    folder.mkdir()
    (folder / "plan.md").write_text("# Weekly plan\n\nShip the guided flow.\n")
    (folder / "retro.md").write_text("# Retro\n\nWhat went well.\n")

    # The shipped academic-researcher profile already carries the `extraction`
    # module this source needs, so the flow adds no module here either.
    # A source that DOES add one on a claimed deployment is #2229; see
    # `test_guided_add_adds_a_module_and_publishes_on_a_claimed_deployment`.
    manifest_path = deployment / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    modules = list(manifest.get("modules") or [])
    if "extraction" not in modules:
        modules.append("extraction")
    manifest["modules"] = modules
    manifest_path.write_text(
        yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8",
    )
    shutil.copyfile(
        REPO_ROOT / "src/okg/substrate/sources/guided_defaults/extractors.yaml",
        deployment / "extractors.yaml",
    )
    subprocess.run(
        ["git", "-C", str(deployment), "add", "."],
        check=True, capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(deployment), "-c", "user.name=t",
         "-c", "user.email=t@example.invalid", "commit", "-q", "-m",
         "extraction module"],
        check=True, capture_output=True,
    )

    # What `okg install` does: apply the catalog, which claims ownership.
    rc, _ = _run(capsys, [
        "catalog", "load", "--deployment", str(deployment), "--apply",
    ])
    assert rc == 0

    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "labnotes",
        "--type", "local-folder", "--setting", f"root={folder}",
        "--apply", "--yes",
    ])
    assert rc == 0, outcome
    assert outcome["status"] == "published", outcome
    # The flow re-claims the identity it wrote; no operator step in between.
    assert [s["step"] for s in outcome["steps"]] == [
        "save", "validate", "catalog", "claim", "policy", "import", "publish",
        "sample",
    ], outcome["steps"]
    assert outcome["counts"]["records_added"] >= 2, outcome["counts"]
    gen = outcome["generation_id"]
    docs = _live_nodes(private_dsn, gen, "document")
    assert set(docs) == {"doc:labnotes:plan.md", "doc:labnotes:retro.md"}
    record_property("published_generation", gen)


@pytest.mark.postgres
def test_guided_add_adds_a_module_and_publishes_on_a_claimed_deployment(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], record_property,
) -> None:
    """#2229: a source that needs a module the deployment does not ship yet,
    added to a deployment whose catalog ownership is already claimed.

    The flow saves the definition and adds the module (changing both the
    schema and the manifest), loads the catalog, and only then re-claims.
    Before #2229 the schema change made this writer look foreign and the
    catalog load was refused; a write-gate manifest-drift refusal later
    refused the same load. One command must publish end to end.
    """
    folder = tmp_path / "fieldnotes"
    folder.mkdir()
    (folder / "plan.md").write_text("# Weekly plan\n\nShip the guided flow.\n")
    (folder / "retro.md").write_text("# Retro\n\nWhat went well.\n")
    manifest_path = deployment / "deployment.yaml"
    modules_before = list(
        yaml.safe_load(manifest_path.read_text(encoding="utf-8")).get("modules")
        or []
    )
    assert "extraction" not in modules_before

    # What `okg install` does: apply the catalog, which claims ownership.
    rc, _ = _run(capsys, [
        "catalog", "load", "--deployment", str(deployment), "--apply",
    ])
    assert rc == 0
    with psycopg.connect(private_dsn, autocommit=True) as conn:
        claimed = conn.execute(
            "SELECT owner_fingerprint, schema_fingerprint "
            "FROM okg.deployment_write_ownership WHERE deployment_name = %s",
            ("addsrc",),
        ).fetchone()
    assert claimed is not None

    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "fieldnotes",
        "--type", "local-folder", "--setting", f"root={folder}",
        "--apply", "--yes",
    ])
    assert rc == 0, outcome
    assert outcome["status"] == "published", outcome
    assert [s["step"] for s in outcome["steps"]] == [
        "save", "validate", "catalog", "claim", "policy", "import", "publish",
        "sample",
    ], outcome["steps"]
    assert "extraction" in yaml.safe_load(
        manifest_path.read_text(encoding="utf-8"),
    )["modules"]
    assert outcome["counts"]["records_added"] >= 2, outcome["counts"]
    gen = outcome["generation_id"]
    docs = _live_nodes(private_dsn, gen, "document")
    assert set(docs) == {"doc:fieldnotes:plan.md", "doc:fieldnotes:retro.md"}

    # Same writer across the module add; only the schema moved.
    with psycopg.connect(private_dsn, autocommit=True) as conn:
        after = conn.execute(
            "SELECT owner_fingerprint, schema_fingerprint "
            "FROM okg.deployment_write_ownership WHERE deployment_name = %s",
            ("addsrc",),
        ).fetchone()
    assert after[0] == claimed[0]
    assert after[1] != claimed[1]
    record_property("published_generation", gen)


@pytest.mark.postgres
def test_guided_add_publishes_under_an_enforced_access_policy(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], record_property,
) -> None:
    """A new source must not be denied by the policy the install wrote (#2475).

    `okg install --profile workspace-graph` turns the access policy (Nomos) on
    and writes a policy that allows exactly the sources the registry had at
    install time. Every journey above runs with the policy off, so none saw
    that add-source never added the new source to it: on dev the import was
    refused with `nomos.default_deny` on every attempt until an operator ran
    `okg provision` by hand.
    """
    from okg.substrate.nomos.baseline import reconcile_deployment_baseline

    folder = tmp_path / "notes"
    folder.mkdir()
    (folder / "plan.md").write_text("# Weekly plan\n\nThe quokkaberry launch.\n")

    manifest_path = deployment / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("nomos", {})["runtime_enforcement"] = "enforce"
    # As in the claimed-ownership journey above: the installed deployment
    # already carries the module this source needs, so the catalog apply
    # the flow runs is not refused for a reason unrelated to this test.
    modules = list(manifest.get("modules") or [])
    if "extraction" not in modules:
        modules.append("extraction")
    manifest["modules"] = modules
    manifest_path.write_text(
        yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8",
    )
    shutil.copyfile(
        REPO_ROOT / "src/okg/substrate/sources/guided_defaults/extractors.yaml",
        deployment / "extractors.yaml",
    )
    for arguments in (
        ["add", "."],
        ["-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-q", "-m", "enforce the access policy"],
    ):
        subprocess.run(
            ["git", "-C", str(deployment), *arguments],
            check=True, capture_output=True,
        )

    # What `okg install` does: apply the catalog, then write the policy for
    # the sources the registry has now.
    rc, _ = _run(capsys, [
        "catalog", "load", "--deployment", str(deployment), "--apply",
    ])
    assert rc == 0
    seeded = reconcile_deployment_baseline(
        deployment=str(manifest["name"]), deployment_dir=deployment,
        dsn=private_dsn, apply=True,
    )
    assert seeded["converged"] is True, seeded
    installed_fingerprint = seeded["active_after"]["policy_fingerprint"]

    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={folder}",
        "--apply", "--yes",
    ])
    assert rc == 0, outcome
    assert outcome["status"] == "published", outcome
    assert [s["step"] for s in outcome["steps"]] == [
        "save", "validate", "catalog", "claim", "policy", "import", "publish",
        "sample",
    ], outcome["steps"]
    policy = next(s for s in outcome["steps"] if s["step"] == "policy")
    assert policy["status"] == "ok", policy
    assert policy["detail"]["mode"] == "enforce"
    assert policy["detail"]["changed"] is True
    assert policy["detail"]["policy_fingerprint"] != installed_fingerprint
    gen = outcome["generation_id"]
    assert set(_live_nodes(private_dsn, gen, "document")) == {"doc:notes:plan.md"}
    record_property("published_generation", gen)

    # The active policy is the one the saved registry calls for, so the
    # next deployment apply or provision has nothing left to change.
    after = reconcile_deployment_baseline(
        deployment=str(manifest["name"]), deployment_dir=deployment,
        dsn=private_dsn, apply=False,
    )
    assert after["converged"] is True, after
    assert (
        after["active_after"]["policy_fingerprint"]
        == policy["detail"]["policy_fingerprint"]
    )


@pytest.mark.postgres
def test_local_folder_failed_import_keeps_definition_and_retries_without_duplicates(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    folder = tmp_path / "docs"
    folder.mkdir()
    (folder / "guide.md").write_text("# Guide\n\nHow to add a source.\n")
    argv = [
        "deployment", "add-source", str(deployment), "docs",
        "--type", "local-folder", "--setting", f"root={folder}", "--apply", "--yes",
    ]
    # Break the database connection for the import step only.
    monkeypatch.setenv("OKG_DSN", private_dsn.replace("okg_test_addsource_", "okg_missing_"))
    rc, outcome = _run(capsys, argv)
    assert rc == 1 and outcome["status"] == "failed"
    failed = outcome["failed_step"]
    assert failed["step"] in ("catalog", "import")
    assert failed["cause"]
    assert outcome["retry"]
    entry = _registry(deployment)["sources"]["docs"]
    assert entry["enabled_by_default"] is False  # saved, not active
    assert read_ledger(deployment, "docs")["status"] == "failed"

    # Correct the cause; re-run the SAME command.
    monkeypatch.setenv("OKG_DSN", private_dsn)
    rc, outcome = _run(capsys, argv)
    assert rc == 0 and outcome["status"] == "published", outcome
    registry = _registry(deployment)
    assert list(registry["sources"]) == ["bootstrap-fixture", "docs"]
    assert registry["sources"]["docs"]["enabled_by_default"] is True
    docs = _live_nodes(private_dsn, outcome["generation_id"], "document")
    assert list(docs) == ["doc:docs:guide.md"]
    assert read_ledger(deployment, "docs")["status"] == "published"


@pytest.mark.postgres
def test_retry_after_a_failed_publish_binds_to_the_staged_admission(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """#2769 on a real database: the retry accepts only a publish of the
    failed attempt's OWN admission, never older records of the same source.

    1. The source publishes from folder `first` (admission 1, published).
    2. It is re-added with folder `second` while an uncommitted file makes
       the publish refuse: admission 2 is staged and unpublished, while the
       newest version still holds folder `first`'s records (stale ones).
    3. The binding says admission 2 is NOT published; admission 1 is.
    4. With the cause fixed, the same command's ingest re-drives admission 2,
       and add-source reports that publish, bound to admission 2.
    """
    from okg.substrate.sources.guided_add import admission_publication

    first = tmp_path / "first"
    first.mkdir()
    (first / "alpha.md").write_text("# Alpha\n\nThe original alpha note.\n")
    second = tmp_path / "second"
    second.mkdir()
    (second / "gamma.md").write_text("# Gamma\n\nThe replacement gamma note.\n")

    def add(folder: Path) -> tuple[int, dict]:
        return _run(capsys, [
            "deployment", "add-source", str(deployment), "notes",
            "--type", "local-folder", "--setting", f"root={folder}",
            "--apply", "--yes",
        ])

    rc, outcome = add(first)
    assert rc == 0 and outcome["status"] == "published", outcome
    gen1 = outcome["generation_id"]
    # The one source-sync envelope of the first add; the helper names the
    # intent and refuses a lookup that matches more than one row.
    with psycopg.connect(private_dsn, autocommit=True) as conn, conn.cursor() as cur:
        admission_1, status_1 = fetch_admission_envelope(
            cur, columns=["admission_id::text", "status"], intent="source_sync",
            deployment_name="addsrc", producer_id="addsrc.notes",
        )
    assert status_1 == "published"

    blocker = deployment / "uncommitted-blocker.txt"
    blocker.write_text("not committed\n")
    rc, outcome = add(second)
    assert rc == 1 and outcome["status"] == "failed", outcome
    assert outcome["failed_step"]["step"] == "publish", outcome["failed_step"]
    staged = read_ledger(deployment, "notes")["unpublished_admission_ids"]
    assert len(staged) == 1 and staged != [admission_1]

    stale = admission_publication(
        dsn=private_dsn, deployment="addsrc", admission_ids=staged,
    )
    assert stale["published"] is False, stale
    assert stale["admissions"][staged[0]]["status"] != "published"
    older = admission_publication(
        dsn=private_dsn, deployment="addsrc", admission_ids=[admission_1],
    )
    assert older == {
        "published": True, "generation_id": gen1,
        "admissions": {admission_1: {
            "status": "published", "generation_id": gen1, "branch": "default",
        }},
    }
    # The newest version still holds the first folder's records: a search
    # for them would have "proved" the retry.
    assert set(_live_nodes(private_dsn, gen1, "document")) == {"doc:notes:alpha.md"}

    blocker.unlink()
    rc, outcome = add(second)
    assert rc == 0 and outcome["status"] == "published", outcome
    publish = next(s for s in outcome["steps"] if s["step"] == "publish")
    # The re-run's own ingest re-drives the staged admission (the path
    # measured on lab-host), and add-source reports it bound to that id.
    redrive = publish["detail"]["redrive"]
    assert redrive["label"] == "staged-source-redrive"
    assert redrive["published_admission_ids"] == staged
    bound = admission_publication(
        dsn=private_dsn, deployment="addsrc", admission_ids=staged,
    )
    assert bound["published"] is True, bound
    assert bound["generation_id"] == outcome["generation_id"]
    assert set(_live_nodes(private_dsn, outcome["generation_id"], "document")) == {
        "doc:notes:gamma.md",
    }
    assert _registry(deployment)["sources"]["notes"]["enabled_by_default"] is True
    assert read_ledger(deployment, "notes")["unpublished_admission_ids"] == []


def _live_edges(dsn: str, generation_id: str, edge_type: str) -> set[tuple[str, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph = store.checkout_generation(generation_id)
        rows = graph.query(
            "SELECT src, dst FROM okg.graph_edges WHERE edge_type = %(edge_type)s",
            {"edge_type": edge_type},
        )
    return {(str(row["src"]), str(row["dst"])) for row in rows}


@pytest.mark.postgres
def test_local_folder_with_markdown_links_publishes_the_link(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    # #3006: one Markdown file linking to another made the reader emit
    # `document_chunk references document`, which the local-folder signature
    # did not declare, so admission refused the whole folder.
    from okg.substrate.sources.guided_catalog import LOCAL_FOLDER

    folder = tmp_path / "notes"
    folder.mkdir()
    (folder / "a.md").write_text(
        "# Alpha notes\n\nAlpha talks about salinity. See [the beta page](b.md).\n",
    )
    (folder / "b.md").write_text("# Beta notes\n\nBeta covers turbidity.\n")
    argv = [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={folder}", "--apply", "--yes",
    ]

    # 1. A definition saved with the pre-fix signature is refused, as on dev.
    signature = LOCAL_FOLDER.registration.output_signature
    current = list(signature["edges"])
    pre_fix = [
        edge for edge in signature["edges"] if edge["edge_type"] != "references"
    ]
    monkeypatch.setitem(signature, "edges", pre_fix)
    rc, outcome = _run(capsys, argv)
    assert rc == 1 and outcome["status"] == "failed", outcome
    failed = outcome["failed_step"]
    assert failed["step"] == "import"
    assert failed["detail"]["source_result"]["admission_status"] == "rejected"
    # The cause names the reject code instead of masking it as a secret.
    assert "producer_edge_triple_not_allowed" in failed["cause"], failed["cause"]
    assert "<redacted>" not in failed["cause"], failed["cause"]
    assert _registry(deployment)["sources"]["notes"]["enabled_by_default"] is False
    signature["edges"] = current

    # 2. Re-running the same command rewrites the saved definition with the
    #    current signature and publishes: the upgrade path for a deployment
    #    that saved the source before the fix.
    rc, outcome = _run(capsys, argv)
    assert rc == 0 and outcome["status"] == "published", outcome
    gen = outcome["generation_id"]
    record_property("published_generation", gen)
    entry = _registry(deployment)["sources"]["notes"]
    edges = entry["admission_policy"]["output_signature"]["edges"]
    assert {
        "src_subtype": "document_chunk", "edge_type": "references",
        "dst_subtype": "document",
    } in edges
    assert entry["enabled_by_default"] is True

    chunks = _live_nodes(private_dsn, gen, "document_chunk")
    links = _live_edges(private_dsn, gen, "references")
    linked = {(src, dst) for src, dst in links if dst == "doc:notes:b.md"}
    assert len(linked) == 1, links
    assert next(iter(linked))[0] in chunks
    record_property("reference_edges", json.dumps(sorted(linked)))


@pytest.mark.postgres
def test_local_folder_with_entity_extraction_publishes_mentions(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], record_property,
) -> None:
    # #3006: with entity extraction enabled in extractors.yaml, a note with a
    # TODO or a change name emits `entity_mention` records that the
    # local-folder signature must declare, or the whole folder is refused.
    template = (
        REPO_ROOT / "src" / "okg" / "substrate" / "library" / "templates"
        / "codebase-index" / "extractors.yaml"
    )
    shutil.copyfile(template, deployment / "extractors.yaml")
    subprocess.run(["git", "-C", str(deployment), "add", "extractors.yaml"],
                   check=True, capture_output=True)
    _commit_all(deployment, "enable entity extraction")
    folder = tmp_path / "todo"
    folder.mkdir()
    (folder / "note.md").write_text(
        "# Follow-ups\n\nTODO: check the fix-foo-bar change.\n",
    )
    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "todo",
        "--type", "local-folder", "--setting", f"root={folder}", "--apply", "--yes",
    ])
    assert rc == 0 and outcome["status"] == "published", outcome
    gen = outcome["generation_id"]
    record_property("published_generation", gen)
    mentions = _live_nodes(private_dsn, gen, "entity_mention")
    assert mentions, "no entity_mention published"
    chunks = _live_nodes(private_dsn, gen, "document_chunk")
    member = {
        (src, dst) for src, dst in _live_edges(private_dsn, gen, "member_of")
        if src in mentions
    }
    assert member and all(dst in chunks for _src, dst in member), member
    record_property("mentions", json.dumps(sorted(mentions)))


# --------------------------------------------------------------------------
# Authenticated service journey (GitHub through a deterministic fake `gh`)
# --------------------------------------------------------------------------


_FAKE_GH = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json, os, re, sys
    args = sys.argv[1:]
    if args[:2] == ["auth", "status"]:
        sys.exit(1 if os.environ.get("FAKE_GH_LOGGED_OUT") else 0)
    if args[0] == "api":
        counter = os.environ.get("FAKE_GH_COUNTER")
        calls = 0
        if counter:
            try:
                calls = int(open(counter).read().strip() or 0)
            except FileNotFoundError:
                calls = 0
            calls += 1
            open(counter, "w").write(str(calls))
        fail_after = os.environ.get("FAKE_GH_FAIL_AFTER")
        if fail_after and calls > int(fail_after):
            print("HTTP 502: bad gateway", file=sys.stderr)
            sys.exit(1)
        path = args[-1]
        items = json.load(open(os.environ["FAKE_GH_FIXTURE"]))
        if re.search(r"per_page=1(?!\\d)", path):
            items = items[:1]
        print(json.dumps(items))
        sys.exit(0)
    sys.exit(2)
    """
)


def _issue(number: int, title: str, *, updated: str, pull: bool = False) -> dict:
    row = {
        "number": number, "title": title, "body": f"Body of {title}",
        "state": "open", "user": {"login": "alice"},
        "html_url": f"https://github.com/acme/widgets/issues/{number}",
        "created_at": "2026-09-01T00:00:00Z", "updated_at": updated,
        "closed_at": None, "labels": [{"name": "bug"}], "comments": 0,
    }
    if pull:
        row["pull_request"] = {"url": f"https://api.github.com/repos/acme/widgets/pulls/{number}"}
    return row


def _chunk_texts(dsn: str, generation_id: str, folder_name: str) -> list[str]:
    return [
        str(attrs.get("text") or "")
        for attrs in _live_nodes(dsn, generation_id, "document_chunk").values()
        if str(attrs.get("citation_label") or "").startswith(f"{folder_name}/")
    ]


def _commit_all(dep: Path, message: str) -> None:
    subprocess.run(
        ["git", "-C", str(dep), "-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-qam", message],
        check=True, capture_output=True,
    )


def _empty_extractors(dep: Path) -> None:
    """The placeholder `okg deployment bootstrap draft` wrote before #2974."""
    (dep / "extractors.yaml").write_text(
        yaml.safe_dump({"parser": [], "chunker": [], "ner": []}), encoding="utf-8",
    )
    _commit_all(dep, "pre-2974 extractors placeholder")


@pytest.mark.postgres
def test_local_folder_imported_under_an_empty_extractors_file_is_repaired(
    tmp_path: Path, deployment: Path, private_dsn: str,
    capsys: pytest.CaptureFixture[str], record_property,
) -> None:
    """#2974: a folder imported with no file readers has no searchable text.

    Adding another local-folder source, or re-running add-source for the same
    one, completes extractors.yaml; the next import of the first folder then
    reads its files again and publishes their passages.
    """
    notes = tmp_path / "notes"
    notes.mkdir()
    (notes / "storm.md").write_text("# Storm\n\nThe zephyrine gale flooded the harbor.\n")
    more = tmp_path / "more"
    more.mkdir()
    (more / "tide.md").write_text("# Tide\n\nQuokkabarrel tides rose overnight.\n")

    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={notes}", "--apply", "--yes",
    ])
    assert rc == 0 and outcome["status"] == "published", outcome
    assert any("zephyrine" in t for t in _chunk_texts(private_dsn, outcome["generation_id"], "notes"))

    # The broken state: the folder is imported again with no file readers.
    _empty_extractors(deployment)
    broken = _last_generation(_ingest(deployment, "notes"))
    assert _chunk_texts(private_dsn, broken, "notes") == []
    doc = _live_nodes(private_dsn, broken, "document")["doc:notes:storm.md"]
    assert doc["parse_warnings"] == ["no_parser"]

    # Adding a second folder completes the file; the next ingest of the
    # first folder publishes its passages again.
    rc, second = _run(capsys, [
        "deployment", "add-source", str(deployment), "more",
        "--type", "local-folder", "--setting", f"root={more}", "--apply", "--yes",
    ])
    assert rc == 0 and second["status"] == "published", second
    save = next(s for s in second["steps"] if s["step"] == "save")
    assert save["detail"]["extractors_yaml_updated"]["added"]["parser"]
    sample = next(s for s in second["steps"] if s["step"] == "sample")
    assert sample["detail"]["basis"] == "body"
    assert any("quokkabarrel" in t.lower() for t in _chunk_texts(private_dsn, second["generation_id"], "more"))
    repaired = _last_generation(_ingest(deployment, "notes"))
    assert any("zephyrine" in t for t in _chunk_texts(private_dsn, repaired, "notes"))

    # Re-running add-source for the same folder repairs it as well.
    _empty_extractors(deployment)
    broken_again = _last_generation(_ingest(deployment, "notes"))
    assert _chunk_texts(private_dsn, broken_again, "notes") == []
    rc, rerun = _run(capsys, [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={notes}", "--apply", "--yes",
    ])
    assert rc == 0 and rerun["status"] == "published", rerun
    sample = next(s for s in rerun["steps"] if s["step"] == "sample")
    assert sample["detail"] == {"query": "zephyrine", "basis": "body"}
    assert any("zephyrine" in t for t in _chunk_texts(private_dsn, rerun["generation_id"], "notes"))
    record_property("repaired_generation", rerun["generation_id"])


@pytest.mark.postgres
@pytest.mark.parametrize(
    "body",
    # Each note holds only the one token, so no other word can pass for it.
    [
        "https://example.org/wombatfield",
        "kangaroo@example.org",
        "Straße",
    ],
    ids=["url", "email", "eszett"],
)
def test_sample_passes_for_notes_whose_words_the_index_keeps_whole(
    tmp_path: Path, deployment: Path, capsys: pytest.CaptureFixture[str], body: str,
) -> None:
    """Text search keeps a URL or an e-mail address as one term and keeps ß.

    A word cut out of a URL or address, or a casefolded `strasse`, is never
    found; the sample must not fail such a note's source (#2975 review).
    """
    notes = tmp_path / "notes"
    notes.mkdir()
    (notes / "note.md").write_text(f"# Note\n\n{body}\n", encoding="utf-8")
    rc, outcome = _run(capsys, [
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={notes}", "--apply", "--yes",
    ])
    assert rc == 0 and outcome["status"] == "published", outcome
    sample = next(s for s in outcome["steps"] if s["step"] == "sample")
    assert sample["status"] == "ok", sample


@pytest.fixture
def fake_gh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "gh"
    script.write_text(_FAKE_GH)
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    fixture = tmp_path / "issues.json"
    fixture.write_text(json.dumps([
        _issue(1, "Login button unresponsive", updated="2026-09-02T00:00:00Z"),
        _issue(2, "Add dark mode", updated="2026-09-03T00:00:00Z", pull=True),
        _issue(3, "Crash on export", updated="2026-09-04T00:00:00Z"),
    ]))
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    monkeypatch.setenv("FAKE_GH_FIXTURE", str(fixture))
    monkeypatch.setenv("FAKE_GH_COUNTER", str(tmp_path / "gh-calls"))
    monkeypatch.delenv("FAKE_GH_FAIL_AFTER", raising=False)
    monkeypatch.delenv("FAKE_GH_LOGGED_OUT", raising=False)
    assert shutil.which("gh") == str(script)
    return fixture


@pytest.mark.postgres
def test_github_journey_with_recovery_and_removal(
    deployment: Path, private_dsn: str, fake_gh: Path,
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    argv = [
        "deployment", "add-source", str(deployment), "widgets",
        "--type", "github", "--setting", "repository=acme/widgets",
    ]
    # Not logged in: the access step names the cause; nothing is saved.
    monkeypatch.setenv("FAKE_GH_LOGGED_OUT", "1")
    rc, plan = _run(capsys, argv)
    assert rc == 1 and plan["ok"] is False
    assert [s for s in plan["steps"] if s["status"] == "failed"][0]["step"] == "access"
    assert "widgets" not in _registry(deployment)["sources"]
    monkeypatch.delenv("FAKE_GH_LOGGED_OUT")

    # Preview reuses the gh session and shows records + posture decisions.
    rc, plan = _run(capsys, argv)
    assert rc == 0 and plan["ok"] is True, plan
    assert {r["subtype"] for r in plan["preview"]["records"]} >= {"issue"}
    kinds = {d["kind"] for d in plan["decisions"]}
    assert kinds == {"ontology_modules", "privacy_posture"}
    assert "widgets" not in _registry(deployment)["sources"]

    # The service fails during import (the preview's one call succeeds, the
    # import's call gets a 502): cause reported, definition kept disabled.
    Path(os.environ["FAKE_GH_COUNTER"]).write_text("0")
    monkeypatch.setenv("FAKE_GH_FAIL_AFTER", "1")
    rc, outcome = _run(capsys, [*argv, "--apply", "--yes"])
    assert rc == 1 and outcome["status"] == "failed", outcome
    assert outcome["failed_step"]["step"] == "import", outcome["failed_step"]
    assert "502" in outcome["failed_step"]["cause"], outcome["failed_step"]
    assert _registry(deployment)["sources"]["widgets"]["enabled_by_default"] is False
    assert read_ledger(deployment, "widgets")["status"] == "failed"
    monkeypatch.delenv("FAKE_GH_FAIL_AFTER")

    # Retry the same command: published, one entry, no duplicate records.
    rc, outcome = _run(capsys, [*argv, "--apply", "--yes"])
    assert rc == 0 and outcome["status"] == "published", outcome
    gen1 = outcome["generation_id"]
    registry = _registry(deployment)
    assert list(registry["sources"]) == ["bootstrap-fixture", "widgets"]
    entry = registry["sources"]["widgets"]
    assert entry["enabled_by_default"] is True
    assert "credential_refs" not in entry
    assert entry["source_policy"]["live_call_allowed"] is True
    manifest = yaml.safe_load((deployment / "deployment.yaml").read_text(encoding="utf-8"))
    posture = manifest["nomos"]["deployment_posture"]
    assert posture["live_call_default"] == "review"
    assert "credentialed_external" in posture["allowed_source_sensitivities"]
    assert "forge" in manifest["modules"]
    issues = _live_nodes(private_dsn, gen1, "issue")
    assert len(issues) == 2 and len(_live_nodes(private_dsn, gen1, "pull_request")) == 1
    assert outcome["sample"]["source"] == "addsrc.widgets"
    assert outcome["sample"]["origin"]["source"] == "addsrc.widgets"
    assert outcome["sample"]["origin"]["record"]
    record_property("published_generation", gen1)
    record_property("sample", json.dumps(outcome["sample"]))

    # Edit updates the same record; a complete scan removes a deleted issue.
    fake_gh.write_text(json.dumps([
        _issue(1, "Login button unresponsive on Safari", updated="2026-09-05T00:00:00Z"),
        _issue(2, "Add dark mode", updated="2026-09-03T00:00:00Z", pull=True),
    ]))
    gen2 = _last_generation(_ingest(deployment, "widgets", "--mode", "reconcile"))
    issues = _live_nodes(private_dsn, gen2, "issue")
    assert set(issues) == {next(k for k in issues if k.endswith("#1"))}
    assert "Safari" in json.dumps(issues)
    record_property("removal_generation", gen2)
