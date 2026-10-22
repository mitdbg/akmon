"""Init cannot report success over a failed catalog load or first publish.

PACT ``fix-init-scaffold-provability`` / ``req.init.fail-loud-first-publish``
(issue #636 residual): before this change, cli/init.py recorded
catalog-load/first-publish failures as a non-fatal ``first_publish_failed``
warning and exited 0 — success over an empty graph. Now every bring-up
stage failure exits nonzero, names the failing stage, and carries a
recovery fix_hint; ``--no-publish`` remains an explicit opt-out. The
happy-path (real init to a published generation with node_count > 0) is
exercised end-to-end by tests/e2e/test_first_run_golden_path.py.
"""
from __future__ import annotations

import json
import shlex
import subprocess
from pathlib import Path

import pytest

from okg.substrate import fastyaml
from okg.substrate.cli import init as init_mod
from okg.substrate.cli import main as cli
from okg.substrate.cli._output import command_output_tail


def _sample_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "pkg" / "mod.py").write_text("def f():\n    return 1\n")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        [
            "git", "-C", str(repo),
            "-c", "user.email=t@example.com", "-c", "user.name=t",
            "commit", "-qm", "init",
        ],
        check=True,
    )
    return repo


def _init_args(repo: Path, deployments_root: Path, *extra: str) -> list[str]:
    return [
        "init",
        "--profile", "workspace-graph",
        "--deployment-name", "proof_demo",
        "--deployments-root", str(deployments_root),
        "--postgres-dsn",
        "postgresql://postgres:okg@127.0.0.1:5433/okg_proof_demo",
        "--repo-root", str(repo),
        "--mcp-port", "18889",
        "--non-interactive",
        "--json",
        *extra,
    ]


def test_command_output_tail_carries_both_streams() -> None:
    """A bring-up stage failure message must not drop stdout.

    The ingest/run CLIs print their structured failure report to stdout
    and log warnings to stderr; the old ``stderr or stdout`` composition
    dropped the report whenever any warning was logged (the 2026-07-17
    golden-path lane failure carried three warning lines and no cause).
    """
    proc = subprocess.CompletedProcess(
        args=["okg", "ingest"],
        returncode=1,
        stdout='{"results": [{"source": "code_graph", "status": "FAILED"}]}',
        stderr="10:07:01 WARNING openspec_root not found: /tmp/x\n",
    )
    message = command_output_tail(proc)
    assert "code_graph" in message, "stdout report must survive"
    assert "openspec_root not found" in message, "stderr must survive"
    assert message.index("stdout") < message.index("stderr")

    silent = subprocess.CompletedProcess(
        args=["okg", "ingest"], returncode=1, stdout="", stderr="  \n",
    )
    assert command_output_tail(silent) == (
        "(no output on either stream)"
    )

    long_tail = subprocess.CompletedProcess(
        args=["okg", "ingest"],
        returncode=1,
        stdout="x" * 9000 + "TAIL_MARKER",
        stderr="",
    )
    bounded = command_output_tail(long_tail)
    assert "TAIL_MARKER" in bounded, "the tail end is the part that matters"
    assert len(bounded) < 5000


@pytest.mark.parametrize(
    ("stage", "expected_code"),
    [
        ("catalog", "catalog_load_failed"),
        ("ingest", "ingest_failed"),
        ("publish", "first_publish_failed"),
        ("publish_proof", "first_publish_empty"),
        ("nomos_baseline", "nomos_baseline_failed"),
    ],
)
def test_init_exits_nonzero_when_a_bringup_stage_fails(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    expected_code: str,
) -> None:
    repo = _sample_repo(tmp_path)

    def _fail(**_kwargs):
        return {
            "ok": False,
            "stage": stage,
            "message": f"forced {stage} failure (test)",
            "details": {},
        }

    monkeypatch.setattr(init_mod, "_run_init_migrate_and_publish", _fail)
    rc = cli.main(_init_args(repo, tmp_path / "deployments"))
    out = capsys.readouterr().out
    payload = json.loads(out.strip().splitlines()[-1] and out)

    assert rc == 1, "init must not exit 0 over a failed bring-up stage"
    assert payload["failed_stage"] == stage
    codes = {w["code"] for w in payload["warnings"]}
    assert expected_code in codes
    assert "docs/runbooks/troubleshooting/first-run.md" in payload["fix_hint"]
    # The scaffold itself was still written and reported.
    assert any(
        str(p).endswith("deployment.yaml") for p in payload["files_written"]
    )


def test_init_missing_database_gets_createdb_hint(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.provision.cold-start-hint covers init too: a DSN whose database
    does not exist fails with the literal createdb command, not a raw
    psycopg error wrapped as a generic migrate failure."""
    import psycopg

    repo = _sample_repo(tmp_path)

    def _missing_db(_dsn):
        raise psycopg.OperationalError(
            'connection failed: FATAL:  database "okg_proof_demo" '
            "does not exist",
        )

    monkeypatch.setattr(
        "okg.substrate.db.migrate.ensure_required_extensions", _missing_db,
    )
    rc = cli.main(_init_args(repo, tmp_path / "deployments"))
    out = capsys.readouterr().out
    payload = json.loads(out.strip().splitlines()[-1] and out)

    assert rc == 1
    assert payload["failed_stage"] == "database_missing"
    codes = {w["code"] for w in payload["warnings"]}
    assert "database_missing" in codes
    assert "createdb -h 127.0.0.1 -p 5433 -U postgres okg_proof_demo" in (
        payload["fix_hint"]
    )
    # No raw psycopg traceback in the operator-facing message.
    assert "OperationalError" not in payload["publish"]["message"]


def test_init_no_publish_still_succeeds(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo = _sample_repo(tmp_path)
    rc = cli.main(_init_args(repo, tmp_path / "deployments", "--no-publish"))
    out = capsys.readouterr().out
    payload = json.loads(out.strip().splitlines()[-1] and out)

    assert rc == 0
    assert "failed_stage" not in payload
    assert payload["next"][0] == "export OKG_DEPLOYMENTS_DIR=" + shlex.quote(
        str((tmp_path / "deployments").resolve()),
    )


def test_init_success_path_unchanged(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _sample_repo(tmp_path)

    def _ok(**_kwargs):
        return {
            "ok": True,
            "message": "first publish complete",
            "details": {"publish_proof": {"ok": True}},
        }

    monkeypatch.setattr(init_mod, "_run_init_migrate_and_publish", _ok)
    rc = cli.main(_init_args(repo, tmp_path / "deployments"))
    out = capsys.readouterr().out
    payload = json.loads(out.strip().splitlines()[-1] and out)

    assert rc == 0
    assert payload["publish"]["ok"] is True
    assert "failed_stage" not in payload


# ---------------------------------------------------------------------------
# Issue #1948: the migrate stage sent `${OKG_DSN}` to Postgres unresolved.
#
# `${OKG_DSN}` is the documented answer to the install's database question
# (cli/main.py prints it as the deployment-manifest example, and Archi's
# bundle README tells operators to pass exactly that). Every command run
# after the install expands it; the installer's own migrate stage did not,
# so libpq received the literal text and refused with
# `missing "=" after "${OKG_DSN}" in connection info string`.
#
# The placeholder must be expanded for the IN-PROCESS connection and for
# the child processes, and must stay a placeholder in the deployment
# manifest on disk, so the password is never written to a file.
# ---------------------------------------------------------------------------

_SENTINEL_DSN = (
    "postgresql://postgres:s3cr3t-sentinel@127.0.0.1:5999/okg_1948_demo"
)


def _stub_database_bringup(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Record what the bring-up would have sent to Postgres, send nothing.

    Every real database touch in `_run_init_migrate_and_publish` is
    replaced: the two migrate-stage calls, the Nomos baseline seed, and
    the three child-process stages. The catalog child returns nonzero so
    the function stops there with everything already recorded.
    """
    import subprocess as subprocess_mod

    from okg.substrate.db import migrate as migrate_mod

    seen: dict = {"child_envs": []}

    def _fake_ensure(dsn, *args, **kwargs):
        seen["extensions_dsn"] = dsn
        return {"installed": []}

    def _fake_migrate(dsn, *args, **kwargs):
        seen["migrate_dsn"] = dsn
        return None

    monkeypatch.setattr(
        migrate_mod, "ensure_required_extensions", _fake_ensure,
    )
    monkeypatch.setattr(migrate_mod, "migrate", _fake_migrate)
    monkeypatch.setattr(
        init_mod,
        "_seed_init_nomos_baseline_if_required",
        lambda **_kwargs: None,
    )

    real_run = subprocess_mod.run

    def _fake_subprocess_run(cmd, *args, **kwargs):
        # Only the init bring-up children are stubbed; the git calls the
        # fixture makes must still really run.
        if not (isinstance(cmd, list) and "okg.substrate.cli.main" in cmd):
            return real_run(cmd, *args, **kwargs)
        seen["child_envs"].append(dict(kwargs.get("env") or {}))
        return subprocess.CompletedProcess(
            args=cmd, returncode=1, stdout="", stderr="stubbed child",
        )

    monkeypatch.setattr(subprocess_mod, "run", _fake_subprocess_run)
    return seen


def test_install_expands_dsn_placeholder_but_persists_it(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The placeholder answer reaches Postgres expanded, disk placeholdered."""
    repo = _sample_repo(tmp_path)
    monkeypatch.setenv("OKG_DSN", _SENTINEL_DSN)
    seen = _stub_database_bringup(monkeypatch)

    deployments_root = tmp_path / "deployments"
    args = _init_args(repo, deployments_root)
    dsn_flag = args.index("--postgres-dsn")
    args[dsn_flag + 1] = "${OKG_DSN}"

    rc = cli.main(args)
    captured = capsys.readouterr()

    # The in-process migrate stage got the EXPANDED value. On today's code
    # both of these are the literal "${OKG_DSN}".
    assert seen["extensions_dsn"] == _SENTINEL_DSN
    assert seen["migrate_dsn"] == _SENTINEL_DSN
    # So did the child processes, which re-resolve `${OKG_DSN}` from the
    # manifest and would otherwise find the variable set to its own name.
    assert seen["child_envs"], "the catalog child must have been invoked"
    assert seen["child_envs"][0]["OKG_DSN"] == _SENTINEL_DSN

    # The manifest keeps the placeholder: the password never lands on disk.
    manifest_path = deployments_root / "proof_demo" / "deployment.yaml"
    manifest_text = manifest_path.read_text()
    manifest = fastyaml.safe_load(manifest_text)
    assert manifest["postgres"]["dsn"] == "${OKG_DSN}"
    assert "s3cr3t-sentinel" not in manifest_text

    # Nor in anything the CLI printed.
    assert "s3cr3t-sentinel" not in captured.out
    assert "s3cr3t-sentinel" not in captured.err

    # The run still fails, but at the stubbed catalog child, not migrate.
    assert rc == 1
    payload = json.loads(captured.out)
    assert payload["failed_stage"] == "catalog"


def test_install_refuses_unset_dsn_variable_before_any_db_call(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unset variable is a named refusal, not a libpq parse error."""
    repo = _sample_repo(tmp_path)
    monkeypatch.delenv("OKG_DSN", raising=False)
    seen = _stub_database_bringup(monkeypatch)

    args = _init_args(repo, tmp_path / "deployments")
    dsn_flag = args.index("--postgres-dsn")
    args[dsn_flag + 1] = "${OKG_DSN}"

    rc = cli.main(args)
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["failed_stage"] == "dsn_unresolved"
    codes = {w["code"] for w in payload["warnings"]}
    assert "postgres_dsn_unresolved" in codes
    message = payload["publish"]["message"]
    assert "OKG_DSN" in message, "the refusal must name the unset variable"
    assert "export OKG_DSN=" in payload["fix_hint"]

    # Fail closed: nothing was sent to Postgres, and no child ran.
    assert "extensions_dsn" not in seen
    assert "migrate_dsn" not in seen
    assert seen["child_envs"] == []


@pytest.mark.parametrize(
    ("raw", "env", "expected"),
    [
        ("${OKG_DSN}", {"OKG_DSN": _SENTINEL_DSN}, _SENTINEL_DSN),
        ("postgresql://postgres@h/db", {}, "postgresql://postgres@h/db"),
        (
            "postgresql://postgres@${PGHOST}/db",
            {"PGHOST": "127.0.0.1"},
            "postgresql://postgres@127.0.0.1/db",
        ),
    ],
)
def test_resolve_install_answer_dsn_expands(
    monkeypatch: pytest.MonkeyPatch,
    raw: str,
    env: dict,
    expected: str,
) -> None:
    """The answer-dict path expands the same way the flag path does."""
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert init_mod._resolve_install_answer_dsn(raw) == expected


def test_resolve_install_answer_dsn_names_every_unset_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal lists the variables to set and leaks no DSN."""
    from okg.substrate.errors import SubstrateError

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_PGHOST", raising=False)

    with pytest.raises(SubstrateError) as excinfo:
        init_mod._resolve_install_answer_dsn(
            "postgresql://u@${OKG_PGHOST}/${OKG_DSN}",
        )
    assert excinfo.value.data["unresolved"] == ["OKG_DSN", "OKG_PGHOST"]
    assert "OKG_DSN" in str(excinfo.value)
    assert "OKG_PGHOST" in str(excinfo.value)

    # A variable set to whitespace is not a usable DSN either.
    monkeypatch.setenv("OKG_DSN", "   ")
    with pytest.raises(SubstrateError) as empty:
        init_mod._resolve_install_answer_dsn("${OKG_DSN}")
    assert "empty" in str(empty.value)


# ---------------------------------------------------------------------------
# Issue #2331: a fresh install whose every source waits for setup approval
# (academic-researcher) gave the bare first `okg ingest` nothing to run; that
# command refuses an empty plan, and init reported the refusal as a failed
# ingest, so every fresh install exited 1. Nothing runnable is now its own
# successful outcome: the ingest child is skipped by name, the empty graph
# is published, and init names the command that adds a first source. A
# runnable source whose ingest fails still fails init.
# ---------------------------------------------------------------------------

_NOTES_ENTRY = """\
  {name}:
    enabled_by_default: {enabled}
    module: okg.substrate.library.sources.notes_dir
    class: NotesDirSource
    ownership_id: demo.{name}
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


def _deployment_with_sources(tmp_path: Path, **enabled: bool) -> Path:
    dep_dir = tmp_path / "deployments" / "demo"
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(
        "name: demo\nsource_registry: source_registry.yaml\n",
    )
    body = "".join(
        _NOTES_ENTRY.format(name=name, enabled=str(flag).lower())
        for name, flag in enabled.items()
    )
    (dep_dir / "source_registry.yaml").write_text(
        ("sources:\n" + body) if body else "sources: {}\n",
    )
    return dep_dir


def _stub_bringup_children(
    monkeypatch: pytest.MonkeyPatch,
    *,
    returncodes: dict[str, int],
    generations: tuple = (None, "gen-empty"),
    counts: tuple = (0, 0),
    run_report: dict | None = None,
    ingest_stdout: str = "",
    argv: dict[str, list[str]] | None = None,
) -> list[str]:
    """Stub every database touch; record which okg children ran.

    ``generations`` is the deployment-scoped lookup before the first
    publish and the version `okg run --once --json` then reports as
    published (#2422); ``counts`` is the (nodes, edges) readback of the
    reported version. ``run_report`` replaces the run's JSON output.
    """
    import subprocess as subprocess_mod

    from okg.substrate.cli import provision as provision_mod
    from okg.substrate.db import migrate as migrate_mod

    lookups = list(generations)

    def _scoped(*_args, **_kwargs):
        return lookups.pop(0)

    monkeypatch.setattr(init_mod, "_deployment_published_generation", _scoped)
    monkeypatch.setattr(
        provision_mod,
        "_generation_row_counts",
        lambda *a, generation_id, **k: (generation_id, *counts),
    )

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

    def _fake_run(cmd, *args, **kwargs):
        if not (isinstance(cmd, list) and "okg.substrate.cli.main" in cmd):
            return real_run(cmd, *args, **kwargs)
        after = cmd[cmd.index("okg.substrate.cli.main") + 1:]
        verb = after[0]
        ran.append(verb)
        if argv is not None:
            argv[verb] = after
        stdout = ingest_stdout if verb == "ingest" else ""
        if verb == "run":
            assert "--json" in after
            reported = generations[-1]
            report = run_report if run_report is not None else {
                "status": "published",
                "run_summary": {
                    "outcome": "published" if reported is not None else "unknown",
                    "generation_id": reported,
                },
            }
            stdout = json.dumps(report)
        return subprocess.CompletedProcess(
            args=cmd, returncode=returncodes.get(verb, 0),
            stdout=stdout, stderr=f"stubbed {verb}",
        )

    monkeypatch.setattr(subprocess_mod, "run", _fake_run)
    return ran


def test_bare_ingest_selection_matches_the_ingest_planner(
    tmp_path: Path,
) -> None:
    dep_dir = _deployment_with_sources(
        tmp_path, awaiting=False, ready=True,
    )
    assert init_mod.bare_ingest_selection(dep_dir) == {
        "runnable": ["ready"],
        "awaiting_setup": ["awaiting"],
    }


def test_no_runnable_sources_skips_ingest_and_publishes_empty_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=False, talks=False)
    ran = _stub_bringup_children(monkeypatch, returncodes={"ingest": 1})

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is True, outcome
    assert outcome["no_runnable_sources"] is True
    # The refusing child is never started; the publish still runs.
    assert ran == ["catalog", "run"]
    assert outcome["details"]["ingest"] == {
        "skipped": True, "reason": "no_runnable_sources",
    }
    assert outcome["details"]["ingest_selection"]["awaiting_setup"] == [
        "notes", "talks",
    ]
    proof = outcome["details"]["publish_proof"]
    assert proof["generation_id"] == "gen-empty"
    assert proof["generation_before"] is None
    assert (proof["node_count"], proof["edge_count"]) == (0, 0)


@pytest.mark.parametrize(
    ("generations", "counts"),
    [
        # No generation for this deployment after the publish.
        ((None, None), (0, 0)),
        # The publish left the generation that was already there.
        (("gen-old", "gen-old"), (0, 0)),
        # Review of PR #2412: the unscoped fallback's shape, a generation
        # whose rows cannot be counted for this deployment.
        ((None, 0), (None, None)),
        ((None, "gen-new"), (0, None)),
    ],
    ids=["missing", "not-newer", "unreadable-gen-0", "unreadable-edges"],
)
def test_no_runnable_sources_without_a_new_readable_generation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    generations: tuple,
    counts: tuple,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=False)
    _stub_bringup_children(
        monkeypatch, returncodes={}, generations=generations, counts=counts,
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "publish_proof"


def test_no_runnable_sources_fails_closed_when_lookup_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=False)
    ran = _stub_bringup_children(monkeypatch, returncodes={})

    def _unreadable(*_args, **_kwargs):
        raise RuntimeError("checkpoint table unreadable")

    monkeypatch.setattr(
        init_mod, "_deployment_published_generation", _unreadable,
    )
    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "publish_proof"
    assert ran == ["catalog"], "no publish may run without a baseline read"


def test_registry_with_no_sources_still_fails_init(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review nit: the skip is for sources waiting for approval, not for a
    scaffold that rendered no sources at all."""
    dep_dir = _deployment_with_sources(tmp_path)
    ran = _stub_bringup_children(monkeypatch, returncodes={})

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "ingest"
    assert "declares no sources" in outcome["message"]
    assert ran == ["catalog"]


def test_bare_ingest_selection_reads_the_manifest_registry_key(
    tmp_path: Path,
) -> None:
    """The registry path comes from the manifest, as `okg ingest` reads it."""
    dep_dir = _deployment_with_sources(tmp_path, ready=True)
    (dep_dir / "source_registry.yaml").rename(dep_dir / "custom_registry.yaml")
    (dep_dir / "source_registry.yaml").write_text("sources: {}\n")
    (dep_dir / "deployment.yaml").write_text(
        "name: demo\nsource_registry: custom_registry.yaml\n",
    )

    assert init_mod.bare_ingest_selection(dep_dir) == {
        "runnable": ["ready"],
        "awaiting_setup": [],
    }


def test_no_runnable_sources_failed_publish_still_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=False)
    _stub_bringup_children(monkeypatch, returncodes={"run": 1})

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "publish"


def test_runnable_source_whose_ingest_fails_still_fails_init(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=True, talks=False)
    ran = _stub_bringup_children(monkeypatch, returncodes={"ingest": 1})

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "ingest"
    assert ran == ["catalog", "ingest"]
    assert "no_runnable_sources" not in outcome


def test_unreadable_registry_fails_the_ingest_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=False)
    (dep_dir / "source_registry.yaml").write_text("sources: [not, a, map]\n")
    ran = _stub_bringup_children(monkeypatch, returncodes={})

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "ingest"
    assert ran == ["catalog"]


@pytest.mark.parametrize("as_json", [True, False])
def test_init_with_no_runnable_sources_exits_zero_with_next_step(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    as_json: bool,
) -> None:
    repo = _sample_repo(tmp_path)

    def _empty(**_kwargs):
        return {
            "ok": True,
            "no_runnable_sources": True,
            "message": "first ingest skipped",
            "details": {},
        }

    monkeypatch.setattr(init_mod, "_run_init_migrate_and_publish", _empty)
    args = _init_args(repo, tmp_path / "deployments")
    if not as_json:
        args.remove("--json")
    rc = cli.main(args)
    captured = capsys.readouterr()

    assert rc == 0
    command = init_mod.add_source_next_step("proof_demo")
    assert command.startswith(
        "okg deployment add-source proof_demo notes --type local-folder ",
    )
    if as_json:
        payload = json.loads(captured.out)
        # One status name for `okg init` and `okg install`.
        assert payload["setup_status"] == "awaiting_operator_approval"
        assert payload["setup_status"] == (
            init_mod.SETUP_STATUS_AWAITING_APPROVAL
        )
        assert "failed_stage" not in payload
        # After the OKG_DEPLOYMENTS_DIR export, before everything else.
        assert payload["next"][1] == command
    else:
        # The short summary carries the command, once, on stdout (#3891).
        assert captured.out.count(command) == 1
        assert command not in captured.err


def test_run_once_outcome_reads_both_run_paths() -> None:
    """#2422: init reads the version `okg run --once --json` reported."""
    def proc(stdout: str) -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(
            args=["okg"], returncode=0, stdout=stdout, stderr="",
        )

    inline = proc(json.dumps({
        "generation_id": "gen-x",
        "run_summary": {"outcome": "published", "generation_id": "gen-x"},
    }))
    queued = proc("progress line\n" + json.dumps({
        "path": "dbos", "outcome": "no_change", "generation_id": "gen-y",
    }))
    assert init_mod._run_once_outcome(inline) == {
        "outcome": "published", "generation_id": "gen-x",
    }
    assert init_mod._run_once_outcome(queued) == {
        "outcome": "no_change", "generation_id": "gen-y",
    }
    assert init_mod._run_once_outcome(proc("not json")) == {
        "outcome": "unknown", "generation_id": None,
    }


def test_runnable_publish_proof_checks_the_reported_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2422: with runnable sources the proof reads exactly the version the
    run reported, not the deployment's latest published version."""
    from okg.substrate.cli import provision as provision_mod

    dep_dir = _deployment_with_sources(tmp_path, notes=True)
    _stub_bringup_children(
        monkeypatch, returncodes={}, generations=("gen-this-run",),
    )
    looked_up: list = []

    def _counts(dsn, *, generation_id, deployment_name):
        looked_up.append((generation_id, deployment_name))
        return (generation_id, 12, 7)

    monkeypatch.setattr(provision_mod, "_generation_row_counts", _counts)
    monkeypatch.setattr(
        provision_mod, "_published_generation_row_counts",
        lambda *a, **k: pytest.fail("the latest-version lookup must not run"),
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.assert_publish_proof",
        lambda payload, **k: {"ok": True, **payload},
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is True, outcome
    assert looked_up == [("gen-this-run", "demo")]
    assert outcome["details"]["publish_proof"]["generation_id"] == "gen-this-run"


@pytest.mark.parametrize(
    "report",
    [
        pytest.param({"run_summary": {"outcome": "skipped"}}, id="skipped"),
        pytest.param({}, id="no-verdict"),
        pytest.param(
            {"run_summary": {"outcome": "published", "generation_id": None}},
            id="no-version",
        ),
    ],
)
def test_runnable_run_that_reports_no_published_version_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, report: dict,
) -> None:
    """An earlier non-empty version can no longer stand in for this run."""
    from okg.substrate.cli import provision as provision_mod

    dep_dir = _deployment_with_sources(tmp_path, notes=True)
    _stub_bringup_children(
        monkeypatch, returncodes={}, run_report=report, counts=(12, 7),
    )
    monkeypatch.setattr(
        provision_mod, "_published_generation_row_counts",
        lambda *a, **k: ("gen-earlier", 12, 7),
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "publish_proof"
    assert "not a published version" in outcome["message"]


def test_reported_version_of_another_deployment_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import provision as provision_mod

    dep_dir = _deployment_with_sources(tmp_path, notes=True)
    _stub_bringup_children(
        monkeypatch, returncodes={}, generations=("gen-elsewhere",),
    )
    monkeypatch.setattr(
        provision_mod, "_generation_row_counts",
        lambda dsn, *, generation_id, deployment_name: (
            generation_id, None, None,
        ),
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "publish_proof"
    assert "gen-elsewhere is not a published version" in outcome["message"]


def test_blocked_first_publish_names_the_version_and_describe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=True)
    _stub_bringup_children(
        monkeypatch, returncodes={"run": 3},
        run_report={"run_summary": {
            "outcome": "blocked", "generation_id": "gen-blocked",
        }},
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "publish"
    assert "gen-blocked" in outcome["message"]
    assert (
        "okg generation describe gen-blocked --deployment demo"
        in outcome["message"]
    )


def test_init_prints_each_agent_line_once(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #3480: the agent step's message went to stderr and its block
    was rendered again in the summary, so every "is not connected" line
    printed twice. Human output prints it once; JSON keeps the block."""
    argv = [
        "install",
        "--deployment-name", "nif_agents_once",
        "--deployments-root", str(tmp_path / "deployments"),
        "--postgres-dsn", "postgresql://postgres:okg@127.0.0.1:1/okg_nif",
        "--non-interactive",
        "--no-publish",
    ]
    # `--verbose` prints the agent step's full message; the short summary
    # (#3891) names one connect command instead.
    assert cli.main([*argv, "--verbose"]) == 0
    captured = capsys.readouterr()
    lines = (captured.out + captured.err).splitlines()
    agent_lines = [line for line in lines if " is not connected (" in line]
    assert agent_lines, "the agent step printed nothing"
    assert len(agent_lines) == len(set(agent_lines)), agent_lines

    assert cli.main([*argv, "--force", "--yes", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["agents"]["not_connected"]


# PACT macos-first-install-kill: a failed first ingest names each failed
# source's own error, and text mode prints the stage, fix, dir, and next
# steps. Before, the text report was cut to its last 4000 characters, which
# dropped the failed source's line, and text mode printed no recovery steps.
_KILLED_REPORT = {
    "results": [
        {"name": "git_files", "status": "OK"},
        {
            "name": "code_structure",
            "status": "FAIL",
            "stage": "child_exit",
            "error": "source child killed by signal SIGKILL",
        },
    ],
    # A long report tail: the cause must not depend on where a cut falls.
    "padding": "x" * 20000,
}


def test_ingest_failure_names_each_failed_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=True, talks=False)
    argv: dict[str, list[str]] = {}
    _stub_bringup_children(
        monkeypatch,
        returncodes={"ingest": 1},
        ingest_stdout=json.dumps(_KILLED_REPORT),
        argv=argv,
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert "--json" in argv["ingest"]
    assert outcome["ok"] is False
    assert outcome["stage"] == "ingest"
    assert outcome["details"]["failed_sources"] == [{
        "source": "code_structure",
        "stage": "child_exit",
        "error": "source child killed by signal SIGKILL",
    }]
    assert outcome["message"].startswith(
        "ingest failed: source code_structure failed at child_exit: "
        "source child killed by signal SIGKILL",
    )
    assert "git_files" not in outcome["message"]
    assert "stubbed ingest" in outcome["message"]


def test_ingest_failure_without_a_report_keeps_both_streams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=True, talks=False)
    _stub_bringup_children(
        monkeypatch, returncodes={"ingest": 1}, ingest_stdout="not json",
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert "failed_sources" not in outcome["details"]
    assert "stdout tail:\nnot json" in outcome["message"]
    assert "stderr tail:\nstubbed ingest" in outcome["message"]


def test_ingest_failed_sources_reads_only_a_report() -> None:
    assert init_mod.ingest_failed_sources("") == []
    assert init_mod.ingest_failed_sources(None) == []
    assert init_mod.ingest_failed_sources('{"code": "x"}') == []
    assert init_mod.ingest_failed_sources(
        "a notice line\n" + json.dumps({"results": [
            {"name": "a", "status": "OK"},
            {"name": "b", "status": "FAIL"},
        ]}),
    ) == [{"source": "b", "stage": "", "error": "no error was recorded"}]


def test_ingest_failed_sources_reads_the_runtime_on_shapes() -> None:
    """With the runtime on, `okg ingest --json` nests the report."""
    killed = {
        "name": "code_structure",
        "status": "FAIL",
        "stage": "child_exit",
        "error": "source child killed by signal SIGKILL",
    }
    expected = [{
        "source": "code_structure",
        "stage": "child_exit",
        "error": "source child killed by signal SIGKILL",
    }]
    one_batch = {
        "workflow_id": "wf-1",
        "status": "SUCCESS",
        "any_failed": True,
        "report": {"results": [{"name": "git_files", "status": "OK"}, killed]},
    }
    # The queued path prints indented JSON over many lines.
    assert init_mod.ingest_failed_sources(
        json.dumps(one_batch, indent=2),
    ) == expected
    second = {"name": "interfaces", "status": "FAIL", "error": "boom"}
    batches = {
        "report": {"results": [second]},
        "workflows": [
            {"workflow_id": "wf-1", "report": {"results": [killed]}},
            {"workflow_id": "wf-2", "report": {"results": [second]}},
        ],
    }
    # Each batch once: the top-level report repeats the last batch.
    assert init_mod.ingest_failed_sources(json.dumps(batches)) == [
        *expected,
        {"source": "interfaces", "stage": "", "error": "boom"},
    ]
    assert init_mod.ingest_failed_sources(
        json.dumps({"workflow_id": "wf-1", "report": None, "error": "x"}),
    ) == []


def test_text_mode_install_failure_prints_cause_and_recovery(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _sample_repo(tmp_path)

    def _killed(**_kwargs):
        return {
            "ok": False,
            "stage": "ingest",
            "message": "ingest failed: (cut)",
            "details": {"failed_sources": [{
                "source": "code_structure",
                "stage": "child_exit",
                "error": "source child killed by signal SIGKILL",
            }]},
        }

    monkeypatch.setattr(init_mod, "_run_init_migrate_and_publish", _killed)
    args = _init_args(repo, tmp_path / "deployments")
    args.remove("--json")
    rc = cli.main(args)
    err = capsys.readouterr().err

    assert rc == 1
    assert "install failed at stage ingest" in err
    assert (
        "source code_structure failed at child_exit: "
        "source child killed by signal SIGKILL"
    ) in err
    assert "fix: fix the failing stage" in err
    assert any(
        line.startswith("dir: ") and line.endswith("proof_demo")
        for line in err.splitlines()
    )
    assert "next: okg migrate --deployment proof_demo --apply" in err


def test_text_mode_install_failure_without_sources_prints_message(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _sample_repo(tmp_path)

    def _fail(**_kwargs):
        return {
            "ok": False,
            "stage": "catalog",
            "message": "catalog load failed: boom",
            "details": {},
        }

    monkeypatch.setattr(init_mod, "_run_init_migrate_and_publish", _fail)
    args = _init_args(repo, tmp_path / "deployments")
    args.remove("--json")
    rc = cli.main(args)
    err = capsys.readouterr().err

    assert rc == 1
    assert "install failed at stage catalog\n  catalog load failed: boom" in err
    assert "fix: " in err
    assert "next: " in err


# Issue #3927 / #3928: the failed-install summary says what became of the
# other sources and gives the fix for the failed one.
_OTHER_SOURCES = {
    "failed": ["cds"],
    "published_without_failed": ["notes"],
    "ingested_not_published": [],
    "not_run_depends_on_failed": ["cds_links"],
    "message": (
        "1 other source was published without cds: notes. 1 source that "
        "depends on cds did not run: cds_links."
    ),
    "next_step": "To go on, fix cds and run `okg ingest` again.",
}
_HEALTH_FAILED_REPORT = {
    "results": [
        {"name": "notes", "status": "OK"},
        {
            "name": "cds",
            "status": "FAIL",
            "stage": "admission_finalize",
            "error": (
                "source admission rejected: source_health_not_healthy "
                "(the connector reported endpoint_failed: harvest "
                "incomplete; no complete scope claimed)"
            ),
        },
    ],
    "other_sources": _OTHER_SOURCES,
}


def test_ingest_failure_names_other_sources_and_the_fix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=True, talks=False)
    _stub_bringup_children(
        monkeypatch,
        returncodes={"ingest": 1},
        ingest_stdout=json.dumps(_HEALTH_FAILED_REPORT),
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["details"]["other_sources"] == _OTHER_SOURCES["message"]
    # The fix names the failed source and the command that leaves it out.
    assert "okg ingest --deployment demo --exclude cds" in outcome["fix_hint"]
    assert "Fix the failed source above" in outcome["fix_hint"]
    assert "remaining steps" in outcome["fix_hint"]
    assert "did not run" in outcome["fix_hint"]
    assert "harvest incomplete" in outcome["message"]


def test_ingest_other_sources_message_reads_only_a_report_that_has_it() -> None:
    assert init_mod.ingest_other_sources_message(None) is None
    assert init_mod.ingest_other_sources_message("not json") is None
    assert init_mod.ingest_other_sources_message(
        json.dumps({"results": [{"name": "a", "status": "OK"}]}),
    ) is None
    assert init_mod.ingest_other_sources_message(
        json.dumps(_HEALTH_FAILED_REPORT),
    ) == _OTHER_SOURCES["message"]


@pytest.mark.parametrize("verbose", [False, True])
def test_text_mode_install_failure_prints_other_sources_and_fix(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    verbose: bool,
) -> None:
    repo = _sample_repo(tmp_path)
    log_noise = '{"phase": "stream.final_flush", "schema": "okg.source_tail.v1"}'

    def _health_failed(**_kwargs):
        return {
            "ok": False,
            "stage": "ingest",
            "message": f"ingest failed: (cut)\nstderr tail:\n{log_noise}",
            "fix_hint": init_mod._ingest_failure_fix("proof_demo", ["cds"]),
            "details": {
                "failed_sources": init_mod.ingest_failed_sources(
                    json.dumps(_HEALTH_FAILED_REPORT),
                ),
                "other_sources": _OTHER_SOURCES["message"],
            },
        }

    monkeypatch.setattr(init_mod, "_run_init_migrate_and_publish", _health_failed)
    args = _init_args(repo, tmp_path / "deployments")
    args.remove("--json")
    if verbose:
        args.append("--verbose")
    rc = cli.main(args)
    err = capsys.readouterr().err

    assert rc == 1
    assert "install failed at stage ingest" in err
    assert "source cds failed at admission_finalize" in err
    assert "harvest incomplete; no complete scope claimed" in err
    assert _OTHER_SOURCES["message"] in err
    assert "fix: the install stopped at ingest" in err
    assert "--exclude cds" in err
    # The failed command's log tail is a screen of JSON lines. The short
    # text leaves it out (the causes above state it); --verbose keeps it.
    assert (log_noise in err) is verbose


# Every source ran and `okg ingest` still exited 1: its final publish failed.
# The entry below is the shape a real run printed (a lab host run, 2026-10-20).
_PUBLISH_FAILED_REPORT = {
    "results": [{"name": "git_files", "status": "OK"}],
    "publishes": [{
        "label": "commit",
        "status": "fail",
        "code": "substrate_error",
        "error": (
            "repository provenance inputs are not committed: commit the "
            "deployment folder first"
        ),
        "fix_hint": "git add -A && git commit in the deployment folder",
    }],
    "padding": "x" * 20000,
}


def test_ingest_with_every_source_ok_names_the_failed_final_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=True, talks=False)
    _stub_bringup_children(
        monkeypatch,
        returncodes={"ingest": 1},
        ingest_stdout=json.dumps(_PUBLISH_FAILED_REPORT),
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["stage"] == "ingest"
    assert "repository provenance inputs are not committed" in outcome["message"]
    assert "[substrate_error]" in outcome["message"]
    # The cause, not the raw streams.
    assert "stdout tail" not in outcome["message"]
    assert "stubbed ingest" not in outcome["message"]
    assert outcome["details"]["ingest_failure"]["code"] == "substrate_error"
    assert outcome["fix_hint"] == "git add -A && git commit in the deployment folder"


def test_ingest_publish_failure_reads_blocked_and_error_envelopes() -> None:
    blocked = init_mod.ingest_publish_failure(
        json.dumps({
            "results": [{"name": "a", "status": "OK"}],
            "publishes": [{
                "label": "commit", "status": "blocked",
                "blocked_reason": {
                    "reason": "consistency gate failed",
                    "failed_consistency_gates": [{"gate_name": "no_orphans"}],
                },
            }],
        }),
        "",
    )
    assert blocked is not None
    assert blocked["message"].startswith("the final publish was blocked: ")
    assert "no_orphans" in blocked["message"]

    # No report at all: the command refused before it ran a source and
    # printed its error envelope on stderr, after some log lines.
    envelope = {"error": {
        "code": "deployment_not_found",
        "message": "deployment dir for 'x' not found",
        "fix_hint": "check deployments/<name>/ exists",
    }}
    refused = init_mod.ingest_publish_failure(
        "", "12:00:00 INFO starting\n" + json.dumps(envelope),
    )
    assert refused == {
        "message": "deployment dir for 'x' not found",
        "code": "deployment_not_found",
        "fix_hint": "check deployments/<name>/ exists",
    }
    # A clean report and an unreadable stream name no cause.
    assert init_mod.ingest_publish_failure(
        json.dumps({"results": [{"name": "a", "status": "OK"}],
                    "publishes": [{"label": "commit", "status": "published",
                                   "generation_id": "g"}]}),
        "plain text",
    ) is None


# #3966: `okg install --force` rewrote the files while the database kept the
# identity claimed before, so the install's own publish was refused.
_DRIFT_CODES = ["catalog_manifest_drifted", "catalog_applied_version_absent"]


def _drifted_report(code: str) -> dict:
    return {
        "results": [{"name": "git_files", "status": "OK"}],
        "publishes": [{
            "label": "commit",
            "status": "fail",
            "code": code,
            "error": "deployment manifest content differs from the claimed identity",
            "fix_hint": "re-claim ownership from the worker directory",
        }],
    }


@pytest.mark.parametrize("code", _DRIFT_CODES)
def test_ingest_refused_for_manifest_drift_names_the_claim_then_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: str,
) -> None:
    dep_dir = _deployment_with_sources(tmp_path, notes=True, talks=False)
    _stub_bringup_children(
        monkeypatch,
        returncodes={"ingest": 1},
        ingest_stdout=json.dumps(_drifted_report(code)),
    )

    outcome = init_mod._run_init_migrate_and_publish(
        deployment_name="demo",
        deployment_dir=dep_dir,
        answers={"postgres_dsn": "postgresql://postgres@127.0.0.1:1/demo"},
    )

    assert outcome["ok"] is False
    assert outcome["stage"] == "ingest"
    assert f"[{code}]" in outcome["message"]
    fix = outcome["fix_hint"]
    assert "`okg status` shows what was published" in fix
    assert "nothing was published" not in fix
    claim = f"okg catalog ownership claim --deployment {dep_dir}"
    run = "okg run --once --deployment demo --apply"
    assert claim in fix and run in fix
    assert fix.index(claim) < fix.index(run)


@pytest.mark.parametrize("code", _DRIFT_CODES)
def test_install_over_drifted_manifest_exits_1_and_prints_next_commands(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    code: str,
) -> None:
    repo = _sample_repo(tmp_path)
    _stub_bringup_children(
        monkeypatch,
        returncodes={"ingest": 1},
        ingest_stdout=json.dumps(_drifted_report(code)),
    )
    # A folder name with a space: the printed claim command must quote it.
    args = _init_args(repo, tmp_path / "my deployments")
    args.remove("--json")

    rc = cli.main(args)
    err = capsys.readouterr().err

    assert rc == 1
    assert "install failed at stage ingest" in err
    assert f"[{code}]" in err
    assert "`okg status` shows what was published" in err
    assert "nothing was published" not in err
    claim = "okg catalog ownership claim --deployment '"
    assert "my deployments/proof_demo'" in err
    run = "okg run --once --deployment proof_demo --apply"
    fix_line = next(x for x in err.splitlines() if x.startswith("fix: "))
    assert fix_line.index(claim) < fix_line.index(run)
    # No `next:` line offers the run before the claim.
    next_lines = [x for x in err.splitlines() if x.startswith("next: ")]
    claim_at = next(i for i, x in enumerate(next_lines) if claim in x)
    run_at = next(i for i, x in enumerate(next_lines) if run in x)
    assert claim_at < run_at
