"""`okg install --dry-run` on the generic scaffold path (issue #946).

`--dry-run` is documented as writing nothing. Before this suite it was
declared only by the academic-researcher profile extension and read only by that
profile's installer, so every other profile — which reaches the generic
scaffold path — accepted the flag, ignored it, and wrote a full deployment
directory before the first step that could refuse.

Coverage:

1. Nothing is written. The whole deployments root is snapshotted before and
   after a dry run and compared path-for-path.
2. An existing deployment directory is reported, not silently merged or
   overwritten, and its bytes are unchanged.
3. `--force --yes` reports `overwrite` rather than pretending it would
   refuse.
4. The reported file list is the real one: it equals what a subsequent real
   scaffold actually writes.
5. `--dry-run` exists on the `install` parser with no profile extensions
   discovered, and no longer promises "Touches nothing" unqualified.
"""
from __future__ import annotations

import argparse
import json
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import okg.substrate.cli.main as cli
from okg.substrate.catalog.profile import load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    planned_writes,
    scaffold_deployment,
)


@pytest.fixture()
def profile_dir(tmp_path) -> Path:
    """Minimal profile that exercises every scaffold branch.

    Ships source defaults and an invariants file so the plan covers more
    than the two always-written files.
    """
    d = tmp_path / "profiles" / "fixture"
    d.mkdir(parents=True)
    (d / "profile.yaml").write_text(textwrap.dedent("""\
        name: fixture
        version: 0.1.0
        description: in-test profile
        init_questions:
          - id: deployment_name
            prompt: deployment name
            type: slug
            required: true
          - id: postgres_dsn
            prompt: postgres dsn
            type: string
            required: false
            default: postgresql://okg:okg@127.0.0.1:59999/unreachable
        modules: modules.yaml
        source_defaults: source-defaults/
        invariants: invariants.yaml
        """))
    (d / "modules.yaml").write_text(yaml.safe_dump({"modules": ["reasoning"]}))
    sd = d / "source-defaults"
    sd.mkdir()
    (sd / "papers.yaml").write_text(
        "class: example.PapersSource\n",
    )
    (d / "invariants.yaml").write_text("invariants: []\n")
    return d


def _args(*, deployments_root: Path, name: str, **overrides) -> SimpleNamespace:
    base = dict(
        command="install",
        profile_name="fixture",
        template=None,
        deployment_name=name,
        deployments_root=str(deployments_root),
        force=False,
        yes=False,
        no_publish=True,
        non_interactive=True,
        dry_run=True,
        target=None,
        repo_root=None,
        openspec_root=None,
        claude_projects_root=None,
        notes_root=None,
        since=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _snapshot(root: Path) -> dict[str, bytes]:
    """Every file under `root`, keyed by relative path, with contents.

    Missing root snapshots as `{}` so "the root itself was created" is a
    detectable difference, not an invisible one.
    """
    if not root.is_dir():
        return {}
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _run(args, capsys) -> tuple[int, dict]:
    rc = cli._cmd_init(args, as_json=True)
    return rc, json.loads(capsys.readouterr().out)


def test_dry_run_writes_nothing_on_fresh_target(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"

    rc, payload = _run(_args(deployments_root=root, name="fresh"), capsys)

    assert rc == 0
    assert payload["status"] == "dry_run_ok"
    assert payload["dry_run"] is True
    assert payload["deployment_dir_action"] == "create"
    assert payload["blockers"] == []
    assert payload["files_that_would_be_written"]
    # The regression itself: the deployments root must not exist. Before
    # the fix this directory held five scaffold files.
    assert not root.exists()


def test_dry_run_leaves_existing_deployment_untouched(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"

    # A real (non-dry) install first, so there is something to clobber.
    rc, _ = _run(
        _args(deployments_root=root, name="existing", dry_run=False), capsys,
    )
    assert rc == 0
    before = _snapshot(root)
    assert before, "setup failed: the real install wrote nothing"

    rc, payload = _run(_args(deployments_root=root, name="existing"), capsys)

    # Nonzero so a wrapper script cannot read a blocked plan as a clear one.
    assert rc == 1
    assert payload["status"] == "dry_run_blocked"
    assert payload["deployment_dir_exists"] is True
    assert payload["deployment_dir_non_empty"] is True
    assert payload["deployment_dir_action"] == "refuse"
    assert payload["blockers"]
    assert _snapshot(root) == before


def test_dry_run_reports_overwrite_under_force_yes(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """`--force --yes` would delete the directory; the plan must say so."""
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"
    rc, _ = _run(
        _args(deployments_root=root, name="clobber", dry_run=False), capsys,
    )
    assert rc == 0
    before = _snapshot(root)

    rc, payload = _run(
        _args(deployments_root=root, name="clobber", force=True, yes=True),
        capsys,
    )

    assert rc == 0
    assert payload["status"] == "dry_run_ok"
    assert payload["deployment_dir_action"] == "overwrite"
    assert _snapshot(root) == before


def test_reported_files_match_what_a_real_scaffold_writes(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """The plan's file list is the real one, not a hand-kept duplicate."""
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"

    _, payload = _run(_args(deployments_root=root, name="listcheck"), capsys)
    promised = payload["files_that_would_be_written"]

    plan = build_plan(
        profile=load_profile(profile_dir / "profile.yaml"),
        deployment_name="listcheck",
        deployments_root=root,
        raw_answers={"deployment_name": "listcheck"},
    )
    actual = [str(p) for p in scaffold_deployment(plan).files_written]

    assert promised == actual
    assert [str(w.path) for w in planned_writes(plan)] == actual


def test_dry_run_reports_what_overwrite_would_destroy(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """`--force` rmtree's the directory; the plan must name the casualties.

    Reporting only the files that would be WRITTEN answers a different
    question than the one the operator asked.
    """
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"
    rc, _ = _run(
        _args(deployments_root=root, name="doomed", dry_run=False), capsys,
    )
    assert rc == 0
    keepme = root / "doomed" / "keepme.txt"
    keepme.write_text("operator's own file")

    _, payload = _run(
        _args(deployments_root=root, name="doomed", force=True, yes=True),
        capsys,
    )

    assert payload["deployment_dir_action"] == "overwrite"
    assert str(keepme) in payload["files_that_would_be_deleted"]
    assert keepme.read_text() == "operator's own file"


def test_dry_run_reports_blockers_when_an_answer_is_missing(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """A missing required answer yields a blocked plan, not an error.

    Asking "what would this do?" before you have every answer is the case
    --dry-run exists for. It must not report an empty file list either —
    that would read as "this would write nothing".
    """
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"

    rc, payload = _run(
        _args(deployments_root=root, name=None, deployment_name=None), capsys,
    )

    assert rc == 1
    assert payload["status"] == "dry_run_blocked"
    assert payload["plan_complete"] is False
    assert payload["blockers"]
    assert "files_that_would_be_written" not in payload
    assert not root.exists()


def test_dry_run_opens_no_database_and_runs_no_subprocess(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """Not writing files is not the whole promise.

    A real run migrates a schema, ingests, and publishes. Booby-trap the
    doors those go through and assert the dry run never knocks. The
    migrate stage catches exceptions and folds them into a `publish`
    result, so assert on the absence of that key rather than on the trap
    raising — a swallowed trap must still be visible.

    (Non-vacuity: with `dry_run=False` and this same fixture the trap does
    fire, surfacing as `publish.stage == "migrate"`.)

    One subprocess is allowed through, by argv, and only that one: the
    read-only `git rev-parse --show-toplevel` probe that decides whether the
    scaffold target is inside a Git repository (issue #2127). The plan renders
    the SAME `deployment.yaml` text a real install writes, and that text
    carries a `release:` block only for a target outside every repository --
    so the plan either asks that question or under-reports what the install
    would write. The probe changes nothing, reaches no network, and the trap
    still fires for every other command, so the promise this test guards --
    no migration, no ingest, no publish, no database -- is unchanged.
    """
    import subprocess

    import psycopg

    real_run = subprocess.run
    real_popen = subprocess.Popen
    probe_argv = ["git", "rev-parse", "--show-toplevel"]

    def _boom(*a, **kw):  # pragma: no cover - the point is it never runs
        raise AssertionError("dry run reached a real side effect")

    def _gate(real):
        # `subprocess.run` builds a `Popen`, so both doors are gated on the
        # same argv rather than leaving `Popen` open to anything.
        def gated(cmd, *a, **kw):
            if list(cmd or ()) == probe_argv:
                return real(cmd, *a, **kw)
            raise AssertionError(
                f"dry run reached a real side effect: {cmd!r}"
            )
        return gated

    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    monkeypatch.setattr(psycopg, "connect", _boom)
    monkeypatch.setattr(subprocess, "run", _gate(real_run))
    monkeypatch.setattr(subprocess, "Popen", _gate(real_popen))
    root = tmp_path / "deployments"

    # `no_publish=False` so the migrate + first-publish stages are the
    # ones being skipped by --dry-run, not by --no-publish.
    rc, payload = _run(
        _args(deployments_root=root, name="nodb", no_publish=False), capsys,
    )

    assert rc == 0
    assert payload["would_migrate_and_publish"] is True
    # These keys exist only once the migrate/publish stage has run.
    assert "publish" not in payload
    assert "failed_stage" not in payload
    assert not root.exists()


def test_template_route_dry_run_writes_nothing(tmp_path, capsys) -> None:
    """The legacy `--template` route honours the shared flag too.

    `--dry-run` is now on the shared parser, so every route it reaches
    must mean it — a route that accepts and ignores it is the original
    defect on a different path.
    """
    target = tmp_path / "tpl"
    args = SimpleNamespace(
        command="init",
        profile_name=None,
        template="devops",
        target=str(target),
        deployment_name=None,
        deployments_root=None,
        force=False,
        yes=False,
        no_publish=True,
        non_interactive=True,
        dry_run=True,
        repo_root=None,
        openspec_root=None,
        claude_projects_root=None,
        notes_root=None,
        since=None,
    )

    rc, payload = _run(args, capsys)

    assert rc == 0
    assert payload["status"] == "dry_run_ok"
    assert payload["target_action"] == "create"
    assert payload["files_that_would_be_written"]
    assert not target.exists()


def test_dry_run_flag_is_substrate_owned(monkeypatch) -> None:
    """`--dry-run` exists even with no profile extension discovered.

    A promise that appears only when academic-researcher happens to be installed
    is not a promise.
    """
    monkeypatch.setattr(
        "okg.substrate.cli.profile_cli_loader.discover_install_extensions",
        lambda: {},
    )
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    cli._build_install_parser(sub)

    args = parser.parse_args(["install", "--profile", "x", "--dry-run"])
    assert args.dry_run is True


def test_dry_run_defaults_to_false_and_is_never_suppressed() -> None:
    """`--dry-run` must default to a real `False`, on both entry verbs.

    The sibling flag `--json` uses `default=argparse.SUPPRESS`, which
    drops the attribute from the namespace when the flag is absent. Every
    reader of `dry_run` uses `getattr(args, "dry_run", False)`, so
    copying that idiom onto this flag would turn every dry run into a
    real install with no error. Pin the default instead.
    """
    parser = cli._build_parser()

    for argv in (["install"], ["init"]):
        args = parser.parse_args(argv)
        assert hasattr(args, "dry_run"), f"{argv[0]} lost the attribute"
        assert args.dry_run is False


def test_deletion_list_covers_everything_rmtree_destroys(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """`is_file()` is not what `shutil.rmtree` removes.

    Symlinks (including broken ones and links to directories) and empty
    directories all get destroyed and none of them is a regular file, so
    filtering on `is_file()` hands the operator a short list and calls it
    complete.
    """
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"
    rc, _ = _run(
        _args(deployments_root=root, name="zoo", dry_run=False), capsys,
    )
    assert rc == 0

    dep = root / "zoo"
    (dep / "real.txt").write_text("x")
    (dep / "empty_dir").mkdir()
    (dep / "link_to_file").symlink_to(dep / "real.txt")
    (dep / "link_to_dir").symlink_to(dep / "empty_dir")
    (dep / "broken_link").symlink_to(dep / "gone")

    _, payload = _run(
        _args(deployments_root=root, name="zoo", force=True, yes=True),
        capsys,
    )
    reported = set(payload["files_that_would_be_deleted"])

    for name in (
        "real.txt", "empty_dir", "link_to_file", "link_to_dir", "broken_link",
    ):
        assert str(dep / name) in reported, f"{name} missing from the plan"
    assert str(dep) in reported, "the deployment dir itself is destroyed too"
    assert payload["deletion_scan_incomplete"] is False
    assert (dep / "real.txt").exists()


def test_missing_answer_a_real_run_would_prompt_for_is_not_a_blocker(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """Don't predict failure for a command that would succeed.

    On a terminal, a required answer with no flag is one a real run simply
    asks for. Reporting that as "a real run would fail" answers a
    different question than the one the operator asked.
    """
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    root = tmp_path / "deployments"

    rc, payload = _run(
        _args(
            deployments_root=root, name=None, deployment_name=None,
            non_interactive=False,
        ),
        capsys,
    )

    assert rc == 1
    assert payload["status"] == "dry_run_blocked"
    assert payload["plan_complete"] is False
    assert payload["blockers"] == []
    assert payload["blockers_resolvable_interactively"]
    assert not root.exists()


def test_template_route_missing_repo_root_is_a_blocked_plan(
    tmp_path, capsys,
) -> None:
    """The template route follows the same could-not-plan contract."""
    args = SimpleNamespace(
        command="init",
        profile_name=None,
        # The one shipped template declaring `requires_repo_context: true`.
        template="codebase-index",
        target=str(tmp_path / "tpl"),
        deployment_name=None,
        deployments_root=None,
        force=False,
        yes=False,
        no_publish=True,
        non_interactive=True,
        dry_run=True,
        repo_root=None,
        openspec_root=None,
        claude_projects_root=None,
        notes_root=None,
        since=None,
    )

    rc, payload = _run(args, capsys)

    assert rc == 1
    assert payload["dry_run"] is True, (
        "got an error envelope, not a plan — the template route must "
        "follow the same could-not-plan contract as the profile route"
    )
    assert payload["status"] == "dry_run_blocked"
    assert payload["plan_complete"] is False
    assert payload["blockers"]
    assert not (tmp_path / "tpl").exists()


def test_every_dry_run_route_sets_plan_complete() -> None:
    """`plan_complete` is the cross-route discriminator, so every route
    that emits a plan must set it — including the profile-owned installer,
    whose payload shape is otherwise entirely its own."""
    from okg.substrate.cli.profile_cli_loader import (
        discover_install_extensions,
    )

    extensions = discover_install_extensions()
    assert extensions, "no install extensions discovered; test is vacuous"
    for name, extension in sorted(extensions.items()):
        source = Path(
            sys.modules[extension.run.__module__].__file__,
        ).read_text()
        assert '"plan_complete"' in source, (
            f"profile {name!r} emits a dry-run plan without plan_complete"
        )


def test_dry_run_end_to_end_through_main(
    profile_dir, tmp_path, monkeypatch, capsys,
) -> None:
    """Drive the real entry point, not a hand-built namespace.

    Every other behavioural test here calls `_cmd_init` with a
    `SimpleNamespace`, which cannot catch a wiring break between argparse
    and the handler — and the handler reads the flag with
    `getattr(args, "dry_run", False)`, which fails open into a real
    install if the attribute ever goes missing.
    """
    monkeypatch.setenv("OKG_PROFILES_DIR", str(profile_dir.parent))
    root = tmp_path / "deployments"

    rc = cli.main([
        "install", "--profile", "fixture",
        "--deployment-name", "e2e",
        "--deployments-root", str(root),
        "--non-interactive", "--no-publish", "--dry-run", "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["status"] == "dry_run_ok"
    assert payload["files_that_would_be_written"]
    assert not root.exists()


def test_no_profile_extension_redeclares_a_substrate_owned_flag() -> None:
    """A colliding extension flag breaks `okg --help`, not just `install`.

    Extensions contribute their arguments to the one shared `install`
    parser before the substrate adds its own, so a profile that declares
    `--dry-run` (or `--json`) raises `argparse.ArgumentError` at
    parser-build time — every verb dies, not only the one at fault. The
    code comments name the hazard; this pins it.
    """
    from okg.substrate.cli.profile_cli_loader import (
        discover_install_extensions,
    )

    extensions = discover_install_extensions()
    # Without this the loop body may never run and the test goes green
    # having checked nothing.
    assert extensions, "no install extensions discovered; test is vacuous"

    substrate_owned = {"--dry-run", "--json"}
    for name, extension in sorted(extensions.items()):
        probe = argparse.ArgumentParser()
        extension.add_arguments(probe)
        declared = set(probe._option_string_actions)
        collision = declared & substrate_owned
        assert not collision, (
            f"profile {name!r} declares substrate-owned {sorted(collision)}; "
            "the substrate owns it in _add_scaffold_arguments"
        )


def test_dry_run_help_does_not_overpromise() -> None:
    """The help text must not say "Touches nothing" while writing files."""
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    cli._build_install_parser(sub)
    install = sub.choices["install"]
    help_text = install._option_string_actions["--dry-run"].help

    assert "Touches nothing" not in help_text
    assert "without writing anything" in help_text
