"""The superseded door is retired, not left running in parallel.

Per PACT ``define-newcomer-entry-path``
(req.entry.migrate-the-other-doors). The repository had two commands that
took a newcomer from nothing to a working graph — ``okg install`` and
``okg init`` — and three documented sequences that disagreed about which
one to type. The operator's decision was one door: ``install`` absorbs
both jobs and ``init`` becomes an alias of it.

What this module holds in place:

1. ``init`` is classified ``alias`` naming ``install``, in both the runtime
   role map and its gated mirror, and the dispatcher actually routes it
   through the door rather than merely labelling it.
2. Running ``okg init`` says so, in plain language, naming the door.
3. The delegation is exact: ``okg init <args>`` runs
   ``okg install <args> --scaffold-only``.
4. The two paths reach the same PUBLISHED end state — not merely the same
   files. This is ``published_generation`` evidence and needs a database,
   so it is ``postgres``-marked and skipped without
   ``OKG_ENTRY_MIGRATION_DSN``. Marked per-test, never file-level: a
   file-level ``postgres`` mark would hide the four checks above from
   per-PR CI.
5. ``start`` / ``stop`` / ``up`` are untouched. They are ``lifecycle``
   verbs, deliberately outside the alias/frozen/delete trichotomy —
   ``start`` is the only day-2 capability the newcomer path has.
6. The ``deployment`` / ``deployments`` near-homonym is resolved: neither
   is an entry-surface door, and each help string says what it operates
   on and names the other, so a trailing ``s`` is never the only signal.
7. The merge-blocking ``first-run-golden-path`` lane, which literately
   executes this doc's bash blocks, still extracts them — and now
   extracts the door. That lane only runs with its own Postgres, so
   without this check a rename here would first be discovered in CI.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli import entry_roles
from okg.substrate.cli import main as cli_main


REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "substrate" / "cli" / "CLI_SMOKE_INDEX.yaml"
QUICKSTART = REPO_ROOT / "docs" / "runbooks" / "workspace-graph" / "setup.md"

DOOR = "install"
ALIASED = "init"


def _manifest_entry(verb: str) -> dict:
    verbs = yaml.safe_load(MANIFEST.read_text())["verbs"]
    entry = verbs.get(verb)
    assert isinstance(entry, dict), f"no manifest entry for {verb!r}"
    return entry


def _subparsers() -> argparse._SubParsersAction:
    parser = cli_main._build_parser()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise AssertionError("no subparsers action on the root parser")


# ── 1. the classification, and a dispatcher that honours it ────────────

def test_the_superseded_door_is_now_an_alias_of_the_door() -> None:
    assert entry_roles.role_of(ALIASED) == entry_roles.ALIAS
    assert entry_roles.ALIAS_TARGETS[ALIASED] == DOOR
    assert entry_roles.newcomer_door() == DOOR

    entry = _manifest_entry(ALIASED)
    assert entry["role"] == "alias"
    assert entry["alias_of"] == DOOR


def test_no_superseded_entry_remains() -> None:
    """The migration is finished, not merely planned."""
    left = sorted(
        verb for verb, role in entry_roles.ENTRY_ROLES.items()
        if role == entry_roles.SUPERSEDED_ENTRY
    )
    assert not left, (
        f"still-superseded doors: {left} — each must reach alias, frozen, "
        "or deleted"
    )


def test_the_alias_is_labelled_where_a_reader_lists_verbs() -> None:
    """A label nobody sees is not a migration."""
    prefix = entry_roles.help_prefix(ALIASED)
    assert prefix == f"[alias for `okg {DOOR}`] ", prefix
    listed = {
        action.dest: (action.help or "")
        for action in getattr(_subparsers(), "_choices_actions", [])
    }
    assert listed[ALIASED].startswith(prefix), listed[ALIASED]


def test_the_dispatcher_routes_the_alias_through_the_door(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classification without delegation would be a label on a lie.

    Driven through `main()` — the real argv path, the real dispatch
    table — so relabelling `init` while leaving it wired to its own
    implementation fails here.
    """
    reached: dict[str, object] = {}

    def _capture(args, *, as_json=False):
        reached["profile"] = getattr(args, "profile", None)
        reached["scaffold_only"] = getattr(args, "scaffold_only", None)
        return 0

    monkeypatch.setattr(cli_main, "_cmd_install", _capture)
    rc = cli_main.main([
        ALIASED,
        "--template", "devops",
        "--target", str(tmp_path / "out"),
    ])
    assert rc == 0
    assert reached == {"profile": None, "scaffold_only": True}


# ── 2. the notice ──────────────────────────────────────────────────────

def test_running_the_alias_names_the_door_in_plain_language(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`okg init` announces the delegation to a human, on stderr."""
    rc = cli_main.main([
        ALIASED,
        "--template", "definitely-not-a-template",
        "--target", str(tmp_path / "out"),
    ])
    captured = capsys.readouterr()

    assert f"`okg {DOOR}`" in captured.err, captured.err
    assert "alias" in captured.err
    assert "alias" not in captured.out
    # A bad template still fails; the notice is not a success signal.
    assert rc != 0
    # Plain language: no coined proper nouns in the operator's face.
    for coined in ("Nomos", "Chronos", "Poros", "AKMON", "narrowing"):
        assert coined not in captured.err


def test_the_notice_never_pollutes_machine_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Under `--json`, stderr is a STRUCTURED channel, not a place for prose.

    The error envelope is written to stderr and callers `json.loads` the
    whole stream (tests/substrate/test_cli_m5b.py does exactly that), so a
    friendly line there breaks every machine consumer. Suppressing the
    notice in JSON mode is the contract; this is the test that caught it
    only after CI did.
    """
    rc = cli_main.main([
        ALIASED,
        "--template", "definitely-not-a-template",
        "--target", str(tmp_path / "out"),
        "--json",
    ])
    captured = capsys.readouterr()

    assert rc != 0
    envelope = json.loads(captured.err)
    assert envelope["error"]["code"] == "library_ref_missing"
    assert "alias" not in captured.out


# ── 3. exact delegation ────────────────────────────────────────────────

def test_the_alias_delegates_with_scaffold_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg init <args>` is `okg install <args> --scaffold-only`.

    Captured at the boundary rather than asserted about the source: the
    claim is what the door receives, and the door's own router decides
    the rest.
    """
    seen: dict[str, object] = {}

    def _capture(args, *, as_json=False):
        seen["profile"] = getattr(args, "profile", None)
        seen["scaffold_only"] = getattr(args, "scaffold_only", None)
        seen["repo_root"] = getattr(args, "repo_root", None)
        seen["as_json"] = as_json
        return 0

    monkeypatch.setattr(cli_main, "_cmd_install", _capture)
    rc = cli_main._cmd_init_alias(
        argparse.Namespace(
            profile_name="workspace-graph", repo_root="/tmp/x", template=None,
        ),
        as_json=True,
    )
    assert rc == 0
    assert seen == {
        "profile": "workspace-graph",
        "scaffold_only": True,
        "repo_root": "/tmp/x",
        "as_json": True,
    }


def test_scaffold_only_skips_a_profiles_own_installer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag that makes the delegation expressible.

    A profile shipping its own installer (Docker provisioning, wizards)
    must NOT run it when the caller asked for the scaffold path — that is
    what `okg init --profile <p>` has always meant, and an alias may not
    silently start provisioning containers.
    """
    from okg.substrate.cli.profile_cli_loader import (
        discover_install_extensions,
    )

    installable = sorted(discover_install_extensions())
    assert installable, "no profile ships an installer to test against"
    profile = installable[0]

    ran: list[str] = []
    monkeypatch.setattr(
        cli_main,
        "_install_via_scaffold",
        lambda args, name, *, as_json: ran.append(f"scaffold:{name}") or 0,
    )
    rc = cli_main._cmd_install(
        argparse.Namespace(profile=profile, scaffold_only=True, template=None),
        as_json=True,
    )
    assert rc == 0
    assert ran == [f"scaffold:{profile}"]


def test_the_alias_really_scaffolds_end_to_end(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`okg init` writes real files through the door — nothing mocked.

    The other delegation tests capture at a boundary, which proves the
    routing but not the outcome. This one runs the whole path and looks at
    the filesystem, because "every existing caller keeps working" is a
    claim about results, not about call graphs.
    """
    target = tmp_path / "scaffolded"
    rc = cli_main.main([
        ALIASED, "--template", "empty", "--target", str(target), "--json",
    ])
    captured = capsys.readouterr()
    assert rc == 0, captured.out + captured.err
    written = sorted(p.name for p in target.rglob("*") if p.is_file())
    assert written, f"the alias wrote no files into {target}"
    # Same invocation through the door itself lands the same files.
    door_target = tmp_path / "door"
    assert cli_main.main([
        DOOR, "--template", "empty", "--target", str(door_target), "--json",
    ]) == 0
    assert sorted(
        p.name for p in door_target.rglob("*") if p.is_file()
    ) == written


def test_a_legacy_template_still_scaffolds_through_the_door(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg install --template <name>` must not fall through to a profile.

    `init` accepted `--template` long before this migration, so the door
    has to honour it; routing it to the default profile's installer would
    silently ignore the flag and build something else entirely.
    """
    ran: list[str] = []
    monkeypatch.setattr(
        cli_main,
        "_install_via_scaffold",
        lambda args, name, *, as_json: ran.append(f"scaffold:{name}") or 0,
    )
    rc = cli_main._cmd_install(
        argparse.Namespace(profile=None, template="devops", target=None),
        as_json=True,
    )
    assert rc == 0
    assert ran == ["scaffold:None"]


# ── 5. lifecycle verbs are untouched ───────────────────────────────────

@pytest.mark.parametrize("verb", ["start", "stop", "up", "restart", "reset"])
def test_lifecycle_verbs_are_untouched(verb: str) -> None:
    """`start` is the only day-2 capability the newcomer path has.

    The migration trichotomy must never pressure anyone into freezing it:
    "after reboot, the container stops".
    """
    assert entry_roles.role_of(verb) == entry_roles.LIFECYCLE
    assert entry_roles.help_prefix(verb) == "", (
        f"{verb!r} carries a migration marker it should not"
    )
    subs = _subparsers()
    assert verb in subs.choices, f"{verb!r} left the parser"
    # Executable: `--help` exits 0 through the real parser.
    with pytest.raises(SystemExit) as exc:
        subs.choices[verb].parse_args(["--help"])
    assert exc.value.code in (0, None)


# ── 6. the deployment / deployments homonym ────────────────────────────

def test_the_deployment_homonym_is_resolved() -> None:
    """A name distinguished only by a trailing `s` is a defect.

    Resolution: neither is a way IN (the door is `okg install`), and each
    help string states what it operates on and names the other, so the
    plural is never the only thing telling them apart.
    """
    for verb in ("deployment", "deployments"):
        assert entry_roles.role_of(verb) == entry_roles.NOT_ENTRY, (
            f"{verb!r} is classified as part of the entry surface; the "
            "door is `okg install`"
        )

    listed = {
        action.dest: (action.help or "")
        for action in getattr(_subparsers(), "_choices_actions", [])
    }
    assert "`okg deployments`" in listed["deployment"], listed["deployment"]
    assert "`okg deployment`" in listed["deployments"], listed["deployments"]
    assert "ALL deployments" in listed["deployments"]
    assert "ONE deployment" in listed["deployment"]


# ── 7. the merge-blocking lane still reads the doc ─────────────────────

def test_the_executed_lane_extracts_the_door_from_the_quickstart() -> None:
    """The literate coupling survives, pointed at the door.

    `tests/e2e/test_first_run_golden_path.py` is a required check and
    executes the quickstart's own bash blocks. It only runs with its own
    Postgres, so this check is where a doc/extractor divergence surfaces
    before CI.
    """
    from tests.e2e.quickstart_extract import (
        DOC_DSN_LITERAL,
        extract_quickstart_blocks,
        substitute_placeholders,
    )

    blocks = extract_quickstart_blocks(QUICKSTART.read_text(encoding="utf-8"))
    assert f"okg {DOOR}" in blocks.install_block
    assert f"okg {ALIASED} " not in blocks.install_block

    filled = substitute_placeholders(
        blocks.install_block,
        {
            DOC_DSN_LITERAL: "postgresql://postgres:okg@127.0.0.1:5433/d",
            "<deployment>": "d",
            "<repo-root>": "/tmp/repo",
            "<mcp-port>": "18999",
        },
    )
    assert "<" not in filled


def test_the_extracted_door_command_parses() -> None:
    """The doc's own command, filled in, parses against the live tree."""
    import shlex

    from tests.e2e.quickstart_extract import (
        DOC_DSN_LITERAL,
        extract_quickstart_blocks,
        substitute_placeholders,
    )

    blocks = extract_quickstart_blocks(QUICKSTART.read_text(encoding="utf-8"))
    filled = substitute_placeholders(
        blocks.install_block,
        {
            DOC_DSN_LITERAL: "postgresql://postgres:okg@127.0.0.1:5433/d",
            "<deployment>": "d",
            "<repo-root>": "/tmp/repo",
            "<mcp-port>": "18999",
        },
    )
    argv = shlex.split(" ".join(filled.split("\\\n")))
    # Strip the `uv run --extra ... okg` prefix the doc's block carries.
    argv = argv[argv.index("okg") + 1:]
    parser = cli_main._build_parser(argv)
    cli_main._maybe_inject_profile_flags(parser, argv)
    parsed = parser.parse_args(argv)
    assert parsed.command == DOOR


# ── 4. the same PUBLISHED end state ────────────────────────────────────

#: Preferred, so an operator can point the proof at a scratch server. Falls
#: back to the standard test DSN, which is what the postgres lane and a
#: local dev stack both set — otherwise the only `published_generation`
#: evidence in the PACT would be a test nothing ever runs.
_DSN_ENV = "OKG_ENTRY_MIGRATION_DSN"
_FALLBACK_DSN_ENV = "OKG_TEST_DSN"


def _base_dsn() -> str | None:
    return os.environ.get(_DSN_ENV) or os.environ.get(_FALLBACK_DSN_ENV)

_PKG_PY = '''\
"""Toy publisher for the entry-path equivalence fixture."""


class PublisherPipeline:
    """Coordinates publisher cycles and tracks generation state."""

    def publish(self, generation_id: str) -> str:
        """Publish a generation and record it as the latest."""
        return generation_id
'''


def _fixture_repo(root: Path) -> Path:
    repo = root / "entry-fixture-repo"
    pkg = repo / "entry_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text('"""Fixture package."""\n')
    (pkg / "pipeline.py").write_text(_PKG_PY)
    (repo / "README.md").write_text(
        "# Entry fixture repo\n\nA publisher whose generation can block.\n"
    )
    subprocess.run(
        ["git", "-c", "init.defaultBranch=main", "init", str(repo)],
        check=True, capture_output=True, text=True,
    )
    for args in (
        ("config", "user.email", "entry-path@example.invalid"),
        ("config", "user.name", "Entry Path Fixture"),
        ("config", "commit.gpgsign", "false"),
        ("add", "-A"),
        ("commit", "-m", "add publisher pipeline"),
    ):
        subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True, capture_output=True, text=True,
        )
    return repo


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _dsn_for(base: str, dbname: str) -> str:
    from psycopg import conninfo

    parts = {
        k: str(v)
        for k, v in conninfo.conninfo_to_dict(base).items()
        if v is not None
    }
    parts["dbname"] = dbname
    return (
        f"postgresql://{parts.get('user', 'postgres')}"
        f":{parts.get('password', '')}@{parts.get('host', '127.0.0.1')}"
        f":{parts.get('port', '5432')}/{dbname}"
    )


def _recreate(base: str, dbname: str) -> None:
    import psycopg
    from psycopg import conninfo

    assert re.fullmatch(r"[a-z_][a-z0-9_]*", dbname), dbname
    server = {
        k: str(v)
        for k, v in conninfo.conninfo_to_dict(base).items()
        if v is not None
    }
    server["dbname"] = "postgres"
    with psycopg.connect(**server, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{dbname}"')


def _okg(
    argv: list[str], *, deployments_root: Path, timeout: int = 1800,
) -> subprocess.CompletedProcess[str]:
    """Run the CLI out-of-process, from an operator-shaped environment."""
    env = os.environ.copy()
    env["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
    for var in (
        "OKG_DEPLOYMENT", "PZ_OKG_DEPLOYMENT", "OKG_DSN",
        "OKG_NOMOS_SUBJECT", "OKG_PACT_DECISION_PROJECTION",
    ):
        env.pop(var, None)
    proc = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main", *argv],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True,
        errors="replace", timeout=timeout,
    )
    print(f"\n===== okg {' '.join(argv[:2])} exit={proc.returncode} =====")
    print(proc.stdout[-6000:])
    print(proc.stderr[-6000:])
    return proc


def _last_json(stdout: str) -> dict:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise AssertionError(f"no JSON object in output:\n{stdout[-4000:]}")


def _bring_up(
    verb: str, *, base_dsn: str, dbname: str, deployment: str, tmp_path: Path,
) -> dict:
    """Drive one entry path all the way to a published generation."""
    _recreate(base_dsn, dbname)
    repo = _fixture_repo(tmp_path / verb)
    deployments_root = tmp_path / verb / "deployments"
    deployments_root.mkdir(parents=True)

    proc = _okg(
        [
            verb,
            "--profile", "workspace-graph",
            "--deployment-name", deployment,
            "--deployments-root", str(deployments_root),
            "--repo-root", str(repo),
            "--postgres-dsn", _dsn_for(base_dsn, dbname),
            "--mcp-port", str(_free_port()),
            "--non-interactive",
            "--json",
        ],
        deployments_root=deployments_root,
    )
    assert proc.returncode == 0, (
        f"`okg {verb}` exited {proc.returncode}; both the door and its "
        f"alias must reach a published graph.\n{proc.stdout[-4000:]}"
    )

    status = _okg(
        ["--json", "status", "--deployment", deployment],
        deployments_root=deployments_root,
        timeout=600,
    )
    assert status.returncode == 0, status.stderr[-2000:]
    payload = _last_json(status.stdout)
    return {
        "stderr": proc.stderr,
        "scaffold": deployments_root / deployment,
        "publishing": payload.get("publishing") or {},
        "live_subtypes": sorted(
            (row["subtype"], row["count"])
            for row in (payload.get("live_subtypes") or [])
        ),
    }


@pytest.mark.postgres
@pytest.mark.slow
@pytest.mark.skipif(
    _base_dsn() is None,
    reason=(
        f"neither {_DSN_ENV} nor {_FALLBACK_DSN_ENV} is set — the "
        "alias/door publish-equivalence proof needs a Postgres server; it "
        "owns and recreates the two databases it names"
    ),
)
def test_alias_and_door_reach_the_same_published_generation(
    tmp_path: Path,
) -> None:
    """`published_generation` evidence for req.entry.migrate-the-other-doors.

    Same files is not the claim. The claim is that a script still calling
    the alias ends up with the same working graph as a newcomer following
    the documented door — so both paths are driven against equivalent
    fixtures, each into its own database, and both must land a
    ``published`` generation with the same node and edge counts.
    """
    base = _base_dsn()
    assert base is not None
    # Same deployment NAME on both sides — separate databases and separate
    # deployment roots keep them isolated. Equivalence is the claim, so the
    # fixtures must differ in nothing a scaffold can see: the profile names
    # one schema file after the deployment, and a different name there
    # would make the file comparison below vacuously fail.
    deployment = "entry_path_proof"
    door = _bring_up(
        DOOR,
        base_dsn=base,
        dbname="okg_entry_door",
        deployment=deployment,
        tmp_path=tmp_path,
    )
    alias = _bring_up(
        ALIASED,
        base_dsn=base,
        dbname="okg_entry_alias",
        deployment=deployment,
        tmp_path=tmp_path,
    )

    # Scope note, kept honest: this proves the END STATE is the same, which
    # is what `published_generation` evidence is for. It does not by itself
    # distinguish the migration — before it, `okg init` published the same
    # graph from its own implementation. That the alias DELEGATES is proven
    # by the fast tests above, which drive `main(["init", ...])` and observe
    # the door receiving `--scaffold-only`. Both runs here pass `--json`,
    # where the delegation notice is deliberately suppressed because stderr
    # is the structured channel in machine mode.
    assert "is an alias for" not in door["stderr"]
    assert "is an alias for" not in alias["stderr"], (
        "the notice leaked into machine-mode stderr, which callers parse "
        "as JSON"
    )

    for label, outcome in (("door", door), ("alias", alias)):
        publishing = outcome["publishing"]
        assert publishing.get("latest_published_status") == "published", (
            f"{label} path did not reach a published generation: "
            f"{json.dumps(publishing, default=str)}"
        )
        assert (publishing.get("latest_published_node_count") or 0) > 0, (
            f"{label} path published an empty graph: "
            f"{json.dumps(publishing, default=str)}"
        )

    assert (
        door["publishing"]["latest_published_node_count"]
        == alias["publishing"]["latest_published_node_count"]
    ), (
        "the alias and the door published different node counts from the "
        "same fixture repo: door="
        f"{door['publishing']['latest_published_node_count']} alias="
        f"{alias['publishing']['latest_published_node_count']}"
    )
    assert (
        door["publishing"].get("latest_published_edge_count")
        == alias["publishing"].get("latest_published_edge_count")
    )
    # Not just the same totals — the same graph, subtype by subtype.
    assert door["live_subtypes"] == alias["live_subtypes"], (
        f"door={door['live_subtypes']} alias={alias['live_subtypes']}"
    )
    assert door["live_subtypes"], "both paths published an empty subtype set"

    # And the scaffolds themselves agree, file for file.
    door_files = sorted(
        p.relative_to(door["scaffold"]).as_posix()
        for p in door["scaffold"].rglob("*") if p.is_file()
    )
    alias_files = sorted(
        p.relative_to(alias["scaffold"]).as_posix()
        for p in alias["scaffold"].rglob("*") if p.is_file()
    )
    assert door_files == alias_files, (
        "the alias and the door wrote different files: "
        f"{set(door_files) ^ set(alias_files)}"
    )
