"""okg CLI child processes never import modules from their working directory.

Issue #2313. ``okg install`` outside a source checkout started its scaffold
child with a working directory inside the installed package
(``.../okg/substrate``), and that child started its catalog-load and ingest
children as ``python -m okg.substrate.cli.main`` without ``-P``. ``-m`` puts
the working directory first on ``sys.path``, so ``import mcp`` found the
package's own ``okg/substrate/mcp`` (editable install) and ``import types``
found ``okg/substrate/types.py`` (wheel install). The first ingest failed as
a "circular import".

Three guards, one per half of the defect plus the class:

* a child started through :func:`running_okg_argv` from a directory holding
  decoy ``mcp/``, ``okg/`` and ``types.py`` imports the real modules;
* ``_find_repo_root()`` never answers with a directory inside the installed
  ``okg`` package; and
* no argv in ``src/okg`` starts the okg CLI with ``-m`` except through the
  helper (known, tracked exceptions are listed and may only shrink).
"""
from __future__ import annotations

import argparse
import ast
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

import okg
from okg.substrate.cli.child_argv import OKG_CLI_MODULE, running_okg_argv
from okg.substrate.cli.main import SUBSTRATE_SUBCOMMANDS
from okg.substrate.cli.profile_cli_loader import get_profile_cli
from tests.mirror_scope import is_private, is_public_copy

pi_cli = get_profile_cli("academic-researcher").install

_SRC_OKG = Path(okg.__file__).resolve().parent
_PACKAGE_DIR = _SRC_OKG

def _decoy_dir(root: Path) -> Path:
    """A working directory whose modules would shadow real ones on sys.path."""
    root.mkdir(parents=True, exist_ok=True)
    marker = root / "decoy-imported"
    body = (
        "import pathlib\n"
        f"pathlib.Path({str(marker)!r}).write_text(__name__)\n"
        "raise ImportError('decoy ' + __name__ + ' imported from cwd')\n"
    )
    for package in ("mcp", "okg"):
        (root / package).mkdir()
        (root / package / "__init__.py").write_text(body)
    (root / "types.py").write_text(body)
    return root


def _interpreter_flags() -> list[str]:
    """The helper's argv up to (not including) ``-m <module>``."""
    argv = running_okg_argv()
    return argv[: argv.index("-m")]


def test_the_helper_is_this_interpreter_with_cwd_off_sys_path() -> None:
    assert running_okg_argv() == [sys.executable, "-P", "-m", OKG_CLI_MODULE]


def test_a_child_from_a_decoy_cwd_imports_the_real_mcp_library(
    tmp_path: Path,
) -> None:
    cwd = _decoy_dir(tmp_path / "operator-folder")
    probe = (
        "import mcp, mcp.server.fastmcp, pathlib, sys\n"
        "print(pathlib.Path(mcp.__file__).resolve())\n"
    )
    proc = subprocess.run(
        [*_interpreter_flags(), "-c", probe],
        cwd=cwd, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    imported = Path(proc.stdout.strip())
    assert not (cwd / "decoy-imported").exists(), proc.stderr
    assert cwd not in imported.parents, imported
    # The third-party library, not okg's own `okg/substrate/mcp`.
    assert _PACKAGE_DIR not in imported.parents, imported


def test_the_okg_cli_child_starts_from_a_decoy_cwd(tmp_path: Path) -> None:
    """The full helper argv, not just its flags, survives a hostile cwd."""
    cwd = _decoy_dir(tmp_path / "operator-folder")
    proc = subprocess.run(
        [*running_okg_argv(), "--help"],
        cwd=cwd, capture_output=True, text=True, timeout=180,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert not (cwd / "decoy-imported").exists(), proc.stderr[-2000:]


def test_find_repo_root_outside_a_checkout_is_none(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    assert pi_cli._find_repo_root() is None


def test_find_repo_root_never_answers_inside_the_installed_package(
    monkeypatch,
) -> None:
    """From inside the package the answer is a checkout above it, or None."""
    monkeypatch.chdir(_PACKAGE_DIR / "substrate")
    root = pi_cli._find_repo_root()
    if root is not None:
        assert root.resolve() != _PACKAGE_DIR, root
        assert _PACKAGE_DIR not in root.resolve().parents, root


def test_without_a_checkout_the_image_build_refuses_by_name() -> None:
    message = pi_cli._build_okg_pg17_image(None)
    assert message is not None
    assert "source checkout" in message, message


def test_without_a_checkout_there_is_no_legacy_in_repo_lookup(
    tmp_path: Path,
) -> None:
    root = tmp_path / "deployments"
    root.mkdir()
    with pytest.raises(FileNotFoundError):
        pi_cli._resolve_existing_dep_dir(
            "absent", repo_root=None, deployments_root=root,
        )


def test_outside_a_checkout_the_scaffold_child_inherits_the_operator_cwd(
    tmp_path: Path, monkeypatch,
) -> None:
    recorded: list[tuple[list[str], dict]] = []

    class _Done:
        returncode = 0
        stdout = ""
        stderr = ""

    def _record(cmd, **kwargs):  # noqa: ANN001
        recorded.append((list(cmd), kwargs))
        return _Done()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pi_cli.subprocess, "run", _record)
    args = argparse.Namespace(
        deployment_name="cwd-probe", orcid=None,
        postgres_dsn="postgresql://okg@127.0.0.1:59999/unreachable",
        postgres_port=15435, use_local_postgres=True, mcp_port=15431,
        papers_dir=None, cv_path=None, group_webpage=None, pi_name=None,
        dblp_pid=None, git_repos=None, reset=False, yes=True, force=False,
        deployments_root_resolved=(tmp_path / "owned").resolve(),
        _invoked_from_install=True,
    )
    assert pi_cli._provision_postgres_and_scaffold(args, as_json=True) == 0
    scaffold = [(c, kw) for c, kw in recorded if "--scaffold-only" in c]
    assert len(scaffold) == 1, recorded
    argv, kwargs = scaffold[0]
    assert argv[:4] == running_okg_argv(), argv
    assert kwargs.get("cwd") is None, kwargs


def _decoy_okg_on_path(tmp_path: Path, monkeypatch) -> Path:
    """Put a different `okg` first on PATH, as a developer checkout would."""
    bin_dir = tmp_path / "other-install" / "bin"
    bin_dir.mkdir(parents=True)
    decoy = bin_dir / "okg"
    decoy.write_text("#!/bin/sh\nexit 97\n")
    decoy.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    return decoy


@pytest.mark.parametrize("which", ["base", "deployment-check"])
def test_the_scheduled_health_check_child_is_this_okg(
    tmp_path: Path, monkeypatch, which: str,
) -> None:
    """Issue #2301: the runtime worker's doctor child is the running okg.

    Before the fix it preferred the venv's `okg` console script, then any
    `okg` on PATH, and fell back to a bare `-m` without `-P`.
    """
    from okg.substrate.runtime import scheduled

    _decoy_okg_on_path(tmp_path, monkeypatch)
    recorded: list[list[str]] = []

    class _Done:
        returncode = 0
        stdout = '{"ok": true}'
        stderr = ""

    def _record(cmd, **kwargs):  # noqa: ANN001
        recorded.append(list(cmd))
        return _Done()

    monkeypatch.setattr(scheduled.subprocess, "run", _record)
    if which == "base":
        payload = scheduled._run_doctor_subprocess(deployment="probe")
        tail = ["doctor", "--deployment", "probe", "--json"]
    else:
        payload = scheduled._run_one_deployment_doctor_check(
            deployment="probe", check_name="c1",
        )
        tail = ["doctor", "--deployment", "probe", "--check", "c1", "--json"]
    assert payload["ok"] is True, payload
    assert recorded == [[*running_okg_argv(), *tail]], recorded


# --- the class guard (issues #2313, #2301) -------------------------------
#
# Every argv that starts the okg CLI goes through running_okg_argv(). The
# scan reads each list/tuple display in src/okg (a `+`-joined chain of
# displays counts as one argv) plus a few non-list forms, and reports:
#
# * ``bare-okg``    -- the literal ``"okg"`` as argv[0], whatever follows
#                      (``["okg", *args]``, ``["okg"] + args``), or a name
#                      bound to ``"okg"`` followed by an okg subcommand: it
#                      runs whatever okg is first on PATH (issue #2301);
# * ``okg-default`` -- ``x or "okg"``, a fallback to the PATH okg;
# * ``runner-okg``  -- ``uv run ... okg``, ``uv run --module okg...``,
#                      ``uvx okg``, ``pipx run okg`` or any ``run`` directly
#                      followed by ``okg``: each runs an okg chosen by a
#                      project or tool environment, not the running package;
# * ``module``      -- ``-m``/``--module`` (or fused ``-mokg...``) followed by
#                      the CLI module, spelled literally, held in a name or
#                      parameter default, imported under any alias, or read
#                      as an attribute. Every such display counts, even one
#                      that already carries ``-P``: only the helper builds it;
# * ``which-okg``   -- ``shutil.which("okg")``, the PATH lookup;
# * ``sibling-okg`` -- ``Path(sys.executable).parent / "okg"``, the venv
#                      console script, which skips ``-P``;
# * ``exec-okg``    -- ``os.execv``/``execvp``/``execvpe``/``execl*`` of
#                      ``"okg"``;
# * ``config-okg``  -- a ``{"command": "okg"}`` dict, an agent or service
#                      config entry that names the PATH okg.
#
# Not counted: a display that is an operand of a comparison (``prefix ==
# ["uv", "run", "okg"]`` recognises an argv, it launches nothing) and a
# display passed straight to a ``join``/``shell_join`` call (a hint for a
# person). Known limits, out of scope by decision: shell command strings
# (``subprocess.run("okg doctor", shell=True)``), f-strings, ``" ".join``
# of a launched list, names imported from another module, and argvs
# assembled by ``append``/``extend`` after the display.

_BARE_OKG = "bare-okg"
_OKG_DEFAULT = "okg-default"
_RUNNER_OKG = "runner-okg"
_MODULE = "module"
_WHICH_OKG = "which-okg"
_SIBLING_OKG = "sibling-okg"
_EXEC_OKG = "exec-okg"
_CONFIG_OKG = "config-okg"

_EXEC_FUNCTIONS = frozenset({
    "execv", "execve", "execvp", "execvpe", "execl", "execle", "execlp",
    "execlpe",
})

_Values = frozenset[str]


def _string_bindings(tree: ast.AST) -> dict[str, set[str]]:
    """Names bound to a string constant anywhere in the module.

    Assignments, annotated fields, parameter defaults, and an import of
    ``OKG_CLI_MODULE`` (under any alias) all bind a name.
    """
    bound: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        value: ast.AST | None = None
        targets: list[ast.AST] = []
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            positional = [*node.args.posonlyargs, *node.args.args]
            pairs = [
                *zip(positional[len(positional) - len(node.args.defaults):],
                     node.args.defaults),
                *zip(node.args.kwonlyargs, node.args.kw_defaults),
            ]
            for arg, default in pairs:
                if isinstance(default, ast.Constant) and isinstance(
                    default.value, str,
                ):
                    bound.setdefault(arg.arg, set()).add(default.value)
            continue
        if isinstance(node, ast.Assign):
            value, targets = node.value, list(node.targets)
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            value, targets = node.value, [node.target]
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "OKG_CLI_MODULE":
                    bound.setdefault(alias.asname or alias.name, set()).add(
                        OKG_CLI_MODULE,
                    )
            continue
        if not (
            isinstance(value, ast.Constant) and isinstance(value.value, str)
        ):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                bound.setdefault(target.id, set()).add(value.value)
    return bound


def _element_values(node: ast.AST, bound: dict[str, set[str]]) -> _Values:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return frozenset({node.value})
    if isinstance(node, ast.Name):
        return frozenset(bound.get(node.id, ()))
    if isinstance(node, ast.Attribute) and node.attr == "OKG_CLI_MODULE":
        return frozenset({OKG_CLI_MODULE})
    return frozenset()


def _is_literal_okg(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value == "okg"


def _flatten(node: ast.AST, consumed: set[int]) -> list[ast.AST] | None:
    """Elements of a display or a ``+`` chain of displays, else None."""
    if isinstance(node, (ast.List, ast.Tuple)):
        consumed.add(id(node))
        return list(node.elts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _flatten(node.left, consumed)
        right = _flatten(node.right, consumed)
        if left is None and right is None:
            return None
        opaque: list[ast.AST] = [ast.Starred(value=ast.Name(id="_"))]
        return [*(left if left is not None else opaque),
                *(right if right is not None else opaque)]
    return None


def _is_okg_program(values: _Values) -> bool:
    return "okg" in values or OKG_CLI_MODULE in values


def _argv_kinds(elements: list[ast.AST], values: list[_Values]) -> list[str]:
    kinds: list[str] = []
    if elements and (
        _is_literal_okg(elements[0])
        or (
            "okg" in values[0]
            and (len(values) == 1 or values[1] & SUBSTRATE_SUBCOMMANDS)
        )
    ):
        kinds.append(_BARE_OKG)
    runner = False
    for index, value in enumerate(values):
        later = values[index + 1:]
        if "uv" in value and later and "run" in later[0] and any(
            _is_okg_program(v) for v in later[1:]
        ):
            runner = True
        elif value & {"uvx", "pipx"} and any(_is_okg_program(v) for v in later):
            runner = True
        elif "run" in value and later and "okg" in later[0]:
            runner = True
    if runner:
        kinds.append(_RUNNER_OKG)
        return kinds
    for first, second in zip(values, values[1:]):
        if first & {"-m", "--module"} and OKG_CLI_MODULE in second:
            kinds.append(_MODULE)
    for value in values:
        if any(v.startswith("-m") and v[2:] == OKG_CLI_MODULE for v in value):
            kinds.append(_MODULE)
    return kinds


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return None


def _mentions_sys_executable(node: ast.AST) -> bool:
    return any(
        isinstance(sub, ast.Attribute) and sub.attr == "executable"
        for sub in ast.walk(node)
    )


def _bypasses_in_source(text: str, filename: str = "<planted>") -> Counter:
    tree = ast.parse(text, filename=filename)
    bound = _string_bindings(tree)
    parents: dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[id(child)] = node
    consumed: set[int] = set()
    found: Counter = Counter()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _call_name(node)
            if (
                name == "which"
                and node.args
                and _element_values(node.args[0], bound) == {"okg"}
            ):
                found[_WHICH_OKG] += 1
            if (
                name in _EXEC_FUNCTIONS
                and node.args
                and _element_values(node.args[0], bound) == {"okg"}
            ):
                found[_EXEC_OKG] += 1
        if (
            isinstance(node, ast.BoolOp)
            and isinstance(node.op, ast.Or)
            and any(_is_literal_okg(value) for value in node.values)
        ):
            found[_OKG_DEFAULT] += 1
        if (
            isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Div)
            and _is_literal_okg(node.right)
            and _mentions_sys_executable(node.left)
        ):
            found[_SIBLING_OKG] += 1
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "command"
                    and value is not None
                    and _element_values(value, bound) == {"okg"}
                ):
                    found[_CONFIG_OKG] += 1
        if id(node) in consumed:
            continue
        parent = parents.get(id(node))
        if isinstance(parent, ast.BinOp) and isinstance(parent.op, ast.Add):
            # The outermost `+` chain is read as one argv.
            if isinstance(node, (ast.List, ast.Tuple, ast.BinOp)):
                continue
        if isinstance(parent, ast.Compare):
            continue
        if isinstance(parent, ast.Call) and _call_name(parent) in {
            "join", "shell_join",
        }:
            # `shlex.join([...])` renders a hint for a person to read.
            continue
        elements = _flatten(node, consumed)
        if elements is None:
            continue
        values = [_element_values(elt, bound) for elt in elements]
        for kind in _argv_kinds(elements, values):
            found[kind] += 1
    return found


# Remaining bypasses, each with the reason it stays. This list may only
# shrink: a count higher than the real one also fails, so a converted site
# cannot leave a stale allowance behind.
_KNOWN_BYPASSES: dict[tuple[str, str], tuple[int, str]] = {
    ("bench/agent_memory/okgmcp.py", _OKG_DEFAULT): (
        1, "benchmark harness, same line as its which(okg) entry: OKG_BIN or "
        "a uv project directory first, PATH okg last on a bench host",
    ),
    ("substrate/cli/connector_security.py", _BARE_OKG): (
        1, "SUBSTRATE_PROCESS_MARKERS names process-name markers to "
        "recognise; nothing is launched",
    ),
    ("substrate/db/connection.py", _OKG_DEFAULT): (
        1, "application_name or okg is the Postgres application_name label, "
        "not a program",
    ),
    ("substrate/db/management.py", _BARE_OKG): (
        1, "(okg, schema) is a schema-qualified SQL identifier, not an argv",
    ),
    ("substrate/db/migrate.py", _BARE_OKG): (
        1, "(okg, v_generations) is a schema-qualified view name, not an argv",
    ),
    ("substrate/deployment_promotion_operations.py", _RUNNER_OKG): (
        1, "legacy dogfood shutdown runs uv run okg with cwd set to "
        "config.source_repo, the source checkout the promotion releases from",
    ),
    ("substrate/deployment_upgrade.py", _SIBLING_OKG): (
        1, "_okg_executable only renders recovery commands for the operator "
        "to type (_display_recovery_command); nothing launches them",
    ),
    ("substrate/extension_package_v2.py", _BARE_OKG): (
        1, "denied_import_prefixes lists module prefixes an extension may not "
        "import; not an argv",
    ),
    ("substrate/library/agent_runtimes/ssh_stdio.py", _CONFIG_OKG): (
        1, "paired-workspace SSH bridge config; paired_workspace.py pins "
        "mcp_command to the literal okg client bridge, so the saved config "
        "names the PATH okg; tracked as the open defect #2428",
    ),
    ("substrate/library/profiles/academic-researcher/cli/install.py", _OKG_DEFAULT): (
        2, "hostname sanitising falls back to the string okg for a name; "
        "not a program",
    ),
    ("substrate/library/sources/profile_measurements.py", _OKG_DEFAULT): (
        1, "target_kind or okg defaults a measurement label; not a program",
    ),
    ("substrate/support/bug_report.py", _BARE_OKG): (
        1, "(okg, pz-okg) are distribution names whose versions the report "
        "reads; nothing is launched",
    ),
    ("bench/ab/arms/claude_code.py", _RUNNER_OKG): (
        1, "benchmark arm serves MCP from an explicit okg checkout "
        "(uv run --project <okg_repo_root>)",
    ),
    ("bench/agent_memory/okgmcp.py", _WHICH_OKG): (
        1, "benchmark harness; OKG_BIN or a uv project directory is the "
        "normal path, PATH only as the last resort on a bench host",
    ),
    ("substrate/chat/source_admin.py", _BARE_OKG): (
        1, "retry_plan default prefix renders a human-readable hint; the "
        "one caller that executes the plan (cli/chat.py) passes the helper",
    ),
    ("substrate/cli/dogfood.py", _MODULE): (
        1, "_launchd_okg_argv rewrites the worker service definition "
        "(uv run okg -> uv run --module); the service runs in its checkout "
        "by design (deployment_apply service_authority)",
    ),
    ("substrate/cli/dogfood.py", _RUNNER_OKG): (
        3, "dogfood control plan for a source-checkout worker; its foreground "
        "and status rows mirror the service authority `uv run okg runtime "
        "worker`, and deployment apply never takes the foreground adapter",
    ),
    ("substrate/cli/poros.py", _BARE_OKG): (
        1, "serve_command is printed as the next step for the operator; "
        "nothing is launched",
    ),
    ("substrate/cli/main.py", _WHICH_OKG): (
        1, "diagnostic report of which okg is on PATH; nothing is launched",
    ),
    ("substrate/deployment_promotion_operations.py", _MODULE): (
        1, "starts through runtime/release_launcher.py, which adds -I",
    ),
    ("substrate/environment_services.py", _MODULE): (
        1, "starts through runtime/release_launcher.py, which adds -I",
    ),
    ("substrate/library/agent_runtimes/coordination_client.py", _BARE_OKG): (
        1, "command run on the REMOTE host over ssh, where okg is that "
        "host's install",
    ),
    ("substrate/pact/bench/commands.py", _BARE_OKG): (
        2, "prefix tables that classify recorded command text; not launched",
    ),
    ("substrate/pact/bench/commands.py", _RUNNER_OKG): (
        2, "prefix tables that classify recorded command text; not launched",
    ),
    ("substrate/pact/agent_install.py", _BARE_OKG): (
        1, "REPOSITORY_KINDS (okg, generic) lists the accepted --repository "
        "values; nothing is launched",
    ),
    ("substrate/pact/repair.py", _BARE_OKG): (
        1, "prefix table that classifies recommended command text; not "
        "launched",
    ),
    ("substrate/pact/repair.py", _RUNNER_OKG): (
        1, "prefix table that classifies recommended command text; not "
        "launched",
    ),
}


def _scan_src_okg() -> dict[tuple[str, str], int]:
    helper = _SRC_OKG / "substrate" / "cli" / "child_argv.py"
    found: dict[tuple[str, str], int] = {}
    for path in sorted(_SRC_OKG.rglob("*.py")):
        if path == helper:
            continue
        counts = _bypasses_in_source(
            path.read_text(encoding="utf-8"), filename=str(path),
        )
        rel = path.relative_to(_SRC_OKG).as_posix()
        for kind, n in counts.items():
            found[(rel, kind)] = n
    return found


@pytest.mark.parametrize(
    ("source", "kind"),
    [
        pytest.param(
            'import sys\nargv = [sys.executable, "-m", "okg.substrate.cli.main"]\n',
            _MODULE, id="literal-module",
        ),
        pytest.param(
            'import sys\nMOD = "okg.substrate.cli.main"\n'
            'argv = [sys.executable, "-m", MOD, "doctor"]\n',
            _MODULE, id="module-in-constant",
        ),
        pytest.param(
            "import sys\n"
            "from okg.substrate.cli.child_argv import OKG_CLI_MODULE as M\n"
            'argv = (sys.executable, "-m", M)\n',
            _MODULE, id="module-imported-alias",
        ),
        pytest.param(
            'import sys\nargv = [sys.executable, "-mokg.substrate.cli.main"]\n',
            _MODULE, id="fused-m-flag",
        ),
        pytest.param(
            'import sys\nargv = [sys.executable, "-m"] + '
            '["okg.substrate.cli.main", "doctor"]\n',
            _MODULE, id="plus-joined",
        ),
        pytest.param(
            'argv = ["okg", "doctor", "--json"]\n',
            _BARE_OKG, id="bare-okg-argv0",
        ),
        pytest.param(
            'argv = ("okg",) + ("doctor",)\n',
            _BARE_OKG, id="bare-okg-plus-joined",
        ),
        pytest.param(
            'def probe(okg_cli="okg"):\n'
            '    return [okg_cli, "dogfood", "status"]\n',
            _BARE_OKG, id="bare-okg-parameter-default",
        ),
        pytest.param(
            'argv = ["uv", "run", "okg", "jobs", "enqueue-source"]\n',
            _RUNNER_OKG, id="uv-run-okg",
        ),
        pytest.param(
            'argv = ["uv", "run", "--module", "okg.substrate.cli.main"]\n',
            _RUNNER_OKG, id="uv-run-module",
        ),
        pytest.param(
            'argv = ["uv", "run", "--project", "x", "okg", "mcp-serve"]\n',
            _RUNNER_OKG, id="uv-run-options-okg",
        ),
        pytest.param(
            'import shutil\nbinary = shutil.which("okg")\n',
            _WHICH_OKG, id="which-okg",
        ),
        pytest.param(
            'def f(args):\n    return ["okg", *args]\n',
            _BARE_OKG, id="bare-okg-then-starred",
        ),
        pytest.param(
            'def f(args):\n    return ["okg"] + args\n',
            _BARE_OKG, id="bare-okg-plus-name",
        ),
        pytest.param(
            'def f(sub):\n    return ["okg", sub, "--json"]\n',
            _BARE_OKG, id="bare-okg-then-name",
        ),
        pytest.param(
            'def f(okg_cli=None):\n    return [okg_cli or "okg", "doctor"]\n',
            _OKG_DEFAULT, id="or-okg-default",
        ),
        pytest.param(
            'import sys\nfrom pathlib import Path\n'
            'binary = Path(sys.executable).parent / "okg"\n',
            _SIBLING_OKG, id="venv-sibling-okg",
        ),
        pytest.param(
            'import os\ndef f(argv):\n    os.execvp("okg", argv)\n',
            _EXEC_OKG, id="execvp-okg",
        ),
        pytest.param(
            'import os\ndef f(argv):\n    os.execv("okg", argv)\n',
            _EXEC_OKG, id="execv-okg",
        ),
        pytest.param(
            'import os\ndef f(argv, env):\n    os.execvpe("okg", argv, env)\n',
            _EXEC_OKG, id="execvpe-okg",
        ),
        pytest.param(
            'entry = {"command": "okg", "args": ["mcp-serve"]}\n',
            _CONFIG_OKG, id="config-command-okg",
        ),
        pytest.param(
            'argv = ["uvx", "okg", "doctor"]\n',
            _RUNNER_OKG, id="uvx-okg",
        ),
        pytest.param(
            'argv = ["pipx", "run", "okg", "doctor"]\n',
            _RUNNER_OKG, id="pipx-run-okg",
        ),
        pytest.param(
            'argv = ["poetry", "run", "okg", "doctor"]\n',
            _RUNNER_OKG, id="run-then-okg",
        ),
    ],
)
def test_the_guard_catches_a_planted_bypass(source: str, kind: str) -> None:
    assert _bypasses_in_source(source)[kind] == 1, _bypasses_in_source(source)


@pytest.mark.parametrize(
    "source",
    [
        pytest.param(
            "from okg.substrate.cli.child_argv import running_okg_argv\n"
            'argv = [*running_okg_argv(), "doctor", "--json"]\n',
            id="helper",
        ),
        pytest.param(
            'ok = prefix[1:] == ["-m", "okg.substrate.cli.main"]\n',
            id="comparison-recognizer",
        ),
        pytest.param(
            'hint = "uv run okg doctor --json"\n',
            id="rendered-hint-string",
        ),
        pytest.param(
            'import sys\nargv = [sys.executable, "-m", "okg.viz"]\n',
            id="other-module",
        ),
        pytest.param(
            'argv = ["git", "okg"]\n',
            id="okg-not-argv0",
        ),
        pytest.param(
            'import os\nos.execvp("git", ["git", "status"])\n',
            id="exec-other-program",
        ),
        pytest.param(
            'from pathlib import Path\nsrc = Path("repo") / "src" / "okg"\n',
            id="source-tree-path",
        ),
        pytest.param(
            'entry = {"command": "/abs/python", "args": ["-P", "-m", "x"]}\n',
            id="config-absolute-command",
        ),
        pytest.param(
            'import shlex\nhint = shlex.join(["okg", "doctor", "--json"])\n',
            id="joined-hint",
        ),
    ],
)
def test_the_guard_passes_a_safe_source(source: str) -> None:
    assert not _bypasses_in_source(source), _bypasses_in_source(source)


def test_no_okg_cli_child_argv_bypasses_the_helper() -> None:
    found = _scan_src_okg()
    new = {
        key: n for key, n in found.items()
        if n > _KNOWN_BYPASSES.get(key, (0, ""))[0]
    }
    assert not new, (
        "start the okg CLI through running_okg_argv() from "
        + "okg.substrate.cli.child_argv (issues #2313, #2301), not a bare "
        + f"okg, uv run okg, which(okg) or -m argv: {new}"
    )
    # The agent-memory benchmark (src/okg/bench/agent_memory/) is not in the
    # public copy; its entries cannot be seen there (tests/mirror_scope.py).
    stale = {
        key: n for key, (n, _reason) in _KNOWN_BYPASSES.items()
        if found.get(key, 0) < n
        and not (is_public_copy() and is_private(f"src/okg/{key[0]}"))
    }
    assert not stale, f"lower these _KNOWN_BYPASSES counts: {stale}"


def test_every_remaining_bypass_names_its_reason() -> None:
    for key, (count, reason) in _KNOWN_BYPASSES.items():
        assert count > 0, key
        assert len(reason.split()) >= 5, key
