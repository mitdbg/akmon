"""Install journeys: extract the install pages' commands and run them.

PACT ``journey-install-tests``. The bugs people hit in September 2026 were on
install paths no test ran: a wheel installed outside a source checkout
(issue #2470), a machine that has Codex session logs (issue #2469), and a
fresh ``academic-researcher`` install (issue #2331). The executed golden path
runs a different page (``docs/runbooks/workspace-graph/setup.md``) from the
checkout with empty Codex folders, so it could not see any of them.

This module is the coupling point between two operator pages and the journey
tests in ``tests/e2e/test_first_run_golden_path.py``:

* ``docs/operate/install-packaged.md``, section "Install from a wheel": build
  a wheel, make a virtual environment, install the wheel, check the version,
  then "follow Install OKG from step 2".
* ``docs/operate/install.md``: step 3's ``okg install`` command, the Codex
  recovery command, step 4's status and search commands, and the profile dry
  run named under "Next steps".
* ``docs/use/profiles/academic-researcher.md``, section "Install on a
  workstation": the default install command and its ``--postgres-dsn``
  option for an existing database; and section "Verify and remove the
  deployment": the ``okg doctor`` command a new installation must pass
  (issue #2281).

Every command is lifted from the page by an exact anchor. When a page changes
a command, the anchor stops matching and extraction raises
:class:`InstallDocExtractionError`, so the page and the journey cannot drift
apart silently. The one deliberate substitution is the wheel install: the
page says ``pip install``, which downloads every dependency from PyPI, and the
journey must not use the network. The journey installs the wheel's locked
runtime dependencies from ``uv.lock`` (:func:`offline_dependency_sync_argv`),
then the same wheel with ``--no-deps`` into the same environment, then checks
that every declared requirement is met, all from the uv cache the lane has
already filled.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_PAGE = REPO_ROOT / "docs" / "operate" / "install.md"
PACKAGED_PAGE = REPO_ROOT / "docs" / "operate" / "install-packaged.md"
ACADEMIC_PAGE = REPO_ROOT / "docs" / "use" / "profiles" / "academic-researcher.md"

#: The local-stack DSN install.md pins in step 3. The journey swaps it for
#: its own throwaway database.
INSTALL_DOC_DSN = "postgresql://postgres:okg@127.0.0.1:5433/okg"

WHEEL_SECTION = "## Install from a wheel"
STEP3_SECTION = "## 3. Build the graph"
STEP4_SECTION = "## 4. Check it and search it"
NEXT_STEPS_SECTION = "## Next steps"
ACADEMIC_SECTION = "## Install on a workstation"
ACADEMIC_VERIFY_SECTION = "## Verify and remove the deployment"

#: The example DSN the academic-researcher page gives for "a PostgreSQL
#: database you already run". The journey swaps in its own database.
ACADEMIC_DOC_DSN = "postgresql://user:pass@host:5432/okg"

#: The page's wheel venv location and wheel path, as written.
DOC_VENV = "~/okg-venv"
DOC_WHEEL = "/path/to/okg-<version>-py3-none-any.whl"

_BASH_BLOCK_RE = re.compile(r"```bash\n(.*?)```", re.DOTALL)
_PLACEHOLDER_RE = re.compile(r"<[A-Za-z][A-Za-z0-9_ -]*>")


class InstallDocExtractionError(AssertionError):
    """An install page no longer matches the journey's anchors."""


def _fail(message: str) -> None:
    raise InstallDocExtractionError(
        f"{message}\n"
        "An operator install page (docs/operate/install.md or "
        "docs/operate/install-packaged.md) no longer matches the install "
        "journey in tests/e2e/test_first_run_golden_path.py. Change the "
        "page and tests/e2e/install_journey.py together."
    )


def _section(text: str, header: str, *, page: str) -> str:
    if header not in text:
        _fail(f"{page}: section {header!r} not found")
    body = text.split(header, 1)[1]
    following = re.search(r"^## ", body, re.MULTILINE)
    return body[: following.start()] if following else body


def _one_block(section: str, anchors: tuple[str, ...], *, where: str) -> str:
    blocks = [
        block
        for block in _BASH_BLOCK_RE.findall(section)
        if all(anchor in block for anchor in anchors)
    ]
    if len(blocks) != 1:
        _fail(
            f"{where}: expected exactly one bash block containing "
            f"{list(anchors)}; found {len(blocks)}"
        )
    return blocks[0]


@dataclass(frozen=True)
class InstallJourneyCommands:
    """Raw command text lifted from the two install pages."""

    build_wheel: str
    make_venv: str
    install_wheel: str
    check_version: str
    workspace_install: str
    codex_recovery: str
    status: str
    search: str
    profile_dry_run: str
    academic_install: str
    academic_dsn_install: str
    academic_doctor: str


def extract_install_journey(
    install_text: str, packaged_text: str, academic_text: str,
) -> InstallJourneyCommands:
    """Lift the journey's commands from the pages, failing loudly on drift."""

    wheel = _section(packaged_text, WHEEL_SECTION, page="install-packaged.md")
    build_block = _one_block(
        wheel, ("uv build --wheel",), where="install-packaged.md wheel build",
    )
    venv_block = _one_block(
        wheel,
        ("-m venv", f"{DOC_VENV}/bin/pip install", "--version"),
        where="install-packaged.md wheel install",
    )
    lines = [line.strip() for line in venv_block.strip().splitlines()]
    if len(lines) != 3:
        _fail(
            "install-packaged.md wheel install block: expected three "
            f"commands (venv, pip install, --version); found {lines}"
        )
    make_venv, install_wheel, check_version = lines
    if make_venv != f"python3.12 -m venv {DOC_VENV}":
        _fail(f"install-packaged.md: venv command changed: {make_venv!r}")
    if install_wheel != f"{DOC_VENV}/bin/pip install {DOC_WHEEL}":
        _fail(f"install-packaged.md: wheel install changed: {install_wheel!r}")
    if check_version != f"{DOC_VENV}/bin/okg --version":
        _fail(f"install-packaged.md: version check changed: {check_version!r}")
    # The wheel page hands the operator to install.md from step 2. If it
    # stops doing that, the rest of this journey is no longer what the
    # page tells a wheel user to run.
    if "follow [Install OKG](install.md) from step 2" not in " ".join(
        wheel.split()
    ):
        _fail(
            "install-packaged.md no longer says to follow install.md from "
            "step 2 after installing the wheel"
        )

    step3 = _section(install_text, STEP3_SECTION, page="install.md")
    workspace_install = _one_block(
        step3,
        (
            "okg install --profile workspace-graph",
            "--deployment-name <name>",
            "--repo-root <repo>",
            f"--postgres-dsn {INSTALL_DOC_DSN}",
            "--json",
        ),
        where="install.md step 3",
    )
    # The Codex recovery: install.md says the install can still exit 1 at
    # ingest when Codex starts a new session file mid-install (#2469 fixed the
    # growing-session case) and names this command.
    if "If you use Codex" not in step3:
        _fail("install.md step 3 no longer has its 'If you use Codex' note")
    codex_recovery = _one_block(
        step3,
        ("okg run --once --deployment <name> --apply",),
        where="install.md step 3 Codex recovery",
    )

    step4 = _section(install_text, STEP4_SECTION, page="install.md")
    status = _one_block(
        step4, ("okg status --deployment <name>",), where="install.md step 4",
    )
    search = _one_block(
        step4,
        ("okg search --deployment <name> --query '<a function name>'",),
        where="install.md step 4 search",
    )
    if "latest_published_status" not in step4:
        _fail("install.md step 4 no longer names latest_published_status")

    next_steps = " ".join(
        _section(install_text, NEXT_STEPS_SECTION, page="install.md").split()
    )
    dry_run = re.search(
        r"`(okg install --profile <profile> --deployment-name <name> "
        r"--dry-run --json)`",
        next_steps,
    )
    if dry_run is None:
        _fail(
            "install.md 'Next steps' no longer names the profile dry run "
            "`okg install --profile <profile> --deployment-name <name> "
            "--dry-run --json`"
        )
    if "`academic-researcher`" not in next_steps:
        _fail("install.md 'Next steps' no longer names academic-researcher")

    academic = _section(
        academic_text, ACADEMIC_SECTION, page="academic-researcher.md",
    )
    academic_install = _one_block(
        academic,
        ("okg install --profile academic-researcher --name <name> --json",),
        where="academic-researcher.md wheel install",
    )
    # The journey runs this command from the wheel, outside a checkout,
    # because the page says that is the route it documents.
    if "From an installed wheel" not in " ".join(academic.split()):
        _fail(
            "academic-researcher.md no longer gives its install command as "
            "the route 'From an installed wheel'"
        )
    academic_dsn_install = _one_block(
        academic,
        (
            "okg install --profile academic-researcher --postgres-dsn "
            f"{ACADEMIC_DOC_DSN}",
        ),
        where="academic-researcher.md existing-database install",
    )

    # The page's check after the install: "`doctor` should complete without
    # an error." The journey runs that command (with --json) against the
    # fresh deployment, so a new installation that doctor rejects fails the
    # lane (issue #2281).
    verify = _section(
        academic_text, ACADEMIC_VERIFY_SECTION, page="academic-researcher.md",
    )
    doctor_block = _one_block(
        verify,
        ("okg doctor --deployment <name>",),
        where="academic-researcher.md verify",
    )
    doctor_lines = [
        line.strip() for line in doctor_block.splitlines()
        if line.strip().startswith("okg doctor")
    ]
    if doctor_lines != ["okg doctor --deployment <name>"]:
        _fail(
            "academic-researcher.md verify block: expected the one command "
            f"'okg doctor --deployment <name>'; found {doctor_lines}"
        )
    if "`doctor` should complete without an error" not in " ".join(
        verify.split()
    ):
        _fail(
            "academic-researcher.md no longer says `doctor` should complete "
            "without an error after the install"
        )

    return InstallJourneyCommands(
        build_wheel=build_block.strip(),
        make_venv=make_venv,
        install_wheel=install_wheel,
        check_version=check_version,
        workspace_install=workspace_install.strip(),
        codex_recovery=codex_recovery.strip(),
        status=status.strip(),
        search=search.strip(),
        profile_dry_run=dry_run.group(1),
        academic_install=academic_install.strip(),
        academic_dsn_install=academic_dsn_install.strip(),
        academic_doctor=doctor_lines[0],
    )


def load_install_journey() -> InstallJourneyCommands:
    return extract_install_journey(
        INSTALL_PAGE.read_text(encoding="utf-8"),
        PACKAGED_PAGE.read_text(encoding="utf-8"),
        ACADEMIC_PAGE.read_text(encoding="utf-8"),
    )


def substitute(command: str, mapping: dict[str, str]) -> str:
    """Fill a command's placeholders; any placeholder left over fails."""

    out = command
    # Longest keys first, so the DSN literal is replaced before any shorter
    # key could touch part of it.
    for key in sorted(mapping, key=len, reverse=True):
        out = out.replace(key, mapping[key])
    leftovers = sorted(set(_PLACEHOLDER_RE.findall(out)))
    if leftovers:
        _fail(
            f"command still holds placeholder(s) {leftovers} after "
            f"substitution; the page grew one this journey does not know:\n"
            f"{out}"
        )
    return out


def offline_dependency_sync_argv(*, uv: str) -> list[str]:
    """Install the wheel's locked runtime dependencies, without the network.

    Run from a source tree holding ``pyproject.toml`` and ``uv.lock``, with
    ``UV_PROJECT_ENVIRONMENT`` naming the target environment. ``uv sync
    --locked`` installs from the lockfile's own file URLs, so it needs only
    the cached wheels. The runner image's warmed uv cache holds those (it
    runs the same locked sync), but it holds no package-index pages. A
    resolving install such as ``uv pip install --offline <wheel>`` needs
    those pages, and it failed on the database pod with "pydantic==2.13.5
    needs to be downloaded from a registry". ``--inexact`` keeps the pip
    that ``python -m venv`` put there; the project, dev groups and extras
    are not installed.
    """

    return [
        uv, "sync", "--locked", "--offline", "--inexact",
        "--no-install-project", "--no-dev",
    ]


def offline_wheel_install_argv(
    *, uv: str, venv_python: Path, wheel: Path,
) -> list[str]:
    """The page's ``pip install <wheel>``, without the network or resolution.

    Same wheel, same target environment. Its dependencies are already there
    from :func:`offline_dependency_sync_argv`; :func:`dependency_check_argv`
    then proves the wheel's own declared requirements are all satisfied, so
    a requirement the lockfile lacks still fails.
    """

    return [
        uv, "pip", "install", "--offline", "--no-deps",
        "--python", str(venv_python),
        str(wheel),
    ]


def dependency_check_argv(*, uv: str, venv_python: Path) -> list[str]:
    """Check that every installed distribution's requirements are met."""

    return [uv, "pip", "check", "--python", str(venv_python)]


# ---------------------------------------------------------------------------
# signatures of the open issues this journey reproduces
# ---------------------------------------------------------------------------

class KnownOpenIssue(AssertionError):
    """The step failed with the exact signature of a named open issue.

    Journey tests mark these steps ``xfail(strict=True, raises=
    KnownOpenIssue)``. Any other failure is a real test failure, and a pass
    (the issue fixed) turns the strict xfail red so the marker is removed.
    """


def matches_2469(output: str) -> bool:
    """#2469: codex_sessions refuses at ingest on a machine with sessions."""

    return (
        "codex_sessions" in output
        and "cannot acknowledge change identity" in output
    )


def matches_2470(output: str) -> bool:
    """#2470: academic-researcher from a wheel does chdir(None)."""

    return (
        "TypeError: chdir: path should be string" in output
        and "not NoneType" in output
        and "_load_source_defaults" in output
    )


def matches_2331(output: str) -> bool:
    """#2331: a fresh academic-researcher install has nothing runnable."""

    return "no runnable sources selected for sync enqueue" in output


def codex_session_rows(*, cwd: str) -> list[dict]:
    """One small Codex rollout, in the shape Codex writes."""

    import json

    return [
        {
            "type": "session_meta",
            "timestamp": "2026-09-26T00:00:00Z",
            "payload": {
                "id": "install-journey-session-1",
                "cwd": cwd,
                "model": "gpt-5",
                "timestamp": "2026-09-26T00:00:00Z",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-09-26T00:00:01Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": "reading the publisher pipeline",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-09-26T00:00:02Z",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps(
                    {"cmd": "cat golden_pkg/pipeline.py"}
                ),
            },
        },
    ]
