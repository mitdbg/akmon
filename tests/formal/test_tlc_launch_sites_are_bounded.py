"""Every place that spawns TLC must go through the shared resource profile.

The caps in `okg.substrate.formal_tlc` only bound the machine at the launch
sites that use them. Six checkers and model-contract tests built their own
`java ... tlc2.TLC` command line and so kept taking every core and an
unbounded heap regardless of the profile -- the failure mode is silent,
because an uncapped run still proves the same properties, it just makes the
host unusable while doing it.

This test enumerates the launch sites structurally rather than trusting
review. It checks the argv literal itself, not the file: a site passes only
when the same list/tuple that names TLC also splats `tlc_jvm_args(...)`, so a
file with two argv builders cannot pass on the strength of the capped one, and
a mention in a comment counts for nothing.
"""

from __future__ import annotations

import ast
from functools import cache
import re
from pathlib import Path

import pytest
from tests.mirror_scope import is_public_copy
from tests.pytest.tracked_files import tracked_files

REPO_ROOT = Path(__file__).resolve().parents[2]
SEARCH_ROOTS = ("scripts", "src", "tests", "ops", "tools", "deployments", "bench")

# The profile's own definition site, plus the one file whose `tlc2.TLC` literal
# is a canned argv handed to build_formal_report_rows for report-shape
# assertions -- it never reaches subprocess, so there is no machine to bound.
EXEMPT = {
    Path("src/okg/substrate/formal_tlc.py"),
    Path("tests/pact/test_formal_tlc_runner.py"),
}


# Both spellings of the gitignored jar path: the "\.tools/tla2tools.jar"
# string literal and the ".tools" / "tla2tools.jar" path join.
_PINNED_JAR_PATTERN = re.compile(
    r"""\.tools/tla2tools\.jar|["']\.tools["']\s*/\s*["']tla2tools\.jar["']"""
)


def _is_profile_call(element: ast.expr) -> bool:
    """True for a `*tlc_jvm_args(...)` element inside an argv literal."""
    if not isinstance(element, ast.Starred):
        return False
    call = element.value
    return (
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "tlc_jvm_args"
    )


def _names_tlc(element: ast.expr) -> bool:
    """True when this argv element starts TLC.

    Two spellings run the model checker: naming the class after `-cp`, and
    `-jar <tla2tools.jar>`, whose manifest Main-class IS tlc2.TLC. The second
    would otherwise be invisible -- it contains no class-name literal at all.
    """
    if not isinstance(element, ast.Constant) or not isinstance(element.value, str):
        return False
    return element.value == "tlc2.TLC" or element.value == "-jar"


@cache
def _launch_sites() -> tuple[tuple[Path, int, bool], ...]:
    """(path, lineno, is_bounded) for every argv literal that starts TLC.

    Cached: this walks the AST of every .py file in the repo's source roots,
    and the `fast` QA lane enforces a wall-clock budget that three unmemoized
    walks measurably ate into.
    """
    sites: list[tuple[Path, int, bool]] = []
    for root in SEARCH_ROOTS:
        if not (REPO_ROOT / root).is_dir():
            # Unchanged from the disk walk, which also found nothing under a
            # root this checkout does not have (e.g. ``tools``).
            continue
        for path in tracked_files(REPO_ROOT / root, "*.py"):
            relative = path.relative_to(REPO_ROOT)
            if relative in EXEMPT:
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if not isinstance(node, (ast.List, ast.Tuple)):
                    continue
                if not any(_names_tlc(element) for element in node.elts):
                    continue
                bounded = any(_is_profile_call(element) for element in node.elts)
                sites.append((relative, node.lineno, bounded))
    return tuple(sites)


def test_launch_sites_are_discoverable() -> None:
    """Guard the guard: if the detector finds nothing, it proves nothing."""
    sites = _launch_sites()
    assert len(sites) >= 6, (
        "the TLC launch-site detector found almost nothing, so the boundedness "
        f"assertion below would pass vacuously; found: {sites}"
    )


@pytest.mark.parametrize(
    "path,lineno,bounded",
    _launch_sites(),
    ids=lambda value: value.name if isinstance(value, Path) else str(value),
)
def test_every_tlc_launch_site_uses_the_shared_profile(
    path: Path, lineno: int, bounded: bool
) -> None:
    assert bounded, (
        f"{path}:{lineno} builds a TLC command without the shared resource "
        "profile. Splat *tlc_jvm_args(tlc_workers()) into this same argv "
        "literal, ahead of the tlc2.TLC class name, or the run takes an "
        "unbounded heap on whatever machine it lands on."
    )


def test_every_test_launch_site_resolves_the_jar_through_an_overridable_resolver() -> None:
    """No TEST launch site may pin the gitignored `.tools/tla2tools.jar` path.

    That path is gitignored, so in a fresh checkout the jar is absent and the
    launch fails. A guard that then asserts only `returncode != 0` reads the
    failure as its mutant being caught, and the invariant-text assertion
    behind it reds against the model instead of the missing tool (#1354). A
    resolver that honours TLA2TOOLS_JAR -- `find_tla2tools_jar`, the shared
    `tests.helpers.tlc` module, or a local reader of the same variable -- lets
    the site either find the jar or skip with a reason that names the
    provisioning path.

    Scoped to tests on purpose. A checker that cannot find the jar already
    refuses with an explicit provisioning message rather than a misleading
    assertion, so it is not the #1354 defect class. One checker,
    scripts/check_formal_deployment_control_plane_kernel.py, does still read
    the gitignored path directly and so ignores TLA2TOOLS_JAR; its bytes are
    digest-bound into another change's formal evidence, and
    scripts/repair_formal_evidence.py cannot re-prove it (it emits
    --property-id, which that checker does not accept), so moving it is
    follow-up work with its own proof, not a rider on this change.
    """
    offenders = []
    test_sites = {
        path
        for path, _lineno, _bounded in _launch_sites()
        if path.parts[0] == "tests"
    }
    # Two of the launch sites are in tests this repository keeps out of its
    # public copy; there, the one shipped site must still be found. (The
    # deployment-backup declassification witness moved from a test into
    # scripts/check_formal_deployment_backup_policy.py, PACT
    # lane-s-storage-drop-copies, so tests hold one launch site fewer.)
    assert len(test_sites) >= (1 if is_public_copy() else 3), (
        "the test-launch-site filter found almost nothing, so the assertion "
        f"below would pass vacuously; found: {sorted(map(str, test_sites))}"
    )
    for path in sorted(test_sites):
        text = (REPO_ROOT / path).read_text(encoding="utf-8")
        # Both halves are required. Mentioning the resolver is not enough: a
        # file can import it and still build its argv from a hardcoded path,
        # which is exactly the regression this guard exists to catch, so the
        # gitignored literal must be ABSENT as well.
        resolves_through_helper = (
            "find_tla2tools_jar" in text or "tests.helpers.tlc" in text
        )
        pins_the_path = _PINNED_JAR_PATTERN.search(text) is not None
        if not resolves_through_helper or pins_the_path:
            offenders.append(str(path))
    assert not offenders, (
        "these TLC launch sites in tests either resolve the jar without an "
        "environment override or still spell out the gitignored "
        f".tools/tla2tools.jar path: {offenders}. Import tests.helpers.tlc, "
        "use its TLA_JAR, and skip with its explicit reason instead."
    )


def test_assert_tlc_ran_separates_a_launch_failure_from_a_missed_mutant() -> None:
    """The helper must fail on a launch failure, not let it read as a pass."""
    import subprocess

    from tests.helpers import tlc as tlc_support

    ran = subprocess.CompletedProcess(
        # A string, not a list: an argv list literal naming the class would
        # register this file as a launch site in the scan above.
        args="java -cp jar tlc2.TLC",
        returncode=1,
        stdout="TLC2 Version 1.7.4\nInvariant X is violated.\n",
        stderr="",
    )
    tlc_support.assert_tlc_ran(ran)  # no banner-less failure: returns quietly

    never_started = subprocess.CompletedProcess(
        args="java -cp jar tlc2.TLC",
        returncode=1,
        stdout="",
        stderr="Error: Could not find or load main class tlc2.TLC\n",
    )
    # pytest.fail raises Failed, exposed as pytest.fail.Exception.
    with pytest.raises(pytest.fail.Exception) as excinfo:
        tlc_support.assert_tlc_ran(never_started)
    message = str(excinfo.value)
    assert "TLC failed to launch" in message
    assert "jar=" in message, "the failure must name the jar it tried to use"


def test_tlc_skip_reason_names_the_provisioning_path(monkeypatch) -> None:
    """A skipped guard must say how to make it run, not just that it did not.

    Both branches are asserted. Checking only the branch this machine happens
    to take would let the other one say nothing actionable -- a guard passing
    for the wrong reason, which is the defect this module removes.
    """
    from tests.helpers import tlc as tlc_support

    for java, expected_term in ((None, "Java"), ("/usr/bin/java", "TLA2TOOLS_JAR")):
        monkeypatch.setattr(tlc_support, "JAVA", java)
        reason = tlc_support.tlc_skip_reason()
        assert "TLC guard skipped" in reason
        assert expected_term in reason
        assert "scripts/bootstrap_formal_admission.py" in reason, (
            f"the skip reason for java={java!r} names no provisioning path: "
            f"{reason}"
        )
        assert "do NOT run" in reason, (
            "the skip must say the guards did not run, not imply coverage"
        )


def test_no_launch_site_reads_the_worker_env_directly() -> None:
    """OKG_TLC_WORKERS has to be read in one place.

    A direct `os.environ.get("OKG_TLC_WORKERS", "auto")` reintroduces both the
    take-the-machine default and an empty-string footgun: an exported-but-empty
    value passes `-workers ''` to TLC, which dies on the empty argument.
    """
    offenders = sorted(
        {
            str(path)
            for path, _lineno, _bounded in _launch_sites()
            if 'environ.get("OKG_TLC_WORKERS"'
            in (REPO_ROOT / path).read_text(encoding="utf-8")
        }
    )
    assert not offenders, (
        "these launch sites read OKG_TLC_WORKERS directly instead of calling "
        f"tlc_workers(): {offenders}"
    )
