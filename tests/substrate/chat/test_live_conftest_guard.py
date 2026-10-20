"""The live proofs must not be collected by an ordinary test run.

This guard is asserted rather than assumed because the first version of it
did not work: ``collect_ignore_glob`` in ``tests/live/conftest.py`` only
covers files in that directory, and the tests live one level down in
``chat/``. Every ordinary ``pytest`` run collected all six live tests and
the guard looked like it was doing its job.

So the check is a real pytest invocation in a subprocess, both ways, for
EVERY subtree flag. There are two now -- ``OKG_CHAT_LIVE_PROOF`` for the
chat proofs and ``OKG_OPENSEARCH_LIVE`` for the OpenSearch reference walk
(#2014) -- and each is checked in both directions, because a conftest that
honoured one flag and silently ignored the other would leave half the live
tests running in every lane while this file still passed.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
LIVE_DIR = REPO_ROOT / "tests/live"


#: Every live subtree: the flag that opts into it, and one test name that
#: only exists inside it. The witness is what makes each direction real --
#: asserting on "no tests collected" alone would pass against a conftest
#: that ignored the whole tree unconditionally.
SUBTREES = (
    ("OKG_CHAT_LIVE_PROOF", "test_audit_payload_equals_recorded_request_body"),
    ("OKG_OPENSEARCH_LIVE", "test_the_agent_walk_reaches_the_live_cluster_through_mcp"),
    ("OKG_PG_LAUNCH_LIVE", "test_rendered_descriptor_completes_first_time_setup"),
    (
        "OKG_MCP_DEPLOYMENT_LIVE",
        "test_the_deployment_serves_live_relations_and_separates_two_principals",
    ),
)

ALL_FLAGS = tuple(flag for flag, _ in SUBTREES)


def _collect(set_flag: str | None) -> subprocess.CompletedProcess:
    """Collect `tests/live` with at most one subtree flag set."""
    env = dict(os.environ)
    for flag in ALL_FLAGS:
        env.pop(flag, None)
    if set_flag is not None:
        env[set_flag] = "1"
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "pytest", str(LIVE_DIR), "-q", "--collect-only"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_live_proofs_are_not_collected_without_any_flag() -> None:
    """The default, for every subtree at once."""
    result = _collect(None)

    for _flag, witness in SUBTREES:
        assert witness not in result.stdout, (
            f"a live proof ({witness}) is being collected by an ordinary "
            f"run:\n{result.stdout[-2000:]}"
        )
    assert "no tests collected" in result.stdout, result.stdout[-2000:]
    # pytest's "no tests collected" exit code, so this is a real zero rather
    # than a run that errored before it could collect anything.
    assert result.returncode == 5, (result.returncode, result.stderr[-1000:])


def _collect_copy(root: Path, set_flag: str | None) -> subprocess.CompletedProcess:
    """Collect ``root/tests/live`` in an isolated copy, as `_collect` does."""
    env = dict(os.environ)
    for flag in ALL_FLAGS:
        env.pop(flag, None)
    # The copy's own pytest.ini is the whole configuration; the repository's
    # addopts must not leak in through the environment either.
    env.pop("PYTEST_ADDOPTS", None)
    if set_flag is not None:
        env[set_flag] = "1"
    return subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            str(root / "tests/live"),
            "-q",
            "--collect-only",
            "-p",
            "no:cacheprovider",
            "--rootdir",
            str(root),
            "-c",
            str(root / "pytest.ini"),
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _write_probe(directory: Path, witness: str, message: str) -> None:
    directory.mkdir(parents=True)
    (directory / "__init__.py").write_text("", encoding="utf-8")
    (directory / "test_probe.py").write_text(
        f"def {witness}() -> None:\n    raise AssertionError({message!r})\n",
        encoding="utf-8",
    )


def test_an_unlisted_subtree_is_withheld_under_every_flag(tmp_path: Path) -> None:
    """The fail-closed branch, which no other case reaches.

    `SUBTREE_FLAGS` maps a directory to the flag that opts into it, and a
    directory absent from that map is WITHHELD rather than collected. That
    branch is the one an oversight lands on: someone adds
    `tests/live/<something>/`, forgets the map entry, and the default
    decides whether their container-dependent proofs start running in every
    lane. Asserted here rather than trusted, because every other case in
    this file exercises a subtree that IS mapped.

    Uses a real directory and a real pytest collection: importing the
    conftest and calling the predicate would assert the helper, not the
    hook pytest actually consults.

    The directory lives in an isolated copy under ``tmp_path``, never in
    the checkout. The first version wrote the probe into ``tests/live/``
    and removed it afterwards; a scan test in a parallel xdist worker
    listed the file, lost it to the cleanup, and failed on dev Tests run
    36370525406 with FileNotFoundError. The copy is faithful because the
    conftest resolves every path from its own ``__file__``: it is the
    repository's file byte for byte, and the mapped control below proves
    the copy still honours a flag rather than ignoring everything.
    """
    live = tmp_path / "tests/live"
    live.mkdir(parents=True)
    (tmp_path / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    (tmp_path / "tests/__init__.py").write_text("", encoding="utf-8")
    (live / "__init__.py").write_text("", encoding="utf-8")
    shutil.copyfile(LIVE_DIR / "conftest.py", live / "conftest.py")
    assert (live / "conftest.py").read_bytes() == (
        LIVE_DIR / "conftest.py"
    ).read_bytes()

    witness = "test_an_unlisted_live_subtree_must_not_be_collected"
    _write_probe(
        live / "zz_unlisted_probe",
        witness,
        "an unlisted tests/live subtree was collected",
    )
    # A control inside a MAPPED subtree. Without it, a copy whose conftest
    # withheld everything (or failed to load in a way that collected
    # nothing) would pass the negative for the wrong reason.
    control = "test_a_mapped_live_subtree_is_collected_when_asked"
    _write_probe(live / "chat", control, "collection-only control")

    for flag in (None, *ALL_FLAGS):
        result = _collect_copy(tmp_path, flag)
        assert witness not in result.stdout, (
            f"an unlisted subtree was collected with flag={flag!r}; the "
            f"map in tests/live/conftest.py must fail closed:\n"
            f"{result.stdout[-2000:]}"
        )
        expect_control = flag == "OKG_CHAT_LIVE_PROOF"
        assert (control in result.stdout) is expect_control, (
            f"flag={flag!r}: the mapped chat/ control was "
            f"{'not ' if expect_control else ''}collected in the copy, so "
            f"the copied conftest is not behaving like the real one:\n"
            f"{result.stdout[-2000:]}\n{result.stderr[-1000:]}"
        )


@pytest.mark.parametrize("flag,witness", SUBTREES)
def test_each_flag_opts_into_its_own_subtree_only(flag: str, witness: str) -> None:
    """The negative above is only meaningful if each positive works.

    Without this, a conftest that raised on import, or a path typo, would
    make the first test pass for the wrong reason. The `only` half matters
    as much: one flag must not drag the other subtree's proofs in, or an
    operator asking for a two-minute OpenSearch walk would also start a
    pinned Open WebUI container.
    """
    result = _collect(flag)

    assert witness in result.stdout, result.stdout[-2000:]
    for other_flag, other_witness in SUBTREES:
        if other_flag == flag:
            continue
        assert other_witness not in result.stdout, (
            f"setting {flag} also collected {other_witness}, which belongs "
            f"to {other_flag}:\n{result.stdout[-2000:]}"
        )
