"""Worker code identity and the divergence surface (issue #945).

The incident: a worker imported a source adapter, the adapter gained a
parameter 84 minutes later, and every subsequent cycle failed with a
``TypeError`` whose premise was false. Nothing compared the worker's
loaded code to the checkout it was serving.

These tests pin the two properties that make that observable: the
snapshot is frozen at boot (a value recomputed later would always agree
with the checkout and verify nothing), and the read side reports four
distinct states of which only ``matched`` is reassuring.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

from okg.substrate.cli import dogfood
from okg.substrate.runtime import code_identity


@pytest.fixture(autouse=True)
def _clean_identity():
    code_identity.clear_worker_code_identity()
    yield
    code_identity.clear_worker_code_identity()


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "checkout"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "test")
    (root / "a.txt").write_text("one\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "first")
    return root


def test_snapshot_is_frozen_at_boot(repo: Path, monkeypatch) -> None:
    """The recorded revision must not follow the checkout.

    This is the whole point of recording it: a revision re-read at
    report time would agree with HEAD forever, including throughout a
    36-hour staleness.
    """
    monkeypatch.setattr(
        code_identity,
        "_repo_root",
        lambda _start: repo,
    )
    identity = code_identity.record_worker_code_identity(deployment="d")
    boot_revision = identity.code_revision
    assert boot_revision == _git(repo, "rev-parse", "HEAD")

    (repo / "a.txt").write_text("two\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "second")
    moved_revision = _git(repo, "rev-parse", "HEAD")
    assert moved_revision != boot_revision

    report = code_identity.worker_code_identity_report()
    assert report["status"] == "recorded"
    assert report["code_revision"] == boot_revision
    assert code_identity.checkout_code_revision(repo) == moved_revision


def test_modified_loaded_module_is_reported(tmp_path: Path) -> None:
    """A module edited after this process imported it is reported stale."""
    package_root = tmp_path / "pkg"
    package_root.mkdir()
    module_path = package_root / "watched_mod.py"
    module_path.write_text("VALUE = 1\n", encoding="utf-8")
    sys.path.insert(0, str(package_root))
    try:
        code_identity.record_worker_code_identity(
            deployment="d",
            extra_roots=[package_root],
        )
        import watched_mod  # noqa: F401 - imported for its side effect

        report = code_identity.worker_code_identity_report()
        assert report["modified_module_count"] == 0

        # Edit it AFTER import. mtime resolution is coarse on some
        # filesystems, so make the change unambiguous in time.
        time.sleep(0.01)
        module_path.write_text("VALUE = 2\n", encoding="utf-8")
        future = time.time() + 5
        import os

        os.utime(module_path, (future, future))

        report = code_identity.worker_code_identity_report()
        names = {row["module"] for row in report["modified_modules"]}
        assert "watched_mod" in names
        assert report["modified_module_count"] >= 1
    finally:
        sys.modules.pop("watched_mod", None)
        sys.path.remove(str(package_root))


def test_lazily_imported_module_is_not_reported_stale(tmp_path: Path) -> None:
    """A module imported AFTER an edit loaded the new file — not stale.

    The rejected design compared file mtime to the worker's BOOT time,
    which inverts here: adapter modules are imported on the first source
    run, so an ordinary edit-then-import loop would be reported as stale
    code and send the operator to restart for nothing.
    """
    package_root = tmp_path / "pkg"
    package_root.mkdir()
    module_path = package_root / "late_mod.py"
    sys.path.insert(0, str(package_root))
    try:
        code_identity.record_worker_code_identity(
            deployment="d",
            extra_roots=[package_root],
        )
        # Written (and therefore mtime-stamped) after boot, then imported.
        time.sleep(0.01)
        module_path.write_text("VALUE = 1\n", encoding="utf-8")
        import late_mod  # noqa: F401 - imported for its side effect

        report = code_identity.worker_code_identity_report()
        names = {row["module"] for row in report["modified_modules"]}
        assert "late_mod" not in names
    finally:
        sys.modules.pop("late_mod", None)
        sys.path.remove(str(package_root))


def test_unrecorded_process_reports_not_recorded() -> None:
    """A process that never recorded must not report agreement.

    The scheduled doctor runs part of its work in a subprocess. A
    freshly-spawned process's code trivially equals the checkout, so if
    it could report `matched` it would do so throughout a staleness.
    """
    report = code_identity.worker_code_identity_report()
    assert report["status"] == "not_recorded"

    evaluated = code_identity.evaluate_code_divergence(
        observed=report,
        checkout_revision="abc123",
    )
    assert evaluated["status"] == "not_observed"
    assert evaluated["ok"] is None


def _observed(
    *,
    revision: str = "aaa",
    modified: int = 0,
    status: str = "recorded",
) -> dict:
    return {
        "status": status,
        "code_revision": revision,
        "started_at": "2026-07-27T00:00:00+00:00",
        "pid": 4242,
        "package_root": "/srv/okg/src/okg",
        "checkout_root": "/srv/okg",
        "modified_module_count": modified,
        "modified_modules": [
            {"module": "cooper.sources.aliases", "path": "/srv/x.py"}
        ] * modified,
    }


def test_divergence_state_table() -> None:
    """Eight inputs, four states, and only `matched` is reassuring."""
    fresh = dict(observed_age_minutes=1.0)
    cases = [
        (
            "equal revisions",
            dict(observed=_observed(revision="aaa"), checkout_revision="aaa",
                 **fresh),
            "matched",
        ),
        (
            "unequal revisions",
            dict(observed=_observed(revision="aaa"), checkout_revision="bbb",
                 **fresh),
            "diverged",
        ),
        (
            "loaded-then-modified module",
            dict(
                observed=_observed(revision="aaa", modified=1),
                checkout_revision="aaa",
                **fresh,
            ),
            "diverged",
        ),
        (
            "expired observation",
            dict(
                observed=_observed(revision="aaa"),
                checkout_revision="aaa",
                observed_age_minutes=180.0,
            ),
            "stale_observation",
        ),
        (
            "two worker roots",
            dict(
                observed=_observed(revision="aaa"),
                checkout_revision="aaa",
                worker_root_count=2,
                **fresh,
            ),
            "not_observed",
        ),
        (
            "absent identity",
            dict(observed=None, checkout_revision="aaa", **fresh),
            "not_observed",
        ),
        (
            "unknown checkout revision",
            dict(observed=_observed(revision="aaa"), checkout_revision=None,
                 **fresh),
            "not_observed",
        ),
        (
            # `matched` requires every input to have been MEASURED, not
            # merely to be uncontradicted.
            "unmeasured observation age",
            dict(observed=_observed(revision="aaa"), checkout_revision="aaa"),
            "not_observed",
        ),
    ]
    for label, kwargs, expected in cases:
        block = code_identity.evaluate_code_divergence(**kwargs)
        assert block["status"] == expected, label
        if expected == "matched":
            assert block["ok"] is True, label
        else:
            # Nothing but `matched` may report a passing check, and every
            # other state must say why.
            assert block["ok"] is not True, label
            assert block["reasons"], label


def test_diverged_reasons_name_the_evidence() -> None:
    block = code_identity.evaluate_code_divergence(
        observed=_observed(revision="aaa", modified=2),
        checkout_revision="bbb",
        observed_age_minutes=1.0,
    )
    assert block["status"] == "diverged"
    assert set(block["reasons"]) == {
        "loaded_modules_modified_since_import",
        "code_revision_mismatch",
    }
    assert block["worker_code_revision"] == "aaa"
    assert block["checkout_code_revision"] == "bbb"
    assert block["compared_root"] == "/srv/okg"


def _build_status(monkeypatch, block: dict) -> dict:
    monkeypatch.setattr(
        dogfood,
        "_code_identity_block",
        lambda live, *, deployment, process_audit=None: dict(block),
    )
    monkeypatch.setattr(
        dogfood,
        "_runtime_worker_process_audit",
        lambda deployment: {
            "ok": True,
            "status": "ok",
            "process_count": 1,
            "root_count": 1,
            "split_executor": False,
            "processes": [],
            "roots": [],
        },
    )
    return dogfood._status_payload(
        deployment="cooper",
        environment={"ok": True, "authority": {"target": {}}},
        live={
            "generation": {"generation_id": 7, "status": "published"},
            "mcp": {"ok": True},
            # The incident shape: the worker is alive and healthy — it is
            # only its LOADED CODE that is stale. A down worker is a
            # different, already-handled problem.
            "source_sync_status": {
                "ok": True,
                "runtime_liveness": {
                    "ok": True,
                    "status": "healthy",
                    "worker": {"ok": True, "status": "healthy"},
                },
            },
        },
        resolved_dsn=True,
        include_details=False,
    )


def test_status_payload_blocks_on_divergence(monkeypatch) -> None:
    """Divergence must reach blockers, trust_level, and next_action."""
    payload = _build_status(monkeypatch, {
        "status": "diverged",
        "ok": False,
        "worker_code_revision": "aaa",
        "checkout_code_revision": "bbb",
        "modified_module_count": 0,
        "modified_modules": [],
        "reasons": ["code_revision_mismatch"],
        "fix_hint": "launchctl kickstart -k gui/501/org.okg.runtime-worker.cooper",
    })

    blocker_ids = {row["id"] for row in payload["blockers"]}
    assert "runtime.code_revision" in blocker_ids
    # A worker running code that no longer exists is not a ready graph.
    assert payload["trust_level"] != "ready"
    assert payload["ok"] is False
    assert payload["runtime_worker"]["code_identity"]["status"] == "diverged"
    assert payload["next_action"]["code"] == "restart_stale_runtime_worker"
    # #945: the advisory `okg dogfood restart` form does not restart a
    # supervised worker, so the hint must be the supervisor's own command.
    assert "launchctl" in payload["next_action"]["command"]


@pytest.mark.parametrize(
    "status",
    ["matched", "stale_observation", "not_observed"],
)
def test_status_payload_only_blocks_on_diverged(monkeypatch, status) -> None:
    payload = _build_status(monkeypatch, {
        "status": status,
        "ok": True if status == "matched" else None,
        "reasons": [] if status == "matched" else ["because"],
    })
    blocker_ids = {row["id"] for row in payload["blockers"]}
    assert "runtime.code_revision" not in blocker_ids
    assert payload["runtime_worker"]["code_identity"]["status"] == status


def test_unavailable_payload_still_carries_code_identity() -> None:
    """Every payload shape carries the key.

    A consumer testing ``status != "diverged"`` must not read a missing
    key as "no divergence".
    """
    payload = dogfood._unavailable_payload(
        deployment="cooper",
        reason="environment_unavailable",
        message="no DSN",
    )
    identity = payload["runtime_worker"]["code_identity"]
    assert identity["status"] == "not_observed"
    assert identity["ok"] is None


def test_doctor_verdict_carries_code_identity(monkeypatch) -> None:
    """The payload the worker publishes is the one the reader parses.

    Guards the seam between `scheduled.doctor_workflow` writing
    ``runtime_code_identity`` onto the verdict and
    `read_worker_code_identity_overlay` reading that exact key back.
    """
    from okg.substrate.runtime import publisher_health_doctor_overlay as overlay

    code_identity.record_worker_code_identity(deployment="cooper")
    published = code_identity.worker_code_identity_report()
    verdict = {"deployment": "cooper", "runtime_code_identity": published}

    monkeypatch.setattr(
        overlay,
        "_latest_doctor_verdict_field",
        lambda *, deployment, dsn, field, limit=5: (
            (verdict[field], 1_800_000_000_000)
            if field in verdict and deployment == "cooper"
            else (None, None)
        ),
    )
    read_back = overlay.read_worker_code_identity_overlay(
        deployment="cooper",
        dsn="postgresql:///unused",
    )
    assert read_back is not None
    assert read_back["status"] == "recorded"
    assert read_back["code_revision"] == published["code_revision"]
    assert read_back["source"] == "scheduled_doctor_event"
    assert read_back["observed_minutes_ago"] is not None


def test_missing_verdict_reads_as_not_observed(monkeypatch) -> None:
    from okg.substrate.runtime import publisher_health_doctor_overlay as overlay

    monkeypatch.setattr(
        overlay,
        "_latest_doctor_verdict_field",
        lambda *, deployment, dsn, field, limit=5: (None, None),
    )
    assert overlay.read_worker_code_identity_overlay(
        deployment="cooper",
        dsn="postgresql:///unused",
    ) is None


def test_module_is_stale_reports_unknown_for_unloaded_module() -> None:
    """Unknown is a real answer, not a synonym for `current`."""
    assert code_identity.module_is_stale("no.such.module") == {
        "status": "unknown",
        "reason": "module_not_loaded",
    }


# --------------------------------------------------------------------
# The real `_code_identity_block` and its helpers — NOT stubbed. The
# tests above drive the evaluator and the payload wiring; these drive
# the seam between them, where the caller decides what to feed the
# evaluator.
# --------------------------------------------------------------------


def _live(observed: dict | None) -> dict:
    return {} if observed is None else {"worker_code_identity": observed}


def _observed_overlay(
    *,
    revision: str = "aaa",
    modified: int = 0,
    **overrides,
) -> dict:
    payload = _observed(revision=revision, modified=modified)
    payload.update({
        "observed_minutes_ago": 1.0,
        "last_observed_at": "2026-07-28T00:00:00+00:00",
        "source": "scheduled_doctor_event",
    })
    payload.update(overrides)
    return payload


def test_code_identity_block_compares_the_workers_own_root(
    monkeypatch, tmp_path,
) -> None:
    """The revision must be read from the WORKER's root, not the caller's.

    `okg dogfood status` is routinely run from a different worktree than
    the worker's sync root; comparing across trees is a permanent false
    mismatch.
    """
    seen: list = []

    def _revision(root=None):
        seen.append(root)
        return "aaa"

    monkeypatch.setattr(code_identity, "checkout_code_revision", _revision)
    block = dogfood._code_identity_block(
        _live(_observed_overlay(revision="aaa")),
        deployment="cooper",
        process_audit={"status": "ok", "root_count": 1},
    )
    assert seen == ["/srv/okg"]
    assert block["status"] == "matched"
    assert block["compared_root"] == "/srv/okg"


def test_code_identity_block_states(monkeypatch) -> None:
    monkeypatch.setattr(
        code_identity, "checkout_code_revision", lambda root=None: "aaa",
    )
    audit = {"status": "ok", "root_count": 1}

    diverged = dogfood._code_identity_block(
        _live(_observed_overlay(revision="bbb")),
        deployment="cooper",
        process_audit=audit,
    )
    assert diverged["status"] == "diverged"
    # A real supervisor command, not the advisory `okg dogfood restart`.
    assert diverged["fix_hint"]
    assert "dogfood restart --deployment cooper --json" not in (
        diverged["fix_hint"]
    )

    stale = dogfood._code_identity_block(
        _live(_observed_overlay(revision="aaa", observed_minutes_ago=600.0)),
        deployment="cooper",
        process_audit=audit,
    )
    assert stale["status"] == "stale_observation"

    absent = dogfood._code_identity_block(
        _live(None), deployment="cooper", process_audit=audit,
    )
    assert absent["status"] == "not_observed"
    assert absent["ok"] is None

    two_workers = dogfood._code_identity_block(
        _live(_observed_overlay(revision="aaa")),
        deployment="cooper",
        process_audit={"status": "ok", "root_count": 2},
    )
    assert two_workers["status"] == "not_observed"
    assert two_workers["reasons"] == ["multiple_worker_process_roots"]


def test_code_identity_block_needs_a_measured_age(monkeypatch) -> None:
    """An unmeasurable observation age is not a passing check.

    Without an age there is nothing to tell a live worker's verdict from
    the last one a dead worker ever wrote.
    """
    monkeypatch.setattr(
        code_identity, "checkout_code_revision", lambda root=None: "aaa",
    )
    block = dogfood._code_identity_block(
        _live(_observed_overlay(revision="aaa", observed_minutes_ago=None)),
        deployment="cooper",
        process_audit={"status": "ok", "root_count": 1},
    )
    assert block["status"] == "not_observed"
    assert "observation_age_unknown" in block["reasons"]


def test_code_identity_block_survives_an_unreadable_overlay(
    monkeypatch,
) -> None:
    """A probe failure reports not_observed, never a clean bill."""
    monkeypatch.setattr(
        code_identity, "checkout_code_revision", lambda root=None: "aaa",
    )
    block = dogfood._code_identity_block(
        _live({"status": "probe_failed", "reason": "connection refused"}),
        deployment="cooper",
        process_audit={"status": "ok", "root_count": 1},
    )
    assert block["status"] == "not_observed"
    assert block["ok"] is None
    assert block["reasons"] == ["connection refused"]


def test_attach_worker_code_identity_never_raises(monkeypatch) -> None:
    """A DB read that blows up must degrade, not break `dogfood status`."""
    def _boom(**kwargs):
        raise RuntimeError("no route to host")

    monkeypatch.setattr(
        "okg.substrate.runtime.publisher_health."
        "read_worker_code_identity_overlay",
        _boom,
    )
    enriched = dogfood._attach_worker_code_identity(
        {}, deployment="cooper", dsn="postgresql:///x",
    )
    assert enriched["worker_code_identity"]["status"] == "probe_failed"


def test_observed_worker_root_count_distinguishes_zero_from_unknown() -> None:
    assert dogfood._observed_worker_root_count(
        {"status": "unavailable", "root_count": 0},
    ) is None
    assert dogfood._observed_worker_root_count(None) is None
    assert dogfood._observed_worker_root_count(
        {"status": "ok", "root_count": 0},
    ) == 0
    assert dogfood._observed_worker_root_count(
        {"status": "ok", "root_count": 3},
    ) == 3


def test_restart_hint_is_a_supervisor_command() -> None:
    """#945: the advisory `okg dogfood restart` form is a no-op."""
    hint = dogfood._restart_worker_hint("cooper")
    assert hint
    assert "--json" not in hint or "--apply" in hint


# ── installed_code_revision: checkout, wheel build record, or unknown ─────


class _FakeDist:
    """importlib.metadata.Distribution stand-in for an installed wheel."""

    def __init__(
        self, site: Path, *, listed: bool = True, located: Path | None = None,
    ) -> None:
        self._site = located or site
        self.files = (
            ["okg/substrate/runtime/code_identity.py"] if listed else ["okg.pth"]
        )

    def locate_file(self, entry):  # noqa: ANN001, ANN201
        return self._site / str(entry)


def _wheel_site(tmp_path: Path, build_info: dict | None) -> Path:
    import json

    site = tmp_path / "site-packages"
    module = site / "okg" / "substrate" / "runtime" / "code_identity.py"
    module.parent.mkdir(parents=True)
    module.write_text("", encoding="utf-8")
    if build_info is not None:
        (site / "okg" / "_build_info.json").write_text(
            json.dumps(build_info), encoding="utf-8",
        )
    return site


def test_installed_code_revision_reads_a_checkout_head() -> None:
    from okg.substrate.runtime.code_identity import (
        checkout_code_revision,
        installed_code_revision,
    )

    package_root = Path(__file__).resolve().parents[3] / "src" / "okg"
    found = installed_code_revision(package_root)
    assert found.source == "checkout"
    assert found.revision == checkout_code_revision(package_root)


def test_installed_code_revision_reads_a_wheels_build_record(tmp_path: Path) -> None:
    """#3585: a wheel has no .git; its build already recorded the commit."""
    from okg.substrate.runtime.code_identity import installed_code_revision

    revision = "a" * 40
    site = _wheel_site(
        tmp_path, {"source_revision": revision, "source_dirty": False},
    )
    found = installed_code_revision(site / "okg", distribution=_FakeDist(site))
    assert (found.revision, found.source, found.dirty) == (
        revision, "build_info", False,
    )


@pytest.mark.parametrize("build_info, listed", [
    ({"source_revision": None}, True),          # built without a revision
    ({"source_revision": "unknown"}, True),     # never the literal "unknown"
    ({"source_revision": "abc123"}, True),      # not a full Git id
    (None, True),                               # no build record at all
    ({"source_revision": "a" * 40}, False),     # the wheel is not this package
])
def test_installed_code_revision_is_unknown_without_a_trustworthy_record(
    tmp_path: Path, build_info, listed,
) -> None:
    from okg.substrate.runtime.code_identity import installed_code_revision

    site = _wheel_site(tmp_path, build_info)
    found = installed_code_revision(
        site / "okg", distribution=_FakeDist(site, listed=listed),
    )
    assert found.revision is None and found.source is None
    assert found.reason


def test_a_wheel_inside_another_repository_never_reports_its_head(
    tmp_path: Path,
) -> None:
    """A venv inside some other Git repository: that HEAD is not okg's."""
    import subprocess

    from okg.substrate.runtime.code_identity import installed_code_revision

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "-c", "user.email=t@x", "-c", "user.name=t",
         "commit", "-q", "--allow-empty", "-m", "outer"],
        check=True,
    )
    revision = "b" * 40
    site = _wheel_site(tmp_path, {"source_revision": revision})
    found = installed_code_revision(site / "okg", distribution=_FakeDist(site))
    assert (found.revision, found.source) == (revision, "build_info")


def test_a_dirty_wheel_build_reports_no_revision(tmp_path: Path) -> None:
    """Review finding: a dirty build named its commit, matched a clean
    install of it, and read current."""
    from okg.substrate.runtime.code_identity import installed_code_revision

    site = _wheel_site(
        tmp_path, {"source_revision": "a" * 40, "source_dirty": True},
    )
    found = installed_code_revision(site / "okg", distribution=_FakeDist(site))
    assert (found.revision, found.source, found.dirty) == (None, None, True)
    assert "uncommitted" in found.reason


def test_a_wheel_whose_file_lives_elsewhere_is_not_this_package(
    tmp_path: Path,
) -> None:
    """The anchor file is listed, but in another install location."""
    from okg.substrate.runtime.code_identity import installed_code_revision

    site = _wheel_site(tmp_path / "a", {"source_revision": "a" * 40})
    other = _wheel_site(tmp_path / "b", {"source_revision": "a" * 40})
    found = installed_code_revision(
        site / "okg", distribution=_FakeDist(site, located=other),
    )
    assert found.revision is None and found.reason


def test_a_checkout_whose_head_cannot_be_read_reports_no_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.code_identity import installed_code_revision

    monkeypatch.setattr(code_identity, "_git_head", lambda _root: None)
    package_root = Path(__file__).resolve().parents[3] / "src" / "okg"
    found = installed_code_revision(package_root)
    assert found.revision is None and "rev-parse" in found.reason
