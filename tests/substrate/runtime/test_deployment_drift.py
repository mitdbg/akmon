"""Witnesses for the deployed-code drift probe.

The probe exists because okg-workspace ran 313 commits behind for six days with
no signal: a 07-12 cutover incident left the sync root detached on unmerged
hotfixes. ``test_reproduces_the_20260712_drift_shape`` rebuilds exactly that
shape and asserts the probe fires — it is the regression witness for the
condition that went unnoticed.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from okg.substrate.runtime import deployment_drift as dd
from okg.substrate.runtime.deployment_drift import check_deployment_code_drift


#: A fixed identity for the scratch repositories' commits. A CI runner has no
#: git user configured, and a clone (the consumer's sync root) inherits none
#: from its source repository, so `git commit` there would exit 128.
_GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "T",
    "GIT_AUTHOR_EMAIL": "t@example.invalid",
    "GIT_COMMITTER_NAME": "T",
    "GIT_COMMITTER_EMAIL": "t@example.invalid",
}


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True, capture_output=True, text=True,
        env={**os.environ, **_GIT_IDENTITY},
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repo with a `dev` branch and an `origin/dev` remote-tracking ref."""
    root = tmp_path / "sync-root"
    root.mkdir()
    subprocess.run(["git", "-c", "init.defaultBranch=dev", "init", str(root)],
                   check=True, capture_output=True)
    _git(root, "config", "user.email", "t@example.invalid")
    _git(root, "config", "user.name", "T")
    _git(root, "config", "commit.gpgsign", "false")
    (root / "f.txt").write_text("base\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "base")
    # Simulate a remote-tracking ref without needing a real remote.
    _git(root, "update-ref", "refs/remotes/origin/dev", "HEAD")
    return root


def _point_manifest_at(monkeypatch: pytest.MonkeyPatch, root: Path | None,
                       *, readable: bool = True, extra: dict | None = None) -> None:
    """Stub the manifest read so tests never need a real deployment on disk."""
    raw: dict | None = None
    if readable:
        raw = dict(extra or {})
        if root is not None:
            raw["sync_root"] = str(root)
    # The probe reads through `_read_manifest_with_dir` so it can resolve a
    # per-machine `${VAR}` sync_root the same way the publish guard does
    # (issue #1014); the third element is the manifest's directory, which
    # anchors path anchors and the local-profile lookup.
    manifest_dir = (root.parent / "deployments" / "d") if root else None
    monkeypatch.setattr(
        "okg.substrate.deployment_authority._read_manifest_with_dir",
        lambda _deployment: (raw, readable, manifest_dir),
    )


def test_clean_sync_root_on_base_is_ok(repo: Path, monkeypatch) -> None:
    _point_manifest_at(monkeypatch, repo)
    v = check_deployment_code_drift(deployment="d")
    assert v.ok and v.status == "ok", v.problems
    assert v.is_ancestor_of_base is True
    assert v.detached is False
    assert v.branch == "dev"
    assert v.commits_ahead == 0
    assert v.problems == []


def test_reproduces_the_20260712_drift_shape(repo: Path, monkeypatch) -> None:
    """Detached HEAD carrying unmerged hotfixes, base ref moved on without it.

    This is the okg-workspace condition that ran unnoticed for six days.
    """
    # Hotfixes committed straight onto the deployment, then left detached.
    (repo / "hotfix.txt").write_text("staging authority fix\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "fix staging generation authority guard")
    hotfix = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", hotfix)
    # Meanwhile dev advanced independently.
    _git(repo, "update-ref", "refs/heads/dev", "HEAD~1")
    _git(repo, "checkout", "dev")
    (repo / "other.txt").write_text("unrelated dev work\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "dev moves on")
    _git(repo, "update-ref", "refs/remotes/origin/dev", "HEAD")
    _git(repo, "checkout", "--detach", hotfix)

    _point_manifest_at(monkeypatch, repo)
    v = check_deployment_code_drift(deployment="okg-workspace")

    assert not v.ok and v.status == "warning"
    assert v.is_ancestor_of_base is False
    assert v.detached is True
    assert v.commits_ahead == 1 and v.commits_behind == 1
    joined = " ".join(v.problems)
    assert "NOT an ancestor" in joined, "stranded-commit defect must be named"
    assert "DETACHED" in joined, "detached-HEAD defect must be named"


def test_behind_only_is_reported_but_not_classified(repo: Path, monkeypatch) -> None:
    """Merely being behind is normal between deploys: report, do not classify."""
    _git(repo, "checkout", "-b", "tmp")
    (repo / "n.txt").write_text("newer\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "newer dev work")
    _git(repo, "update-ref", "refs/remotes/origin/dev", "HEAD")
    _git(repo, "checkout", "dev")  # deployed rev is an ancestor, just behind

    _point_manifest_at(monkeypatch, repo)
    v = check_deployment_code_drift(deployment="d")
    assert v.ok, v.problems
    assert v.commits_behind == 1
    assert v.is_ancestor_of_base is True


def test_behind_is_classified_once_a_threshold_is_declared(repo: Path, monkeypatch) -> None:
    _git(repo, "checkout", "-b", "tmp")
    for i in range(3):
        (repo / f"n{i}.txt").write_text("x\n", encoding="utf-8")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-m", f"dev {i}")
    _git(repo, "update-ref", "refs/remotes/origin/dev", "HEAD")
    _git(repo, "checkout", "dev")

    _point_manifest_at(monkeypatch, repo,
                       extra={"code_drift": {"max_commits_behind": 1}})
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok
    assert v.behind_threshold == 1 and v.commits_behind == 3
    assert "behind" in " ".join(v.problems)


def test_undeclared_sync_root_is_loud_misconfiguration(monkeypatch) -> None:
    _point_manifest_at(monkeypatch, None)
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok and v.status == "misconfigured"
    assert "declares no sync_root" in " ".join(v.problems)


def test_unreadable_manifest_is_loud_error(monkeypatch) -> None:
    _point_manifest_at(monkeypatch, None, readable=False)
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok and v.status == "error"
    assert "unreadable" in " ".join(v.problems)


def test_missing_and_non_git_sync_root_are_loud(tmp_path: Path, monkeypatch) -> None:
    missing = tmp_path / "gone"
    _point_manifest_at(monkeypatch, missing)
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok and v.status == "error"
    assert "does not exist" in " ".join(v.problems)

    plain = tmp_path / "plain"
    plain.mkdir()
    _point_manifest_at(monkeypatch, plain)
    v2 = check_deployment_code_drift(deployment="d")
    assert not v2.ok and v2.status == "error"
    assert "not a git checkout" in " ".join(v2.problems)


def test_absent_base_ref_never_silently_passes(repo: Path, monkeypatch) -> None:
    _point_manifest_at(monkeypatch, repo)
    v = check_deployment_code_drift(deployment="d", base_ref="origin/nope")
    assert not v.ok and v.status == "error"
    assert "not present" in " ".join(v.problems)


@pytest.mark.parametrize(
    "bad",
    [True, "2", 2.0, -1, None, [2]],
    ids=["bool", "yaml-string", "float", "negative", "null", "list"],
)
def test_invalid_threshold_is_loud_not_silently_disarmed(
    repo: Path, monkeypatch, bad
) -> None:
    """An operator who declares a threshold armed a gate.

    Adversarial review F2: dropping an unusable value on the floor left the
    gate disarmed and the verdict reading "ok" — `max_commits_behind: "2"` is
    an ordinary YAML quoting typo. `bool` is called out separately because it
    is an `int` subclass, so `true` would otherwise silently mean 1.
    """
    _point_manifest_at(monkeypatch, repo,
                       extra={"code_drift": {"max_commits_behind": bad}})
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok, f"{bad!r} silently disarmed the gate"
    assert v.status == "misconfigured"
    assert v.behind_threshold is None
    assert "max_commits_behind" in " ".join(v.problems)


def test_absent_threshold_stays_legitimately_unclassified(repo: Path, monkeypatch) -> None:
    """Absent != invalid: no declaration means staleness simply isn't policy."""
    _point_manifest_at(monkeypatch, repo, extra={"code_drift": {}})
    v = check_deployment_code_drift(deployment="d")
    assert v.ok and v.behind_threshold is None and v.problems == []


def test_unmeasurable_counts_never_leave_a_declared_gate_unevaluated(
    repo: Path, monkeypatch
) -> None:
    """Adversarial review F1: if rev-list output can't be parsed, `behind`
    stays None — the threshold clause then short-circuited and the verdict
    read "ok" while an armed gate went unevaluated."""
    real_git = dd._git

    def fake_git(root, *args):
        if args[:1] == ("rev-list",):
            return 0, "not-a-count"
        return real_git(root, *args)

    monkeypatch.setattr(dd, "_git", fake_git)
    _point_manifest_at(monkeypatch, repo,
                       extra={"code_drift": {"max_commits_behind": 1}})
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok and v.status == "error"
    assert v.commits_behind is None
    joined = " ".join(v.problems)
    assert "could not measure" in joined
    assert "could NOT be evaluated" in joined, "must name the unevaluated gate"


def test_git_unavailable_yields_an_envelope_not_a_traceback(
    repo: Path, monkeypatch
) -> None:
    """Adversarial review F4: subprocess errors escaped as a raw traceback,
    breaking the --json contract for a doctor check."""
    # Patch subprocess.run (NOT _git) so the real _git runs and we exercise the
    # actual TimeoutExpired -> _GitUnavailable conversion rather than faking it.
    def boom(*_a, **_kw):
        raise subprocess.TimeoutExpired(cmd="git", timeout=dd._GIT_TIMEOUT_S)

    monkeypatch.setattr(dd.subprocess, "run", boom)
    _point_manifest_at(monkeypatch, repo)
    v = check_deployment_code_drift(deployment="d")  # must not raise
    assert not v.ok and v.status == "error"
    assert "timed out" in " ".join(v.problems).lower()


def test_git_missing_binary_also_yields_an_envelope(repo: Path, monkeypatch) -> None:
    def no_git(*_a, **_kw):
        raise FileNotFoundError("git: command not found")

    monkeypatch.setattr(dd.subprocess, "run", no_git)
    _point_manifest_at(monkeypatch, repo)
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok and v.status == "error"
    assert "failed to run" in " ".join(v.problems)


def test_sync_root_inside_another_checkout_is_rejected(
    repo: Path, monkeypatch
) -> None:
    """Adversarial review F6: `rev-parse --git-dir` walks UP, so a stale or
    typo'd sync_root landing anywhere inside a clean checkout read healthy."""
    inner = repo / "src" / "deep"
    inner.mkdir(parents=True)
    _point_manifest_at(monkeypatch, inner)
    v = check_deployment_code_drift(deployment="d")
    assert not v.ok and v.status == "error"
    assert "not a checkout root" in " ".join(v.problems)


def test_missing_deployment_is_not_misreported_as_undeclared(monkeypatch) -> None:
    """Adversarial review F7: a typo'd --deployment told the operator to edit a
    deployment.yaml that does not exist."""
    monkeypatch.setattr(
        "okg.substrate.deployment_authority._read_manifest_with_dir",
        lambda _d: (None, True, None),
    )
    v = check_deployment_code_drift(deployment="definitely-not-real")
    assert not v.ok and v.status == "error"
    joined = " ".join(v.problems)
    assert "check the deployment name" in joined
    assert "declares no sync_root" not in joined


def test_verdict_serializes_for_the_doctor_envelope(repo: Path, monkeypatch) -> None:
    _point_manifest_at(monkeypatch, repo)
    v = check_deployment_code_drift(deployment="d")
    d = v.to_dict()
    for key in (
        "ok", "status", "deployment", "base_ref", "sync_root", "head",
        "branch", "detached", "is_ancestor_of_base", "commits_ahead",
        "commits_behind", "behind_threshold", "problems",
    ):
        assert key in d, f"doctor payload missing {key}"
    # values, not just key presence (review F10)
    assert d["ok"] is True and d["status"] == "ok"
    assert d["deployment"] == "d" and d["base_ref"] == "origin/dev"
    assert d["head"] == v.head and d["detached"] is False
    assert d["problems"] == []


# --------------------------------------------------------------------------
# The doctor handler itself (review F11: it had no witness at all).
# --------------------------------------------------------------------------

def test_doctor_handler_exit_codes_and_payload(repo: Path, monkeypatch, capsys) -> None:
    import argparse

    from okg.substrate.cli.doctor import _doctor_code_drift

    _point_manifest_at(monkeypatch, repo)
    args = argparse.Namespace(deployment="d")
    rc = _doctor_code_drift(args, as_json=True)
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert rc == 0
    assert payload["check"] == "code-drift"
    assert payload["ok"] is True

    # a drifted deployment must exit NON-zero so a scheduled runner reds
    _git(repo, "checkout", "-b", "hot")
    (repo / "h.txt").write_text("x\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "unmerged hotfix")
    _git(repo, "checkout", "--detach", "HEAD")
    rc2 = _doctor_code_drift(argparse.Namespace(deployment="d"), as_json=True)
    payload2 = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert rc2 == 1
    assert payload2["ok"] is False and payload2["is_ancestor_of_base"] is False


def test_doctor_handler_requires_a_deployment(monkeypatch, capsys) -> None:
    import argparse

    from okg.substrate.cli.doctor import _doctor_code_drift

    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    rc = _doctor_code_drift(argparse.Namespace(deployment=None), as_json=True)
    assert rc != 0
    assert "code-drift" in capsys.readouterr().err


# --------------------------------------------------------------------------
# code_drift.mode: pinned (issue #2987). A consumer that embeds okg as a
# pinned git submodule has no base ref to follow; the pin is healthy when the
# sync_root checkout is clean and at the commit the INSTALLED okg package was
# built from. Every repository below is a real temporary git repository.


def _init_repo(root: Path, files: dict[str, str]) -> str:
    root.mkdir(parents=True)
    subprocess.run(["git", "-c", "init.defaultBranch=main", "init", str(root)],
                   check=True, capture_output=True)
    _git(root, "config", "user.email", "t@example.invalid")
    _git(root, "config", "user.name", "T")
    _git(root, "config", "commit.gpgsign", "false")
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "okg release")
    return _git(root, "rev-parse", "HEAD")


@pytest.fixture
def pinned(tmp_path: Path, monkeypatch) -> dict:
    """An okg source checkout (the installed package) and a pinned sync_root.

    The sync_root is a separate clone detached at the same commit, like a
    consumer's submodule: no ``origin/dev`` ref, detached HEAD.
    """
    okg_src = tmp_path / "okg-src"
    revision = _init_repo(okg_src, {
        "src/okg/__init__.py": "",
        "README.md": "okg\n",
    })
    sync_root = tmp_path / "consumer" / "okg"
    subprocess.run(["git", "clone", "--quiet", str(okg_src), str(sync_root)],
                   check=True, capture_output=True)
    _git(sync_root, "checkout", "--quiet", "--detach", revision)
    monkeypatch.setattr(
        dd, "_installed_package_root", lambda: okg_src / "src" / "okg",
    )
    return {"okg_src": okg_src, "sync_root": sync_root, "revision": revision}


def _pinned_manifest(monkeypatch, root: Path, **block) -> None:
    _point_manifest_at(
        monkeypatch, root, extra={"code_drift": {"mode": "pinned", **block}},
    )


def test_pinned_matching_clean_checkout_is_healthy(pinned, monkeypatch) -> None:
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.ok and v.status == "ok", v.problems
    assert v.problems == []
    assert v.mode == "pinned"
    assert v.head == pinned["revision"] == v.installed_revision
    assert v.detached is True, "a detached pin is expected, not a defect"
    assert v.commits_behind is None and v.commits_ahead is None
    assert v.base_ref is None
    d = v.to_dict()
    assert d["mode"] == "pinned" and d["dirty_path_count"] == 0
    assert d["installed_revision"] == pinned["revision"]


@pytest.mark.parametrize("moved", [False, True], ids=["at-install", "head-moved"])
def test_pinned_non_submodule_editable_install_cannot_verify_the_pin(
    pinned, monkeypatch, moved
) -> None:
    """An editable install from the sync_root itself, with no superproject.

    The build revision is re-read from the same checkout, so it moves with
    any `git checkout`; lane A saw a moved HEAD read healthy on kb76. With
    nothing independent recording the pin, the verdict must say it cannot
    verify -- before and after a move -- never ok.
    """
    root = pinned["okg_src"]
    if moved:
        (root / "local.txt").write_text("x\n", encoding="utf-8")
        _git(root, "add", "-A")
        _git(root, "commit", "-m", "operator checks out another okg commit")
    _pinned_manifest(monkeypatch, root)
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert v.installed_revision == v.head, "the tautology this guards"
    assert v.pin_source == "none"
    assert "cannot verify the pin" in " ".join(v.problems)


def test_pinned_separate_checkout_reports_its_pin_source(
    pinned, monkeypatch
) -> None:
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.ok and v.pin_source == "installed_package_checkout", v.problems


def test_track_base_cannot_read_healthy_on_the_same_pin(pinned, monkeypatch) -> None:
    """The #2987 shape: without the mode, a pinned submodule is an error."""
    _point_manifest_at(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.status == "error"
    assert "'origin/dev' is not present" in " ".join(v.problems)


def test_pinned_different_head_is_an_error_naming_both_commits(
    pinned, monkeypatch
) -> None:
    root = pinned["sync_root"]
    (root / "upgrade.txt").write_text("newer okg\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "submodule bumped, package not reinstalled")
    moved = _git(root, "rev-parse", "HEAD")
    _pinned_manifest(monkeypatch, root)
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    joined = " ".join(v.problems)
    assert moved in joined and pinned["revision"] in joined
    assert "installed okg package" in joined


@pytest.mark.parametrize("extra_files", [1, 13], ids=["one", "over-cap"])
def test_pinned_dirty_tree_is_a_warning_naming_the_paths(
    pinned, monkeypatch, extra_files
) -> None:
    root = pinned["sync_root"]
    (root / "README.md").write_text("edited in place\n", encoding="utf-8")
    for i in range(extra_files):
        (root / f"stray{i:02d}.py").write_text("x = 1\n", encoding="utf-8")
    _pinned_manifest(monkeypatch, root)
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "warning"
    total = extra_files + 1
    assert v.dirty_path_count == total
    joined = " ".join(v.problems)
    assert "README.md" in joined and "stray00.py" in joined
    assert f"{total} uncommitted change(s)" in joined
    shown = min(total, dd.MAX_REPORTED_DIRTY_PATHS)
    assert len(v.dirty_paths) == shown
    if total > dd.MAX_REPORTED_DIRTY_PATHS:
        assert f"(and {total - shown} more)" in joined
        assert "stray12.py" not in joined


def test_pinned_mismatch_and_dirty_is_an_error_with_both_problems(
    pinned, monkeypatch
) -> None:
    root = pinned["sync_root"]
    (root / "n.txt").write_text("n\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-m", "moved")
    (root / "README.md").write_text("dirty\n", encoding="utf-8")
    _pinned_manifest(monkeypatch, root)
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.status == "error" and len(v.problems) == 2


def test_pinned_without_a_build_revision_is_an_error_never_healthy(
    pinned, tmp_path, monkeypatch
) -> None:
    """A package outside any git checkout (wheel, git archive) cannot say
    what it was built from; the pin must not pass unverified."""
    archive = tmp_path / "site-packages" / "okg"
    archive.mkdir(parents=True)
    monkeypatch.setattr(dd, "_installed_package_root", lambda: archive)
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert v.installed_revision is None
    assert "cannot report the commit it was built from" in " ".join(v.problems)


def test_pinned_wheel_inside_another_repository_is_not_given_its_head(
    pinned, monkeypatch
) -> None:
    """A wheel in a virtualenv under the sync_root's own checkout: walking up
    finds that checkout, whose HEAD would trivially 'match'. Refuse it."""
    root = pinned["sync_root"]
    wheel = root / ".venv" / "lib" / "site-packages" / "okg"
    wheel.mkdir(parents=True)
    (root / ".git" / "info" / "exclude").write_text(".venv/\n", encoding="utf-8")
    monkeypatch.setattr(dd, "_installed_package_root", lambda: wheel)
    _pinned_manifest(monkeypatch, root)
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert "not the src/okg tree" in " ".join(v.problems)


def test_pinned_git_failure_is_an_envelope(pinned, monkeypatch) -> None:
    real_git = dd._git

    def fake_git(root, *args):
        if args[:1] == ("status",):
            raise dd._GitUnavailable("git status timed out")
        return real_git(root, *args)

    monkeypatch.setattr(dd, "_git", fake_git)
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.status == "error" and v.mode == "pinned"
    assert "timed out" in " ".join(v.problems)


@pytest.mark.parametrize(
    ("block", "needle"),
    [
        ({"mode": "pinnned"}, "code_drift.mode must be one of"),
        ({"mode": 1}, "code_drift.mode must be one of"),
        ({"mode": None}, "code_drift.mode must be one of"),
        ({"mode": "pinned", "base_ref": "origin/main"}, "code_drift.base_ref"),
        ({"mode": "pinned", "max_commits_behind": 5},
         "code_drift.max_commits_behind"),
        ({"base_ref": ""}, "code_drift.base_ref must be"),
        ({"base_ref": "origin dev"}, "code_drift.base_ref must be"),
        ({"base_ref": 3}, "code_drift.base_ref must be"),
        ({"mdoe": "pinned"}, "code_drift has unknown key(s): 'mdoe'"),
        ({"mode": "pinned", "extra": 1}, "code_drift has unknown key(s): 'extra'"),
    ],
    ids=["typo-mode", "int-mode", "null-mode", "pinned+base_ref",
         "pinned+threshold", "empty-ref", "space-ref", "int-ref",
         "typo-key", "unknown-key-with-pinned"],
)
def test_bad_code_drift_config_is_refused(
    pinned, monkeypatch, block, needle
) -> None:
    _point_manifest_at(monkeypatch, pinned["sync_root"],
                       extra={"code_drift": block})
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "misconfigured"
    assert needle in " ".join(v.problems)


def test_track_base_accepts_an_explicit_mode_and_base_ref(
    repo: Path, monkeypatch
) -> None:
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    _point_manifest_at(monkeypatch, repo, extra={
        "code_drift": {"mode": "track_base", "base_ref": "origin/main"},
    })
    v = check_deployment_code_drift(deployment="d")
    assert v.ok and v.base_ref == "origin/main", v.problems
    assert "mode" not in v.to_dict(), "track_base keeps the old payload shape"


def test_pinned_healthy_verdict_does_not_degrade_dogfood_trust(
    pinned, monkeypatch
) -> None:
    from okg.substrate.cli.dogfood import _trust_level

    live = {
        "generation": {"status": "published"},
        "mcp": {"ok": True},
    }
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    healthy = check_deployment_code_drift(deployment="example-kb").to_dict()
    assert _trust_level(
        environment={"ok": True}, resolved_dsn=True,
        live={**live, "code_drift": healthy}, blockers=[],
    ) == "ready"

    # The same checkout without the mode is what #2987 reported.
    _point_manifest_at(monkeypatch, pinned["sync_root"])
    unpinned = check_deployment_code_drift(deployment="example-kb").to_dict()
    assert _trust_level(
        environment={"ok": True}, resolved_dsn=True,
        live={**live, "code_drift": unpinned}, blockers=[],
    ) == "unavailable"


@pytest.mark.parametrize("dirty", [False, True], ids=["clean", "dirty"])
def test_pinned_verdict_through_dogfood_readiness(
    pinned, tmp_path, monkeypatch, dirty
) -> None:
    """The readiness run dogfood status reads: a healthy pin is a pass, not
    a blocker or warning; a dirty pin is a dogfood blocker."""
    import psycopg

    from okg.substrate.cli import deployment as deployment_cli

    if dirty:
        (pinned["sync_root"] / "README.md").write_text("x\n", encoding="utf-8")
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    monkeypatch.setattr(
        psycopg, "connect",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            psycopg.OperationalError("database unavailable")
        ),
    )
    payload = deployment_cli._deployment_ready_live_checks(
        deployment="example-kb",
        deployment_dir=tmp_path,
        dsn="postgres://redacted",
        max_active_queue_depth=0,
        max_source_age_seconds=3600,
        readiness_profile="dogfood",
        skip_doctor=True,
    )
    blocker_ids = {row["id"] for row in payload["blockers"]}
    warning_ids = {row["id"] for row in payload["warnings"]}
    check = next(
        row for row in payload["checks"] if row["id"] == "deployment.code_drift"
    )
    if dirty:
        assert "deployment.code_drift" in blocker_ids
        assert "README.md" in check["message"]
    else:
        assert "deployment.code_drift" not in blocker_ids | warning_ids
        assert check["status"] == "pass"
        assert pinned["revision"] in check["message"]


def test_the_manifest_loader_accepts_a_code_drift_block(tmp_path: Path) -> None:
    """A deployment.yaml carrying code_drift must load: the real run on
    a shared build host found `okg search` refusing it as an unknown top-level key."""
    from okg.substrate import deployments
    from okg.substrate.deployments import DeploymentConfigError

    manifest = tmp_path / "deployment.yaml"
    deployments._reject_unknown_top_level_keys(
        {"name": "d", "code_drift": {"mode": "pinned"}}, manifest,
    )
    deployments._reject_wrong_top_level_types(
        {"name": "d", "code_drift": {"mode": "pinned"}}, manifest,
    )
    with pytest.raises(DeploymentConfigError, match="code_drift"):
        deployments._reject_wrong_top_level_types(
            {"name": "d", "code_drift": "pinned"}, manifest,
        )


# --------------------------------------------------------------------------
# Pinned mode on a real submodule (the example-kb shape): a superproject commits
# the okg gitlink and okg is installed editable from the submodule checkout.
# The build revision then moves with the submodule's HEAD, so the committed
# gitlink is what catches a moved HEAD.


def _commit_all(root: Path, message: str) -> str:
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", message)
    return _git(root, "rev-parse", "HEAD")


@pytest.fixture
def examplekb(tmp_path: Path, monkeypatch) -> dict:
    okg_src = tmp_path / "okg-upstream"
    first = _init_repo(okg_src, {"src/okg/__init__.py": "", "README.md": "okg\n"})
    (okg_src / "later.txt").write_text("later\n", encoding="utf-8")
    later = _commit_all(okg_src, "upstream moves on")

    superproject = tmp_path / "example-kb"
    _init_repo(superproject, {"README.md": "example-kb\n"})
    subprocess.run(
        ["git", "-C", str(superproject), "-c", "protocol.file.allow=always",
         "submodule", "add", "--quiet", str(okg_src), "okg"],
        check=True, capture_output=True,
    )
    sub = superproject / "okg"
    _git(sub, "config", "user.email", "t@example.invalid")
    _git(sub, "config", "user.name", "T")
    _git(sub, "config", "commit.gpgsign", "false")
    _git(sub, "checkout", "-q", "--detach", first)
    _git(superproject, "add", "okg")
    _git(superproject, "commit", "-q", "-m", "pin okg")
    # Editable install from the submodule checkout itself.
    monkeypatch.setattr(dd, "_installed_package_root", lambda: sub / "src" / "okg")
    _pinned_manifest(monkeypatch, sub)
    return {"super": superproject, "sub": sub, "pin": first, "later": later}


def test_submodule_at_its_committed_pin_is_healthy(examplekb) -> None:
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.ok, v.problems
    assert v.pin_source == "superproject_gitlink"
    assert v.pinned_revision == v.head == v.installed_revision == examplekb["pin"]
    assert v.submodule_path == "okg" and v.staged_revision is None
    assert v.to_dict()["pinned_revision"] == examplekb["pin"]


@pytest.mark.parametrize("how", ["new-local-commit", "checkout-upstream"])
def test_submodule_head_moved_off_the_pin_is_an_error(examplekb, how) -> None:
    """Lane A's kb76 gap: the moved HEAD also moves the editable install's
    build revision, so only the committed gitlink can catch it."""
    sub = examplekb["sub"]
    if how == "new-local-commit":
        (sub / "local.txt").write_text("x\n", encoding="utf-8")
        moved = _commit_all(sub, "local okg change")
    else:
        _git(sub, "checkout", "-q", "--detach", examplekb["later"])
        moved = examplekb["later"]
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert v.installed_revision == v.head == moved
    joined = " ".join(v.problems)
    assert moved in joined and examplekb["pin"] in joined
    assert "without updating the pin" in joined
    assert "staged" not in joined


def test_submodule_moved_and_staged_but_not_committed_is_an_error(examplekb) -> None:
    """Decision: the superproject's COMMITTED HEAD is the pin. A staged
    gitlink is not the pin until committed; the message says it is staged."""
    _git(examplekb["sub"], "checkout", "-q", "--detach", examplekb["later"])
    _git(examplekb["super"], "add", "okg")
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert v.staged_revision == examplekb["later"]
    assert v.pinned_revision == examplekb["pin"]
    assert "a staged gitlink is not the pin until it is committed" in " ".join(
        v.problems
    )


def test_submodule_at_pin_with_a_different_staged_pin_is_a_warning(examplekb) -> None:
    """The running code IS the committed pin; a pending staged bump to some
    other commit is named as a warning, not ignored."""
    _git(examplekb["sub"], "checkout", "-q", "--detach", examplekb["later"])
    _git(examplekb["super"], "add", "okg")
    _git(examplekb["sub"], "checkout", "-q", "--detach", examplekb["pin"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "warning"
    assert v.head == v.pinned_revision == examplekb["pin"]
    assert v.staged_revision == examplekb["later"]
    assert "stages a pin change" in " ".join(v.problems)


def test_submodule_bump_committed_in_the_superproject_is_healthy(examplekb) -> None:
    _git(examplekb["sub"], "checkout", "-q", "--detach", examplekb["later"])
    _git(examplekb["super"], "add", "okg")
    _git(examplekb["super"], "commit", "-q", "-m", "bump okg")
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.ok, v.problems
    assert v.pinned_revision == v.head == examplekb["later"]


def test_submodule_never_committed_in_the_superproject_is_an_error(
    tmp_path, monkeypatch
) -> None:
    okg_src = tmp_path / "okg-upstream"
    _init_repo(okg_src, {"src/okg/__init__.py": ""})
    superproject = tmp_path / "example-kb"
    _init_repo(superproject, {"README.md": "example-kb\n"})
    subprocess.run(
        ["git", "-C", str(superproject), "-c", "protocol.file.allow=always",
         "submodule", "add", "--quiet", str(okg_src), "okg"],
        check=True, capture_output=True,
    )
    sub = superproject / "okg"
    monkeypatch.setattr(dd, "_installed_package_root", lambda: sub / "src" / "okg")
    _pinned_manifest(monkeypatch, sub)
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert "records no gitlink" in " ".join(v.problems)


def test_dirty_submodule_is_a_warning_and_a_dogfood_blocker(
    examplekb, tmp_path, monkeypatch
) -> None:
    """The probe says warning; dogfood readiness turns every non-ok verdict
    into a blocker (as it does on dev for track_base warnings)."""
    import psycopg

    from okg.substrate.cli import deployment as deployment_cli

    (examplekb["sub"] / "README.md").write_text("edited\n", encoding="utf-8")
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.status == "warning" and "README.md" in " ".join(v.problems)
    monkeypatch.setattr(
        psycopg, "connect",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            psycopg.OperationalError("database unavailable")
        ),
    )
    for profile, bucket in (("dogfood", "blockers"), ("general", "warnings")):
        payload = deployment_cli._deployment_ready_live_checks(
            deployment="example-kb", deployment_dir=tmp_path,
            dsn="postgres://redacted", max_active_queue_depth=0,
            max_source_age_seconds=3600, readiness_profile=profile,
            skip_doctor=True,
        )
        assert "deployment.code_drift" in {r["id"] for r in payload[bucket]}, profile


# --------------------------------------------------------------------------
# Review of 10c4b00efc (round 3).


def test_dirty_separate_running_checkout_is_a_warning(pinned, monkeypatch) -> None:
    """MED-1: the package runs from a checkout other than sync_root; editing
    that checkout's src/okg changed the running code while the verdict read
    ok."""
    running = pinned["okg_src"]
    (running / "src" / "okg" / "__init__.py").write_text(
        "RUNNING = 'edited'\n", encoding="utf-8",
    )
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "warning"
    assert v.package_checkout == str(running)
    assert v.package_dirty_path_count == 1
    assert v.package_dirty_paths == ["src/okg/__init__.py"]
    assert v.dirty_path_count == 0, "the sync_root itself is clean"
    joined = " ".join(v.problems)
    assert f"the running okg checkout {running}" in joined
    assert "src/okg/__init__.py" in joined
    assert v.to_dict()["package_dirty_paths"] == ["src/okg/__init__.py"]


def _fsmonitor_hook(tmp_path: Path, name: str) -> tuple[Path, Path]:
    marker = tmp_path / f"{name}.ran"
    hook = tmp_path / f"{name}-fsmonitor.sh"
    hook.write_text(f"#!/bin/sh\ntouch '{marker}'\nexit 1\n", encoding="utf-8")
    hook.chmod(0o755)
    return hook, marker


def test_probe_never_runs_a_repository_fsmonitor_hook(
    examplekb, tmp_path, monkeypatch
) -> None:
    """MED-2: `git status`, `ls-files` and the superproject read ran the
    program `core.fsmonitor` names in the checked-out repositories' config."""
    hooks = {}
    for name, repo in (("sub", examplekb["sub"]), ("super", examplekb["super"])):
        hook, marker = _fsmonitor_hook(tmp_path, name)
        _git(repo, "config", "core.fsmonitor", str(hook))
        hooks[name] = (repo, marker)
    # The hooks are live: plain git runs them. Clear the markers only after
    # every plain run, because `git status` in the superproject recurses into
    # the submodule and fires its hook too.
    for name, (repo, marker) in hooks.items():
        subprocess.run(["git", "-C", str(repo), "status", "--porcelain"],
                       capture_output=True, check=False)
        assert marker.exists(), f"{name} fsmonitor hook did not fire for plain git"
    for _, marker in hooks.values():
        marker.unlink()
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.ok, v.problems
    for name, (_, marker) in hooks.items():
        assert not marker.exists(), f"the probe ran the {name} fsmonitor hook"


def test_probe_does_not_rewrite_the_index(pinned, monkeypatch) -> None:
    """LOW-1: `git status` refreshed the index under index.lock, which can
    race an operator's commit. The probe must leave the index untouched."""
    import os

    root = pinned["sync_root"]
    later = (root / "README.md").stat().st_mtime + 120
    os.utime(root / "README.md", (later, later))
    index = root / ".git" / "index"
    before = index.stat().st_mtime_ns
    _pinned_manifest(monkeypatch, root)
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.ok, v.problems
    assert index.stat().st_mtime_ns == before, "the probe rewrote the index"
    # Plain `git status` does rewrite it here, so the check above is live.
    subprocess.run(["git", "-C", str(root), "status", "--porcelain"],
                   capture_output=True, check=True)
    assert index.stat().st_mtime_ns != before


def test_probe_budget_caps_every_git_call(pinned, monkeypatch) -> None:
    """LOW-2: pinned mode's worst case exceeded the doctor's 60s budget."""
    seen: list[float] = []
    real_run = dd.subprocess.run

    def recording_run(*args, **kwargs):
        # Only the probe's own git calls; the build-revision read in
        # code_identity carries its own fixed 5s timeout.
        if "--no-optional-locks" in args[0]:
            seen.append(kwargs["timeout"])
        return real_run(*args, **kwargs)

    monkeypatch.setattr(dd.subprocess, "run", recording_run)
    monkeypatch.setattr(dd, "_PROBE_BUDGET_S", 3.0)
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert v.ok, v.problems
    assert seen and max(seen) <= 3.0, seen
    assert len(seen) <= 8, f"pinned mode made {len(seen)} git calls"


def test_spent_probe_budget_is_an_error_not_a_pass(pinned, monkeypatch) -> None:
    monkeypatch.setattr(dd, "_PROBE_BUDGET_S", 0.0)
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert "spent its 0s budget" in " ".join(v.problems)


def test_code_drift_schema_is_closed_over_the_probe_keys() -> None:
    """LOW-3: the schema and the probe agree on the block's keys and modes."""
    from okg.substrate.deployments.keys import deployment_json_schema

    block = deployment_json_schema()["properties"]["code_drift"]
    assert block["additionalProperties"] is False
    assert set(block["properties"]) == set(dd._CODE_DRIFT_KEYS)
    assert block["properties"]["mode"]["enum"] == list(dd._MODES)


@pytest.mark.parametrize("fails", ["superproject-read", "ls-tree", "ls-files"])
def test_unreadable_superproject_pin_is_an_error_with_gits_message(
    examplekb, monkeypatch, fails
) -> None:
    """LOW-4: a failed read reported "not a submodule" or "records no
    gitlink" instead of git's own error."""
    real = dd._git_run
    trigger = {
        "superproject-read": "--show-superproject-working-tree",
        "ls-tree": "ls-tree",
        "ls-files": "ls-files",
    }[fails]

    def failing(root, *args):
        if trigger in args:
            return 128, "", "fatal: simulated failure"
        return real(root, *args)

    monkeypatch.setattr(dd, "_git_run", failing)
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert v.pin_source == "unreadable"
    joined = " ".join(v.problems)
    assert "fatal: simulated failure" in joined
    assert "records no gitlink" not in joined


def test_failed_superproject_read_in_the_separate_checkout_shape_is_an_error(
    pinned, monkeypatch
) -> None:
    """LOW-4: with a separate checkout the verdict could still read ok after a
    failed superproject read, silently skipping the gitlink check."""
    real = dd._git_run

    def failing(root, *args):
        if "--show-superproject-working-tree" in args:
            return 128, "", "fatal: simulated failure"
        return real(root, *args)

    monkeypatch.setattr(dd, "_git_run", failing)
    _pinned_manifest(monkeypatch, pinned["sync_root"])
    v = check_deployment_code_drift(deployment="example-kb")
    assert not v.ok and v.status == "error"
    assert "could not tell whether sync_root is a git submodule" in " ".join(
        v.problems
    )
