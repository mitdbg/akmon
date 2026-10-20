"""Acceptance-satisfaction flip census, measured by RUNNING the validator.

`pact-acceptance-prose-is-enforceable`
req.pact-acceptance-prose-is-enforceable.flip-is-disclosed requires the census
to be produced by executing the validator at the pre-change commit and at the
post-change tree -- not by reasoning about which criteria ought to flip, and
not by reconstructing the old verdicts with new code.

So each arm runs in its OWN subprocess against its OWN copy of the package:

* the pre arm imports `okg.substrate.pact` from a `git archive` export of the
  pre-change commit;
* the post arm imports the working tree's package.

BOTH arms read the same manifests -- the working tree's `pact/changes/`. Only
the code differs. Letting the manifests differ too would conflate the rule
change with ordinary forward progress: every criterion closed by evidence
attached since the pre-change commit would surface as "newly satisfied", and
the census would expire on the next commit that attaches any evidence.

Both arms evaluate with ``repo_root=None`` but with the same project evidence
registry captured in ``PactEvaluationContext``. The missing repo root prevents
formal-evidence freshness from digesting artifacts that exist in the working
tree and not in a `git archive` export; the captured registry prevents project
aliases from silently falling back to code defaults. Held equal, the two
verdicts differ only by the CODE being measured.

The module is a library for `test_acceptance_flip_census.py` AND the worker
those subprocesses execute (see ``__main__`` at the bottom). The leading
underscore keeps pytest from collecting it as a test module.
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
CENSUS_PATH = (
    REPO_ROOT
    / "pact"
    / "archive"
    / "pact-acceptance-prose-is-enforceable"
    / "reports"
    / "acceptance-flip-census.json"
)


# --------------------------------------------------------------------------
# worker: one arm's verdicts
# --------------------------------------------------------------------------
def _verdicts(changes_root: Path) -> dict[str, Any]:
    """Every acceptance and requirement verdict under one `pact/changes/`."""
    import yaml

    from okg.substrate.pact.evaluation_context import PactEvaluationContext
    from okg.substrate.pact.evidence_kinds import load_registry
    from okg.substrate.pact.models import PactManifest
    from okg.substrate.pact.validation import derive_alignment

    context = PactEvaluationContext(
        evidence_registry=load_registry(changes_root.parents[1])
    )
    out: dict[str, Any] = {}
    for path in sorted(changes_root.glob("*/pact.yaml")):
        change_id = path.parent.name
        try:
            manifest = PactManifest.model_validate(
                yaml.safe_load(path.read_text(encoding="utf-8"))
            )
            alignment = derive_alignment(manifest, context=context)
        except Exception as exc:  # noqa: BLE001 - a manifest this arm cannot
            # parse is recorded as such; dropping it silently would let a
            # parse regression read as "nothing flipped here".
            out[change_id] = {"error": f"{type(exc).__name__}: {exc}"[:200]}
            continue
        out[change_id] = {
            "acceptance": {
                acc_id: {
                    "status": state.get("satisfaction_status"),
                    "requirement": state.get("requirement"),
                    "evidence": sorted(
                        {
                            evidence_id
                            for row in state.get("proofs", [])
                            for evidence_id in row.get("evidence", [])
                        }
                    ),
                }
                for acc_id, state in alignment.get("acceptance", {}).items()
            },
            "requirements": {
                req_id: state.get("satisfaction_status")
                for req_id, state in alignment.get("requirements", {}).items()
            },
        }
    return out


# --------------------------------------------------------------------------
# library: run both arms and diff them
# --------------------------------------------------------------------------
def _ensure_commit_present(commit: str, repo_root: Path) -> None:
    """Fetch `commit` if this checkout does not have it.

    CI checks out at depth 1, so the pre-change commit is simply absent and
    `git archive` exits 128. Fetching just that object is far cheaper than
    giving the most-run lane a full-history clone, and it keeps the census a
    REAL comparison rather than something that silently skips wherever the
    checkout happens to be shallow -- a census that skips on CI would report
    nothing while looking green.
    """
    present = subprocess.run(
        ["git", "-C", str(repo_root), "cat-file", "-e", f"{commit}^{{commit}}"],
        capture_output=True,
    )
    if present.returncode == 0:
        return
    fetched = subprocess.run(
        ["git", "-C", str(repo_root), "fetch", "--depth=1", "origin", commit],
        capture_output=True,
        text=True,
    )
    if fetched.returncode != 0:
        raise RuntimeError(
            f"the census pre-change commit {commit} is not in this checkout "
            f"and could not be fetched: {fetched.stderr.strip()!r}. On CI this "
            f"means the job needs `fetch-depth: 0` (or a fetch of that commit) "
            f"— the census cannot be computed without pre-change code."
        )


def export_commit(commit: str, dest: Path, *, repo_root: Path = REPO_ROOT) -> Path:
    """Extract `src` and `pact` at `commit` into `dest`, read-only.

    `git archive` never touches the index or the working tree, which matters:
    this repo is a shared checkout with other worktrees live against it.
    """
    _ensure_commit_present(commit, repo_root)
    dest.mkdir(parents=True, exist_ok=True)
    archive = subprocess.run(
        ["git", "-C", str(repo_root), "archive", commit, "src", "pact"],
        check=True,
        capture_output=True,
    ).stdout
    with tempfile.NamedTemporaryFile(suffix=".tar") as handle:
        handle.write(archive)
        handle.flush()
        with tarfile.open(handle.name) as tar:
            tar.extractall(dest, filter="data")
    return dest


def verdicts_from(src_root: Path, changes_root: Path) -> dict[str, Any]:
    """Run one arm in a subprocess bound to `src_root`'s package copy."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(src_root), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            str(src_root),
            str(changes_root),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return json.loads(completed.stdout)


def build_census(pre_commit: str, *, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    """The census: both arms, and the criteria that flipped between them."""
    changes_root = repo_root / "pact" / "changes"
    with tempfile.TemporaryDirectory(prefix="pact-flip-census-") as tmp:
        pre_root = export_commit(pre_commit, Path(tmp) / "pre", repo_root=repo_root)
        # Pre-change CODE, working-tree MANIFESTS. Only ONE variable moves.
        #
        # Reading the pre-commit's own pact/changes/ here would move two: the
        # rules AND the manifests. Every criterion closed by evidence attached
        # since pre_commit would then read as "newly satisfied" — ordinary
        # forward progress on unrelated PACTs, indistinguishable from the
        # change loosening a rule. It also makes the census expire: the census
        # would fail again on the next commit that attaches any evidence, so
        # the only way to keep it passing is to keep advancing pre_commit,
        # which is a treadmill, not a measurement.
        #
        # Held this way, a flip can only come from the rule change, which is
        # what req.pact-acceptance-prose-is-enforceable.flip-is-disclosed asks
        # the census to disclose. The pre arm still IMPORTS the package from
        # the git archive export, so the pre-change verdicts still come from
        # pre-change code — the property the requirement actually names.
        pre = verdicts_from(pre_root / "src", changes_root)
    post = verdicts_from(repo_root / "src", changes_root)

    flips: list[dict[str, Any]] = []
    newly_satisfied: list[dict[str, Any]] = []
    requirement_flips: list[dict[str, Any]] = []
    # A change the PRE-change code cannot evaluate is skipped below. Silent
    # skipping would let the census shrink toward vacuity while still
    # reporting a non-empty flip set, so the skips are counted and named.
    #
    # The expected members of this set are the manifests that ADOPTED the
    # schema this change introduces -- `status: partial`, acceptance `arms`.
    # Pre-change code rejects them at model validation, which is not a defect
    # in the census but a consequence of measuring a change that extends the
    # schema: a manifest written in the new vocabulary has no pre-change
    # reading. They are named with their error so the class stays checkable
    # rather than being taken on trust.
    not_compared: list[dict[str, Any]] = []
    for change_id, pre_state in sorted(pre.items()):
        post_state = post.get(change_id)
        if post_state is None or "error" in pre_state or "error" in post_state:
            if post_state is None:
                reason, detail = "absent from the post arm", ""
            elif "error" in pre_state:
                reason, detail = "pre arm errored", str(pre_state["error"])
            else:
                reason, detail = "post arm errored", str(post_state["error"])
            not_compared.append(
                {
                    "change": change_id,
                    "reason": reason,
                    "detail": detail[:200],
                }
            )
            continue
        for acc_id, before in sorted(pre_state["acceptance"].items()):
            after = post_state["acceptance"].get(acc_id)
            if after is None:
                continue
            if before["status"] == "satisfied" and after["status"] != "satisfied":
                flips.append(
                    {
                        "change": change_id,
                        "criterion": acc_id,
                        "requirement": before.get("requirement"),
                        "pre": before["status"],
                        "post": after["status"],
                        "prior_evidence": before["evidence"],
                    }
                )
            elif before["status"] != "satisfied" and after["status"] == "satisfied":
                newly_satisfied.append(
                    {
                        "change": change_id,
                        "criterion": acc_id,
                        "pre": before["status"],
                        "post": after["status"],
                    }
                )
        for req_id, before_status in sorted(pre_state["requirements"].items()):
            after_status = post_state["requirements"].get(req_id)
            if before_status == "satisfied" and after_status != "satisfied":
                requirement_flips.append(
                    {
                        "change": change_id,
                        "requirement": req_id,
                        "pre": before_status,
                        "post": after_status,
                    }
                )

    # Both sets are restricted to the COMPARABLE changes: present and
    # error-free in both arms. Spanning all changes would put the six the pre
    # arm cannot parse into `post_satisfied` with no `pre_satisfied` counterpart
    # and make post ⊆ pre fail for a reason that is not a loosening — the pre
    # arm has no reading there, which is not the same as "unsatisfied".
    # Comparing a set against a domain one side never covered is the same
    # absence-as-agreement error the census exists to catch.
    skipped = {row["change"] for row in not_compared}
    comparable = {c for c in pre if c in post and c not in skipped}
    pre_satisfied = sorted(
        {
            f"{change_id}::{acc_id}"
            for change_id in comparable
            for acc_id, row in pre[change_id]["acceptance"].items()
            if row["status"] == "satisfied"
        }
    )
    post_satisfied = sorted(
        {
            f"{change_id}::{acc_id}"
            for change_id in comparable
            for acc_id, row in post[change_id]["acceptance"].items()
            if row["status"] == "satisfied"
        }
    )
    return {
        "generated_by": "tests/pact/_acceptance_flip_census.py",
        "method": (
            "acceptance satisfaction computed by executing the PACT validator "
            "twice over the SAME manifests (the working tree's pact/changes/), "
            "once importing the package from a git archive export of the "
            "pre-change commit and once from the working tree. Only the CODE "
            "differs between arms, so a flip is attributable to the rule "
            "change rather than to evidence attached since the pre-change "
            "commit. Both arms run with repo_root=None so formal-artifact "
            "digests cannot manufacture flips, while both receive the same "
            "captured project evidence-kind registry. No verdict in this file "
            "is reasoned about."
        ),
        "pre_commit": pre_commit,
        "pre_arm": "pre-change code, working-tree manifests",
        "post": "working tree",
        "counts": {
            "changes_scanned": len(post),
            "changes_compared": len(comparable),
            "changes_not_compared": len(not_compared),
            "acceptance_satisfied_pre": len(pre_satisfied),
            "acceptance_satisfied_post": len(post_satisfied),
            "acceptance_flipped": len(flips),
            "acceptance_newly_satisfied": len(newly_satisfied),
            "requirements_flipped": len(requirement_flips),
        },
        "flips": flips,
        "requirement_flips": requirement_flips,
        "newly_satisfied": newly_satisfied,
        "not_compared": not_compared,
        "pre_satisfied": pre_satisfied,
        "post_satisfied": post_satisfied,
    }


def load_census() -> dict[str, Any]:
    return json.loads(CENSUS_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    _src_root, _changes_root = Path(sys.argv[1]), Path(sys.argv[2])
    sys.path.insert(0, str(_src_root))
    _validation = importlib.import_module("okg.substrate.pact.validation")

    # No silent fallback: if an editable-install finder beat the injected
    # path, this arm would silently measure the WRONG package copy and the
    # census would read as "nothing flipped".
    if not str(Path(_validation.__file__).resolve()).startswith(
        str(_src_root.resolve())
    ):
        raise SystemExit(
            f"census arm loaded {_validation.__file__}, expected a module "
            f"under {_src_root}"
        )
    json.dump(_verdicts(_changes_root), sys.stdout)
