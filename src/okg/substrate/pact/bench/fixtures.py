"""Frozen fixture corpus for the PACT friction benchmark.

Everything the harness measures is authored here: a tmp-dir git repo with a
minimal-but-valid ``pact/`` tree, a clean manifest corpus, and the two-family
mutation corpus (a mutant that VIOLATES each check, and a subject-deletion
mutant that removes what the check quantifies over).
"""
from __future__ import annotations

import copy
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from okg.substrate.pact.bench.isolation import FIXTURE_GIT_DATE

PROJECT_SETTINGS: dict[str, Any] = {
    "pact_version": 4,
    "project": "pact-bench",
    "default_change_status": "proposal",
    "canonical_change_file": "pact.yaml",
    "pact_defaults": {
        "artifact_format_version": 4,
        "target_pact_version": 4,
        "review_policy": "pact_v4_decision_ledger",
    },
    "command_authority": {"repo_command": "uv run okg"},
    "graph_projection": {"deployment": "pact-bench"},
    "default_evidence_policy": {"required": ["planned_test"]},
}


# The probe kind for every "outside the registry" row. It must be a kind the
# registry genuinely does not know: the audit found this class through
# `intent_review`, but `okg pact evidence approve` WRITES that kind, so it is
# now a canonical kind (a verb must not emit a row its validator refuses).
# Probing with a registered kind would measure nothing.
UNREGISTERED_KIND = "rogue_kind"


def _requirement(
    req_id: str,
    *,
    required: list[str] | None = None,
    acceptance: list[dict[str, Any]] | None = None,
    statement: str | None = None,
) -> dict[str, Any]:
    return {
        "id": req_id,
        "title": f"Requirement {req_id}",
        "statement": statement or f"The behavior named by {req_id} must hold.",
        "evidence_policy": {"required": list(required or ["executed_test"])},
        "scenarios": [],
        "acceptance": list(acceptance or []),
    }


def _task(
    task_id: str,
    *,
    status: str = "todo",
    requirements: list[str] | None = None,
    evidence: list[str] | None = None,
    verification: str | None = None,
    coordination_generation_id: int | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "title": f"Task {task_id}",
        "status": status,
        "verification": verification
        or "uv run pytest tests/bench/test_probe.py -q passes.",
        "requirements": list(requirements if requirements is not None else ["req.one"]),
        "evidence": list(evidence or []),
        "environment": [],
    }
    if coordination_generation_id is not None:
        task["coordination_generation_id"] = coordination_generation_id
    return task


def base_manifest(change_id: str, *, status: str = "implementation") -> dict[str, Any]:
    """The clean, valid, single-requirement/single-task shape."""
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {
            "id": change_id,
            "title": f"Bench fixture {change_id}",
            "status": status,
            "tier": 1,
            "rationale": "Frozen benchmark fixture; not a real change.",
            "root_cause": "Frozen benchmark fixture; not a real change.",
        },
        "requirements": [_requirement("req.one")],
        "tasks": [_task("task.one")],
        "dependencies": [],
        "test_plan": [
            {
                "id": "test.req-one.probe",
                "requirement": "req.one",
                "test_id": "tests/bench/test_probe.py",
                "evidence": ["pytest"],
            }
        ],
        "formal": [],
        "waivers": [],
        "evidence": [],
    }


def evidence_row(
    ev_id: str,
    *,
    kind: str = "executed_test",
    refs: list[str] | None = None,
    status: str = "pass",
) -> dict[str, Any]:
    return {
        "id": ev_id,
        "kind": kind,
        "status": status,
        # `refs or [...]` would silently rewrite an EMPTY list back to the
        # default, so the subject-deletion mutant that removes every ref never
        # actually removed one — it scored as a miss against a defect it had
        # not seeded. None means "unspecified"; [] means "deleted".
        "refs": ["req.one"] if refs is None else list(refs),
        "artifact": "tests/bench/test_probe.py",
        "command": "uv run pytest tests/bench/test_probe.py -q",
        "summary": "Frozen benchmark fixture evidence.",
        "data": {},
    }


def proven_manifest(change_id: str) -> dict[str, Any]:
    """A manifest whose single task's proof obligation is fully met."""
    manifest = base_manifest(change_id)
    manifest["evidence"] = [evidence_row("ev.executed-test.req-one.frozen")]
    manifest["tasks"][0]["evidence"] = ["ev.executed-test.req-one.frozen"]
    return manifest


def blocked_manifest(change_id: str) -> dict[str, Any]:
    """Every task blocked — the bootstrap lie-class subject."""
    manifest = base_manifest(change_id)
    manifest["tasks"] = [
        {
            **_task("task.blocked", status="blocked"),
            "blocked_reason": "waiting on an out-of-band operator action",
        }
    ]
    return manifest


# The generation the pinned arm CLAIMS. Deliberately not
# ``RuntimeControls.graph_generation_id`` (4242, what the stubbed projection
# reports): the subject-present arm is a pin the graph can refute, so the two
# numbers must differ or the arm asserts nothing.
FIXTURE_PINNED_GENERATION = 4243


def graph_scoped_manifest(change_id: str, *, pinned: bool) -> dict[str, Any]:
    """Graph-authority fixture: a change whose proof depends on the graph.

    The change is graph-scoped in the way the requirement describes: its
    requirement's evidence policy DEMANDS ``graph_report`` proof. Both arms
    carry that demand — it is the change's contract, not the mutation.

    Pinned is the subject-present arm: the task pins a coordination generation
    the graph DISAGREES with, so the gate has an explicit claim to refute
    (`generation_pin_mismatch`). Unpinned is the subject-deletion arm: the pin
    is deleted while the graph demand remains, and the projection is missing
    (`graph_not_ready` / `graph_not_fresh`). The two arms must therefore refuse
    for DIFFERENT graph reasons; identical blocker lists would mean one arm is
    riding on the other's refusal.
    """
    manifest = proven_manifest(change_id)
    manifest["requirements"][0]["evidence_policy"] = {
        "required": ["executed_test", "graph_report"]
    }
    if pinned:
        manifest["tasks"][0]["coordination_generation_id"] = (
            FIXTURE_PINNED_GENERATION
        )
    return manifest


# --- clean corpus (metric 2b specificity sample + metric 5 subjects) --------


def structured_manifest(change_id: str) -> dict[str, Any]:
    """A clean manifest that uses the structured acceptance slots."""
    manifest = proven_manifest(change_id)
    manifest["requirements"][0]["acceptance"] = [
        {
            "id": "acc.one",
            "description": "The probe test passes under the frozen fixture.",
            "evidence": ["ev.executed-test.req-one.frozen"],
        }
    ]
    return manifest


def multi_manifest(change_id: str) -> dict[str, Any]:
    """Two requirements, two tasks, one done and proven."""
    manifest = proven_manifest(change_id)
    manifest["requirements"].append(
        _requirement("req.two", required=["design_review"])
    )
    manifest["evidence"].append(
        evidence_row(
            "ev.design-review.req-two.frozen",
            kind="design_review",
            refs=["req.two"],
        )
    )
    manifest["tasks"][0]["status"] = "done"
    manifest["tasks"].append(
        _task(
            "task.two",
            requirements=["req.two"],
            evidence=["ev.design-review.req-two.frozen"],
        )
    )
    manifest["test_plan"].append(
        {
            "id": "test.req-two.probe",
            "requirement": "req.two",
            "test_id": "tests/bench/test_probe.py",
            "evidence": ["pytest"],
        }
    )
    return manifest


def clean_corpus() -> dict[str, dict[str, Any]]:
    return {
        "bench-clean-minimal": base_manifest("bench-clean-minimal"),
        "bench-clean-proven": proven_manifest("bench-clean-proven"),
        "bench-clean-structured": structured_manifest("bench-clean-structured"),
        "bench-clean-multi": multi_manifest("bench-clean-multi"),
    }


# --- frozen sample of the ACTIVE corpus (metric 2b specificity subject) -----

CORPUS_DIR = Path(__file__).parent / "corpus"


def frozen_corpus() -> dict[str, dict[str, Any]]:
    """Real repo manifests, copied here once and frozen.

    Metric 2b asks whether `gate post-edit` accepts real, unmutated work.
    Four synthetic manifests written by the harness itself cannot answer that
    — they only prove the gate accepts what the harness knows how to write.
    These are verbatim copies of manifests from ``pact/changes/``, committed
    so the number cannot move when someone edits the live change.
    """
    manifests: dict[str, dict[str, Any]] = {}
    for path in sorted(CORPUS_DIR.glob("*.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise ValueError(f"frozen corpus manifest is not a mapping: {path}")
        change_id = document["change"]["id"]
        if change_id != path.stem:
            raise ValueError(
                f"frozen corpus file {path.name} declares change id {change_id!r}"
            )
        manifests[change_id] = document
    if not manifests:
        raise ValueError(f"frozen corpus is empty: {CORPUS_DIR}")
    return manifests


# --- two-family mutation corpus (metric 2) ---------------------------------


@dataclass(frozen=True)
class Mutant:
    """One seeded defect.

    ``family`` is ``subject_present`` (the mutant VIOLATES the check) or
    ``subject_absent`` (the mutant DELETES what the check quantifies over).
    ``gate`` names which gate is expected to notice.
    """

    check: str
    family: str
    name: str
    manifest: dict[str, Any]
    gate: str = "post_edit"
    task_id: str | None = None
    graph_projection: str = "withheld"
    defect_codes: tuple[str, ...] = ()


def _mutate(builder: Any, change_id: str, fn: Any) -> dict[str, Any]:
    manifest = copy.deepcopy(builder(change_id))
    fn(manifest)
    return manifest


def mutation_corpus() -> tuple[Mutant, ...]:
    """Both families for every gate check the program measures."""
    mutants: list[Mutant] = []

    def add(
        check: str,
        family: str,
        name: str,
        builder: Any,
        fn: Any,
        *,
        gate: str = "post_edit",
        task_id: str | None = None,
        graph_projection: str = "withheld",
        codes: tuple[str, ...] = (),
    ) -> None:
        mutants.append(
            Mutant(
                check=check,
                family=family,
                name=name,
                manifest=_mutate(builder, f"bench-mut-{name.replace('_', '-')}", fn),
                gate=gate,
                task_id=task_id,
                graph_projection=graph_projection,
                defect_codes=codes,
            )
        )

    # missing_evidence_policy -----------------------------------------------
    def _empty_policy(m: dict[str, Any]) -> None:
        m["requirements"][0]["evidence_policy"] = {"required": []}

    add(
        "missing_evidence_policy",
        "subject_present",
        "empty_evidence_policy",
        base_manifest,
        _empty_policy,
        codes=("missing_evidence_policy",),
    )

    def _delete_requirements(m: dict[str, Any]) -> None:
        m["requirements"] = []
        m["test_plan"] = []
        m["tasks"][0]["requirements"] = []

    add(
        "missing_evidence_policy",
        "subject_absent",
        "no_requirements_at_all",
        base_manifest,
        _delete_requirements,
        codes=("missing_evidence_policy",),
    )

    # unknown_evidence_ref --------------------------------------------------
    def _dangling_ref(m: dict[str, Any]) -> None:
        m["evidence"] = [
            evidence_row("ev.executed-test.dangling", refs=["req.does-not-exist"])
        ]

    add(
        "unknown_evidence_ref",
        "subject_present",
        "dangling_evidence_ref",
        base_manifest,
        _dangling_ref,
        codes=("unknown_evidence_ref",),
    )

    def _refless_evidence(m: dict[str, Any]) -> None:
        m["evidence"] = [evidence_row("ev.executed-test.refless", refs=[])]

    add(
        "unknown_evidence_ref",
        "subject_absent",
        "evidence_without_refs",
        base_manifest,
        _refless_evidence,
        codes=("unknown_evidence_ref",),
    )

    # task_done_missing_evidence -------------------------------------------
    def _done_unproven(m: dict[str, Any]) -> None:
        m["tasks"][0]["status"] = "done"

    add(
        "task_done_missing_evidence",
        "subject_present",
        "done_task_zero_evidence",
        base_manifest,
        _done_unproven,
        task_id="task.one",
        codes=("task_done_missing_evidence",),
    )

    def _done_unlinked(m: dict[str, Any]) -> None:
        m["tasks"][0]["status"] = "done"
        m["tasks"][0]["requirements"] = []
        m["tasks"][0]["evidence"] = []

    # The named compound row: the 35-task incident's exact mechanism.
    add(
        "task_done_missing_evidence",
        "subject_absent",
        "compound_unlinked_and_done",
        base_manifest,
        _done_unlinked,
        task_id="task.one",
        codes=("task_done_missing_evidence",),
    )

    # task_requirements_link ------------------------------------------------
    def _unlinked_todo(m: dict[str, Any]) -> None:
        m["tasks"][0]["requirements"] = []

    # No validator clause emits this code today — that IS the miss. It is
    # declared so the row becomes scoreable the moment the check lands: a
    # catch must be attributed to this code, not to some unrelated refusal.
    add(
        "task_requirements_link",
        "subject_absent",
        "unlinked_task",
        base_manifest,
        _unlinked_todo,
        codes=("task_requirements_link",),
    )

    def _unknown_requirement_link(m: dict[str, Any]) -> None:
        m["tasks"][0]["requirements"] = ["req.does-not-exist"]

    add(
        "task_requirements_link",
        "subject_present",
        "task_links_unknown_requirement",
        base_manifest,
        _unknown_requirement_link,
        codes=("unknown_requirement_ref",),
    )

    # unknown_evidence_kind -------------------------------------------------
    def _rogue_kind(m: dict[str, Any]) -> None:
        m["evidence"] = [
            evidence_row("ev.rogue-kind.req-one.rogue", kind=UNREGISTERED_KIND)
        ]
        m["tasks"][0]["evidence"] = ["ev.rogue-kind.req-one.rogue"]

    add(
        "unknown_evidence_kind",
        "subject_present",
        "evidence_kind_outside_registry",
        base_manifest,
        _rogue_kind,
        codes=("unknown_evidence_kind",),
    )

    def _rogue_policy_kind(m: dict[str, Any]) -> None:
        m["requirements"][0]["evidence_policy"] = {"required": [UNREGISTERED_KIND]}

    add(
        "unknown_evidence_kind",
        "subject_absent",
        "policy_kind_outside_registry",
        base_manifest,
        _rogue_policy_kind,
        codes=("unknown_evidence_kind",),
    )

    # graph_authority -------------------------------------------------------
    # Subject-present: the task pins a coordination generation and the stubbed
    # projection is FRESH but reports a different generation, so the pin is a
    # claim the graph refutes — `generation_pin_mismatch`, on its own, with no
    # help from the missing-projection blockers the other arm rides on.
    add(
        "graph_authority",
        "subject_present",
        "pinned_generation_refuted_by_fresh_projection",
        lambda cid: graph_scoped_manifest(cid, pinned=True),
        lambda _m: None,
        gate="task_done",
        task_id="task.one",
        graph_projection="fresh",
    )
    # Subject-absent: the same graph-scoped change with the pin DELETED and the
    # projection missing. The requirement still demands `graph_report` proof,
    # so `graph_required` stays True and the clause still has a subject —
    # reading the pin alone used to collapse it to False, which is the vacuity
    # this row measures. Its blockers are the projection ones, and must NOT
    # include `generation_pin_mismatch`: with no pin there is nothing to
    # mismatch, and reporting one would be absence read as disagreement.
    add(
        "graph_authority",
        "subject_absent",
        "unpinned_generation_projection_missing",
        lambda cid: graph_scoped_manifest(cid, pinned=False),
        lambda _m: None,
        gate="task_done",
        task_id="task.one",
        graph_projection="missing",
    )

    return tuple(mutants)


# --- repo construction -----------------------------------------------------


@dataclass
class FixtureRepo:
    root: Path
    changes: list[str] = field(default_factory=list)

    def write_change(self, manifest: dict[str, Any]) -> str:
        change_id = manifest["change"]["id"]
        directory = self.root / "pact" / "changes" / change_id
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "pact.yaml").write_text(
            yaml.safe_dump(manifest, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )
        if change_id not in self.changes:
            self.changes.append(change_id)
        return change_id

    def manifest_path(self, change_id: str) -> Path:
        return self.root / "pact" / "changes" / change_id / "pact.yaml"


def install_agent_skills(root: Path) -> dict[str, int]:
    """Install the bundled agent-skill trees the bootstrap gate checks.

    Without them ``agent_skill_freshness`` reports every skill missing, and
    that becomes the BINDING blocker on every bootstrap packet — so a probe
    asking "does bootstrap hide a blocked change?" would pass for the wrong
    reason (an uninstalled fixture, not a blocked manifest). The gate reads
    ``.claude/skills`` and ``.agents/skills`` (agent_install.py:564-571), so
    the fixture ships them.
    """
    from okg.substrate.pact.agent_install import bundled_skills

    installed = {}
    for relative in (Path(".claude") / "skills", Path(".agents") / "skills"):
        skills_root = root / relative
        skills_root.mkdir(parents=True, exist_ok=True)
        count = 0
        for skill in bundled_skills():
            target = skills_root / skill.name
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(skill, target)
            count += 1
        installed[relative.as_posix()] = count
    return installed


def build_repo(root: Path) -> FixtureRepo:
    """Create the fixture repo: pact tree, probe test, agent skills, git commit."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "pact" / "changes").mkdir(parents=True, exist_ok=True)
    (root / "pact" / "project.yaml").write_text(
        yaml.safe_dump(PROJECT_SETTINGS, sort_keys=False), encoding="utf-8"
    )
    tests_dir = root / "tests" / "bench"
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / "test_probe.py").write_text(
        "def test_probe():\n    assert True\n", encoding="utf-8"
    )
    install_agent_skills(root)
    return FixtureRepo(root=root)


def git_init(root: Path) -> None:
    """Initialize the fixture repo with the frozen identity and date.

    This runs during SETUP, not in the measured path: the fixture must be a
    real git repo because ``task_completion_gate`` reads git evidence, and the
    frozen identity/date keep its commit sha stable across machines.
    """
    env = {
        **os.environ,
        "GIT_AUTHOR_DATE": FIXTURE_GIT_DATE,
        "GIT_COMMITTER_DATE": FIXTURE_GIT_DATE,
    }
    for command in (
        ["git", "init", "-q", "-b", "main"],
        ["git", "add", "-A"],
        ["git", "commit", "-q", "-m", "pact bench fixture"],
    ):
        subprocess.run(  # noqa: S603 - fixed argv, fixture dir, no shell
            command,
            cwd=root,
            check=True,
            capture_output=True,
            env=env,
        )
