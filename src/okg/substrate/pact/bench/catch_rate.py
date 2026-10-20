"""Metric 2 — gate catch rate (two-family mutation testing) and 2b — specificity.

For every gate check C the corpus seeds two mutants: one that VIOLATES C, and
one SUBJECT-DELETION mutant that removes what C quantifies over. The second
family exists because the motivating incident is invisible to violation
mutants — ``task_done_missing_evidence`` quantifies over a task's linked
requirements, so deleting the links deletes the subject.

Catch rate alone is gameable (refuse everything scores 100%), so metric 2b is
computed from the same run: ``gate post-edit`` must accept 100% of a frozen,
unmutated sample, and the corpus waiver count is reported as its own capped
number.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from okg.substrate.pact.bench import calls
from okg.substrate.pact.bench.calls import Recorder
from okg.substrate.pact.bench.fixtures import (
    FixtureRepo,
    Mutant,
    clean_corpus,
    frozen_corpus,
    mutation_corpus,
)
from okg.substrate.pact.bench.isolation import RuntimeControls

CAUGHT = "caught"
WARNING_MISS = "warning_miss"
MISS = "miss"

# Waivers are the mass-waiver escape hatch metric 2b caps (design: <=10, each
# dated with check + owner).
WAIVER_CAP = 10

# Blockers raised by the graph-authority clause specifically. The graph rows
# must not be scored "caught" because the ledger blocker fired — with no DSN
# every task-done refuses, and reading that as a graph catch would report a
# miss as a victory.
GRAPH_BLOCKER_TOKENS = frozenset(
    {
        "graph_authority_unknown",
        "graph_not_fresh",
        "graph_not_ready",
        "generation_pin_mismatch",
    }
)


def _codes(rows: list[dict[str, Any]] | None) -> list[str]:
    return sorted({str(row.get("code")) for row in (rows or [])})


def _run_validation_mutant(
    recorder: Recorder,
    root: Path,
    mutant: Mutant,
) -> dict[str, Any]:
    change_id = mutant.manifest["change"]["id"]
    result = calls.gate_post_edit(recorder, root, change_id)
    validation = result.payload.get("validation") or {}
    errors = _codes(validation.get("errors"))
    warnings = _codes(validation.get("warnings"))
    # A refusal only counts as a CATCH if the gate refused FOR THIS DEFECT.
    # `ok is False` alone is not evidence: a mutant manifest can trip an
    # unrelated clause, and scoring that as a catch would credit the gate with
    # noticing something it never looked at — the same error the graph arm
    # already avoids by intersecting against GRAPH_BLOCKER_TOKENS.
    attributed = sorted(set(errors) & set(mutant.defect_codes))
    attributed_warnings = sorted(set(warnings) & set(mutant.defect_codes))
    # `refused`, not `not ok`: an unreachable ledger DEFERS the gate, and
    # reading that as a refusal would score every mutant as refused-for-some-
    # reason on a fixture that has no database.
    if attributed and result.refused:
        outcome = CAUGHT
    elif attributed_warnings:
        outcome = WARNING_MISS
    else:
        outcome = MISS
    observed = {
        "outcome": outcome,
        "gate_ok": result.ok,
        "gate_verdict": result.verdict,
        "error_codes": errors,
        "warning_codes": warnings,
        "expected_defect_codes": list(mutant.defect_codes),
        "attributed_codes": attributed,
        # The gate refused, but for something other than the seeded defect.
        # Under the old rule this scored as a catch.
        "refused_without_attribution": bool(result.refused and not attributed),
    }
    if mutant.task_id:
        # The vacuity signal for task-done. With no DSN the gate refuses on
        # the ledger blocker regardless, so "does task-done catch this?" has
        # to be read off the proof obligation itself: an empty `missing_proof`
        # with no validation error means the check found nothing to check.
        done = calls.gate_task_done(recorder, root, change_id, mutant.task_id)
        done_errors = [
            str(row.get("code"))
            for row in done.payload.get("validation_errors") or []
            if row.get("severity") == "error"
        ]
        observed["task_done_missing_proof"] = list(
            done.payload.get("missing_proof") or []
        )
        observed["task_done_error_codes"] = sorted(set(done_errors))
        observed["task_done_proof_obligation_vacuous"] = not (
            observed["task_done_missing_proof"] or done_errors
        )
    return observed


def _run_graph_mutant(
    recorder: Recorder,
    root: Path,
    mutant: Mutant,
    controls: RuntimeControls,
) -> dict[str, Any]:
    change_id = mutant.manifest["change"]["id"]
    previous = controls.graph_projection
    controls.graph_projection = mutant.graph_projection
    try:
        result = calls.gate_task_done(recorder, root, change_id, mutant.task_id or "")
    finally:
        controls.graph_projection = previous
    authority = result.payload.get("workflow_authority") or {}
    blockers = list(authority.get("blockers") or [])
    graph_blockers = sorted(set(blockers) & GRAPH_BLOCKER_TOKENS)
    return {
        "outcome": CAUGHT if graph_blockers else MISS,
        "gate_ok": result.ok,
        "graph_blockers": graph_blockers,
        "other_blockers": sorted(set(blockers) - GRAPH_BLOCKER_TOKENS),
        # READ OFF THE GATE, never inferred from the blockers this same row
        # scores. Deriving it as "some graph blocker fired" made the assertion
        # `graph_required is True` a restatement of `graph_blockers != []` —
        # true by construction on every caught row, and therefore evidence of
        # nothing. `None` when the gate does not report it, so a payload that
        # stops carrying the field reads as unknown rather than False.
        "graph_required": authority.get("graph_required"),
        "graph_generation_pin": authority.get("graph_generation_pin"),
        "projected_generation_id": (
            (authority.get("graph_projection") or {}).get("projected_generation_id")
        ),
        "graph_projection_status": (
            (authority.get("graph_projection") or {}).get("status")
        ),
    }


def seed(repo: FixtureRepo) -> tuple[Mutant, ...]:
    corpus = mutation_corpus()
    for mutant in corpus:
        repo.write_change(mutant.manifest)
    for manifest in clean_corpus().values():
        repo.write_change(manifest)
    # The specificity SUBJECT: real, unmutated manifests copied verbatim out
    # of `pact/changes/`. Written into the fixture so the gate runs against
    # them under the same isolation as everything else.
    for manifest in frozen_corpus().values():
        repo.write_change(manifest)
    return corpus


def run(
    recorder: Recorder,
    root: Path,
    corpus: tuple[Mutant, ...],
    controls: RuntimeControls,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for mutant in corpus:
        if mutant.gate == "task_done":
            observed = _run_graph_mutant(recorder, root, mutant, controls)
        else:
            observed = _run_validation_mutant(recorder, root, mutant)
        rows.append(
            {
                "check": mutant.check,
                "family": mutant.family,
                "name": mutant.name,
                "gate": mutant.gate,
                **observed,
            }
        )
    rows.sort(key=lambda row: (row["check"], row["family"]))
    return {
        "rows": rows,
        "mutants": len(rows),
        "caught": sum(1 for row in rows if row["outcome"] == CAUGHT),
        "warning_miss": sum(1 for row in rows if row["outcome"] == WARNING_MISS),
        "miss": sum(1 for row in rows if row["outcome"] == MISS),
        "catch_rate": _rate(rows),
        "catch_rate_subject_present": _rate(
            [row for row in rows if row["family"] == "subject_present"]
        ),
        "catch_rate_subject_absent": _rate(
            [row for row in rows if row["family"] == "subject_absent"]
        ),
    }


def _rate(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return round(sum(1 for row in rows if row["outcome"] == CAUGHT) / len(rows), 4)


def run_specificity(
    recorder: Recorder,
    root: Path,
    *,
    repo_root: Path | None,
) -> dict[str, Any]:
    # The subject is the FROZEN REAL CORPUS, not the harness's own synthetic
    # manifests. Four manifests the harness wrote itself can only prove the
    # gate accepts what the harness knows how to write; the question metric 2b
    # asks is whether the gate accepts real, unmutated work.
    rows: list[dict[str, Any]] = []
    for change_id in sorted(frozen_corpus()):
        result = calls.gate_post_edit(recorder, root, change_id)
        # The default (budgeted) envelope is measured too: overflow used to
        # flip its `ok` to False, turning a size problem into a fake gate
        # failure. A disagreement here is that defect, not a real refusal.
        summary = calls.gate_post_edit_summary(recorder, root, change_id)
        validation = result.payload.get("validation") or {}
        rows.append(
            {
                "change": change_id,
                # Specificity asks "does the gate refuse clean work?" — a
                # deferral is not a refusal of the manifest, it is an
                # unreachable ledger, so it counts as accepted.
                "accepted": result.accepted,
                "summary_ok": summary.ok,
                "summary_verdict": summary.verdict,
                "summary_disagrees": summary.verdict != result.verdict,
                "error_codes": _codes(validation.get("errors")),
            }
        )
    accepted = sum(1 for row in rows if row["accepted"])
    return {
        "rows": rows,
        "subject": "frozen_real_corpus",
        "sample_size": len(rows),
        "accepted": accepted,
        "acceptance_rate": round(accepted / len(rows), 4) if rows else 0.0,
        "summary_disagreements": sum(1 for row in rows if row["summary_disagrees"]),
        "waiver_sweep": waiver_sweep(repo_root),
    }


def waiver_sweep(repo_root: Path | None) -> dict[str, Any]:
    """Count committed waivers in the repo corpus.

    Two numbers, deliberately not one. ``waivers`` is every waiver in the
    corpus — mostly requirement-scope waivers that predate this program, so it
    moves with unrelated work and cannot be the capped quantity. The CAPPED
    quantity is ``hygiene_waivers``: waivers taken INSTEAD OF fixing a
    corpus-hygiene violation, counted by the same rule the hygiene lane uses to
    suppress a refusal (owner + date required), which is what
    ``pact-vacuous-gate-close`` req.vacuous.adoption caps at 10 so mass-waiver
    cannot satisfy the sweep.

    Never falls back to zero: an unreadable corpus reports ``unavailable`` so
    a missing sweep cannot read as "no waivers".
    """
    if repo_root is None:
        return {
            "status": "unavailable",
            "reason": "no repo root supplied",
            "cap": WAIVER_CAP,
        }
    changes_dir = repo_root / "pact" / "changes"
    if not changes_dir.is_dir():
        return {
            "status": "unavailable",
            "reason": f"no corpus at {changes_dir}",
            "cap": WAIVER_CAP,
        }
    from okg.substrate.pact.corpus_hygiene import hygiene_waivers

    manifests = 0
    waivers = 0
    hygiene_rows: list[dict[str, Any]] = []
    unreadable: list[str] = []
    for path in sorted(changes_dir.glob("*/pact.yaml")):
        manifests += 1
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            unreadable.append(path.parent.name)
            continue
        if not isinstance(document, dict):
            continue
        rows = document.get("waivers") or []
        waivers += len(rows)
        waived = hygiene_waivers(rows)
        if not waived:
            continue
        # Only manifests that actually take a hygiene waiver pay for the
        # violation count, so the sweep stays a YAML read over the whole
        # corpus and a validation pass over the handful that waive.
        suppressed = _suppressed_violations(path, sorted(waived))
        hygiene_rows.extend(
            {
                "change": path.parent.name,
                "check": check,
                "waiver": detail.get("waiver", ""),
                "owner": detail.get("owner", ""),
                # A waiver that swallows 200 refusals is not one waived
                # violation, and the cap on the COUNT cannot see the
                # difference — so the number rides alongside every row.
                "suppressed_violations": suppressed.get(check, 0),
            }
            for check, detail in sorted(waived.items())
        )
    hygiene = len(hygiene_rows)
    return {
        "status": "measured" if not unreadable else "partial",
        "manifests": manifests,
        "waivers": waivers,
        "hygiene_waivers": hygiene,
        "hygiene_waiver_rows": hygiene_rows,
        "suppressed_violations": sum(
            int(row["suppressed_violations"]) for row in hygiene_rows
        ),
        "unreadable": unreadable,
        "cap": WAIVER_CAP,
        "within_cap": waivers <= WAIVER_CAP,
        "hygiene_within_cap": hygiene <= WAIVER_CAP,
    }


def _suppressed_violations(path: Path, checks: list[str]) -> dict[str, int]:
    """How many refusals each waived check raises in this manifest.

    Reported, never inferred: a waiver's cost is the number of violations it
    hides, and a sweep that counted only the waivers would read one waiver
    absorbing a hundred violations as one violation waived.
    """
    from okg.substrate.pact.store import load_manifest_file
    from okg.substrate.pact.validation import structural_issues

    try:
        manifest = load_manifest_file(path)
    except Exception:  # noqa: BLE001 - an unreadable manifest is reported, not zeroed
        return dict.fromkeys(checks, -1)
    counts = dict.fromkeys(checks, 0)
    # The REPO's registry, not the code defaults: a kind this project adopted
    # in `pact/project.yaml` is not a violation, and counting it as one would
    # credit a waiver with suppressing something that never fired.
    repo_root = path.parents[2]
    for issue in structural_issues(manifest, repo_root=repo_root):
        if issue.severity == "error" and issue.code in counts:
            counts[issue.code] += 1
    return counts
