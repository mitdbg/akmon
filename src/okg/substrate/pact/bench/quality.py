"""Metric 5 — manifest quality (static, per-pact.yaml).

Scores STRUCTURED SLOTS ONLY. Requirements whose acceptance lives in prose
``ACCEPTANCE:`` lines (the current lean convention, used by this program's own
manifests) go in an explicit unscored bucket: the metric does not regex-score
English, because scoring prose with regexes produces adjective-counting.

Measurement only — enforcement thresholds are a follow-on decision.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# #1047: a verification clause that names a test path and then deselects it is
# a clause that cannot fail. Detection is string-level and deliberately cheap;
# enforcement is out of scope for this wave.
DESELECTION_MARKERS = (
    "-m 'not ",
    '-m "not ',
    "-k 'not ",
    '-k "not ',
    "--ignore",
    "--deselect",
    "--no-cov",
)

BOILERPLATE_MARKERS = (
    "TODO:",
    "TODO ",
    "state the requirement",
    "the observable check that proves the requirement",
    "describe the scenario",
)

PROSE_ACCEPTANCE_MARKER = "ACCEPTANCE:"


def _requirement_refs(document: dict[str, Any]) -> set[str]:
    return {str(row.get("id")) for row in document.get("requirements") or []}


def _known_ids(document: dict[str, Any]) -> set[str]:
    known = {str((document.get("change") or {}).get("id"))}
    known |= _requirement_refs(document)
    known |= {str(row.get("id")) for row in document.get("tasks") or []}
    known |= {str(row.get("id")) for row in document.get("test_plan") or []}
    known |= {str(row.get("id")) for row in document.get("formal") or []}
    for requirement in document.get("requirements") or []:
        known |= {
            str(row.get("id")) for row in requirement.get("acceptance") or []
        }
    return known


def score_document(change_id: str, document: dict[str, Any]) -> dict[str, Any]:
    requirements = list(document.get("requirements") or [])
    tasks = list(document.get("tasks") or [])
    evidence = list(document.get("evidence") or [])
    evidence_ids = {str(row.get("id")) for row in evidence}
    known = _known_ids(document)

    components: dict[str, float] = {}

    if tasks:
        components["requirements_link_coverage"] = round(
            sum(1 for task in tasks if task.get("requirements")) / len(tasks), 4
        )
    if requirements:
        components["proof_token_coverage"] = round(
            sum(
                1
                for req in requirements
                if (req.get("evidence_policy") or {}).get("required")
                or (req.get("evidence_policy") or {}).get("required_any")
            )
            / len(requirements),
            4,
        )

    scored_requirements = []
    unscored_requirements = []
    for req in requirements:
        structured = list(req.get("acceptance") or [])
        prose = PROSE_ACCEPTANCE_MARKER in str(req.get("statement") or "")
        if structured:
            scored_requirements.append(req)
        elif prose:
            unscored_requirements.append(str(req.get("id")))
        else:
            scored_requirements.append(req)
    if scored_requirements:
        components["structured_acceptance_coverage"] = round(
            sum(
                1
                for req in scored_requirements
                if any(row.get("evidence") for row in req.get("acceptance") or [])
            )
            / len(scored_requirements),
            4,
        )

    refs = 0
    dangling = 0
    for task in tasks:
        for ref in task.get("requirements") or []:
            refs += 1
            dangling += int(str(ref) not in known)
        for ref in task.get("evidence") or []:
            refs += 1
            dangling += int(str(ref) not in evidence_ids)
    for row in evidence:
        for ref in row.get("refs") or []:
            refs += 1
            dangling += int(str(ref) not in known)
    for row in document.get("test_plan") or []:
        refs += 1
        dangling += int(str(row.get("requirement")) not in known)
    for req in requirements:
        for criterion in req.get("acceptance") or []:
            for ref in criterion.get("evidence") or []:
                refs += 1
                dangling += int(str(ref) not in evidence_ids)
    if refs:
        components["reference_integrity"] = round(1 - dangling / refs, 4)

    if tasks:
        components["verification_clause_honesty"] = round(
            sum(
                1
                for task in tasks
                if not any(
                    marker in str(task.get("verification") or "")
                    for marker in DESELECTION_MARKERS
                )
            )
            / len(tasks),
            4,
        )

    slots = []
    for req in requirements:
        slots.append(str(req.get("statement") or ""))
    for task in tasks:
        slots.append(str(task.get("verification") or ""))
        slots.append(str(task.get("title") or ""))
    rationale = str((document.get("change") or {}).get("rationale") or "").strip()
    boilerplate = sum(
        1
        for slot in slots
        if any(marker in slot for marker in BOILERPLATE_MARKERS)
        or (rationale and slot.strip() == rationale)
    )
    if slots:
        components["boilerplate_freedom"] = round(1 - boilerplate / len(slots), 4)

    score = round(sum(components.values()) / len(components), 4) if components else 0.0
    return {
        "change": change_id,
        "score": score,
        "components": dict(sorted(components.items())),
        "unscored_requirements": sorted(unscored_requirements),
        "dangling_refs": dangling,
        "boilerplate_slots": boilerplate,
    }


def score_manifest_file(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    change_id = str((document.get("change") or {}).get("id") or path.parent.name)
    return score_document(change_id, document)


def run(root: Path, change_ids: list[str]) -> dict[str, Any]:
    rows = []
    for change_id in sorted(change_ids):
        path = root / "pact" / "changes" / change_id / "pact.yaml"
        if not path.is_file():
            continue
        rows.append(score_manifest_file(path))
    mean = round(sum(row["score"] for row in rows) / len(rows), 4) if rows else 0.0
    return {
        "manifests": rows,
        "scored": len(rows),
        "mean_score": mean,
        "unscored_requirements": sum(
            len(row["unscored_requirements"]) for row in rows
        ),
    }
