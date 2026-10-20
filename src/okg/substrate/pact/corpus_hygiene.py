"""Corpus-wide PACT hygiene: the gate's own checks, over every manifest.

`gate post-edit` refuses one change at a time, which is exactly one manifest
too few: the vacuity classes it now catches (pact-vacuous-gate-close
req.vacuous.adoption, the hygiene lint of #978) enter the tree whenever a
manifest is authored without the gate being run on it. This module runs the
gate's OWN check functions across `pact/changes/` in-process — no subprocess
per manifest, no ledger, no graph — so a blocking test lane can assert the
whole corpus holds.

Archived manifests are out of scope by LOCATION, not by lifecycle state and
not by weakening a check: `pact/archive/` is where this repo PUTS closed
history, and it is never edited to satisfy a new rule. The distinction matters
because a lifecycle exemption would be self-defeating — a manifest could exempt
itself by declaring itself finished. Nothing declares itself out of this sweep:
`change.status` has no terminal value (`ACTIVE_STATUSES` is the whole domain
the schema admits), and the lane asserts that, so "archived" can only ever mean
"moved to pact/archive/".

A manifest may carry an explicit waiver for a hygiene check instead of a fix.
The waiver must name the violated check as its `scope`, carry an owner, and be
dated (`expires`, or an ISO date in `reason`) — an undated, ownerless waiver is
how a cap becomes a rubber stamp. `HYGIENE_WAIVER_CAP` is the corpus-wide
ceiling the design puts on them, and every waiver reports how many violations
it SUPPRESSES: capping the waiver count alone would leave one per-code waiver
free to absorb an unbounded number of violations.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from okg.substrate.pact.models import PactManifest

# The design's cap on waived hygiene violations across the whole corpus
# (pact-reliability-program design, metric 2b).
HYGIENE_WAIVER_CAP = 10

# Every `change.status` the v4 schema admits (models.py Change.status). None of
# them is terminal, which is what makes the location-based archive exemption
# sound: a change leaves this sweep by being MOVED to `pact/archive/`, never by
# declaring itself done. The lane asserts the corpus stays inside this set, so a
# future "archived"/"complete" status cannot quietly become a self-exemption.
ACTIVE_STATUSES = frozenset({
    "proposal",
    "investigation",
    "implementation",
    "verification",
})

# What the blocking lane refuses. Deliberately NOT "every validation error":
# the corpus carries older debt (requirements with no evidence policy, stale
# formal evidence, done tasks whose real proof obligation is unmet) that a
# corpus-wide gate cannot clear without attaching evidence or rewriting
# statuses, and a lane that is red on arrival is a lane nobody reads. These two
# are the classes pact-vacuous-gate-close closed AND swept to zero, so the lane
# holds a line that is actually held:
#
#   task_requirements_link  a task linked to no requirement — closing it
#                           checks nothing (the subject-deletion class)
#   unknown_evidence_kind   a kind outside the registry — resolves to no
#                           policy slot, so the row can never be proof
#
# Both are structural: they need no alignment, no filesystem, no ledger, which
# is what lets this run over the whole tree in a fast lane.
#
# The lane EVALUATES all five vacuity arms (`structural_issues` runs every one
# of them over every manifest); it BLOCKS on these two. Measured over the 477
# active manifests, three of the other three arms are at zero and one is not:
#
#   missing_evidence_policy (no requirements at all)   0
#   unknown_evidence_ref (empty refs)                  0
#   task_done_missing_evidence (no obligation)        14, in 6 manifests
#
# The 14 are inherited debt with a shared cause — their linked requirements
# declare no evidence policy — so clearing them means writing a proof contract
# for six PACTs, which is an authoring decision, not a mechanical sweep. They
# are counted by name in `reported_only` rather than blocked on, and the two
# zero arms are deliberately NOT promoted while their shared error CODES still
# cover unswept arms (`missing_evidence_policy` also fires per requirement,
# `unknown_evidence_ref` also fires on dangling refs): promoting by code would
# block on debt nobody swept. Promotion belongs to the follow-on that sweeps
# those, and adding a class here without sweeping it first is how a gate
# becomes decorative.
BLOCKING_CHECKS = frozenset({"task_requirements_link", "unknown_evidence_kind"})

_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

# Checks introduced or promoted to `error` by pact-vacuous-gate-close. Keyed by
# code, with a predicate on the issue path (or message) where one code covers
# both an older arm and a new one — so a report can separate the debt this
# change creates from the debt it inherits.
_VACUITY_ARMS: dict[str, Any] = {
    "task_requirements_link": lambda issue: True,
    "unknown_evidence_kind": lambda issue: True,
    "missing_evidence_policy": lambda issue: issue["path"] == "requirements",
    "task_done_missing_evidence": lambda issue: issue["path"].endswith(
        ".requirements"
    ),
    "unknown_evidence_ref": lambda issue: "(empty refs)" in issue["message"],
}


def is_vacuity_check(issue: dict[str, str]) -> bool:
    """Did pact-vacuous-gate-close introduce this particular refusal?"""
    arm = _VACUITY_ARMS.get(issue["code"])
    return bool(arm and arm(issue))


def hygiene_waivers(rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    """The hygiene checks a manifest's raw waiver rows waive, by check code.

    Ownerless or undated waivers are NOT returned: they would suppress a
    refusal with nobody accountable for it. Reads raw mappings so the count in
    `metrics.json` and the lane's suppression list are computed by ONE rule,
    from the document and from the parsed model alike.
    """
    waived: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        scope = str(row.get("scope") or "").removeprefix("hygiene.")
        if scope not in _VACUITY_ARMS:
            continue
        reason = str(row.get("reason") or "")
        expires = str(row.get("expires") or "")
        if not row.get("owner") or not (expires or _ISO_DATE.search(reason)):
            continue
        waived[scope] = {
            "waiver": str(row.get("id") or ""),
            "owner": str(row["owner"]),
            "expires": expires,
            "reason": reason,
        }
    return waived


def waived_checks(manifest: PactManifest) -> dict[str, dict[str, str]]:
    """The hygiene checks this manifest waives, by check code."""
    return hygiene_waivers([
        {
            "id": waiver.id,
            "scope": waiver.scope,
            "reason": waiver.reason,
            "owner": waiver.owner,
            "expires": waiver.expires,
        }
        for waiver in manifest.waivers
    ])


def manifest_paths(repo_root: Path) -> list[Path]:
    return sorted((repo_root / "pact" / "changes").glob("*/pact.yaml"))


def _load(path: Path) -> tuple[PactManifest | None, dict | None, str | None]:
    from okg.substrate.pact.store import load_manifest_file, load_yaml_mapping

    try:
        return load_manifest_file(path), load_yaml_mapping(path), None
    except Exception as exc:  # noqa: BLE001 - an unparsable manifest is a refusal
        return None, None, str(exc)[:200]


def corpus_report(repo_root: Path) -> dict[str, Any]:
    """The blocking lane's report: `BLOCKING_CHECKS` over every active manifest.

    Structural only, so the whole corpus is one fast pass. `full_report` runs
    the complete gate validation when the inherited-debt counts are wanted.
    """
    from okg.substrate.pact.validation import structural_issues

    return _report(
        repo_root,
        lambda manifest, _raw: structural_issues(manifest, repo_root=repo_root),
    )


def full_report(repo_root: Path, paths: list[Path] | None = None) -> dict[str, Any]:
    """Every gate error, alignment included, over `paths` (default: all).

    Materially slower per manifest than `corpus_report` — alignment recomputes
    the intent digest, which is two YAML dumps — so the lane passes the subset
    it cares about rather than the whole tree.
    """
    from okg.substrate.pact.operations import _gate_validation_report

    def check(manifest: PactManifest, raw: dict | None) -> list[Any]:
        return _gate_validation_report(manifest, repo_root, raw).issues

    return _report(repo_root, check, paths=paths)


def _report(
    repo_root: Path,
    check: Any,
    *,
    paths: list[Path] | None = None,
) -> dict[str, Any]:
    refusals: list[dict[str, Any]] = []
    unreadable: list[dict[str, str]] = []
    waivers: list[dict[str, Any]] = []
    statuses: dict[str, int] = {}
    manifests = 0
    for path in manifest_paths(repo_root) if paths is None else paths:
        manifests += 1
        change_id = path.parent.name
        manifest, raw_document, load_error = _load(path)
        if manifest is None:
            unreadable.append({"change": change_id, "error": load_error or "unknown"})
            continue
        status = str(manifest.change.status)
        statuses[status] = statuses.get(status, 0) + 1
        waived = waived_checks(manifest)
        errors = [
            issue.to_dict()
            for issue in check(manifest, raw_document)
            if issue.severity == "error"
        ]
        for code, detail in waived.items():
            waivers.append({
                "change": change_id,
                "check": code,
                # How many refusals this ONE waiver swallows. A cap on the
                # waiver COUNT alone leaves a single per-code waiver free to
                # absorb an unbounded number of violations, which is
                # mass-waiver wearing one row's clothes.
                "suppressed_violations": sum(
                    1 for issue in errors if issue["code"] == code
                ),
                **detail,
            })
        if not errors:
            continue
        unwaived = [issue for issue in errors if issue["code"] not in waived]
        refusals.append({
            "change": change_id,
            "errors": [
                {**issue, "vacuity_check": is_vacuity_check(issue)}
                for issue in errors
            ],
            "unwaived_codes": sorted({issue["code"] for issue in unwaived}),
            "waived_codes": sorted(waived),
            "blocking": sorted(
                {
                    issue["code"] for issue in unwaived
                    if issue["code"] in BLOCKING_CHECKS
                }
            ),
            "vacuity_check": all(is_vacuity_check(issue) for issue in errors),
        })
    reported: dict[str, int] = {}
    vacuity: dict[str, int] = {}
    for row in refusals:
        for issue in row["errors"]:
            if issue["vacuity_check"]:
                vacuity[issue["code"]] = vacuity.get(issue["code"], 0) + 1
            if issue["code"] not in BLOCKING_CHECKS:
                reported[issue["code"]] = reported.get(issue["code"], 0) + 1
    return {
        "manifests": manifests,
        "refusals": refusals,
        "unreadable": unreadable,
        "waivers": waivers,
        "waiver_cap": HYGIENE_WAIVER_CAP,
        # The number the cap cannot see on its own: ten waivers suppressing a
        # thousand violations is not ten violations waived.
        "suppressed_violations": sum(
            int(row["suppressed_violations"]) for row in waivers
        ),
        "statuses": dict(sorted(statuses.items())),
        # Inherited debt: counted and named, enforcement deferred. Never
        # summarized as a single "ok", so it cannot fade out of view. The
        # structural pass populates this for the three non-blocking VACUITY
        # arms; `full_report` adds every other class the full gate evaluates.
        "reported_only": dict(sorted(reported.items())),
        # The classes pact-vacuous-gate-close owns, whatever their severity
        # source: the number the sweep drives to zero.
        "vacuity_errors": dict(sorted(vacuity.items())),
    }


def blocking_refusals(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Refusals the hygiene lane fails on: a blocking check, unwaived."""
    return [row for row in report["refusals"] if row["blocking"]]


def unwaived_refusals(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [row for row in report["refusals"] if row["unwaived_codes"]]
