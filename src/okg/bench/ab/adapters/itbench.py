"""ITBench adapter — SCOPED DOWN to recorded-telemetry incident diagnosis.

Scope ruling (PACT okg-bench-domain-adapters, req.dom.itbench,
designs/first-gates.md): live ITBench scenarios need a Kubernetes cluster
plus an OTel/Prometheus/Jaeger/Clickhouse stack per run, and the repo's
snapshot-and-replay mode is announced but NOT shipped. This adapter
therefore runs INCIDENT DIAGNOSIS OVER RECORDED ARTIFACTS: no live
cluster, no scenario execution.

Data is never fetched by the framework — two operator-supplied local
copies:

1. Scenario index JSONs: a local ``git clone`` of
   github.com/itbench-hub/ITBench (Apache-2.0), scenario files like
   ``scenarios/sre/library/indexes/scenarios/1.json`` with fields
   ``{alerts, category, complexity, description, disruptions[].injections,
   environment, platforms, solutions[{command, text}]}``.
2. Recorded telemetry: the HF ``ibm-research/ITBench-Trajectories``
   dataset (**CC-BY-NC-4.0** — the NonCommercial restriction is recorded;
   operator-supplied local copy, never fetched, never vendored), layout
   model -> domain -> scenario -> run per the dataset card.

An instance exists ONLY for scenarios that also have recorded telemetry;
scenarios without recordings (the "19 held out" reduction) are counted
into a ``scope_report`` stamped on every kept instance.

Prompt-leakage discipline: ``disruptions`` (the injection specs) and
``solutions`` (the gold remediation) are ALWAYS excluded from the prompt;
``description`` is included only when it shares no identifier token with
the injection spec (``_leaks_injection``) — when in doubt, excluded.

Scoring is deterministic, no LLM judge:
- fault localization (the Score.value): the answer's ``FAULT:`` line
  names an identifier token from the injection spec (case-insensitive
  substring match on any token);
- remediation overlap (reported in detail, separately): the fraction of
  gold solution steps whose command key tokens (kubectl verb + resource
  identifiers) all appear in the answer's ``REMEDIATION:`` block.
"""

from __future__ import annotations

import dataclasses
import json
import re
from pathlib import Path, PurePath
from typing import Any

from okg.substrate.library.sources.ops_telemetry import (
    SNAPSHOT_DENY_LIST,
    TRAJECTORY_DENY_LIST,
)

from ..errors import DatasetError, ScoringError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

SCENARIOS_REPO = "github.com/itbench-hub/ITBench"
TRAJECTORIES_DATASET = "ibm-research/ITBench-Trajectories"
SNAPSHOT_TOKEN = "{SNAPSHOT_DIR}"

ACQUISITION_INSTRUCTION = (
    "itbench data is never fetched by the framework. Two operator steps:\n"
    f"1. Scenario JSONs: `git clone https://{SCENARIOS_REPO}` (Apache-2.0) "
    "and point scenarios_root at the scenario index directory "
    "(scenarios/sre/library/indexes/scenarios).\n"
    f"2. Recorded telemetry: download HF `{TRAJECTORIES_DATASET}` "
    "(CC-BY-NC-4.0 — NonCommercial restriction recorded; "
    "operator-supplied local copy only, never vendored) and point "
    "trajectories_root at it.\n"
    "Then pass --offline-fixture a JSON file "
    '{"scenarios_root": "...", "trajectories_root": "..."}.'
)

OPEN_BOOK_PREAMBLE = (
    "You are diagnosing a recorded IT incident. A frozen snapshot of the "
    "incident corpus — the recorded observability telemetry for this "
    "scenario plus the scenario's alert definitions — is available "
    f"read-only under {SNAPSHOT_TOKEN}. Consult it however you like. The "
    "snapshot carries no ground-truth answers.\n\n"
)

ANSWER_INSTRUCTIONS = (
    "Investigate the recorded telemetry and identify the root cause.\n"
    "Finish your answer with exactly these two blocks:\n"
    "FAULT: <the component/config that was broken>\n"
    "REMEDIATION:\n"
    "1. <first step>\n"
    "2. <next step, and so on — ordered steps>"
)

#: files whose presence marks a directory as one recorded run.
#:
#: This is the SAME constant the ops_telemetry source enforces, imported
#: rather than restated: it is a hard deny-list, and the corpus-side and
#: prompt-side boundaries must not be able to drift apart. `agent_output.json`
#: and `session.jsonl` are a prior agent's answer — which for fault
#: localization IS the answer unit, so no substring assertion could catch
#: the leak — and `judge_output.json` is a grading artifact scored against
#: the gold. Their one permitted role is this filename test.
#: PACT okg-corpus-ops / req.ops.telemetry.
RECORDING_MARKERS = TRAJECTORY_DENY_LIST

#: scenario JSON fields that must all be present (per first-gates.md)
_REQUIRED_FIELDS = (
    "alerts",
    "category",
    "complexity",
    "description",
    "disruptions",
    "environment",
    "platforms",
    "solutions",
)

#: kubectl verbs — key tokens of a gold remediation command
_KUBECTL_VERBS = frozenset({
    "annotate", "apply", "create", "delete", "describe", "edit", "exec",
    "get", "label", "logs", "patch", "replace", "restart", "rollout",
    "scale", "set",
})

#: generic words never treated as fault-identifying tokens. The kubectl
#: verbs are folded in so `_identifier_tokens` over an injection spec
#: never mistakes "delete" for a component name.
_STOPWORDS = frozenset({
    "and", "app", "application", "are", "chaos", "cluster", "config",
    "configmap", "configmaps", "container", "containers", "default",
    "deployment", "deployments", "disable", "disabled", "disruption",
    "disruptions", "enabled", "env", "environment", "error", "errors",
    "failure", "failures", "false", "fault", "faults", "for", "from",
    "image", "images", "inject", "injection", "injections", "into",
    "key", "keys", "kind", "kubectl", "kubernetes", "misconfiguration",
    "misconfigure", "misconfigured", "name", "names", "namespace",
    "namespaces", "node", "nodes", "none", "null", "pod", "pods",
    "service", "services", "spec", "target", "targets", "that", "the",
    "then", "this", "true", "type", "value", "values", "when", "with",
}) | _KUBECTL_VERBS


class ItBenchAdapter(BenchmarkAdapter):
    name = "itbench"
    pilot_size = 10  # first 10 recorded scenarios by number
    requires_corpus_config = True

    #: populated by load_instances: the scope reduction made visible
    scope_report: dict[str, Any] | None = None

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        raise DatasetError(ACQUISITION_INSTRUCTION)

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        return {
            "dataset": (
                f"{SCENARIOS_REPO} scenario JSONs + {TRAJECTORIES_DATASET} "
                "(operator-supplied local copies)"
            ),
            "revision": None,
            "license": (
                "scenarios Apache-2.0; trajectories CC-BY-NC-4.0 "
                "(NonCommercial — never fetched by the framework, "
                "never vendored)"
            ),
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is None:
            raise DatasetError(
                "itbench requires an operator-supplied roots file "
                "(no downloadable dataset).\n" + ACQUISITION_INSTRUCTION
            )
        roots = load_json(offline_fixture)
        if not isinstance(roots, dict):
            raise DatasetError(
                f"itbench roots file {offline_fixture} must be a JSON "
                f"object, got {type(roots).__name__}"
            )
        missing = [k for k in ("scenarios_root", "trajectories_root")
                   if not roots.get(k)]
        if missing:
            raise DatasetError(
                f"itbench roots file {offline_fixture} missing {missing}; "
                'expected {"scenarios_root": "...", "trajectories_root": "..."}'
            )
        scenarios_root = Path(roots["scenarios_root"])
        trajectories_root = Path(roots["trajectories_root"])
        for label, root in (("scenarios_root", scenarios_root),
                            ("trajectories_root", trajectories_root)):
            if not root.is_dir():
                raise DatasetError(
                    f"itbench {label} {root} is not a directory.\n"
                    + ACQUISITION_INSTRUCTION
                )

        scenarios = load_scenarios(scenarios_root)
        recordings = find_recordings(trajectories_root)

        recorded_numbers = sorted(n for n in scenarios if n in recordings)
        unrecorded_numbers = sorted(n for n in scenarios if n not in recordings)
        scope_report = {
            "scope": "recorded-telemetry incident diagnosis "
                     "(no live cluster, no scenario execution)",
            "scenarios_total": len(scenarios),
            "recorded": len(recorded_numbers),
            "unrecorded": len(unrecorded_numbers),
            "unrecorded_scenario_numbers": unrecorded_numbers,
        }
        self.scope_report = scope_report
        if not recorded_numbers:
            raise DatasetError(
                "itbench scope reduction left zero instances: none of the "
                f"{len(scenarios)} scenario(s) under {scenarios_root} has "
                f"recorded telemetry under {trajectories_root}. The "
                "recorded-telemetry scope excludes scenarios without "
                "trajectories (the same reduction that holds out the 19 "
                "unrecorded public scenarios); check the trajectories_root "
                "layout (model/domain/scenario/run)."
            )
        return [
            _instance(number, scenarios[number], recordings[number],
                      scope_report)
            for number in recorded_numbers
        ]

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: i.extra["scenario_number"])

    # -- open-book binding ---------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        """Substitute the snapshot dir into every prompt and mount it via
        add_dirs. ITBench diagnosis is open-book for EVERY arm (the
        recorded telemetry + alert definitions), so the corpus freeze
        manifest is required regardless of arm."""
        snapshot_dir = corpus_cfg.get("snapshot_dir")
        if not snapshot_dir or not Path(snapshot_dir).is_dir():
            raise DatasetError(
                f"corpus config has no usable snapshot_dir (got "
                f"{snapshot_dir!r}); build one from the scenario JSONs + "
                "recorded trajectories (solutions/injections excluded)"
            )
        _assert_no_denied_files(Path(snapshot_dir))
        bound = []
        for inst in instances:
            bound.append(
                dataclasses.replace(
                    inst,
                    prompt=inst.prompt.replace(SNAPSHOT_TOKEN, str(snapshot_dir)),
                    extra={**inst.extra, "add_dirs": [str(snapshot_dir)]},
                )
            )
        return bound

    # -- scoring --------------------------------------------------------
    def score_task(
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        injections = instance.gold["injections"]
        solutions = instance.gold["solutions"]
        gold_tokens = _identifier_tokens(injections)
        if not gold_tokens:
            raise ScoringError(
                f"{instance.instance_id}: injection spec yields no "
                "identifier tokens — fault localization is unscoreable"
            )
        answer = result.answer or ""
        fault_line = _extract_fault_line(answer)
        matched = (
            sorted(t for t in gold_tokens if t in fault_line.lower())
            if fault_line is not None else []
        )
        localized = bool(matched)
        overlap, per_step = _remediation_overlap(
            solutions, _extract_remediation_block(answer)
        )
        detail: dict[str, Any] = {
            "fault_line": fault_line,
            "matched_tokens": matched,
            "gold_tokens": sorted(gold_tokens),
            "remediation_overlap": overlap,
            "remediation_steps": per_step,
        }
        if fault_line is None:
            detail["reason"] = "no FAULT: line in answer"
        return Score(
            instance_id=instance.instance_id,
            value=1.0 if localized else 0.0,
            kind="fault_localization",
            detail=detail,
        )


# -- loading ------------------------------------------------------------


def _assert_no_denied_files(snapshot_dir: Path) -> None:
    """The prompt-side half of the deny-list.

    The snapshot is mounted read-only into the agent's workspace for EVERY
    arm, so a deny-listed file inside it hands the agent the answer directly.
    The corpus-side half lives in `ops_telemetry`; both read
    `SNAPSHOT_DENY_LIST`, so they cannot drift apart.

    TWO families, and the second is the one the published data actually
    ships. `TRAJECTORY_DENY_LIST` is a prior agent's answer and the judge's
    grading of it. `GROUND_TRUTH_DENY_LIST` is `ground_truth.yaml`, which
    `ibm-research/ITBench-Lite` bundles INSIDE every scenario's telemetry
    directory — 50 files on the release, 35 under `snapshots/sre/` and 15
    under `snapshots/finops/`. Matching is by FILENAME anywhere beneath the
    snapshot, never by domain path prefix, because a prefix scoped to the SRE
    tree would leave the finops 15 mounted.

    PACT okg-corpus-ops / req.ops.telemetry."""
    found = sorted(
        str(path.relative_to(snapshot_dir))
        for path in snapshot_dir.rglob("*")
        if path.is_file() and path.name in SNAPSHOT_DENY_LIST
    )
    if found:
        raise DatasetError(
            f"itbench snapshot {snapshot_dir} contains "
            f"{len(found)} deny-listed file(s): "
            f"{found[:10]}{' ...' if len(found) > 10 else ''}. "
            f"{sorted(TRAJECTORY_DENY_LIST)} are a prior agent's answer and "
            "the judge's grading of it; ground_truth.yaml is the complete "
            "answer key (fault entity, labelled alert set, propagation "
            "graph) and ships inside the published telemetry. None are ever "
            "mounted. Rebuild the snapshot with them excluded."
        )


def load_scenarios(scenarios_root: Path) -> dict[int, dict[str, Any]]:
    """Parse + validate every scenario index JSON. Loud on any defect."""
    paths = sorted(scenarios_root.glob("*.json"))
    if not paths:
        raise DatasetError(
            f"itbench scenarios_root {scenarios_root} contains no scenario "
            "index JSONs (expected files like 1.json)"
        )
    scenarios: dict[int, dict[str, Any]] = {}
    for path in paths:
        if not re.fullmatch(r"\d+", path.stem):
            raise DatasetError(
                f"itbench scenario file {path} is not named "
                "<scenario_number>.json — point scenarios_root at the "
                "scenario index directory"
            )
        number = int(path.stem)
        scenarios[number] = _validate_scenario(path, load_json(path))
    return scenarios


def flatten_solution_steps(solutions: Any) -> list[dict[str, Any]]:
    """Flatten upstream's nested solutions into an ordered step list.

    Verified shape (2026-07-27, itbench-hub/ITBench scenario 1.json):
    ``solutions: [[{"steps": [{"command", "text"}, ...]}, ...], ...]`` —
    a list of alternative solution groups, each a list of stages holding
    ``steps``. A flat ``[{command, text}]`` list (the older documented
    shape) passes through unchanged, so both survive."""
    steps: list[dict[str, Any]] = []

    def _walk(node: Any) -> None:
        if isinstance(node, list):
            for child in node:
                _walk(child)
        elif isinstance(node, dict):
            if isinstance(node.get("steps"), list):
                _walk(node["steps"])
            elif "command" in node or "text" in node:
                steps.append(node)

    _walk(solutions)
    return steps


def _validate_scenario(path: Path, doc: Any) -> dict[str, Any]:
    if not isinstance(doc, dict):
        raise DatasetError(
            f"itbench scenario {path} must be a JSON object, "
            f"got {type(doc).__name__}"
        )
    missing = [k for k in _REQUIRED_FIELDS if k not in doc]
    if missing:
        raise DatasetError(
            f"itbench scenario {path} missing fields {missing}; "
            f"expected {list(_REQUIRED_FIELDS)}"
        )
    # Verified against real upstream scenarios (2026-07-27, scenario 1.json):
    # `alerts` is present but EMPTY in the index — alerts are runtime
    # artifacts that live in the recorded telemetry, not in the scenario
    # definition. The field must exist (shape check) but may be empty; a
    # scenario with neither index alerts nor recorded telemetry is rejected
    # at instance construction, where both are known.
    if not isinstance(doc["alerts"], list):
        raise DatasetError(f"itbench scenario {path} alerts must be a list")
    if not isinstance(doc["disruptions"], list) or not doc["disruptions"]:
        raise DatasetError(f"itbench scenario {path} has no disruptions")
    for i, disruption in enumerate(doc["disruptions"]):
        if (not isinstance(disruption, dict)
                or not isinstance(disruption.get("injections"), list)
                or not disruption["injections"]):
            raise DatasetError(
                f"itbench scenario {path} disruption {i} has no injections "
                "list"
            )
    if not isinstance(doc["solutions"], list) or not doc["solutions"]:
        raise DatasetError(f"itbench scenario {path} has no solutions")
    steps = flatten_solution_steps(doc["solutions"])
    if not steps:
        raise DatasetError(
            f"itbench scenario {path} solutions contain no remediation steps"
        )
    for i, step in enumerate(steps):
        if not (
            str(step.get("command") or "").strip()
            or str(step.get("text") or "").strip()
        ):
            raise DatasetError(
                f"itbench scenario {path} solution step {i} must have a "
                "command and/or text"
            )
    # Normalize the nested upstream shape once, so gold + scoring see a
    # flat ordered step list.
    doc["solutions"] = steps
    if not isinstance(doc["description"], str):
        raise DatasetError(
            f"itbench scenario {path} description must be a string"
        )
    return doc


def find_recordings(trajectories_root: Path) -> dict[int, list[str]]:
    """Map scenario number -> recorded run dirs (relative POSIX paths).

    A directory is a recording when it holds a marker file
    (agent_output.json / session.jsonl) or any observability file. The
    scenario number comes from the path per the dataset-card layout
    model -> domain -> scenario -> run: the run dir's parent is checked
    first, then the dir itself, then further ancestors — the first part
    carrying digits wins. A recording whose path carries no scenario
    number anywhere raises (never silently dropped)."""
    recordings: dict[int, list[str]] = {}
    for run_dir in sorted(p for p in trajectories_root.rglob("*") if p.is_dir()):
        if not _is_recording_dir(run_dir):
            continue
        rel = run_dir.relative_to(trajectories_root)
        number = _scenario_number_for(rel)
        if number is None:
            raise DatasetError(
                f"itbench recording {run_dir} has no scenario number in "
                f"its path (relative {rel}); expected the "
                "model/domain/scenario/run layout"
            )
        recordings.setdefault(number, []).append(rel.as_posix())
    return recordings


def _is_recording_dir(path: Path) -> bool:
    for child in path.iterdir():
        if not child.is_file():
            continue
        if child.name in RECORDING_MARKERS:
            return True
        if "observability" in child.name.lower():
            return True
    return False


def _scenario_number_for(rel: PurePath) -> int | None:
    parts = rel.parts
    candidates: list[str] = []
    if len(parts) >= 2:
        candidates.append(parts[-2])  # canonical scenario position
    candidates.append(parts[-1])  # markers directly under the scenario dir
    candidates.extend(reversed(parts[:-2]))
    for part in candidates:
        match = re.search(r"(\d+)", part)
        if match:
            return int(match.group(1))
    return None


# -- prompt -------------------------------------------------------------


def _instance(
    number: int,
    scenario: dict[str, Any],
    recordings: list[str],
    scope_report: dict[str, Any],
) -> TaskInstance:
    description = scenario["description"]
    include_description = not _leaks_injection(
        description, scenario["disruptions"]
    )
    # Upstream scenario indexes carry alerts: [] (alerts are runtime
    # artifacts inside the recorded telemetry). An instance with neither
    # index alerts nor recordings has no diagnosable signal at all —
    # refuse rather than prompt an agent with nothing to go on.
    if not scenario["alerts"] and not recordings:
        raise DatasetError(
            f"itbench scenario {number} has neither index alerts nor "
            "recorded telemetry — no diagnosable signal"
        )
    alerts_block = (
        "Alerts fired:\n"
        + json.dumps(scenario["alerts"], indent=2, sort_keys=True)
        if scenario["alerts"]
        else (
            "Alerts: not in the scenario index — read them from the "
            "recorded telemetry under the mounted snapshot "
            f"({len(recordings)} recorded run(s))."
        )
    )
    body_parts = [
        f"Scenario itbench/sre/{number} "
        f"(category: {scenario['category']}, "
        f"complexity: {scenario['complexity']}).",
        alerts_block,
        "Environment topology:\n"
        + json.dumps(scenario["environment"], indent=2, sort_keys=True),
    ]
    if include_description:
        body_parts.append("Symptom description:\n" + description)
    body_parts.append(ANSWER_INSTRUCTIONS)
    injections = [
        injection
        for disruption in scenario["disruptions"]
        for injection in disruption["injections"]
    ]
    return TaskInstance(
        instance_id=f"itbench/sre/{number}",
        benchmark="itbench",
        prompt=OPEN_BOOK_PREAMBLE + "\n\n".join(body_parts),
        gold={"injections": injections, "solutions": scenario["solutions"]},
        extra={
            "scenario_number": number,
            "category": scenario["category"],
            "complexity": scenario["complexity"],
            "platforms": scenario["platforms"],
            "recordings": recordings,
            "description_included": include_description,
            "scope_report": scope_report,
        },
    )


def _leaks_injection(description: str, disruptions: Any) -> bool:
    """True when the description names anything from the injection spec.

    Conservative by design: any identifier token shared between the
    description and the disruption spec counts as a leak — when in
    doubt, the description stays out of the prompt."""
    low = description.lower()
    return any(token in low for token in _identifier_tokens(disruptions))


def _identifier_tokens(obj: Any) -> set[str]:
    """Identifier-ish tokens (ConfigMap names, deployment names,
    feature-flag keys) from any JSON fragment: every string key and
    value, tokenized to [letter][word/-/./_]{2,}, lowercased, minus
    generic stopwords. Hyphen/dot/underscore compounds stay whole, so
    e.g. otel-collector survives as one token."""
    tokens: set[str] = set()
    for text in _strings(obj):
        for token in re.findall(r"[A-Za-z][A-Za-z0-9_.\-]{2,}", text):
            low = token.lower().strip(".-_")
            if len(low) >= 3 and low not in _STOPWORDS:
                tokens.add(low)
    return tokens


def _strings(obj: Any) -> list[str]:
    out: list[str] = []
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for key, value in obj.items():
            out.append(str(key))
            out.extend(_strings(value))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            out.extend(_strings(item))
    return out


# -- scoring helpers ----------------------------------------------------


def _extract_fault_line(answer: str) -> str | None:
    matches = re.findall(r"^\s*FAULT:\s*(.+)$", answer or "",
                         re.IGNORECASE | re.MULTILINE)
    return matches[-1].strip() if matches else None


def _extract_remediation_block(answer: str) -> str:
    matches = list(re.finditer(r"^\s*REMEDIATION:\s*$|^\s*REMEDIATION:\s*(.+)$",
                               answer or "", re.IGNORECASE | re.MULTILINE))
    if not matches:
        return ""
    last = matches[-1]
    inline = last.group(1) or ""
    return (inline + "\n" + (answer or "")[last.end():]).strip()


def _remediation_overlap(
    solutions: list[dict[str, Any]], remediation: str
) -> tuple[float, list[dict[str, Any]]]:
    """Fraction of gold solution steps whose key tokens all appear in
    the answer's REMEDIATION block."""
    low = remediation.lower()
    per_step: list[dict[str, Any]] = []
    covered = 0
    for step in solutions:
        keys = _solution_key_tokens(step)
        hit = bool(low) and all(key in low for key in keys)
        covered += hit
        per_step.append({
            "command": step.get("command"),
            "key_tokens": sorted(keys),
            "covered": hit,
        })
    return covered / len(solutions), per_step


def _solution_key_tokens(step: dict[str, Any]) -> set[str]:
    """Key tokens of one gold step: the kubectl verbs in the command
    plus the command's identifier tokens; a command-less step falls back
    to its text's identifier tokens. A step with no key tokens at all is
    unscoreable and raises — never silently counted."""
    command = str(step.get("command") or "")
    words = re.findall(r"[a-z0-9][\w.\-]*", command.lower())
    verbs = {w for w in words if w in _KUBECTL_VERBS}
    keys = verbs | _identifier_tokens(command)
    if not keys:
        keys = _identifier_tokens(str(step.get("text") or ""))
    if not keys:
        raise ScoringError(
            f"gold solution step has no key tokens: {step!r}"
        )
    return keys
