"""Deployment objectives, measurement records, and the scorer between them.

A deployment states what its knowledge base must achieve in
``objectives.yaml``: three fixed hard constraints, scored objectives, cost
terms, and operator-specific floors and ceilings. A replay run writes one
measurement record per generation it scored. ``score_objectives`` reads both
and reports each entry; ``replay_decision`` and ``acceptance_thresholds``
turn that into the inputs Althea's acceptance gate expects.

A measure the record does not carry is reported as unmeasured, never read as
zero: a default value would make a failed run look like a result. Pure: no
substrate, database, or CLI imports.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

from .acceptance import RewriteReplayDecision


OBJECTIVES_VERSION = 1
MEASUREMENT_SCHEMA = "okg.akb.measurement.v1"
HARD_CONSTRAINTS = ("provenance", "versioning", "access_control")


class Unit(str, Enum):
    RATE = "rate"
    SECONDS = "seconds"
    COUNT = "count"
    USD = "usd"
    BYTES = "bytes"


class Direction(str, Enum):
    MAXIMIZE = "maximize"
    MINIMIZE = "minimize"


@dataclass(frozen=True)
class Measure:
    name: str
    unit: Unit
    direction: Direction
    # Computed from answers to a question set, so it needs a named set and
    # enough tries per question to count.
    answer_based: bool


def _measures(*rows: tuple[str, Unit, Direction, bool]) -> Mapping[str, Measure]:
    return MappingProxyType({
        name: Measure(name, unit, direction, answer_based)
        for name, unit, direction, answer_based in rows
    })


# The one list of measures. Later stages add rows here; any name not listed
# is refused by the objectives lint and the measurement-record lint.
MEASURES: Mapping[str, Measure] = _measures(
    ("required_fact_recall", Unit.RATE, Direction.MAXIMIZE, True),
    ("answer_correct_rate", Unit.RATE, Direction.MAXIMIZE, True),
    ("out_of_scope_reported_rate", Unit.RATE, Direction.MAXIMIZE, True),
    ("contradicted_claim_rate", Unit.RATE, Direction.MINIMIZE, True),
    ("source_lag_seconds", Unit.SECONDS, Direction.MINIMIZE, False),
    ("open_conflicts", Unit.COUNT, Direction.MINIMIZE, False),
    ("build_usd", Unit.USD, Direction.MINIMIZE, False),
    ("storage_bytes", Unit.BYTES, Direction.MINIMIZE, False),
    ("publish_seconds", Unit.SECONDS, Direction.MINIMIZE, False),
    ("usd_per_question", Unit.USD, Direction.MINIMIZE, True),
    ("model_calls_per_try", Unit.COUNT, Direction.MINIMIZE, True),
    ("tokens_per_try", Unit.COUNT, Direction.MINIMIZE, True),
    ("tool_errors_per_try", Unit.COUNT, Direction.MINIMIZE, True),
    ("latency_seconds_per_try", Unit.SECONDS, Direction.MINIMIZE, True),
)


@dataclass(frozen=True)
class ObjectivesProblem:
    """One refusal, with a stable code and a JSON pointer into the input."""

    code: str
    message: str
    pointer: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "pointer": self.pointer}


class ObjectivesError(ValueError):
    def __init__(self, problems: list[ObjectivesProblem]) -> None:
        self.problems = tuple(problems)
        first = problems[0]
        more = f" (+{len(problems) - 1} more)" if len(problems) > 1 else ""
        super().__init__(f"{first.code}: {first.message}{more}")


@dataclass(frozen=True)
class Objective:
    id: str
    measure: str
    question_set: str | None = None


@dataclass(frozen=True)
class SpecificObjective:
    id: str
    measure: str
    floor: float | None = None
    ceiling: float | None = None
    scope_tag: str | None = None
    question_set: str | None = None


@dataclass(frozen=True)
class Objectives:
    version: int
    hard_constraints: tuple[str, ...]
    min_tries: int
    objectives: tuple[Objective, ...]
    costs: tuple[Objective, ...]
    specific: tuple[SpecificObjective, ...]
    policy_id: str


@dataclass(frozen=True)
class MeasurementRecord:
    deployment: str
    generation_id: str
    run_id: str
    setup_digest: str
    question_set_id: str | None
    question_set_digest: str | None
    tries: int | None
    measures: Mapping[str, float]
    per_scope_tag: Mapping[str, Mapping[str, float]]
    constraints: Mapping[str, str]


_TOP_KEYS = frozenset({"version", "hard_constraints", "noise", "objectives", "costs", "specific"})
_ENTRY_KEYS = frozenset({"id", "measure", "question_set"})
_SPECIFIC_KEYS = frozenset({"id", "measure", "floor", "ceiling", "scope_tag", "question_set"})
_NOISE_KEYS = frozenset({"min_tries"})
_RECORD_KEYS = frozenset({
    "schema", "deployment", "generation_id", "run_id", "setup_digest",
    "question_set", "tries", "measures", "per_scope_tag", "constraints",
})
_RECORD_REQUIRED = ("deployment", "generation_id", "run_id", "setup_digest")
_QUESTION_SET_KEYS = frozenset({"id", "digest"})
_CONSTRAINT_STATUSES = frozenset({"satisfied", "violated"})


# --- objectives.yaml -------------------------------------------------------


class _UniqueKeyLoader(yaml.SafeLoader):
    """safe_load that refuses a repeated mapping key instead of keeping the last."""


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False):
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark,
            )
        seen.add(key)
    return loader.construct_mapping(node, deep=deep)


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping,
)


def load_objectives(path: str | Path) -> Objectives:
    """Read and validate one objectives.yaml; raise ObjectivesError on refusal."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ObjectivesError([
            ObjectivesProblem("unreadable", f"objectives file cannot be read: {exc}"),
        ]) from exc
    try:
        raw = yaml.load(text, Loader=_UniqueKeyLoader)  # noqa: S506 - SafeLoader subclass
    except yaml.YAMLError as exc:
        raise ObjectivesError([
            ObjectivesProblem("yaml_parse", f"objectives file does not parse: {exc}"),
        ]) from exc
    return parse_objectives(raw)


def parse_objectives(raw: Any) -> Objectives:
    problems = validate_objectives(raw)
    if problems:
        raise ObjectivesError(problems)
    assert isinstance(raw, Mapping)
    noise = raw["noise"]
    return Objectives(
        version=int(raw["version"]),
        hard_constraints=tuple(raw["hard_constraints"]),
        min_tries=int(noise["min_tries"]),
        objectives=tuple(_objective(e) for e in raw.get("objectives") or ()),
        costs=tuple(_objective(e) for e in raw.get("costs") or ()),
        specific=tuple(_specific(e) for e in raw.get("specific") or ()),
        policy_id=_policy_id(raw),
    )


def validate_objectives(raw: Any) -> list[ObjectivesProblem]:
    """Return every refusal in an objectives document, in document order."""
    out: list[ObjectivesProblem] = []
    if not isinstance(raw, Mapping):
        return [ObjectivesProblem(
            "not_mapping", "objectives file must be a mapping", "",
        )]
    _unknown_keys(raw, _TOP_KEYS, "", out)
    if type(raw.get("version")) is not int or raw.get("version") != OBJECTIVES_VERSION:
        out.append(ObjectivesProblem(
            "version_unsupported",
            f"version must be {OBJECTIVES_VERSION}, got {raw.get('version')!r}",
            "/version",
        ))
    hard = raw.get("hard_constraints")
    if not isinstance(hard, list) or sorted(map(str, hard)) != sorted(HARD_CONSTRAINTS) \
            or len(hard) != len(HARD_CONSTRAINTS):
        out.append(ObjectivesProblem(
            "hard_constraints_mismatch",
            "hard_constraints must list exactly "
            + ", ".join(HARD_CONSTRAINTS) + "; they cannot be removed or added",
            "/hard_constraints",
        ))
    # The repeat count has no default: a file that does not say how many
    # tries a measurement needs would otherwise count single-try results.
    noise = raw.get("noise")
    if noise is None:
        out.append(ObjectivesProblem(
            "noise_missing", "noise.min_tries (tries per question) is required", "/noise",
        ))
    elif not isinstance(noise, Mapping):
        out.append(ObjectivesProblem("noise_invalid", "noise must be a mapping", "/noise"))
    else:
        _unknown_keys(noise, _NOISE_KEYS, "/noise", out)
        if "min_tries" not in noise:
            out.append(ObjectivesProblem(
                "noise_missing", "noise.min_tries (tries per question) is required",
                "/noise/min_tries",
            ))
        else:
            tries = noise["min_tries"]
            if isinstance(tries, bool) or not isinstance(tries, int) or tries < 1:
                out.append(ObjectivesProblem(
                    "noise_invalid", "noise.min_tries must be a positive integer",
                    "/noise/min_tries",
                ))
    seen: set[str] = set()
    for block in ("objectives", "costs"):
        for index, entry in _entries(raw, block, out):
            _check_entry(entry, f"/{block}/{index}", _ENTRY_KEYS, seen, out)
    for index, entry in _entries(raw, "specific", out):
        pointer = f"/specific/{index}"
        measure = _check_entry(entry, pointer, _SPECIFIC_KEYS, seen, out)
        _check_threshold(entry, pointer, measure, out)
        tag = entry.get("scope_tag")
        if tag is not None and (not isinstance(tag, str) or not tag.strip()):
            out.append(ObjectivesProblem(
                "scope_tag_invalid", "scope_tag must be a non-empty string",
                f"{pointer}/scope_tag",
            ))
    return out


def _entries(raw: Mapping[str, Any], block: str, out: list[ObjectivesProblem]):
    value = raw.get(block)
    if value is None:
        return []
    if not isinstance(value, list):
        out.append(ObjectivesProblem("block_not_list", f"{block} must be a list", f"/{block}"))
        return []
    rows = []
    for index, entry in enumerate(value):
        if not isinstance(entry, Mapping):
            out.append(ObjectivesProblem(
                "entry_not_mapping", f"{block} entry must be a mapping", f"/{block}/{index}",
            ))
            continue
        rows.append((index, entry))
    return rows


def _check_entry(
    entry: Mapping[str, Any],
    pointer: str,
    allowed: frozenset[str],
    seen: set[str],
    out: list[ObjectivesProblem],
) -> Measure | None:
    _unknown_keys(entry, allowed, pointer, out)
    entry_id = entry.get("id")
    if not isinstance(entry_id, str) or not entry_id.strip():
        out.append(ObjectivesProblem("id_missing", "entry needs a non-empty id", f"{pointer}/id"))
    elif entry_id in seen:
        out.append(ObjectivesProblem(
            "duplicate_id", f"id {entry_id!r} is used twice", f"{pointer}/id",
        ))
    else:
        seen.add(entry_id)
    measure = MEASURES.get(entry.get("measure")) if isinstance(entry.get("measure"), str) else None
    if measure is None:
        out.append(ObjectivesProblem(
            "measure_unknown",
            f"measure {entry.get('measure')!r} is not in the measure list",
            f"{pointer}/measure",
        ))
        return None
    question_set = entry.get("question_set")
    if measure.answer_based and (not isinstance(question_set, str) or not question_set.strip()):
        out.append(ObjectivesProblem(
            "question_set_required",
            f"measure {measure.name!r} is computed from answers and must name its question_set",
            f"{pointer}/question_set",
        ))
    elif not measure.answer_based and "question_set" in entry:
        out.append(ObjectivesProblem(
            "question_set_not_applicable",
            f"measure {measure.name!r} is not computed from answers; it takes no question_set",
            f"{pointer}/question_set",
        ))
    return measure


def _check_threshold(
    entry: Mapping[str, Any],
    pointer: str,
    measure: Measure | None,
    out: list[ObjectivesProblem],
) -> None:
    has_floor = "floor" in entry
    has_ceiling = "ceiling" in entry
    if has_floor == has_ceiling:
        out.append(ObjectivesProblem(
            "threshold_both" if has_floor else "threshold_missing",
            "a specific objective needs exactly one of floor or ceiling",
            pointer,
        ))
        return
    key = "floor" if has_floor else "ceiling"
    value = entry[key]
    if not _is_number(value):
        out.append(ObjectivesProblem(
            "threshold_not_number", f"{key} must be a number", f"{pointer}/{key}",
        ))
        return
    if measure is None:
        return
    if measure.unit is Unit.RATE and not 0.0 <= float(value) <= 1.0:
        out.append(ObjectivesProblem(
            "threshold_out_of_range",
            f"{key} for rate measure {measure.name!r} must be between 0 and 1",
            f"{pointer}/{key}",
        ))
    elif float(value) < 0.0:
        out.append(ObjectivesProblem(
            "threshold_out_of_range",
            f"{key} for measure {measure.name!r} cannot be negative",
            f"{pointer}/{key}",
        ))
    # A floor bounds a measure to maximize, a ceiling one to minimize; the
    # other pairing (a ceiling on correctness) states no useful goal.
    expected = "floor" if measure.direction is Direction.MAXIMIZE else "ceiling"
    if key != expected:
        out.append(ObjectivesProblem(
            "threshold_direction",
            f"measure {measure.name!r} is to {measure.direction.value}; "
            f"bound it with a {expected}, not a {key}",
            f"{pointer}/{key}",
        ))


def _objective(entry: Mapping[str, Any]) -> Objective:
    return Objective(id=entry["id"], measure=entry["measure"], question_set=entry.get("question_set"))


def _specific(entry: Mapping[str, Any]) -> SpecificObjective:
    return SpecificObjective(
        id=entry["id"],
        measure=entry["measure"],
        floor=float(entry["floor"]) if "floor" in entry else None,
        ceiling=float(entry["ceiling"]) if "ceiling" in entry else None,
        scope_tag=entry.get("scope_tag"),
        question_set=entry.get("question_set"),
    )


def _policy_id(raw: Mapping[str, Any]) -> str:
    """Digest of the parsed document. Key order, comments and layout do not
    change it; list order and literal spelling (1 vs 1.0) do, and so does
    nothing outside the file, such as an edit to MEASURES."""
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"), default=str)
    return "akb-objectives:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


# --- measurement record ----------------------------------------------------


def parse_measurement(raw: Any) -> MeasurementRecord:
    problems = validate_measurement(raw)
    if problems:
        raise ObjectivesError(problems)
    assert isinstance(raw, Mapping)
    question_set = raw.get("question_set") or {}
    return MeasurementRecord(
        deployment=raw["deployment"],
        generation_id=raw["generation_id"],
        run_id=raw["run_id"],
        setup_digest=raw["setup_digest"],
        question_set_id=question_set.get("id"),
        question_set_digest=question_set.get("digest"),
        tries=raw.get("tries"),
        measures=MappingProxyType({k: float(v) for k, v in (raw.get("measures") or {}).items()}),
        per_scope_tag=MappingProxyType({
            tag: MappingProxyType({k: float(v) for k, v in values.items()})
            for tag, values in (raw.get("per_scope_tag") or {}).items()
        }),
        constraints=MappingProxyType(dict(raw.get("constraints") or {})),
    )


def validate_measurement(raw: Any) -> list[ObjectivesProblem]:
    """Return every refusal in one measurement record."""
    out: list[ObjectivesProblem] = []
    if not isinstance(raw, Mapping):
        return [ObjectivesProblem("not_mapping", "measurement record must be a mapping")]
    _unknown_keys(raw, _RECORD_KEYS, "", out)
    if raw.get("schema") != MEASUREMENT_SCHEMA:
        out.append(ObjectivesProblem(
            "schema_mismatch", f"schema must be {MEASUREMENT_SCHEMA!r}", "/schema",
        ))
    for name in _RECORD_REQUIRED:
        value = raw.get(name)
        if not isinstance(value, str) or not value.strip():
            out.append(ObjectivesProblem(
                "field_missing", f"{name} must be a non-empty string", f"/{name}",
            ))
    question_set = raw.get("question_set")
    if question_set is not None:
        if not isinstance(question_set, Mapping) or not all(
            isinstance(question_set.get(k), str) and question_set.get(k) for k in ("id", "digest")
        ):
            out.append(ObjectivesProblem(
                "question_set_invalid", "question_set must carry id and digest", "/question_set",
            ))
        else:
            _unknown_keys(question_set, _QUESTION_SET_KEYS, "/question_set", out)
    tries = raw.get("tries")
    if tries is not None and (isinstance(tries, bool) or not isinstance(tries, int) or tries < 1):
        out.append(ObjectivesProblem("tries_invalid", "tries must be a positive integer", "/tries"))
    _check_values(raw.get("measures"), "/measures", out)
    per_tag = raw.get("per_scope_tag")
    if per_tag is not None:
        if not isinstance(per_tag, Mapping):
            out.append(ObjectivesProblem(
                "block_not_mapping", "per_scope_tag must be a mapping", "/per_scope_tag",
            ))
        else:
            for tag, values in per_tag.items():
                if not isinstance(tag, str) or not tag.strip():
                    out.append(ObjectivesProblem(
                        "scope_tag_invalid", f"scope tag {tag!r} must be a non-empty string",
                        "/per_scope_tag",
                    ))
                    continue
                if not isinstance(values, Mapping):
                    out.append(ObjectivesProblem(
                        "block_not_mapping", f"per_scope_tag {tag!r} must be a mapping",
                        f"/per_scope_tag/{tag}",
                    ))
                    continue
                _check_values(values, f"/per_scope_tag/{tag}", out)
    constraints = raw.get("constraints")
    if constraints is not None:
        if not isinstance(constraints, Mapping):
            out.append(ObjectivesProblem(
                "block_not_mapping", "constraints must be a mapping", "/constraints",
            ))
        else:
            for name, status in constraints.items():
                if name not in HARD_CONSTRAINTS:
                    out.append(ObjectivesProblem(
                        "constraint_unknown", f"{name!r} is not a hard constraint",
                        f"/constraints/{name}",
                    ))
                elif not isinstance(status, str) or status not in _CONSTRAINT_STATUSES:
                    out.append(ObjectivesProblem(
                        "constraint_status_invalid",
                        f"constraint {name!r} must be satisfied or violated",
                        f"/constraints/{name}",
                    ))
    return out


def _check_values(values: Any, pointer: str, out: list[ObjectivesProblem]) -> None:
    if values is None:
        return
    if not isinstance(values, Mapping):
        out.append(ObjectivesProblem("block_not_mapping", "measures must be a mapping", pointer))
        return
    for name, value in values.items():
        measure = MEASURES.get(name) if isinstance(name, str) else None
        where = f"{pointer}/{name}"
        if measure is None:
            out.append(ObjectivesProblem(
                "measure_unknown", f"measure {name!r} is not in the measure list", where,
            ))
        elif not _is_number(value):
            out.append(ObjectivesProblem("value_not_number", f"{name} must be a number", where))
        elif measure.unit is Unit.RATE and not 0.0 <= float(value) <= 1.0:
            out.append(ObjectivesProblem(
                "rate_out_of_range", f"{name} is a rate and must be between 0 and 1", where,
            ))
        elif float(value) < 0:
            out.append(ObjectivesProblem("negative_value", f"{name} cannot be negative", where))


# --- scoring ---------------------------------------------------------------


@dataclass(frozen=True)
class ScoreEntry:
    kind: str  # objective | cost | specific | hard_constraint
    id: str
    status: str  # measured | unmeasured | met | violated | satisfied
    measure: str | None = None
    unit: str | None = None
    value: float | None = None
    threshold: float | None = None
    reason: str | None = None
    blocking: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass(frozen=True)
class ObjectivesScore:
    policy_id: str
    generation_id: str
    run_id: str
    entries: tuple[ScoreEntry, ...]

    @property
    def passes(self) -> bool:
        """True only when every hard constraint is satisfied and every
        specific objective is met; unmeasured counts as not passing.
        Objectives and costs are reported, not gated: they place the
        generation on the quality-cost curve (#4283)."""
        for entry in self.entries:
            if entry.kind == "hard_constraint" and entry.blocking:
                return False
            if entry.kind == "specific" and entry.status != "met":
                return False
        return True

    def as_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "generation_id": self.generation_id,
            "run_id": self.run_id,
            "passes": self.passes,
            "entries": [entry.as_dict() for entry in self.entries],
        }


def score_objectives(
    objectives: Objectives,
    record: MeasurementRecord,
    *,
    deployment: str,
) -> ObjectivesScore:
    """Score one record against the objectives of ``deployment``.

    A record from another deployment is refused by name: scoring it would
    judge one knowledge base by another's run."""
    if record.deployment != deployment:
        raise ObjectivesError([ObjectivesProblem(
            "deployment_mismatch",
            f"record is for deployment {record.deployment!r}, objectives are for {deployment!r}",
            "/deployment",
        )])
    entries: list[ScoreEntry] = []
    for name in objectives.hard_constraints:
        status = record.constraints.get(name)
        if status is None:
            entries.append(ScoreEntry(
                "hard_constraint", name, "unmeasured", reason="not_recorded", blocking=True,
            ))
        else:
            entries.append(ScoreEntry(
                "hard_constraint", name, status, blocking=status != "satisfied",
            ))
    for kind, rows in (("objective", objectives.objectives), ("cost", objectives.costs)):
        for row in rows:
            value, reason = _read_value(objectives, record, row.measure, row.question_set, None)
            entries.append(ScoreEntry(
                kind, row.id,
                "measured" if reason is None else "unmeasured",
                measure=row.measure, unit=MEASURES[row.measure].unit.value,
                value=value, reason=reason,
            ))
    for row in objectives.specific:
        value, reason = _read_value(objectives, record, row.measure, row.question_set, row.scope_tag)
        threshold = row.floor if row.floor is not None else row.ceiling
        if reason is not None:
            status = "unmeasured"
        elif row.floor is not None:
            status = "met" if value >= row.floor else "violated"
        else:
            status = "met" if value <= row.ceiling else "violated"
        entries.append(ScoreEntry(
            "specific", row.id, status,
            measure=row.measure, unit=MEASURES[row.measure].unit.value,
            value=value, threshold=threshold, reason=reason,
        ))
    return ObjectivesScore(
        policy_id=objectives.policy_id,
        generation_id=record.generation_id,
        run_id=record.run_id,
        entries=tuple(entries),
    )


def _read_value(
    objectives: Objectives,
    record: MeasurementRecord,
    measure_name: str,
    question_set: str | None,
    scope_tag: str | None,
) -> tuple[float | None, str | None]:
    measure = MEASURES[measure_name]
    if measure.answer_based:
        if question_set is not None and question_set != record.question_set_id:
            return None, "question_set_mismatch"
        if record.tries is None or record.tries < objectives.min_tries:
            return None, "too_few_tries"
    source = record.measures
    if scope_tag is not None:
        source = record.per_scope_tag.get(scope_tag)
        if source is None:
            return None, "scope_tag_not_recorded"
    value = source.get(measure_name)
    if value is None or not math.isfinite(value):
        return None, "not_recorded"
    return float(value), None


# --- acceptance gate inputs -----------------------------------------------


def acceptance_thresholds(objectives: Objectives) -> tuple[str, dict[str, float]]:
    """The policy_id and threshold_values a RewriteReplayMeasurement carries.

    The gate records these but does not compare metrics against them; the
    PASS or FAIL it trusts comes from ``replay_decision``. The gate refuses an
    empty threshold mapping, so a file without specific entries is refused
    here by name."""
    if not objectives.specific:
        raise ObjectivesError([ObjectivesProblem(
            "thresholds_empty",
            "the acceptance gate needs at least one specific floor or ceiling",
            "/specific",
        )])
    thresholds: dict[str, float] = {}
    for row in objectives.specific:
        if row.floor is not None:
            thresholds[f"{row.id}.floor"] = row.floor
        else:
            assert row.ceiling is not None
            thresholds[f"{row.id}.ceiling"] = row.ceiling
    return objectives.policy_id, thresholds


def replay_decision(score: ObjectivesScore) -> RewriteReplayDecision:
    return RewriteReplayDecision.PASS if score.passes else RewriteReplayDecision.FAIL


def measured_values(score: ObjectivesScore) -> dict[str, float]:
    """Metric values for a RewriteReplayMeasurement: every measured entry."""
    return {
        entry.id: entry.value
        for entry in score.entries
        if entry.value is not None
    }


# --- helpers ---------------------------------------------------------------


def _unknown_keys(
    mapping: Mapping[str, Any],
    allowed: frozenset[str],
    pointer: str,
    out: list[ObjectivesProblem],
) -> None:
    for key in mapping:
        if key not in allowed:
            out.append(ObjectivesProblem(
                "unknown_key", f"unknown key {key!r}; allowed: {', '.join(sorted(allowed))}",
                f"{pointer}/{key}",
            ))


def _is_number(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int | float)
        and math.isfinite(float(value))
    )
