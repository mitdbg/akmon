"""The ground-truth KG schema — engine-independent, versioned, JSON-stable.

This is the benchmark's truth artifact. It is deliberately its own schema
(``okg.synth.world.v1``) with no okg substrate imports: gold is never
constructed by, passed through, or validated against the system under
test, so SUT blind spots cannot leak into it.

Facts carry stable ids so the corruption plan and rendering trace can
reference them; derived facts declare the functional dependency that
produced them (the n-ary gold surface).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping

SCHEMA_VERSION = "okg.synth.world.v1"


class WorldError(ValueError):
    """Typed failure for world construction/validation problems."""


@dataclass(frozen=True)
class Patient:
    patient_id: str
    given: str
    family: str
    birthdate: str  # ISO date
    gender: str
    aliases: tuple[str, ...] = ()

    @property
    def full_name(self) -> str:
        return f"{self.given} {self.family}"


@dataclass(frozen=True)
class Practitioner:
    practitioner_id: str
    name: str


@dataclass(frozen=True)
class Fact:
    """One atomic ground-truth assertion about a patient.

    ``kind`` is the claim predicate family (e.g. ``medication_dose``,
    ``observation:body_weight``, ``allergy_status``, ``care_provider``).
    ``key`` disambiguates multiple facts of one kind (e.g. the drug name).
    ``value`` is the canonical value; ``unit`` its canonical unit ("" when
    unitless). ``derived_from`` names the functional dependency and input
    fact ids when this fact is computed, e.g. ``("bmi", (h_id, w_id))``.
    """

    fact_id: str
    patient_id: str
    kind: str
    key: str
    value: str
    unit: str = ""
    derived_from: tuple[str, tuple[str, ...]] | None = None

    def quantity(self) -> float:
        try:
            return float(self.value)
        except ValueError as exc:  # pragma: no cover - guarded by callers
            raise WorldError(f"fact {self.fact_id} value {self.value!r} is not numeric") from exc


@dataclass(frozen=True)
class GroundTruthWorld:
    """The complete static world: who exists and what is true."""

    schema_version: str
    provenance: Mapping[str, Any]
    patients: tuple[Patient, ...]
    practitioners: tuple[Practitioner, ...]
    facts: tuple[Fact, ...]
    reference_date: str  # the corpus-wide "as of" date (static world)

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise WorldError(
                f"world schema {self.schema_version!r} != expected {SCHEMA_VERSION!r}"
            )
        patient_ids = {p.patient_id for p in self.patients}
        if len(patient_ids) != len(self.patients):
            raise WorldError("duplicate patient ids in world")
        fact_ids: set[str] = set()
        for fact in self.facts:
            if fact.fact_id in fact_ids:
                raise WorldError(f"duplicate fact id {fact.fact_id}")
            fact_ids.add(fact.fact_id)
            if fact.patient_id not in patient_ids:
                raise WorldError(f"fact {fact.fact_id} references unknown patient")
        for fact in self.facts:
            if fact.derived_from is not None:
                _, inputs = fact.derived_from
                missing = [i for i in inputs if i not in fact_ids]
                if missing:
                    raise WorldError(
                        f"derived fact {fact.fact_id} references missing inputs {missing}"
                    )

    # -- accessors ---------------------------------------------------------

    def patient(self, patient_id: str) -> Patient:
        for candidate in self.patients:
            if candidate.patient_id == patient_id:
                return candidate
        raise WorldError(f"unknown patient {patient_id}")

    def facts_for(self, patient_id: str) -> tuple[Fact, ...]:
        return tuple(f for f in self.facts if f.patient_id == patient_id)

    def fact(self, fact_id: str) -> Fact:
        for candidate in self.facts:
            if candidate.fact_id == fact_id:
                return candidate
        raise WorldError(f"unknown fact {fact_id}")

    def kind_range(self, kind: str) -> tuple[float, float]:
        """Empirical (min, max) over numeric values of one fact kind.

        This is the plausible-in-isolation reference range: perturbations
        sampled inside it cannot be dismissed on domain knowledge alone.
        Raises when the kind has no numeric population.
        """
        values = []
        for fact in self.facts:
            if fact.kind == kind:
                try:
                    values.append(float(fact.value))
                except ValueError:
                    continue
        if not values:
            raise WorldError(f"kind {kind!r} has no numeric population for a range")
        return (min(values), max(values))

    # -- serialization -----------------------------------------------------

    def to_json_bytes(self) -> bytes:
        payload = {
            "schema_version": self.schema_version,
            "provenance": dict(self.provenance),
            "reference_date": self.reference_date,
            "patients": [asdict(p) for p in self.patients],
            "practitioners": [asdict(p) for p in self.practitioners],
            "facts": [_fact_to_json(f) for f in self.facts],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _fact_to_json(fact: Fact) -> dict[str, Any]:
    row = asdict(fact)
    if fact.derived_from is not None:
        row["derived_from"] = [fact.derived_from[0], list(fact.derived_from[1])]
    return row


def world_from_json_bytes(raw: bytes) -> GroundTruthWorld:
    payload = json.loads(raw.decode("utf-8"))
    facts = []
    for row in payload["facts"]:
        derived = row.get("derived_from")
        facts.append(
            Fact(
                fact_id=row["fact_id"],
                patient_id=row["patient_id"],
                kind=row["kind"],
                key=row["key"],
                value=row["value"],
                unit=row.get("unit", ""),
                derived_from=(derived[0], tuple(derived[1])) if derived else None,
            )
        )
    return GroundTruthWorld(
        schema_version=payload["schema_version"],
        provenance=payload["provenance"],
        reference_date=payload["reference_date"],
        patients=tuple(
            Patient(**{**p, "aliases": tuple(p.get("aliases", ()))})
            for p in payload["patients"]
        ),
        practitioners=tuple(Practitioner(**p) for p in payload["practitioners"]),
        facts=tuple(facts),
    )
