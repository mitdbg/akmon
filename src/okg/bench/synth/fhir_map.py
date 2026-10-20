"""Standalone FHIR R4 (Synthea export) → ground-truth world mapping.

Copy-from prior art: the intersystems deployment's ``SyntheaFHIRSource``
(okg-deployments) parses the same bundles — but it imports substrate code,
and this mapping must not (independence rule), so the parsing here is
self-contained. The mapping is a pure function of the frozen export
directory: no network, no substrate, deterministic ordering throughout.

Extracted fact kinds (v1):
- ``observation:<slug>`` — latest numeric vital/lab per LOINC code we map
  (height, weight, BMI kept as the derived-value FD triple).
- ``medication:<drug-slug>`` — active MedicationRequest display string.
- ``condition_status:<slug>`` — active condition's clinical status.
- ``allergy_status:<slug>`` — allergy intolerance presence (polarity site).
- ``care_provider`` — the (singleton) requester/GP — cardinality site.
- ``birthdate`` — patient birthdate (derived-value input for age).
- ``age_years`` — derived from birthdate at the world reference date.
- ``record_reference`` — the chart's own record identifier (the FHIR record
  id); the surface a linked_pair corruption rewrites to an entity key.

Future-world hazards (safe for the frozen export, re-verify on any world
regeneration): latest-observation selection tie-breaks lexicographically
over offset-bearing ISO timestamps and bundle entry order; medication
dedupe keys on the display's first word (distinct strengths of one drug
merge); ``max_facts_per_patient`` truncates the tail of the ordered fact
list.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Iterator, Mapping

from okg.bench.synth.worldschema import (
    SCHEMA_VERSION,
    Fact,
    GroundTruthWorld,
    Patient,
    Practitioner,
    WorldError,
)

# LOINC codes → observation slugs (the numeric vitals/labs v1 maps).
_LOINC_MAP = {
    "8302-2": ("body_height", "cm"),
    "29463-7": ("body_weight", "kg"),
    "39156-5": ("body_mass_index", "kg/m2"),
    "8480-6": ("systolic_blood_pressure", "mm[Hg]"),
    "8462-4": ("diastolic_blood_pressure", "mm[Hg]"),
    "2093-3": ("total_cholesterol", "mg/dL"),
    "4548-4": ("hemoglobin_a1c", "%"),
}


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _iter_bundle_resources(bundle_path: Path) -> Iterator[Mapping[str, Any]]:
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    for entry in payload.get("entry", ()):
        resource = entry.get("resource")
        if isinstance(resource, Mapping):
            yield resource


def _clean_synthea_name(raw: str) -> str:
    """Synthea appends digit suffixes ('Abram53') — keep them: they are the
    uniqueness-by-construction property the leakage scanner keys on."""
    return raw.strip()


def _age_years(birthdate: str, reference_date: str) -> int:
    born = date.fromisoformat(birthdate)
    ref = date.fromisoformat(reference_date)
    years = ref.year - born.year - ((ref.month, ref.day) < (born.month, born.day))
    if years < 0:
        raise WorldError(f"reference date {reference_date} precedes birthdate {birthdate}")
    return years


def world_from_synthea_dir(
    bundles_dir: str | Path,
    *,
    reference_date: str,
    provenance: Mapping[str, Any],
    max_facts_per_patient: int = 40,
) -> GroundTruthWorld:
    """Map a frozen Synthea FHIR export directory into the ground-truth world."""
    root = Path(bundles_dir)
    if not root.is_dir():
        raise WorldError(f"synthea export dir {root} is not a directory")
    bundle_paths = sorted(
        p
        for p in root.glob("*.json")
        if not p.name.startswith(("hospital", "practitioner"))
        and p.name != "provenance.json"
    )
    if not bundle_paths:
        raise WorldError(f"synthea export dir {root} has no patient bundles")

    patients: list[Patient] = []
    practitioners: dict[str, Practitioner] = {}
    facts: list[Fact] = []

    for bundle_path in bundle_paths:
        patient: Patient | None = None
        observations: dict[str, tuple[str, str, str]] = {}  # slug -> (date, value, unit)
        medications: dict[str, str] = {}  # drug slug -> dose text
        med_display: dict[str, str] = {}
        conditions: dict[str, str] = {}
        allergies: dict[str, str] = {}
        provider: tuple[str, str] | None = None

        for resource in _iter_bundle_resources(bundle_path):
            rtype = resource.get("resourceType")
            if rtype == "Patient":
                name_entry = next(
                    (n for n in resource.get("name", ()) if n.get("use") == "official"),
                    None,
                ) or (resource.get("name") or [{}])[0]
                given = _clean_synthea_name(" ".join(name_entry.get("given", ())))
                family = _clean_synthea_name(name_entry.get("family", ""))
                if not given or not family:
                    raise WorldError(f"{bundle_path.name}: patient has no usable name")
                aliases = tuple(
                    sorted(
                        {
                            f"{family}, {given}",
                            f"{given[0]}. {family}",
                        }
                    )
                )
                patient = Patient(
                    patient_id=resource.get("id", bundle_path.stem),
                    given=given,
                    family=family,
                    birthdate=resource.get("birthDate", ""),
                    gender=resource.get("gender", "unknown"),
                    aliases=aliases,
                )
            elif rtype == "Observation":
                coding = (resource.get("code") or {}).get("coding") or []
                code = coding[0].get("code") if coding else None
                mapped = _LOINC_MAP.get(code or "")
                quantity = resource.get("valueQuantity")
                effective = resource.get("effectiveDateTime", "")
                if mapped and isinstance(quantity, Mapping) and "value" in quantity:
                    slug, unit = mapped
                    value = f"{float(quantity['value']):.1f}"
                    previous = observations.get(slug)
                    if previous is None or effective >= previous[0]:
                        observations[slug] = (effective, value, unit)
            elif rtype == "MedicationRequest":
                if resource.get("status") != "active":
                    continue
                concept = resource.get("medicationCodeableConcept") or {}
                display = concept.get("text") or (
                    (concept.get("coding") or [{}])[0].get("display", "")
                )
                if not display:
                    continue
                drug = _slug(display.split(" ")[0])
                if drug and drug not in medications:
                    medications[drug] = display
                    med_display[drug] = display
                requester = resource.get("requester") or {}
                display_name = requester.get("display")
                if display_name and provider is None:
                    provider = (_slug(display_name), display_name)
            elif rtype == "Condition":
                status = ((resource.get("clinicalStatus") or {}).get("coding") or [{}])[0].get(
                    "code", ""
                )
                text = (resource.get("code") or {}).get("text", "")
                if text and status == "active":
                    conditions[_slug(text)] = text
            elif rtype == "AllergyIntolerance":
                text = (resource.get("code") or {}).get("text", "")
                if text:
                    allergies[_slug(text)] = text
            elif rtype == "Practitioner":
                name_entry = (resource.get("name") or [{}])[0]
                display = " ".join(
                    filter(None, [" ".join(name_entry.get("given", ())), name_entry.get("family")])
                )
                if display:
                    practitioners.setdefault(
                        resource.get("id", display), Practitioner(resource.get("id", display), display)
                    )

        if patient is None:
            raise WorldError(f"{bundle_path.name}: bundle has no Patient resource")

        patient_facts: list[Fact] = []

        def add(kind: str, key: str, value: str, unit: str = "",
                derived: tuple[str, tuple[str, ...]] | None = None) -> str:
            fact_id = f"fact:{patient.patient_id}:{kind}:{key}"
            patient_facts.append(
                Fact(
                    fact_id=fact_id,
                    patient_id=patient.patient_id,
                    kind=kind,
                    key=key,
                    value=value,
                    unit=unit,
                    derived_from=derived,
                )
            )
            return fact_id

        # The record reference: the chart's own record identifier (the FHIR
        # record id). It renders in a sentence where a slug-shaped identifier is
        # natural ("Filed under record reference <id>."), so the linked_pair
        # corruption's rewritten value survives prose extraction — the failure
        # mode the age_years surface had ("The patient is <slug> of age." was
        # semantic nonsense the extractor skipped). The clean value is the record
        # id, NOT the patient entity key, so a clean record_reference never links
        # to its own birthdate (that self-link would be a clean-corpus false
        # positive); only the corruption rewrites it to the entity key. Added
        # EARLY so the per-patient fact cap never truncates it away.
        add("record_reference", "record_reference", patient.patient_id)
        if patient.birthdate:
            birth_id = add("birthdate", "birthdate", patient.birthdate)
            add(
                "age_years",
                "age_years",
                str(_age_years(patient.birthdate, reference_date)),
                derived=("age_from_birthdate", (birth_id,)),
            )
        height_id = weight_id = None
        for slug in sorted(observations):
            effective, value, unit = observations[slug]
            fact_id = add(f"observation:{slug}", slug, value, unit)
            if slug == "body_height":
                height_id = fact_id
            elif slug == "body_weight":
                weight_id = fact_id
        if height_id and weight_id and "body_mass_index" in observations:
            # re-tag BMI as derived so the FD is declared in gold
            patient_facts = [
                (
                    Fact(
                        fact_id=f.fact_id,
                        patient_id=f.patient_id,
                        kind=f.kind,
                        key=f.key,
                        value=f.value,
                        unit=f.unit,
                        derived_from=("bmi_from_height_weight", (height_id, weight_id)),
                    )
                    if f.kind == "observation:body_mass_index"
                    else f
                )
                for f in patient_facts
            ]
        for drug in sorted(medications):
            add(f"medication:{drug}", drug, med_display[drug])
        for slug in sorted(conditions):
            add(f"condition_status:{slug}", slug, "active")
        for slug in sorted(allergies):
            add(f"allergy_status:{slug}", slug, "present")
        if provider is not None:
            add("care_provider", "care_provider", provider[1])
            practitioners.setdefault(provider[0], Practitioner(provider[0], provider[1]))

        if len(patient_facts) > max_facts_per_patient:
            patient_facts = patient_facts[:max_facts_per_patient]
        patients.append(patient)
        facts.extend(patient_facts)

    return GroundTruthWorld(
        schema_version=SCHEMA_VERSION,
        provenance=dict(provenance),
        reference_date=reference_date,
        patients=tuple(patients),
        practitioners=tuple(practitioners[k] for k in sorted(practitioners)),
        facts=tuple(facts),
    )
