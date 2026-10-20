"""Chronos read-surface audit for substrate-library invariants."""
from __future__ import annotations

import re
from pathlib import Path

from okg.substrate.catalog.invariants import load_invariants


REPO_ROOT = Path(__file__).resolve().parents[2]
INVARIANT_ROOT = REPO_ROOT / "src/okg/substrate/library/invariants"
PROFILE_ROOT = REPO_ROOT / "src/okg/substrate/library/profiles"
DB_ROOT = REPO_ROOT / "src/okg/substrate/db"
FORBIDDEN_TOKENS = (
    "okg.nodes_live",
    "okg.edges_live",
    "nodes_live",
    "edges_live",
    "valid_to_gen",
)


def test_shipped_library_invariants_use_chronos_view_surfaces() -> None:
    offenders: list[str] = []
    for path in sorted(INVARIANT_ROOT.glob("*.yaml")):
        text = path.read_text()
        for token in FORBIDDEN_TOKENS:
            if token in text:
                rel = path.relative_to(REPO_ROOT)
                offenders.append(f"{rel}: contains {token!r}")

    assert not offenders, "\n".join(offenders)


def test_shipped_library_invariants_still_parse() -> None:
    names = {
        "no_orphan_edges",
        "every_text_node_has_text",
        "subtype_counts_within_bounds",
        "monotonic_observation_at",
        "catalog_version_consistent",
    }
    for name in names:
        specs = load_invariants(INVARIANT_ROOT / f"{name}.yaml")
        assert len(specs) == 1
        assert specs[0].name == name


def test_subtype_count_invariant_has_active_bound() -> None:
    specs = load_invariants(
        INVARIANT_ROOT / "subtype_counts_within_bounds.yaml"
    )
    assert len(specs) == 1
    sql_body = specs[0].sql_body
    assert sql_body is not None
    assert "okg.v_nodes" in sql_body
    normalized_sql = sql_body.lower()
    assert "where false" not in normalized_sql
    assert "current_count > b.max_expected" in normalized_sql


# ---- profile bundles ------------------------------------------------------
#
# `okg init` copies a profile's `invariants.yaml` to the deployment root and
# (per `compose-root-invariants`) the composer installs it, so every bundle
# below is evaluated on each publish of a deployment scaffolded from it. The
# publisher EXPLAINs every installed SQL invariant before Phase B and refuses
# the whole cycle when one cannot be planned, so a bundle naming a relation
# the schema never creates would block every publish of that profile.


def _profile_bundles() -> list[Path]:
    bundles = sorted(PROFILE_ROOT.glob("*/invariants.yaml"))
    assert bundles, PROFILE_ROOT
    return bundles


def _schema_relation_names() -> set[str]:
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:UNLOGGED\s+)?"
        r"(?:TABLE|VIEW|MATERIALIZED\s+VIEW|FUNCTION)\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?okg\.([a-z_][a-z0-9_]*)",
        re.IGNORECASE,
    )
    names: set[str] = set()
    for sql_path in sorted(DB_ROOT.glob("*.sql")):
        names.update(m.group(1).lower() for m in pattern.finditer(sql_path.read_text()))
    assert "v_nodes" in names and "v_runtime_queue_health" in names
    return names


def test_profile_bundles_reference_only_schema_relations() -> None:
    known = _schema_relation_names()
    offenders: list[str] = []
    for bundle in _profile_bundles():
        for spec in load_invariants(bundle):
            body = spec.sql_body or spec.candidates_sql or ""
            for rel in sorted(set(re.findall(r"\bokg\.([a-z_][a-z0-9_]*)", body))):
                if rel not in known:
                    rel_path = bundle.relative_to(REPO_ROOT)
                    offenders.append(f"{rel_path}: {spec.name} reads okg.{rel}")
    assert not offenders, "\n".join(offenders)


def test_profile_bundles_ship_warn_severity_only() -> None:
    offenders = [
        f"{bundle.relative_to(REPO_ROOT)}: {spec.name} is {spec.severity}"
        for bundle in _profile_bundles()
        for spec in load_invariants(bundle)
        if spec.severity != "warn"
    ]
    assert not offenders, "\n".join(offenders)
