"""Catalog composition installs a deployment's root ``invariants.yaml``.

A profile's invariant bundle is copied verbatim to ``<deployment>/
invariants.yaml`` by ``okg init``; before ``compose-root-invariants``
the composer read only ``invariants/*.yaml``, so profile-shipped gates
were never installed and the publisher evaluated none of them. These
tests pin the union, the scaffold default, duplicate collapsing, the
conflict refusal, and that the wiring resolver's gate components come
from the same file list the composer installs.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.invariants import (
    discover_invariant_files,
    load_deployment_invariants,
)
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import SubstrateError


def _invariant(name: str, *, severity: str = "error") -> dict:
    return {
        "name": name,
        "severity": severity,
        "scope": "cross",
        "sql": f"SELECT 1 AS {name} WHERE false",
    }


def _deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "dep"
    (dep / "schemas").mkdir(parents=True)
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas", "modules": [],
    }))
    return dep


def _write(path: Path, invariants: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"invariants": invariants}))


def test_compose_installs_root_invariants_file(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    _write(dep / "invariants.yaml", [_invariant("profile_floor")])

    composed = compose_catalog(dep)

    assert [inv.name for inv in composed.invariants] == ["profile_floor"]
    assert composed.invariants[0].scope == "cross"


def test_compose_unions_root_file_and_directory(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    _write(dep / "invariants.yaml", [_invariant("from_root")])
    _write(dep / "invariants" / "b.yaml", [_invariant("from_dir_b")])
    _write(dep / "invariants" / "a.yaml", [_invariant("from_dir_a")])

    composed = compose_catalog(dep)

    assert [inv.name for inv in composed.invariants] == [
        "from_root", "from_dir_a", "from_dir_b",
    ]
    assert discover_invariant_files(dep) == [
        dep / "invariants.yaml",
        dep / "invariants" / "a.yaml",
        dep / "invariants" / "b.yaml",
    ]


def test_scaffold_default_root_file_contributes_nothing(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    (dep / "invariants.yaml").write_text("invariants: []\n")
    _write(dep / "invariants" / "core.yaml", [_invariant("core_gate")])

    composed = compose_catalog(dep)

    assert [inv.name for inv in composed.invariants] == ["core_gate"]


def test_identical_duplicate_across_files_is_kept_once(tmp_path: Path) -> None:
    # The LLM bootstrap writes the same bundle to invariants.yaml and
    # invariants/bootstrap.yaml; the union must not double-install it.
    dep = _deployment(tmp_path)
    _write(dep / "invariants.yaml", [_invariant("bootstrap_gate")])
    _write(dep / "invariants" / "bootstrap.yaml", [_invariant("bootstrap_gate")])

    specs = load_deployment_invariants(dep)

    assert [inv.name for inv in specs] == ["bootstrap_gate"]


def test_conflicting_duplicate_across_files_is_refused(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    _write(dep / "invariants.yaml", [_invariant("gate", severity="error")])
    _write(dep / "invariants" / "core.yaml", [_invariant("gate", severity="warn")])

    with pytest.raises(SubstrateError) as excinfo:
        compose_catalog(dep)

    message = str(excinfo.value)
    assert "'gate'" in message
    assert "invariants.yaml" in message
    assert "core.yaml" in message


def test_no_invariant_files_composes_empty(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)

    assert discover_invariant_files(dep) == []
    assert compose_catalog(dep).invariants == []


@pytest.mark.parametrize(
    "profile", ["workspace-graph", "academic-researcher"],
)
def test_library_profile_bundle_composes_from_deployment_root(
    tmp_path: Path, profile: str,
) -> None:
    """The exact file `okg init` copies to the deployment root loads
    through the composer with at least one invariant, so a scaffolded
    deployment installs what its profile ships."""
    import okg.substrate.library as _library

    bundle = Path(_library.__file__).parent / "profiles" / profile / "invariants.yaml"
    assert bundle.is_file(), bundle
    dep = _deployment(tmp_path)
    (dep / "invariants.yaml").write_bytes(bundle.read_bytes())

    composed = compose_catalog(dep)

    assert composed.invariants, f"{profile} bundle composed to nothing"
    assert all(inv.scope in {"cross", "source"} for inv in composed.invariants)


def test_wiring_resolver_gates_match_composed_invariants(tmp_path: Path) -> None:
    from okg.substrate.deployment_wiring import resolve_deployment_wiring_plan

    dep = _deployment(tmp_path)
    _write(dep / "invariants.yaml", [_invariant("from_root")])
    _write(dep / "invariants" / "core.yaml", [_invariant("from_dir")])
    (dep / "source_registry.yaml").write_text("sources: {}\n")

    plan = resolve_deployment_wiring_plan(dep)
    gate_ids = {
        component.component_id
        for component in plan.gates
        if component.kind == "invariant_gate"
    }
    composed = {inv.name for inv in compose_catalog(dep).invariants}

    assert gate_ids == composed == {"from_root", "from_dir"}
