"""task.synth.world witnesses: config schema, frozen world, mapping, purity."""

from __future__ import annotations

import ast
import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from okg.bench.synth.config import ConfigError, load_config, load_preset
from okg.bench.synth.corruption import Planner
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.worldschema import WorldError, world_from_json_bytes

from tests.bench.synth.conftest import CORE_MODULES, WORLD_DIR

SRC_ROOT = Path(__file__).resolve().parents[3] / "src"


def _base_config(**overrides):
    raw = {
        "seed": "witness-1",
        "world_path": "world_smoke",
        **overrides,
    }
    return raw


# -- config schema ---------------------------------------------------------


def test_unknown_key_is_error():
    with pytest.raises(ConfigError, match="invalid synth config"):
        load_config(_base_config(fresh_seed_rule=False))


def test_tier3_contract_keys_do_not_exist():
    # determinism / leakage / purity have no schema keys: setting any is an
    # unknown-key error, which is the tier-3 fixed-contract witness.
    for key in ("determinism", "leakage_gates", "purity", "prf_derivation"):
        with pytest.raises(ConfigError):
            load_config(_base_config(**{key: False}))


def test_majority_wrong_requires_k3():
    with pytest.raises(ConfigError, match="majority-wrong"):
        load_config(
            _base_config(
                redundancy={"renderings_per_fact_min": 1, "renderings_per_fact_max": 2},
                corruption={
                    "classes": {"value_perturbation": {"count": 1, "visibility_corrupted": 2}}
                },
            )
        )


def test_visibility_beyond_k_rejected():
    with pytest.raises(ConfigError, match="exceeds renderings_per_fact_max"):
        load_config(
            _base_config(
                corruption={
                    "classes": {"value_perturbation": {"count": 1, "visibility_corrupted": 9}}
                }
            )
        )


def test_derived_value_without_fds_is_inexpressible(world, smoke_config):
    # a world stripped of derived facts cannot express derived_value
    stripped = world_from_json_bytes(world.to_json_bytes())
    object.__setattr__(
        stripped, "facts", tuple(f for f in stripped.facts if f.derived_from is None)
    )
    config = load_config(
        _base_config(corruption={"classes": {"derived_value": {"count": 1}}})
    )
    planner = Planner(stripped, config, SynthPrf(config.seed))
    with pytest.raises(Exception, match="no functional dependencies|inexpressible"):
        planner.plan()


def test_config_digest_stability_and_sensitivity(smoke_config):
    again = load_preset("smoke")
    assert smoke_config.config_digest() == again.config_digest()
    changed = load_config({**json.loads(smoke_config.canonical_json_bytes()), "seed": "other"})
    assert changed.config_digest() != smoke_config.config_digest()


def test_all_presets_validate():
    for name in ("smoke", "default", "easy", "hard"):
        config = load_preset(name)
        assert config.preset_name == name


# -- frozen world ----------------------------------------------------------


def test_world_provenance_fresh_seed(provenance):
    assert provenance["generator"] == "synthea"
    assert "fresh_seed_attestation" in provenance
    assert provenance["seed"] == 749202607
    assert provenance["raw_export_digest"] != provenance["pruned_export_digest"]
    assert "pruning_transform" in provenance
    assert len(provenance["generator_jar_sha256"]) == 64


def test_world_maps_with_fds_and_aliases(world):
    assert len(world.patients) == 8
    derived = [f for f in world.facts if f.derived_from is not None]
    assert derived, "world must declare functional dependencies"
    kinds = {f.derived_from[0] for f in derived}
    assert "age_from_birthdate" in kinds
    assert "bmi_from_height_weight" in kinds
    for patient in world.patients:
        assert patient.aliases, "every patient carries alias variants"
        assert any(c.isdigit() for c in patient.given + patient.family), (
            "Synthea digit-suffix property must hold - it is the leakage key"
        )


def test_world_roundtrip_and_digest_stable(world):
    payload = world.to_json_bytes()
    again = world_from_json_bytes(payload)
    assert again.to_json_bytes() == payload


def test_fresh_process_determinism():
    """Byte-identical world artifacts across two FRESH interpreter processes
    with distinct hash seeds (a same-process double run cannot catch
    hash-order nondeterminism)."""
    script = (
        "import json,sys,hashlib;"
        "sys.path.insert(0,'src');"
        "from okg.bench.synth.fhir_map import world_from_synthea_dir;"
        f"prov=json.loads(open('{WORLD_DIR}/provenance.json').read());"
        f"w=world_from_synthea_dir('{WORLD_DIR}',reference_date=prov['reference_date'],provenance=prov);"
        "print(hashlib.sha256(w.to_json_bytes()).hexdigest())"
    )
    digests = set()
    for hash_seed in ("0", "424242"):
        out = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            env={"PYTHONHASHSEED": hash_seed, "PATH": "/usr/bin:/bin"},
            cwd=str(Path(__file__).resolve().parents[3]),
        )
        digests.add(out.stdout.strip())
    assert len(digests) == 1, f"world digest differs across processes: {digests}"


def test_seed_disjointness(world, smoke_config):
    a = Planner(world, smoke_config, SynthPrf("seed-a")).plan()
    b = Planner(world, smoke_config, SynthPrf("seed-b")).plan()
    assert {r.corruption_id for r in a.rows} != {r.corruption_id for r in b.rows} or [
        r.corrupted_value for r in a.rows
    ] != [r.corrupted_value for r in b.rows]


# -- conformance negative control -----------------------------------------


def test_conformance_fails_on_unsatisfiable_target(world):
    """The negative control: an unsatisfiable target FAILS, never warns or
    silently under-delivers."""
    from okg.bench.synth.corruption import CorruptionError

    config = load_config(
        _base_config(
            corruption={"classes": {"value_perturbation": {"count": 10_000}}},
        )
    )
    planner = Planner(world, config, SynthPrf(config.seed))
    with pytest.raises(CorruptionError, match="exceeds eligible population"):
        planner.plan()


# -- purity ----------------------------------------------------------------


def test_slug_copies_are_byte_equal():
    """The purity boundary forces four slug COPIES (core cannot import the
    substrate grammar); this witness pins them to each other over a
    punishing name corpus so an edit to one cannot silently zero extraction
    recall (review finding L14). Tests may import both sides."""
    from okg.bench.synth.corruption import entity_slug as corruption_entity_slug
    from okg.bench.synth.fhir_map import _slug as fhir_slug
    from okg.bench.synth.goldseal import _entity_slug
    from okg.bench.synth.runner import _slug as runner_slug
    from okg.bench.injection.claim_corpus_source import (
        _slug as grammar_slug,
    )

    corpus = [
        "Abram53 Hackett68",
        "Sharen57 Barrows492 Domenica436",
        "O'Brien-Smith Jr.",
        "  Doubled  Spaces  ",
        "Ünïcode Náme77",
        "ALLCAPS99",
        "trailing-punct...",
    ]
    for name in corpus:
        assert fhir_slug(name) == runner_slug(name) == grammar_slug(name)
        # goldseal._entity_slug AND corruption.entity_slug both reproduce the
        # H1-title slug the extractors derive ("Patient {name}"): the linked_pair
        # corruption rewrites a record_reference value to this slug so it links to
        # the patient's birthdate claim, and a byte-divergent copy would zero the
        # link (the same purity-boundary discipline, pinned here so it cannot).
        assert _entity_slug(name) == grammar_slug(f"Patient {name}")
        assert corruption_entity_slug(name) == grammar_slug(f"Patient {name}")


def test_scale_patients_subsets_and_fails_closed(world, smoke_config):
    from okg.bench.synth.config import ConfigError
    from okg.bench.synth.runner import _subset_world

    smaller = _subset_world(world, 3)
    assert len(smaller.patients) == 3
    kept = {p.patient_id for p in smaller.patients}
    assert all(f.patient_id in kept for f in smaller.facts)
    assert _subset_world(world, len(world.patients)) is world
    with pytest.raises(ConfigError, match="exceeds the frozen world"):
        _subset_world(world, 999)


def test_core_imports_no_substrate():
    """Import-graph witness over the enumerated core modules (AST-level)."""
    violations = []
    for module in CORE_MODULES:
        path = SRC_ROOT / (module.replace(".", "/") + ".py")
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for name in names:
                if name.startswith("okg.substrate"):
                    violations.append(f"{module}: imports {name}")
    assert violations == []


def test_generation_is_offline(world, smoke_config, monkeypatch):
    """The core chain completes with network egress mocked to raise."""

    def _no_network(*args, **kwargs):
        raise AssertionError("synth core attempted network access")

    monkeypatch.setattr(socket.socket, "connect", _no_network)
    monkeypatch.setattr(socket, "create_connection", _no_network)
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    from okg.bench.synth.goldseal import build_manifest
    from okg.bench.synth.render import render_corpus

    rendered = render_corpus(world, smoke_config, planner, plan, prf)
    manifest = build_manifest(
        config=smoke_config, world=world, plan=plan, rendered=rendered
    )
    assert manifest["schema"] == "okg.synth.manifest.v1"


def test_world_error_on_missing_dir(provenance):
    from okg.bench.synth.fhir_map import world_from_synthea_dir

    with pytest.raises(WorldError, match="not a directory"):
        world_from_synthea_dir(
            "does/not/exist", reference_date="2026-07-01", provenance=provenance
        )
