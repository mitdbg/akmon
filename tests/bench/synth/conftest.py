"""Shared fixtures for the synth benchmark witnesses.

The DB-backed e2e witness reuses the substrate publisher harness by fixture
re-export (the tests/views precedent): pytest registers fixtures found in a
conftest module's namespace, so importing them here makes ``clean_runtime``
resolvable under tests/bench/synth/.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.synth.config import SynthConfig, load_preset
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.worldschema import GroundTruthWorld
from tests.substrate.conftest import (  # noqa: F401  (fixture re-export)
    clean_runtime,
    okg_dsn,
    okg_migrated,
)

WORLD_DIR = Path(__file__).resolve().parents[3] / "src/okg/bench/synth/world_smoke"

CORE_MODULES = (
    "okg.bench.synth.prf",
    "okg.bench.synth.worldschema",
    "okg.bench.synth.fhir_map",
    "okg.bench.synth.config",
    "okg.bench.synth.corruption",
    "okg.bench.synth.render",
    "okg.bench.synth.goldseal",
    "okg.bench.synth.arms",
    "okg.bench.synth.provenance",
    # The cost layer turns gold counts into the paper's dollars. It is GOLD-side
    # arithmetic over the sealed manifest and the funnel, so it is held to the
    # same substrate-free rule as the rest of the core.
    "okg.bench.synth.cost",
    # The history generator is GOLD: it computes the reference accepted-result
    # answer per update. A reference answer that imported the engine could not
    # falsify the engine, so it is held to the same import-graph rule as the rest
    # of the core.
    "okg.bench.synth.history",
)


@pytest.fixture(scope="session")
def provenance() -> dict:
    return json.loads((WORLD_DIR / "provenance.json").read_text())


@pytest.fixture(scope="session")
def world(provenance: dict) -> GroundTruthWorld:
    return world_from_synthea_dir(
        WORLD_DIR,
        reference_date=provenance["reference_date"],
        provenance=provenance,
    )


@pytest.fixture(scope="session")
def smoke_config() -> SynthConfig:
    return load_preset("smoke")
