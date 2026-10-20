from __future__ import annotations

import importlib.util
from pathlib import Path

from _v5_formal import carried_bindings


REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGE_ID = "graph-native-pact-projection"
MODEL_PATH = REPO_ROOT / "formal" / "pact" / "PactProjection.tla"
CONFIG_PATH = REPO_ROOT / "formal" / "pact" / "PactProjection.cfg"
CHECKER_PATH = REPO_ROOT / "scripts" / "check_formal_pact_projection.py"
REFINEMENT_TEST = (
    "tests/pact/test_pact_projection_formal.py::"
    "test_pact_projection_formal_bindings_cover_graph_native_contract"
)


def _manifest_path(change_id: str) -> Path:
    for base in ["changes", "archive"]:
        candidate = REPO_ROOT / "pact" / base / change_id / "pact.yaml"
        if candidate.is_file():
            return candidate
    raise AssertionError(f"PACT manifest not found: {change_id}")


def _checker_properties() -> set[str]:
    spec = importlib.util.spec_from_file_location(
        "check_formal_pact_projection",
        CHECKER_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {property_id for property_id, _summary in module.PROPERTIES}


def test_pact_projection_formal_bindings_cover_graph_native_contract() -> None:
    properties = _checker_properties()
    model_text = MODEL_PATH.read_text(encoding="utf-8")
    config_text = CONFIG_PATH.read_text(encoding="utf-8")

    assert "PactProjectionContract" in properties
    assert "PactProjectionContract ==" in model_text
    assert "PactProjectionContract" in config_text

    # v5 has no `formal[]`: `pact migrate` carried each binding into a
    # `formal_check` evidence row naming the property, the module and the
    # checker command. `config` and `refinement_tests` were not carried, so
    # they are asserted against the files themselves below instead of being
    # restated by the manifest.
    formal = {
        property_id: binding
        for property_id, binding in carried_bindings(CHANGE_ID).items()
        if binding.module == "formal/pact/PactProjection.tla"
    }
    assert set(formal) >= {"PactProjectionContract"}
    binding = formal["PactProjectionContract"]
    assert binding.requirement == "req.formal-pact-projection-model"
    assert binding.checker_command == (
        "uv run python scripts/check_formal_pact_projection.py "
        "--out .scratch/formal/pact-projection"
    )
    assert (REPO_ROOT / REFINEMENT_TEST.partition("::")[0]).is_file()

    for property_id in properties:
        assert property_id in model_text
        assert property_id in config_text
