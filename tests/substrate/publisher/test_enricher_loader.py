"""Per `wire-deployment-enrichers` (2026-04-26):
`load_enrichers` reads `deployment.yaml`'s `enrichers:` block,
instantiates each, returns the ordered list.

Tests:
- good YAML → list of instances;
- missing `enrichers:` block → empty list;
- bad class path → EnricherLoadError;
- constructor failure → chain-level error;
- malformed YAML shapes (`enrichers:` not a list, entry
  without `class:`) raise structured.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from okg.substrate.publisher.enricher_loader import (
    EnricherLoadError,
    active_enricher_profiles_from_env,
    load_enrichers,
)


def _write_manifest(tmp_path: Path, doc: dict) -> Path:
    out = tmp_path / "dep"
    out.mkdir()
    (out / "deployment.yaml").write_text(yaml.safe_dump(doc))
    return out


_LINKER = "okg.substrate.library.linkers.declarative.DeclarativeLinker"
_CLAIM_EXTRACTOR = (
    "okg.substrate.library.enrichers.extraction.claim_extractor.ClaimExtractor"
)


def _profile_manifest(tmp_path: Path) -> Path:
    """One untagged enricher + one tagged `profile: evaluation`."""
    empty = tmp_path / "empty_rules"
    empty.mkdir()
    return _write_manifest(tmp_path, {
        "name": "test",
        "enrichers": [
            {"class": _LINKER, "config": {"rules_dir": str(empty)}},
            {"class": _LINKER, "config": {"rules_dir": str(empty)},
             "profile": "evaluation"},
        ],
    })


def test_good_yaml_instantiates_in_order(tmp_path: Path) -> None:
    # Empty rules_dir keeps construction hermetic — exercises the
    # constructor path without loading the substrate-shipped rules.
    empty = tmp_path / "empty_rules"
    empty.mkdir()
    dep = _write_manifest(tmp_path, {
        "name": "test",
        "enrichers": [
            {
                "class": (
                    "okg.substrate.library.linkers."
                    "declarative.DeclarativeLinker"
                ),
                "config": {"rules_dir": str(empty)},
            },
            {
                "class": (
                    "okg.substrate.library.linkers."
                    "declarative.DeclarativeLinker"
                ),
                "config": {"rules_dir": str(empty)},
            },
        ],
    })
    enrichers = load_enrichers(dep)
    assert len(enrichers) == 2
    assert enrichers[0].rules == []
    assert enrichers[1].rules == []


def test_deployment_local_class_path_evicts_stale_package(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    saved_path = list(sys.path)
    try:
        for deployment, value in ((first, "first"), (second, "second")):
            package = deployment / "enrichers"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("")
            (package / "local.py").write_text(
                "class LocalEnricher:\n"
                f"    deployment = {value!r}\n",
            )
            (deployment / "deployment.yaml").write_text(yaml.safe_dump({
                "name": value,
                "enrichers": [
                    {"class": "enrichers.local.LocalEnricher"},
                ],
            }))

        assert load_enrichers(first)[0].deployment == "first"
        assert load_enrichers(second)[0].deployment == "second"
    finally:
        sys.path[:] = saved_path
        for name in list(sys.modules):
            if name == "enrichers" or name.startswith("enrichers."):
                sys.modules.pop(name, None)


def test_missing_enrichers_block_returns_empty(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {"name": "test"})
    assert load_enrichers(dep) == []


def test_no_manifest_returns_empty(tmp_path: Path) -> None:
    dep = tmp_path / "empty"
    dep.mkdir()
    assert load_enrichers(dep) == []


def test_bad_class_path_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "enrichers": [
            {"class": "okg.does.not.exist.Class"},
        ],
    })
    with pytest.raises(EnricherLoadError) as exc_info:
        load_enrichers(dep)
    err = exc_info.value
    assert err.data["class"] == "okg.does.not.exist.Class"


def test_malformed_class_path_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "enrichers": [
            {"class": "no_dots_here"},
        ],
    })
    with pytest.raises(EnricherLoadError) as exc_info:
        load_enrichers(dep)
    assert "no_dots_here" in str(exc_info.value)


def test_entry_without_class_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "enrichers": [
            {"config": {}},
        ],
    })
    with pytest.raises(EnricherLoadError) as exc_info:
        load_enrichers(dep)
    assert "class" in str(exc_info.value)


def test_enrichers_not_a_list_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "enrichers": "not-a-list",
    })
    with pytest.raises(EnricherLoadError):
        load_enrichers(dep)


def test_profile_none_loads_all(tmp_path: Path) -> None:
    """active_profiles=None disables gating — validation/inspection (lint,
    doctor) see the full pipeline including opt-in enrichers."""
    dep = _profile_manifest(tmp_path)
    assert len(load_enrichers(dep)) == 2
    assert len(load_enrichers(dep, active_profiles=None)) == 2


def test_profiled_enricher_gated_off_by_default(tmp_path: Path) -> None:
    """With an empty active set, a `profile:`-tagged enricher does NOT load;
    the untagged one still does (existing deployments unaffected)."""
    dep = _profile_manifest(tmp_path)
    enrichers = load_enrichers(dep, active_profiles=set())
    assert len(enrichers) == 1


def test_profiled_enricher_loads_when_active(tmp_path: Path) -> None:
    """Naming the profile in the active set opts the enricher in."""
    dep = _profile_manifest(tmp_path)
    assert len(load_enrichers(dep, active_profiles={"evaluation"})) == 2
    # an unrelated active profile does not opt it in
    assert len(load_enrichers(dep, active_profiles={"other"})) == 1


def test_active_profiles_from_env(monkeypatch) -> None:
    monkeypatch.delenv("OKG_ENRICHER_PROFILES", raising=False)
    assert active_enricher_profiles_from_env() == set()
    monkeypatch.setenv("OKG_ENRICHER_PROFILES", "evaluation, extra ,")
    assert active_enricher_profiles_from_env() == {"evaluation", "extra"}


def test_constructor_typeerror_lifts_to_chain(tmp_path: Path) -> None:
    """A bad config kwarg surfaces as a chain-level error
    rather than a raw TypeError."""
    dep = _write_manifest(tmp_path, {
        "enrichers": [
            {
                "class": (
                    "okg.substrate.library.linkers."
                    "declarative.DeclarativeLinker"
                ),
                "config": {"unknown_kwarg": "x"},
            },
        ],
    })
    with pytest.raises(EnricherLoadError) as exc_info:
        load_enrichers(dep)
    err = exc_info.value
    assert "unknown_kwarg" in err.data["config_keys"]
    assert isinstance(err.__cause__, TypeError)


def test_model_backed_extractor_missing_retry_limit_is_loader_error(
    tmp_path: Path,
) -> None:
    dep = _write_manifest(
        tmp_path,
        {
            "name": "test",
            "enrichers": [
                {
                    "class": _CLAIM_EXTRACTOR,
                    "config": {
                        "confidence_floor": 0.6,
                        "llm_model": "fixture/model",
                    },
                }
            ],
        },
    )
    with pytest.raises(EnricherLoadError) as exc_info:
        load_enrichers(dep)
    assert "input_quarantine_max_attempts" in str(exc_info.value.__cause__)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_extractor_without_model_does_not_invent_retry_limit(tmp_path: Path) -> None:
    dep = _write_manifest(
        tmp_path,
        {
            "name": "test",
            "enrichers": [
                {
                    "class": _CLAIM_EXTRACTOR,
                    "config": {"confidence_floor": 0.6},
                }
            ],
        },
    )
    enricher = load_enrichers(dep)[0]
    assert enricher.llm is None
    assert enricher.input_quarantine_max_attempts is None
