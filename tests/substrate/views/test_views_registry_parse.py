"""Coverage tests for `okg.substrate.views.registry` — the
parsing + helper functions. The DB-touching `apply_views_registry`
happy path is exercised at substrate-integration level by
existing tests; this file targets pure paths."""
from __future__ import annotations

import importlib
import json
import sys
from types import ModuleType
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.views.registry import (
    ViewRegistryEntry,
    _build_llm_client,
    _resolve_attr,
    load_views_registry,
    materializers_from_registry_entries,
    parse_views_registry,
)


# ---------- ViewRegistryEntry shape ----------


def test_entry_to_node_attrs_round_trips_fields() -> None:
    entry = ViewRegistryEntry(
        view_def_id="af_pubs.x",
        view_name="X",
        deployment="af_pubs",
        view_def_factory="m:f",
        build_corpus="m:c",
        schedule="0 * * * *",
        params={"k": "v"},
        llm_client="stub",
        anchor_resolver="m:r",
    )
    attrs = entry.to_node_attrs()
    assert attrs["view_def_id"] == "af_pubs.x"
    assert attrs["params"] == {"k": "v"}
    assert attrs["schedule"] == "0 * * * *"
    assert attrs["llm_client"] == "stub"
    assert attrs["anchor_resolver_ref"] == "m:r"
    assert "view_def_hash" in attrs
    # Hash is deterministic.
    h1 = entry._hash()
    h2 = entry._hash()
    assert h1 == h2


def test_entry_hash_changes_with_params() -> None:
    e1 = ViewRegistryEntry(
        view_def_id="x", view_name="X", deployment="d",
        view_def_factory="m:f", build_corpus="m:c",
        params={"a": 1},
    )
    e2 = ViewRegistryEntry(
        view_def_id="x", view_name="X", deployment="d",
        view_def_factory="m:f", build_corpus="m:c",
        params={"a": 2},
    )
    assert e1._hash() != e2._hash()


# ---------- parse_views_registry ----------


def test_parse_minimal_valid_entry() -> None:
    entries = parse_views_registry({
        "deployment": "af_pubs",
        "views": [
            {
                "view_def_id": "af_pubs.x",
                "view_def_factory": "views.x:factory",
                "build_corpus": "views.x:corpus",
            },
        ],
    })
    assert len(entries) == 1
    assert entries[0].view_def_id == "af_pubs.x"
    assert entries[0].deployment == "af_pubs"
    assert entries[0].view_name == "af_pubs.x"  # default to view_def_id
    assert entries[0].llm_client == "stub"


def test_parse_full_entry() -> None:
    entries = parse_views_registry({
        "deployment": "af_pubs",
        "views": [
            {
                "view_def_id": "af_pubs.x",
                "view_name": "Eks",
                "view_def_factory": "views.x:factory",
                "build_corpus": "views.x:corpus",
                "schedule": "0 * * * *",
                "params": {"a": 1},
                "llm_client": "litellm:gpt-4o",
                "anchor_resolver": "views.x:anchor",
            },
        ],
    })
    e = entries[0]
    assert e.view_name == "Eks"
    assert e.schedule == "0 * * * *"
    assert e.params == {"a": 1}
    assert e.llm_client == "litellm:gpt-4o"
    assert e.anchor_resolver == "views.x:anchor"


def test_parse_reads_declarations_round_trip() -> None:
    """reads_subtypes/reads_edge_types parse to tuples and round-trip into the
    view_definition node attrs (which _materializer_from_attrs reads back)."""
    entries = parse_views_registry({
        "deployment": "af_pubs",
        "views": [
            {
                "view_def_id": "af_pubs.calls",
                "view_def_factory": "views.x:factory",
                "build_corpus": "views.x:corpus",
                "reads_subtypes": ["code_symbol"],
                "reads_edge_types": ["calls"],
            },
        ],
    })
    e = entries[0]
    assert e.reads_subtypes == ("code_symbol",)
    assert e.reads_edge_types == ("calls",)
    attrs = e.to_node_attrs()
    assert attrs["reads_subtypes"] == ["code_symbol"]
    assert attrs["reads_edge_types"] == ["calls"]


def test_parse_defaults_reads_to_empty() -> None:
    entries = parse_views_registry({
        "deployment": "af_pubs",
        "views": [
            {
                "view_def_id": "af_pubs.x",
                "view_def_factory": "views.x:factory",
                "build_corpus": "views.x:corpus",
            },
        ],
    })
    assert entries[0].reads_subtypes == ()
    assert entries[0].reads_edge_types == ()


def test_parse_rejects_non_list_reads_subtypes() -> None:
    with pytest.raises(SubstrateError, match="reads_subtypes.*list"):
        parse_views_registry({
            "deployment": "af_pubs",
            "views": [
                {
                    "view_def_id": "af_pubs.x",
                    "view_def_factory": "views.x:factory",
                    "build_corpus": "views.x:corpus",
                    "reads_subtypes": "code_symbol",
                },
            ],
        })


def test_parse_rejects_empty_string_in_reads_edge_types() -> None:
    with pytest.raises(SubstrateError, match="reads_edge_types.*list"):
        parse_views_registry({
            "deployment": "af_pubs",
            "views": [
                {
                    "view_def_id": "af_pubs.x",
                    "view_def_factory": "views.x:factory",
                    "build_corpus": "views.x:corpus",
                    "reads_edge_types": ["calls", ""],
                },
            ],
        })


def test_entry_hash_changes_with_reads() -> None:
    base = dict(
        view_def_id="x", view_name="X", deployment="d",
        view_def_factory="m:f", build_corpus="m:c",
    )
    e1 = ViewRegistryEntry(**base)
    e2 = ViewRegistryEntry(**base, reads_edge_types=("calls",))
    # A newly-declared edge dependency must bust fragment-cache reuse.
    assert e1._hash() != e2._hash()


def test_parse_disabled_registry_returns_no_entries() -> None:
    entries = parse_views_registry({
        "deployment": "af_pubs",
        "enabled": False,
        "views": [
            {
                "view_def_id": "af_pubs.x",
                "view_def_factory": "views.x:factory",
                "build_corpus": "views.x:corpus",
            },
        ],
    })
    assert entries == []


def test_parse_rejects_non_boolean_enabled() -> None:
    with pytest.raises(SubstrateError, match="enabled.*boolean"):
        parse_views_registry({
            "deployment": "af_pubs",
            "enabled": "false",
            "views": [],
        })


def test_parse_rejects_non_dict_top_level() -> None:
    with pytest.raises(SubstrateError, match="top-level must be a mapping"):
        parse_views_registry([])


def test_parse_rejects_missing_deployment() -> None:
    with pytest.raises(SubstrateError, match="deployment:"):
        parse_views_registry({"views": []})


def test_parse_rejects_non_list_views() -> None:
    with pytest.raises(SubstrateError, match="must be a list"):
        parse_views_registry({"deployment": "x", "views": "not a list"})


def test_parse_rejects_non_dict_view_entry() -> None:
    with pytest.raises(SubstrateError, match="must be a mapping"):
        parse_views_registry({
            "deployment": "x",
            "views": ["not a dict"],
        })


def test_parse_rejects_missing_required_field() -> None:
    with pytest.raises(SubstrateError, match="missing required field"):
        parse_views_registry({
            "deployment": "x",
            "views": [
                {"view_def_id": "x.y"},  # missing factory + corpus
            ],
        })


def test_parse_rejects_duplicate_view_def_id() -> None:
    with pytest.raises(SubstrateError, match="duplicate"):
        parse_views_registry({
            "deployment": "x",
            "views": [
                {
                    "view_def_id": "x.y",
                    "view_def_factory": "m:f",
                    "build_corpus": "m:c",
                },
                {
                    "view_def_id": "x.y",
                    "view_def_factory": "m:f2",
                    "build_corpus": "m:c2",
                },
            ],
        })


def test_parse_rejects_malformed_import_ref() -> None:
    with pytest.raises(SubstrateError, match="module:attribute"):
        parse_views_registry({
            "deployment": "x",
            "views": [
                {
                    "view_def_id": "x.y",
                    "view_def_factory": "no_colon",
                    "build_corpus": "m:c",
                },
            ],
        })


def test_parse_rejects_malformed_anchor_resolver() -> None:
    with pytest.raises(SubstrateError, match="anchor_resolver"):
        parse_views_registry({
            "deployment": "x",
            "views": [
                {
                    "view_def_id": "x.y",
                    "view_def_factory": "m:f",
                    "build_corpus": "m:c",
                    "anchor_resolver": "bogus_no_colon",
                },
            ],
        })


def test_parse_empty_anchor_resolver_allowed() -> None:
    entries = parse_views_registry({
        "deployment": "x",
        "views": [
            {
                "view_def_id": "x.y",
                "view_def_factory": "m:f",
                "build_corpus": "m:c",
                "anchor_resolver": "",
            },
        ],
    })
    assert entries[0].anchor_resolver == ""


# ---------- load_views_registry (file IO) ----------


def test_load_returns_empty_when_file_absent(tmp_path: Path) -> None:
    """A deployment without a views_registry.yaml is valid."""
    assert load_views_registry(tmp_path) == []


def test_load_parses_real_file(tmp_path: Path) -> None:
    (tmp_path / "views_registry.yaml").write_text(
        "deployment: af_pubs\n"
        "views:\n"
        "  - view_def_id: af_pubs.x\n"
        "    view_def_factory: m:f\n"
        "    build_corpus: m:c\n"
    )
    entries = load_views_registry(tmp_path)
    assert len(entries) == 1
    assert entries[0].view_def_id == "af_pubs.x"


def test_load_raises_on_bad_yaml(tmp_path: Path) -> None:
    (tmp_path / "views_registry.yaml").write_text(
        "deployment: af_pubs\nviews: [bad: yaml\n"
    )
    with pytest.raises(SubstrateError, match="YAML parse failed"):
        load_views_registry(tmp_path)


# ---------- _resolve_attr ----------


def test_resolve_attr_returns_module_member() -> None:
    """`json:dumps` resolves to the json module's dumps function."""
    fn = _resolve_attr("json:dumps", context="test")
    assert fn is json.dumps


def test_resolve_attr_invalidates_import_caches(monkeypatch) -> None:
    calls = 0
    real_invalidate = importlib.invalidate_caches

    def record_invalidation() -> None:
        nonlocal calls
        calls += 1
        real_invalidate()

    monkeypatch.setattr(importlib, "invalidate_caches", record_invalidation)

    fn = _resolve_attr("json:dumps", context="test")

    assert fn is json.dumps
    assert calls == 1


def test_materializer_discovery_reloads_same_deployment_modules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment_root = tmp_path / "demo"
    views_dir = deployment_root / "views"
    views_dir.mkdir(parents=True)
    (views_dir / "__init__.py").write_text("")
    (views_dir / "hot.py").write_text(
        "def factory():\n"
        "    return 'fresh'\n\n"
        "def build_corpus(conn, *, generation_id):\n"
        "    return {}\n"
    )

    stale_pkg = ModuleType("views")
    stale_pkg.__file__ = str(views_dir / "__init__.py")
    stale_pkg.__path__ = [str(views_dir)]
    stale_mod = ModuleType("views.hot")
    stale_mod.__file__ = str(views_dir / "hot.py")
    stale_mod.factory = lambda: "stale"
    stale_mod.build_corpus = lambda conn, *, generation_id: {}
    monkeypatch.setitem(sys.modules, "views", stale_pkg)
    monkeypatch.setitem(sys.modules, "views.hot", stale_mod)

    materializers = materializers_from_registry_entries(
        [
            ViewRegistryEntry(
                view_def_id="demo.hot",
                view_name="Hot",
                deployment="demo",
                view_def_factory="views.hot:factory",
                build_corpus="views.hot:build_corpus",
            )
        ],
        deployment_name="demo",
        deployment_root=deployment_root,
    )

    assert materializers[0].view_def_factory() == "fresh"


def test_resolve_attr_rejects_malformed_ref() -> None:
    with pytest.raises(SubstrateError, match="malformed import ref"):
        _resolve_attr("no_colon", context="test")


def test_resolve_attr_rejects_unknown_module() -> None:
    with pytest.raises(SubstrateError, match="not found"):
        _resolve_attr("totally_bogus_module_xyz:foo", context="test")


def test_resolve_attr_rejects_unknown_attribute() -> None:
    with pytest.raises(SubstrateError, match="no attribute"):
        _resolve_attr("json:not_an_attribute", context="test")


# ---------- _build_llm_client ----------


def test_build_llm_client_default_returns_stub() -> None:
    from okg.views.execution.llm import DeterministicStubLLM
    client = _build_llm_client("stub")
    assert isinstance(client, DeterministicStubLLM)


def test_build_llm_client_empty_returns_stub() -> None:
    from okg.views.execution.llm import DeterministicStubLLM
    assert isinstance(_build_llm_client(""), DeterministicStubLLM)
    assert isinstance(_build_llm_client(None), DeterministicStubLLM)


def test_build_llm_client_none_returns_none() -> None:
    assert _build_llm_client("none") is None


def test_build_llm_client_litellm_requires_model() -> None:
    with pytest.raises(SubstrateError, match="requires a model"):
        _build_llm_client("litellm:")


def test_build_llm_client_litellm_uses_timeout_env(monkeypatch) -> None:
    from okg.views.execution.llm import LiteLLMClient

    monkeypatch.setenv("OKG_VIEW_LLM_TIMEOUT_SECONDS", "2.5")
    client = _build_llm_client("litellm:gpt-4o")
    assert isinstance(client, LiteLLMClient)
    assert client._timeout_s == 2.5


def test_build_llm_client_litellm_rejects_bad_timeout_env(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_VIEW_LLM_TIMEOUT_SECONDS", "nope")
    with pytest.raises(SubstrateError, match="must be a number"):
        _build_llm_client("litellm:gpt-4o")


def test_build_llm_client_unknown_spec_raises() -> None:
    with pytest.raises(SubstrateError, match="unknown llm_client"):
        _build_llm_client("ollama:llama3")
