"""PACT view-output-carry-forward-3981: ``external_inputs`` and closed keys.

A view that reads outside the graph declares it in ``views_registry.yaml``.
The registry refuses an unknown entry key or ``external_inputs`` value by
name, and ``okg deployment lint`` and the upgrade preflight run the same
parse, so a stray key cannot stop the publisher at start.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.views import registry as registry_module
from okg.substrate.views.registry import (
    VIEW_ENTRY_KEYS,
    ViewRegistryEntry,
    load_views_registry,
    parse_views_registry,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _registry(**entry_fields) -> dict:
    return {
        "deployment": "demo",
        "views": [
            {
                "view_def_id": "demo.fleet",
                "view_def_factory": "builtins:dict",
                "build_corpus": "builtins:dict",
                **entry_fields,
            }
        ],
    }


def test_external_inputs_parse_to_the_entry() -> None:
    (entry,) = parse_views_registry(
        _registry(
            external_inputs=["publish_time", "source_runs", "live:host_now"],
        )
    )

    assert entry.external_inputs == (
        "publish_time",
        "source_runs",
        "live:host_now",
    )


def test_a_view_without_external_inputs_keeps_its_hash_and_attrs() -> None:
    declared = ViewRegistryEntry(
        view_def_id="x", view_name="X", deployment="d",
        view_def_factory="builtins:dict", build_corpus="builtins:dict",
    )
    with_input = ViewRegistryEntry(
        view_def_id="x", view_name="X", deployment="d",
        view_def_factory="builtins:dict", build_corpus="builtins:dict",
        external_inputs=("publish_time",),
    )

    assert "external_inputs" not in declared.to_node_attrs()
    assert with_input.to_node_attrs()["external_inputs"] == ["publish_time"]
    # declaring an input changes the view's identity; declaring none does not
    assert declared._hash() != with_input._hash()


def test_external_inputs_reach_the_materializer_and_the_asset_lineage() -> None:
    entry = ViewRegistryEntry(
        view_def_id="x", view_name="X", deployment="d",
        view_def_factory="builtins:dict", build_corpus="builtins:dict",
        reads_subtypes=("host",),
        external_inputs=("source_runs", "live:host_now"),
    )

    materializer = registry_module._materializer_from_attrs(
        entry.to_node_attrs()
    )
    contract = entry.to_read_asset_contract()

    assert materializer.external_inputs == ("source_runs", "live:host_now")
    assert materializer.reads_subtypes == ("host",)
    assert list(contract.lineage_sources) == [
        "subtype:host",
        "external:source_runs",
        "external:live:host_now",
    ]


def test_an_unknown_entry_key_is_refused_naming_entry_and_key() -> None:
    with pytest.raises(SubstrateError) as caught:
        parse_views_registry(_registry(external_input=["publish_time"]))

    message = str(caught.value)
    assert "'demo.fleet'" in message
    assert "external_input" in message
    assert "external_inputs" in message  # the allowed keys are listed
    assert caught.value.data["unknown_keys"] == ["external_input"]
    assert caught.value.data["view_def_id"] == "demo.fleet"


def test_an_unknown_external_input_value_is_refused_naming_the_values() -> None:
    with pytest.raises(SubstrateError) as caught:
        parse_views_registry(_registry(external_inputs=["wall_clock"]))

    message = str(caught.value)
    assert "'demo.fleet'" in message
    assert "external_inputs" in message
    assert "'wall_clock'" in message
    assert "publish_time" in message and "source_runs" in message
    assert "live:<relation>" in message


@pytest.mark.parametrize("value", ["live:", "live:has space", "live", 7])
def test_malformed_external_input_values_are_refused(value) -> None:
    with pytest.raises(SubstrateError):
        parse_views_registry(_registry(external_inputs=[value]))


def test_external_inputs_must_be_a_list() -> None:
    with pytest.raises(SubstrateError, match="must be a list"):
        parse_views_registry(_registry(external_inputs="publish_time"))


def test_every_entry_key_the_dataclass_reads_is_allowed() -> None:
    # a key the parser reads but VIEW_ENTRY_KEYS omits would be refused
    allowed = set(VIEW_ENTRY_KEYS)
    fields = {
        "view_def_id", "view_name", "view_def_factory", "build_corpus",
        "schedule", "params", "llm_client", "tracking_mode",
        "anchor_resolver", "reads_subtypes", "reads_edge_types",
        "external_inputs",
    }
    assert fields == allowed


def test_every_registry_in_the_repository_still_loads() -> None:
    paths = sorted(
        [
            *(_REPO_ROOT / "deployments").rglob("views_registry.yaml"),
            *(_REPO_ROOT / "tests" / "fixtures").rglob("views_registry.yaml"),
        ]
    )
    assert paths, "expected at least one views_registry.yaml"
    for path in paths:
        entries = load_views_registry(path.parent)
        assert isinstance(entries, list), path


def test_okg_workspace_views_are_knowingly_left_undeclared() -> None:
    """The 7 okg-workspace views that read outside the graph stay undeclared in
    this change (a follow-up issue: cost measurement on a copy, then
    declarations or a slower refresh). The audit of what each reads is in the
    PACT evidence note build-cost.md."""
    workspace = _REPO_ROOT / "deployments" / "okg-workspace"
    if not (workspace / "views_registry.yaml").is_file():
        pytest.skip("okg-workspace is not part of this copy of the repository")
    entries = load_views_registry(workspace)

    assert len(entries) == 8
    assert all(entry.external_inputs == () for entry in entries)


def _write_deployment(root: Path, registry_text: str | None) -> Path:
    from tests.substrate.cli.test_deployment_lint import _write_valid_deployment

    deployment_dir = _write_valid_deployment(root)
    if registry_text is not None:
        (deployment_dir / "views_registry.yaml").write_text(
            registry_text, encoding="utf-8"
        )
    return deployment_dir


_BAD_REGISTRY = (
    "deployment: demo\n"
    "views:\n"
    "  - view_def_id: demo.fleet\n"
    "    view_def_factory: builtins:dict\n"
    "    build_corpus: builtins:dict\n"
    "    reads_subtype: [host]\n"
)
_GOOD_REGISTRY = (
    "deployment: demo\n"
    "views:\n"
    "  - view_def_id: demo.fleet\n"
    "    view_def_factory: builtins:dict\n"
    "    build_corpus: builtins:dict\n"
    "    external_inputs: [publish_time, 'live:host_now']\n"
)


def _lint(deployment_dir: Path) -> dict:
    from okg.substrate.deployment_lint import lint_deployment

    return lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u@localhost:5432/okg_demo"},
    )


def test_deployment_lint_refuses_an_unknown_registry_key(tmp_path: Path) -> None:
    report = _lint(_write_deployment(tmp_path, _BAD_REGISTRY))

    assert report["ok"] is False
    findings = [
        finding
        for finding in report["findings"]
        if finding["code"] == "deployment.views_registry.invalid"
    ]
    assert len(findings) == 1
    assert findings[0]["severity"] == "blocker"
    assert "'demo.fleet'" in findings[0]["message"]
    assert "reads_subtype" in findings[0]["message"]


def test_deployment_lint_accepts_declared_external_inputs(tmp_path: Path) -> None:
    report = _lint(_write_deployment(tmp_path, _GOOD_REGISTRY))

    assert "deployment.views_registry.invalid" not in {
        finding["code"] for finding in report["findings"]
    }


def _preflight(monkeypatch, root: Path):
    from okg.substrate import deployment_upgrade

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    ctx = SimpleNamespace(options=SimpleNamespace(deployment="demo"))
    return deployment_upgrade._views_registry_preflight(ctx)


def test_upgrade_preflight_refuses_an_unknown_registry_key(
    tmp_path: Path, monkeypatch
) -> None:
    _write_deployment(tmp_path, _BAD_REGISTRY)

    refusal = _preflight(monkeypatch, tmp_path)

    assert refusal is not None
    assert refusal["ok"] is False
    assert "'demo.fleet'" in refusal["reason"]
    assert "reads_subtype" in refusal["reason"]


def test_upgrade_preflight_passes_a_valid_or_absent_registry(
    tmp_path: Path, monkeypatch
) -> None:
    _write_deployment(tmp_path, _GOOD_REGISTRY)
    assert _preflight(monkeypatch, tmp_path) is None


def test_upgrade_preflight_passes_without_a_registry(
    tmp_path: Path, monkeypatch
) -> None:
    _write_deployment(tmp_path, None)
    assert _preflight(monkeypatch, tmp_path) is None
