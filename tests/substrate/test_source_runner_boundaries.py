from __future__ import annotations

import ast
import builtins
from datetime import datetime, timezone
import inspect
from dataclasses import fields, is_dataclass
from pathlib import Path
from unittest.mock import patch

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest import runner
from okg.substrate.ingest.runner import (
    GraphWriteSet,
    GraphWriteSetAccumulator,
    SourceRunner,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe


def _module_tree() -> ast.Module:
    source_path = Path(inspect.getsourcefile(SourceRunner) or "")
    return ast.parse(source_path.read_text())


def _function_node(tree: ast.AST, name: str) -> ast.FunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"missing function node {name!r}")


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        if isinstance(node.func, ast.Name):
            return node.func.id
    return None


def test_source_runner_boundary_dataclasses_are_named() -> None:
    expected = {
        runner.RunPrelude: [
            "prior_cursor",
            "prior_progress",
            "no_prior_facts",
            "replay_required",
            "persisted_cursor",
        ],
        runner.FactLogRows: [
            "node_rows",
            "edge_rows",
            "marker_rows",
            "nodes_inserted",
            "nodes_retracted",
            "edges_inserted",
            "edges_retracted",
        ],
        runner.GraphWriteSet: [
            "nodes_by_subtype",
            "node_attr_changes_by_subtype",
            "edges_by_type",
            "edge_table_available",
            "timings_ms",
        ],
        runner.FlushCounts: [
            "nodes_inserted",
            "nodes_retracted",
            "edges_inserted",
            "edges_retracted",
        ],
        runner.PersistedDedupeDetail: [
            "revision_only_nodes",
            "revision_only_edges",
            "kept_by_key",
        ],
        runner.DedupeDropSummary: [
            "in_chunk_nodes",
            "in_chunk_edges",
            "persisted_nodes",
            "persisted_edges",
            "first_dropped",
            "by_prefix",
            "persisted_detail",
        ],
        runner.FlushResult: [
            "counts",
            "dedupe",
            "graph_write_set",
            "sanitize",
        ],
    }
    for dataclass_type, field_names in expected.items():
        assert is_dataclass(dataclass_type)
        assert [field.name for field in fields(dataclass_type)] == field_names


def test_source_runner_boundary_helpers_are_explicit() -> None:
    helper_names = [
        "_authorize_source_ingest_boundaries",
        "_authorize_source_emit_boundary",
        "_load_cursor_progress_boundary",
        "_advance_cursor_watermark_boundary",
        "_build_fact_log_rows",
        "_build_progress_marker_rows",
        "_persist_fact_log_chunk",
        "_apply_chronos_facts",
    ]
    for helper_name in helper_names:
        assert callable(getattr(SourceRunner, helper_name))


def test_flush_chunk_returns_named_result_not_positional_tuple() -> None:
    annotation = inspect.signature(SourceRunner._flush_chunk).return_annotation
    assert annotation in {"FlushResult", runner.FlushResult}

    flush_node = _function_node(_module_tree(), "_flush_chunk")
    returns = [node for node in ast.walk(flush_node) if isinstance(node, ast.Return)]
    assert returns
    assert all(not isinstance(node.value, ast.Tuple) for node in returns)
    assert any(_call_name(node.value) == "FlushResult" for node in returns)


def test_do_flush_does_not_destructure_flush_chunk_tuple() -> None:
    do_flush_node = _function_node(_module_tree(), "_do_flush")
    for node in ast.walk(do_flush_node):
        if not isinstance(node, ast.Assign):
            continue
        if _call_name(node.value) != "_flush_chunk":
            continue
        assert all(not isinstance(target, ast.Tuple) for target in node.targets)


def test_bounded_graph_write_set_degrades_without_leaking_partial_hint() -> None:
    accumulator = GraphWriteSetAccumulator(max_entries=4)
    accumulator.observe(
        GraphWriteSet(
            nodes_by_subtype={"document": ["n1"]},
            node_attr_changes_by_subtype={"document": {"n1": ["title"]}},
            edges_by_type={"contains": [1]},
        )
    )
    assert accumulator.available is True
    assert accumulator.retained_entries == 4
    assert accumulator.nodes_by_subtype == {"document": ["n1"]}

    accumulator.observe(
        GraphWriteSet(nodes_by_subtype={"document": ["n2"]})
    )
    assert accumulator.available is False
    assert accumulator.retained_entries == 0
    assert accumulator.nodes_by_subtype == {}
    assert accumulator.node_attr_changes_by_subtype == {}
    assert accumulator.edges_by_type == {}

    accumulator.observe(
        GraphWriteSet(nodes_by_subtype={"document": ["must-not-return"]})
    )
    assert accumulator.nodes_by_subtype == {}


def test_audited_unique_result_omits_graph_write_set_above_copy_boundary() -> None:
    accumulator = GraphWriteSetAccumulator(
        max_entries=runner.MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES
    )
    accumulator.observe(GraphWriteSet(nodes_by_subtype={
        "document": [
            f"n{index}"
            for index in range(runner.MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES + 1)
        ]
    }))
    assert accumulator.available is False
    assert accumulator.retained_entries == 0

    now = datetime.now(timezone.utc)
    result = runner.RunResult(
        run_id="00000000-0000-0000-0000-000000000001",
        source_name="ancient-greece.zenodo-10060888",
        mode="release_new",
        started_at=now,
        finished_at=now,
        nodes_appended=runner.MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES + 1,
        edges_appended=0,
        retractions_node=0,
        retractions_edge=0,
        graph_nodes_by_subtype={
            subtype: tuple(node_ids)
            for subtype, node_ids in accumulator.nodes_by_subtype.items()
        },
        graph_node_attr_changes_by_subtype={},
        graph_edges_by_type={},
        graph_edge_table_available=accumulator.edge_table_available,
    )
    assert result.graph_write_set_as_dict() is None
    assert "graph_write_set" not in result.as_dict()


_AUDITED_EMIT_VERSION = (
    "0b95d92eef51b61a9bc1f776abd7ead5b341c8e9806e12c9ce5e688514a02e0d"
)
_MISSING = object()


def _content_hash_probe(
    emit_version: object = _AUDITED_EMIT_VERSION,
) -> ContentHashProbe:
    with patch(
        "okg.substrate.library.sources.content_hash_probe.emit_version_for",
        return_value=emit_version,
    ):
        return ContentHashProbe(
            content_items=lambda: (),
            config={},
            emit_targets=object(),
        )


def _identity_policy_runner(
    *,
    registry_name: str = "archaeos-tuff",
    ownership_id: str = "ancient-greece.zenodo-10060888",
    executable_profile: str = "reference_catalog",
    registry_binding_fingerprint: str = "test-binding",
) -> SourceRunner:
    return SourceRunner(
        "unused",
        source_name="ancient-greece.zenodo-10060888",
        executable_profile=executable_profile,
        registry_name=registry_name,
        ownership_id=ownership_id,
        registry_binding_fingerprint=registry_binding_fingerprint,
    )


def _identity_policy_adapter(change_probe: object = _MISSING) -> object:
    attributes = {
        "__module__": "sources.adapters.science.archaeos_tuff",
        "run_wide_identity_policy": "adapter_guaranteed_unique",
    }
    if change_probe is not _MISSING:
        attributes["change_probe"] = change_probe
    adapter_type = type("ArchaeosTuffSource", (), attributes)
    return adapter_type()


def test_run_wide_identity_policy_defaults_to_runner_retention() -> None:
    class ExplicitRunnerDedupe:
        run_wide_identity_policy = "runner_dedup"

    runner_instance = _identity_policy_runner()
    assert runner_instance._retain_run_seen_identities(object()) is True
    assert runner_instance._retain_run_seen_identities(ExplicitRunnerDedupe()) is True


def test_run_wide_identity_policy_refuses_unknown_and_unregistered_opt_out() -> None:
    class UnknownPolicy:
        run_wide_identity_policy = "unknown"

    class UnregisteredUnique:
        run_wide_identity_policy = "adapter_guaranteed_unique"

    runner_instance = _identity_policy_runner()
    with pytest.raises(SubstrateError, match="unknown run-wide identity policy"):
        runner_instance._retain_run_seen_identities(UnknownPolicy())
    with pytest.raises(SubstrateError, match="is not registered"):
        runner_instance._retain_run_seen_identities(UnregisteredUnique())


def test_run_wide_identity_policy_rejects_coercible_non_string() -> None:
    class CoerciblePolicy:
        def __str__(self) -> str:
            return "adapter_guaranteed_unique"

    class CoerciblePolicyAdapter:
        run_wide_identity_policy = CoerciblePolicy()

    with pytest.raises(SubstrateError, match="policy must be a string"):
        _identity_policy_runner()._retain_run_seen_identities(
            CoerciblePolicyAdapter()
        )


def test_exact_registered_identity_policy_binding_opts_out() -> None:
    audited = _identity_policy_adapter(_content_hash_probe())
    assert _identity_policy_runner()._retain_run_seen_identities(audited) is False


def test_identity_policy_rejects_changed_executable_profile() -> None:
    audited = _identity_policy_adapter(_content_hash_probe())
    runner_instance = _identity_policy_runner(executable_profile="mutable_api")

    with pytest.raises(SubstrateError, match="is not registered"):
        runner_instance._retain_run_seen_identities(audited)


@pytest.mark.parametrize(
    ("runner_kwargs", "adapter_attributes", "error"),
    [
        ({"ownership_id": "wrong-owner"}, {}, "ownership does not match"),
        ({"registry_name": "wrong-registry"}, {}, "is not registered"),
        ({}, {"__module__": "wrong.module"}, "is not registered"),
        ({}, {"__qualname__": "WrongArchaeosTuffSource"}, "is not registered"),
    ],
)
def test_identity_policy_rejects_every_changed_binding_field(
    runner_kwargs: dict[str, str],
    adapter_attributes: dict[str, str],
    error: str,
) -> None:
    audited = _identity_policy_adapter(_content_hash_probe())
    for key, value in adapter_attributes.items():
        setattr(type(audited), key, value)
    with pytest.raises(SubstrateError, match=error):
        _identity_policy_runner(**runner_kwargs)._retain_run_seen_identities(audited)


def test_identity_policy_rejects_missing_change_probe() -> None:
    with pytest.raises(SubstrateError, match="public change-probe emit-version"):
        _identity_policy_runner()._retain_run_seen_identities(
            _identity_policy_adapter()
        )


def test_identity_policy_rejects_private_only_and_permissive_fallbacks() -> None:
    class PrivateOnlyProbe:
        _emit_version = _AUDITED_EMIT_VERSION

        def emit_version_for(self):
            return self._emit_version

        def build_token(self):
            return {"emit_version": self._emit_version}

    with pytest.raises(SubstrateError, match="exact ContentHashProbe"):
        _identity_policy_runner()._retain_run_seen_identities(
            _identity_policy_adapter(PrivateOnlyProbe())
        )


def test_identity_policy_rejects_content_hash_probe_subclass() -> None:
    class SpoofedContentHashProbe(ContentHashProbe):
        @property
        def emit_version(self) -> str:
            return _AUDITED_EMIT_VERSION

    with patch(
        "okg.substrate.library.sources.content_hash_probe.emit_version_for",
        return_value=_AUDITED_EMIT_VERSION,
    ):
        spoofed_probe = SpoofedContentHashProbe(
            content_items=lambda: (),
            config={},
            emit_targets=object(),
        )

    with pytest.raises(SubstrateError, match="exact ContentHashProbe"):
        _identity_policy_runner()._retain_run_seen_identities(
            _identity_policy_adapter(spoofed_probe)
        )


def test_identity_policy_rejects_emit_version_accessor_exception(monkeypatch) -> None:
    probe = _content_hash_probe()

    def _explode(_probe):
        raise RuntimeError("must fail closed")

    monkeypatch.setattr(ContentHashProbe, "emit_version", property(_explode))

    with pytest.raises(SubstrateError, match="public change-probe emit-version"):
        _identity_policy_runner()._retain_run_seen_identities(
            _identity_policy_adapter(probe)
        )


@pytest.mark.parametrize(
    "emit_version",
    [None, b"0" * 64, "", "0" * 63, "A" * 64, "g" * 64],
)
def test_identity_policy_rejects_malformed_emit_versions(
    emit_version: object,
) -> None:
    with pytest.raises(SubstrateError, match="malformed emit-version"):
        _identity_policy_runner()._retain_run_seen_identities(
            _identity_policy_adapter(_content_hash_probe(emit_version))
        )


def test_identity_policy_rejects_changed_emit_version() -> None:
    with pytest.raises(SubstrateError, match="not the audited fingerprint"):
        _identity_policy_runner()._retain_run_seen_identities(
            _identity_policy_adapter(_content_hash_probe("0" * 64))
        )


@pytest.mark.parametrize(
    "registry_binding_fingerprint",
    [None, 1, "", " ", " test-binding", "test-binding "],
)
def test_identity_policy_rejects_noncanonical_registry_binding(
    registry_binding_fingerprint: object,
) -> None:
    runner_instance = _identity_policy_runner()
    runner_instance.registry_binding_fingerprint = registry_binding_fingerprint

    with pytest.raises(SubstrateError, match="canonical nonempty registry"):
        runner_instance._retain_run_seen_identities(
            _identity_policy_adapter(_content_hash_probe())
        )


def test_identity_policy_authorization_reads_only_public_cached_authority(
    monkeypatch,
) -> None:
    probe = _content_hash_probe()
    original_getter = ContentHashProbe.emit_version.fget
    assert original_getter is not None
    accesses = 0

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("authorization must not inspect, read, or recompute")

    def _tracked_emit_version(target):
        nonlocal accesses
        accesses += 1
        return original_getter(target)

    monkeypatch.setattr(
        ContentHashProbe,
        "emit_version",
        property(_tracked_emit_version),
    )
    monkeypatch.setattr(runner.inspect, "getsource", _forbidden)
    monkeypatch.setattr(runner.inspect, "getsourcefile", _forbidden)
    monkeypatch.setattr(
        "okg.substrate.library.sources.change_probe.emit_version_for",
        _forbidden,
    )
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.emit_version_for",
        _forbidden,
    )
    monkeypatch.setattr(
        "okg.substrate.library.sources.content_hash_probe.hashlib.sha256",
        _forbidden,
    )
    monkeypatch.setattr(builtins, "open", _forbidden)

    assert (
        _identity_policy_runner()._retain_run_seen_identities(
            _identity_policy_adapter(probe)
        )
        is False
    )
    assert accesses == 1


def test_identity_policy_implementation_has_no_private_or_recompute_fallback() -> None:
    function_node = _function_node(
        _module_tree(),
        "_retain_run_seen_identities",
    )
    forbidden_calls = {"emit_version_for", "getsource", "getsourcefile"}

    assert all(
        node.attr != "_emit_version"
        for node in ast.walk(function_node)
        if isinstance(node, ast.Attribute)
    )
    assert all(
        _call_name(node) not in forbidden_calls
        for node in ast.walk(function_node)
        if isinstance(node, ast.Call)
    )
