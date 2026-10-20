from __future__ import annotations

import pytest

import okg.bench.orchestrator as orchestrator
from okg.bench.differential_replay_fixture import (
    SYNTHETIC_REPLAY_EXPERIMENT_ID,
    SYNTHETIC_REPO_NAME,
)


def test_rebuild_equivalence_report_passes_for_matching_graph_and_outputs() -> None:
    exp = _experiment()
    graph = _graph_fingerprint(node_count=12, edge_count=20)
    outputs = _outputs_fingerprint(count=0, digest="sha256:empty")
    attrs = _attrs_fingerprint(count=4, digest="sha256:attrs")

    report = orchestrator._build_rebuild_equivalence_report(
        exp=exp,
        incremental_graph=graph,
        rebuild_graph=graph,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=attrs,
        rebuild_attrs=attrs,
        delta_proof=_delta_proof(),
        rebuild_experiment_id="rebuild-test",
        rebuild_generation_id="gen-rebuild",
    )

    assert report["ok"] is True
    assert report["checks"] == {
        "node_count_equal": True,
        "edge_count_equal": True,
        "node_digest_equal": True,
        "edge_digest_equal": True,
        "view_cites_count_equal": True,
        "view_cites_digest_equal": True,
        "derived_output_count_equal": True,
        "derived_output_digest_equal": True,
        "attr_effective_count_equal": True,
        "attr_effective_digest_equal": True,
        "bounded_source_scope": True,
    }
    assert report["bounded_work"]["max_changed_paths_per_commit"] == 2


def test_rebuild_equivalence_report_fails_on_view_cites_mismatch() -> None:
    """A view_cites_node divergence (incremental dropped a citation the rebuild
    has) must fail the proof. These edges are excluded from the general edge
    digest and are not derived_output_slots rows, so before the dedicated check
    they were invisible -- the gap that hid the warm view-cites over-retraction.
    """
    exp = _experiment()
    incremental = _graph_fingerprint(
        node_count=12,
        edge_count=20,
        view_cites_count=1,
        view_cites_digest="sha256:incremental-cites",
    )
    rebuild = _graph_fingerprint(
        node_count=12,
        edge_count=20,
        view_cites_count=2,
        view_cites_digest="sha256:rebuild-cites",
    )
    outputs = _outputs_fingerprint(count=0, digest="sha256:empty")

    report = orchestrator._build_rebuild_equivalence_report(
        exp=exp,
        incremental_graph=incremental,
        rebuild_graph=rebuild,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=_attrs_fingerprint(count=2, digest="sha256:attrs"),
        rebuild_attrs=_attrs_fingerprint(count=2, digest="sha256:attrs"),
        delta_proof=_delta_proof(),
        rebuild_experiment_id="rebuild-test",
        rebuild_generation_id="gen-rebuild",
    )

    assert report["ok"] is False
    assert report["checks"]["view_cites_count_equal"] is False
    assert report["checks"]["view_cites_digest_equal"] is False
    # The general edge checks stay green -- proving the gap these edges fell into.
    assert report["checks"]["edge_count_equal"] is True
    assert report["checks"]["edge_digest_equal"] is True


def test_rebuild_equivalence_report_includes_attribute_effective_view_checks() -> None:
    exp = _experiment()
    graph = _graph_fingerprint(node_count=12, edge_count=20)
    outputs = _outputs_fingerprint(count=0, digest="sha256:empty")
    attrs = _attrs_fingerprint(count=5, digest="sha256:attrs")

    report = orchestrator._build_rebuild_equivalence_report(
        exp=exp,
        incremental_graph=graph,
        rebuild_graph=graph,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=attrs,
        rebuild_attrs=attrs,
        delta_proof=_delta_proof(),
        rebuild_experiment_id="rebuild-test",
        rebuild_generation_id="gen-rebuild",
    )

    assert report["ok"] is True
    assert report["checks"]["attr_effective_count_equal"] is True
    assert report["checks"]["attr_effective_digest_equal"] is True
    # The report surfaces the attr counts so a count > 0 (non-vacuous) gate is
    # possible -- count 5 here, not a hidden 0 silently passing.
    assert report["incremental"]["attribute_effective"]["count"] == 5
    assert report["forced_rebuild"]["attribute_effective"]["count"] == 5


def test_rebuild_equivalence_report_fails_on_attribute_effective_digest_mismatch() -> (
    None
):
    """Divergence guard (req non-vacuous-guard): the incremental engine drifting
    on effective attribute state -- a stale value or the wrong producer winner --
    must fail the proof even when nodes, edges, view-cites, and derived outputs
    all match. Proves the new dimension actually discriminates."""
    exp = _experiment()
    graph = _graph_fingerprint(node_count=12, edge_count=20)
    outputs = _outputs_fingerprint(count=0, digest="sha256:empty")
    incremental_attrs = _attrs_fingerprint(count=5, digest="sha256:attrs-v1")
    rebuild_attrs = _attrs_fingerprint(count=5, digest="sha256:attrs-v2")

    report = orchestrator._build_rebuild_equivalence_report(
        exp=exp,
        incremental_graph=graph,
        rebuild_graph=graph,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=incremental_attrs,
        rebuild_attrs=rebuild_attrs,
        delta_proof=_delta_proof(),
        rebuild_experiment_id="rebuild-test",
        rebuild_generation_id="gen-rebuild",
    )

    assert report["ok"] is False
    assert report["checks"]["attr_effective_count_equal"] is True
    assert report["checks"]["attr_effective_digest_equal"] is False
    # Every other surface matches -- the attribute dimension alone caught the
    # drift (exactly the gap this change closes).
    assert report["checks"]["node_digest_equal"] is True
    assert report["checks"]["edge_digest_equal"] is True
    assert report["checks"]["view_cites_digest_equal"] is True
    assert report["checks"]["derived_output_digest_equal"] is True


def test_rebuild_equivalence_report_fails_on_attribute_effective_count_mismatch() -> (
    None
):
    exp = _experiment()
    graph = _graph_fingerprint(node_count=12, edge_count=20)
    outputs = _outputs_fingerprint(count=0, digest="sha256:empty")
    incremental_attrs = _attrs_fingerprint(count=5, digest="sha256:attrs")
    rebuild_attrs = _attrs_fingerprint(count=3, digest="sha256:attrs")

    report = orchestrator._build_rebuild_equivalence_report(
        exp=exp,
        incremental_graph=graph,
        rebuild_graph=graph,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=incremental_attrs,
        rebuild_attrs=rebuild_attrs,
        delta_proof=_delta_proof(),
        rebuild_experiment_id="rebuild-test",
        rebuild_generation_id="gen-rebuild",
    )

    assert report["ok"] is False
    assert report["checks"]["attr_effective_count_equal"] is False


def test_rebuild_equivalence_report_fails_on_graph_digest_mismatch() -> None:
    exp = _experiment()
    incremental = _graph_fingerprint(node_count=12, edge_count=20)
    rebuild = _graph_fingerprint(
        node_count=12,
        edge_count=20,
        node_digest="sha256:different",
    )
    outputs = _outputs_fingerprint(count=0, digest="sha256:empty")

    report = orchestrator._build_rebuild_equivalence_report(
        exp=exp,
        incremental_graph=incremental,
        rebuild_graph=rebuild,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=_attrs_fingerprint(count=2, digest="sha256:attrs"),
        rebuild_attrs=_attrs_fingerprint(count=2, digest="sha256:attrs"),
        delta_proof=_delta_proof(),
        rebuild_experiment_id="rebuild-test",
        rebuild_generation_id="gen-rebuild",
    )

    assert report["ok"] is False
    assert report["checks"]["node_count_equal"] is True
    assert report["checks"]["node_digest_equal"] is False


def test_rebuild_experiment_preserves_source_config() -> None:
    exp = orchestrator.Experiment(
        id="tinydb-lsp-enr-10",
        repo=orchestrator.Repo(
            name="tinydb",
            url="https://github.com/msiemens/tinydb.git",
        ),
        source_set="files+code+lsp",
        replay=orchestrator.Replay(strategy="first_n", n=10),
        config=orchestrator.BenchConfig(
            enrichers="code-reference",
            views=True,
            native_view_min_refresh_interval_ms=60_000,
        ),
    )

    rebuild = orchestrator._rebuild_experiment(exp)

    assert rebuild.id.startswith("rebuild-")
    assert rebuild.repo == exp.repo
    assert rebuild.source_set == exp.source_set
    assert rebuild.replay.strategy == "full_history"
    assert rebuild.config == exp.config


def test_stable_digest_rows_is_order_sensitive_and_content_stable() -> None:
    rows = [
        {"id": "a", "attrs": {"b": 2, "a": 1}},
        {"id": "b", "attrs": {"value": "x"}},
    ]

    assert orchestrator._stable_digest_rows(rows) == (
        orchestrator._stable_digest_rows(
            [
                {"attrs": {"a": 1, "b": 2}, "id": "a"},
                {"attrs": {"value": "x"}, "id": "b"},
            ]
        )
    )
    assert orchestrator._stable_digest_rows(rows) != (
        orchestrator._stable_digest_rows(list(reversed(rows)))
    )


def test_semantic_graph_rows_drop_rebuild_specific_source_lineage() -> None:
    rows = [
        {
            "id": "n1",
            "attrs": {
                "nomos_source_lineage": {"deployment": "incremental"},
                "title": "same",
            },
        }
    ]

    assert orchestrator._semantic_graph_rows(rows) == [
        {"id": "n1", "attrs": {"title": "same"}}
    ]


def test_semantic_graph_edge_rows_drop_view_citation_projection() -> None:
    rows = [
        {"edge_type": "contains", "src": "a", "dst": "b", "attrs": {}},
        {"edge_type": "view_cites_node", "src": "view:x", "dst": "b", "attrs": {}},
    ]

    assert orchestrator._semantic_graph_edge_rows(rows) == [
        {"edge_type": "contains", "src": "a", "dst": "b", "attrs": {}}
    ]
    # The excluded view_cites_node edges are not lost: a dedicated check covers
    # them via _view_cites_edge_rows (the complement of the general edge rows).
    assert orchestrator._view_cites_edge_rows(rows) == [
        {"edge_type": "view_cites_node", "src": "view:x", "dst": "b", "attrs": {}}
    ]


def test_semantic_attribute_effective_rows_drop_volatile_and_partition_keys() -> None:
    """The attribute-effective digest must exclude `materialized_at` (a wall-clock
    stamp) and `generation_id` (the partition key, which differs between the
    incremental and rebuild generations by construction) -- else a correct
    incremental run would never match its rebuild."""
    row = {
        "target_kind": "node",
        "target_id": "n1",
        "namespace": "default",
        "subtype": "code_symbol",
        "raw_attrs": {"base": 1},
        "attrs": {"base": 1, "label": "x"},
        "overlay_count": 1,
        "raw_text_hash": "sha256:raw",
        "generation_id": "okg:dev:branch:default:gen:incremental",
        "materialized_at": "2026-06-25T00:00:00Z",
    }

    assert orchestrator._semantic_attribute_effective_rows([row]) == [
        {
            "target_kind": "node",
            "target_id": "n1",
            "namespace": "default",
            "subtype": "code_symbol",
            "raw_attrs": {"base": 1},
            "attrs": {"base": 1, "label": "x"},
            "overlay_count": 1,
            "raw_text_hash": "sha256:raw",
        }
    ]
    # Two runs that differ ONLY in generation_id + materialized_at must digest
    # identically (the property that makes the equivalence check sound).
    other = {
        **row,
        "generation_id": "okg:dev:branch:default:gen:rebuild",
        "materialized_at": "2026-12-31T23:59:59Z",
    }
    assert orchestrator._stable_digest_rows(
        orchestrator._semantic_attribute_effective_rows([row])
    ) == orchestrator._stable_digest_rows(
        orchestrator._semantic_attribute_effective_rows([other])
    )


def test_semantic_attribute_effective_rows_drop_runtime_source_lineage() -> None:
    row = {
        "target_kind": "edge",
        "target_id": "123",
        "namespace": "default",
        "subtype": None,
        "src": "file:tinydb:storages.py",
        "dst": "branch:tinydb:master",
        "edge_type": "member_of",
        "raw_attrs": {
            "nomos_source_lineage": {
                "source_ids": ["tinydb-files-only-10.git-files"],
                "sources": [{"source_revisions": [{"head_sha": "old"}]}],
            },
            "semantic": "same",
        },
        "attrs": {
            "nomos_source_lineage": {
                "source_ids": ["tinydb-files-only-10.git-files"],
                "sources": [{"source_revisions": [{"head_sha": "old"}]}],
            },
            "semantic": "same",
        },
        "overlay_count": 0,
        "raw_text_hash": None,
    }
    rebuild = {
        **row,
        "raw_attrs": {
            "nomos_source_lineage": {
                "source_ids": ["rebuild-db61fbf0f16d.git-files"],
                "sources": [{"source_revisions": [{"head_sha": "new"}]}],
            },
            "semantic": "same",
        },
        "attrs": {
            "nomos_source_lineage": {
                "source_ids": ["rebuild-db61fbf0f16d.git-files"],
                "sources": [{"source_revisions": [{"head_sha": "new"}]}],
            },
            "semantic": "same",
        },
    }

    incremental_rows = orchestrator._semantic_attribute_effective_rows([row])
    rebuild_rows = orchestrator._semantic_attribute_effective_rows([rebuild])

    assert incremental_rows == rebuild_rows
    assert incremental_rows[0]["raw_attrs"] == {"semantic": "same"}
    assert incremental_rows[0]["attrs"] == {"semantic": "same"}
    assert orchestrator._stable_digest_rows(incremental_rows) == (
        orchestrator._stable_digest_rows(rebuild_rows)
    )


def test_semantic_derived_output_rows_drop_view_runtime_metadata() -> None:
    rows = [
        {
            "logical_key": "bench.symbol_calls",
            "inline_payload": {
                "kind": "json",
                "value": {
                    "refresh_stats": {"added": 1},
                    "view_manifest": {
                        "generation_id": 123,
                        "fragments": [{"id": "f"}],
                    },
                },
            },
        }
    ]

    assert orchestrator._semantic_derived_output_rows(rows) == [
        {
            "logical_key": "bench.symbol_calls",
            "inline_payload": {
                "kind": "json",
                "value": {
                    "view_manifest": {"fragments": [{"id": "f"}]},
                },
            },
        }
    ]


def test_semantic_derived_output_rows_normalize_runtime_byte_count() -> None:
    def _row(generation_id: int, refresh_stats: dict[str, int], byte_count: int):
        return {
            "logical_key": "bench.symbol_calls",
            "byte_count": byte_count,
            "inline_payload": {
                "kind": "json",
                "value": {
                    "refresh_stats": refresh_stats,
                    "view_manifest": {
                        "generation_id": generation_id,
                        "fragments": [{"id": "same-fragment"}],
                    },
                },
            },
        }

    incremental = orchestrator._semantic_derived_output_rows(
        [_row(7, {"added": 1}, 92)]
    )
    rebuild = orchestrator._semantic_derived_output_rows(
        [_row(123456789, {"added": 100, "removed": 20}, 128)]
    )

    assert incremental == rebuild


def test_semantic_derived_output_rows_drop_input_watermarks() -> None:
    def _row(edge_watermark: int):
        return {
            "logical_key": "bench.file_symbols",
            "byte_count": 1,
            "inline_payload": {
                "kind": "json",
                "value": {
                    "materializer_input_signature": {
                        "view_def_hash": "same",
                        "input_fact_watermark": 0,
                        "input_edge_fact_watermark": edge_watermark,
                    },
                    "view_manifest": {"fragments": [{"id": "same"}]},
                },
            },
        }

    assert orchestrator._semantic_derived_output_rows([_row(12)]) == (
        orchestrator._semantic_derived_output_rows([_row(999)])
    )


def test_semantic_derived_output_rows_hash_relation_visible_rows() -> None:
    def _row(byte_count: int):
        return {
            "logical_key": "bench.file_symbols.rows",
            "storage_kind": "relation",
            "row_count": 999,
            "byte_count": byte_count,
            "relation_visible_rows": [
                {
                    "scope_key": "file:a",
                    "row_key": "symbol:a",
                    "row_data": {"name": "same"},
                }
            ],
        }

    normalized = orchestrator._semantic_derived_output_rows([_row(10)])[0]
    assert normalized["row_count"] == 1
    assert "byte_count" not in normalized
    assert "relation_visible_rows" not in normalized
    assert orchestrator._semantic_derived_output_rows([_row(10)]) == (
        orchestrator._semantic_derived_output_rows([_row(20)])
    )


def test_relation_visible_rows_require_physical_relation_name() -> None:
    with pytest.raises(ValueError, match="storage_ref.relation"):
        orchestrator._relation_visible_rows_for_output_slot(
            object(),
            definition_id="bench.file_symbols",
            manifest_id="manifest-1",
            storage_ref={},
        )


def test_target_generation_id_for_fingerprint_accepts_numeric_token() -> None:
    assert (
        orchestrator._target_generation_id_for_fingerprint(
            "unused",
            "123",
        )
        == 123
    )


def test_target_generation_id_for_fingerprint_resolves_checkpoint(monkeypatch) -> None:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def fetchone(self):
            return ("456",)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(orchestrator.psycopg, "connect", lambda _dsn: _Conn())

    assert (
        orchestrator._target_generation_id_for_fingerprint(
            "postgres://example",
            "okg:test:branch:default:gen:checkpoint",
        )
        == 456
    )


def _experiment() -> orchestrator.Experiment:
    return orchestrator.Experiment(
        id=SYNTHETIC_REPLAY_EXPERIMENT_ID,
        repo=orchestrator.Repo(
            name=SYNTHETIC_REPO_NAME,
            url="synthetic:differential",
        ),
        source_set="tier1-enriched",
        replay=orchestrator.Replay(strategy="full_history"),
    )


def _graph_fingerprint(
    *,
    node_count: int,
    edge_count: int,
    node_digest: str = "sha256:nodes",
    edge_digest: str = "sha256:edges",
    view_cites_count: int = 0,
    view_cites_digest: str = "sha256:view_cites",
) -> dict:
    return {
        "generation_id": "gen",
        "nodes": {"count": node_count, "digest": node_digest},
        "edges": {"count": edge_count, "digest": edge_digest},
        "view_cites": {"count": view_cites_count, "digest": view_cites_digest},
    }


def _outputs_fingerprint(*, count: int, digest: str) -> dict:
    return {"count": count, "digest": digest, "rows": []}


def _attrs_fingerprint(*, count: int, digest: str) -> dict:
    return {"count": count, "digest": digest, "generation_id": "gen", "present": True}


def _delta_proof() -> dict:
    return {
        "summary": {"final_generation_id": "gen-incremental"},
        "rows": [
            {
                "boundaries": {
                    "repo_scope": {"scope_count": 1},
                    "graph_delta": {
                        "nodes_added": 3,
                        "edges_added": 4,
                    },
                }
            },
            {
                "boundaries": {
                    "repo_scope": {"scope_count": 2},
                    "graph_delta": {
                        "nodes_added": 5,
                        "edges_added": 6,
                    },
                }
            },
        ],
    }
