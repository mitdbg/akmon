"""Coverage tests for `okg.substrate.catalog.invariants_llm`.

Three groups:
1. `parse_verdict` — pure function, many shape-error paths.
2. `_strip_code_fences` — small pure helper.
3. `_validate_spans_against_bodies` — DB-touching span validator
   that partitions valid/invalid spans against the cited node's
   `attrs->>'body'`.

The runner (`LLMJudgeInvariantRunner`) itself is exercised
end-to-end via the `stub_llm_factory` fixture for a single happy
path — full coverage of every error branch is left to the larger
runner tests.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from tests.substrate._db_helpers import ensure_published_generation
from okg.substrate.catalog.invariants_llm import (
    VerdictParseError,
    _strip_code_fences,
    _validate_spans_against_bodies,
    parse_verdict,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact

pytestmark = pytest.mark.postgres


# ---------- _strip_code_fences ----------


def test_strip_code_fences_removes_json_fence() -> None:
    raw = "```json\n{\"a\": 1}\n```"
    assert _strip_code_fences(raw) == '{"a": 1}'


def test_strip_code_fences_removes_plain_fence() -> None:
    raw = "```\n{\"a\": 1}\n```"
    assert _strip_code_fences(raw) == '{"a": 1}'


def test_strip_code_fences_passes_through_unfenced() -> None:
    assert _strip_code_fences('{"a": 1}') == '{"a": 1}'


def test_strip_code_fences_handles_leading_whitespace() -> None:
    raw = "  \n```json\n{\"a\": 1}\n```\n"
    assert _strip_code_fences(raw) == '{"a": 1}'


# ---------- parse_verdict: success paths ----------


def test_parse_verdict_minimal_success() -> None:
    out = parse_verdict(json.dumps({
        "verdict": "consistent",
        "rationale": "all checks aligned",
    }))
    assert out["verdict"] == "consistent"
    assert out["rationale"] == "all checks aligned"
    assert out["evidence_spans"] == []


def test_parse_verdict_with_evidence_spans() -> None:
    out = parse_verdict(json.dumps({
        "verdict": "inconsistent",
        "rationale": "spec says X, body says Y",
        "evidence_spans": [
            {"node_id": "pub:1", "byte_start": 100, "byte_end": 250},
            {"node_id": "pub:1", "byte_start": 0, "byte_end": 50},
        ],
    }))
    assert len(out["evidence_spans"]) == 2
    assert out["evidence_spans"][0]["byte_start"] == 100


def test_parse_verdict_uncertain_verdict_accepted() -> None:
    out = parse_verdict(json.dumps({
        "verdict": "uncertain",
        "rationale": "insufficient evidence to judge",
    }))
    assert out["verdict"] == "uncertain"


def test_parse_verdict_strips_code_fences() -> None:
    raw = '```json\n' + json.dumps({
        "verdict": "consistent", "rationale": "ok",
    }) + '\n```'
    out = parse_verdict(raw)
    assert out["verdict"] == "consistent"


# ---------- parse_verdict: shape errors ----------


def test_parse_verdict_rejects_non_json() -> None:
    with pytest.raises(VerdictParseError, match="not JSON"):
        parse_verdict("not json at all")


def test_parse_verdict_rejects_array_top_level() -> None:
    with pytest.raises(VerdictParseError, match="must be a JSON object"):
        parse_verdict("[1, 2, 3]")


def test_parse_verdict_rejects_unknown_verdict() -> None:
    with pytest.raises(VerdictParseError, match="verdict.verdict"):
        parse_verdict(json.dumps({
            "verdict": "maybe",
            "rationale": "...",
        }))


def test_parse_verdict_rejects_missing_rationale() -> None:
    with pytest.raises(VerdictParseError, match="rationale"):
        parse_verdict(json.dumps({"verdict": "consistent"}))


def test_parse_verdict_rejects_non_string_rationale() -> None:
    with pytest.raises(VerdictParseError, match="rationale"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": 42,
        }))


def test_parse_verdict_rejects_non_list_spans() -> None:
    with pytest.raises(VerdictParseError, match="evidence_spans"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": "not a list",
        }))


def test_parse_verdict_rejects_non_dict_span_entry() -> None:
    with pytest.raises(VerdictParseError, match="must be an object"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": ["not a dict"],
        }))


def test_parse_verdict_rejects_span_missing_key() -> None:
    with pytest.raises(VerdictParseError, match="missing key"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": [
                {"node_id": "n:a", "byte_start": 0},  # missing byte_end
            ],
        }))


def test_parse_verdict_rejects_empty_node_id() -> None:
    with pytest.raises(VerdictParseError, match="node_id"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": [
                {"node_id": "", "byte_start": 0, "byte_end": 10},
            ],
        }))


def test_parse_verdict_rejects_non_int_byte_value() -> None:
    with pytest.raises(VerdictParseError, match="byte_start"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": [
                {"node_id": "n:a", "byte_start": "0", "byte_end": 10},
            ],
        }))


def test_parse_verdict_rejects_negative_byte_start() -> None:
    with pytest.raises(VerdictParseError, match="invalid range"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": [
                {"node_id": "n:a", "byte_start": -1, "byte_end": 10},
            ],
        }))


def test_parse_verdict_rejects_inverted_range() -> None:
    with pytest.raises(VerdictParseError, match="invalid range"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": [
                {"node_id": "n:a", "byte_start": 100, "byte_end": 50},
            ],
        }))


def test_parse_verdict_rejects_bool_byte_value() -> None:
    """`isinstance(True, int)` is True — the parser must catch
    that with an explicit `not isinstance(.., bool)` check."""
    with pytest.raises(VerdictParseError, match="byte_start"):
        parse_verdict(json.dumps({
            "verdict": "consistent", "rationale": "ok",
            "evidence_spans": [
                {"node_id": "n:a", "byte_start": True, "byte_end": 10},
            ],
        }))


# ---------- _validate_spans_against_bodies ----------


def _seed_node_with_body(
    conn: psycopg.Connection,
    node_id: str,
    body: str,
    *,
    gen: int = 1,
) -> None:
    # Write the publication node into the Chronos graph, then
    # checkpoint the pinned generation so the reader's
    # `checkout_generation(gen)` resolves to this snapshot.
    ChronosGraphStore.from_connection(conn).apply_facts(
        [NodeFact(node_id, "publication", {"body": body}, {"id": node_id}, {"v": 1})],
        source="default",
        strict_endpoints=True,
    )
    ensure_published_generation(conn, gen)


def test_validate_spans_empty_input(clean_runtime: str) -> None:
    valid, invalid = _validate_spans_against_bodies(
        clean_runtime, [], generation_id=1,
    )
    assert valid == []
    assert invalid == []


def test_validate_spans_within_body_range(clean_runtime: str) -> None:
    """A span entirely within the body's UTF-8 byte range
    is valid; the function echoes a `preview` of the byte slice."""
    body = "Hello world. This is a test."
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_node_with_body(conn, "pub:1", body)

    valid, invalid = _validate_spans_against_bodies(
        clean_runtime,
        [{"node_id": "pub:1", "byte_start": 0, "byte_end": 11}],
        generation_id=1,
    )
    assert len(valid) == 1
    assert invalid == []
    assert valid[0]["preview"] == "Hello world"


def test_validate_spans_out_of_range(clean_runtime: str) -> None:
    body = "short body"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_node_with_body(conn, "pub:1", body)

    valid, invalid = _validate_spans_against_bodies(
        clean_runtime,
        [{"node_id": "pub:1", "byte_start": 0, "byte_end": 999}],
        generation_id=1,
    )
    assert valid == []
    assert len(invalid) == 1
    assert invalid[0]["reason"].startswith("out_of_range")


def test_validate_spans_node_not_found(clean_runtime: str) -> None:
    """A span citing a node that doesn't exist at the generation
    is invalid with reason `node_not_found`."""
    # Checkpoint an (otherwise empty) generation so the reader's
    # pinned `checkout_generation(1)` resolves; the cited node is
    # genuinely absent from the snapshot.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_published_generation(conn, 1)

    valid, invalid = _validate_spans_against_bodies(
        clean_runtime,
        [{"node_id": "pub:nonexistent", "byte_start": 0, "byte_end": 10}],
        generation_id=1,
    )
    assert valid == []
    assert len(invalid) == 1
    assert invalid[0]["reason"] == "node_not_found"


def test_validate_spans_node_body_empty(clean_runtime: str) -> None:
    """A node whose body attr is missing/empty fails span
    validation with reason `node_body_empty`."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Node with empty body, written into the Chronos graph then
        # checkpointed at generation 1.
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(
                "pub:empty", "publication", {"body": ""},
                {"id": "pub:empty"}, {"v": 1},
            )],
            source="default",
            strict_endpoints=True,
        )
        ensure_published_generation(conn, 1)

    valid, invalid = _validate_spans_against_bodies(
        clean_runtime,
        [{"node_id": "pub:empty", "byte_start": 0, "byte_end": 10}],
        generation_id=1,
    )
    assert valid == []
    assert len(invalid) == 1
    assert invalid[0]["reason"] == "node_body_empty"


def test_validate_spans_groups_per_node(clean_runtime: str) -> None:
    """Two spans on the same node are batched into one body lookup
    and both validated."""
    body = "Hello world. This is a test."  # 28 bytes
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_node_with_body(conn, "pub:1", body)

    valid, invalid = _validate_spans_against_bodies(
        clean_runtime,
        [
            {"node_id": "pub:1", "byte_start": 0, "byte_end": 5},
            {"node_id": "pub:1", "byte_start": 100, "byte_end": 200},
        ],
        generation_id=1,
    )
    assert len(valid) == 1
    assert len(invalid) == 1
    assert valid[0]["byte_end"] == 5
    assert invalid[0]["byte_start"] == 100
