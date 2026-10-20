"""Property tests for `parse_verdict`.

The parser accepts a JSON-shaped LLM verdict and validates its
shape. Two invariants:
1. **Round-trip**: any well-formed verdict survives the parser
   with `verdict` and `rationale` preserved.
2. **No exception leakage**: any random byte string either parses
   successfully (rare) or raises `VerdictParseError` — never any
   other exception. The "best-effort, never crashes" contract is
   structural to the runner.
"""
from __future__ import annotations

import json

import pytest
from hypothesis import HealthCheck, example, given, settings, strategies as st

from okg.substrate.catalog.invariants_llm import (
    VerdictParseError,
    parse_verdict,
)


_VERDICTS = ("consistent", "inconsistent", "uncertain")


# Strategy: well-formed verdict dicts.
_well_formed_verdict = st.fixed_dictionaries({
    "verdict": st.sampled_from(_VERDICTS),
    "rationale": st.text(min_size=0, max_size=200),
    "evidence_spans": st.lists(
        st.fixed_dictionaries({
            "node_id": st.text(min_size=1, max_size=40).filter(
                lambda s: s.strip()
            ),
            "byte_start": st.integers(min_value=0, max_value=10_000),
            "byte_end": st.integers(min_value=0, max_value=10_000),
        }).filter(lambda d: d["byte_end"] >= d["byte_start"]),
        min_size=0, max_size=5,
    ),
})


# ---------- round-trip ----------


@settings(
    max_examples=200, deadline=None,
    suppress_health_check=[HealthCheck.filter_too_much],
)
@given(_well_formed_verdict)
@example({
    "verdict": "consistent",
    "rationale": "all checks aligned",
    "evidence_spans": [],
})
def test_parse_verdict_roundtrips_well_formed(d: dict) -> None:
    """Any well-formed verdict dict serialized to JSON parses back
    with `verdict` and `rationale` byte-equal."""
    out = parse_verdict(json.dumps(d))
    assert out["verdict"] == d["verdict"]
    assert out["rationale"] == d["rationale"]
    assert len(out["evidence_spans"]) == len(d["evidence_spans"])


# ---------- no exception leakage ----------


@settings(max_examples=300, deadline=None)
@given(st.text(min_size=0, max_size=500))
@example("not json at all")
@example("")
@example("```json\n{}\n```")
@example("[1, 2, 3]")
def test_parse_verdict_only_raises_verdict_parse_error(raw: str) -> None:
    """Any random text either parses successfully (extremely rare
    given valid-shape constraints) or raises `VerdictParseError`.
    Bare exceptions (KeyError, TypeError, JSONDecodeError) leaking
    out would crash the runner mid-loop."""
    try:
        parse_verdict(raw)
    except VerdictParseError:
        pass  # expected
    except Exception as exc:
        raise AssertionError(
            f"parse_verdict({raw!r}) raised {type(exc).__name__} "
            f"({exc!r}); only VerdictParseError is allowed"
        )


def test_parse_verdict_handles_json_exception_alias_skew(
    monkeypatch,
) -> None:
    """A stale exported alias must not leak the decoder's ValueError."""
    class StaleJSONDecodeError(ValueError):
        pass

    monkeypatch.setattr(json, "JSONDecodeError", StaleJSONDecodeError)
    with pytest.raises(VerdictParseError):
        parse_verdict('"')


# ---------- code fences ----------


@settings(max_examples=100, deadline=None)
@given(_well_formed_verdict)
def test_parse_verdict_strips_json_code_fence(d: dict) -> None:
    """A response wrapped in ```json ... ``` parses identically to
    the unwrapped payload."""
    raw = "```json\n" + json.dumps(d) + "\n```"
    out = parse_verdict(raw)
    assert out["verdict"] == d["verdict"]


@settings(max_examples=100, deadline=None)
@given(_well_formed_verdict)
def test_parse_verdict_strips_plain_code_fence(d: dict) -> None:
    """Plain ``` (no language) fence also strips."""
    raw = "```\n" + json.dumps(d) + "\n```"
    out = parse_verdict(raw)
    assert out["verdict"] == d["verdict"]
