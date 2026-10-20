"""Machine-surface byte budgets (budget-the-pact-loop
req.budget.machine-surfaces). Exceeding a budget is a failure, not a
warning."""

from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.mcp.server import _preview_truncate_results
from okg.substrate.pact.cli import POST_EDIT_SUMMARY_BUDGET_BYTES
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest


def _fat_manifest(change_id: str, n: int = 40) -> PactManifest:
    """A deliberately wide manifest: n requirements x n tasks of prose."""
    return PactManifest(
        pact_version=0,
        target_pact_version=4,
        change=Change(
            id=change_id,
            title="Fat fixture " + "x" * 120,
            status="proposal",
            tier=1,
            rationale="r " * 400,
            root_cause="rc " * 400,
        ),
        requirements=[
            {
                "id": f"req.{change_id}.{i}",
                "statement": f"statement {i} " + "verbose " * 60,
                "evidence_policy": {"required": ["executed_test"]},
            }
            for i in range(n)
        ],
        tasks=[
            {
                "id": f"task.{change_id}.{i}",
                "title": f"task {i}",
                "verification": "pytest -q " + "and check " * 30,
                "requirements": [f"req.{change_id}.{i}"],
            }
            for i in range(n)
        ],
    )


def _json_out(capsys) -> dict:
    return json.loads(capsys.readouterr().out)


def test_post_edit_default_fits_budget(tmp_path: Path, capsys) -> None:
    save_manifest(tmp_path, _fat_manifest("fat-demo"))
    rc = main([
        "pact", "--root", str(tmp_path), "gate", "post-edit",
        "fat-demo", "--json",
    ])
    payload = _json_out(capsys)
    # no ledger deployment in a tmp root, so intent registration is
    # unavailable and the gate defers; the budget is what is under test
    assert rc == 3
    assert payload["verdict"] == "deferred"
    encoded = len(json.dumps(payload).encode("utf-8"))
    assert encoded <= POST_EDIT_SUMMARY_BUDGET_BYTES, (
        f"summary is {encoded} bytes against a "
        f"{POST_EDIT_SUMMARY_BUDGET_BYTES}-byte budget"
    )
    # "rerun with --full" is RETIRED (pact-self-repairing-refusals
    # req.refusal.complete): it was emitted 406 times and honored 9, so the
    # compact form carries the messages and `full_report` names the actual
    # command for the verbose artifacts.
    assert payload["full_report"] == "okg pact gate post-edit fat-demo --full --json"
    assert "rerun with --full" not in json.dumps(payload)
    assert "error_count" in payload


def test_post_edit_full_flag_returns_unbudgeted_report(
    tmp_path: Path, capsys
) -> None:
    save_manifest(tmp_path, _fat_manifest("fat-demo"))
    rc = main([
        "pact", "--root", str(tmp_path), "gate", "post-edit",
        "fat-demo", "--full", "--json",
    ])
    payload = _json_out(capsys)
    # --full changes the report size, not the verdict: still deferred on the
    # unavailable intent registration
    assert rc == 3
    assert payload["verdict"] == "deferred"
    # The full report keeps rich keys the summary deliberately drops, and
    # has no budget banner.
    assert "intent_digest" in payload
    assert "full_report" not in payload
    encoded = len(json.dumps(payload).encode("utf-8"))
    assert encoded > POST_EDIT_SUMMARY_BUDGET_BYTES


def test_post_edit_summary_caps_error_list(tmp_path: Path, capsys) -> None:
    from okg.substrate.pact.cli import _post_edit_summary

    full = {
        "ok": False,
        "change": "fat-broken",
        "errors": [
            {"code": f"err_{i}", "path": f"tasks[{i}]", "message": "m" * 200}
            for i in range(40)
        ],
        "warnings": [{"code": "w"}] * 25,
        "review_view": {"status": "stale"},
        "agent_guidance": {"recommended_commands": ["okg pact view x"]},
    }
    payload = _post_edit_summary(full)
    assert payload["ok"] is False
    assert payload["error_count"] == 40
    assert len(payload["errors"]) <= 10
    # Every surviving row carries its MESSAGE. Dropping it was what made
    # "rerun with --full" necessary; the row is no longer complete without it
    # (pact-self-repairing-refusals req.refusal.complete). The list length is
    # still capped, and the budget below is still enforced.
    assert payload["errors"][0] == {
        "code": "err_0",
        "path": "tasks[0]",
        "message": "m" * 200,
    }
    assert payload["warning_count"] == 25
    encoded = len(json.dumps(payload).encode("utf-8"))
    assert encoded <= POST_EDIT_SUMMARY_BUDGET_BYTES


def test_bootstrap_default_fits_budget(tmp_path: Path, capsys) -> None:
    from okg.substrate.pact.cli import BOOTSTRAP_SUMMARY_BUDGET_BYTES

    save_manifest(tmp_path, _fat_manifest("fat-demo"))
    rc = main([
        "pact", "--root", str(tmp_path), "view", "fat-demo",
        "--format=bootstrap", "--json",
    ])
    payload = _json_out(capsys)
    assert rc == 0
    encoded = len(json.dumps(payload).encode("utf-8"))
    assert encoded <= BOOTSTRAP_SUMMARY_BUDGET_BYTES
    assert payload["task"]["id"]
    assert payload["next_action"]
    assert payload["skills"]["status"] in {"blocked", "current"}
    assert payload["contracts"]["reporting"].endswith(".md")
    assert payload["full_report"] == (
        "okg pact view fat-demo --format=bootstrap --full --json"
    )
    assert "rerun with --full" not in json.dumps(payload)


def test_bootstrap_full_flag_returns_unbudgeted(tmp_path: Path, capsys) -> None:
    save_manifest(tmp_path, _fat_manifest("fat-demo"))
    rc = main([
        "pact", "--root", str(tmp_path), "view", "fat-demo",
        "--format=bootstrap", "--full", "--json",
    ])
    payload = _json_out(capsys)
    assert rc == 0
    assert "report_contract" in payload
    assert "skill_freshness" in payload
    assert "task_bundle" in payload


def test_preview_truncation_marks_fields() -> None:
    payload = {
        "results": [
            {
                "node_id": "pact-change:x",
                "attrs": {
                    "title": "short",
                    "why_text": "w" * 5000,
                    "text": "t" * 600,
                    "count": 7,
                },
            }
        ]
    }
    out = _preview_truncate_results(payload)
    attrs = out["results"][0]["attrs"]
    assert len(attrs["why_text"]) == 280
    assert attrs["why_text"].endswith("...")
    assert len(attrs["text"]) == 280
    assert attrs["title"] == "short"
    assert attrs["count"] == 7
    assert attrs["_truncated_fields"] == ["text", "why_text"]
    assert "truncated by default" in out["prose_preview"]


def test_preview_truncation_noop_when_nothing_long() -> None:
    payload = {"results": [{"attrs": {"title": "short"}}]}
    out = _preview_truncate_results(payload)
    assert "prose_preview" not in out
    assert "attribute_truncation" not in out
    assert out["results"][0]["attrs"] == {"title": "short"}


# ---------- mcp-authority-diversity-budgets req.budget.complete-truncation ----

def test_preview_truncation_recurses_into_nested_dicts_and_lists() -> None:
    """A long string one level down was previously returned in FULL.

    `_preview_truncate_results` only walked top-level attrs, so
    `attrs.meta.summary` and `attrs.sections[0].body` sailed past the budget
    while `_truncated_fields` said nothing about them — the budget was enforced
    on the SHAPE of the data rather than on its size (issue #1444).
    """
    payload = {
        "results": [
            {
                "node_id": "n:1",
                "attrs": {
                    "text": "t" * 600,
                    "meta": {"summary": "s" * 900, "tag": "short"},
                    "sections": [
                        {"body": "b" * 700},
                        {"body": "fine"},
                    ],
                    "count": 7,
                },
            }
        ]
    }
    out = _preview_truncate_results(payload)
    attrs = out["results"][0]["attrs"]
    assert len(attrs["text"]) == 280
    assert len(attrs["meta"]["summary"]) == 280
    assert attrs["meta"]["summary"].endswith("...")
    assert attrs["meta"]["tag"] == "short"
    assert len(attrs["sections"][0]["body"]) == 280
    assert attrs["sections"][1]["body"] == "fine"
    assert attrs["count"] == 7
    assert attrs["_truncated_fields"] == [
        "meta.summary", "sections[0].body", "text",
    ]


def test_attribute_truncation_is_stated_in_the_envelope() -> None:
    """`truncated: false` with silently shortened attrs is the defect.

    The statement is STRUCTURAL and distinct from the two existing keys:
    `truncated` still means "more rows exist" and `truncation_reason` still
    means "the byte budget shrank the page".
    """
    from okg.substrate.mcp.response_bounds import BYTE_TRUNCATION_REASON
    from okg.substrate.mcp.server import ATTRIBUTE_TRUNCATION_REASON

    payload = {
        "truncated": False,
        "results": [
            {"node_id": "n:1", "attrs": {"text": "a" * 600}},
            {"node_id": "n:2", "attrs": {"text": "b" * 600, "why": "c" * 600}},
            {"node_id": "n:3", "attrs": {"text": "short"}},
        ],
    }
    out = _preview_truncate_results(payload)
    assert out["attribute_truncation"] == {
        "applied": True,
        "reason": ATTRIBUTE_TRUNCATION_REASON,
        "max_chars": 280,
        "rows_truncated": 2,
        "fields_truncated": 3,
    }
    # The two pre-existing keys keep their meanings untouched.
    assert out["truncated"] is False
    assert "truncation_reason" not in out
    assert ATTRIBUTE_TRUNCATION_REASON != BYTE_TRUNCATION_REASON


def test_preview_truncation_honours_an_explicit_char_budget() -> None:
    payload = {"results": [{"attrs": {"text": "t" * 5000}}]}
    out = _preview_truncate_results(payload, max_chars=1024)
    assert len(out["results"][0]["attrs"]["text"]) == 1024
    assert out["attribute_truncation"]["max_chars"] == 1024
    assert "1024 chars" in out["prose_preview"]


def test_preview_truncation_works_on_filter_map_and_aggregate_shapes() -> None:
    """The helper is shape-driven: any payload with a `results` list of rows
    carrying `attrs` is handled the same way, and a payload without one is
    returned untouched rather than raising."""
    filter_shape = {
        "results": [{"node_id": "n:1", "attrs": {"body": "x" * 400}}],
        "total_count": 1,
    }
    assert _preview_truncate_results(filter_shape)["attribute_truncation"][
        "applied"
    ] is True
    map_shape = {
        "results": [
            {"node_id": "n:1", "attrs": {"nested": ["y" * 400, "ok"]}},
        ],
    }
    out = _preview_truncate_results(map_shape)
    assert len(out["results"][0]["attrs"]["nested"][0]) == 280
    assert out["results"][0]["attrs"]["nested"][1] == "ok"
    assert out["results"][0]["attrs"]["_truncated_fields"] == ["nested[0]"]
    # aggregate returns rollup rows without `attrs`, and a payload with no
    # `results` list at all: neither is changed, and neither raises.
    aggregate_shape = {"results": [{"group": "a", "count": 3}]}
    assert _preview_truncate_results(dict(aggregate_shape)) == aggregate_shape
    assert _preview_truncate_results({"groups": []}) == {"groups": []}


def test_max_chars_is_refused_out_of_range_not_clamped() -> None:
    from okg.substrate.mcp.search import (
        DEFAULT_PREVIEW_CHARS,
        MAX_PREVIEW_CHARS,
        MIN_PREVIEW_CHARS,
        InvalidPreviewCharBudget,
        validate_preview_chars,
    )
    import pytest

    assert validate_preview_chars(None) == DEFAULT_PREVIEW_CHARS
    assert validate_preview_chars(MIN_PREVIEW_CHARS) == MIN_PREVIEW_CHARS
    assert validate_preview_chars(MAX_PREVIEW_CHARS) == MAX_PREVIEW_CHARS
    for bad in (0, MIN_PREVIEW_CHARS - 1, MAX_PREVIEW_CHARS + 1, "280", 1.5, True):
        with pytest.raises(InvalidPreviewCharBudget) as exc:
            validate_preview_chars(bad)
        assert exc.value.code == "invalid_preview_char_budget"
    # Refused, never clamped: the message says so, because a clamped budget
    # returns a different response from the one the caller asked for.
    with pytest.raises(InvalidPreviewCharBudget) as exc:
        validate_preview_chars(MAX_PREVIEW_CHARS + 1)
    assert "clamped" in str(exc.value)


def test_attribute_truncation_runs_first_and_the_byte_budget_still_applies(
) -> None:
    """Both budgets, in order. Shortening strings does not exempt a response
    from the byte budget, and the byte budget's own keys stay distinct."""
    from okg.substrate.mcp.response_bounds import (
        BYTE_TRUNCATION_REASON,
        bound_read_tool,
        public_call_tool_result_bytes,
    )

    def _rows(n: int) -> list[dict]:
        return [
            {"node_id": f"n:{i}", "attrs": {"text": "t" * 5000}}
            for i in range(n)
        ]

    def search_like(
        *, query: str, limit: int, offset: int = 0, max_bytes: int,
    ) -> dict:
        payload = {
            "results": _rows(limit),
            "limit": limit,
            "offset": offset,
            "total_count": 200,
            "truncated": True,
            "next_offset": offset + limit,
        }
        return _preview_truncate_results(payload)

    from okg.substrate.mcp.result_text import result_format_scope

    wrapped = bound_read_tool("search", search_like)
    # JSON prints every attribute, so 200 rows of 280 chars are over 8 KiB.
    # These rows carry no passage text to shorten, so the page is read again
    # at a smaller limit, as before.
    with result_format_scope("json"):
        out = wrapped(query="q", limit=200, max_bytes=8 * 1024)
        # Attribute truncation happened...
        assert out["attribute_truncation"]["applied"] is True
        assert all(
            len(row["attrs"]["text"]) == 280 for row in out["results"]
        )
        # ...and the byte budget was ALSO enforced, with its own reason token.
        assert out["truncation_reason"] == BYTE_TRUNCATION_REASON
        assert out["effective_limit"] < 200
        assert public_call_tool_result_bytes(out, tool="search") <= 8 * 1024
    # Text (the default) prints a search row as its head line and passage,
    # never the attrs dump (PACT read-passage-rows-3666), so the same 200
    # rows fit 8 KiB whole: attribute truncation still ran, no row is cut.
    out = wrapped(query="q", limit=200, max_bytes=8 * 1024)
    assert out["attribute_truncation"]["applied"] is True
    assert len(out["results"]) == 200
    assert "truncation_reason" not in out
    assert public_call_tool_result_bytes(out, tool="search") <= 8 * 1024


# ---------- fence safety in the preview budget (#1444 review, MAJOR 5) ----------
#
# Attribute values on an untrusted node arrive already wrapped between fence
# markers. A naive slice through one is a SECURITY defect, not a cosmetic one:
# it drops the closing marker, and the producer text that follows an
# unterminated block reads as trusted content to whatever consumes the
# response.

def _fenced(body: str, source: str = "source:untrusted") -> str:
    from okg.substrate.mcp.provenance import FENCE_CLOSE, FENCE_OPEN

    return f"{FENCE_OPEN.format(source=source)}\n{body}\n{FENCE_CLOSE}"


def test_truncating_a_fenced_attribute_keeps_the_closing_marker() -> None:
    from okg.substrate.mcp.provenance import FENCE_CLOSE, is_fenced

    value = _fenced("z" * 5000)
    payload = {"results": [{"node_id": "n:1", "attrs": {"text": value}}]}
    out = _preview_truncate_results(payload)
    cut = out["results"][0]["attrs"]["text"]

    assert len(cut) < len(value)
    assert cut.rstrip().endswith(FENCE_CLOSE), cut[-80:]
    assert cut.count(FENCE_CLOSE) == 1
    assert is_fenced(cut), "the shortened value must still be a closed block"
    assert out["attribute_truncation"]["applied"] is True


def test_a_forged_early_closer_cannot_end_the_block_inside_the_body() -> None:
    """Attacker text that embeds a closer must not survive the cut unescaped.

    Producer text is escaped when it is fenced, but a value that arrives
    already fenced, or one whose body carries a marker for any other reason,
    must not be able to terminate the untrusted block early once the body has
    been shortened around it.
    """
    from okg.substrate.mcp.provenance import FENCE_CLOSE

    body = "a" * 40 + FENCE_CLOSE + " now trusted? " + "b" * 5000
    payload = {"results": [{"node_id": "n:1", "attrs": {"text": _fenced(body)}}]}
    out = _preview_truncate_results(payload)
    cut = out["results"][0]["attrs"]["text"]

    # Exactly one closer, and it is the real one at the end.
    assert cut.count(FENCE_CLOSE) == 1
    assert cut.rstrip().endswith(FENCE_CLOSE)
    # The forged one was neutralized rather than dropped silently.
    assert "ESCAPED_/UNTRUSTED_DATA" in cut


def test_a_fenced_value_nested_in_a_dict_is_cut_safely_too() -> None:
    """The nested walk is exactly where the naive slice used to live."""
    from okg.substrate.mcp.provenance import FENCE_CLOSE, is_fenced

    payload = {
        "results": [
            {
                "node_id": "n:1",
                "attrs": {
                    "meta": {"summary": _fenced("y" * 4000)},
                    "sections": [{"body": _fenced("w" * 4000)}],
                },
            }
        ]
    }
    out = _preview_truncate_results(payload)
    attrs = out["results"][0]["attrs"]
    for value in (attrs["meta"]["summary"], attrs["sections"][0]["body"]):
        assert is_fenced(value), value[-80:]
        assert value.count(FENCE_CLOSE) == 1
    assert attrs["_truncated_fields"] == ["meta.summary", "sections[0].body"]


def test_an_unfenced_value_is_still_cut_to_the_budget() -> None:
    """Fence preservation must not stop the budget applying to normal text."""
    payload = {"results": [{"attrs": {"text": "t" * 5000}}]}
    out = _preview_truncate_results(payload, max_chars=300)
    assert len(out["results"][0]["attrs"]["text"]) == 300


# ---- issue #2670: the preview budget counts source text, not markers ----


def _body(value: str) -> str:
    """The text between a fenced value's opener and closer lines."""
    return "\n".join(value.split("\n")[1:-1])


def test_the_markers_do_not_consume_the_preview_budget() -> None:
    """A 280-char budget on a fenced value keeps 280 chars of text.

    Before #2670 the markers were charged against the budget, leaving about
    200 characters of source text in a 280-character preview.
    """
    from okg.substrate.mcp.provenance import FENCE_CLOSE, is_fenced
    from okg.substrate.mcp.search import cap_preview_text

    value = _fenced("z" * 5000)
    cut = cap_preview_text(value, max_chars=280)

    assert is_fenced(cut)
    assert cut.count(FENCE_CLOSE) == 1
    assert cut.split("\n")[0] == value.split("\n")[0], "opener kept whole"
    assert len(_body(cut)) == 280
    assert _body(cut).endswith("...")
    assert len(cut) > 280, "the markers ride on top of the budget"


def test_a_fenced_value_whose_text_fits_is_not_reported_as_cut() -> None:
    """Total length over the budget, text under it: nothing to truncate."""
    value = _fenced("q" * 250)
    assert len(value) > 280
    payload = {"results": [{"node_id": "n:1", "attrs": {"text": value}}]}
    out = _preview_truncate_results(payload, max_chars=280)

    assert out["results"][0]["attrs"]["text"] == value
    assert "_truncated_fields" not in out["results"][0]["attrs"]
    assert "attribute_truncation" not in out


def test_trusted_text_ending_in_a_closer_line_cannot_borrow_the_rule() -> None:
    """Only a well-formed opener earns the body-only budget."""
    from okg.substrate.mcp.provenance import FENCE_CLOSE
    from okg.substrate.mcp.search import cap_preview_text

    value = "x" * 400 + "\n" + "y" * 400 + "\n" + FENCE_CLOSE
    assert len(cap_preview_text(value, max_chars=280)) == 280


def test_an_over_long_opener_becomes_the_bounded_opener() -> None:
    from okg.substrate.mcp.provenance import FENCE_OPEN, is_fenced
    from okg.substrate.mcp.search import cap_preview_text

    cut = cap_preview_text(_fenced("z" * 50, source="s" * 400), max_chars=280)
    assert cut.split("\n")[0] == FENCE_OPEN.format(source="bounded")
    assert is_fenced(cut)
    assert _body(cut) == "z" * 50


def test_an_embedded_closer_that_overflows_once_escaped_stays_bounded() -> None:
    """Raw body fits; escaping lengthens it past the budget. Still one closer."""
    from okg.substrate.mcp.provenance import FENCE_CLOSE
    from okg.substrate.mcp.search import cap_preview_text

    body = "a" * 270 + FENCE_CLOSE
    assert len(body) <= 290
    cut = cap_preview_text(_fenced(body), max_chars=290)
    assert cut.count(FENCE_CLOSE) == 1
    assert cut.endswith("\n" + FENCE_CLOSE)
    assert len(_body(cut)) == 290
    assert _body(cut).endswith("...")


def test_an_over_limit_integer_max_chars_is_lowered_and_disclosed() -> None:
    """Issue #3252: the MCP tool's `clamp_preview_chars` lowers an integer
    above the maximum and returns what it did; the rest is still refused."""
    from okg.substrate.mcp.search import (
        DEFAULT_PREVIEW_CHARS,
        MAX_PREVIEW_CHARS,
        MIN_PREVIEW_CHARS,
        InvalidPreviewCharBudget,
        clamp_preview_chars,
    )
    import pytest

    assert clamp_preview_chars(None) == (DEFAULT_PREVIEW_CHARS, None)
    assert clamp_preview_chars(MAX_PREVIEW_CHARS) == (MAX_PREVIEW_CHARS, None)
    for asked in (MAX_PREVIEW_CHARS + 1, 4500, 5000, 10**9):
        assert clamp_preview_chars(asked) == (
            MAX_PREVIEW_CHARS, {"requested": asked, "applied": MAX_PREVIEW_CHARS},
        )
    for bad in (0, MIN_PREVIEW_CHARS - 1, "5000", 4500.0, True):
        with pytest.raises(InvalidPreviewCharBudget):
            clamp_preview_chars(bad)
