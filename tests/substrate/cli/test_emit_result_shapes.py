"""Regression tests for ``emit_result`` (the canonical CLI output
helper).

Pinned by ``extract-cli-shared-options-and-output``. Before this
test landed, the substrate CLI shipped four near-duplicates of the
helper (``_emit_ok``, ``_emit_payload``, ``_emit`` x2). These
shapes ARE the canonical ones the migrated callers
(``cli/federation``, ``cli/poros``, ``cli/nomos``) depend on; any
divergence here ripples through every poros/federation/nomos
verb.
"""
from __future__ import annotations

import io
import json
from datetime import datetime
from decimal import Decimal

from okg.substrate.cli._output import emit_result


def _capture(payload, *, as_json: bool) -> str:
    buf = io.StringIO()
    emit_result(payload, as_json=as_json, stream=buf)
    return buf.getvalue()


def test_emit_result_json_sorts_keys() -> None:
    out = _capture({"b": 2, "a": 1}, as_json=True)
    assert out == '{"a": 1, "b": 2}\n'


def test_emit_result_json_uses_default_str_for_non_serializable() -> None:
    """Datetime / Decimal / UUID-style objects must NOT raise.
    They round-trip via ``default=str``."""
    when = datetime(2026, 1, 2, 3, 4, 5)
    out = _capture({"when": when, "amount": Decimal("1.5")}, as_json=True)
    parsed = json.loads(out)
    assert parsed["when"] == "2026-01-02 03:04:05"
    assert parsed["amount"] == "1.5"


def test_emit_result_text_flat_dict() -> None:
    out = _capture({"status": "ok", "count": 3}, as_json=False)
    assert out == "status: ok\ncount: 3\n"


def test_emit_result_text_one_level_nested_dict() -> None:
    out = _capture(
        {"summary": {"approved": 5, "rejected": 2}},
        as_json=False,
    )
    assert out == "summary:\n  approved: 5\n  rejected: 2\n"


def test_emit_result_text_list_value() -> None:
    out = _capture(
        {"messages": ["alpha", "beta"]},
        as_json=False,
    )
    assert out == "messages:\n  - alpha\n  - beta\n"


def test_emit_result_text_scalar_payload() -> None:
    out = _capture("just a string", as_json=False)
    assert out == "just a string\n"


def test_emit_result_stream_kwarg_redirects_output() -> None:
    err = io.StringIO()
    emit_result({"ok": False}, as_json=True, stream=err)
    assert err.getvalue() == '{"ok": false}\n'


def test_emit_result_default_stream_is_stdout(capsys) -> None:
    emit_result({"x": 1}, as_json=False)
    captured = capsys.readouterr()
    assert captured.out == "x: 1\n"
    assert captured.err == ""
