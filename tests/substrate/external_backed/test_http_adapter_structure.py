"""The HTTP adapter's egress guarantees, enforced by structure (#2154).

Three defects in `http_adapter.py` were each found by measurement or by a
reviewer, never by anything in the code's shape: the success-path body read
sat outside the watchdog's window, then the non-2xx excerpt read did, and a
credential refusal kept the pasted token on `__context__`, where a formatted
traceback printed it. Each was fixed and pinned by a behaviour test, but
nothing stopped the NEXT read path or the next `except` from repeating them.

These rows stop it, and they are hermetic -- no sockets and no database:

* every response read goes through `_guarded_response`, which arms the
  watchdog before it hands anything out, and no other code in the module can
  reach a response object;
* every `raise` inside an `except` block in the adapter and its guard module
  either re-raises the same exception or sets its cause explicitly, so the
  wrapped exception -- and whatever secret its message carried -- cannot
  ride along as implicit context.
"""
from __future__ import annotations

import ast
import dataclasses
import threading
from pathlib import Path
from typing import Any

import pytest

import okg.substrate.external_backed.http_adapter as http_adapter

ROOT = Path(__file__).resolve().parents[3]
ADAPTER = ROOT / "src/okg/substrate/external_backed/http_adapter.py"
GUARD = ROOT / "src/okg/substrate/external_backed/_http_guard.py"


# ═══════════════════════════ exception chaining ═════════════════════════

def unchained_raises(source: str) -> list[str]:
    """Every raise in an `except` or `finally` block that would chain implicitly.

    Allowed there: a bare `raise` (the same exception, so nothing new is
    attached) and `raise X from <anything>` (`from None` drops the context;
    `from exc` names it on purpose, where a reviewer sees it). Refused:
    `raise X` with no `from`, and any call to a `_refuse*` helper, because a
    helper raises on the caller's behalf and cannot write `from None` at the
    caller's raise site.

    A `finally` block counts: it also runs while an exception is in flight,
    and a new exception raised there picks that one up as its context.

    A function or class DEFINED inside such a block is NOT exempt. Its body
    usually runs later, outside the handler -- but it can be called right
    there, and then it chains like any other raise. Strict is cheaper than
    telling the two apart.

    Known limits, recorded in the PACT: a raising helper not named
    `_refuse*`, and a raise reached through a call into another module, are
    not seen. The scan is lexical.
    """
    tree = ast.parse(source)
    found: list[str] = []

    def check(node: ast.AST) -> None:
        if (isinstance(node, ast.Raise) and node.exc is not None
                and node.cause is None):
            found.append(f"line {node.lineno}: raise without `from`")
        if isinstance(node, ast.Call):
            func = node.func
            name = (func.id if isinstance(func, ast.Name)
                    else func.attr if isinstance(func, ast.Attribute)
                    else "")
            if name.startswith("_refuse"):
                found.append(
                    f"line {node.lineno}: `{name}` raises inside an "
                    f"`except` or `finally` block"
                )

    for node in ast.walk(tree):
        blocks: list[list[ast.stmt]] = []
        if isinstance(node, ast.ExceptHandler):
            blocks.append(node.body)
        if isinstance(node, (ast.Try, ast.TryStar)):
            blocks.append(node.finalbody)
        for block in blocks:
            for stmt in block:
                for inner in ast.walk(stmt):
                    check(inner)
    # A handler nested in a handler is walked from both; report each once.
    return sorted(set(found))


@pytest.mark.parametrize("path", [ADAPTER, GUARD], ids=lambda p: p.name)
def test_no_raise_in_an_except_block_chains_implicitly(path):
    """A wrapped exception's message is how a pasted secret reached a log."""
    assert unchained_raises(path.read_text(encoding="utf-8")) == []


@pytest.mark.parametrize(
    "source, expected",
    [
        pytest.param(
            "try:\n    f()\nexcept ValueError:\n    raise KeyError('x')\n",
            1, id="implicit-chain",
        ),
        pytest.param(
            "try:\n    f()\nexcept ValueError:\n    if c:\n"
            "        raise KeyError('x')\n",
            1, id="nested-in-if",
        ),
        pytest.param(
            "try:\n    f()\nexcept ValueError:\n    _refuse('r', 'f', 'why')\n",
            1, id="refuse-helper",
        ),
        pytest.param(
            "try:\n    f()\nexcept ValueError:\n    raise KeyError('x') from None\n",
            0, id="from-none",
        ),
        pytest.param(
            "try:\n    f()\nexcept ValueError as e:\n    raise KeyError('x') from e\n",
            0, id="from-exc",
        ),
        pytest.param(
            "try:\n    f()\nexcept ValueError:\n    raise\n", 0, id="bare-reraise",
        ),
        pytest.param(
            "try:\n    raise KeyError('x')\nexcept ValueError:\n    pass\n",
            0, id="raise-in-try-body",
        ),
        pytest.param(
            "try:\n    f()\nexcept ValueError:\n    def g():\n"
            "        raise KeyError('x')\n    g()\n",
            1, id="function-defined-in-handler",
        ),
        pytest.param(
            "try:\n    f()\nfinally:\n    raise KeyError('x')\n",
            1, id="raise-in-finally",
        ),
        pytest.param(
            "try:\n    f()\nfinally:\n    raise KeyError('x') from None\n",
            0, id="finally-from-none",
        ),
    ],
)
def test_the_chaining_scan_flags_what_it_should(source, expected):
    """The scan itself, against a violation of each shape and each exemption.

    Without this row a scanner that found nothing would pass the row above.
    """
    assert len(unchained_raises(source)) == expected, unchained_raises(source)


# ═══════════════════════════ the one read path ══════════════════════════

#: Functions allowed to touch a `requests.Response`. Nested closures inside
#: them count as inside.
RESPONSE_OWNERS = frozenset({"_guarded_response", "_read_excerpt",
                             "_close_quietly"})

#: What reaching a response looks like in source. `request` is the call that
#: creates one; the rest read from one.
RESPONSE_ATTRS = frozenset({
    "request", "iter_content", "iter_lines", "raw", "content", "text",
    "json", "status_code", "headers",
})

#: The low-level readers. Only the owners may name them.
RESPONSE_READERS = frozenset({"read_bounded_body", "_read_excerpt",
                              "_close_quietly"})


def response_touches_outside_owners(source: str) -> list[str]:
    """Every way the module reaches a response outside the owning functions.

    Owners are recognised ONLY as module-level functions. A nested function
    that happened to be named `_guarded_response` inside the runner would
    otherwise be a hole shaped exactly like the exemption.

    Seen: attribute access by name, `getattr` with a constant name, the
    low-level readers by name or as a module attribute, and an import that
    renames a reader (an alias would otherwise hide it from the name check).

    Known limits, recorded in the PACT: `getattr` with a computed name,
    `exec`/`eval`, and a response handed to another module that reads it
    there. The scan is lexical.
    """
    tree = ast.parse(source)
    found: list[str] = []
    owners = {
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in RESPONSE_OWNERS
    }
    inside: set[int] = set()
    for owner in owners:
        inside.update(id(n) for n in ast.walk(owner))

    for node in ast.walk(tree):
        if id(node) in inside:
            continue
        if isinstance(node, ast.Attribute) and (
            node.attr in RESPONSE_ATTRS or node.attr in RESPONSE_READERS
        ):
            found.append(f"line {node.lineno}: `.{node.attr}`")
        if isinstance(node, ast.Name) and node.id in RESPONSE_READERS:
            found.append(f"line {node.lineno}: `{node.id}`")
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "getattr" and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value in RESPONSE_ATTRS | RESPONSE_READERS):
            found.append(
                f"line {node.lineno}: `getattr(..., {node.args[1].value!r})`"
            )
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                base = alias.name.rsplit(".", 1)[-1]
                if base in RESPONSE_READERS and alias.asname not in (
                    None, base,
                ):
                    found.append(
                        f"line {node.lineno}: `{alias.name}` imported as "
                        f"`{alias.asname}`"
                    )
    return found


def test_only_the_guarded_reader_touches_a_response():
    """No code outside `_guarded_response` can open or read a response.

    So a new read path cannot be written outside the watchdog's window: the
    runner has no response object to read, only the view the guarded reader
    yields after the watchdog is armed.
    """
    assert response_touches_outside_owners(
        ADAPTER.read_text(encoding="utf-8")
    ) == []


def test_the_response_scan_flags_a_read_outside_the_owners():
    bad = (
        "def runner(response):\n"
        "    return response.iter_content(10)\n"
        "def other(response):\n"
        "    return read_bounded_body(response, max_bytes=1, deadline=0)\n"
    )
    assert len(response_touches_outside_owners(bad)) == 2
    good = "def _guarded_response(r):\n    return r.iter_content(1)\n"
    assert response_touches_outside_owners(good) == []


@pytest.mark.parametrize(
    "source",
    [
        pytest.param(
            "def runner(r):\n    def _guarded_response():\n"
            "        return r.iter_content(1)\n",
            id="nested-function-named-like-an-owner",
        ),
        pytest.param(
            "from okg.substrate.external_backed._http_guard import "
            "read_bounded_body as rb\n",
            id="import-alias",
        ),
        pytest.param(
            "import okg.substrate.external_backed._http_guard as g\n"
            "def runner(r):\n    return g.read_bounded_body(r)\n",
            id="module-attribute",
        ),
        pytest.param(
            "def runner(r):\n    return getattr(r, 'iter_content')(1)\n",
            id="getattr-constant",
        ),
    ],
)
def test_the_response_scan_sees_the_cheap_bypasses(source):
    """Each shape the review named as a way around the name check."""
    assert response_touches_outside_owners(source), source


def test_the_view_carries_no_response_object():
    """The runner's view is four named fields, none of them the response."""
    fields = {f.name for f in dataclasses.fields(http_adapter._UpstreamResponse)}
    assert fields == {"status", "content_type", "read_body", "read_excerpt"}


# ═════════════════════ the watchdog is armed on entry ═══════════════════

class _Recorder:
    def __init__(self) -> None:
        self.events: list[str] = []


class _FakeResponse:
    def __init__(self, rec: _Recorder, chunks: list[bytes]) -> None:
        self._rec = rec
        self._chunks = chunks
        self.headers = {"Content-Type": "application/json"}

    @property
    def status_code(self) -> int:
        self._rec.events.append("status")
        return 200

    def iter_content(self, _size: int):
        self._rec.events.append("read")
        yield from self._chunks

    def close(self) -> None:
        self._rec.events.append("close")


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def request(self, *_a: Any, **_k: Any) -> _FakeResponse:
        self._response._rec.events.append("request")
        return self._response


def _config() -> Any:
    return http_adapter.HttpBindingConfig(
        relation="r", scheme="http", address="127.0.0.1", port=9,
        allowed=(), allow_non_global=True, method="GET", path="/",
        content_type="application/json", auth_header=None, body=None,
        rows_path=(), max_rows=1, max_response_bytes=1024,
    )


@pytest.fixture
def recorded_timer(monkeypatch):
    rec = _Recorder()
    real = threading.Timer

    class Timer(real):  # type: ignore[misc,valid-type]
        def start(self) -> None:
            rec.events.append("watchdog-armed")
            super().start()

        def cancel(self) -> None:
            rec.events.append("watchdog-cancelled")
            super().cancel()

    monkeypatch.setattr(http_adapter.threading, "Timer", Timer)
    return rec


def test_the_watchdog_is_armed_before_anything_reads_the_response(
    recorded_timer,
):
    import time

    rec = recorded_timer
    session = _FakeSession(_FakeResponse(rec, [b"[1]"]))
    with http_adapter._guarded_response(
        session, _config(), payload=None, headers={},
        deadline=time.monotonic() + 5,
    ) as upstream:
        assert upstream.read_body(1024) == b"[1]"
    assert rec.events[:3] == ["request", "watchdog-armed", "status"], rec.events
    assert rec.events.index("watchdog-armed") < rec.events.index("read")
    assert rec.events[-2:] == ["watchdog-cancelled", "close"], rec.events


def test_the_watchdog_is_cancelled_and_the_response_closed_on_a_refusal(
    recorded_timer,
):
    import time

    rec = recorded_timer
    session = _FakeSession(_FakeResponse(rec, [b"x" * 2048]))
    with pytest.raises(http_adapter.HttpGuardRefusal) as caught:
        with http_adapter._guarded_response(
            session, _config(), payload=None, headers={},
            deadline=time.monotonic() + 5,
        ) as upstream:
            upstream.read_body(1024)
    assert caught.value.reason == "response_over_cap"
    assert "watchdog-cancelled" in rec.events
    assert rec.events[-1] == "close", rec.events


class _ShortReadResponse(_FakeResponse):
    """Yields part of a body, then goes quiet past the deadline and ENDS.

    That is what a read cut by the watchdog looks like from the reader's
    side: no exception, just fewer bytes than the upstream meant to send.
    """

    def iter_content(self, _size: int):
        import time

        self._rec.events.append("read")
        yield b"[1"
        time.sleep(0.4)


def test_a_short_read_after_the_deadline_is_a_timeout_not_a_body(
    recorded_timer,
):
    """The read comes back SHORT; it must read as a timeout, not a body.

    Checked inside the guarded reader, so no caller can forget the check
    and parse a truncated body as a smaller answer.
    """
    import time

    rec = recorded_timer
    session = _FakeSession(_ShortReadResponse(rec, []))
    with pytest.raises(http_adapter.HttpGuardRefusal) as caught:
        with http_adapter._guarded_response(
            session, _config(), payload=None, headers={},
            deadline=time.monotonic() + 0.2,
        ) as upstream:
            upstream.read_body(1024)
    assert caught.value.reason == "deadline_exceeded"
    assert rec.events[-1] == "close", rec.events


def test_a_nesting_bomb_maps_to_response_over_cap_not_the_backstop():
    """#2170 at the mapping: the guard's reason has a declared failure class."""
    assert http_adapter._GUARD_FAILURES["body_too_deep"] == "response_over_cap"
