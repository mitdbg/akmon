"""Shared CLI output helper.

The substrate CLI used to have four slightly-different output
helpers: ``_emit_ok`` in ``cli/main.py``, ``_emit_payload`` in
``cli/poros.py``, and ``_emit`` in ``cli/federation.py`` and
``cli/nomos.py``. Each emitted a slightly different JSON envelope
for the same input; agents downstream saw shape divergence
depending on which verb they hit.

``emit_result`` is the canonical helper. It writes either a
single-line JSON document (``--json``) or a human-readable text
table for shallow payloads. Text mode renders:

* scalars as ``key: value``
* one-level nested dicts as ``key:\\n  child: value``
* lists of scalars/dicts as ``key:\\n  - item``

JSON mode always sorts keys (stable diffs) and passes
``default=str`` so datetimes/Decimals/UUIDs render to strings
instead of raising.

Deeply paginated renderers — e.g., the read-side ``okg search``
printer in ``cli/_read_side.py`` — stay in their own modules.
Their item-type sniffing and 20-item truncation are read-side
rendering for one CLI subcommand, not a generic output contract.
"""
from __future__ import annotations

import json
import sys
from typing import IO, Any, Mapping

from okg.substrate.unknown import Unknown, read_ok, to_wire


def emit_result(
    payload: Mapping[str, Any] | Any,
    *,
    as_json: bool,
    stream: IO[str] | None = None,
) -> None:
    """Print ``payload`` to ``stream`` (defaults to stdout).

    ``as_json=True`` → single-line ``json.dumps`` with sorted keys
    and ``default=str``.

    ``as_json=False`` → text rendering as documented in the module
    docstring (one-level nesting + lists). Scalar payloads print as
    ``str(payload)``.
    """
    out = stream or sys.stdout
    # The one CLI exit for read payloads: an `Unknown` fact becomes `null`
    # with its reason under `unknown_reasons` (issue #2573), in both modes.
    payload = to_wire(payload)
    if as_json:
        out.write(json.dumps(payload, sort_keys=True, default=str))
        out.write("\n")
        return
    if not isinstance(payload, Mapping):
        out.write(f"{payload}\n")
        return
    for key, value in payload.items():
        if isinstance(value, Mapping):
            out.write(f"{key}:\n")
            for child_key, child_value in value.items():
                out.write(f"  {child_key}: {child_value}\n")
        elif isinstance(value, list):
            out.write(f"{key}:\n")
            for item in value:
                out.write(f"  - {item}\n")
        else:
            out.write(f"{key}: {value}\n")


def payload_verdict_exit(
    payload: Any,
    *,
    source: str,
    stream: IO[str] | None = None,
) -> tuple[Any, int]:
    """Read a command payload's ``ok`` verdict for its exit code.

    Returns ``(payload_to_emit, exit_code)``: 0 only when ``payload["ok"]`` is
    ``True``, 1 when it is ``False``. A payload with no real boolean ``ok`` is
    an unknown verdict (issue #2573, decision D1): it fails closed with exit 1,
    one ``error: <source>_unknown: ...`` line on ``stream`` (stderr), and the
    emitted payload carries ``ok: null`` with the reason under
    ``unknown_reasons``. It is never read as a pass.
    """
    verdict = read_ok(payload, source=source)
    if isinstance(verdict, Unknown):
        (stream or sys.stderr).write(
            f"error: {source}_unknown: the {source} result carries no ok "
            f"verdict ({verdict.reason}); treating it as not ok\n"
        )
        if isinstance(payload, Mapping):
            payload = {**payload, "ok": verdict}
        return payload, 1
    return payload, 0 if verdict else 1


def command_output_tail(proc: Any, *, limit: int = 4000) -> str:
    """Both subprocess streams, labeled and tail-bounded, for a failure message.

    The substrate CLIs print their structured failure report to STDOUT
    (per-source results, skipped publishes, the ``--json`` envelope) and
    log progress/warnings to STDERR. A ``stderr or stdout`` message drops
    the report whenever any warning was logged — which made the 2026-07-17
    golden-path lane failure carry three warning lines and no cause. Render
    BOTH streams, each stripped and tail-bounded to ``limit`` chars so the
    enclosing message (often embedded in a JSON payload) stays bounded.

    Accepts anything exposing ``.stdout``/``.stderr`` — a
    ``subprocess.CompletedProcess`` or a ``subprocess.CalledProcessError``.
    """
    parts = []
    for label, text in (("stdout", getattr(proc, "stdout", None)),
                        ("stderr", getattr(proc, "stderr", None))):
        tail = (text or "").strip()
        if tail:
            parts.append(f"{label} tail:\n{tail[-limit:]}")
    return "\n".join(parts) or "(no output on either stream)"


__all__ = ["emit_result", "command_output_tail", "payload_verdict_exit"]
