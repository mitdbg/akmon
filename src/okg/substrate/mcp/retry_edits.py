"""A refusal's working call, made from the caller's own call (PACT read-refusal-retry-calls).

Many refusals of a read tool are fixed by one change to the call that was
sent: drop an argument the form does not read, or send one fixed value the
refusal names (`method="grep"`). Such a refusal declares the change here,
where it is raised, with :func:`declare_retry_edit`. The served tool
(`ByteBoundedFastMCP._okg_serve_outcome`) applies it to the caller's own
arguments, offers the result as the refusal's `retry_call` only when the
server takes that call as written (`offer_retry_call`), and names it in the
`fix_hint` as `retry: <call>`.

A change only drops arguments the caller sent or sets an argument to a
value the refusal itself names; it never adds a record, field or value the
caller did not send, so a retry names nothing a reader could not see.
"""
from __future__ import annotations

import contextvars
from typing import Any, Mapping, Optional, Sequence

#: `mode` of an offered call: `corrected` when the caller's own call
#: determines it, `guess` when the refusal names more than one fix.
MODES = ("corrected", "guess")


def declare_retry_edit(
    error: Any,
    *,
    drop: Sequence[str] = (),
    use: Optional[Mapping[str, Any]] = None,
    mode: str = "corrected",
) -> Any:
    """Declare on `error` one change to the caller's call that fixes it.

    Several declarations are tried in order; the first the server takes as
    written is offered. Returns `error`, so a site can `raise` the result.
    """
    if mode not in MODES:
        raise ValueError(f"retry edit mode must be one of {MODES}; got {mode!r}")
    edits = tuple(getattr(error, "retry_edits", None) or ())
    error.retry_edits = (*edits, {"drop": tuple(drop), "use": dict(use or {}), "mode": mode})
    return error


def declare_retry_call(
    error: Any, tool: str, arguments: Mapping[str, Any], *, mode: str = "corrected",
) -> Any:
    """Declare on `error` a whole call built from the caller's own (the
    grammar's repair of a condition), offered as an edit is: when served."""
    if mode not in MODES:
        raise ValueError(f"retry edit mode must be one of {MODES}; got {mode!r}")
    edits = tuple(getattr(error, "retry_edits", None) or ())
    error.retry_edits = (*edits, {"tool": tool, "call": dict(arguments), "mode": mode})
    return error


def edited_call(
    sent: Optional[Mapping[str, Any]], edit: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """The caller's call with `edit` applied, or None when it changes nothing.

    A drop of an argument the caller did not send is no change, so a call
    the edit does not reach is never offered as its own retry. A declared
    whole call is itself; any other edit needs the call that was sent.
    """
    if "call" in edit:
        return dict(edit["call"])
    if sent is None:
        return None
    dropped = [name for name in edit.get("drop", ()) if name in sent]
    use = dict(edit.get("use") or {})
    if not dropped and all(sent.get(name) == value for name, value in use.items()):
        return None
    call = {name: value for name, value in sent.items() if name not in dropped}
    call.update(use)
    return call


class FirstReadReached(BaseException):
    """A tool body run only to check an offered call's arguments reached
    its first read: every check before it took the call. A BaseException,
    so a body's `except Exception` cannot take it for a failure."""


#: Set while a tool body runs only to check an offered call's arguments:
#: its first database connection stops it (`stop_at_first_read`), and the
#: tracing wrapper writes no row for it.
_ARGUMENTS_ONLY: "contextvars.ContextVar[bool]" = contextvars.ContextVar(
    "okg_arguments_only", default=False,
)


def arguments_only() -> bool:
    """Whether the tool body running now only checks an offered call."""
    return _ARGUMENTS_ONLY.get()


def stop_at_first_read() -> None:
    """Called where a tool body takes a database connection. Inside an
    argument-only run it stops the body there, before any statement."""
    if _ARGUMENTS_ONLY.get():
        raise FirstReadReached()


def checking_arguments() -> "contextvars.Token[bool]":
    """Start an argument-only run; reset the returned token to end it."""
    return _ARGUMENTS_ONLY.set(True)


def end_checking_arguments(token: "contextvars.Token[bool]") -> None:
    _ARGUMENTS_ONLY.reset(token)
