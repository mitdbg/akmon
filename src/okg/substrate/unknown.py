"""Unknown as its own value (issue #2573, PACT ``unknown-as-a-value``).

When a read does not have a fact it says "unknown" and why. It never puts a
plausible value in the fact's place: not ``0`` for an unpriced model call, not
``True`` for a check that reported nothing.

* ``Unknown(reason)`` is that value. ``bool()``, ``str()`` and ``format()``
  all raise, and ``json.dumps`` does not know it (``default=str`` raises too),
  so it cannot pass a check, be printed as a number or a string, or leave the
  process silently. ``repr()`` stays readable for ``%r`` logging.
* ``read_ok(payload, source=...)`` reads a check's ``ok`` verdict and returns
  ``Unknown`` -- with the reason -- when the payload does not carry a real
  boolean, instead of reading the absence as a pass.
* ``to_wire(obj)`` is the one conversion to the wire: each ``Unknown`` becomes
  ``null`` and its reason lands in a top-level ``unknown_reasons`` map keyed by
  the dotted path. The CLI JSON emitter, the viz read routes, and the MCP tool
  registration call it (``wire_callable`` wraps a route or tool function).

Standard library only; this module imports nothing from okg.
"""
from __future__ import annotations

import dataclasses
import functools
import inspect
from collections.abc import Callable, Mapping
from typing import Any

__all__ = [
    "UNKNOWN_REASONS_KEY",
    "Unknown",
    "read_ok",
    "to_wire",
    "wire_callable",
]

#: The top-level wire key that carries ``{"dotted.path": reason}``. It is not
#: ``unknown``: payloads already use that key for counts.
UNKNOWN_REASONS_KEY = "unknown_reasons"


class Unknown:
    """A fact the read does not have, with a stable reason code.

    A plain slotted class, deliberately NOT a dataclass: if it were one,
    ``dataclasses.asdict`` on a row holding it would flatten it into
    ``{"reason": ...}`` and the marker would be lost. As it is, ``asdict``
    deep-copies it and it stays an ``Unknown``. ``to_wire`` does not walk
    dataclasses, pydantic models or sets; it refuses an ``Unknown`` inside one
    rather than let it through.
    """

    __slots__ = ("reason",)

    def __init__(self, reason: str) -> None:
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("Unknown needs a non-empty reason code")
        self.reason = reason

    def __repr__(self) -> str:
        return f"Unknown({self.reason!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Unknown):
            return self.reason == other.reason
        return NotImplemented

    def __hash__(self) -> int:
        return hash((Unknown, self.reason))

    def __bool__(self) -> bool:
        raise TypeError(
            f"Unknown has no truth value; handle it (reason: {self.reason})"
        )

    def __str__(self) -> str:
        # `json.dumps(..., default=str)` is common at our exits; without this
        # it would print the string "Unknown('...')" where a fact belongs.
        raise TypeError(
            f"Unknown has no string value; render its reason (reason: {self.reason})"
        )

    def __format__(self, format_spec: str) -> str:
        raise TypeError(
            f"Unknown cannot be formatted as a value; render its reason "
            f"(reason: {self.reason})"
        )


def read_ok(payload: Any, *, source: str) -> bool | Unknown:
    """Return ``payload["ok"]`` when it is a real boolean, else ``Unknown``.

    ``source`` names the check; the reason codes are
    ``<source>_not_an_object``, ``<source>_reported_no_ok`` and
    ``<source>_ok_not_boolean``.
    """
    if not isinstance(payload, Mapping):
        return Unknown(f"{source}_not_an_object")
    if "ok" not in payload:
        return Unknown(f"{source}_reported_no_ok")
    verdict = payload["ok"]
    if not isinstance(verdict, bool):
        return Unknown(f"{source}_ok_not_boolean")
    return verdict


def _wire(obj: Any, path: tuple[str, ...], reasons: dict[str, str]) -> Any:
    """Return ``obj`` with each Unknown replaced by None.

    Containers are rebuilt only when something below them changed, so a
    payload without an Unknown comes back as the same object.
    """
    if isinstance(obj, Unknown):
        dotted = ".".join(path)
        if dotted in reasons:
            raise ValueError(
                f"duplicate unknown path {dotted!r}: two values would share one "
                "unknown_reasons entry"
            )
        reasons[dotted] = obj.reason
        return None
    if isinstance(obj, Mapping):
        changed = False
        out: dict[Any, Any] = {}
        for key, value in obj.items():
            wired = _wire(value, (*path, str(key)), reasons)
            changed = changed or wired is not value
            out[key] = wired
        return out if changed else obj
    if isinstance(obj, (list, tuple)):
        items = [
            _wire(value, (*path, str(index)), reasons)
            for index, value in enumerate(obj)
        ]
        if all(new is old for new, old in zip(items, obj)):
            return obj
        if isinstance(obj, tuple):
            # A namedtuple keeps its type; a plain tuple stays a tuple.
            return type(obj)(*items) if hasattr(obj, "_fields") else tuple(items)
        return items
    if _is_unwalked_container(obj) and _holds_unknown(obj, set()):
        raise TypeError(
            f"to_wire does not walk a {type(obj).__name__} and one holds an "
            f"Unknown at {'.'.join(path) or '<top>'!r}; convert it to a "
            "mapping or list first"
        )
    return obj


def _is_pydantic_model(obj: Any) -> bool:
    return hasattr(type(obj), "model_fields") and hasattr(obj, "__dict__")


def _is_unwalked_container(obj: Any) -> bool:
    return (
        isinstance(obj, (set, frozenset))
        or (dataclasses.is_dataclass(obj) and not isinstance(obj, type))
        or _is_pydantic_model(obj)
    )


def _holds_unknown(obj: Any, seen: set[int]) -> bool:
    """Deep search for an Unknown, only below a container `_wire` does not walk."""
    if isinstance(obj, Unknown):
        return True
    if id(obj) in seen:
        return False
    seen.add(id(obj))
    if isinstance(obj, Mapping):
        values: Any = obj.values()
    elif isinstance(obj, (list, tuple, set, frozenset)):
        values = obj
    elif dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        values = [getattr(obj, f.name) for f in dataclasses.fields(obj)]
    elif _is_pydantic_model(obj):
        values = vars(obj).values()
    else:
        return False
    return any(_holds_unknown(value, seen) for value in values)


def to_wire(obj: Any) -> Any:
    """Replace each Unknown with None and collect the reasons at the top.

    Raises ``TypeError`` when an Unknown is found but ``obj`` is not a mapping
    (there is nowhere to put the reason), and ``ValueError`` when an existing
    ``unknown_reasons`` entry names the same path with a different reason.
    """
    reasons: dict[str, str] = {}
    wired = _wire(obj, (), reasons)
    if not reasons:
        return wired
    if not isinstance(wired, dict):
        raise TypeError(
            "an Unknown can only leave the process inside a mapping payload; "
            f"found at {sorted(reasons)!r} in a {type(obj).__name__}"
        )
    existing = wired.get(UNKNOWN_REASONS_KEY)
    merged: dict[str, str] = {}
    if isinstance(existing, Mapping):
        merged.update({str(k): v for k, v in existing.items()})
    elif existing is not None:
        raise ValueError(
            f"payload key {UNKNOWN_REASONS_KEY!r} is not a mapping; "
            "cannot record unknown reasons"
        )
    for path_key, reason in reasons.items():
        if path_key in merged and merged[path_key] != reason:
            raise ValueError(
                f"unknown_reasons conflict at {path_key!r}: "
                f"{merged[path_key]!r} vs {reason!r}"
            )
        merged[path_key] = reason
    out = dict(wired)
    out[UNKNOWN_REASONS_KEY] = dict(sorted(merged.items()))
    return out


def wire_callable(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a route or tool function so its return value goes through ``to_wire``.

    Keeps the sync/async kind and the signature (``functools.wraps`` copies
    ``__wrapped__`` and any explicit ``__signature__``), so frameworks that
    inspect parameters see the original. Wrapping a wired function again is a
    no-op. The marker names the wired function itself, because
    ``functools.wraps`` copies ``__dict__`` onto any later wrapper, and that
    wrapper may add values the inner conversion never saw.
    """
    if getattr(fn, "__okg_to_wire__", None) is fn:
        return fn
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def _async_wired(*args: Any, **kwargs: Any) -> Any:
            return to_wire(await fn(*args, **kwargs))

        _async_wired.__okg_to_wire__ = _async_wired  # type: ignore[attr-defined]
        return _async_wired

    @functools.wraps(fn)
    def _wired(*args: Any, **kwargs: Any) -> Any:
        return to_wire(fn(*args, **kwargs))

    _wired.__okg_to_wire__ = _wired  # type: ignore[attr-defined]
    return _wired
