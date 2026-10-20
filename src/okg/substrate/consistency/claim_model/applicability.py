"""Typed applicability bounds and the single conflict-overlap operator.

All claim applicability — v2 dimensions/time and the v1 heading adapter —
normalizes into :class:`Applicability` before conflict evaluation.  This is the
only authoritative overlap algebra; the legacy ``scope_relation`` name is a
compatibility wrapper in ``views.py``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping

REL_OVERLAP = "overlap"
REL_UNKNOWN = "unknown"
REL_DISJOINT = "disjoint"

HEADING_ABSENT = "absent"
HEADING_KNOWN = "known"
HEADING_UNKNOWN = "unknown"
_HEADING_STATES = frozenset({HEADING_ABSENT, HEADING_KNOWN, HEADING_UNKNOWN})

TIME_ABSENT = "absent"
TIME_KNOWN = "known"
TIME_UNKNOWN = "unknown"
TIME_UNBOUNDED = "unbounded"
_TIME_STATES = frozenset({TIME_ABSENT, TIME_KNOWN, TIME_UNKNOWN, TIME_UNBOUNDED})

SCOPE_PATH_SEP = " > "


class _Omega:
    _instance: "_Omega | None" = None

    def __new__(cls) -> "_Omega":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - representation only
        return "OMEGA"


OMEGA = _Omega()
DimBound = frozenset[str] | _Omega


def _is_omega(value: object) -> bool:
    return isinstance(value, _Omega)


def _normalize_bound_map(name: str, bounds: Mapping[str, Any]) -> dict[str, DimBound]:
    if not isinstance(bounds, Mapping):
        raise ValueError(f"{name} must be an object mapping dimensions to bounds")
    out: dict[str, DimBound] = {}
    for raw_dim, value in bounds.items():
        if not isinstance(raw_dim, str) or not raw_dim:
            raise ValueError(f"{name}: dimension names must be non-empty")
        dim = raw_dim
        if _is_omega(value):
            out[dim] = OMEGA
        elif not isinstance(value, (list, tuple, set, frozenset)):
            raise ValueError(
                f"{name}[{dim!r}] must be a list/set of strings or OMEGA"
            )
        else:
            items = frozenset(value)
            if not all(isinstance(item, str) and item for item in items):
                raise ValueError(f"{name}[{dim!r}] values must be non-empty strings")
            out[dim] = items
    return out


def _intersects(a: DimBound, b: DimBound) -> bool:
    if _is_omega(a) and _is_omega(b):
        return True
    if _is_omega(a):
        return bool(b)
    if _is_omega(b):
        return bool(a)
    return bool(a & b)


@dataclass(frozen=True)
class ScopeBounds:
    """Per-dimension definite lower and possible upper applicability bounds."""

    lo: Mapping[str, DimBound] = field(default_factory=dict)
    hi: Mapping[str, DimBound] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "lo", _normalize_bound_map("lo", self.lo))
        object.__setattr__(self, "hi", _normalize_bound_map("hi", self.hi))
        for dim, lo_value in self.lo.items():
            hi_value = self.hi.get(dim, OMEGA)
            if _is_omega(lo_value):
                if not _is_omega(hi_value):
                    raise ValueError(
                        f"dimension {dim!r}: OMEGA lower bound requires OMEGA upper bound"
                    )
            elif not _is_omega(hi_value) and not lo_value <= hi_value:
                raise ValueError(
                    f"dimension {dim!r}: lower bound is not a subset of upper bound"
                )

    @classmethod
    def known(
        cls,
        dims: Mapping[str, Iterable[str]],
        *,
        vocabulary: Iterable[str] | None = None,
    ) -> "ScopeBounds":
        _check_vocabulary(dims, vocabulary)
        return cls(lo=dims, hi=dims)  # type: ignore[arg-type]

    @classmethod
    def unknown(cls) -> "ScopeBounds":
        return cls()

    @classmethod
    def unbounded(
        cls, dims: Iterable[str], *, vocabulary: Iterable[str] | None = None
    ) -> "ScopeBounds":
        if isinstance(dims, (str, bytes)):
            raise ValueError("unbounded() dimensions must be an iterable of names")
        dims = tuple(dims)
        if not dims:
            raise ValueError("unbounded() requires at least one dimension")
        if not all(isinstance(dim, str) and dim for dim in dims):
            raise ValueError("unbounded() dimension names must be non-empty strings")
        _check_vocabulary(dict.fromkeys(dims), vocabulary)
        return cls(
            lo={dim: OMEGA for dim in dims}, hi={dim: OMEGA for dim in dims}
        )

    @classmethod
    def partial(
        cls,
        lo: Mapping[str, Iterable[str]],
        hi: Mapping[str, Iterable[str] | _Omega],
        *,
        vocabulary: Iterable[str] | None = None,
    ) -> "ScopeBounds":
        _check_vocabulary(lo, vocabulary)
        _check_vocabulary(hi, vocabulary)
        return cls(lo=lo, hi=hi)  # type: ignore[arg-type]

    def dimensions(self) -> frozenset[str]:
        return frozenset(self.lo) | frozenset(self.hi)

    def lo_bound(self, dim: str) -> DimBound:
        return self.lo.get(dim, frozenset())

    def hi_bound(self, dim: str) -> DimBound:
        return self.hi.get(dim, OMEGA)

    def to_json_dict(self) -> dict[str, Any]:
        def encode(value: DimBound) -> str | list[str]:
            return "*" if _is_omega(value) else sorted(value)

        return {
            "lo": {dim: encode(value) for dim, value in sorted(self.lo.items())},
            "hi": {dim: encode(value) for dim, value in sorted(self.hi.items())},
        }

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "ScopeBounds":
        if not isinstance(payload, Mapping):
            raise ValueError("scope_bounds must be an object")
        _reject_unknown_keys(payload, {"lo", "hi"}, "scope_bounds")

        def decode(name: str) -> dict[str, DimBound]:
            raw = payload.get(name, {})
            if not isinstance(raw, Mapping):
                raise ValueError(f"scope_bounds.{name} must be an object")
            out: dict[str, DimBound] = {}
            for dim, value in raw.items():
                if not isinstance(dim, str) or not dim:
                    raise ValueError(
                        f"scope_bounds.{name} dimension names must be non-empty strings"
                    )
                if value == "*":
                    out[dim] = OMEGA
                    continue
                if not isinstance(value, list):
                    raise ValueError(
                        f"scope_bounds.{name}[{dim!r}] must be a list or '*'"
                    )
                if not all(isinstance(item, str) and item for item in value):
                    raise ValueError(
                        f"scope_bounds.{name}[{dim!r}] values must be "
                        "non-empty strings"
                    )
                out[dim] = frozenset(value)
            return out

        return cls(lo=decode("lo"), hi=decode("hi"))


def _reject_unknown_keys(
    payload: Mapping[str, Any], allowed: set[str], name: str
) -> None:
    unknown = sorted(key for key in payload if key not in allowed)
    if unknown:
        raise ValueError(f"{name} contains unknown fields {unknown}")


def _check_vocabulary(
    dims: Mapping[str, object], vocabulary: Iterable[str] | None
) -> None:
    if vocabulary is None:
        return
    allowed = frozenset(vocabulary)
    unknown = sorted(set(dims) - allowed)
    if unknown:
        raise ValueError(
            f"unknown applicability dimensions {unknown}; declared vocabulary is "
            f"{sorted(allowed)}"
        )


@dataclass(frozen=True)
class TimeBounds:
    """Claim-level half-open valid-time applicability."""

    state: str = TIME_ABSENT
    start: str = ""
    end: str = ""

    def __post_init__(self) -> None:
        if not all(isinstance(value, str) for value in (self.state, self.start, self.end)):
            raise ValueError("valid_time state/start/end must be strings")
        if self.state not in _TIME_STATES:
            raise ValueError(f"valid_time.state must be one of {sorted(_TIME_STATES)}")
        if self.state == TIME_KNOWN:
            if not self.start or not self.end:
                raise ValueError("known valid_time requires start and end")
            try:
                start = date.fromisoformat(self.start)
                end = date.fromisoformat(self.end)
            except ValueError as exc:
                raise ValueError("valid_time start/end must be ISO dates") from exc
            if start >= end:
                raise ValueError("valid_time must be a non-empty half-open interval")
            object.__setattr__(self, "start", start.isoformat())
            object.__setattr__(self, "end", end.isoformat())
        elif self.start or self.end:
            raise ValueError(f"valid_time state {self.state!r} must not set endpoints")

    @classmethod
    def known(cls, start: str, end: str) -> "TimeBounds":
        return cls(TIME_KNOWN, start, end)

    @classmethod
    def unknown(cls) -> "TimeBounds":
        return cls(TIME_UNKNOWN)

    @classmethod
    def unbounded(cls) -> "TimeBounds":
        return cls(TIME_UNBOUNDED)

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"state": self.state}
        if self.state == TIME_KNOWN:
            payload.update(start=self.start, end=self.end)
        return payload

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "TimeBounds":
        if not isinstance(payload, Mapping):
            raise ValueError("valid_time must be an object")
        _reject_unknown_keys(payload, {"state", "start", "end"}, "valid_time")
        for field_name, default in (
            ("state", TIME_ABSENT), ("start", ""), ("end", "")
        ):
            if not isinstance(payload.get(field_name, default), str):
                raise ValueError(f"valid_time.{field_name} must be a string")
        return cls(
            state=payload.get("state", TIME_ABSENT),
            start=payload.get("start", ""),
            end=payload.get("end", ""),
        )


@dataclass(frozen=True)
class Applicability:
    """All applicability axes consumed by the authoritative relation."""

    scope_bounds: ScopeBounds | None = None
    heading_path: str = ""
    heading_state: str = HEADING_ABSENT
    valid_time: TimeBounds = field(default_factory=TimeBounds)
    logical_subject: str = ""
    activity: str = ""
    modality: str = ""

    def __post_init__(self) -> None:
        if self.scope_bounds is not None and not isinstance(
            self.scope_bounds, ScopeBounds
        ):
            raise ValueError("applicability.scope_bounds must be ScopeBounds or null")
        if not isinstance(self.valid_time, TimeBounds):
            raise ValueError("applicability.valid_time must be TimeBounds")
        for name in ("heading_path", "heading_state", "logical_subject", "activity", "modality"):
            if not isinstance(getattr(self, name), str):
                raise ValueError(f"applicability.{name} must be a string")
        if self.heading_state not in _HEADING_STATES:
            raise ValueError(
                f"heading_state must be one of {sorted(_HEADING_STATES)}"
            )
        if self.heading_state != HEADING_KNOWN and self.heading_path:
            raise ValueError(
                f"heading_state {self.heading_state!r} must not carry a heading path"
            )

    @classmethod
    def from_v1(cls, scope: str, scope_state: str) -> "Applicability":
        if scope_state == HEADING_KNOWN:
            return cls(heading_path=scope, heading_state=HEADING_KNOWN)
        return cls(heading_state=HEADING_UNKNOWN)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "scope_bounds": (
                None if self.scope_bounds is None else self.scope_bounds.to_json_dict()
            ),
            "heading_path": self.heading_path,
            "heading_state": self.heading_state,
            "valid_time": self.valid_time.to_json_dict(),
        }

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "Applicability":
        if not isinstance(payload, Mapping):
            raise ValueError("applicability must be an object")
        _reject_unknown_keys(
            payload,
            {"scope_bounds", "heading_path", "heading_state", "valid_time"},
            "applicability",
        )
        raw_bounds = payload.get("scope_bounds")
        for field_name, default in (
            ("heading_path", ""),
            ("heading_state", HEADING_ABSENT),
        ):
            if not isinstance(payload.get(field_name, default), str):
                raise ValueError(f"applicability.{field_name} must be a string")
        return cls(
            scope_bounds=(
                None
                if raw_bounds is None
                else ScopeBounds.from_json_dict(raw_bounds)
            ),
            heading_path=payload.get("heading_path", ""),
            heading_state=payload.get("heading_state", HEADING_ABSENT),
            valid_time=TimeBounds.from_json_dict(payload.get("valid_time", {})),
        )


def _scope_component(
    a: ScopeBounds | None, b: ScopeBounds | None
) -> tuple[bool, bool] | None:
    if a is None and b is None:
        return None
    dims = (a.dimensions() if a is not None else frozenset()) | (
        b.dimensions() if b is not None else frozenset()
    )
    if not dims:
        return False, True
    lo = all(
        _intersects(
            a.lo_bound(dim) if a is not None else frozenset(),
            b.lo_bound(dim) if b is not None else frozenset(),
        )
        for dim in dims
    )
    hi = all(
        _intersects(
            a.hi_bound(dim) if a is not None else OMEGA,
            b.hi_bound(dim) if b is not None else OMEGA,
        )
        for dim in dims
    )
    return lo, hi


def _exact_axis(a: str, b: str) -> tuple[bool, bool] | None:
    if not a and not b:
        return None
    if not a or not b:
        return False, True
    same = a == b
    return same, same


def _heading_component(a: Applicability, b: Applicability) -> tuple[bool, bool] | None:
    if a.heading_state == b.heading_state == HEADING_ABSENT:
        return None
    if HEADING_UNKNOWN in (a.heading_state, b.heading_state):
        return False, True
    if HEADING_ABSENT in (a.heading_state, b.heading_state):
        return False, True
    sa, sb = a.heading_path, b.heading_path
    overlap = sa == sb or bool(
        sa
        and sb
        and (sa.startswith(sb + SCOPE_PATH_SEP) or sb.startswith(sa + SCOPE_PATH_SEP))
    )
    return overlap, overlap


def _time_component(a: TimeBounds, b: TimeBounds) -> tuple[bool, bool] | None:
    if a.state == b.state == TIME_ABSENT:
        return None
    if TIME_UNKNOWN in (a.state, b.state) or TIME_ABSENT in (a.state, b.state):
        return False, True
    if a.state == TIME_UNBOUNDED or b.state == TIME_UNBOUNDED:
        return True, True
    overlap = a.start < b.end and b.start < a.end
    return overlap, overlap


def applicability_relation(a: Applicability, b: Applicability) -> str:
    """Return overlap / unknown / disjoint across every declared axis."""

    components = [
        _scope_component(a.scope_bounds, b.scope_bounds),
        _heading_component(a, b),
        _time_component(a.valid_time, b.valid_time),
        _exact_axis(a.logical_subject, b.logical_subject),
        _exact_axis(a.activity, b.activity),
        _exact_axis(a.modality, b.modality),
    ]
    present = [component for component in components if component is not None]
    if not present:
        return REL_UNKNOWN
    if not all(hi for _lo, hi in present):
        return REL_DISJOINT
    if all(lo for lo, _hi in present):
        return REL_OVERLAP
    return REL_UNKNOWN
