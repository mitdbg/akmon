"""Narrow `success_condition` predicate DSL (per D2).

Supported syntax:

  >= NUMERIC      <= NUMERIC      > NUMERIC      < NUMERIC
  == NUMERIC      != NUMERIC
  between A and B
  not between A and B

`NUMERIC` is any literal that ``float()`` accepts (integers,
decimals, signed values, scientific notation).

Anything else — multi-metric conjunctions, symbolic references,
expressions — raises :class:`PredicateParseError` with a fix-hint
naming the supported subset. The slow lane (operator-curated
``Observation`` nodes in the ``reasoning`` module) covers the
complex case.

Per `extend-substrate-experimental-with-compute-job-and-sources` D2.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

from okg.substrate.errors import SubstrateError


__all__ = [
    "Predicate",
    "PredicateParseError",
]


class PredicateParseError(SubstrateError):
    """Raised when a ``success_condition.predicate`` string does
    not match the narrow DSL declared in D2."""

    code: ClassVar[str] = "success_condition_predicate_parse_error"


_COMPARISON_RE = re.compile(
    r"^\s*(?P<op>>=|<=|==|!=|>|<)\s*(?P<num>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\s*$"
)

_BETWEEN_RE = re.compile(
    r"^\s*(?P<not>not\s+)?between\s+"
    r"(?P<lo>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\s+"
    r"and\s+"
    r"(?P<hi>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\s*$",
    re.IGNORECASE,
)


_FIX_HINT = (
    "supported predicates: '>= N', '<= N', '> N', '< N', '== N', "
    "'!= N', 'between A and B', 'not between A and B'. "
    "Use operator-curated Observation nodes (slow lane) for "
    "complex predicates."
)


@dataclass(frozen=True)
class Predicate:
    """A parsed ``success_condition.predicate`` expression.

    Use :meth:`parse` to construct from a YAML string and
    :meth:`evaluate` to test a measured value.
    """

    op: str
    """One of ``'>='``, ``'<='``, ``'>'``, ``'<'``, ``'=='``,
    ``'!='``, ``'between'``, ``'not_between'``."""

    rhs: float
    """The right-hand value for comparison ops. For ``between`` /
    ``not_between``, this is the lower bound."""

    rhs2: float | None = None
    """The upper bound for ``between`` / ``not_between``. ``None``
    for plain comparisons."""

    @classmethod
    def parse(cls, text: str) -> "Predicate":
        """Parse a predicate string. Raises
        :class:`PredicateParseError` on anything outside the DSL."""
        if not isinstance(text, str):
            raise PredicateParseError(
                f"predicate must be a string; got {type(text).__name__}",
                fix_hint=_FIX_HINT,
            )

        m = _COMPARISON_RE.match(text)
        if m is not None:
            return cls(op=m.group("op"), rhs=float(m.group("num")))

        m = _BETWEEN_RE.match(text)
        if m is not None:
            lo = float(m.group("lo"))
            hi = float(m.group("hi"))
            if lo > hi:
                raise PredicateParseError(
                    f"`between A and B` requires A <= B; got "
                    f"A={lo} B={hi} in predicate {text!r}",
                    fix_hint=_FIX_HINT,
                )
            return cls(
                op=("not_between" if m.group("not") else "between"),
                rhs=lo,
                rhs2=hi,
            )

        raise PredicateParseError(
            f"unsupported predicate {text!r}",
            fix_hint=_FIX_HINT,
        )

    def evaluate(self, value: float) -> bool:
        """Return ``True`` when ``value`` satisfies the predicate."""
        if self.op == ">=":
            return value >= self.rhs
        if self.op == "<=":
            return value <= self.rhs
        if self.op == ">":
            return value > self.rhs
        if self.op == "<":
            return value < self.rhs
        if self.op == "==":
            return value == self.rhs
        if self.op == "!=":
            return value != self.rhs
        if self.op == "between":
            assert self.rhs2 is not None
            return self.rhs <= value <= self.rhs2
        if self.op == "not_between":
            assert self.rhs2 is not None
            return not (self.rhs <= value <= self.rhs2)
        # Unreachable; parse() enforces op membership.
        raise PredicateParseError(
            f"internal: unknown predicate op {self.op!r}",
            fix_hint=_FIX_HINT,
        )
