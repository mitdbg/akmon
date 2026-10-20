"""Hermeticity wrapper — structural enforcement of declared sources.

The umbrella ``views-contract`` capability declares hermeticity as
a structural property: synthesize SHALL receive a wrapper that
exposes only the anchors declared in ``spec.sources``; any attempt
to access an anchor not in the declared sources SHALL raise
``HermeticityViolation``. The wrapper MUST NOT expose iteration
methods or a silent-default ``get`` that would let a planner
discover undeclared anchors at synthesize time.

This module is the implementation. It is the only place in the
framework that decides what synthesize can read.
"""
from __future__ import annotations

from collections.abc import Iterable

from okg.views.types import Anchor


class HermeticityViolation(KeyError):
    """Raised when synthesize tries to read an anchor not in
    ``spec.sources``.

    The exception message names the violating anchor id and the
    declared sources so the failure points directly at the offending
    planner.
    """


class RestrictedCorpus:
    """Wraps a full corpus, exposing only the anchors in ``allowed_ids``.

    Use::

        restricted = RestrictedCorpus(full_corpus, spec.sources)
        anchor = restricted[some_id]   # raises if some_id not in spec.sources

    The wrapper deliberately does NOT define ``__iter__``,
    ``keys()``, ``values()``, ``items()``, or ``get(default=...)``.
    All access flows through ``__getitem__`` and therefore through
    the hermeticity check. This is structural enforcement: a
    planner cannot accidentally — or sneakily — discover undeclared
    anchor ids at synthesize time.

    Callers that want "this anchor or None" must write::

        anchor = corpus[id] if id in spec.sources else None

    so the intent (skip an undeclared anchor on purpose) is
    explicit and visible at the call site.
    """

    __slots__ = ("_full", "_allowed")

    def __init__(
        self,
        full_corpus: dict[str, Anchor],
        allowed_ids: Iterable[str],
    ) -> None:
        self._full: dict[str, Anchor] = full_corpus
        self._allowed: frozenset[str] = frozenset(allowed_ids)

    def __getitem__(self, anchor_id: str) -> Anchor:
        if anchor_id not in self._allowed:
            raise HermeticityViolation(
                f"synthesize tried to read anchor {anchor_id!r}, "
                f"not declared in sources {sorted(self._allowed)!r}"
            )
        # The id is in the allow-list, but the underlying full corpus
        # may not actually contain it (e.g. a planner declared a
        # source that turned out to be missing upstream). Surface that
        # as a KeyError on the full corpus rather than a hermeticity
        # violation — the latter would mislead the caller into
        # thinking the planner had a hermeticity bug.
        return self._full[anchor_id]

    def __contains__(self, anchor_id: object) -> bool:
        # Allow membership checks against the allow-list. This is
        # what callers use to write the explicit "anchor or None"
        # pattern documented above. We do NOT consult ``self._full``
        # here — the question we answer is "is this id declared in
        # this fragment's sources?", not "does the full corpus have
        # it?". Mixing the two would let a planner do
        # ``"x" in corpus`` to discover undeclared anchor ids.
        return anchor_id in self._allowed

    # Explicitly NOT implemented. These would defeat structural
    # hermeticity: a planner could iterate the full corpus or query
    # for keys it never declared. We document the omission in
    # __getattr__ so the failure mode is loud (AttributeError with a
    # message) rather than silent.

    _BLOCKED_ATTRS = frozenset({
        "keys",
        "values",
        "items",
        "get",
    })

    def __getattr__(self, name: str):  # pragma: no cover - never reached for normal attrs
        if name in self._BLOCKED_ATTRS:
            raise AttributeError(
                f"RestrictedCorpus deliberately does not implement "
                f"{name!r}; access anchors by declared id via "
                f"corpus[anchor_id] only."
            )
        raise AttributeError(
            f"RestrictedCorpus has no attribute {name!r}"
        )

    def __iter__(self):
        # Block iteration explicitly — without this, frozenset's
        # __iter__ would not be inherited, but a `for x in corpus`
        # call would still call __getitem__ with integer indices and
        # fail with HermeticityViolation. That's a confusing error
        # message; raise something specific here instead.
        raise TypeError(
            "RestrictedCorpus is not iterable. Access anchors by "
            "declared id via corpus[anchor_id]."
        )

    def __repr__(self) -> str:
        return f"RestrictedCorpus(allowed={sorted(self._allowed)!r})"
