"""Deployment-escalator fixtures for the entity-resolution tier.

These live in an importable package rather than inside a test module because
``EntityResolutionStage._escalator`` loads the configured class by dotted path
through ``importlib.import_module``; a class defined in a pytest-collected file
is not reliably importable that way.

Together they model the shape from issue 1004: a *fact*-shaped subtype whose
name is model prose (different on every extraction run) and whose identity is a
tuple of attribute slots.
"""

from __future__ import annotations

from typing import Any, Mapping

from okg.substrate.entity_resolution import ResolutionDecision, ResolutionStatus


class SlotKeyedEscalator:
    """Resolve identity from attribute slots, ignoring the node's name entirely.

    Deliberately never reads ``raw_name`` or ``normalized_key`` for matching:
    if this class can resolve, the seam is genuinely independent of the name,
    which is the property issue 1004 asks for.

    Records two things the tests assert on: every ``attrs`` mapping it was
    handed, and -- per call -- how many rows the *same* query returns over the
    bare connection versus over the checkout it was passed.
    """

    def __init__(
        self,
        *,
        slots: tuple[str, ...] = ("target_region", "uses_method"),
        synonyms: Mapping[str, str] | None = None,
        materialize: bool = False,
    ):
        self.slots = tuple(slots)
        # Domain knowledge the substrate cannot have: two spellings the model
        # uses for one method. This is why the seam has to be deployment code
        # and not a declarative hash of the slot values -- a hash of
        # "ABCD method" and "ABCD-SR" gives two identities no matter which
        # slots it covers.
        self.synonyms = dict(synonyms or {})
        # Deployments configure this through `escalation.config`, which the
        # stage splats into the constructor. True means "a later extraction of
        # this fact should refresh the canonical's attrs"; False keeps the
        # first-writer-wins behaviour that predates issue 1004.
        self.materialize = materialize
        self.seen_attrs: list[dict[str, Any]] = []
        self.raw_connection_row_counts: list[int] = []
        self.checkout_row_counts: list[int] = []

    def _key(self, attrs: Mapping[str, Any]) -> tuple[str, ...] | None:
        values = tuple(
            self.synonyms.get(folded, folded)
            for folded in (
                str(attrs.get(slot) or "").strip().casefold() for slot in self.slots
            )
        )
        return values if all(values) else None

    def resolve(
        self,
        *,
        conn: Any,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        graph_token: str,
        attrs: Mapping[str, Any],
        graph: Any,
    ) -> ResolutionDecision | None:
        del raw_name
        self.seen_attrs.append(dict(attrs))
        key = self._key(attrs)

        # Pin the measurement that justifies passing `graph` at all: the same
        # SELECT over the bare connection reads the empty Chronos base table.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.graph_nodes WHERE subtype = %s",
                (subtype,),
            )
            self.raw_connection_row_counts.append(int(cur.fetchone()[0]))

        rows = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE subtype = :subtype ORDER BY node_id",
            {"subtype": subtype},
        )
        self.checkout_row_counts.append(len(rows))

        if key is None:
            return ResolutionDecision(
                ResolutionStatus.NOVEL, None, "slot_key:no_key",
                normalized_key, None, graph_token,
            )
        matches = sorted(
            str(row["node_id"])
            for row in rows
            if self._key(row.get("attrs") or {}) == key
        )
        if not matches:
            return ResolutionDecision(
                ResolutionStatus.NOVEL, None, "slot_key:novel",
                normalized_key, None, graph_token,
            )
        if len(matches) > 1:
            return ResolutionDecision(
                ResolutionStatus.AMBIGUOUS, None, "slot_key:ambiguous",
                normalized_key, None, graph_token,
            )
        return ResolutionDecision(
            ResolutionStatus.MATCHED, matches[0], "slot_key:matched",
            normalized_key, None, graph_token, materialize=self.materialize,
        )


class MutatingEscalator:
    """Try to rewrite the attrs it was handed, then report what happened.

    The two writes exercise the two halves of the guarantee separately. The
    top-level rebind is refused outright by the read-only view, so
    ``top_level_error`` records a TypeError. The nested append is *not*
    refused -- a list inside a read-only mapping is still a list -- so
    ``nested_error`` stays None and the test's assertion moves to the caller's
    dict: the append landed on the deep copy and the original is untouched.
    Without the deep copy that second write would reach the attrs the
    extractor goes on to emit.
    """

    def __init__(self) -> None:
        self.top_level_error: str | None = None
        self.nested_error: str | None = None

    def resolve(
        self,
        *,
        conn: Any,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        graph_token: str,
        attrs: Mapping[str, Any],
        graph: Any,
    ) -> ResolutionDecision | None:
        del conn, subtype, raw_name, graph
        # Both handlers name the exact exception the write can raise rather
        # than catching broadly: a broad catch would swallow an unrelated bug
        # in this fixture and record it as if the mapping had refused the
        # write, which is the assertion under test.
        try:
            attrs["name"] = "rewritten by the escalator"  # type: ignore[index]
        except TypeError as exc:
            self.top_level_error = type(exc).__name__
        try:
            attrs["tags"].append("rewritten by the escalator")
        except (AttributeError, TypeError) as exc:
            self.nested_error = type(exc).__name__
        return ResolutionDecision(
            ResolutionStatus.NOVEL, None, "mutating:novel",
            normalized_key, None, graph_token,
        )


class LegacySignatureEscalator:
    """The pre-change escalator contract: no `attrs`, no `graph`.

    Calling it with the new keywords raises TypeError, so it doubles as the
    incompatible plugin for the call-site backstop test.
    """

    def resolve(
        self,
        *,
        conn: Any,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        graph_token: str,
    ) -> ResolutionDecision | None:
        del conn, subtype, raw_name, graph_token
        return ResolutionDecision(
            ResolutionStatus.NOVEL, None, "legacy:novel", normalized_key, None, "",
        )


class RaisingEscalator:
    """Accepts the new keywords and then fails inside."""

    def resolve(self, **_kwargs: Any) -> ResolutionDecision | None:
        raise RuntimeError("deployment escalator blew up")


class _UnreadableSignature:
    """A callable whose signature cannot be read, like a C-level callable.

    `inspect.signature` consults `__signature__` first, so raising from the
    property reproduces the unintrospectable case for real, without patching
    the stdlib `inspect` module (which would break every other caller in the
    process for the duration of the test).
    """

    @property
    def __signature__(self) -> Any:
        raise ValueError("no signature available")

    def __call__(
        self,
        *,
        conn: Any,
        subtype: str,
        raw_name: str,
        normalized_key: str,
        graph_token: str,
    ) -> ResolutionDecision | None:
        # The pre-change parameter list, spelled out rather than a hand-raised
        # TypeError: the interpreter's own binding failure is the signal under
        # test, and a simulated one could drift from what CPython actually
        # raises.
        raise AssertionError("unreachable: binding fails before the body runs")


class UnintrospectableEscalator:
    """Unintrospectable AND incompatible: the case the call-site guard owns."""

    resolve = _UnreadableSignature()
