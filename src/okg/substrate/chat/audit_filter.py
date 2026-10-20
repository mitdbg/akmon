"""The Open WebUI context-audit filter (PACT chat-context-audit-filter).

Open WebUI, not the OKG, decides what text actually reaches the model. This
module is the per-deployment filter that records it and stamps the answer.

Three jobs, in the order they happen
------------------------------------
1. :meth:`ContextAuditFilter.inlet` — before the model runs. Refuse the turn
   when compaction is enabled and nobody has proved the audit observes the
   final payload; otherwise append the immutable inlet record carrying the
   exact bytes.
2. :meth:`ContextAuditFilter.complete_turn` — at turn end, once the turn's
   MCP envelopes exist. Append the completion record with the generation the
   turn's graph reads actually observed.
3. :meth:`ContextAuditFilter.outlet` — render the answer's provenance: the
   generation stamp, one citation per cited node with its source lineage,
   and console deep links built by the ``chat-deep-links`` contract.

The refusal predicate
---------------------
``compaction_enabled AND NOT verified(compaction_on)``. Both halves matter.
A preset running with compaction OFF under a verified off-configuration
answers normally — refusing it unconditionally would turn the fail-closed
default into a total outage, which is not what "compaction is turned off for
that preset rather than audited on faith" means.

What this filter does NOT do
----------------------------
* It never writes a positive observability verdict. It imports
  :func:`~okg.substrate.chat.observability_verdict.is_verified` and nothing
  else from that module. A runtime component cannot know whether the
  operator-run proof happened.
* It never renders a per-answer ``trust_level``. The MCP read envelope does
  not carry one and nothing produces one per call; trust_level reaches a
  reader through the surfaces that do produce it (``okg dogfood status``,
  the inbox, the console Health view).
* It never defines a URL shape. Every link comes from
  :mod:`okg.substrate.chat.deep_links`; where that contract has no base for
  the deployment, the id renders as plain text with the reason.
* It holds NO cross-turn state. The stamp is a pure function of the current
  turn's envelopes (:func:`~okg.substrate.chat.audit_record.observe_generation`),
  so reusing the previous turn's stamp is not a bug this code could have —
  there is nothing to reuse.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from okg.substrate.chat.audit_record import (
    AuditArtifact,
    AuditWriteError,
    GenerationAgreement,
    GenerationObservation,
    UnknownSchemaVersion,
    canonical_payload,
    observe_generation,
    resolve_artifact_path,
    task_kind_from_metadata,
)
from okg.substrate.chat.deep_links import (
    LinkTarget,
    RenderedLink,
    Refusal,
    deep_link,
    link_target_from_row,
)
from okg.substrate.chat.observability_verdict import (
    Configuration,
    VerdictKey,
    is_verified,
    resolve_verdict_path,
)

__all__ = [
    "CITATION_HEADER",
    "CITATION_SEPARATOR",
    "STAMP_FIELD",
    "STAMP_UNAVAILABLE",
    "STAMP_CONFLICTING",
    "LINEAGE_UNAVAILABLE",
    "REFUSAL_UNVERIFIED_OBSERVABILITY",
    "REFUSAL_AUDIT_WRITE_FAILED",
    "REFUSAL_AUDIT_UNREADABLE",
    "RefusalSurface",
    "TurnRefused",
    "FilterSettings",
    "ContextAuditFilter",
    "render_citations",
    "render_provenance_block",
    "render_stamp",
]


#: The stamp's field name, spelled as ``okg dogfood status`` spells it
#: (``read_authority.generation_id``) and as the console Health view reads
#: it. One vocabulary: an operator comparing an answer against status output
#: must not have to translate.
STAMP_FIELD = "generation_id"

#: Absence is a distinct STATE, never a value. This text names the missing
#: envelope field so a reader knows what was absent, and deliberately
#: contains no id-shaped token that could be mistaken for a generation.
STAMP_UNAVAILABLE = (
    "unavailable — this answer's MCP envelope carried no pinned_generation_id"
)

#: Two graph reads in one turn pinned to different generations. Rendering
#: either one would be a synthesized stamp, so both are shown and named.
STAMP_CONFLICTING = (
    "conflicting — this turn's MCP envelopes reported more than one "
    "pinned_generation_id"
)

#: A cited row with no ``source``. Same rule as the stamp: name the absence,
#: never omit the line and never invent a lineage.
LINEAGE_UNAVAILABLE = "source lineage unavailable — the row carried no source field"

#: The citation block's own grammar, named as data because a SECOND child
#: reads it back: `chat-transcript-source` turns these bullets into
#: `(chat_message, references, X)` edges. Spelling it once means the producer
#: and the consumer cannot drift into two different formats. The values are
#: exactly what this module already rendered.
CITATION_HEADER = "Sources:"
CITATION_SEPARATOR = " — "

REFUSAL_UNVERIFIED_OBSERVABILITY = "unverified-observability-verdict"
REFUSAL_AUDIT_WRITE_FAILED = "audit-write-failed"
REFUSAL_AUDIT_UNREADABLE = "audit-artifact-unreadable"


class RefusalSurface(str, Enum):
    """How a refused turn reaches the person who sent it.

    ``RAISE`` propagates an exception out of the inlet, which is what Open
    WebUI's plugin contract nominally does with a failing filter. Whether
    that text actually reaches the user is a VENDOR behavior, measured on
    the pinned version rather than assumed; ``ASSISTANT_TURN`` is the
    re-specification the PACT names for the case where the exception proves
    invisible — the refusal is returned as the answer instead.
    """

    RAISE = "raise"
    ASSISTANT_TURN = "assistant_turn"


class TurnRefused(RuntimeError):
    """The turn was refused, with a named reason.

    A silent skip is a failure: every refusal carries a machine-readable
    ``reason`` and a sentence a reader can act on.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason

    def as_assistant_message(self) -> dict[str, str]:
        """The refusal rendered as the assistant's turn."""
        return {
            "role": "assistant",
            "content": f"**This turn was refused ({self.reason}).** {self}",
        }


@dataclass(frozen=True)
class FilterSettings:
    """Everything the filter needs, supplied by the deployment.

    No field has a runtime default that stands in for a missing value.
    ``operator_ui_base`` is ``None`` when the deployment declares none, and
    that None is a refusal in the link renderer, not a hostname guess.
    """

    deployment: str
    preset: str
    openwebui_version: str
    compaction_enabled: bool
    operator_ui_base: str | None = None
    state_home: str | os.PathLike[str] | None = None
    refusal_surface: RefusalSurface = RefusalSurface.RAISE
    environ: Mapping[str, str] | None = field(default=None, repr=False)


def render_stamp(observation: GenerationObservation) -> str | None:
    """The freshness stamp line, or None when the turn read no graph rows.

    Four observations, four renderings, none of which can be produced by
    accident from another:

    * ``NO_GRAPH_READ`` -> None (no stamp; the answer used no graph rows)
    * ``SINGLE`` -> the observed generation id
    * ``MISSING`` -> the explicit unavailable marker
    * ``CONFLICTING`` -> the named conflict plus every id observed
    """
    if observation.agreement is GenerationAgreement.NO_GRAPH_READ:
        return None
    if observation.agreement is GenerationAgreement.SINGLE:
        return f"{STAMP_FIELD}: {observation.generation_ids[0]}"
    if observation.agreement is GenerationAgreement.CONFLICTING:
        listed = ", ".join(observation.generation_ids)
        return f"{STAMP_FIELD}: {STAMP_CONFLICTING} ({listed})"
    return f"{STAMP_FIELD}: {STAMP_UNAVAILABLE}"


def _lineage_of(row: Mapping[str, Any] | object) -> str:
    """The row's source lineage, read from the shapes MCP rows actually use.

    ``provenance.source`` is what a live ``search`` row carries
    (``okg.substrate.mcp.search``); ``source`` is the flatter shape other
    read paths set. Both are read, in that order.

    ``provenance.trust`` is deliberately NOT rendered. It is a per-SOURCE
    trust marker, and putting it next to the generation stamp would read as
    the per-answer trust_level that decision B says nothing produces. Two
    different quantities that would be indistinguishable to a reader.
    """
    if not isinstance(row, Mapping):
        return LINEAGE_UNAVAILABLE
    provenance = row.get("provenance")
    if isinstance(provenance, Mapping):
        name = provenance.get("source")
        if isinstance(name, str) and name.strip():
            return name.strip()

    source = row.get("source")
    if isinstance(source, str) and source.strip():
        return source.strip()
    if isinstance(source, Mapping):
        name = source.get("source_id") or source.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return LINEAGE_UNAVAILABLE


def _rendered_link(
    row: Mapping[str, Any],
    envelope: Mapping[str, Any] | None,
    operator_ui_base: str | None,
) -> RenderedLink | None:
    target = link_target_from_row(row, envelope)
    return deep_link(target, operator_ui_base=operator_ui_base)


def render_citations(
    rows: Sequence[Mapping[str, Any]],
    *,
    envelope: Mapping[str, Any] | None,
    operator_ui_base: str | None,
) -> list[str]:
    """One citation line per cited row: the id, its lineage, and a link.

    The link is whatever ``chat-deep-links`` returns — a URL when the
    deployment declared a console base and the envelope declared a kind, and
    plain text with the stated reason otherwise. This function constructs no
    URL of its own; there is no string concatenation here that could produce
    one.
    """
    lines: list[str] = []
    for row in rows:
        if isinstance(row, Mapping):
            link = _rendered_link(row, envelope, operator_ui_base)
        else:
            # A malformed MCP result must remain visible; dropping it would
            # turn a refusal into a silent omission.
            link = deep_link(
                LinkTarget(
                    id="(malformed MCP row)",
                    kind=None,
                    refusal=Refusal.MALFORMED_ROW,
                ),
                operator_ui_base=operator_ui_base,
            )
        if link is None:
            continue
        rendered = link.to_markdown()
        lineage = _lineage_of(row)
        if link.linked:
            lines.append(f"- {rendered}{CITATION_SEPARATOR}{lineage}")
        else:
            lines.append(
                f"- {rendered}{CITATION_SEPARATOR}{lineage} "
                f"(no console link: {link.reason})"
            )
    return lines


def render_provenance_block(
    *,
    observation: GenerationObservation,
    rows: Sequence[Mapping[str, Any]] = (),
    envelope: Mapping[str, Any] | None = None,
    operator_ui_base: str | None = None,
) -> str:
    """The whole footer the outlet appends: stamp first, then citations."""
    parts: list[str] = []
    stamp = render_stamp(observation)
    if stamp is not None:
        parts.append(stamp)
    citations = render_citations(
        rows, envelope=envelope, operator_ui_base=operator_ui_base
    )
    if citations:
        parts.append(CITATION_HEADER)
        parts.extend(citations)
    return "\n".join(parts)


class ContextAuditFilter:
    """The per-deployment audit filter.

    Instantiated once per deployment chat instance. Stateless across turns
    by construction: the only mutable thing it touches is the append-only
    artifact on disk.
    """

    def __init__(self, settings: FilterSettings) -> None:
        self.settings = settings

    # -- paths -----------------------------------------------------------

    @property
    def artifact_path(self) -> Path:
        return resolve_artifact_path(
            self.settings.deployment,
            self.settings.preset,
            state_home=self.settings.state_home,
            environ=self.settings.environ,
        )

    @property
    def verdict_path(self) -> Path:
        return resolve_verdict_path(
            self.settings.deployment,
            state_home=self.settings.state_home,
            environ=self.settings.environ,
        )

    @property
    def verdict_key(self) -> VerdictKey:
        return VerdictKey(
            deployment=self.settings.deployment,
            preset=self.settings.preset,
            openwebui_version=self.settings.openwebui_version,
        )

    # -- the refusal predicate -------------------------------------------

    def observability_verified(self, configuration: Configuration) -> bool:
        """READ ONLY. This class has no path to a positive verdict write."""
        return is_verified(self.verdict_path, self.verdict_key, configuration)

    def refusal(self) -> TurnRefused | None:
        """The refusal this turn would hit, or None.

        ``compaction_enabled AND NOT verified(compaction_on)``. When
        compaction is off, the off-configuration verdict is what applies,
        and an unverified off-configuration does NOT refuse: it is the
        state every preset starts in, and the design's stated consequence of
        an unprovable payload is that compaction goes off, not that the
        deployment stops answering.
        """
        if not self.settings.compaction_enabled:
            return None
        if self.observability_verified(Configuration.COMPACTION_ON):
            return None
        return TurnRefused(
            REFUSAL_UNVERIFIED_OBSERVABILITY,
            "compaction is enabled for preset "
            f"{self.settings.preset!r} but no observability verdict records "
            f"verified_compaction_on for Open WebUI "
            f"{self.settings.openwebui_version} — the audit cannot show the "
            "bytes the model receives, so the turn is refused rather than "
            "answered with an audit that proves nothing",
        )

    # -- the three hooks --------------------------------------------------

    def inlet(
        self,
        body: Mapping[str, Any],
        *,
        metadata: Mapping[str, Any] | None = None,
        principal: str | None = None,
        provider: str | None = None,
    ) -> dict[str, Any]:
        """Record the exact bytes, or refuse the turn.

        Returns the appended inlet record. Raises :class:`TurnRefused` for
        an unverified observability verdict, and for a failed audit write —
        the filter never answers unaudited and never degrades to a partial
        record.
        """
        refusal = self.refusal()
        if refusal is not None:
            raise refusal

        meta = metadata if isinstance(metadata, Mapping) else {}
        task_kind = task_kind_from_metadata(meta)
        payload = canonical_payload(body)
        try:
            artifact = AuditArtifact(path=self.artifact_path)
        except UnknownSchemaVersion as exc:
            # Resolving the artifact can itself refuse — a damaged head
            # record, or a file that is not this artifact kind. Letting that
            # escape unwrapped would give the user a refusal with no named
            # reason, which is the silent-skip failure the contract forbids.
            raise TurnRefused(
                REFUSAL_AUDIT_UNREADABLE,
                f"the turn was not answered because its audit artifact could "
                f"not be opened: {exc}",
            ) from exc
        try:
            return artifact.append_inlet(
                deployment=self.settings.deployment,
                preset=self.settings.preset,
                conversation_id=_string_or_none(meta.get("chat_id")),
                message_id=_string_or_none(meta.get("message_id")),
                task_kind=task_kind,
                task_metadata_raw=_string_or_none(meta.get("task")),
                principal=principal,
                model=_string_or_none(body.get("model")),
                provider=provider,
                openwebui_version=self.settings.openwebui_version,
                # The CONFIGURATION. Whether this particular payload was
                # compacted is derived from the payload itself, inside the
                # writer, so no field holds a value it could not know.
                compaction_enabled=self.settings.compaction_enabled,
                payload=payload,
            )
        except AuditWriteError as exc:
            raise TurnRefused(
                REFUSAL_AUDIT_WRITE_FAILED,
                f"the turn was not answered because its audit record could "
                f"not be written ({exc.reason}): {exc}",
            ) from exc

    def inlet_or_refusal_message(
        self,
        body: Mapping[str, Any],
        **kwargs: Any,
    ) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
        """:meth:`inlet`, with the refusal delivered per ``refusal_surface``.

        ``RAISE`` re-raises, which is what Open WebUI's plugin contract does
        with a failing filter and what the v0.11.0 measurement showed
        reaches the user. ``ASSISTANT_TURN`` returns the refusal as the
        answer instead — the re-specification the PACT names for a version
        where a raised inlet exception proves invisible.

        Returns ``(record, None)`` or ``(None, refusal message)``. It never
        returns ``(None, None)``: a silently skipped turn is the failure
        mode both surfaces exist to prevent.
        """
        try:
            return self.inlet(body, **kwargs), None
        except TurnRefused as refused:
            if self.settings.refusal_surface is RefusalSurface.RAISE:
                raise
            return None, refused.as_assistant_message()

    def complete_turn(
        self,
        record_id: str,
        envelopes: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Append the completion record for a turn, joined by ``record_id``."""
        observation = observe_generation(envelopes)
        artifact = AuditArtifact(path=self.artifact_path)
        try:
            return artifact.append_completion(
                record_id=record_id,
                observation=observation,
            )
        except AuditWriteError as exc:
            raise TurnRefused(
                REFUSAL_AUDIT_WRITE_FAILED,
                f"the turn's completion record could not be written "
                f"({exc.reason}): {exc}",
            ) from exc

    def outlet(
        self,
        body: Mapping[str, Any],
        *,
        envelopes: Sequence[Mapping[str, Any]] | None = None,
        rows: Sequence[Mapping[str, Any]] = (),
        envelope: Mapping[str, Any] | None = None,
        conversation_id: str | None = None,
    ) -> dict[str, Any]:
        """Append the provenance footer to the assistant's last message.

        ``envelopes`` is every MCP envelope the turn observed (the stamp);
        ``envelope`` is the one whose rows are being cited (the link kinds).
        Passing rows without an envelope is legal and refuses every link
        with ``undeclared-kind`` rather than guessing a route from the id's
        shape.

        ``conversation_id`` is accepted and DELIBERATELY UNUSED for the
        stamp: it exists so the reuse tests can drive two different
        conversations through one filter object and show the previous
        conversation's stamp does not reappear. The stamp is a pure
        function of ``envelopes``; there is no per-conversation state for a
        conversation id to key.
        """
        del conversation_id
        observation = observe_generation(envelopes)
        block = render_provenance_block(
            observation=observation,
            rows=rows,
            envelope=envelope,
            operator_ui_base=self.settings.operator_ui_base,
        )
        result = dict(body)
        messages = [dict(m) for m in result.get("messages") or []]
        if not block:
            result["messages"] = messages
            return result
        for message in reversed(messages):
            if message.get("role") == "assistant":
                content = message.get("content")
                message["content"] = (
                    f"{content}\n\n---\n{block}" if content else block
                )
                break
        else:
            messages.append({"role": "assistant", "content": block})
        result["messages"] = messages
        return result


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
