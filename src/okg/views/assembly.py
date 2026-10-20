"""assemble() — concatenate fragments into the rendered output.

The default assembler emits ``"\\n\\n".join(f.content for f in
view.fragments)``. Views that need structural formatting (markdown
headers, table assembly from per-cell quotes, blockquote wrappers,
references resolution) provide their own ``assembler`` callable
through their :class:`ViewDefinition`.

Pure deterministic. No hash comparisons. No LLM. The umbrella's
Property A guarantees that ``assemble`` over an unchanged-fragment
view produces byte-identical output across runs because the
fragment list is the same Python object.

Per ``add-portal-fragment-annotations`` (2026-04-27), this module
also exposes :func:`assemble_with_markers` which produces a
parallel artifact carrying the byte-clean output, a markered
variant (HTML-comment-wrapped per fragment), and per-fragment
byte offsets into the byte-clean output. The graph node's
``markdown_body`` attr continues to carry the byte-clean output
only — markers never appear in the contract surface.

Per ``refine-af-pubs-llm-views`` (2026-04-28), this module also
exposes :func:`translate_src_citations`, which rewrites
``[@src:<anchor_id>]`` markers in LLM-generated prose into
numeric ``[N]`` chips that the viz's ``activateCitationChips``
post-parse pass can chip-ify.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from okg.views.types import View


# Citation translation — substrate-level, used by every af_pubs
# LLM view assembler. Lives here (not in deployments/) so other
# deployments can lean on the same convention.
_SRC_CITE_RE = re.compile(r"\[@src:([^\]\s]+)\]")


def translate_src_citations(
    content: str,
) -> tuple[str, list[dict]]:
    """Replace ``[@src:<anchor_id>]`` markers in `content` with
    numeric ``[N]`` markers (1-indexed by first-occurrence order).

    Returns `(rewritten_content, citations)` where `citations` is
    a list of `{"n": int, "src_anchor_id": str}` records covering
    each unique anchor cited, in first-seen order. Multiple
    occurrences of the same anchor receive the same `[N]`.

    Deterministic on `content`: identical input prose produces
    identical output prose AND identical citations list. Property
    A holds at the post-translation fragment-content layer.

    Examples
    --------
    >>> rewritten, cites = translate_src_citations(
    ...     "X [@src:foo] Y [@src:bar] Z [@src:foo]"
    ... )
    >>> rewritten
    'X [1] Y [2] Z [1]'
    >>> cites
    [{'n': 1, 'src_anchor_id': 'foo'}, {'n': 2, 'src_anchor_id': 'bar'}]
    """
    seen: dict[str, int] = {}
    citations: list[dict] = []

    def _repl(match: re.Match) -> str:
        aid = match.group(1)
        if aid not in seen:
            seen[aid] = len(seen) + 1
            citations.append({
                "n": seen[aid],
                "src_anchor_id": aid,
            })
        return f"[{seen[aid]}]"

    rewritten = _SRC_CITE_RE.sub(_repl, content)
    return rewritten, citations

# Fragment separator. Centralized here so default assembler and
# `assemble_with_markers` stay byte-aligned: the offsets the
# markered variant records into the byte-clean output assume the
# same join-with-blank-line shape the default emitter uses.
_FRAGMENT_SEPARATOR = "\n\n"


def assemble(view: View) -> str:
    """Default assembler: blank-line-separated concatenation.

    Fragment ordering is preserved as the planner emitted it.
    Empty content (rare) does not produce an empty paragraph
    artifact — adjacent blank fragments collapse via the join
    semantics.
    """
    return _FRAGMENT_SEPARATOR.join(
        f.content for f in view.fragments if f.content
    )


# ---------------------------------------------------------------------------
# Markered variant — for portal-side per-fragment annotations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FragmentOffset:
    """One fragment's byte range within an :class:`AssembledWithMarkers`'
    ``markdown`` field.

    ``markdown[start:start + length]`` is byte-equal to the
    fragment's content as it appears in the byte-clean output.
    Frozen + tuple-friendly for serialization to
    ``fragment_offsets.json`` on disk.
    """

    fragment_id: str
    start: int
    length: int

    @classmethod
    def from_dict(cls, raw: dict) -> "FragmentOffset":
        return cls(
            fragment_id=str(raw["fragment_id"]),
            start=int(raw["start"]),
            length=int(raw["length"]),
        )

    def to_dict(self) -> dict:
        return {
            "fragment_id": self.fragment_id,
            "start": self.start,
            "length": self.length,
        }


@dataclass(frozen=True)
class AssembledWithMarkers:
    """Rich assembled-output artifact for portal consumers.

    Three fields:

    * ``markdown``: byte-clean output, byte-equal to
      ``assemble(view)`` for the same view. Programmatic
      consumers (MCP, agent tools, replay) read this.
    * ``markered``: byte-clean output spliced with HTML comments
      around each fragment's content
      (``<!--frag:{fragment_id} start-->`` /
      ``<!--frag:{fragment_id} end-->``). Portal renderer reads
      this to wrap each fragment's text in
      ``<span data-fragment-id>`` after markdown render.
    * ``fragment_offsets``: tuple of :class:`FragmentOffset` —
      ``markdown[start:start + length]`` is the fragment's
      content as a slice of the byte-clean output. The offsets
      address the BYTE-CLEAN output (NOT the markered output);
      consumers that need fragment-level reads can do that
      directly without parsing comments.

    The umbrella's Property A holds end-to-end: when fragment
    contents are byte-reused, every field of
    :class:`AssembledWithMarkers` is byte-identical across runs.
    """

    markdown: str
    markered: str
    fragment_offsets: tuple[FragmentOffset, ...]


def assemble_with_markers(view: View) -> AssembledWithMarkers:
    """Render the byte-clean output AND the markered variant
    using the default fragment-only assembler.

    The byte-clean output is byte-equal to ``assemble(view)``.
    The markered variant wraps each fragment's content in
    ``<!--frag:{id} start-->`` / ``<!--frag:{id} end-->`` HTML
    comments. The fragment-offsets tuple gives each fragment's
    `(start, length)` byte range in the BYTE-CLEAN output.

    Empty-content fragments (rare) are skipped exactly as the
    default ``assemble`` skips them.

    Custom assemblers that need per-fragment markers should use
    the :class:`MarkeredAssembly` builder, write fragment
    content via ``write_fragment``, and call ``build()`` to
    produce the same artifact shape.
    """
    builder = MarkeredAssembly()
    first = True
    for fragment in view.fragments:
        if not fragment.content:
            continue
        if not first:
            builder.write(_FRAGMENT_SEPARATOR)
        first = False
        builder.write_fragment(fragment.id, fragment.content)
    return builder.build()


# ---------------------------------------------------------------------------
# MarkeredAssembly — builder for custom assemblers
# ---------------------------------------------------------------------------


class MarkeredAssembly:
    """Builder for custom assemblers that need per-fragment markers.

    Two write methods:

    * ``write(text)`` — plain prose, structure, headers; not
      attributed to any fragment. Bytes flow into both the
      byte-clean and markered outputs identically.
    * ``write_fragment(fragment_id, content)`` — content that's
      attributable to one fragment. Bytes flow into the byte-
      clean output verbatim; the markered output wraps them in
      ``<!--frag:{id} start--> ... <!--frag:{id} end-->`` and
      a :class:`FragmentOffset` records the byte range in the
      byte-clean output.

    ``build()`` returns a frozen :class:`AssembledWithMarkers`.
    Re-using the builder after build is a usage error (not
    enforced; just keep one builder per assembler invocation).

    Custom assemblers may emit a fragment in multiple chunks:
    the framework concatenates byte-cleanly and emits one
    ``FragmentOffset`` per ``write_fragment`` call. Two
    consecutive calls with the same ``fragment_id`` are allowed
    and produce two offset entries (a fragment whose rendered
    chunks are split by intervening structure).
    """

    def __init__(self) -> None:
        self._chunks: list[str] = []
        self._markered: list[str] = []
        self._offsets: list[FragmentOffset] = []
        self._cursor: int = 0

    def write(self, text: str) -> None:
        """Emit plain text not attributed to any fragment."""
        if not text:
            return
        self._chunks.append(text)
        self._markered.append(text)
        self._cursor += len(text)

    def write_fragment(self, fragment_id: str, content: str) -> None:
        """Emit fragment-attributed text, bracketing the markered
        variant with ``<!--frag:{id}-->`` comments.
        """
        if not content:
            # Empty fragment chunk: no offset emitted, no
            # markers written. Mirrors the default assembler's
            # "skip empty content" rule.
            return
        self._offsets.append(FragmentOffset(
            fragment_id=fragment_id,
            start=self._cursor,
            length=len(content),
        ))
        self._chunks.append(content)
        self._markered.append(
            f"<!--frag:{fragment_id} start-->"
        )
        self._markered.append(content)
        self._markered.append(
            f"<!--frag:{fragment_id} end-->"
        )
        self._cursor += len(content)

    def build(self) -> AssembledWithMarkers:
        return AssembledWithMarkers(
            markdown="".join(self._chunks),
            markered="".join(self._markered),
            fragment_offsets=tuple(self._offsets),
        )


__all__ = [
    "AssembledWithMarkers",
    "FragmentOffset",
    "MarkeredAssembly",
    "assemble",
    "assemble_with_markers",
    "translate_src_citations",
]
