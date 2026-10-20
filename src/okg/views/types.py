"""Core data types for the views layer.

Three value objects: :class:`Anchor`, :class:`Fragment`, :class:`View`.
Plus the canonical content-hash function :func:`content_hash` and
its underlying normalizer :func:`_normalize`.

The umbrella ``views-contract`` capability (in
``openspec/changes/establish-views-contract``) declares the
cross-cutting invariants these types serve. The
``add-incremental-views`` change owns this implementation slice.

Design points worth keeping in mind:

* ``Anchor`` is *frozen*: it is the atom of corpus input, and we
  rely on its hash being a pure function of its content. Editing
  an anchor's content produces a *new* anchor (same id, new hash);
  it is not an in-place mutation.

* ``Fragment`` carries its own dependency record via
  ``source_hashes``. The ``__post_init__`` validator enforces that
  ``source_hashes`` covers exactly ``sources`` — no missing keys,
  no extras. This binding is what the umbrella's Property A
  reasons over: when every anchor in a fragment's sources has the
  same hash as recorded, the fragment is reused byte-identically
  by ``refresh()`` without invoking any synthesizer or LLM.

* ``View`` carries an optional ``generation_id`` that pins the
  substrate generation a production view was rendered against.
  Snapshot-JSON test fixtures leave it ``None``.

The hash function uses sha256 truncated to 16 hex characters
(64 bits). At the af_pubs scale (~13K anchors live, ~1M body-text
anchors across history) the birthday-collision probability is
< 1e-10. Widen to the full 64 hex chars later if a deployment
approaches 1e9 anchors — old hashes are prefixes of new hashes,
so widening is a non-breaking schema change.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Literal


# ---------------------------------------------------------------------------
# Content normalization + hashing
# ---------------------------------------------------------------------------


_WHITESPACE_RE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    """Collapse whitespace runs to a single space; strip ends.

    The single pure normalizer applied whenever content is hashed.
    Deliberately weak: trivial whitespace edits (a wrapped line, a
    trailing newline, tabs vs spaces between words) do not produce
    different hashes. Substantive content edits do.

    Known limitation: this normalizer is correct for prose. Anchors
    that carry text where whitespace is semantic (code, tab-separated
    tables, YAML) need a different builder; the views layer does not
    ship one yet.
    """
    return _WHITESPACE_RE.sub(" ", text.strip())


def content_hash(text: str) -> str:
    """Canonical content-hash function: ``sha256(_normalize(text))[:16]``.

    Returns a 16-character hex string (64 bits truncated). Used by
    :meth:`Anchor.from_content` and by the refresh loop's stability
    predicate.
    """
    digest = hashlib.sha256(_normalize(text).encode("utf-8")).hexdigest()
    return digest[:16]


def prompt_hash(system_prompt: str, task_prompt: str) -> str:
    """Identity of a generated fragment's *prompt closure*.

    The refresh loop reuses a cached generated fragment only when its
    source anchors are unchanged. But a fragment's content also depends
    on the ``system_prompt`` + ``task_prompt`` the planner supplied: two
    runs with the same sources but a different prompt produce different
    output. Recording this hash on the fragment (and comparing it on
    reuse) folds the prompt into the reuse key, so editing a prompt busts
    stale fragment reuse (#693).

    Unlike :func:`content_hash`, this does NOT collapse whitespace:
    prompt whitespace can be semantically meaningful (fenced blocks,
    indentation), so an exact hash is used. The two prompts are joined
    with a NUL so ``("ab", "c")`` and ``("a", "bc")`` never collide.
    """
    payload = f"{system_prompt}\x00{task_prompt}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Anchor — atom of corpus input
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Anchor:
    """The atom of a view's corpus input.

    A planner declares its sources by anchor id; the synthesizer
    reads anchor content to build fragment content. Anchors are
    immutable; editing content produces a new anchor with the same
    id and a different ``content_hash``.

    Construct via :meth:`from_content` so the hash is derived from
    the canonical normalizer. The bare constructor accepts an
    explicit hash for round-tripping from a manifest.
    """

    id: str
    content: str
    content_hash: str

    @classmethod
    def from_content(cls, id: str, content: str) -> "Anchor":
        """Build an anchor whose ``content_hash`` is computed from
        ``content`` using the canonical normalizer.
        """
        return cls(id=id, content=content, content_hash=content_hash(content))


# ---------------------------------------------------------------------------
# Fragment — atom of view output
# ---------------------------------------------------------------------------


FragmentKind = Literal["quote", "generated"]


@dataclass(frozen=True)
class Fragment:
    """An atom of a view's rendered output.

    ``kind == "quote"``: ``content`` is a byte-literal slice of a
    single source anchor. ``len(sources) == 1`` typically, but the
    framework does not enforce that — a multi-source quote (e.g.
    "first 3 sentences of body, then a catalog row") is allowed if
    a planner builds one explicitly.

    ``kind == "generated"``: ``content`` is the LLM's response
    grounded in the declared sources.

    ``source_hashes`` is the per-fragment dependency record. The
    refresh loop's ``is_stable`` predicate checks that every
    ``aid in sources`` has the same content hash now as recorded
    here. ``__post_init__`` enforces that ``source_hashes`` covers
    exactly the ids in ``sources``.
    """

    id: str
    kind: FragmentKind
    sources: tuple[str, ...]
    source_hashes: dict[str, str]
    content: str
    display_label: str = ""
    # Identity of the prompt closure (system + task prompt) for a
    # ``generated`` fragment; empty for ``quote`` fragments, which invoke
    # no LLM. The refresh loop compares this against the current spec's
    # prompt so editing a prompt busts reuse (#693).
    prompt_hash: str = ""

    def __post_init__(self) -> None:
        # The umbrella spec says: "source_hashes MUST contain exactly
        # the ids in sources and no others." Enforce it here so
        # malformed fragments can never reach refresh().
        sources_set = frozenset(self.sources)
        hashes_set = frozenset(self.source_hashes.keys())
        if sources_set != hashes_set:
            missing = sources_set - hashes_set
            extra = hashes_set - sources_set
            parts = []
            if missing:
                parts.append(f"missing source_hashes for {sorted(missing)!r}")
            if extra:
                parts.append(f"unexpected source_hashes keys {sorted(extra)!r}")
            raise ValueError(
                f"Fragment {self.id!r}: source_hashes must cover sources "
                f"exactly; {'; '.join(parts)}"
            )


# ---------------------------------------------------------------------------
# View — ordered list of fragments
# ---------------------------------------------------------------------------


@dataclass
class View:
    """A rendered view: an ordered list of fragments plus identity.

    ``view_id`` and ``view_def_hash`` together identify a specific
    rendering. ``manifest_version`` lets future schema changes
    dispatch on the manifest format. ``generation_id`` pins the
    substrate generation this view was rendered against
    (``None`` for views built from snapshot-JSON test fixtures).

    Not frozen: ``fragments`` is mutated during fresh generation
    (planner emits specs, synthesize fills them in, the list grows).
    Once a view is persisted to a manifest, treat it as immutable.
    """

    view_id: str
    view_def_hash: str
    manifest_version: int = 1
    fragments: list[Fragment] = field(default_factory=list)
    generation_id: int | None = None
