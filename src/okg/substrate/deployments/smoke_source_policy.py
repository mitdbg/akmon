"""Suitability policy for deployment smoke sources.

A deployment's ``runtime.smoke_sources`` drive the ``smoke_publish`` phase of
``okg deployment up``: each named source is synced and published as the
post-migration canary, and the risk-admitted backup planner requires that
publish to produce a real node/edge delta (the "must change live rows" gate).
A source that can never emit new content therefore makes ``smoke_publish`` a
silent no-op that fails that gate — which is exactly what happened when
okg-workspace shipped ``smoke_sources: [openspec]`` (the 2026-07-22/23
catch-up deploy; PR #854 switched it to the active ``pact`` source).

There is no machine ``frozen``/``active``/``lifecycle`` field on ``SourceEntry``
today — "OpenSpec is frozen" is a *policy* statement (CLAUDE.md: "OpenSpec is
retired — author every change as a PACT"; ``pact/project.yaml``:
``openspec_authoring: frozen``). So :data:`FROZEN_SMOKE_SOURCE_ADAPTERS` below
is the reviewed single-source-of-truth for "frozen", maintained like code (the
same pattern as ``scripts/ci/fail_open_allowlist.yaml``). Promoting "frozen" to
a first-class ``SourceEntry`` field, and teaching the runtime smoke-source
classifier (``deployment_upgrade._target_smoke_inventory``) to consult it, is a
principled follow-up tracked by the deployment-lifecycle-e2e-ci PACT; this
module keeps the policy in one importable place so both the static meta-check
and a future runtime consumer share it.
"""
from __future__ import annotations

from typing import Any

# Adapter module paths of sources that are frozen/retired: they remain in a
# source registry so their historical content still projects into the graph,
# but they can no longer emit fresh records, so a ``smoke_publish`` of them can
# never change live rows. Unsuitable as a deploy-up smoke canary. Reviewed like
# code; add an entry only for a source that is genuinely append-frozen.
FROZEN_SMOKE_SOURCE_ADAPTERS: frozenset[str] = frozenset(
    {
        # OpenSpec authoring is frozen repo-wide; the history/openspec/ tree is
        # read-only history (content hash never advances).
        "okg.substrate.library.sources.openspec",
    }
)


class SmokeSourceUnsuitable(ValueError):
    """A configured smoke source cannot serve as a ``deployment up`` canary."""


# A source can produce a fresh published generation only if it is versioned and
# publishes generations. Mirrors the runtime smoke-source classifier's
# ``versioned`` predicate (deployment_upgrade._target_smoke_inventory).
_PUBLISHABLE_REVISION_KINDS: frozenset[str] = frozenset(
    {"content_hash", "git_commit", "immutable_version"}
)


def _raw(entry: Any) -> dict[str, Any]:
    """The verbatim registry mapping for a source ``entry``.

    ``entry`` is a ``SourceEntry`` (duck-typed to avoid an import cycle); its
    ``raw`` mapping carries the verbatim registry keys (``module``,
    ``source_revision_kind``, ``publication_mode``, ...).
    """
    raw = getattr(entry, "raw", None)
    return raw if isinstance(raw, dict) else {}


def check_smoke_source_suitable(deployment: str, name: str, entry: Any) -> None:
    """Refuse loudly unless ``name`` is an active, non-frozen, deployment-owned
    smoke source for ``deployment``.

    ``entry`` is the resolved ``SourceEntry`` for ``name`` from the deployment's
    source registry, or ``None`` when the name is absent. The checks are:

    - **deployment-owned** by the portable ``ownership_id`` prefix rather than
      the Nomos admission predicate (``producer_kind``/``trust_label``), because
      not every deployment populates an ``admission_policy`` block (e.g. okg-ci's
      ``pact`` entry) yet those sources are still legitimate canaries;
    - **not frozen** — a reviewed denylist of retired adapter *modules*. This is
      the narrow exception for a source that is structurally publishable yet
      frozen in *content* (openspec: ``content_hash`` + ``published_generation``,
      but its ``history/openspec/`` tree is read-only so the hash never
      advances). It is
      keyed on ``module`` (a non-mandatory registry field), so the positive
      capability check below is the general gate;
    - **active / publishable** — versioned (``source_revision_kind`` in a
      publishable set) and ``publication_mode == published_generation``, so its
      ``smoke_publish`` can actually change live rows. Mirrors the runtime
      classifier, catching registry-only / legacy / non-published sources the
      denylist would miss.

    Raises:
        SmokeSourceUnsuitable: if the source is missing, not deployment-owned,
            frozen, or not publish-capable.
    """
    if entry is None:
        raise SmokeSourceUnsuitable(
            f"{deployment}: smoke source {name!r} is not declared in the "
            f"deployment's source registry"
        )
    ownership_id = str(getattr(entry, "ownership_id", None) or "")
    if not ownership_id.startswith(f"{deployment}."):
        raise SmokeSourceUnsuitable(
            f"{deployment}: smoke source {name!r} is not deployment-owned "
            f"(ownership_id={ownership_id!r} must start with {deployment + '.'!r})"
        )
    raw = _raw(entry)
    module = str(raw.get("module") or "")
    if module in FROZEN_SMOKE_SOURCE_ADAPTERS:
        raise SmokeSourceUnsuitable(
            f"{deployment}: smoke source {name!r} uses the frozen/retired "
            f"adapter {module!r}. A frozen source can never emit new content, "
            f"so its smoke_publish is a no-op that fails the deploy-up "
            f"must-change-live-rows gate. Point smoke_sources at an active, "
            f"deployment-owned source (e.g. `pact`)."
        )
    revision_kind = raw.get("source_revision_kind")
    publication_mode = raw.get("publication_mode")
    if (
        revision_kind not in _PUBLISHABLE_REVISION_KINDS
        or publication_mode != "published_generation"
    ):
        raise SmokeSourceUnsuitable(
            f"{deployment}: smoke source {name!r} cannot produce a published "
            f"generation (source_revision_kind={revision_kind!r}, "
            f"publication_mode={publication_mode!r}); a smoke canary must be a "
            f"versioned, published-generation source so its smoke_publish "
            f"changes live rows."
        )
