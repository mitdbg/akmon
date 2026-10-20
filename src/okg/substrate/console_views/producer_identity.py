"""The one identity rule every console read uses for a producer.

A deployment's ``source_registry.yaml`` gives each source two names:

  - the document key, which is what an operator calls it (``bib-references``);
  - the declared ``ownership_id``, which is what the runtime attributes its
    records, its counts and its runs to (``okg-workspace.bib_references``).

For most sources the second is ``<deployment>.<key>``, so joining on that
string works by accident. It is not a rule. ``deployments/okg-ci`` declares
``ci-triage-verdicts`` with ``ownership_id: agent:okg-ci-triage``, which no
normalisation of underscores, hyphens or separators derives from the key, and
``deployments/okg-workspace`` declares ``bib-references`` as
``okg-workspace.bib_references``. A console read that builds its own key from
the document name therefore misses those sources' own counts, reports the
configured row as empty, and lists the unmatched runtime key beside it as a
second, unconfigured producer. That is the double count this module exists to
prevent: on an Archiv3 deployment it turned five healthy sources into eight
with three needing review.

The registry is the authority and the loader already parses it, enforces that
``ownership_id`` is unique within a deployment, and caches on the file's mtime.
This module is the only place the console turns a deployment into producer
identities; there is no second rule in ``console_views``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProducerIdentity:
    """One configured producer, under both the names it is known by."""

    #: The registry document key: what the operator configured and sees.
    name: str
    #: The declared ``ownership_id``: what the runtime attributes work to, and
    #: the key of the console count rollup and of ``okg.source_progress``.
    ownership_id: str


def registry_identities(deployment_name: str) -> list[ProducerIdentity]:
    """The configured producers of a deployment, with both of their names.

    Returns ``[]`` only when the deployment has no registry file at all. A
    registry that exists but cannot be resolved, parsed or admitted is a
    user-visible read failure, never an empty source list: a console that
    silently showed no sources would read as a deployment that configures
    none.
    """
    try:
        from okg.substrate.deployments import find_deployment_dir

        dep_dir = find_deployment_dir(deployment_name)
        if dep_dir is None:
            return []
        path = Path(dep_dir) / "source_registry.yaml"
        if not path.is_file():
            return []

        from okg.substrate.sources.registry import load_source_registry

        entries = load_source_registry(path)
        return [
            ProducerIdentity(name=str(name), ownership_id=str(entry.ownership_id))
            for name, entry in entries.items()
        ]
    except Exception:
        logger.exception(
            "failed to load source registry for deployment=%s",
            deployment_name,
        )
        raise


def runtime_key_index(
    identities: list[ProducerIdentity],
) -> dict[str, ProducerIdentity]:
    """Every recorded key that resolves to a producer, mapped to it.

    The ``ownership_id`` is the rule. ``okg.source_progress.source_name`` is
    the one column that does not keep to it: rows written before the ownership
    id was recorded carry the registry document key instead, which is why the
    old composer had to special-case a doubly prefixed alias. Both names are
    therefore indexed, and a name that two producers would claim is an
    ambiguous registry rather than a row silently given to whichever came
    first.
    """
    index: dict[str, ProducerIdentity] = {}
    for identity in identities:
        for key in (identity.ownership_id, identity.name):
            claimed = index.get(key)
            if claimed is not None and claimed is not identity:
                raise ValueError(
                    f"source registry is ambiguous: {key!r} is claimed by both "
                    f"{claimed.name!r} (ownership_id {claimed.ownership_id!r}) "
                    f"and {identity.name!r} (ownership_id "
                    f"{identity.ownership_id!r})"
                )
            index[key] = identity
    return index


def is_unlisted_alias_key(
    key: str,
    *,
    deployment_name: str,
    by_key: dict[str, ProducerIdentity],
    has_counts: Callable[[], bool],
) -> bool:
    """Whether a recorded key nothing claims is a legacy spelling to omit.

    ``okg.source_progress.source_name`` carries ``<deployment>.<name>`` on
    rows written before the ownership id was recorded. Where the registry
    declares an ``ownership_id`` that is not that string (``agent:okg-ci-triage``
    for ``ci-triage-verdicts``), the historical row matches neither of the
    producer's names. Listed, it is a second producer under the same displayed
    name as the configured one. Such a key is omitted when it carries no
    records and no edges of its own; the caller names it in
    ``alias_keys_not_listed`` so the omission is on the record. A prefixed key
    that does carry counts is a producer of its own and stays listed.

    ``has_counts`` is called only for a key that is such a spelling, so a
    caller may read the counts lazily. ``key`` must not be in ``by_key``.
    """
    prefix = deployment_name + "."
    if not key.startswith(prefix) or key[len(prefix):] not in by_key:
        return False
    return not has_counts()


def registry_source_names(deployment_name: str) -> list[str]:
    """The document keys alone, for callers that only name the configured set."""
    return [identity.name for identity in registry_identities(deployment_name)]


#: `source_names_unavailable_reason` when the deployment has no registry file.
NO_SOURCE_REGISTRY = "no_source_registry_for_deployment"


def source_display_names(deployment_name: str) -> tuple[dict[str, str], str | None]:
    """Each declared ``ownership_id`` mapped to the source's declared name.

    For record, search and attempt reads, which carry a row's ``source`` as
    the ownership id (``cern-r3final.twiki-eos``) while Sources names the same
    producer by its declared name (``twiki_eos``). Those reads serve the name
    beside the id as ``source_name``, so a page can print one name for one
    source on every route.

    Returns the map and ``None``, or an empty map and the reason there is no
    name to serve. Unlike `registry_identities` an unreadable registry does
    not raise here: a search or a record must still load when the registry
    does not parse, and the reason says why every ``source_name`` is null.
    """
    try:
        from okg.substrate.deployments import find_deployment_dir

        dep_dir = find_deployment_dir(deployment_name)
        if dep_dir is None or not (Path(dep_dir) / "source_registry.yaml").is_file():
            return {}, NO_SOURCE_REGISTRY
        identities = registry_identities(deployment_name)
    except Exception as exc:  # noqa: BLE001 -- named in the payload, and logged
        logger.exception(
            "source display names unavailable for deployment=%s", deployment_name,
        )
        return {}, f"source_registry_unreadable:{type(exc).__name__}"
    return {identity.ownership_id: identity.name for identity in identities}, None
