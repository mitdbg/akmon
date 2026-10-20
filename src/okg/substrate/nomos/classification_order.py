"""Which data classification Nomos evaluates when a client also declares one.

Issue #2891: the MCP record and graph-read boundaries copied a
client-supplied ``authority.classification`` straight into the trusted
Nomos request, so a client could claim a lower class and pass a policy that
gates on classification.

The rule here: the server-side classification is the floor. A client may
declare a STRICTER class (the write or read is then evaluated under it), but
never a laxer one. A declared value that is not a known classification, or
one that cannot be compared with the server-side class, is refused before any
evaluation runs.

The ranking follows the classes the Nomos surface mappings already assign
(``vocabulary.SURFACE_DECISION_MAPPINGS``): ``public`` for the public demo
read, ``internal`` for ordinary MCP and graph reads, ``restricted`` for SQL
admin reads, export, and every host-side runtime action, and ``secret`` for
``secret.use`` alone. The four category classes the mappings never assign
(``confidential``, ``private``, ``regulated_metadata``, ``synthetic_phi``) are
the classes deployment profiles require for sensitive data, so they rank
above ``internal`` and below ``restricted``. Two different classes of the same
rank are not comparable; declaring one against the other is refused rather
than guessed.
"""
from __future__ import annotations

from typing import Any, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.vocabulary import DATA_CLASSIFICATIONS

__all__ = [
    "CLASSIFICATION_RANK",
    "DeclaredClassificationInvalid",
    "effective_classification",
]

CLASSIFICATION_RANK: Mapping[str, int] = {
    "public": 0,
    "internal": 1,
    "confidential": 2,
    "private": 2,
    "regulated_metadata": 2,
    "synthetic_phi": 2,
    "restricted": 3,
    "secret": 4,
}

# Every known class must have a rank; a new class without one would otherwise
# be refused at every boundary without explanation.
if set(CLASSIFICATION_RANK) != set(DATA_CLASSIFICATIONS):
    raise RuntimeError(
        "CLASSIFICATION_RANK must rank exactly the Nomos DATA_CLASSIFICATIONS"
    )


class DeclaredClassificationInvalid(SubstrateError):
    """A client-declared classification Nomos cannot safely use."""

    code = "nomos_declared_classification_invalid"


def effective_classification(
    *,
    server_classification: str,
    declared: Any,
    surface: str,
) -> str:
    """The classification to evaluate: the server's, raised by the client's.

    ``declared`` is the raw ``authority.classification`` value. ``None`` or an
    empty string means the client declared nothing. A declared class ranked
    below the server's is ignored (the server's class stands); one ranked
    above it is used. Anything else is refused with
    :class:`DeclaredClassificationInvalid`.
    """
    if server_classification not in CLASSIFICATION_RANK:
        raise DeclaredClassificationInvalid(
            f"server-side classification {server_classification!r} for "
            f"{surface} is not a known Nomos data classification",
            data={
                "surface": surface,
                "server_classification": server_classification,
                "declared_classification": declared,
            },
        )
    if declared is None or declared == "":
        return server_classification
    if not isinstance(declared, str) or declared not in CLASSIFICATION_RANK:
        raise DeclaredClassificationInvalid(
            f"authority.classification {declared!r} is not a known Nomos "
            f"data classification",
            data={
                "surface": surface,
                "server_classification": server_classification,
                "declared_classification": declared,
                "known_classifications": sorted(CLASSIFICATION_RANK),
            },
            fix_hint=(
                "omit authority.classification, or declare one of "
                f"{sorted(CLASSIFICATION_RANK)} at or above "
                f"{server_classification!r}"
            ),
        )
    if declared == server_classification:
        return server_classification
    declared_rank = CLASSIFICATION_RANK[declared]
    server_rank = CLASSIFICATION_RANK[server_classification]
    if declared_rank < server_rank:
        return server_classification
    if declared_rank > server_rank:
        return declared
    raise DeclaredClassificationInvalid(
        f"authority.classification {declared!r} cannot be compared with the "
        f"server-side classification {server_classification!r} for {surface}",
        data={
            "surface": surface,
            "server_classification": server_classification,
            "declared_classification": declared,
        },
        fix_hint=(
            "omit authority.classification, or declare a class ranked above "
            f"{server_classification!r}"
        ),
    )
