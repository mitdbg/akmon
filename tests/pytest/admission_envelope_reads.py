"""Read ONE admission envelope, unambiguously.

A single publisher cycle records MORE THAN ONE ``okg.admission_envelopes``
row for the same derived producer: the emission decision
(``derived_edge_emission`` / ``derived_node_emission``) and, since PR #788,
the publisher's own fact-application pass under the SAME producer id
(``publisher_derived:*``). A test that looks an envelope up by
``(deployment_name, producer_id)`` alone therefore matches two rows, and
``fetchone()`` returns whichever one the scan reaches first.

That order is a physical-storage artifact, not a contract. Both rows are
equal on every column of the index such a query uses, so their relative
order is their heap position of the moment -- an UPDATE plus a vacuum swaps
it. Issue #1556: ``test_multi_input_derived_trust_uses_lowest_input`` read
the ACCEPTED application envelope instead of the REJECTED emission one on
dev run 33780127412 and asserted ``('accepted', None, None)``, while the
identical code passed in the pull request's own run.

The fix is to say which envelope you mean. ``fetch_admission_envelope``
refuses a lookup that names no intent, and refuses a lookup that matches
more than one row -- an ambiguous read fails loudly instead of returning an
arbitrary answer. ``scripts/ci/admission_envelope_lookup_lint.py`` keeps new
unscoped SQL out of ``tests/``.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg


# The only orderings a caller may ask for. `order_by` exists so an
# ordering test can prove the result does not depend on it, which needs no
# more than these; interpolating an arbitrary caller string into the SQL
# would.
ALLOWED_ORDERINGS = ("ctid", "ctid DESC", "created_at", "created_at DESC")


class AmbiguousAdmissionLookup(LookupError):
    """More than one envelope matched a lookup meant to identify one.

    A LookupError rather than an AssertionError: this is a fact about the
    rows, not a failed assertion, and an `except AssertionError` around a
    read should not be able to swallow it.
    """


def fetch_admission_envelope(
    cur: psycopg.Cursor,
    *,
    columns: Sequence[str],
    intent: str,
    deployment_name: str,
    producer_id: str | None = None,
    source: str | None = None,
    order_by: str | None = None,
) -> tuple[Any, ...] | None:
    """Return the single envelope matching this producer AND intent.

    ``columns`` are SQL expressions selected in order. ``intent`` is
    REQUIRED: it is the column that separates a producer's emission
    decision from the publisher's application pass over the same output.

    ``deployment_name`` is required for the same reason ``intent`` is: a
    lookup that does not say which deployment it means is not identifying
    anything, and a default would answer for one silently.

    ``order_by`` is accepted only so an ordering test can prove the result
    does not depend on it, and only from ``ALLOWED_ORDERINGS``; it is never
    needed for correctness, because a lookup that matches more than one row
    raises instead of picking one.

    Raises ``ValueError`` for an unscoped call and
    ``AmbiguousAdmissionLookup`` when the predicate matches several rows.
    Returns ``None`` when nothing matches.
    """
    if not intent:
        raise ValueError(
            "an admission-envelope lookup must name its intent: a producer "
            "holds both an emission envelope and a publisher_derived:* "
            "application envelope in one cycle (#1556)"
        )
    if (producer_id is None) == (source is None):
        raise ValueError(
            "pass exactly one of producer_id or source to identify the "
            "producer of the envelope"
        )
    if not columns:
        raise ValueError("columns must name at least one expression")
    if order_by is not None and order_by not in ALLOWED_ORDERINGS:
        raise ValueError(
            f"order_by must be one of {ALLOWED_ORDERINGS}, got {order_by!r}"
        )

    key_column = "producer_id" if producer_id is not None else "source"
    key_value = producer_id if producer_id is not None else source
    ordering = f"\n         ORDER BY {order_by}" if order_by else ""
    cur.execute(
        f"""
        SELECT {", ".join(columns)}
          FROM okg.admission_envelopes
         WHERE deployment_name = %(deployment_name)s
           AND intent = %(intent)s
           AND {key_column} = %(key_value)s{ordering}
        """,
        {
            "deployment_name": deployment_name,
            "intent": intent,
            "key_value": key_value,
        },
    )
    rows = cur.fetchall()
    if len(rows) > 1:
        raise AmbiguousAdmissionLookup(
            f"{len(rows)} envelopes match deployment={deployment_name!r} "
            f"{key_column}={key_value!r} intent={intent!r}; a lookup that "
            "identifies one decision must match one row"
        )
    return tuple(rows[0]) if rows else None
