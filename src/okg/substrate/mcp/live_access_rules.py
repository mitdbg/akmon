"""Live relation calls are refused while access rules are on (issue #3923).

A live call asks an outside system a question on the deployment's behalf. The
per-action live gate decides it as the deployment's service principal, not as
the reader, so it cannot stand in for the reader's access rules. Until a live
call can be decided as the reader, every live call is refused by name, before
any outside call, whenever the deployment's access rules (Nomos read
enforcement) are in ``audit`` or ``enforce`` mode, and also when that cannot
be told. "Cannot be told" is never read as "off".

One helper, :func:`refuse_live_under_access_rules`, is the whole check. Every
path that can make an outside call for a live relation calls it first:

* ``expand(live=...)`` (``server._prepare_live_for_expand``), before the
  anchor node is read and before any adapter is chosen; a ``run`` step's
  ``expand(live=...)`` comes through the same function;
* every read of a live type, a live node read and the growth of a kept live
  set (``live_resolver.check_caller``), after the surface checks and before
  any upstream call;
* ``expand(next=<token>)`` on a kept live answer (``server._expand_live_page``),
  so a page of an answer kept while the rules were off is not served once
  they are on.
"""
from __future__ import annotations

from typing import Any, Optional

from okg.substrate.errors import SubstrateError

#: Why a call was refused: the rules are on, or their state cannot be told.
REASON_ON = "access_rules_on"
REASON_UNKNOWN = "access_rules_unknown"


class LiveRefusedUnderAccessRules(SubstrateError):
    """A live relation call was refused because access rules are on (or
    their state cannot be told). Raised before any outside call."""

    code = "live_refused_under_access_rules"


def access_rules_state(server: Any) -> Optional[bool]:
    """Whether `server`'s deployment evaluates access rules (audit or
    enforce): True, False, or None when that cannot be told.

    Asks the server's own answer (``_okg_access_rules_state``, which reads the
    deployment's Nomos mode on every call) and never guesses: a server without
    that answer, or an answer that raises, is None."""

    state_of = getattr(server, "_okg_access_rules_state", None)
    if not callable(state_of):
        return None
    try:
        state = state_of()
    except Exception:  # noqa: BLE001 - unknown is said as unknown, never as off.
        return None
    return state if isinstance(state, bool) else None


def refuse_live_under_access_rules(server: Any, subject: str, **data: Any) -> None:
    """Raise :class:`LiveRefusedUnderAccessRules` unless the deployment's access
    rules are known to be off. `subject` says which call this is, for the
    message; `data` is added to the error's data."""

    rules = access_rules_state(server)
    if rules is False:
        return
    raise LiveRefusedUnderAccessRules(
        f"{subject}, but live reads are refused while access rules are on "
        "(audit/enforce)"
        + ("" if rules else ", and whether they are on cannot be told here"),
        data={
            "reason": REASON_ON if rules else REASON_UNKNOWN,
            "access_rules": "on" if rules else "unknown",
            **data,
        },
        fix_hint=(
            "read the stored graph (search, expand without live, filter, "
            "inspect); live reads under access rules are not served"
        ),
    )
