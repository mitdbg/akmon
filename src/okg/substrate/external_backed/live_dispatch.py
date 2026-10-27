"""Dispatch one subtype-declared live relation, gated by Nomos.

Issue #2013, PACT ``live-relations-on-expand``, Sprint 20 charter #2008.

THE ORDER OF ONE CALL, AND WHY IT IS THIS ORDER
-----------------------------------------------
1. Resolve the relation from the target node's subtype.
2. Validate the arguments against the model compiled at load.
3. Resolve the principal (service mode only this sprint) -- EXPLICITLY, by
   the relation's declared mode. An unresolved subject denies the call with
   `live_principal_unresolved` before step 4 and never reaches step 5.
4. Ask Nomos with the DECLARED action, the resolved principal, and the node
   as resource.
5. **On allow only**, run the adapter, under the relation's declared deadline,
   ENFORCED (not advised).
6. Write the sample, in its own short transaction, AFTER the adapter returns.
7. Return rows in the ``external_live`` envelope, beside the pinned graph
   rows and never merged into them.

Steps 4 and 5 are two statements with nothing between them and step 5 is
inside the ``if`` -- not a flag consulted later. The tests count ADAPTER
CALLS rather than inspecting the returned state, because a state check
cannot tell "denied" from "called, then discarded".

Steps 1 and 2 precede the gate on purpose: a malformed argument is a client
error and should never become a policy question, or every typo becomes a
Nomos decision and a sample row. That ordering has a consequence to carry
forward -- under caller mode it is an EXISTENCE ORACLE, because a denied
caller still learns which relations exist from the difference between
"unknown relation" and "unknown argument". Under service mode the principal
is the runtime itself and nothing leaks. Resolving it is a precondition on
the caller-mode follow-up, recorded on #2013.

NO RESULT CACHE (lead ruling, 2026-09-20)
-----------------------------------------
Every live call goes gate then adapter. There is no cache on this path and
this PACT does not add one, for two reasons. A cached allow could outlive the
policy change that should have stopped it -- the gate is consulted per call,
but a cache hit that skipped the adapter would still be serving bytes fetched
under a permission that has since been revoked. And a relation is a ROW QUERY
against a live system, not an idempotent view: "the same arguments" does not
mean "the same answer", which is the premise a cache needs.

`canonical_args_fingerprint` survives, but only as the QUERY IDENTITY recorded
on the sample, so two calls can be recognised as the same question after the
fact. It is not a key and nothing looks anything up with it.

NO TRANSACTION IS HELD ACROSS THE NETWORK CALL
----------------------------------------------
The graph read closes before the adapter runs and the sample is written
afterwards in a transaction of its own. Holding a connection through an
external call is how one slow endpoint becomes a connection-pool outage for
every other caller of the deployment, and a sample is a record of what
happened, so it cannot be written before the thing it records.

When the sample write fails, the call FAILS -- and a structured failure log
goes out of band naming the relation, the node, the decision and, crucially,
whether the adapter ran. Rolling back would not un-call the external system;
it would only erase the evidence that it was called. There is no intent-row
mechanism: a pre-call intent row is a second write on the hot path that
still would not prove the adapter ran.
"""
from __future__ import annotations

import collections
import hashlib
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional

from okg.substrate.db.concurrency_budget import width as budget_width
from okg.substrate.errors import SubstrateError
from okg.substrate.external_backed.live_relations import (
    BoundLiveRelation,
    LiveBindingUndeclaredRelation,
    LiveRelation,
    LiveRelationUnbound,
)

log = logging.getLogger(__name__)

#: How many live calls may be in flight in one process at once.
#:
#: Drawn from the `live.adapter_calls` term rather than named here. A bare
#: literal is what `tests/substrate/test_concurrency_budget_guard.py`
#: refuses, and rightly: an unregistered pool width is a lane the budget's
#: feasibility check cannot see. The term declares zero connections per
#: worker, because a live call holds a thread and somebody else's socket,
#: never one of ours.
MAX_CONCURRENT_LIVE_CALLS = budget_width("live.adapter_calls")

#: Bounds concurrent live calls. Module-level because the thing it protects --
#: this process's threads -- is module-level too.
_LIVE_SLOTS = threading.BoundedSemaphore(MAX_CONCURRENT_LIVE_CALLS)

#: ONE executor for the whole process, sized by the same term. Previously a
#: fresh single-worker `ThreadPoolExecutor` per call, which churned a thread
#: on every dispatch and hid the real bound in a literal. Sharing it makes
#: the width the term says it is, and an abandoned task holds one of these
#: workers until its I/O returns -- which is exactly the resource the
#: semaphore is counting.
_LIVE_EXECUTOR = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_LIVE_CALLS, thread_name_prefix="okg-live",
)

#: Emitted out of band when a sample write fails. Named so an operator can
#: alert on exactly this, and so the string is greppable in a log pipeline.

SAMPLE_FAILURE_EVENT = "okg.live_relation.sample_write_failed"

#: What a live call can come back as. `ok` is the only one that carries rows.
LIVE_STATES = frozenset({"ok", "denied", "error", "timeout"})


#: Longest upstream excerpt an adapter may attach to an error. Bounded
#: because an upstream error page is untrusted text arriving on the
#: least-inspected path there is.
MAX_ERROR_EXCERPT_CHARS = 256

#: Fixed, safe messages. The dispatcher NEVER passes an exception's own
#: message through: a network exception's `str()` routinely carries the URL
#: it was dialling, userinfo and all.
_FAILURE_MESSAGES: Mapping[str, str] = {
    "adapter_error": "the adapter could not complete the call",
    "adapter_raised": "the adapter raised an unexpected error",
    "result_unusable": "the adapter returned a result this call cannot shape",
    "shaping_failed": (
        "the adapter answered, and shaping its result for the response failed"
    ),
    "timeout": "the adapter did not answer within the declared deadline",
    # #2100. Four classes the HTTP adapter can distinguish, each a different
    # operational problem: the upstream is down or said no; it tried to send
    # the request somewhere else; it answered with more than we will hold;
    # it answered with something that is not the agreed shape. One generic
    # message for all four would make them one alert.
    "upstream_unavailable": (
        "the external system refused the connection or returned an error "
        "status"
    ),
    # The system is up and answered, but its tool reports "nothing matched"
    # as an error. Its own message is the excerpt.
    "upstream_empty_result": (
        "the external system answered that this query matched nothing; no "
        "rows were served"
    ),
    # The system is up and answered that the object the arguments name does
    # not exist there (Rucio: "Data identifier not found", "No replication
    # rule found"). Not an outage: the arguments are what to check.
    "upstream_not_found": (
        "the external system answered that what this call names was not "
        "found there; that is its answer, not an outage, so check the "
        "arguments"
    ),
    # The system is up and answered that it cannot use these arguments
    # (Rucio: "Provided RSE expression is considered invalid"). Not an
    # outage and not an empty match: the argument's grammar is what to check.
    "upstream_rejected_arguments": (
        "the external system rejected these arguments as invalid; that is "
        "its answer, not an outage: check the argument's grammar (inspect "
        "publishes it)"
    ),
    "redirect_refused": (
        "the external system answered with a redirect, which is never "
        "followed: the target is outside every check this call made"
    ),
    "response_over_cap": (
        "the response exceeded the declared size limit and was refused "
        "rather than truncated"
    ),
    "response_unusable": (
        "the response was not usable JSON in the shape this relation "
        "declares"
    ),
}

#: Fallback when a bundle answers with no reason code of its own.
GATE_DENY_REASON = "nomos.default_deny"

#: The subject a live call runs as under ``principal_mode: service`` when the
#: binding declares no ``service_principal`` of its own. Named here and
#: resolved by :func:`resolve_live_principal` on one explicit branch, because
#: the thing it replaced -- ``principal or "service:okg-runtime"`` -- turned a
#: MISSING identity into the one privileged subject (#2160). A default chosen
#: for a mode is not the same as a fallback from an empty value: the first is
#: the answer to "who does the runtime act as", the second is "we do not know,
#: so assume the most powerful answer".
DEFAULT_SERVICE_PRINCIPAL = "service:okg-runtime"

#: Recorded and returned when identity does not resolve. Registered in
#: docs/reference/error_codes.yaml beside the other `live_*` codes; it has no
#: exception class because it is a DENY, not a raise -- the call comes back in
#: its envelope with `state: denied`, and its sample row is written like any
#: other deny.
PRINCIPAL_UNRESOLVED_REASON = "live_principal_unresolved"

#: What the sample stores as the principal of a call that had none. The sample
#: store requires non-empty text, and a blank would read as a missing column
#: rather than as the fact being recorded. It is deliberately not a principal
#: any policy can name.
UNRESOLVED_PRINCIPAL = "unresolved"

#: The reason code recorded when the GATE ITSELF fails -- an unreadable
#: bundle, a malformed policy, an import error. Distinct from a policy deny
#: on purpose: "policy said no" and "we could not ask" are different
#: operational problems and must not read the same in the sample table.
GATE_ERROR_REASON = "nomos.gate_error"


class LiveRelationUndeclared(SubstrateError):
    """`expand(live=...)` named a relation the node's subtype does not declare."""

    code = "live_relation_undeclared"


class LiveArgumentsInvalid(SubstrateError):
    """`expand(live=...)` arguments failed the declared schema."""

    code = "live_arguments_invalid"


class LiveRelationsUnboundDeployment(SubstrateError):
    """`expand(live=...)` on a deployment that binds no live relation.

    Its own code, separate from "this subtype declares no such relation".
    "This deployment binds none" is an operator configuration fact the agent
    cannot fix by renaming its relation, and telling it to pick another name
    would send it round a loop with no exit.
    """

    code = "live_relations_unbound_deployment"


class LiveSampleWriteFailed(SubstrateError):
    """The call ran (or was denied) and its sample could not be written.

    The call fails. An uncounted external call is exactly what the sample
    policy exists to prevent, so "the adapter ran but we have no record"
    is not a state this substrate returns rows from.
    """

    code = "live_sample_write_failed"


@dataclass(frozen=True)
class GateOutcome:
    """What the gate answered, with enough detail for the sample row.

    `build_probe_gate` returns a bare bool, which loses the reason code the
    sample has to record. This carries it. The boolean gate stays as it is so
    the #83 hydration path is untouched.
    """

    allowed: bool
    reason: str
    principal: str
    action: str
    policy_fingerprint: str | None = None
    raised: bool = False


def canonical_args_fingerprint(validated: Mapping[str, Any]) -> str:
    """The QUERY IDENTITY recorded on the sample. Not a cache key.

    Fingerprints the VALIDATED argument dump, not the raw input: defaults
    applied, keys sorted, strings NFC-normalised, numbers rendered stably.
    So ``{"q": "x"}`` and ``{"q": "x", "size": 10}`` -- where 10 IS the
    declared default -- are recognisably the same question in the sample
    table, which is what makes "how often is this asked" answerable.

    There is no cache on this path (see the module docstring), so nothing is
    looked up with this value. If a future change adds one, the resolved
    principal must join the fingerprint first: today one service principal
    per deployment makes it unambiguous, and caller mode would not.
    """
    def norm(value: Any) -> Any:
        if isinstance(value, str):
            return unicodedata.normalize("NFC", value)
        if isinstance(value, bool):
            return value
        if isinstance(value, float) and value.is_integer():
            # 10.0 and 10 are the same argument to every adapter this
            # substrate runs; rendering them differently would split the
            # cache on a distinction no caller made.
            return int(value)
        if isinstance(value, Mapping):
            return {k: norm(v) for k, v in sorted(value.items())}
        if isinstance(value, (list, tuple)):
            return [norm(v) for v in value]
        return value

    blob = json.dumps(
        norm(dict(validated)), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def build_live_gate(
    deployment_name: str,
    *,
    deployments_root: Any = None,
) -> Callable[[LiveRelation, Optional[str], Optional[str]], GateOutcome]:
    """A Nomos gate that asks about the relation's DECLARED action.

    Two differences from :func:`external_backed.gating.build_probe_gate`,
    which this deliberately mirrors rather than replaces:

    * the action is the relation's, not the constant ``external.probe.invoke``;
    * the resource carries the NODE as well as the deployment, so a bundle can
      allow a relation on some nodes and not others.

    Everything else is the posture that module already had and that is already
    right: no bundle on disk is a gate that always denies, and any effect that
    is not exactly ``allow`` is a deny.

    It FAILS CLOSED. `gating.py` has no try/except at all, so an exception
    from bundle compilation or from `evaluate` propagates out of the gate --
    and a propagating exception is not a deny, it is a deny with NO SAMPLE,
    which loses the record that a call was attempted. Here it becomes a deny
    that is recorded, with its own reason code so "policy said no" and "we
    could not ask policy" never read the same.

    The bundle compiles ONCE, here, which is worth stating plainly: editing
    the bundle on disk does not take effect until the server is relaunched.
    That is a restart, not a silent stale allow -- the gate is consulted on
    every call, ahead of any cache.
    """
    from pathlib import Path

    from okg.substrate.external_backed.gating import BUNDLE_FILENAME

    bundle: Any = None
    compile_error: str | None = None
    try:
        from okg.substrate.nomos.core import (
            compile_policy_graph,
            load_policy_graph,
        )

        root = deployments_root
        if root is None:
            from okg.substrate.deployments import find_deployments_root

            root = find_deployments_root()
        path = Path(root) / deployment_name / BUNDLE_FILENAME
        if path.is_file():
            bundle = compile_policy_graph(load_policy_graph(path))
    except Exception as exc:  # noqa: BLE001 — see the fail-closed note above.
        compile_error = f"{type(exc).__name__}: {exc}"
        log.warning(
            "live relation gate for deployment %r could not compile its "
            "policy bundle (%s); every live call will be denied with "
            "reason %s until it is fixed",
            deployment_name, compile_error, GATE_ERROR_REASON,
        )

    def gate(
        relation: LiveRelation,
        principal: Optional[str],
        node_id: Optional[str],
    ) -> GateOutcome:
        # NO FALLBACK. This line used to read
        # ``subject = principal or "service:okg-runtime"``, so an unresolved
        # principal was authorized as the runtime itself -- the wrong
        # direction for a gate that otherwise fails closed (#2160). A missing
        # subject is not a policy question either: there is nothing to ask
        # about. It is refused here, before the bundle is consulted, so the
        # answer does not depend on whether a bundle happens to be on disk.
        subject = (principal or "").strip()
        if not subject:
            return GateOutcome(
                allowed=False, reason=PRINCIPAL_UNRESOLVED_REASON,
                principal=UNRESOLVED_PRINCIPAL, action=relation.action,
            )
        if compile_error is not None:
            return GateOutcome(
                allowed=False, reason=GATE_ERROR_REASON, principal=subject,
                action=relation.action, raised=True,
            )
        try:
            from okg.substrate.nomos.core import DecisionInput, evaluate

            decision = evaluate(
                bundle,
                DecisionInput(
                    subject=subject,
                    action=relation.action,
                    resource={
                        "deployment": deployment_name,
                        "relation": relation.name,
                        "subtype": relation.subtype,
                        "node_id": node_id,
                    },
                    classification="internal",
                ),
                enforcement_point="external.live_relation.invoke",
            )
        except Exception as exc:  # noqa: BLE001 — fail closed, and RECORD it.
            log.warning(
                "live relation gate raised for %r on %r (%s: %s); denying",
                relation.name, node_id, type(exc).__name__, exc,
            )
            return GateOutcome(
                allowed=False, reason=GATE_ERROR_REASON, principal=subject,
                action=relation.action, raised=True,
            )
        effect = getattr(decision, "effect", "deny")
        return GateOutcome(
            allowed=effect == "allow",
            reason=(
                getattr(decision, "reason_code", None) or GATE_DENY_REASON
            ),
            principal=subject,
            action=relation.action,
            policy_fingerprint=getattr(decision, "policy_fingerprint", None),
        )

    return gate


def resolve_live_principal(
    bound: BoundLiveRelation, principal: Optional[str],
) -> str:
    """The subject this call acts as, or ``""`` when identity is missing.

    ONE owner for one effect (#2049 cluster 3). The resolution branches on the
    relation's DECLARED ``principal_mode`` rather than or-ing candidates
    together:

    * ``service`` -- the binding's ``service_principal`` if the operator
      declared one, else :data:`DEFAULT_SERVICE_PRINCIPAL`. The caller's own
      principal is NOT consulted: under service mode the call is the runtime's,
      and reading a caller value here would make the subject depend on who
      asked while the policy was written for the runtime.
    * anything else (``caller``, refused at load today) -- the caller's
      principal and nothing else.

    Returning ``""`` rather than a default is the whole point: the caller
    decides what an unresolved identity means, and in this module it means
    deny.
    """
    if bound.relation.principal_mode == "service":
        declared = bound.binding.service_principal or DEFAULT_SERVICE_PRINCIPAL
        return (declared or "").strip()
    return (principal or "").strip()


#: Longest refusal hint that still prints on the reply's first line. The
#: text renderer moves a hint over 200 characters off the status line
#: (``retry: see fix_hint``), which is exactly where an agent stops reading.
RETRY_LINE_CHARS = 200

#: Longest declared pattern quoted in the one-line hint. A pattern is
#: operator text and can be long; past this it is cut there, and the message
#: and `inspect` carry the whole of it.
MAX_PATTERN_CHARS = 60

#: Characters tried in place of `*` to tell "the pattern rejects the
#: wildcard" from "the value is wrong in some other way too".
_WILDCARD_STAND_INS = ("a", "A", "0", "x1")


def _short_pattern(pattern: str) -> str:
    if len(pattern) <= MAX_PATTERN_CHARS:
        return pattern
    return pattern[: MAX_PATTERN_CHARS - 1] + "…"


#: Longest caller value the wildcard probe runs Python's backtracking `re`
#: over. The argument model's own check is linear; this probe only words a
#: refusal, so a longer value gets the plain pattern refusal instead.
PROBE_VALUE_CHARS = 256


def _backtracks(pattern: str) -> bool:
    """Whether `pattern` holds a repeat whose body repeats or branches
    (`(a+)+`, `(a|aa)*`): the forms Python's `re` can take exponential time
    on. A pattern this cannot read answers True (no probe)."""
    try:
        from re import _constants as _C  # type: ignore[attr-defined]
        from re import _parser as _P  # type: ignore[attr-defined]

        repeats = {_C.MAX_REPEAT, _C.MIN_REPEAT}
        possessive = getattr(_C, "POSSESSIVE_REPEAT", None)
        if possessive is not None:
            repeats.add(possessive)

        def holds(items: Any, *, inside: bool) -> bool:
            for op, av in items:
                if op in repeats:
                    if inside or holds(av[2], inside=True):
                        return True
                elif op is _C.BRANCH:
                    if inside:
                        return True
                    if any(holds(branch, inside=inside) for branch in av[1]):
                        return True
                elif op is _C.SUBPATTERN:
                    if holds(av[3], inside=inside):
                        return True
                elif op in (_C.ASSERT, _C.ASSERT_NOT):
                    if holds(av[1], inside=inside):
                        return True
            return False

        return holds(_P.parse(pattern), inside=False)
    except Exception:  # noqa: BLE001 - unreadable means no probe.
        return True


def _only_wildcards_rejected(
    pattern: str, value: Any, *, max_length: int | None = None,
) -> bool:
    """True when `value` has a `*` and would match `pattern` without it.

    Checked with Python's `re`, which agrees with the model's engine on the
    anchored character-class patterns the load check admits. A pattern `re`
    cannot compile answers False, and the refusal names the pattern instead.

    `re` backtracks, so the probe never runs over a value longer than
    :data:`PROBE_VALUE_CHARS` (or the argument's `max_length`, when
    smaller), nor over a pattern with a repeat inside a repeat or a branch
    inside a repeat (:func:`_backtracks`): those answer False and the
    refusal names the pattern (review P2-5).
    """
    import re

    if not isinstance(value, str) or "*" not in value:
        return False
    cap = PROBE_VALUE_CHARS if max_length is None else min(max_length, PROBE_VALUE_CHARS)
    if len(value) > cap or _backtracks(pattern):
        return False
    try:
        compiled = re.compile(pattern)
    except re.error:
        return False
    return any(
        compiled.search(value.replace("*", stand_in)) is not None
        for stand_in in _WILDCARD_STAND_INS
    )


def _with_leading_slash(
    pattern: str, value: Any, *, max_length: int | None = None,
) -> str | None:
    """`"/" + value` when the pattern requires a leading `/` the value lacks
    and the corrected value passes the argument's pattern and length; else
    None.

    CompOps q015/q018: names sent without their leading "/" reached Rucio and
    came back "Data identifier not found", read as "not registered". A
    pattern starting `^/` refuses them before the call; this words that
    refusal as the one change that fixes it. Same probe bounds as
    :func:`_only_wildcards_rejected` (value length, no nested repeats).
    """
    import re

    if not isinstance(value, str) or value.startswith("/"):
        return None
    if not pattern.startswith("^/"):
        return None
    fixed = "/" + value
    if max_length is not None and len(fixed) > max_length:
        return None
    if len(fixed) > PROBE_VALUE_CHARS or _backtracks(pattern):
        return None
    try:
        compiled = re.compile(pattern)
    except re.error:
        return None
    return fixed if compiled.search(fixed) is not None else None


def _argument_violation_phrases(
    bound: BoundLiveRelation,
    errors: list[Mapping[str, Any]],
    declared: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """Each violation in plain words: (full phrases, short phrases, extras).

    The full phrases go in the message. The short ones are the same with a
    declared pattern cut to :data:`MAX_PATTERN_CHARS`, for the one-line
    hint. `extras` names the supplied keys the relation does not take.
    """
    by_name = {arg.name: arg for arg in bound.relation.args}
    full: list[str] = []
    short: list[str] = []
    extras: list[str] = []
    for err in errors:
        loc = err.get("loc") or ()
        name = str(loc[0]) if loc else "arguments"
        kind = str(err.get("type") or "")
        ctx = err.get("ctx") or {}
        arg = by_name.get(name)
        cut: str | None = None
        if kind == "extra_forbidden":
            extras.append(name)
            phrase = (
                f"{name} is not an argument of {bound.name}; it takes "
                f"[{', '.join(declared)}]"
            )
        elif kind == "missing":
            phrase = f"{name} is required"
        elif kind == "string_pattern_mismatch":
            pattern = str(
                ctx.get("pattern")
                or (arg.pattern if arg is not None else None) or ""
            )
            if _only_wildcards_rejected(
                pattern, err.get("input"),
                max_length=arg.max_length if arg is not None else None,
            ):
                # A pattern that admits `*` somewhere refused it where it
                # stands (a leading `/*/` read as "no
                # wildcards" when `/ab*/` is accepted).
                phrase = (
                    f"a wildcard is not accepted where it stands in {name} "
                    f"(pattern {_short_pattern(pattern)})" if "*" in pattern
                    else f"wildcards are not accepted in {name}"
                )
            elif (fixed := _with_leading_slash(
                pattern, err.get("input"),
                max_length=arg.max_length if arg is not None else None,
            )) is not None:
                # The message names the corrected value whole; the one-line
                # hint says the change, which stays short for a long name.
                phrase = f"{name} must start with '/'; did you mean {fixed!r}?"
                cut = f"{name} must start with '/'; send '/' + the value you gave"
            else:
                phrase = f"{name} does not match its pattern {pattern}"
                cut = (
                    f"{name} does not match its pattern "
                    f"{_short_pattern(pattern)}"
                )
        elif kind == "string_too_long":
            limit = ctx.get("max_length")
            if limit is None and arg is not None:
                limit = arg.max_length
            phrase = f"{name} is longer than {limit} characters"
        elif kind == "literal_error":
            phrase = f"{name} must be one of {ctx.get('expected')}"
        elif kind in ("less_than_equal", "less_than"):
            phrase = f"{name} must be at most {ctx.get('le', ctx.get('lt'))}"
        elif kind in ("greater_than_equal", "greater_than"):
            phrase = (
                f"{name} must be at least {ctx.get('ge', ctx.get('gt'))}"
            )
        elif arg is not None and (
            kind.endswith("_type") or kind.endswith("_parsing")
        ):
            phrase = f"{name} must be a {arg.type_name}"
        else:
            phrase = f"{name}: {err.get('msg') or kind}"
        full.append(phrase)
        short.append(cut or phrase)
    return full, short, extras


def _argument_fix_hint(short: list[str], extras: list[str]) -> str:
    """The one-line hint: each violation, then the one change that fixes it.

    "supply only ..." is said ONLY when an argument the relation does not
    take was supplied. Telling a caller who supplied exactly the declared
    names to "supply only" them reads as "your names are wrong" when it is a
    value that is (measured: Format v7 runs, live_arguments_invalid).
    Bounded to :data:`RETRY_LINE_CHARS` so it stays on the first line.
    """
    tail = (
        "supply only the declared arguments"
        if extras else "`inspect` publishes the argument schema"
    )
    shown: list[str] = []
    for index, phrase in enumerate(short):
        rest = len(short) - index - 1
        more = f"; and {rest} more in the message" if rest else ""
        candidate = "; ".join([*shown, phrase]) + more
        if shown and len(candidate) + len(tail) + 2 > RETRY_LINE_CHARS:
            break
        shown.append(phrase)
    text = "; ".join(shown) or "the arguments failed the declared schema"
    hidden = len(short) - len(shown)
    if hidden:
        text += f"; and {hidden} more in the message"
    with_tail = f"{text}; {tail}"
    return with_tail if len(with_tail) <= RETRY_LINE_CHARS else text


def closest_relation_names(
    name: str, declared: list[str], *, limit: int = 3,
) -> list[str]:
    """Up to `limit` declared relation names closest to `name`, closest first.

    Spelling similarity (difflib, case-folded) first, then any declared name
    sharing a `_`-separated word with the guess, so a guess that differs in
    more than a typo still points somewhere.
    """
    import difflib

    folded = {item.casefold(): item for item in declared}
    picks = [
        folded[match] for match in difflib.get_close_matches(
            name.casefold(), list(folded), n=limit, cutoff=0.5,
        )
    ]
    words = {w for w in name.casefold().replace("-", "_").split("_") if w}
    for item in declared:
        if len(picks) >= limit:
            break
        if item in picks:
            continue
        item_words = set(item.casefold().replace("-", "_").split("_"))
        if words & item_words:
            picks.append(item)
    return picks[:limit]


def validate_live_arguments(
    bound: BoundLiveRelation,
    args: Mapping[str, Any] | None,
    *,
    args_model: type,
) -> dict[str, Any]:
    """Validate against the model compiled at load; name an unknown key.

    Pydantic's ``extra="forbid"`` already names the offending key. What is
    added here is the ACCEPTED key list, because "q is not permitted" tells a
    caller what is wrong and not what to write instead.
    """
    declared = sorted(arg.name for arg in bound.relation.args)
    supplied = dict(args or {})

    # An EXPLICIT null is refused here, by name, before pydantic sees it.
    # The model is non-nullable so pydantic would refuse it anyway, but with
    # "input should be a valid string" -- which reads as a type mistake when
    # what actually happened is that the caller overrode a declared default
    # with nothing. A null time window is a full scan, so this deserves to
    # say what it means.
    explicit_nulls = sorted(k for k, v in supplied.items() if v is None)
    if explicit_nulls:
        raise LiveArgumentsInvalid(
            f"live relation {bound.name!r} was given explicit null for "
            f"{', '.join(repr(k) for k in explicit_nulls)}. There is no null "
            f"argument: omit the key to take the declared default, or send a "
            f"value. An explicit null would defeat the default the relation "
            f"declares, and an unset bound is an unbounded call.",
            data={
                "relation": bound.name,
                "null_arguments": explicit_nulls,
                "declared_arguments": declared,
            },
            fix_hint="omit the key to use its default, or supply a value",
        )
    try:
        model = args_model(**supplied)
    except Exception as exc:  # noqa: BLE001 — pydantic ValidationError et al.
        errors = list(getattr(exc, "errors", lambda: [])())
        full, short, extras = _argument_violation_phrases(
            bound, errors, declared,
        )
        raise LiveArgumentsInvalid(
            f"live relation {bound.name!r} refused its arguments: "
            + ("; ".join(full) if full else str(exc)) + ".",
            data={
                "relation": bound.name,
                "declared_arguments": declared,
                "supplied": sorted(supplied),
                # Which CONSTRAINT was violated, not just which field. An
                # agent told "size is invalid" retries blind; one told
                # "size: less_than_equal" fixes it.
                "violations": sorted({
                    f"{'.'.join(str(p) for p in err.get('loc', ()))}: "
                    f"{err.get('type')}"
                    for err in errors
                }) or None,
            },
            fix_hint=_argument_fix_hint(short, extras),
        ) from exc
    return dict(model.model_dump())


#: `live.each` (compops-read-fixes): one call asks the relation once per
#: value of ONE argument and answers with every row, each tagged with its
#: value. A question over 30 items took 30 single calls and left the
#: agent to add 30 sizes by hand; another needs over a thousand values.
#: At most `MAX_EACH_VALUES`
#: values, `EACH_WORKERS` upstream calls at a time, all inside one deadline of
#: the relation's own per `EACH_WORKERS` values, at most
#: `EACH_MAX_DEADLINE_SECONDS`. The reply shows at most `EACH_ROWS_SHOWN`
#: rows; every row is kept for `aggregate` and for `next`.
MAX_EACH_VALUES = 1_500
EACH_WORKERS = 8
EACH_MAX_DEADLINE_SECONDS = 180.0
EACH_ROWS_SHOWN = 20


#: How many of a fanned-out argument's values a REPLY names; the rest are
#: counted. A reply's `query` used to list every value (about 67 B each), and
#: the byte reserve is sized from the reply, so a fan-out over more than about
#: 600 values was refused before it ran under a 60,000 B budget although the
#: reply shows at most `EACH_ROWS_SHOWN` rows (a fan-out of about 1,400
#: values, reserve 95,860 B). The kept answer, the redo call and the total's
#: cover line still get every value: they are handed the full arguments.
EACH_QUERY_VALUES_SHOWN = 3


def _printed_query(query: Mapping[str, Any]) -> dict[str, Any]:
    """`query` as a reply prints it: a fanned-out argument's list of more
    than :data:`EACH_QUERY_VALUES_SHOWN` values becomes `{"values": <count>,
    "first": [<the first ones>]}`. Declared arguments are never lists, so
    only `each` is ever shortened."""
    printed: dict[str, Any] = {}
    for key, value in query.items():
        if isinstance(value, list) and len(value) > EACH_QUERY_VALUES_SHOWN:
            value = {
                "values": len(value),
                "first": list(value[:EACH_QUERY_VALUES_SHOWN]),
            }
        printed[key] = value
    return printed


def _within_each_rows(fragment: Mapping[str, Any]) -> bool:
    rows = fragment.get("rows")
    return not isinstance(rows, list) or len(rows) <= EACH_ROWS_SHOWN


def validate_each(
    bound: BoundLiveRelation,
    args: Mapping[str, Any] | None,
    each: Any,
    *,
    args_model: type,
) -> tuple[str, list[dict[str, Any]]]:
    """The argument `each` names, and the validated arguments of every call.

    Each value is validated as that argument, exactly as a single call's
    would be (a wildcard refused for one value is refused here too)."""

    declared = sorted(arg.name for arg in bound.relation.args)
    shape_ok = (
        isinstance(each, Mapping) and len(each) == 1
        and isinstance(next(iter(each.values())), (list, tuple))
    )
    key = str(next(iter(each))) if shape_ok else ""
    values = list(next(iter(each.values()))) if shape_ok else []
    if not shape_ok or not 1 <= len(values) <= MAX_EACH_VALUES:
        raise LiveArgumentsInvalid(
            f"live.each names ONE argument of {bound.name!r} and a list of "
            f"1-{MAX_EACH_VALUES} values to call it with, e.g. "
            '{"item": ["a", "b"]}',
            data={"relation": bound.name, "declared_arguments": declared},
            fix_hint="pass each={<argument>: [value, ...]}",
        )
    if key not in declared:
        raise LiveArgumentsInvalid(
            f"live.each names {key!r}, which {bound.name!r} does not declare",
            data={"relation": bound.name, "declared_arguments": declared},
            fix_hint="name one of the declared arguments in each",
        )
    if key in (args or {}):
        raise LiveArgumentsInvalid(
            f"live.each and live.args both give {key!r}; give it in one of them",
            data={"relation": bound.name, "argument": key},
            fix_hint=f"drop {key!r} from args",
        )
    seen: set[str] = set()
    for value in values:
        text = json.dumps(value, sort_keys=True, default=str)
        if text in seen:
            raise LiveArgumentsInvalid(
                f"live.each lists a value of {key!r} twice",
                data={"relation": bound.name, "argument": key},
                fix_hint="list each value once",
            )
        seen.add(text)
    per_value = [
        validate_live_arguments(bound, {**dict(args or {}), key: value}, args_model=args_model)
        for value in values
    ]
    return key, per_value


def each_query(key: str, per_value: list[dict[str, Any]]) -> dict[str, Any]:
    """What a fanned-out call asked: the shared arguments, and the list."""

    return {**per_value[0], key: [validated[key] for validated in per_value]}


def each_deadline(relation_deadline: float, values: int) -> float:
    rounds = -(-values // EACH_WORKERS)
    return min(EACH_MAX_DEADLINE_SECONDS, float(relation_deadline) * rounds)


def _matched_nothing_args(
    sent: Mapping[str, Any] | None, validated: Mapping[str, Any],
) -> dict[str, Any]:
    """The arguments an empty match is said against: the text arguments the
    caller sent (an expression or a pattern is what matched nothing), else
    every argument the caller sent."""
    sent_names = set(sent) if isinstance(sent, Mapping) else set()
    given = {key: value for key, value in validated.items() if key in sent_names}
    texts = {key: value for key, value in given.items() if isinstance(value, str)}
    return texts or given


#: Failures that are the external system's answer "nothing", not an outage:
#: an empty match, and "what you named does not exist" (its own message says
#: "that is its answer, not an outage"). A call answered so serves 0 rows,
#: `state: ok`, and says which in its `matched_nothing` note, as a run's live
#: step already counts them (`VALUE_EMPTY`, `VALUE_NOT_FOUND`).
_NOTHING_FAILURES = frozenset({"upstream_empty_result", "upstream_not_found"})


def _empty_match_as_rows(
    adapter: Callable[..., Any], note: dict[str, Any],
    sent: Mapping[str, Any] | None,
) -> Callable[..., Any]:
    """The adapter, with the external system's "this matched nothing" or
    "not found" (`_NOTHING_FAILURES`) read as its answer: 0 rows, said in
    `note` with the system's own words.

    An empty match is an answer, not a failure (lane re, issue 4). Served
    as `state: error` it read as a broken call, and the real failures beside
    it were lost in the same shape."""

    def call(**kwargs: Any) -> Any:
        try:
            return adapter(**kwargs)
        except LiveAdapterError as exc:
            if exc.failure not in _NOTHING_FAILURES:
                raise
            note.update(
                # `failures` on both forms (a fan-out may mix the two kinds).
                failure=exc.failure, failures=[exc.failure],
                args=_matched_nothing_args(sent, kwargs.get("args") or {}),
                **({"excerpt": exc.excerpt} if exc.excerpt else {}),
            )
            return []

    return call


class _NothingFor(list):
    """One fanned-out value the external system answered "nothing" for:
    no rows, and which failure said so."""

    def __init__(self, failure: str, excerpt: str | None) -> None:
        super().__init__()
        self.failure = failure
        self.excerpt = excerpt


def each_adapter(
    adapter: Callable[..., Any],
    key: str,
    per_value: list[dict[str, Any]],
    relation_deadline: float,
    slots: Optional[threading.Semaphore] = None,
    note: Optional[dict[str, Any]] = None,
) -> Callable[..., Any]:
    """An adapter that runs `adapter` once per value and joins the rows.

    Rows keep upstream order within a value, values keep the caller's
    order. A row (an object) that does not carry the argument gets it as
    its first field, so a total can be grouped or checked by value. Any
    one call's failure fails the whole answer, naming the value.

    With `note`, a value the system answers "nothing" for
    (`_NOTHING_FAILURES`) is 0 rows for that value, never a failure of the
    whole answer: the values that answered keep their rows, and `note`
    names the values that matched nothing, in the caller's order.

    `slots` is the binding's shared upstream cap (:func:`upstream_slots`):
    each value's call holds one while it runs, so fan-outs side by side
    never put more calls in flight to one system than the cap."""

    def run(*, config: Any, args: Any, node_id: str, deadline_seconds: float) -> list[Any]:
        import time as _time

        stop = _time.monotonic() + float(deadline_seconds)

        def one(validated: dict[str, Any]) -> Any:
            left = stop - _time.monotonic()
            if left <= 0 or (slots is not None and not slots.acquire(timeout=left)):
                raise LiveAdapterError(
                    "timeout", excerpt=f"each {key}={validated[key]!r}: not started in time",
                )
            try:
                left = stop - _time.monotonic()
                return adapter(
                    config=config, args=dict(validated), node_id=node_id,
                    deadline_seconds=max(0.001, min(left, float(relation_deadline))),
                )
            except LiveAdapterError as exc:
                if note is not None and exc.failure in _NOTHING_FAILURES:
                    return _NothingFor(exc.failure, exc.excerpt)
                raise LiveAdapterError(
                    exc.failure,
                    excerpt=f"each {key}={validated[key]!r}: {exc.excerpt or exc.failure}",
                    status=exc.status,
                ) from exc
            finally:
                if slots is not None:
                    slots.release()

        with ThreadPoolExecutor(
            max_workers=min(EACH_WORKERS, len(per_value)),
            thread_name_prefix="okg-live-each",
        ) as pool:
            answers = list(pool.map(one, per_value))
        nothing = [
            (validated[key], answer) for validated, answer in zip(per_value, answers)
            if isinstance(answer, _NothingFor)
        ]
        if nothing and note is not None:
            # One value's own words are kept (bounded), named by that value.
            worded = next(((value, answer) for value, answer in nothing if answer.excerpt), None)
            note.update(
                failures=sorted({answer.failure for _, answer in nothing}),
                # Printed like `query`: a long list is its count and first few.
                args=_printed_query({key: [value for value, _ in nothing]}),
                each_values=len(per_value),
                **({"excerpt": worded[1].excerpt, "excerpt_for": {key: worded[0]}} if worded else {}),
            )
        rows: list[Any] = []
        for validated, answer in zip(per_value, answers):
            for row in (answer if isinstance(answer, list) else [answer]):
                if isinstance(row, Mapping) and key not in row:
                    row = {key: validated[key], **row}
                rows.append(row)
        cut = sum(1 for answer in answers if isinstance(answer, UpstreamCutRows))
        if cut:
            # One cut call makes the joined answer short too; say so.
            return UpstreamCutRows(
                rows, received=len(rows), parts_cut=cut, parts=len(answers),
            )
        return rows

    return run


def resolve_live_relation(
    declared: Mapping[str, BoundLiveRelation],
    relation_name: str,
    *,
    subtype: str,
) -> BoundLiveRelation:
    """Resolve the named relation, or refuse listing what IS declared."""
    bound = declared.get(relation_name)
    if bound is None:
        names = sorted(declared)
        closest = closest_relation_names(relation_name, names)
        if closest:
            lead = f"closest declared relations: {', '.join(closest)}"
        else:
            lead = f"no declared relation is close to {relation_name!r}"
        where = f"all {len(names)} are listed in data.declared"
        inspect_call = (
            f"`inspect(target={{'kind':'subtype','name':'{subtype}'}})`"
        )
        fix_hint = f"{lead}; {where}, or call {inspect_call}"
        if len(fix_hint) > RETRY_LINE_CHARS:
            fix_hint = f"{lead}; {where}"
        raise LiveRelationUndeclared(
            f"subtype {subtype!r} declares no live relation "
            f"{relation_name!r}. "
            + (f"Closest declared: {', '.join(closest)}. " if closest else "")
            + f"It declares {len(names)} live relation(s): {names}.",
            data={
                "subtype": subtype,
                "relation": relation_name,
                "declared": names,
            },
            fix_hint=fix_hint,
        )
    return bound


def _safe_excerpt(text: Any, relation: str, *, kind: str = "external_live_error") -> str:
    """Bound, redact and fence an adapter-supplied excerpt.

    Redaction first: an excerpt is the one place upstream text is echoed, so
    any URL in it loses its userinfo before anything else happens. `kind`
    names the fence's source: an error's words, or the system's words beside
    a "nothing" answer (`external_live_said`), which is not an error.
    """
    from okg.substrate.external_backed.resolver import _fence_payload

    raw = str(text)[:MAX_ERROR_EXCERPT_CHARS]
    raw = _redact_userinfo(raw)
    return _fence_payload(raw, source=f"{kind}:{relation}")


# Redaction now lives in `_http_guard` (#2100 section 7), shared with the
# #2014 client, and is imported UNGUARDED -- no `except ImportError` around
# it. That is the point: an import guarded by a fallback would mean no
# redaction at all whenever the module was absent, which is exactly how the
# missing redaction was found in the first place. A missing module must be an
# ImportError at startup, not a URL with its password in a log line.
from okg.substrate.external_backed._http_guard import (  # noqa: E402
    redact_userinfo as _redact_userinfo,
)


#: Most bytes, as compact escaped JSON, of one field kept in a capped error.
#: Two of them plus the fixed fields stay well under `ERROR_DATA_CAP_BYTES`.
_KEPT_ERROR_FIELD_BYTES = 300


def _clip_text(value: Any, max_bytes: int) -> str:
    """`value` as text whose JSON string encoding is at most `max_bytes`."""
    text = str(value)
    while len(json.dumps(text).encode("utf-8")) > max_bytes:
        # Shrink by the overshoot, at least one character, so a long string
        # of multi-byte characters converges in a few passes.
        over = len(json.dumps(text).encode("utf-8")) - max_bytes
        text = text[: max(0, len(text) - max(1, over // 2))]
    return text


def _capped_error(body: Mapping[str, Any]) -> dict[str, Any]:
    """Bound an adapter's error at the shared 1 KiB error cap.

    Reused from #2029's `response_bounds.ERROR_DATA_CAP_BYTES` rather than
    given a cap of its own: an adapter that returns a four-megabyte stack
    trace must not be able to spend the response budget on it, and there is
    no reason for live errors to have a different ceiling from every other
    error in the envelope.
    """
    from okg.substrate.mcp.response_bounds import ERROR_DATA_CAP_BYTES

    blob = json.dumps(dict(body), separators=(",", ":"), default=str)
    if len(blob.encode("utf-8")) <= ERROR_DATA_CAP_BYTES:
        return dict(body)
    # The two fields kept are clipped too. A bundle's reason code is not
    # bounded anywhere upstream, and keeping it whole would let one long
    # reason push the error past the cap -- and past the reserve the graph
    # step leaves for it (#2141 review).
    kept: dict[str, Any] = {
        k: _clip_text(body[k], _KEPT_ERROR_FIELD_BYTES)
        for k in ("reason", "relation") if k in body
    }
    if isinstance(body.get("narrow_with"), list):
        # Already bounded at `_NARROW_WITH_MAX_BYTES` by `_over_cap_remedy`,
        # and kept as a list: the remedy is the useful part of an over-cap
        # error, so truncating the error must not drop it.
        kept["narrow_with"] = list(body["narrow_with"])
    return {
        **kept,
        "truncated": True,
        "truncation_reason": "error_over_cap",
        "digest": "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest(),
        "original_bytes": len(blob.encode("utf-8")),
    }


#: Most bytes, as compact JSON, `narrow_with` may spend. With the two other
#: fields `_capped_error` keeps at 300 bytes each, the truncated form stays
#: under `ERROR_DATA_CAP_BYTES`, so the remedy survives truncation too.
_NARROW_WITH_MAX_BYTES = 160


def _over_cap_remedy(relation: Any) -> dict[str, Any]:
    """What a caller can do about a reply over the adapter's size limit.

    The size limit is the operator's (`max_response_bytes`); the caller's
    lever is the relation's declared arguments, which narrow the question.
    Named, bounded, and only the NAMES -- never the values the caller sent.
    """
    names: list[str] = []
    for name in sorted(arg.name for arg in getattr(relation, "args", ())):
        candidate = names + [name]
        if len(json.dumps(candidate, separators=(",", ":"))) > _NARROW_WITH_MAX_BYTES:
            break
        names = candidate
    return {
        "narrow_with": names,
        "remedy": (
            "ask a narrower question with these arguments"
            if names else "this relation takes no arguments to narrow it"
        ),
    }


class _AdapterRaised(Exception):
    """Carries whatever the adapter raised, out of the worker thread.

    Without it the two timeouts are INDISTINGUISHABLE (#2149).
    `Future.result(timeout=...)` raises `concurrent.futures.TimeoutError` when
    the wait expires and re-raises the worker's own exception when it failed
    -- and since Python 3.11 those are the same class for an adapter whose
    socket timed out, so the dispatcher's `except` could not tell "we stopped
    waiting" from "the adapter reported a timeout". Wrapping every exception
    the worker raises leaves exactly one thing that can arrive as a futures
    timeout: our own expired wait.
    """

    def __init__(self, original: BaseException) -> None:
        super().__init__(type(original).__name__)
        self.original = original


class LiveDeadlineExceeded(Exception):
    """The DISPATCHER's own wait expired. Raised by nothing else.

    Deliberately NOT a `TimeoutError` (#2149). Since Python 3.11
    `concurrent.futures.TimeoutError` IS the builtin `TimeoutError`, so while
    the deadline was reported with that class there was no way to tell "we
    stopped waiting" from "the adapter raised its own socket timeout": an
    adapter's `TimeoutError` landed in the abandonment branch and logged a
    worker-thread leak that had not happened -- the exact line an operator
    alerts on. A private class cannot collide with anything an adapter raises.

    Not a `SubstrateError` either, because it never reaches a caller: the
    dispatcher converts it into `state: timeout` in the envelope. Giving it a
    wire code would register an error nothing can ever emit.
    """


def acquire_live_slot() -> bool:
    """Take one concurrency slot, or report that none is free.

    Called by :func:`_run_adapter_under_deadline` and by nothing else on the
    dispatch path. It is still module-level, and still exported, because a
    test that needs to hold the cap down has to be able to take a slot -- but
    a slot taken here is released here (see that function's ONE OWNER note).
    """
    return _take_live_slot() is not None


def _take_live_slot() -> Any:
    """Take a slot and return the SEMAPHORE it came from, or ``None``.

    The object, not a boolean, so the release can be given back to the same
    semaphore the acquire took it from. `_LIVE_SLOTS` is a module global that
    a test may swap to bound the cap; reading it again at release time -- as
    the release used to -- gives the slot back to whatever is bound to the
    name by then, which for a call abandoned at its deadline can be seconds
    later and a different object. That is the other half of #2142: releasing
    a slot you did not take, from the far side of a rebinding.
    """
    slots = _LIVE_SLOTS
    return slots if slots.acquire(blocking=False) else None


def release_live_slot() -> None:
    """Give back a slot taken by :func:`acquire_live_slot`. Tests only.

    The dispatch path does not call this: `_run_adapter_under_deadline`
    releases the exact semaphore it took from (#2142).
    """
    _LIVE_SLOTS.release()


def _run_adapter_under_deadline(
    adapter: Callable[..., Any],
    *,
    config: Mapping[str, Any],
    args: Mapping[str, Any],
    node_id: str,
    deadline_seconds: float,
    on_started: Optional[Callable[[], None]] = None,
) -> Any:
    """Run the adapter and ENFORCE the declared deadline.

    ONE OWNER FOR THE SLOT (#2142). This function takes the concurrency slot
    and this function gives it back -- on return, on failure, or, when the
    call is abandoned at its deadline, through the callback that fires when
    the worker finally finishes. The slot used to be taken by
    `dispatch_live_relation` and released here, so a direct caller of this
    function released a slot it had never taken; the `BoundedSemaphore` said
    so, loudly, on stderr, once the abandoned thread came back.

    No slot free is `LiveConcurrencyExhausted`, raised BEFORE anything is
    submitted, so a call that never ran cannot read as a call that crashed.
    The dispatcher reports it as its own outcome with `adapter_ran=False`.

    `on_started` fires once the work is submitted -- which is when the adapter
    is committed to run, because the semaphore and the executor are sized from
    the same budget term, so a submitted task never waits for a worker.

    The deadline used to be passed to the adapter as a keyword and trusted.
    That bounds only a COOPERATIVE adapter: one that hangs in a socket read,
    or ignores the argument, ran forever and held an agent's turn open on a
    system this graph does not control. A test whose fake raised TimeoutError
    proved the envelope shape and nothing about the bound.

    The call runs on a worker thread and is abandoned on expiry.
    ABANDONED, not killed -- Python cannot cancel a thread blocked in a
    syscall. Stated plainly because it has a cost: the thread lives until its
    own I/O returns, holding whatever socket it opened. The executor is NOT
    shut down with `wait=True`, so this function returns at the deadline;
    `cancel_futures` stops anything not yet started. The adapter is still
    handed `deadline_seconds` so a well-behaved one can bound its own socket
    and exit promptly, which is the only way the thread ends early. That is
    the trade this design accepts: an unbounded wait is a certain failure,
    a leaked thread is a bounded one.
    """
    from concurrent.futures import TimeoutError as FuturesTimeout

    slots = _take_live_slot()
    if slots is None:
        raise LiveConcurrencyExhausted(
            f"{MAX_CONCURRENT_LIVE_CALLS} live calls are already in flight in "
            f"this process and none has returned; further calls are refused "
            f"rather than queued invisibly",
            data={"limit": MAX_CONCURRENT_LIVE_CALLS},
            fix_hint="retry once the external system recovers",
        )

    # ATOMIC. A check-then-set `if not released` races two callers -- the
    # completion path and the abandonment callback can both see False -- and
    # a double release on a BoundedSemaphore raises ValueError from whichever
    # thread got there second.
    release_lock = threading.Lock()
    released: list[bool] = [False]

    def _release(_f: Any = None) -> None:
        with release_lock:
            if released[0]:
                return
            released[0] = True
        slots.release()

    def _invoke(**kwargs: Any) -> Any:
        try:
            return adapter(**kwargs)
        except BaseException as exc:  # noqa: BLE001 — re-raised below, intact.
            raise _AdapterRaised(exc) from None

    #: Told ONCE, and only where the task was not cancelled before it ran.
    #: Calling it straight after `submit` reported `adapter_ran=True` for a
    #: task the executor never started (review finding L1): the sample then
    #: claimed an external call that provably did not happen -- the adapter
    #: recorded zero invocations for the same call.
    notified: list[bool] = [False]

    def _started() -> None:
        if on_started is None or notified[0]:
            return
        notified[0] = True
        on_started()

    try:
        future = _LIVE_EXECUTOR.submit(
            _invoke,
            config=dict(config),
            args=dict(args),
            node_id=node_id,
            deadline_seconds=deadline_seconds,
        )
    except BaseException:
        _release()
        raise

    #: Set when the slot's life has been handed to the abandonment callback.
    #: Everything else leaves through the `finally` below, so no exit -- not
    #: even a callback that raises (review finding L3) -- can leak the slot.
    handed_off: list[bool] = [False]
    #: What the adapter raised, re-raised BELOW the handler so the internal
    #: `_AdapterRaised` wrapper never lands on its `__context__`. The
    #: dispatcher, its log and any traceback then see the adapter's own
    #: exception exactly as the adapter raised it -- the same rule #2143
    #: applies to the sample-write failure.
    adapter_failure: BaseException | None = None
    try:
        try:
            result = future.result(timeout=deadline_seconds)
        except FuturesTimeout:
            # WHOSE timeout this is, answered by the future rather than by
            # the exception's class (#2149). Only the wait raises
            # `FuturesTimeout` here -- an adapter's own exception comes out
            # of `result()` wrapped -- and the future then says whether
            # anything is actually abandoned. A worker that finished in the
            # moment the wait expired, or one cancelled before it started,
            # holds no thread and no socket, so it must not be logged as a
            # leak.
            #
            # `done()` is asked AGAIN after a failed `cancel()` (review
            # finding L2): the worker can finish in the window between the
            # two calls, and a finished worker reported as abandoned is the
            # false thread-leak alert this arm exists to prevent.
            finished = future.done()
            cancelled = future.cancel()
            still_running = not (finished or cancelled or future.done())
            if still_running:
                # The slot is released by the CALLBACK, when the abandoned
                # call finally returns -- not here. Releasing it now would
                # let the cap count calls that have finished waiting rather
                # than calls still holding a thread, which is the resource
                # it exists to bound.
                future.add_done_callback(_release)
                handed_off[0] = True
                log.warning(
                    "okg.live_relation.adapter_abandoned node=%s "
                    "deadline=%ss; the worker thread is left to finish its "
                    "own I/O and its result, if it ever arrives, is "
                    "DISCARDED -- never sampled and never returned. %s "
                    "slot(s) of %s remain.",
                    node_id, deadline_seconds,
                    slots._value,  # noqa: SLF001 — diagnostic only
                    MAX_CONCURRENT_LIVE_CALLS,
                    extra={
                        "event": "okg.live_relation.adapter_abandoned",
                        "node_id": node_id,
                        "deadline_seconds": deadline_seconds,
                    },
                )
            if not cancelled:
                _started()
            # `from None`: the futures timeout carries nothing a reader needs
            # and chaining it would put this module's internals in every
            # traceback.
            raise LiveDeadlineExceeded(
                f"the call did not return within {deadline_seconds}s and was "
                f"cut at its declared deadline"
            ) from None
        except _AdapterRaised as wrapper:
            # The adapter's OWN failure, unwrapped and handed on as itself so
            # the dispatcher classifies it on its type -- including a
            # `TimeoutError`, which is an adapter error and not this
            # function's deadline.
            _started()
            adapter_failure = wrapper.original
        except BaseException:
            _started()
            raise
        if adapter_failure is not None:
            raise adapter_failure
        _started()
        return result
    finally:
        if not handed_off[0]:
            _release()



#: Structural caps on an adapter result, applied BEFORE fencing or
#: serialising. A payload can sit under the byte cap and still be a depth
#: bomb: 20,000 nested lists is a few kilobytes and blows the stack in
#: `_fence_payload`'s recursion long before `json.dumps` sees it.
MAX_RESULT_DEPTH = 24
MAX_RESULT_NODES = 50_000


def _assert_result_is_shapeable(payload: Any) -> None:
    """Refuse a result that is too deep or too wide to shape safely.

    Iterative, not recursive: a checker that recursed would hit the same
    stack limit it exists to prevent.
    """
    stack = [(payload, 1)]
    nodes = 0
    while stack:
        node, depth = stack.pop()
        nodes += 1
        if depth > MAX_RESULT_DEPTH:
            raise LiveAdapterError(
                "result_unusable",
                excerpt=f"nesting deeper than {MAX_RESULT_DEPTH} levels",
            )
        if nodes > MAX_RESULT_NODES:
            raise LiveAdapterError(
                "result_unusable",
                excerpt=f"more than {MAX_RESULT_NODES} values",
            )
        if isinstance(node, Mapping):
            stack.extend((v, depth + 1) for v in node.values())
        elif isinstance(node, (list, tuple)):
            stack.extend((v, depth + 1) for v in node)


@dataclass(frozen=True)
class ShapedRows:
    """What shaping produced, for the envelope AND for the sample.

    Both come out of one call because they must be the SAME bytes. The
    envelope fragment used to be the only return value and `write_sample`
    was handed the adapter's raw result, so a hostile key neutralised for
    the model was persisted as it arrived for the retention class's whole
    lifetime -- up to 730 days on the cold class. The sample is the audit of
    what the model received, so it stores what the model received.
    """

    envelope: dict[str, Any]
    #: The FENCED, key-neutralised, depth-capped rows the CALLER received
    #: (all of them, or the prefix), or the full set when the caller got only
    #: its digest. The store's own inline/digest decision runs against it.
    sample_payload: Any
    #: Where a digested payload can be fetched again. `build_sample` REFUSES
    #: a digest sample without one -- "storing a hash nobody can resolve is
    #: not a sample" -- and the dispatcher used to build it for the envelope
    #: and then drop it, so every digest relation, and every inline payload
    #: over the 16 KiB cap, failed its sample write and took the whole call
    #: down with it. A search result is exactly that case.
    refetch: Any
    #: What the caller got: `rows`, `rows_prefix` or `digest`. Recorded on
    #: the sample so it says what the model saw even when the store kept
    #: only a digest (issue #2171).
    caller_received: str = "rows"
    #: `sha256:` of the rows the caller received, by the call sample's own
    #: encoding; for `digest`, the digest the caller was given.
    caller_rows_sha256: str = ""
    #: `sha256:` of the FULL fenced set, whichever form the caller got: the
    #: digest a retained set reports (live result sets).
    full_digest: str = ""
    #: How the text form cut each row to fit (#3666), or None: the
    #: envelope's `result_text.LIVE_TEXT_SHORTENED_KEY` block.
    text_shortened: Optional[dict[str, Any]] = None


#: The one name for "the response budget cut this", on graph rows and live
#: rows alike (issue #2141). It replaces the two live-only names
#: `rows_over_inline_cap` and `response_max_bytes`.
LIVE_TRUNCATION_REASON = "response_byte_budget"

#: Most bytes `summary.first_row_keys` may spend. Keys are whatever the
#: external system returned, fenced, so their total is otherwise unbounded,
#: and the reserve the graph step leaves has to be known before the adapter
#: runs.
FIRST_ROW_KEYS_MAX_BYTES = 512

def _refetch_pointer(
    *, relation: str, source: str, query: Mapping[str, Any],
    args_fingerprint: str,
) -> Any:
    """THE re-fetch pointer for a live call. There is no second builder.

    Built under the RELATION'S OWN allowlist -- `refetch_query_keys`, via
    `refetch_pointer_for_call` -- and not from the validated arguments
    directly. `register_sample_policies` leaves that allowlist EMPTY for
    every live relation, so the pointer carries the argument SHAPE and the
    call's fingerprint and no VALUE at all. A value copied here would sit in
    the sample's `refetch` column for the retention class's whole life, up to
    730 days on `cold`. An unregistered relation gets the registry's computed
    default, whose allowlist is also empty.

    `args_fingerprint` is REQUIRED (issue #2204): a pointer that names the
    argument shape but not the call is a pointer to every call with that
    shape. The server's second pointer builder left it out; that builder is
    gone.
    """
    from okg.substrate.external_backed.call_sample import (
        RefetchPointer,
        default_registry,
        refetch_pointer_for_call,
    )

    if not isinstance(args_fingerprint, str) or not args_fingerprint.strip():
        raise ValueError(
            "a live re-fetch pointer needs the call's args_fingerprint; "
            "without it the pointer names every call with the same shape",
        )
    policy = default_registry().policy_for(relation)
    allowed = refetch_pointer_for_call(
        policy,
        kwargs=query,
        arg_shape="args(" + ",".join(sorted(query)) + ")",
        source=source,
    )
    return RefetchPointer(
        query={**allowed.query, "args_fingerprint": args_fingerprint},
        source=source,
    )


def _first_row_keys(rows: list[Any]) -> tuple[list[str], bool]:
    """The first row's keys, sorted, cut to `FIRST_ROW_KEYS_MAX_BYTES`."""
    if not rows or not isinstance(rows[0], Mapping):
        return [], False
    kept: list[str] = []
    for key in sorted(str(k) for k in rows[0]):
        candidate = kept + [key]
        if len(json.dumps(candidate, separators=(",", ":")).encode("utf-8")) > (
            FIRST_ROW_KEYS_MAX_BYTES
        ):
            return kept, True
        kept = candidate
    return kept, False


def _digest_fragment(
    *,
    digest: str,
    row_count: int,
    total_bytes: int,
    first_row_keys: list[str],
    keys_truncated: bool,
    relation: str,
    source: str,
    pointer: Any,
    rows_returned: int,
    budget_bytes: Optional[int],
) -> dict[str, Any]:
    """What a truncated envelope carries beside whatever rows still fit."""
    return {
        "truncated": True,
        "truncation_reason": LIVE_TRUNCATION_REASON,
        "rows_returned": rows_returned,
        **({"budget_bytes": budget_bytes} if budget_bytes is not None else {}),
        # Of the FULL fenced set, by the call sample's own encoding, so a
        # digest sample of the same set carries the same hash.
        "digest": digest,
        "summary": {
            "row_count": row_count,
            "bytes": total_bytes,
            "first_row_keys": first_row_keys,
            **({"first_row_keys_truncated": True} if keys_truncated else {}),
        },
        "refetch": {
            "relation": relation,
            "source": source,
            # The POINTER's query, not the caller's arguments. This block
            # tells a reader how the payload is fetched again, and it would be
            # a lie -- and a second copy of the values -- if it named
            # arguments the stored pointer does not hold.
            "query": dict(pointer.query),
            "pointer": getattr(pointer, "source", source),
        },
    }


def _largest_fitting(
    low: int, high: int, ok: Callable[[int], bool],
) -> Optional[int]:
    """The largest ``n`` in ``low..high`` with ``ok(n)``, for a monotone
    ``ok`` (true up to some ``n``, false after it); None if none is."""
    best: Optional[int] = None
    while low <= high:
        middle = (low + high) // 2
        if ok(middle):
            best, low = middle, middle + 1
        else:
            high = middle - 1
    return best


def _bounded_rows(
    payload: Any,
    *,
    relation: str,
    source: str,
    query: Mapping[str, Any],
    args_fingerprint: str,
    fits: Optional[Callable[[dict[str, Any]], bool]],
    budget_bytes: Optional[int] = None,
) -> "ShapedRows":
    """Fence the rows, then return the most of them the response can hold.

    `fits(fragment)` answers whether the WHOLE response -- graph rows, this
    envelope with `fragment` merged in, session metadata -- fits the one
    response budget, measured as the SDK serves it. Three forms, largest
    first (decision 2A on #2141):

    1. every row, inline; when that does not fit, every row with the text
       form told to cut each value evenly, then to keep fewer non-key
       fields per row (#3666, D4: shorten before dropping rows);
    2. the longest PREFIX of rows that fits, in the order the external
       system returned them, plus a digest of the full set;
    3. no rows: the digest, a bounded summary and the re-fetch pointer.

    Forms 2 and 3 say so: `truncated: true`, `truncation_reason:
    response_byte_budget`, `rows_returned`, `budget_bytes`. The 16 KiB
    `INLINE_MAX_BYTES` no longer decides anything here (decision 1A): it is
    the call sample's storage rule and nothing else. `fits=None` means no
    response budget applies, which only a direct caller of the dispatcher
    can ask for; the MCP path always passes one.

    If even form 3 does not fit, it is returned anyway and the dispatcher's
    final check replaces it with the bounded error form.
    """
    from okg.substrate.external_backed.call_sample import digest_of

    # Fence the strings BEFORE measuring and returning them, through the same
    # primitive the #83 hydration overlay uses (`resolver._fence_payload` ->
    # `provenance._fence_value`). `trust: "untrusted"` is a label a model can
    # ignore; the fence markers are what the read path already relies on to
    # stop external text being read as instructions.
    from okg.substrate.external_backed.resolver import (
        _fence_payload,
        neutralised_keys,
    )

    # KEYS are fenced too, unlike the #83 overlay: a live relation's keys are
    # whatever the external system returned, and an attacker who controls a
    # document controls its `_source` key names.
    _assert_result_is_shapeable(payload)
    altered = neutralised_keys(payload)
    payload = _fence_payload(
        payload, source=f"external_live:{relation}", fence_keys=True,
    )
    pointer = _refetch_pointer(
        relation=relation, source=source, query=query,
        args_fingerprint=args_fingerprint,
    )
    sha, total_bytes = digest_of(payload)
    digest = "sha256:" + sha
    full = {
        "rows": payload, "policy": "inline", "inline_bytes": total_bytes,
        **({"neutralised_keys": list(altered)} if altered else {}),
    }
    if fits is None or fits(full):
        return ShapedRows(
            envelope=full, sample_payload=payload, refetch=pointer,
            caller_received="rows", caller_rows_sha256=digest,
            full_digest=digest,
        )

    # Every row before fewer rows (#3666, decision D4): the text form cuts
    # each row's values evenly, then keeps fewer non-key fields per row. The
    # block only changes how the text form prints the same rows; the JSON
    # form ignores it, so there it never fits and the forms below run.
    from okg.substrate.mcp.result_text import (
        LIVE_TEXT_SHORTENED_KEY,
        LIVE_VALUE_CHARS,
        LIVE_VALUE_FLOOR,
    )

    def shortened(envelope: dict[str, Any], spec: dict[str, int]) -> dict[str, Any]:
        return {**envelope, LIVE_TEXT_SHORTENED_KEY: spec}

    if isinstance(payload, (list, Mapping)) and payload and fits(
        shortened(full, {"value_chars": LIVE_VALUE_FLOOR, "fields": 0}),
    ):
        spec: dict[str, int]
        chars = _largest_fitting(
            LIVE_VALUE_FLOOR, LIVE_VALUE_CHARS - 1,
            lambda n: fits(shortened(full, {"value_chars": n})),
        )
        if chars is not None:
            spec = {"value_chars": chars}
        else:
            widest = (
                max((len(r) for r in payload if isinstance(r, Mapping)), default=0)
                if isinstance(payload, list) else len(payload)
            )
            fields = _largest_fitting(
                1, widest,
                lambda m: fits(shortened(
                    full, {"value_chars": LIVE_VALUE_FLOOR, "fields": m},
                )),
            )
            spec = {"value_chars": LIVE_VALUE_FLOOR, "fields": fields or 0}
        return ShapedRows(
            envelope=shortened(full, spec), sample_payload=payload,
            refetch=pointer, caller_received="rows",
            caller_rows_sha256=digest, full_digest=digest,
            text_shortened=spec,
        )

    rows = payload if isinstance(payload, list) else []
    keys, keys_truncated = _first_row_keys(rows)

    def digest_block(rows_returned: int) -> dict[str, Any]:
        return _digest_fragment(
            digest=digest, row_count=len(rows), total_bytes=total_bytes,
            first_row_keys=keys, keys_truncated=keys_truncated,
            relation=relation, source=source, pointer=pointer,
            rows_returned=rows_returned, budget_bytes=budget_bytes,
        )

    def prefix(count: int) -> dict[str, Any]:
        return {
            "rows": rows[:count], "policy": "inline",
            "inline_bytes": digest_of(rows[:count])[1],
            **({"neutralised_keys": list(altered)} if altered else {}),
            **digest_block(count),
        }

    # The longest prefix that fits. Size grows with the row count, so a
    # binary search over 1..n-1 finds it; n rows already did not fit. The
    # text form prints a prefix's identifying fields only, which holds the
    # most rows there; the plain prefix is tried past that count, and wins
    # in the JSON form, where the text block only adds bytes.
    keys_only = {"value_chars": LIVE_VALUE_FLOOR, "fields": 0}
    best_short = _largest_fitting(
        1, len(rows) - 1, lambda n: fits(shortened(prefix(n), keys_only)),
    ) or 0
    best_plain = _largest_fitting(
        max(best_short, 1), len(rows) - 1, lambda n: fits(prefix(n)),
    ) or 0
    best = max(best_short, best_plain)
    if best:
        kept = rows[:best]
        # Equal counts: the plain prefix, which carries no text block.
        spec_used = keys_only if best_plain < best_short else None
        return ShapedRows(
            envelope=(
                shortened(prefix(best), keys_only) if spec_used else prefix(best)
            ),
            sample_payload=kept, refetch=pointer,
            caller_received="rows_prefix",
            caller_rows_sha256="sha256:" + digest_of(kept)[0],
            full_digest=digest, text_shortened=spec_used,
        )

    return ShapedRows(
        envelope={
            "policy": "digest", "declared_policy": "inline",
            **digest_block(0),
        },
        # The caller got no rows. The sample is handed the full set so it
        # can store ITS digest -- the same hash the caller was given -- and
        # is told not to store the rows inline, because it holds what the
        # caller received and the caller received none.
        sample_payload=payload, refetch=pointer,
        caller_received="digest", caller_rows_sha256=digest,
        full_digest=digest,
    )


def _is_cut(shaped: "ShapedRows") -> bool:
    """Whether the caller got less than the whole answer: rows left out, or
    fields or values shortened to fit (#3666, Live build 3)."""
    return shaped.caller_received != "rows" or shaped.text_shortened is not None


def _nested_cut(shaped: "ShapedRows") -> bool:
    """Whether the TEXT form, printing these rows at full width, counts away
    entries of a nested (object or list) value. Values already cut to fit
    are a cut of their own; the JSON form prints nested values whole."""
    if shaped.text_shortened is not None:
        return False
    from okg.substrate.mcp.result_text import current_result_context, live_nested_cut

    if current_result_context().format != "text":
        return False
    return live_nested_cut(shaped.envelope.get("rows"))


def _rows_served(envelope: Mapping[str, Any]) -> int:
    """How many rows an envelope fragment serves."""
    count = envelope.get("rows_returned")
    if isinstance(count, int) and not isinstance(count, bool):
        return count
    rows = envelope.get("rows")
    if isinstance(rows, list):
        return len(rows)
    return 1 if isinstance(rows, Mapping) and rows else 0


class _ShapingFailed(Exception):
    """Shaping the adapter's result raised; carries only the type name."""

    def __init__(self, type_name: str) -> None:
        super().__init__(type_name)
        self.type_name = type_name


def _shape_distinctly(payload: Any, **kwargs: Any) -> "ShapedRows":
    """`_bounded_rows`, with its own failures told apart from the adapter's.

    `LiveAdapterError` passes through: `result_unusable` (too deep, too
    wide) is a verdict on what the adapter returned. Anything else raised
    while fencing, fitting or building the pointer is ours.
    """
    try:
        return _bounded_rows(payload, **kwargs)
    except LiveAdapterError:
        raise
    except Exception as exc:  # noqa: BLE001 — re-raised, typed, below.
        failure = _ShapingFailed(type(exc).__name__)
    # Raised outside the handler so nothing of the original is chained on.
    raise failure


def _begin_retain_distinctly(
    retain: Callable[[Any, Mapping[str, Any]], Any],
    payload: Any,
    ok_base: Mapping[str, Any],
) -> Any:
    """`retain`, with its own failures told apart from the adapter's.

    Drafting a retained set is our side, like shaping: anything it raises
    is reported as `shaping_failed`, never blamed on the external system.
    """
    try:
        return retain(payload, ok_base)
    except Exception as exc:  # noqa: BLE001 — re-raised, typed, below.
        failure = _ShapingFailed(type(exc).__name__)
    raise failure


def _base_envelope(
    *,
    bound: BoundLiveRelation,
    validated: Mapping[str, Any],
    fingerprint: str,
    observed_at: datetime,
    generation_id: Any,
) -> dict[str, Any]:
    """The part of every `external_live` envelope that never shrinks."""
    return {
        "schema": "okg.external_live.v1",
        "relation": bound.relation.name,
        "trust": "untrusted",
        "source": bound.binding.kind,
        # Fixed width: plain isoformat() drops the fraction when the
        # microsecond is zero, which made the reserve 7 bytes short.
        "observed_at": observed_at.isoformat(timespec="microseconds"),
        # As printed: a fan-out's values are counted past the first few.
        "query": _printed_query(validated),
        "args_fingerprint": fingerprint,
        "pinned_generation_id": generation_id,
    }


def _bound_failed_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """The bounded error form: what the caller gets if nothing else fits.

    Only the base fields the reserve was sized with, plus a fixed error. It
    replaces the old fallback that returned the UNBOUNDED envelope when
    bounding failed.
    """
    kept = {
        key: envelope[key]
        for key in ("schema", "relation", "trust", "source", "observed_at",
                    "query", "args_fingerprint", "pinned_generation_id")
        if key in envelope
    }
    return {
        **kept,
        "state": "error",
        "error": {
            "reason": "response_bound_failed",
            "relation": envelope.get("relation"),
            "failure": "response_bound_failed",
        },
    }


def live_reserve_envelope(
    *,
    bound: BoundLiveRelation,
    args_model: type,
    args: Mapping[str, Any] | None,
    generation_id: Any = None,
    each: Any = None,
) -> dict[str, Any]:
    """The reserve R: the largest envelope this call can return unshrunk.

    A digest form with a `next` token (#3666, Live build 3) needs no room of
    its own: the reserve carries the digest form AND a full-size error body,
    and that body is larger than the longest token and its line, in JSON and
    in text (`test_live_next_token.py` holds this).

    A kept answer's handle needs no room of its own either (PACT
    read-live-handles: every live set read is kept, so every call carries
    one). An `ok` form never carries an error body, and that body is larger
    than the largest `result_reference` block plus the longest token and
    their lines, in JSON and in text; an error form carries only the short
    not-kept block, which the digest form's own fields outweigh
    (`test_live_result_sets.py` and `test_live_rows_text.py` hold both).
    Adding the block on top made the reserve about 470 B larger in JSON, and
    a live call at the 4 KiB floor was refused before it ran (#4060).

    Sized BEFORE the adapter runs, from the validated arguments, so the
    graph step can be paged to leave room for it (issue #2141). A live call
    ends in one of two forms that cannot shrink further: the digest form
    (no rows), or an error form (denied, error, timeout), whose `error` body
    `_capped_error` holds to `ERROR_DATA_CAP_BYTES`. Both are built at their
    worst case -- the longest state name, a full-size error body, a
    capped `first_row_keys` at its cap, the largest counts -- and the
    reserve is the digest form with the error body beside it, larger than
    either in JSON and in text. The bounded error form is smaller than
    either, so whatever the live step returns fits in the room the reserve
    kept. `generation_id` is the graph page's pin; the caller that sizes a
    graph page substitutes its own, because a pin has no fixed length.
    """
    from okg.substrate.mcp.response_bounds import (
        ERROR_DATA_CAP_BYTES,
        MAX_RESPONSE_BYTES,
    )

    relation = bound.relation
    each_key: Optional[str] = None
    if each is not None:
        each_key, per_value = validate_each(bound, args, each, args_model=args_model)
        validated = each_query(each_key, per_value)
    else:
        validated = validate_live_arguments(bound, args, args_model=args_model)
    fingerprint = canonical_args_fingerprint(validated)
    base = _base_envelope(
        bound=bound, validated=validated, fingerprint=fingerprint,
        observed_at=datetime.now(timezone.utc),
        generation_id=generation_id,
    )
    if each_key is not None:
        base["each"] = each_key
    pointer = _refetch_pointer(
        relation=relation.name, source=bound.binding.kind, query=validated,
        args_fingerprint=fingerprint,
    )
    # One key whose list serialises to exactly the cap.
    placeholder_key = "k" * (FIRST_ROW_KEYS_MAX_BYTES - len('[""]'))
    digest_form = {
        **base,
        "state": "ok",
        "policy": "digest", "declared_policy": "inline",
        **_digest_fragment(
            digest="sha256:" + "0" * 64,
            row_count=MAX_RESULT_NODES,
            total_bytes=2**31 - 1,
            first_row_keys=[placeholder_key],
            keys_truncated=True,
            relation=relation.name, source=bound.binding.kind,
            pointer=pointer, rows_returned=0,
            budget_bytes=MAX_RESPONSE_BYTES,
        ),
    }
    error_form = {
        **base,
        "state": "timeout",
        "error": {"reason": "e" * (ERROR_DATA_CAP_BYTES - len('{"reason":""}'))},
    }

    # Both forms in one (#3666): the text form prints a digest form and an
    # error form very differently from their JSON, so the larger JSON need
    # not be the larger text. The digest form with the error body beside it
    # is at least as large as either, in JSON and in text (the text form
    # prints an `error` it finds on any envelope).
    return {**digest_form, "error": error_form["error"]}


def dispatch_live_relation(
    *,
    bound: BoundLiveRelation,
    args_model: type,
    args: Mapping[str, Any] | None,
    node_id: str,
    gate: Callable[[LiveRelation, Optional[str], Optional[str]], GateOutcome],
    adapter: Callable[..., Any],
    write_sample: Callable[..., Any],
    room: Any,
    refuse_before_adapter: Optional[str] = None,
    principal: Optional[str] = None,
    generation_id: Any = None,
    now: Optional[datetime] = None,
    retain: Optional[Callable[[Any, Mapping[str, Any]], Any]] = None,
    keep: Optional[Callable[[Any, Mapping[str, Any]], Any]] = None,
    each: Any = None,
) -> dict[str, Any]:
    """Run one live call and return its `external_live` envelope.

    `each` (`{argument: [values]}`) makes it one call per value, joined
    into one answer (:func:`each_adapter`); `query` then lists the values.

    `keep` (#3666, Live build 3) is called, like `retain` and with the same
    arguments, only when the answer is CUT to fit (rows left out, or fields
    or values shortened) and the caller did not already keep it. The kept
    set backs one `next` token on the envelope (`next`, `next_from_row`):
    `expand(next=...)` serves the following rows from it, every field whole,
    without calling the adapter again. Fields are cut before rows, so a
    token after a field cut starts again at row 1; after a row cut, at the
    first row not shown. A set that cannot be kept says why in
    `next_unavailable`.

    `retain`, when the caller asked to keep the answer, is called once with
    the adapter's UNWRAPPED result and the `ok` envelope base, before the
    rows are fitted. It returns a `live_result_sets.RetainHandle`, whose
    `result_reference` block is counted in every fit. Any state other than
    `ok`, and the bounded error form, keep nothing.

    `room` is the one response budget as this call sees it (a
    `response_bounds.ResponseRoom`): what the graph rows beside the envelope
    left. The rows shrink to fit it, and a final check replaces anything
    that still does not fit with the bounded error form BEFORE the sample is
    written, so the sample is always what the caller received. It is a
    required argument: `None` says, explicitly, that no response budget
    applies, which only a direct caller outside MCP may say.

    `refuse_before_adapter` names why the call is refused before its
    adapter could run -- today only the byte budget, when not even one graph
    row plus the reserve fits. The gate is still asked, so the sample
    records a real principal and verdict; the adapter is not called; the
    sample's status is `error` and `attrs.refused_before_adapter` carries
    the reason. A new sample status would need a schema change to three
    hypertables' CHECK constraints for one reason code.

    `adapter` and `write_sample` are injected rather than imported so the
    deny tests can count invocations of a fake that raises if it is ever
    called. That is the only way to prove "denied never invokes the adapter":
    checking the returned state cannot tell a call that never happened from
    one whose result was discarded.

    Never raises for a live FAILURE. A denied, errored or timed-out live call
    still returns, with its state in the envelope, because the pinned graph
    rows beside it are a complete and correct answer to the graph question
    and throwing them away because an external system was down would make
    every live relation a liability on the read. The one exception is a
    failed sample write, which raises: see :class:`LiveSampleWriteFailed`.
    """
    relation = bound.relation
    observed_at = now or datetime.now(timezone.utc)
    # Every upstream call holds one of its system's shared slots (#3666,
    # lane ri): a direct call, each value of a fan-out, a run's live step.
    slots = upstream_slots(bound)
    run_adapter = adapter
    #: A direct call waits for its upstream slot before it takes a live slot.
    capped_slots: Any = slots
    deadline_seconds = relation.deadline_seconds
    #: Filled when the external system answered "nothing" (matched nothing,
    #: not found) for the call or for some values of a fan-out: those are
    #: 0 rows, `state: ok`, and the reply says so.
    matched_nothing: dict[str, Any] = {}
    if each is not None:
        each_key, per_value = validate_each(bound, args, each, args_model=args_model)
        validated = each_query(each_key, per_value)
        run_adapter = each_adapter(
            adapter, each_key, per_value, relation.deadline_seconds, slots=slots,
            note=matched_nothing,
        )
        # A fan-out holds one live slot; each value takes its upstream slot.
        capped_slots = None
        deadline_seconds = each_deadline(relation.deadline_seconds, len(per_value))
    else:
        validated = validate_live_arguments(bound, args, args_model=args_model)
        run_adapter = _empty_match_as_rows(adapter, matched_nothing, args)
    fingerprint = canonical_args_fingerprint(validated)

    # WHO this call acts as, resolved on one explicit branch (#2160). An
    # unresolved subject is refused HERE and the gate is never asked: a call
    # with no identity is not a policy question, the same way a malformed
    # argument is not one. Asking anyway would make the answer depend on the
    # bundle, and a bundle that allows the action for everyone would then
    # allow a call whose caller nobody could name.
    subject = resolve_live_principal(bound, principal)
    if subject:
        outcome = gate(relation, subject, node_id)
    else:
        outcome = GateOutcome(
            allowed=False, reason=PRINCIPAL_UNRESOLVED_REASON,
            principal=UNRESOLVED_PRINCIPAL, action=relation.action,
        )

    envelope: dict[str, Any] = _base_envelope(
        bound=bound, validated=validated, fingerprint=fingerprint,
        observed_at=observed_at, generation_id=generation_id,
    )
    if each is not None:
        # The argument the answer was fanned out over (its values: `query`).
        envelope["each"] = each_key

    payload: Any = None
    # What the SAMPLE stores, and the pointer a digest sample needs. Both
    # default to None so a denied, errored or timed-out call -- which has no
    # payload -- degrades to a metadata sample and needs no pointer.
    sample_payload: Any = None
    refetch: Any = None
    sample_attrs_extra: dict[str, Any] = {}
    #: False when the caller received no rows, so an inline sample policy
    #: stores the digest: the sample holds what the caller received.
    store_inline = True
    status: str
    # Whether the ADAPTER ran, tracked explicitly. It used to be inferred
    # from `outcome.allowed`, which was right until a gate-allowed call could
    # fail before dispatch: concurrency exhaustion then reported
    # `adapter_ran=True` for a call that never started.
    adapter_ran = False
    #: A `BaseException` the adapter raised -- `SystemExit`, a
    #: `KeyboardInterrupt`, anything outside `Exception`. It is RE-RAISED
    #: after the sample is written (review finding L4): the backstop below
    #: used to catch `Exception` only, so such a call escaped
    #: `dispatch_live_relation` with the external system already contacted
    #: and no record of it -- the one outcome the sample policy exists to
    #: prevent. Swallowing it is not the answer either: an interpreter-level
    #: exit means what it says and keeps travelling.
    base_exit: BaseException | None = None
    #: The retained-set handle (live result sets), once the adapter answered.
    retain_handle: Any = None
    #: The set kept because the answer was cut (#3666, Live build 3).
    keep_handle: Any = None
    #: `next`, `next_from_row` or `next_unavailable` for a cut answer.
    next_fragment: dict[str, Any] = {}
    if not outcome.allowed:
        # No adapter call. The next statement is the sample write; there is
        # nothing between this branch and the return that could reach one.
        status = "denied"
        envelope["state"] = "denied"
        envelope["error"] = _capped_error({
            "reason": outcome.reason,
            "relation": relation.name,
            "principal": outcome.principal,
            "action": outcome.action,
            **({"gate_raised": True} if outcome.raised else {}),
        })
        if refuse_before_adapter is not None:
            # The gate's verdict stays the status, but the caller received
            # the budget refusal, not this denial: the sample must say why
            # no live answer was served.
            sample_attrs_extra["refused_before_adapter"] = refuse_before_adapter
    elif refuse_before_adapter is not None:
        # Allowed, and still not run: the response could not hold even the
        # smallest answer. Nothing between this branch and the sample write
        # can reach the adapter.
        status = "error"
        envelope["state"] = "error"
        envelope["error"] = _capped_error({
            "reason": refuse_before_adapter,
            "relation": relation.name,
        })
        sample_attrs_extra["refused_before_adapter"] = refuse_before_adapter
    else:
        # Shaping lives INSIDE this guard. It used to sit in the `else`
        # clause, so a RecursionError or ValueError while fencing or
        # serialising meant the adapter had run and NO sample was written --
        # the one outcome the sample policy exists to prevent.
        def _mark_adapter_started() -> None:
            """The adapter is committed to run; the sample must say so.

            Set from the guard's own owner rather than inferred here: the
            slot, the executor and the deadline all belong to
            `_run_adapter_under_deadline`, and exhaustion is refused there
            BEFORE anything is submitted, which is the one exit from that
            function where the adapter did not run.
            """
            nonlocal adapter_ran
            adapter_ran = True

        try:
            payload = (
                _run_adapter_under_deadline(
                    run_adapter,
                    config=dict(bound.binding.config),
                    args=dict(validated),
                    node_id=node_id,
                    deadline_seconds=deadline_seconds,
                    on_started=_mark_adapter_started,
                ) if capped_slots is None else _run_capped_under_deadline(
                    run_adapter, capped_slots,
                    config=dict(bound.binding.config),
                    args=dict(validated),
                    node_id=node_id,
                    deadline_seconds=deadline_seconds,
                    on_started=_mark_adapter_started,
                )
            )
            # The external system's own cut, said on every form the rows are
            # fitted into, so it is counted in every fit.
            upstream_fragment = _upstream_cut_fragment(bound, validated, payload)
            if matched_nothing:
                # Counted in every fit, like the external system's own cut.
                # The system's own words travel, fenced and redacted like any
                # upstream excerpt, so a reader can tell its "nothing" apart.
                said = dict(matched_nothing)
                if said.get("excerpt"):
                    said["excerpt"] = _safe_excerpt(
                        said["excerpt"], relation.name, kind="external_live_said",
                    )
                upstream_fragment = {**upstream_fragment, "matched_nothing": said}
            ok_base = {**envelope, "state": "ok", **upstream_fragment}
            # What a kept set records: the FULL arguments, every fanned-out
            # value included (its redo call and its totals' cover line count
            # them), where the reply prints only the first few.
            hook_base = {**ok_base, "query": dict(validated)}
            if retain is not None:
                # The structural check first, exactly as shaping runs it: a
                # depth or width bomb is `result_unusable` whether or not the
                # caller asked to keep the answer, never a capture failure.
                _assert_result_is_shapeable(payload)
                retain_handle = _begin_retain_distinctly(retain, payload, hook_base)
            reference_fragment = (
                {"result_reference": retain_handle.block}
                if retain_handle is not None else {}
            )
            def shape(extra: Mapping[str, Any]) -> ShapedRows:
                return _shape_distinctly(
                    payload,
                    relation=relation.name,
                    source=bound.binding.kind,
                    query=dict(validated),
                    args_fingerprint=fingerprint,
                    fits=(
                        None if room is None
                        else lambda fragment: (
                            (each is None or _within_each_rows(fragment))
                            and room.fits(
                                {**ok_base, **fragment, **reference_fragment, **extra},
                            )
                        )
                    ),
                    budget_bytes=getattr(room, "budget_bytes", None),
                )

            shaped = shape({})
            nested = _nested_cut(shaped)
            if _is_cut(shaped) or nested:
                # Live build 3: keep the whole answer and name ONE call that
                # goes on from it. The caller's own kept set serves when there
                # is one; otherwise `keep` keeps it now.
                holder = retain_handle
                if holder is None and keep is not None:
                    keep_handle = _begin_retain_distinctly(keep, payload, hook_base)
                    holder = keep_handle
                if holder is not None and holder.kept:
                    from okg.substrate.mcp.live_result_sets import (
                        next_token as _next_token,
                        next_token_placeholder,
                    )

                    set_id = holder.draft.id
                    total = len(payload) if isinstance(payload, list) else 1
                    # Fitted with a token at least as long as the real one.
                    flag = {"nested_cut": True} if nested else {}
                    shaped = shape({
                        "next": next_token_placeholder(set_id, total),
                        "next_from_row": total,
                        **flag,
                    })
                    nested = _nested_cut(shaped)
                    if _is_cut(shaped) or nested:
                        # Fields, values or nested entries cut on the rows
                        # shown: the token starts again at row 1, whole.
                        start = (
                            0 if shaped.text_shortened is not None or nested
                            else _rows_served(shaped.envelope)
                        )
                        next_fragment = {
                            "next": _next_token(set_id, start),
                            "next_from_row": start + 1,
                            **({"nested_cut": True} if nested else {}),
                        }
                    elif keep_handle is not None:
                        keep_handle.abandon()
                        keep_handle = None
                elif holder is not None:
                    next_fragment = {
                        "next_unavailable": str(
                            holder.block.get("reason") or "not_kept",
                        ),
                        **({"nested_cut": True} if nested else {}),
                    }
                    # Fitted with the line that says so.
                    shaped = shape(next_fragment)
        except LiveConcurrencyExhausted as exc:
            # Its OWN outcome, and never an adapter crash: the refusal is
            # raised before anything is submitted, so `adapter_ran` stays
            # False and `live_concurrency_exhausted` reaches the caller by
            # name instead of being swallowed by the backstop below.
            status = "error"
            envelope["state"] = "error"
            envelope["error"] = _capped_error({
                "reason": "live_concurrency_exhausted",
                "relation": relation.name,
                "limit": MAX_CONCURRENT_LIVE_CALLS,
                "detail": str(exc),
            })
        except LiveDeadlineExceeded:
            status = "timeout"
            envelope["state"] = "timeout"
            envelope["error"] = _capped_error({
                "reason": "live_relation_timeout",
                "relation": relation.name,
                "deadline_seconds": deadline_seconds,
                "detail": _FAILURE_MESSAGES["timeout"],
                # WHICH mechanism cut the call. The STATE no longer depends
                # on the microsecond race between them (#2149) -- both are
                # `timeout` -- but which one won is still worth recording,
                # because only the dispatcher's arm leaves a worker thread
                # behind.
                "cut_by": "dispatcher",
            })
        except _ShapingFailed as exc:
            # The adapter ANSWERED; fencing, fitting or building the pointer
            # failed on our side. Named apart from an adapter crash, so the
            # log and the envelope do not blame the external system.
            log.warning(
                "live relation %s on %s: shaping the adapter's result raised %s",
                relation.name, node_id, exc.type_name,
            )
            status = "error"
            envelope["state"] = "error"
            envelope["error"] = _capped_error({
                "reason": "live_relation_shaping_failed",
                "relation": relation.name,
                "failure": "shaping_failed",
                "exception": exc.type_name,
                "detail": _FAILURE_MESSAGES["shaping_failed"],
            })
        except LiveAdapterError as exc:
            if exc.failure == "timeout":
                # The adapter cut its own call at the same deadline the
                # dispatcher is holding it to. Same event, same state: a
                # call whose budget ran out is `timeout`, never `error` with
                # a `timeout` failure buried inside it. Which of the two
                # mechanisms won used to decide the state, and it is a race
                # of microseconds (#2149).
                status = "timeout"
                envelope["state"] = "timeout"
                envelope["error"] = _capped_error({
                    "reason": "live_relation_timeout",
                    "relation": relation.name,
                    "deadline_seconds": deadline_seconds,
                    "detail": _FAILURE_MESSAGES["timeout"],
                    "failure": exc.failure,
                    "cut_by": "adapter",
                    **(
                        {"excerpt": _safe_excerpt(exc.excerpt, relation.name)}
                        if exc.excerpt else {}
                    ),
                })
            else:
                status = "error"
                envelope["state"] = "error"
                envelope["error"] = _capped_error({
                    "reason": "live_relation_adapter_error",
                    "relation": relation.name,
                    "failure": exc.failure,
                    "detail": _FAILURE_MESSAGES.get(
                        exc.failure, _FAILURE_MESSAGES["adapter_error"],
                    ),
                    **(
                        {"status": exc.status}
                        if exc.status is not None else {}
                    ),
                    **(
                        {"excerpt": _safe_excerpt(exc.excerpt, relation.name)}
                        if exc.excerpt else {}
                    ),
                    **(
                        _over_cap_remedy(relation)
                        if exc.failure == "response_over_cap" else {}
                    ),
                })
        except Exception as exc:  # noqa: BLE001 — an adapter failure is DATA.
            # Including `TimeoutError` (#2149). An adapter that raises the
            # builtin is reporting ITS OWN failure, not the dispatcher's
            # deadline: nothing was abandoned, no thread is parked, and
            # calling it a timeout here would hide a crash behind the one
            # state operators treat as "the far side was slow". An adapter
            # that means the deadline raises `LiveAdapterError("timeout")`,
            # which is handled above.
            #
            # The exception TYPE is safe to name; its message is not. A
            # network exception's str() routinely carries the URL it was
            # dialling, userinfo included.
            log.warning(
                "live relation %s on %s: adapter raised %s",
                relation.name, node_id, type(exc).__name__,
            )
            status = "error"
            envelope["state"] = "error"
            envelope["error"] = _capped_error({
                "reason": "live_relation_adapter_error",
                "relation": relation.name,
                "failure": "adapter_raised",
                "exception": type(exc).__name__,
                "detail": _FAILURE_MESSAGES["adapter_raised"],
            })
        except (SystemExit, KeyboardInterrupt, GeneratorExit) as exc:
            # The three exceptions outside the `Exception` tree, NAMED rather
            # than caught as `BaseException`: a bare base handler also
            # swallows whatever a future Python or a test framework puts
            # there, and that is a promise this function cannot keep.
            #
            # The call still happened, so it is recorded exactly like any
            # other adapter failure -- type name only, never the message --
            # and the exception is re-raised after the sample write rather
            # than converted into an envelope nobody asked for.
            log.warning(
                "live relation %s on %s: adapter raised %s (re-raised after "
                "the sample is written)",
                relation.name, node_id, type(exc).__name__,
            )
            base_exit = exc
            status = "error"
            envelope["state"] = "error"
            envelope["error"] = _capped_error({
                "reason": "live_relation_adapter_error",
                "relation": relation.name,
                "failure": "adapter_raised",
                "exception": type(exc).__name__,
                "detail": _FAILURE_MESSAGES["adapter_raised"],
            })
        else:
            status = "ok"
            envelope["state"] = "ok"
            envelope.update(shaped.envelope)
            sample_payload = shaped.sample_payload
            refetch = shaped.refetch
            store_inline = shaped.caller_received != "digest"
            sample_attrs_extra["caller_received"] = shaped.caller_received
            sample_attrs_extra["caller_rows_sha256"] = (
                shaped.caller_rows_sha256
            )
            if shaped.caller_received != "rows":
                sample_attrs_extra["rows_returned"] = envelope["rows_returned"]
                sample_attrs_extra["full_set_digest"] = envelope["digest"]
            if shaped.text_shortened is not None:
                # The caller's text cut these rows' values to fit (#3666).
                sample_attrs_extra["text_shortened"] = dict(shaped.text_shortened)
            envelope.update(upstream_fragment)
            envelope.update(next_fragment)
            if matched_nothing:
                sample_attrs_extra["matched_nothing"] = True
            if "upstream_cut" in upstream_fragment:
                sample_attrs_extra["upstream_cut_rows"] = (
                    upstream_fragment["upstream_cut"]["rows"]
                )
                if "next" in next_fragment:
                    # The kept rows page on; the cut is still said by
                    # `upstream_cut` and `truncated`.
                    envelope.pop("next_unavailable", None)
                else:
                    # Nothing past the external system's cut can be paged:
                    # its sentence is the one the cut line prints.
                    envelope["next_unavailable"] = (
                        upstream_fragment["next_unavailable"]
                    )
            if "next" in next_fragment:
                # The row the token starts at; never the token (it holds the
                # kept set's id, which no sample records).
                sample_attrs_extra["next_from_row"] = next_fragment["next_from_row"]
                if keep_handle is not None:
                    keep_handle.finalize(
                        digest=shaped.full_digest,
                        rows_returned=_rows_served(shaped.envelope),
                        refetch=shaped.refetch,
                    )
                    sample_attrs_extra.update(keep_handle.sample_attrs)
            elif "next_unavailable" in next_fragment:
                sample_attrs_extra["next_unavailable"] = next_fragment["next_unavailable"]
            if retain_handle is not None:
                rows_returned = envelope.get("rows_returned")
                if not isinstance(rows_returned, int):
                    rows = envelope.get("rows")
                    rows_returned = len(rows) if isinstance(rows, list) else 0
                retain_handle.finalize(
                    digest=shaped.full_digest, rows_returned=rows_returned,
                    refetch=shaped.refetch,
                )
                envelope["result_reference"] = retain_handle.block
                sample_attrs_extra.update(retain_handle.sample_attrs)
        # No `finally` releasing anything: `_run_adapter_under_deadline`
        # takes the slot and gives it back, for every exit it has (#2142).

    if retain is not None and envelope.get("state") != "ok":
        # Denied, refused, timed out or failed: nothing is kept, and a set
        # drafted before shaping failed gives its reservation back.
        from okg.substrate.mcp.live_result_sets import not_kept

        if retain_handle is not None:
            retain_handle.abandon()
        retain_handle = not_kept("state_not_ok")
        envelope["result_reference"] = retain_handle.block
        sample_attrs_extra.update(retain_handle.sample_attrs)
    if keep_handle is not None and envelope.get("state") != "ok":
        # Kept for a cut answer that then failed (shaping raised): nothing
        # serves its token, so the reservation goes back now.
        keep_handle.abandon()
        keep_handle = None

    # THE FINAL CHECK, before the sample, so the sample is what the caller
    # receives. The rows were already fitted to the room; anything that
    # still does not fit -- an error body, a defect in the fitting, a
    # measure that raised -- becomes the bounded error form. Never a retry
    # (the adapter has run) and never the unbounded envelope.
    if room is not None and refuse_before_adapter is None:
        bound_error: str | None = None
        try:
            fits_room = bool(room.fits(envelope))
        except Exception as exc:  # noqa: BLE001 — the sample must still be written.
            fits_room = False
            bound_error = type(exc).__name__
        if not fits_room:
            log.warning(
                "live relation %s on %s: envelope did not fit the %s-byte "
                "response budget (%s); returning the bounded error form",
                relation.name, node_id, getattr(room, "budget_bytes", None),
                bound_error or "over budget",
            )
            envelope = _bound_failed_envelope(envelope)
            if retain_handle is not None:
                # The bounded error form carries no id, so nothing is kept;
                # the reservation goes back now rather than at the call's end.
                retain_handle.abandon()
                for key in ("retained_rows", "retained_reason"):
                    sample_attrs_extra.pop(key, None)
                sample_attrs_extra.update(retain_handle.sample_attrs)
            if keep_handle is not None:
                # Likewise for a set kept only to back a `next` token.
                keep_handle.abandon()
                keep_handle = None
                for key in ("retained", "retained_rows", "next_from_row"):
                    sample_attrs_extra.pop(key, None)
            # A denied or timed-out call keeps its own status and verdict on
            # the sample; only a call that succeeded and then could not be
            # served becomes an error.
            if status == "ok":
                status = "error"
            sample_payload = None
            refetch = None
            store_inline = True
            for key in ("caller_received", "caller_rows_sha256",
                        "rows_returned", "full_set_digest"):
                sample_attrs_extra.pop(key, None)
            sample_attrs_extra["response_bound_failed"] = True
            if bound_error is not None:
                sample_attrs_extra["response_bound_error"] = bound_error

    # The sample, in its own short transaction, AFTER the adapter has
    # returned or failed. Never inside the call's read transaction.
    #
    # The refusal is BUILT in the handler and RAISED after it (#2143). Raising
    # inside the handler chains the original database error onto the new
    # exception -- `from exc` puts it on `__cause__`, and plain `raise` puts
    # it on `__context__` -- and a psycopg error's text routinely carries the
    # DSN with its password. Every other channel here is redacted, so the
    # traceback would have been the one place it survived, printed by any
    # handler that formats the exception. Raising outside the handler, with no
    # exception being handled, leaves both attributes None; only the TYPE NAME
    # crosses over, as data.
    sample_failure: LiveSampleWriteFailed | None = None
    try:
        write_sample(
            relation=relation.name,
            node_id=node_id,
            principal=outcome.principal,
            # The VERDICT, which is the only thing `SampleIdentity` accepts
            # here -- `allow` or `deny`, nothing else. The bundle's reason
            # code is richer and goes in `attrs`, where it can be read
            # without being mistaken for the decision itself. Putting the
            # reason in this field was refused by the sample store, loudly,
            # which is the store doing its job.
            decision="allow" if outcome.allowed else "deny",
            status=status,
            # The SHAPED payload -- fenced, key-neutralised, depth-capped --
            # not the adapter's raw return. The sample is the audit of what
            # the model received, so it stores what the model received.
            payload=sample_payload,
            refetch=refetch,
            store_inline=store_inline,
            observed_at=observed_at,
            attrs={
                "args_fingerprint": fingerprint,
                "action": outcome.action,
                "adapter_kind": bound.binding.kind,
                "reason_code": outcome.reason,
                "adapter_ran": adapter_ran,
                **sample_attrs_extra,
                **(
                    {"policy_fingerprint": outcome.policy_fingerprint}
                    if outcome.policy_fingerprint else {}
                ),
                **({"gate_raised": True} if outcome.raised else {}),
            },
        )
    except Exception as exc:  # noqa: BLE001 — see below; we RE-RAISE.
        # Out of band, structured, and it says whether the adapter RAN. A
        # rollback cannot un-call an external system; it can only erase the
        # evidence that it was called, so the evidence goes to the log before
        # the call fails.
        log.error(
            "%s relation=%s node=%s principal=%s decision=%s status=%s "
            "adapter_ran=%s error=%s",
            SAMPLE_FAILURE_EVENT, relation.name, node_id, outcome.principal,
            outcome.reason, status, adapter_ran,
            # The exception TYPE only. A psycopg error's message routinely
            # carries connection details, and this record is written on the
            # least-inspected path there is.
            type(exc).__name__,
            extra={
                "event": SAMPLE_FAILURE_EVENT,
                "relation": relation.name,
                "node_id": node_id,
                "principal": outcome.principal,
                "decision": outcome.reason,
                "status": status,
                "adapter_ran": adapter_ran,
            },
        )
        sample_failure = LiveSampleWriteFailed(
            f"live relation {relation.name!r} on node {node_id!r} could not "
            f"write its call sample ({type(exc).__name__}). The call "
            f"fails: an external call with no record is what the sample "
            f"policy exists to prevent. The adapter "
            f"{'DID' if adapter_ran else 'did NOT'} run; a structured "
            f"{SAMPLE_FAILURE_EVENT} record was emitted out of band.",
            data={
                "relation": relation.name,
                "node_id": node_id,
                "adapter_ran": adapter_ran,
                "event": SAMPLE_FAILURE_EVENT,
            },
            fix_hint=(
                "check the call-sample hypertable for the relation's "
                "retention class and the deployment's write connection"
            ),
        )

    if sample_failure is not None:
        # Outside the handler. `raise ... from None` would not be enough: it
        # clears `__cause__` and sets `__suppress_context__`, but `__context__`
        # still holds the original exception and any code that reads it -- or
        # a formatter that ignores the suppression flag -- prints the DSN.
        raise sample_failure

    if base_exit is not None:
        # The record is written; the interpreter-level exception continues on
        # its way, with its own traceback and nothing chained onto it. Raised
        # AFTER `sample_failure` because "the call has no record" is the
        # worse failure of the two and must be the one the caller sees.
        raise base_exit

    return envelope


__all__ = [
    "GATE_DENY_REASON",
    "GATE_ERROR_REASON",
    "LIVE_STATES",
    "LIVE_TRUNCATION_REASON",
    "SAMPLE_FAILURE_EVENT",
    "GateOutcome",
    "LiveArgumentsInvalid",
    "LiveAdapterError",
    "LiveConcurrencyExhausted",
    "LiveDeadlineExceeded",
    "MAX_CONCURRENT_LIVE_CALLS",
    "DEFAULT_SERVICE_PRINCIPAL",
    "PRINCIPAL_UNRESOLVED_REASON",
    "UNRESOLVED_PRINCIPAL",
    "LiveRelationUndeclared",
    "resolve_live_principal",
    "LiveRelationsUnboundDeployment",
    "LiveSampleWriteFailed",
    "build_live_gate",
    "canonical_args_fingerprint",
    "dispatch_live_relation",
    "live_reserve_envelope",
    "resolve_live_relation",
    "validate_live_arguments",
]


class LiveAdapterError(SubstrateError):
    """What an adapter raises to say what went wrong, safely.

    An adapter that raises anything else gets a FIXED message per failure
    class and none of its exception text. That is deliberate: a network
    exception's `str()` routinely embeds the full URL it was dialling,
    userinfo included, and the dispatcher used to pass `str(exc)` straight
    into the envelope, the sample and the log. An adapter that wants to say
    more supplies a bounded `excerpt`, which is fenced and redacted here.
    """

    code = "live_adapter_error"

    def __init__(
        self,
        failure: str,
        *,
        excerpt: str | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(f"live adapter failure: {failure}")
        self.failure = failure
        self.excerpt = excerpt
        self.status = status


class UpstreamCutRows(list):
    """Rows from a reply the external system marked as cut short.

    An adapter returns this in place of a plain list when the upstream said,
    beside its rows, that it stopped early (rucio-mcp: `truncated: true` next
    to `items`, at the tool's own `limit`). The dispatcher then never reports
    the answer as whole: the envelope says `truncated: true` and carries
    `upstream_cut`, and the reply says where it was cut and how to ask for
    more (:func:`_upstream_cut_fragment`). A `list` subclass, so every reader
    of rows is unchanged; slicing or copying it gives a plain list, and the
    mark is read once, right after the adapter returns.
    """

    def __init__(
        self, rows: Any, *, received: int,
        parts_cut: int | None = None, parts: int | None = None,
    ) -> None:
        super().__init__(rows)
        #: Rows the external system sent before it stopped.
        self.received = int(received)
        #: For a fanned-out call: how many of its calls were cut, of how many.
        self.parts_cut = parts_cut
        self.parts = parts


#: The relation argument the cut note names as the way to ask for more rows.
#: A naming convention, not a mapping: the binding may slot any argument into
#: the tool's own limit, but the CompOps relations, like the fixtures, call
#: it `limit`. A relation without one is told to narrow its arguments.
UPSTREAM_LIMIT_ARGUMENT = "limit"


def _upstream_cut_fragment(
    bound: BoundLiveRelation, validated: Mapping[str, Any], payload: Any,
) -> dict[str, Any]:
    """What an envelope says when the external system cut its answer, or
    `{}` when it did not.

    `truncated: true` keeps the status line from saying "all shown" (it
    says "rows 1-N of count unknown"), and `next_unavailable` carries the
    sentence the reply prints on its cut line: no `next` token can page
    past rows the external system never sent, so the one call that goes on
    is the same call with a larger `limit`.
    """
    if not isinstance(payload, UpstreamCutRows):
        return {}
    received = payload.received
    what = (
        f"the external system cut {payload.parts_cut} of its {payload.parts} "
        f"answers short ({received} rows in all)"
        if payload.parts is not None
        else f"the external system cut its answer at {received} rows"
    )
    limit_arg = next(
        (
            arg for arg in bound.relation.args
            if arg.name == UPSTREAM_LIMIT_ARGUMENT and arg.type_name == "integer"
        ),
        None,
    )
    current = validated.get(UPSTREAM_LIMIT_ARGUMENT) if limit_arg else None
    top = (
        int(limit_arg.maximum)
        if limit_arg is not None and limit_arg.maximum is not None else None
    )
    block: dict[str, Any] = {"rows": received}
    if payload.parts is not None:
        block.update({"calls_cut": payload.parts_cut, "calls": payload.parts})
    if limit_arg is None:
        remedy = f"{what}; this relation takes no limit, so narrow the arguments"
    else:
        block["limit"] = current
        block["limit_max"] = top
        if top is not None and isinstance(current, int) and current >= top:
            remedy = (
                f"{what}, at the largest limit ({top}); narrow the other "
                f"arguments"
            )
        elif top is not None:
            remedy = (
                f"{what}; ask again with limit up to {top} (this call had "
                f"{current})"
            )
        else:
            remedy = f"{what}; ask again with a larger limit (this call had {current})"
    return {"truncated": True, "upstream_cut": block, "next_unavailable": remedy}


class LiveConcurrencyExhausted(SubstrateError):
    """Too many live calls already in flight in this process.

    A hung adapter leaves an abandoned thread behind (see
    `_run_adapter_under_deadline`), so without a cap a slow external system
    converts every live call into a permanently parked thread and socket
    until the process dies. The cap makes that pressure VISIBLE and bounded:
    the caller is refused by name instead of joining a queue nobody can see.
    """

    code = "live_concurrency_exhausted"


class LiveAdapterUnavailable(SubstrateError):
    """A binding names an adapter kind no runner is registered for.

    Refused at LOAD, by name, rather than at the first call: a deployment
    whose binding can never run should not start believing it can.

    The substrate ships two built-in runners, for kind `http` (#2100) and
    kind `mcp` (PACT live-relation-mcp-adapter), so neither reaches this
    refusal. `sql`, `command` and `python` remain vocabulary in
    `ADAPTER_KINDS` with no runner behind them, and a binding naming one of
    those still fails the build here.
    """

    code = "live_adapter_unavailable"


#: Adapter kinds this REPOSITORY implements, by kind. Filled at import of the
#: module that implements each one; `_build_server` merges it under any
#: caller-supplied `live_adapters`, so production needs no caller to pass
#: anything and a test that supplies its own runner still gets its own.
#:
#: NOT a plugin system, and it must not become one. A deployment cannot add a
#: runner, because a deployment-supplied adapter would be arbitrary code
#: behind a Nomos gate that authorises a RELATION, not the code serving it.
BUILTIN_LIVE_ADAPTERS: dict[str, Callable[..., Any]] = {}


def builtin_live_adapters() -> Mapping[str, Callable[..., Any]]:
    """The built-in runners, read-only, for `_build_server` to merge under.

    The mapping is a read-only view rather than the dict itself, so a caller
    that merges it cannot register a kind by mutating what it was handed.
    Registration belongs to the module that implements a kind, at its own
    import; see `http_adapter.register`.
    """
    return MappingProxyType(BUILTIN_LIVE_ADAPTERS)


def declared_live_relations(
    conn: Any, session: Any, subtype: str,
) -> dict[str, LiveRelation]:
    """Rebuild a subtype's declared live relations from the catalog.

    Reads `okg.subtypes.metadata`, the same jsonb `inspect` reads. The
    connection is used and released here; nothing holds it across the
    adapter call that may follow.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT metadata FROM okg.subtypes "
            "WHERE catalog_version_id = %s AND name = %s",
            (session.catalog_version_id, subtype),
        )
        row = cur.fetchone()
    metadata = row[0] if row and isinstance(row[0], dict) else {}
    stored = metadata.get("live_relations") or {}
    if not isinstance(stored, dict):
        return {}
    return {
        name: LiveRelation.from_metadata(name, subtype, body)
        for name, body in sorted(stored.items())
        if isinstance(body, dict)
    }


class LiveRelationDeclaredTwice(SubstrateError):
    """One live relation name declared on two subtypes (issue #3666, B7).

    Bindings are keyed by relation name alone, so a second declaration made
    the dispatcher's map name whichever subtype sorted last -- the gate
    resource then said the call read one subtype while it served the other.
    A START refusal.
    """

    code = "live_relation_declared_twice"


def _catalog_metadata(conn: Any, session: Any) -> dict[str, dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, metadata FROM okg.subtypes "
            "WHERE catalog_version_id = %s ORDER BY name",
            (session.catalog_version_id,),
        )
        rows = cur.fetchall()
    return {
        str(name): (metadata if isinstance(metadata, dict) else {})
        for name, metadata in rows
    }


def all_declared_live_relations(conn: Any, session: Any) -> dict[str, str]:
    """Every live relation the enrolled catalog declares, name -> subtype.

    Used by the LOAD-time cross-check, which has to see the whole catalog:
    a binding is undeclared only if NO subtype declares it.

    A relation declared on two subtypes is REFUSED
    (:class:`LiveRelationDeclaredTwice`), naming both. It used to be kept
    silently, with the last subtype in name order winning.
    """
    declared: dict[str, str] = {}
    for name, metadata in _catalog_metadata(conn, session).items():
        stored = metadata.get("live_relations") or {}
        if not isinstance(stored, dict):
            continue
        for relation in stored:
            relation = str(relation)
            other = declared.get(relation)
            if other is not None and other != name:
                raise LiveRelationDeclaredTwice(
                    f"live relation {relation!r} is declared on two subtypes, "
                    f"{other!r} and {name!r}. A binding names a relation, not "
                    f"a subtype, so the server could not say which subtype a "
                    f"call reads.",
                    data={"relation": relation, "subtypes": [other, name]},
                    fix_hint=(
                        "declare the relation on one subtype only, or give "
                        "the second one its own name"
                    ),
                )
            declared[relation] = name
    return declared


#: Statement timeout for each start-time stored-node probe, in ms. The probe
#: is `LIMIT 1` and stops at its first match; with no match it reads the node
#: set once per live prefix. Past this budget the start is refused by name,
#: never assumed clean.
STORED_LIVE_NODE_PROBE_MS = 10_000


def _like_literal(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def assert_no_stored_live_nodes(conn: Any, session: Any, registry: Any) -> None:
    """START time: refuse when a stored node uses a live type or prefix.

    One bounded probe per live type, at the session's pinned generation:
    ``SELECT node_id ... WHERE node_id LIKE '<prefix>:%' OR subtype = <type>
    LIMIT 1`` under a statement timeout of
    :data:`STORED_LIVE_NODE_PROBE_MS`. The cost is at most one read of the
    node set per live type (two in part 1), and nothing when the first row
    matches. A probe that runs out of time refuses the start: "could not
    check" is never read as "none".

    The publish-time half is the `no_stored_live_nodes` consistency program
    (`transform/consistency.py`), which blocks a generation adding one.
    """
    from psycopg import pq

    from okg.substrate.external_backed.live_types import LiveTypeInvalid
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    graph = store.checkout_generation(
        session.generation_id, branch=session.branch_name,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('statement_timeout')")
        before = cur.fetchone()[0]
    for live_type in sorted(registry.types.values(), key=lambda t: t.subtype):
        pattern = _like_literal(live_type.id_prefix) + ":%"
        try:
            # The same budget mechanism as the close-id read
            # (`mcp.tools._read_budget`): set, read, put the caller's back.
            conn.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(STORED_LIVE_NODE_PROBE_MS),),
            )
            try:
                rows = graph.query(
                    "SELECT node_id, subtype FROM okg.graph_nodes "
                    "WHERE node_id LIKE :pattern OR subtype = :subtype "
                    "LIMIT 1",
                    {"pattern": pattern, "subtype": live_type.subtype},
                )
            finally:
                if conn.info.transaction_status != pq.TransactionStatus.INERROR:
                    conn.execute(
                        "SELECT set_config('statement_timeout', %s, false)",
                        (before,),
                    )
        except Exception as exc:  # noqa: BLE001 - a probe that did not finish
            raise LiveTypeInvalid(
                f"could not check that no stored node uses live type "
                f"{live_type.subtype!r} or its prefix "
                f"`{live_type.id_prefix}:` at generation "
                f"{session.generation_id} ({type(exc).__name__}). The start "
                f"is refused rather than assumed clean.",
                data={"subtype": live_type.subtype,
                      "prefix": live_type.id_prefix,
                      "generation_id": str(session.generation_id),
                      "reason": "stored_node_check_failed"},
                fix_hint="check the database, then start the server again",
            ) from exc
        if rows:
            row = rows[0]
            raise LiveTypeInvalid(
                f"stored node {row['node_id']!r} (subtype "
                f"{row['subtype']!r}) at generation {session.generation_id} "
                f"uses live type {live_type.subtype!r} or its prefix "
                f"`{live_type.id_prefix}:`. A live type is never stored, so "
                f"a read could not tell the stored node from the live one.",
                data={"subtype": live_type.subtype,
                      "prefix": live_type.id_prefix,
                      "node_id": str(row["node_id"]),
                      "stored_subtype": str(row["subtype"]),
                      "generation_id": str(session.generation_id),
                      "reason": "stored_node_uses_live_prefix"},
                fix_hint=(
                    "retract the stored nodes, or give the live type another "
                    "prefix"
                ),
            )


def build_live_type_registry(
    conn: Any,
    session: Any,
    *,
    bound: Any,
    served: Any = (),
) -> Any:
    """START time: the deployment's live types, checked, as one registry.

    Builds :class:`~okg.substrate.external_backed.live_types.LiveTypeRegistry`
    from the enrolled catalog and refuses, by name: a relation a live type
    names that is not bound; an ``agent_surface.live_types`` name that is
    not a live type; and a stored node that uses a live type or prefix.
    Prefix uniqueness, far-end types and the relation and argument checks
    run inside the registry build.
    """
    from okg.substrate.external_backed.live_types import LiveTypeRegistry

    registry = LiveTypeRegistry.from_catalog(_catalog_metadata(conn, session))
    registry.assert_bound(bound)
    registry.assert_served(served)
    if registry.types:
        assert_no_stored_live_nodes(conn, session, registry)
    return registry


def assert_live_bindings_are_declared(
    conn: Any,
    session: Any,
    bindings: Mapping[str, Any],
    *,
    adapter_runners: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """LOAD-time cross-check of `agent_surface.live_bindings`.

    Issue #2013, review finding 5. `parse_live_bindings` validates the
    manifest's SHAPE and cannot see the catalog, so on its own it never
    catches a binding for a relation nothing declares -- the refusal existed
    in `bind_live_relations` and nothing called it. This is the production
    caller, run from `build_server` for any deployment that binds anything.

    Both directions refuse, and so does an adapter kind with no runner: a
    binding that can never dispatch must fail the start, not the first call.
    Returns relation -> subtype for the relations that checked out.
    """
    declared = all_declared_live_relations(conn, session)
    undeclared = sorted(set(bindings) - set(declared))
    if undeclared:
        raise LiveBindingUndeclaredRelation(
            f"`agent_surface.live_bindings` names "
            f"{', '.join(repr(n) for n in undeclared)}, which no enrolled "
            f"subtype declares. The adapter is configured and can never run. "
            f"Declared relations are {sorted(declared)}.",
            data={"undeclared": undeclared, "declared": sorted(declared)},
            fix_hint=(
                "fix the spelling, enrol the module that declares the "
                "relation, or remove the binding"
            ),
        )
    unbound = sorted(set(declared) - set(bindings))
    if unbound:
        raise LiveRelationUnbound(
            f"live relation(s) {', '.join(repr(n) for n in unbound)} are "
            f"declared by an enrolled subtype but bound by no "
            f"`agent_surface.live_bindings` entry, so nothing could serve "
            f"them. `inspect` would advertise a relation that always fails.",
            data={"unbound": unbound, "bound": sorted(bindings)},
            fix_hint=(
                "add an `agent_surface.live_bindings` entry for each, or "
                "remove the declaration from the subtype"
            ),
        )
    runners = adapter_runners or {}
    for name in sorted(bindings):
        kind = getattr(bindings[name], "kind", None)
        if kind not in runners:
            raise LiveAdapterUnavailable(
                f"`agent_surface.live_bindings.{name}` binds adapter kind "
                f"{kind!r}, but this runtime has no registered runner for "
                f"it. Registered kinds are {sorted(runners)}. The built-in "
                f"runners are `http` and `mcp`; `sql`, `command` and "
                f"`python` are vocabulary with nothing behind them.",
                data={"relation": name, "kind": kind,
                      "registered": sorted(runners)},
                fix_hint=(
                    "register a runner for this kind, or remove the binding"
                ),
            )
    return declared


__all__ += [
    "BUILTIN_LIVE_ADAPTERS",
    "LiveAdapterUnavailable",
    "LiveRelationDeclaredTwice",
    "STORED_LIVE_NODE_PROBE_MS",
    "all_declared_live_relations",
    "assert_no_stored_live_nodes",
    "build_live_type_registry",
    "builtin_live_adapters",
    "assert_live_bindings_are_declared",
    "declared_live_relations",
]


# ---------------------------------------------------------------------------
# One upstream cap per bound system, and `run`'s live steps (#3666, lane ri)
# ---------------------------------------------------------------------------

#: Upstream calls in flight to one bound system (one endpoint) from this
#: process, whoever makes them (review finding 8: 8 live slots times a
#: fan-out's own 8 threads was 64 calls in flight to one system).
UPSTREAM_CALLS_PER_BINDING = budget_width("live.upstream_per_binding")
#: Slots of a system one run live step leaves to other callers: its pool is
#: the cap less these, so a direct call to the system finds one free while
#: a run fans out (review H1).
RUN_LEAVES_FREE = 2
_UPSTREAM_LOCK = threading.Lock()


class FairSlots:
    """One system's upstream slots, given out first come, first served
    (#3666, lane ri, review H1).

    A ``threading.Semaphore`` lets the thread that just released a slot take
    it again before a woken waiter can. A run's fan-out cycles its threads
    through the cap that way, and a direct call to the same system waited
    until its deadline (5 of 6 timed out). Here a waiter keeps its place in
    line: a freed slot goes to the longest waiter."""

    def __init__(self, width: int) -> None:
        self._width = int(width)
        self._free = int(width)
        self._line: collections.deque[object] = collections.deque()
        self._cond = threading.Condition(threading.Lock())

    @property
    def in_use(self) -> int:
        with self._cond:
            return self._width - self._free

    def acquire(
        self, blocking: bool = True, timeout: Optional[float] = None,
        stop: Optional[Callable[[], bool]] = None,
    ) -> bool:
        """Take a slot. False when none came to this waiter within
        `timeout` seconds, or once `stop()` is true (asked every 50 ms)."""
        import time as _time

        with self._cond:
            if self._free > 0 and not self._line:
                self._free -= 1
                return True
            if not blocking or (timeout is not None and timeout <= 0):
                return False
            ticket = object()
            self._line.append(ticket)
            end = None if timeout is None else _time.monotonic() + timeout
            try:
                while True:
                    if self._line[0] is ticket and self._free > 0:
                        self._line.popleft()
                        self._free -= 1
                        return True
                    if stop is not None and stop():
                        return False
                    wait = None if end is None else end - _time.monotonic()
                    if wait is not None and wait <= 0:
                        return False
                    if stop is not None:
                        wait = 0.05 if wait is None else min(wait, 0.05)
                    self._cond.wait(wait)
            finally:
                if ticket in self._line:  # left the line without a slot
                    self._line.remove(ticket)
                # The next in line may now be first with a slot free.
                self._cond.notify_all()

    def release(self) -> None:
        with self._cond:
            if self._free >= self._width:
                raise ValueError("an upstream slot was released that was not taken")
            self._free += 1
            self._cond.notify_all()


_UPSTREAM_SLOTS: dict[str, FairSlots] = {}
#: The binding config keys that name the system a call goes to.
_ENDPOINT_KEYS = ("server_url", "url", "base_url")


def binding_key(bound: BoundLiveRelation) -> str:
    """The system a binding calls: its kind and endpoint. Relations bound to
    one endpoint (every relation of a system names one `server_url`) share a key; a
    binding that names no endpoint key is keyed by its whole config, so it
    is a system of its own."""
    binding = bound.binding
    config = dict(binding.config or {})
    endpoint = next((config[k] for k in _ENDPOINT_KEYS if config.get(k)), None)
    basis = endpoint if endpoint is not None else config
    text = json.dumps(basis, sort_keys=True, default=str)
    return f"{binding.kind}:{hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]}"


def upstream_slots(bound: BoundLiveRelation) -> FairSlots:
    """The shared cap of the system `bound` calls."""
    key = binding_key(bound)
    with _UPSTREAM_LOCK:
        slots = _UPSTREAM_SLOTS.get(key)
        if slots is None:
            slots = FairSlots(UPSTREAM_CALLS_PER_BINDING)
            _UPSTREAM_SLOTS[key] = slots
        return slots


#: Why a direct call never started: no slot of its system came to it.
UPSTREAM_SLOT_TIMEOUT = "no upstream slot of this system came free before the deadline"


def _run_capped_under_deadline(
    adapter: Callable[..., Any],
    slots: Any,
    *,
    config: Mapping[str, Any],
    args: Mapping[str, Any],
    node_id: str,
    deadline_seconds: float,
    on_started: Optional[Callable[[], None]] = None,
) -> Any:
    """A direct call: its system's upstream slot first, then a live slot.

    The upstream wait is in the caller's thread and comes out of the call's
    own deadline; no live slot is held while it waits (review H1: 7 direct
    calls waiting on one system held 7 of the 8 live slots, and calls to
    every other system were refused). The upstream slot is given back when
    the adapter returns, in its worker, so a call abandoned at its deadline
    holds it until its I/O ends; a call that never started gives it back
    here. Whichever side claims the slot first owns its release."""
    import time as _time

    started = _time.monotonic()
    left = float(deadline_seconds)
    if not slots.acquire(blocking=False):
        # Only a call that waited for a slot has less of its deadline.
        if not slots.acquire(timeout=max(0.0, left)):
            raise LiveAdapterError("timeout", excerpt=UPSTREAM_SLOT_TIMEOUT)
        left = max(0.001, left - (_time.monotonic() - started))
    owner_lock = threading.Lock()
    owner: list[Optional[str]] = [None]

    def claim(who: str) -> bool:
        with owner_lock:
            if owner[0] is None:
                owner[0] = who
                return True
            return False

    def capped(**kwargs: Any) -> Any:
        if not claim("worker"):
            # Abandoned before it began: its slot is already given back.
            raise LiveAdapterError("timeout", excerpt="abandoned before it started")
        try:
            return adapter(**kwargs)
        finally:
            slots.release()

    try:
        return _run_adapter_under_deadline(
            capped, config=config, args=args, node_id=node_id,
            deadline_seconds=left, on_started=on_started,
        )
    finally:
        if claim("caller"):
            slots.release()


#: What one value of a run live step came to. `ok`, `empty` and `not_found`
#: are the external system's own answers; the others leave the step partial.
VALUE_OK = "ok"
VALUE_EMPTY = "empty"
VALUE_NOT_FOUND = "not_found"
VALUE_FAILED = "failed"
VALUE_CUT = "cut"
VALUE_NOT_STARTED = "not_started"
VALUE_OUTCOMES = (
    VALUE_OK, VALUE_EMPTY, VALUE_NOT_FOUND, VALUE_FAILED, VALUE_CUT, VALUE_NOT_STARTED,
)
WHOLE_OUTCOMES = frozenset({VALUE_OK, VALUE_EMPTY, VALUE_NOT_FOUND})
#: Failures the external system answers with that are answers, not
#: outages: an empty match is 0 rows (never retried, never partial).
_EMPTY_FAILURES = frozenset({"upstream_empty_result"})
_NOT_FOUND_FAILURES = frozenset({"upstream_not_found"})

#: The executor a run's live step waits on (#3666, lane ri). Its threads
#: open no database connection: they wait on the step's calls.
RUN_WAIT_WIDTH = budget_width("live.run_waits")
_RUN_WAIT_LOCK = threading.Lock()
_RUN_WAIT_EXECUTOR: Optional[ThreadPoolExecutor] = None


def run_wait_executor() -> ThreadPoolExecutor:
    """The wait-only executor, made on first use."""
    global _RUN_WAIT_EXECUTOR
    with _RUN_WAIT_LOCK:
        if _RUN_WAIT_EXECUTOR is None:
            _RUN_WAIT_EXECUTOR = ThreadPoolExecutor(
                max_workers=RUN_WAIT_WIDTH, thread_name_prefix="okg-run-wait",
            )
        return _RUN_WAIT_EXECUTOR


#: Runs whose live step fans out right now: one per reader and logical MCP
#: session (review finding 8, review M3), and at most
#: :data:`HEAVY_RUNS_PER_READER` per reader across its sessions, so one
#: principal cannot hold every wait thread. Without a stable session the
#: reader alone is the key: one such run per reader.
HEAVY_RUNS_PER_READER = 2
_HEAVY_LOCK = threading.Lock()
_HEAVY_RUNS: set[str] = set()
_HEAVY_PER_READER: "collections.Counter[str]" = collections.Counter()


def _heavy_key(reader_key: str, session: Optional[str]) -> str:
    return reader_key if not session else f"{reader_key}:{session}"


def claim_live_heavy_run(reader_key: str, session: Optional[str] = None) -> bool:
    """Take a fan-out run for this reader and session; False when the
    session already has one or the reader has its ceiling."""
    key = _heavy_key(reader_key, session)
    with _HEAVY_LOCK:
        if key in _HEAVY_RUNS or _HEAVY_PER_READER[reader_key] >= HEAVY_RUNS_PER_READER:
            return False
        _HEAVY_RUNS.add(key)
        _HEAVY_PER_READER[reader_key] += 1
        return True


def release_live_heavy_run(reader_key: str, session: Optional[str] = None) -> None:
    key = _heavy_key(reader_key, session)
    with _HEAVY_LOCK:
        if key not in _HEAVY_RUNS:
            return
        _HEAVY_RUNS.discard(key)
        _HEAVY_PER_READER[reader_key] -= 1
        if _HEAVY_PER_READER[reader_key] <= 0:
            del _HEAVY_PER_READER[reader_key]


#: Heavy reads of a live TYPE (a set read that calls once per node, spec 7,
#: N4; PACT live-graph-rucio-sites) at once, per reader and okg logical
#: session. Without a durable session (stateless HTTP) the reader alone is
#: the key. Counted apart from `run`'s fan-out runs above.
LIVE_TYPE_READS_PER_KEY = HEAVY_RUNS_PER_READER
_LIVE_TYPE_READS: "collections.Counter[str]" = collections.Counter()


def claim_live_type_read(reader_key: str, session: Optional[str] = None) -> bool:
    """Take a heavy live-type read for this reader and session; False when
    that key already runs :data:`LIVE_TYPE_READS_PER_KEY`."""
    key = _heavy_key(reader_key, session)
    with _HEAVY_LOCK:
        if _LIVE_TYPE_READS[key] >= LIVE_TYPE_READS_PER_KEY:
            return False
        _LIVE_TYPE_READS[key] += 1
        return True


def release_live_type_read(reader_key: str, session: Optional[str] = None) -> None:
    key = _heavy_key(reader_key, session)
    with _HEAVY_LOCK:
        if _LIVE_TYPE_READS[key] <= 1:
            _LIVE_TYPE_READS.pop(key, None)
        else:
            _LIVE_TYPE_READS[key] -= 1


@dataclass
class LiveCallsResult:
    """What a run live step's calls came to, value by value."""

    #: Every row of every answered value, in value order, each a JSON object.
    rows: list[dict[str, Any]]
    #: One outcome (`VALUE_OUTCOMES`) per value, in value order.
    outcomes: list[str]
    #: Why a value failed, was cut or was not started (None for the rest).
    reasons: list[Optional[str]]
    #: Adapter calls started.
    started: int
    #: Bytes of rows held (compact JSON).
    held_bytes: int
    #: Why the step stopped starting calls: `deadline`, `memory`,
    #: `cancelled`, or None when every value was called.
    stopped: Optional[str]
    #: Rows that were not objects, held as {"value": <row>}.
    non_object_rows: int
    elapsed_ms: int

    def counts(self) -> dict[str, int]:
        return {name: self.outcomes.count(name) for name in VALUE_OUTCOMES}

    @property
    def partial(self) -> bool:
        return any(outcome not in WHOLE_OUTCOMES for outcome in self.outcomes)

    @property
    def answered(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome != VALUE_NOT_STARTED)


def gate_for_call(
    bound: BoundLiveRelation,
    gate: Callable[[LiveRelation, Optional[str], Optional[str]], GateOutcome],
    node_id: str,
    principal: Optional[str] = None,
) -> GateOutcome:
    """The relation's gate, asked as `dispatch_live_relation` asks it: an
    unresolved subject is denied without asking."""
    subject = resolve_live_principal(bound, principal)
    if subject:
        return gate(bound.relation, subject, node_id)
    return GateOutcome(
        allowed=False, reason=PRINCIPAL_UNRESOLVED_REASON,
        principal=UNRESOLVED_PRINCIPAL, action=bound.relation.action,
    )


def run_live_calls(
    *,
    bound: BoundLiveRelation,
    adapter: Callable[..., Any],
    per_value: list[dict[str, Any]],
    each_key: Optional[str],
    node_id: str,
    deadline: float,
    stop: threading.Event,
    max_bytes: int,
) -> LiveCallsResult:
    """Every call of one run live step; no database (#3666, lane ri).

    Runs on the wait-only executor. It takes one live slot for the whole
    step, as a direct ``expand(live=...)`` takes one (none free is
    :class:`LiveConcurrencyExhausted`, before any call), and each value's
    call holds one of its system's shared upstream slots
    (:func:`upstream_slots`). Each value's outcome is counted: the system's
    answers (`ok`, `empty`: it said nothing matched, `not_found`: it said
    the named object does not exist) leave the step whole; `failed`, `cut`
    (the system cut its own answer short) and `not_started` leave it
    partial.

    No call starts once the run's `stop` is set (the client went away) or
    `deadline` (``time.monotonic``) has passed. The step's own halt is set
    under the same lock as the check and the count of started calls, so
    the count of adapter calls is flat from the moment this function
    returns; `stop` is never set here, so a later step of the run starts. Calls not yet started are cancelled
    (``cancel_futures``); a call in flight is abandoned, counted `failed`
    (cut at the run's deadline) and its answer discarded. The live slot is
    given back when the last abandoned call returns. Past `max_bytes` of
    rows no further call starts (`stopped` = ``memory``)."""
    import time as _time
    from concurrent.futures import FIRST_COMPLETED, wait

    started_at = _time.monotonic()
    live_slots = _take_live_slot()
    if live_slots is None:
        raise LiveConcurrencyExhausted(
            f"{MAX_CONCURRENT_LIVE_CALLS} live calls are already in flight in "
            "this process; this live step is refused rather than queued",
            data={"limit": MAX_CONCURRENT_LIVE_CALLS},
            fix_hint="send the run again once the other live calls have returned",
        )
    upstream = upstream_slots(bound)
    total = len(per_value)
    outcomes = [VALUE_NOT_STARTED] * total
    reasons: list[Optional[str]] = [None] * total
    answers: list[Any] = [None] * total
    lock = threading.Lock()
    state: dict[str, Any] = {
        "started": 0, "bytes": 0, "closed": False, "stopped": None,
        "inflight": set(),
    }
    relation_deadline = float(bound.relation.deadline_seconds)
    config = dict(bound.binding.config)
    #: This step's own stop (the byte bound, the deadline, the step's end);
    #: `stop` is the run's, set only from outside (the client went away).
    halt = threading.Event()

    def halted() -> bool:
        return halt.is_set() or stop.is_set()

    def _stop(reason: str) -> None:
        # Under `lock`: the first reason stands.
        if state["stopped"] is None:
            state["stopped"] = reason
        halt.set()

    def one(index: int) -> None:
        validated = per_value[index]
        left = deadline - _time.monotonic()
        # In line for a slot of the system until one comes, the step halts
        # or the deadline passes; the line is first come, first served.
        if halted() or left <= 0 or not upstream.acquire(timeout=left, stop=halted):
            return
        try:
            with lock:
                if halted() or _time.monotonic() >= deadline:
                    return
                state["started"] += 1
                state["inflight"].add(index)
            left = deadline - _time.monotonic()
            outcome, reason, answer = VALUE_FAILED, None, None
            try:
                answer = adapter(
                    config=config, args=dict(validated), node_id=node_id,
                    deadline_seconds=max(0.001, min(left, relation_deadline)),
                )
                _assert_result_is_shapeable(answer)
            except LiveAdapterError as exc:
                if exc.failure in _EMPTY_FAILURES:
                    outcome, answer = VALUE_EMPTY, []
                elif exc.failure in _NOT_FOUND_FAILURES:
                    outcome, answer = VALUE_NOT_FOUND, []
                else:
                    reason = exc.failure
            except Exception as exc:  # noqa: BLE001 - a value's failure is counted, by type.
                reason = f"adapter_raised:{type(exc).__name__}"
            else:
                rows = answer if isinstance(answer, list) else [answer]
                cut = isinstance(answer, UpstreamCutRows)
                outcome = VALUE_CUT if cut else (VALUE_OK if rows else VALUE_EMPTY)
                if cut:
                    reason = f"the system cut its answer at {answer.received:,} row" + ("" if answer.received == 1 else "s")
                answer = list(rows)
            size = 0
            if answer:
                try:
                    size = len(json.dumps(answer, separators=(",", ":"), default=str))
                except (TypeError, ValueError):
                    outcome, reason, answer = VALUE_FAILED, "result_unusable", None
            with lock:
                state["inflight"].discard(index)
                if state["closed"]:
                    return
                outcomes[index] = outcome
                reasons[index] = reason
                answers[index] = answer
                state["bytes"] += size
                if state["bytes"] > max_bytes:
                    _stop("memory")
        finally:
            upstream.release()

    try:
        pool = ThreadPoolExecutor(
            # The cap less RUN_LEAVES_FREE: a direct call to the system
            # finds a slot while this step fans out (review H1).
            max_workers=max(1, min(UPSTREAM_CALLS_PER_BINDING - RUN_LEAVES_FREE, total)),
            thread_name_prefix="okg-run-live",
        )
    except BaseException:
        live_slots.release()
        raise
    futures: list[Any] = []
    remaining = [total]
    remaining_lock = threading.Lock()

    def _finished(_future: Any) -> None:
        with remaining_lock:
            remaining[0] -= 1
            last = remaining[0] == 0
        if last:
            live_slots.release()

    pending: set[Any] = set()
    try:
        futures.extend(pool.submit(one, index) for index in range(total))
        pending = set(futures)
        while pending:
            left = deadline - _time.monotonic()
            if left <= 0:
                break
            if stop.is_set() and state["stopped"] is None:
                # Cancelled from outside (the client went away). Past the
                # byte bound the calls in flight are waited for: their
                # rows are counted, and nothing new starts.
                break
            _done, pending = wait(pending, timeout=min(left, 0.25), return_when=FIRST_COMPLETED)
    finally:
        # A value never submitted (a failed submit) is a future that never
        # finishes: count it finished now, so the slot comes back.
        with remaining_lock:
            remaining[0] -= total - len(futures)
            if remaining[0] == 0:
                live_slots.release()
        with lock:
            # A value left unstarted is a stop too: once `stop` or the
            # deadline is seen, the queued calls return at once, so every
            # future can be done before this loop looks at `stop` again.
            if pending or VALUE_NOT_STARTED in outcomes:
                _stop("cancelled" if stop.is_set() and state["stopped"] is None
                      and _time.monotonic() < deadline else "deadline")
            else:
                halt.set()
            state["closed"] = True
            for index in state["inflight"]:
                outcomes[index] = VALUE_FAILED
                reasons[index] = (
                    "in flight at the run's deadline; its answer is not read"
                    if state["stopped"] == "deadline"
                    else f"in flight when the step stopped ({state['stopped']}); not read"
                )
            started = state["started"]
            held = state["bytes"]
            stopped = state["stopped"]
        pool.shutdown(wait=False, cancel_futures=True)
        for future in futures:
            future.add_done_callback(_finished)
    for index, outcome in enumerate(outcomes):
        if outcome == VALUE_NOT_STARTED:
            reasons[index] = stopped or "not started"
    rows: list[dict[str, Any]] = []
    non_objects = 0
    for index, answer in enumerate(answers):
        if outcomes[index] not in (VALUE_OK, VALUE_CUT) or not answer:
            continue
        value = per_value[index].get(each_key) if each_key is not None else None
        for row in answer:
            if not isinstance(row, Mapping):
                non_objects += 1
                row = {"value": row}
            elif not all(isinstance(key, str) for key in row):
                row = {str(key): item for key, item in row.items()}
            if each_key is not None and each_key not in row:
                row = {each_key: value, **row}
            rows.append(dict(row))
    return LiveCallsResult(
        rows=rows, outcomes=outcomes, reasons=reasons, started=started,
        held_bytes=held, stopped=stopped, non_object_rows=non_objects,
        elapsed_ms=int((_time.monotonic() - started_at) * 1000),
    )


def run_sample_kwargs(
    *,
    bound: BoundLiveRelation,
    node_id: str,
    outcome: GateOutcome,
    query: Mapping[str, Any],
    result: Optional[LiveCallsResult],
    observed_at: datetime,
    refused: Optional[str] = None,
) -> dict[str, Any]:
    """The call sample of one run live step (#3666, lane ri): what
    ``write_sample`` takes. The rows went to the run, not to a reply, so the
    sample holds their digest and the step's counts, never the rows."""
    from okg.substrate.external_backed.call_sample import digest_of

    attrs: dict[str, Any] = {
        "args_fingerprint": canonical_args_fingerprint(query),
        "action": outcome.action,
        "adapter_kind": bound.binding.kind,
        "reason_code": outcome.reason,
        "adapter_ran": bool(result is not None and result.started),
        "served_to": "run",
        **(
            {"policy_fingerprint": outcome.policy_fingerprint}
            if outcome.policy_fingerprint else {}
        ),
        **({"gate_raised": True} if outcome.raised else {}),
    }
    if refused is not None:
        attrs["refused_before_adapter"] = refused
    if not outcome.allowed:
        status = "denied"
    elif result is None:
        status = "error"
    else:
        counts = result.counts()
        attrs.update({
            "run_values": len(result.outcomes),
            "run_outcomes": counts,
            "run_calls_started": result.started,
            "run_rows": len(result.rows),
            "run_rows_sha256": "sha256:" + digest_of(result.rows)[0],
            **({"run_stopped": result.stopped} if result.stopped else {}),
        })
        answered = counts[VALUE_OK] + counts[VALUE_EMPTY] + counts[VALUE_NOT_FOUND] + counts[VALUE_CUT]
        if answered:
            status = "ok"
        elif result.stopped == "deadline":
            status = "timeout"
        else:
            status = "error"
    return {
        "relation": bound.relation.name,
        "node_id": node_id,
        "principal": outcome.principal,
        "decision": "allow" if outcome.allowed else "deny",
        "status": status,
        "payload": None,
        "refetch": None,
        "store_inline": True,
        "observed_at": observed_at,
        "attrs": attrs,
    }


__all__ += [
    "FairSlots",
    "HEAVY_RUNS_PER_READER",
    "LiveCallsResult",
    "RUN_LEAVES_FREE",
    "UPSTREAM_CALLS_PER_BINDING",
    "VALUE_OUTCOMES",
    "WHOLE_OUTCOMES",
    "binding_key",
    "claim_live_heavy_run",
    "claim_live_type_read",
    "gate_for_call",
    "release_live_heavy_run",
    "release_live_type_read",
    "run_live_calls",
    "run_sample_kwargs",
    "run_wait_executor",
    "upstream_slots",
]
