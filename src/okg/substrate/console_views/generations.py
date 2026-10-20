"""Recent-generations composer for the console (B4 substrate debugging).

"What did the last few publishes actually change?" — the operator's first
debugging question. Chronos checkpoints are now the generation registry, so
this composer reads generation history from Chronos metadata and computes
node/edge deltas by comparing each checkpoint's pinned graph counts.

Backs the right-rail generation panel opened from the topbar gen pill.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from okg.substrate.console_views.attempts_by_day import (
    parse_checkpoint_created_at,
)
from okg.substrate.console_views._chronos import (
    GENERATION_COUNT_METADATA_KEYS,
    GenerationCountsUnavailable,
    counts_from_projected_metadata,
)
from okg.substrate.console_views.console_count_rollup_read import (
    require_console_count_rollup_enabled,
)
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp.session import Session, assert_session_matches

_MAX_LIMIT = 50

# The checkpoint metadata `compose_attempt_row` reads. Named here, and
# asserted against that function's source by a guard test, so the bounded
# reader projects these keys and leaves the rest in the database.
ATTEMPT_ROW_METADATA_KEYS = (
    "status",
    "target_generation_id",
    "blocked_reason",
    "excluded_sources",
)


class UnknownGenerationCursor(ValueError):
    """The cursor names no checkpoint in this deployment's history.

    Raised rather than answered with the first page. A reader that asked for
    "what comes after X" and was handed the newest rows again would append them
    to a table that already holds them, and every attempt would be listed
    twice. The endpoint turns this into a named 400.
    """

    def __init__(self, cursor: str) -> None:
        super().__init__(
            f"cursor {cursor!r} names no attempt in this deployment's "
            "generation history; no page was returned",
        )
        self.cursor = cursor


# One grouped read of the per-producer results recorded for the attempts on
# THIS page. okg.derived_artifact_runs.target_generation_id (db/schema.sql) is
# the same value the publisher writes into a checkpoint's
# metadata["target_generation_id"], so the join is the record's own key and not
# a guess. It is a single query over the page's ids -- never one query per
# attempt -- and it is filtered to views: nothing records a SOURCE run against
# a generation (okg.v_source_run_diagnostics is keyed by run_id out of
# okg.source_progress and carries no generation), so no SOURCE rows are served.
#
# Every artifact_kind is served, not just 'view'. The 'view' filter that stood
# here returned nothing on a real deployment whose only definition is a
# 'consistency_check' -- one run per attempt, recorded and keyed, while the
# page said "no per-producer result is recorded". The kind is named on each
# row instead of being assumed.
#
# The cap is PER ATTEMPT, by a window function. A page-wide LIMIT ordered
# newest-first starved the older attempts on the page: they came back with no
# rows and the page reported that as "none recorded", which is a different
# fact. Each attempt carries its own runs_total and runs_capped.
_VIEW_RUNS_SQL = """
SELECT * FROM (
  SELECT
    run.target_generation_id,
    run.definition_id,
    def.artifact_kind,
    run.run_id,
    run.status,
    run.trigger_kind,
    run.error,
    run.started_at,
    run.completed_at,
    row_number() OVER (
      PARTITION BY run.target_generation_id
      ORDER BY run.started_at DESC, run.run_id
    ) AS rank_in_attempt,
    count(*) OVER (PARTITION BY run.target_generation_id) AS runs_total
  FROM okg.derived_artifact_runs run
  JOIN okg.derived_artifact_definitions def
    ON def.definition_id = run.definition_id
  WHERE def.deployment_name = %(deployment)s
    AND run.target_generation_id = ANY(%(targets)s)
) ranked
WHERE rank_in_attempt <= %(per_attempt)s
ORDER BY target_generation_id DESC, definition_id, started_at DESC
"""


# What this read will hand a browser out of a recorded blocker.
#
# An ALLOWLIST, not a denylist. The denylist that stood here named one key
# (`sample`) and a comment claimed the four writers carried nothing sensitive.
# Both were wrong. There is a fifth writer, and the comment below already
# named its table:
#
#   * publisher.py 7256-7273 builds a gate-exception summary whose
#     `details.error` is `str(exc)`, merged into `blocked_reason` by
#     `_merge_consistency_blocked_reason` (publisher.py 7282). A psycopg
#     failure there carries the host, the IP and the role name.
#   * consistency/transform_gate.py 1141-1175 and 1004-1030 put graph row ids
#     under `dangling_edge_ids`, `pruned_dangling_edge_ids`,
#     `orphan_retracted_derived_edge_ids`, `narrowing_violation_edge_ids`,
#     `node_subtype_violation_node_ids`, `cardinality_violation_edge_ids`,
#     `derived_node_cleanup.blocking_node_ids`, `blocking_declarations[].node_id`,
#     `unsupported_node_ids` and `retracted_node_ids`; derived_node_liveness.py
#     555 adds another.
#
# This read is authorized at the OPERATOR SURFACE, not per record, so a row id
# can name a record the reader's own policy would hide and an exception
# message can name infrastructure. Enumerating what to hide loses to the next
# writer; enumerating what may be shown does not.
#
# A known blocker shape is rebuilt from its allowlisted SCALAR fields and
# nothing else. Every other value becomes a marker naming the kind, the size
# and why it was withheld. An unknown top-level shape is withheld whole. The
# identities and the messages are served by `okg generation describe`, under a
# per-record authorization.

#: Reason codes for blocker shapes this reader knows how to serve.
_KNOWN_BLOCKER_SHAPES = frozenset({
    "consistency_publish_gate_blocked", "multiple_gate_failures",
    "checkpoint_attestation_failed", "nomos_generation_publish_denied",
    "fork_base_generation_unresolved", "fork_delta_scope_unavailable",
    "ingest_source_incomplete", "publish_blocked",
})

#: Scalar fields of a blocker that may be served, by key.
_BLOCKER_SCALARS = frozenset({
    "reason", "code", "attestor_id", "branch_name", "nomos_decision_id",
    "nomos_effect", "nomos_reason_code", "nomos_final_effect", "detail",
})

#: Scalar fields of a consistency-gate summary that may be served. The checks
#: section uses this same allowlist, so a gate cannot be safe in one place and
#: unsafe in the other.
_GATE_SCALARS = frozenset({
    "check_id", "gate_name", "result", "reason", "blocks_publish", "is_probe",
    "elapsed_ms", "budget_ms", "coverage_basis", "coverage_complete",
    "open_observation_count", "newly_opened_count", "resolved_count",
    "run_id", "target_generation_id", "gate_pass",
})

#: Key names this reader recognises. A key outside it is reported as
#: "unknown_key": echoing an unrecognised key name leaks the vocabulary of
#: whatever wrote it, which can itself name a source or a record type.
_KNOWN_KEY_NAMES = _BLOCKER_SCALARS | _GATE_SCALARS | frozenset({
    "failed_consistency_gates", "phase_b", "details", "preflight", "sample",
    "excluded_sources", "error_type", "error",
})

#: An allowlisted string must also LOOK like a code, and the shape has to be
#: narrow enough that a location cannot wear it. The first version of this
#: pattern allowed `/` and `@` and ran to 200 characters, which admits
#: `postgres://okg:pw@db.example:5432/okg` and `/Users/example/x` character for
#: character: a DSN or a path sitting on an allowlisted key would have been
#: served whole. So `/` and `@` are out of the general charset and the cap is
#: 80 -- longer than any reason code, gate name, uuid or coverage basis this
#: reader has seen, shorter than a useful path.
_CODE_SHAPED = re.compile(r"^[A-Za-z0-9_.:+-]{1,80}$")

#: Keys whose value legitimately holds a `/`. A branch name does; nothing else
#: this reader serves does.
_SLASH_KEYS = frozenset({"branch_name"})
_CODE_SHAPED_WITH_SLASH = re.compile(r"^[A-Za-z0-9_.:+/-]{1,80}$")

#: Refused on EVERY key, whatever its charset allows: a scheme, an absolute
#: POSIX path, a home-relative path, or a Windows drive letter. The charset
#: alone would let `C:/Users/example` through on a slash-bearing key.
_LOCATION_SHAPED = re.compile(r"://|^[/~]|^[A-Za-z]:[\\/]")


def _is_servable_string(key: str, value: str) -> bool:
    """Is this string safe to serve under this key?"""
    if _LOCATION_SHAPED.search(value):
        return False
    pattern = _CODE_SHAPED_WITH_SLASH if key in _SLASH_KEYS else _CODE_SHAPED
    return bool(pattern.match(value))


def _kind_and_size(value: Any) -> tuple[str, int]:
    """What a withheld value was, without any of what it said."""
    if isinstance(value, (list, tuple)):
        return "list", len(value)
    if isinstance(value, dict):
        return "object", len(value)
    if isinstance(value, str):
        return "string", len(value.encode("utf-8"))
    return type(value).__name__, 0


def _withheld(key: str, value: Any, reason: str) -> dict[str, Any]:
    kind, size = _kind_and_size(value)
    return {
        "key": key if key in _KNOWN_KEY_NAMES else "unknown_key",
        "kind": kind,
        "size": size,
        "withheld_reason": reason,
    }


def _scalar_or_none(key: str, value: Any) -> Any:
    """An allowlisted scalar, or None when it is not servable as written."""
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value if _is_servable_string(key, value) else None
    return None


def safe_gate_summary(gate: Any) -> dict[str, Any]:
    """One consistency-gate summary, rebuilt from its allowlisted scalars.

    `details` and `preflight` are always withheld: they carry the violating
    row ids and, on the exception path, `str(exc)`.
    """
    if not isinstance(gate, dict):
        return {"withheld": [_withheld("unknown_key", gate, "unknown_field")]}
    out: dict[str, Any] = {}
    withheld: list[dict[str, Any]] = []
    for key, value in gate.items():
        if key in _GATE_SCALARS:
            served = _scalar_or_none(key, value)
            if served is None and value is not None:
                withheld.append(_withheld(key, value, "not_code_shaped"))
            else:
                out[str(key)] = served
            continue
        withheld.append(_withheld(
            str(key), value,
            "exception_text" if key in ("details", "preflight", "error")
            else "graph_rows_not_policy_filtered",
        ))
    if withheld:
        out["withheld"] = withheld
    return out


def _safe_blocker(value: Any, depth: int = 0) -> Any:
    """A recorded blocker, rebuilt from what this reader knows is servable."""
    if not isinstance(value, dict):
        return _withheld("unknown_key", value, "unknown_field")
    reason = value.get("reason")
    if depth == 0 and (
        not isinstance(reason, str) or reason not in _KNOWN_BLOCKER_SHAPES
    ):
        # Fail closed and loud. An unrecognised shape is not passed through
        # field by field: its unknown fields are exactly the ones nobody has
        # checked.
        return {
            "reason": reason if isinstance(reason, str)
            and _is_servable_string("reason", reason) else None,
            "withheld_whole": True,
            "withheld_reason": "unknown_blocker_shape",
            "size": len(value),
        }
    out: dict[str, Any] = {}
    withheld: list[dict[str, Any]] = []
    for key, item in value.items():
        if key in _BLOCKER_SCALARS:
            served = _scalar_or_none(key, item)
            if served is None and item is not None:
                withheld.append(_withheld(key, item, "not_code_shaped"))
            else:
                out[str(key)] = served
        elif key == "failed_consistency_gates" and isinstance(item, (list, tuple)):
            out["failed_consistency_gates"] = [safe_gate_summary(g) for g in item]
        elif key == "phase_b":
            out["phase_b"] = _safe_blocker(item, depth + 1)
        else:
            withheld.append(_withheld(
                str(key), item,
                "exception_text" if key in ("details", "error") else "unknown_field",
            ))
    if withheld:
        out["withheld"] = withheld
    return out

# A view run's `error` column is free-form jsonb and the writers put whatever
# the failure carried into it: sql_gate.py 3505-3529 and ir_executor.py 318
# both write `str(exc)` and a traceback, which name filesystem paths and can
# quote SQL and row values. Only the `artifact_kind = 'view'` filter keeps
# those particular writers out of this read today, and nothing enforces that
# they stay out.
#
# So the error is not served whole. What an operator needs to act is the kind
# of failure; the message and the traceback are withheld with a marker and the
# byte count, and `okg generation describe` serves them under a per-record
# authorization.
_ERROR_KEPT_KEYS = ("type", "error_type", "code", "reason", "status", "kind")

#: Key names this reader recognises INSIDE a run's error blob. `message` and
#: `traceback` are the error writers' own fixed vocabulary and are named so
#: the page can say which field it is not showing. Anything else is collapsed
#: to "unknown_key" by the same rule `_withheld` follows: echoing an
#: unrecognised key name leaks the vocabulary of whatever wrote it, which can
#: itself name a source or a record type.
_ERROR_KNOWN_KEY_NAMES = frozenset(_ERROR_KEPT_KEYS) | frozenset({
    "message", "traceback", "details", "detail", "args", "sample",
})

#: The one reason this composer gives for withholding a run error's content.
#: It is served under `withheld_reason`, NOT under `reason`: `reason` is one
#: of the kept scalars and holds the failure's own code, and overwriting it
#: with this sentence would state a reason the run never recorded.
_ERROR_WITHHELD_REASON = "error_may_carry_message_or_traceback"


def _safe_view_run_error(error: Any) -> Any:
    """The kind of failure, without its message or its traceback.

    Every KEPT scalar goes through `_scalar_or_none`, the same shape filter
    the gate and blocker scalars use. Being on the keep list is not a promise
    about what a writer put there: psycopg's own connection failure writes
    `could not connect to server at /tmp/.s.PGSQL.5432` into `reason`, which
    is a filesystem path on an allowlisted key. A scalar that fails the
    filter is not served at all; it falls into the withheld set and is named
    there.
    """
    if error is None:
        return None
    if not isinstance(error, dict):
        text = str(error)
        return {
            "error_text_withheld": True,
            "bytes": len(text.encode("utf-8")),
            "withheld_reason": _ERROR_WITHHELD_REASON,
        }
    kept: dict[str, Any] = {}
    for key in _ERROR_KEPT_KEYS:
        if key not in error:
            continue
        value = error[key]
        if not isinstance(value, (str, int, bool)):
            continue
        served = _scalar_or_none(key, value)
        if served is None and value is not None:
            continue
        kept[key] = _jsonable(served)
    withheld = {key: error[key] for key in error if key not in kept}
    if withheld:
        try:
            size = len(json.dumps(withheld, default=str).encode("utf-8"))
        except (TypeError, ValueError):
            size = None
        kept["error_text_withheld"] = True
        kept["withheld_keys"] = sorted(
            str(key) if str(key) in _ERROR_KNOWN_KEY_NAMES else "unknown_key"
            for key in withheld
        )
        kept["bytes"] = size
        kept["withheld_reason"] = _ERROR_WITHHELD_REASON
    return kept


#: How many producer runs this read serves FOR ONE ATTEMPT. The runs table is
#: unbounded per generation, so each attempt's list is capped on its own and
#: the payload states its true total beside it.
_VIEW_RUNS_PER_ATTEMPT = 20


def _jsonable(value: Any) -> Any:
    """A payload-safe copy of a recorded value, with nothing edited out."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _view_runs_by_target(
    conn: Any,
    *,
    deployment_name: str,
    targets: list[int],
) -> dict[int, dict[str, Any]]:
    """The producer runs recorded for these attempts, grouped by attempt.

    Each entry is {"runs": [...], "runs_total": N, "runs_capped": bool}:
    the attempt's own total, so a capped list never reads as the whole of it
    and an attempt with no row never reads as an attempt that was starved by
    another attempt's rows.
    """
    grouped: dict[int, dict[str, Any]] = {}
    if not targets:
        return grouped
    with conn.cursor() as cur:
        cur.execute(
            _VIEW_RUNS_SQL,
            {
                "deployment": deployment_name,
                "targets": targets,
                "per_attempt": _VIEW_RUNS_PER_ATTEMPT,
            },
        )
        columns = [description.name for description in cur.description]
        for raw in cur.fetchall():
            row = dict(zip(columns, raw))
            target = int(row["target_generation_id"])
            total = int(row["runs_total"])
            entry = grouped.setdefault(target, {
                "runs": [],
                "runs_total": total,
                "runs_capped": total > _VIEW_RUNS_PER_ATTEMPT,
            })
            entry["runs"].append({
                "definition_id": str(row["definition_id"]),
                # Named, not assumed: a deployment's only producer may be a
                # consistency_check rather than a view.
                "artifact_kind": str(row["artifact_kind"]),
                "run_id": str(row["run_id"]),
                "status": str(row["status"]),
                "trigger_kind": str(row["trigger_kind"]),
                # Only the KIND of failure. The writers put str(exc) and
                # tracebacks in this column.
                "error": _safe_view_run_error(_jsonable(row["error"])),
                "started_at": _jsonable(row["started_at"]),
                "completed_at": _jsonable(row["completed_at"]),
            })
    return grouped


def _view_runs_fields(
    grouped: dict[int, dict[str, Any]],
    target: Any,
) -> dict[str, Any]:
    """One attempt's runs fields, and whether they could be looked up at all.

    "Could not look" is not "looked and found none". The runs table is joined
    on the `target_generation_id` the publisher wrote into the checkpoint; an
    attempt whose metadata records no such value, or records something that
    is not an integer, cannot be joined to it. Serving that as an empty list
    would say the deployment recorded no producer run for the attempt, which
    is a claim nothing measured -- so it carries a named marker instead, and
    the page prints it as one.
    """
    if isinstance(target, int) and not isinstance(target, bool):
        entry = grouped.get(int(target))
        if entry is not None:
            return {
                "view_runs": entry["runs"],
                "view_runs_total": entry["runs_total"],
                "view_runs_capped": entry["runs_capped"],
                "view_runs_unavailable": None,
            }
        # Looked, and this attempt has no row. A measured zero.
        return {
            "view_runs": [], "view_runs_total": 0, "view_runs_capped": False,
            "view_runs_unavailable": None,
        }
    return {
        "view_runs": [], "view_runs_total": 0, "view_runs_capped": False,
        "view_runs_unavailable": {"reason": "target_generation_id_not_recorded"},
    }


def _excluded_source_names(excluded_sources: Any) -> list[str]:
    """The sources the gate recorded as excluded, as the list it recorded.

    A list, not a chosen one: `metadata["excluded_sources"]` can name several,
    and collapsing it to a single "stopped on" producer would put a name on the
    page that the record does not single out. `stopped_producer` therefore
    stays null (nothing records it) and the view names every entry here.
    """
    if isinstance(excluded_sources, (list, tuple)):
        return [str(name) for name in excluded_sources]
    if isinstance(excluded_sources, dict):
        return [str(name) for name in excluded_sources]
    return []


# How much recorded blocker this read will hand a browser.
#
# The four writers of `blocked_reason` emit structured, identifier-shaped
# values -- a reason code, a Nomos decision id and effect, a branch name,
# counts, an attestor id and its refusal code (publisher.py 10774, 10884,
# 11945, 12137). None of them carries a DSN, a filesystem path or a token: the
# attestation runner records only `code="attestor_exception"` for an
# unexpected exception and never `str(exc)` (checkpoint_attestations.py
# 927-931), and `dsn`, `database`, `connection` and `conn` are already
# reserved against attestor config. So nothing here is masked -- masking a
# field that cannot hold a secret would only hide the operator's own evidence.
#
# One field IS unbounded: `failed_consistency_gates` carries per-gate
# summaries, and a gate that fired can attach sample rows. A blocker that runs
# to megabytes must not be pushed into a page, so the size is capped HERE,
# where the size is known, rather than clipped in a view that would then be
# claiming to print a record verbatim while showing part of it.
_BLOCKED_DETAIL_MAX_BYTES = 8192


def _blocked_detail(blocked_reason: Any) -> tuple[Any, dict[str, Any] | None]:
    """The recorded blocker, unflattened, and what was withheld if any.

    `blocked_reason` beside it stays exactly what it has always been: a
    one-line, 200-character summary for a list. This is the record itself, for
    the detail page's verbatim block. They are not the same string under two
    names -- one is an edited summary, this is what the run wrote.

    Returns (detail, oversize). Over the cap the detail is withheld ENTIRELY
    rather than cut mid-record: half a JSON document printed as "the recorded
    blocker" is worse than a line saying how large it is and where to read it.
    """
    if not blocked_reason:
        return None, None
    detail = _safe_blocker(_jsonable(blocked_reason))
    # No `except` around this dump. `default=str` makes it total, so the
    # unserialisable branch that used to sit here could never run; a dead
    # branch claiming to report a failure mode is worse than no branch. If a
    # future value does defeat it, the read fails loudly rather than
    # pretending the record was absent.
    size = len(json.dumps(detail, default=str).encode("utf-8"))
    if size <= _BLOCKED_DETAIL_MAX_BYTES:
        return detail, None
    return None, {
        "reason": "blocked_detail_too_large",
        "bytes": size,
        "limit": _BLOCKED_DETAIL_MAX_BYTES,
        "keys": sorted(detail) if isinstance(detail, dict) else [],
    }


#: Why `blocked_reason` is null on an attempt that WAS blocked: what it
#: recorded is not code-shaped, so it may be a message, a path or exception
#: text, and a list row is not the place to serve it.
BLOCKED_REASON_NOT_CODE_SHAPED = "blocked_reason_not_code_shaped"


def _blocked_reason_code(blocked_reason: Any) -> tuple[str | None, str | None]:
    """The recorded blocker's reason CODE, or null and why.

    Returns ``(code, unavailable_reason)``. Only the ``reason`` or ``code`` of
    a recorded blocker is served, and only when it is code-shaped: the old
    one-line summary fell back to the blocker's ``message``, ``detail`` or
    ``error`` and then to the whole object, which served exception text and
    paths the publisher recorded (#2130). ``(None, None)`` means not blocked.
    """
    if not blocked_reason:
        return None, None
    candidates: list[Any]
    if isinstance(blocked_reason, str):
        candidates = [blocked_reason]
    elif isinstance(blocked_reason, dict):
        candidates = [blocked_reason.get("reason"), blocked_reason.get("code")]
    else:
        candidates = []
    for value in candidates:
        if isinstance(value, str) and _is_servable_string("reason", value.strip()):
            return value.strip(), None
    return None, BLOCKED_REASON_NOT_CODE_SHAPED


def _blocked_summary(blocked_reason: Any) -> str | None:
    """The recorded blocker's reason code, or None when there is none to serve.

    Callers that serve this field serve `_blocked_reason_code`'s second value
    beside it, so a null here on a blocked attempt says why.
    """
    return _blocked_reason_code(blocked_reason)[0]


def _excluded_count(excluded_sources: Any) -> int:
    if isinstance(excluded_sources, (list, tuple)):
        return len(excluded_sources)
    if isinstance(excluded_sources, dict):
        return len(excluded_sources)
    return 0


def _parse_created_at(value: Any) -> datetime | None:
    """The checkpoint's created_at as an AWARE UTC datetime, or None.

    The one parser, shared with the attempts composer: a naive stamp is read as
    UTC (every publisher writes UTC) and a stamp in another zone is converted,
    so `created_at` in this payload means one thing whatever wrote the row. Two
    parsers with different rules is how the same checkpoint ends up on two
    different days in two panels of the same page.
    """
    return parse_checkpoint_created_at(value)


def _display_generation_id(checkpoint: Any) -> int | str:
    metadata = checkpoint.metadata or {}
    return metadata.get("target_generation_id") or checkpoint.checkpoint_id


#: Why a delta could not be measured because the read never saw the attempt
#: before this one. Not "this is the first generation": the reader is bounded,
#: so the oldest row it returned may simply be the oldest row it LOOKED at.
#:
#: A SENTENCE, not a code, because the console prints this field verbatim
#: beside the place the number would have gone (`changePhrase` in
#: `console/model/shared/change.ts`), and every other value the field carries
#: is a sentence too -- they come from `GenerationCountsUnavailable.message`.
NO_PREDECESSOR_IN_WINDOW = (
    "the attempt before this one is older than the window this read scanned"
)


def compose_attempt_row(
    generation: Any,
    *,
    session: Session,
    view_runs: dict[int, dict[str, Any]],
    previous_generation: Any | None,
    counts_or_reason: Any,
    previous_unavailable_reason: str | None = None,
) -> dict[str, Any]:
    """One attempt, in the shape the list serves.

    Extracted so the attempt DETAIL read serves the identical row rather than
    a second, drifting copy of it: a detail page whose headline disagreed with
    the row the operator clicked would be a defect nothing could catch. Both
    callers pass the same three inputs -- the checkpoint, the attempt before it
    (None when this is the first), and a `counts_or_reason` callable returning
    ``((nodes, edges) | None, reason | None)`` for a checkpoint.

    `previous_unavailable_reason` is what makes `previous_generation=None`
    READABLE. It means two different things, and the difference is the whole
    delta: the attempt really is the first this branch ever recorded, in which
    case its delta is measured against nothing and the `(0, 0)` baseline is the
    fact; or the reader simply did not scan far enough back, in which case
    there IS an earlier attempt and `(0, 0)` is a number nobody measured,
    printed in the place a measurement goes. The caller knows which -- it is
    the one holding `window_exhausted` -- so it says, and a row whose
    predecessor was merely out of reach carries `delta_available: False` with
    this reason instead of a fabricated first publish.
    """
    metadata = generation.metadata or {}
    stamp = _parse_created_at(generation.created_at)
    blocked_detail, blocked_detail_oversize = _blocked_detail(
        metadata.get("blocked_reason"),
    )
    row: dict[str, Any] = {
        "generation_id": _display_generation_id(generation),
        # The checkpoint's OWN id, beside the compat id above. The console
        # copies it and describes a generation by it; the compat integer
        # names no generation to `okg generation describe`. Existing
        # readers keep reading `generation_id` unchanged.
        "full_generation_id": generation.checkpoint_id,
        "status": str(metadata.get("status") or "unknown"),
        "age": _humanize_age(stamp) if stamp else None,
        # The stamp `age` was humanised from, so a client can order
        # attempts and compute its own spans instead of parsing "10h ago".
        # Null when the checkpoint's created_at column could not be read.
        "created_at": stamp.isoformat() if stamp else None,
        # Not recorded per attempt on any checkpoint: no column holds a
        # publish's wall clock, the stage it stopped in, the producer it
        # stopped on, or the run's own error text. Null says "not
        # recorded"; a zero or an empty string would read as a measured
        # fact. `blocked_reason` below is what IS recorded about a failure.
        "duration_ms": None,
        "stopped_stage": None,
        "stopped_producer": None,
        "error_text": None,
        "is_current": generation.checkpoint_id == session.generation_id,
        "blocked_reason": _blocked_summary(metadata.get("blocked_reason")),
        # Why `blocked_reason` is null on a blocked attempt; null otherwise.
        "blocked_reason_unavailable_reason": (
            _blocked_reason_code(metadata.get("blocked_reason"))[1]
        ),
        # The recorded blocker itself, for the detail page's verbatim
        # block. `blocked_reason` above stays the one-line summary a list
        # can hold; this is what the run wrote.
        "blocked_detail": blocked_detail,
        # Present only when the record was withheld, with its size and its
        # top-level keys, so the page can say what it is not showing.
        "blocked_detail_oversize": blocked_detail_oversize,
        "excluded_source_count": _excluded_count(
            metadata.get("excluded_sources"),
        ),
        # The gate's exclusions as the LIST the checkpoint recorded, on
        # every attempt whatever its outcome: a published attempt can
        # exclude a source too, so this is not "what it stopped on".
        "excluded_sources": _excluded_source_names(
            metadata.get("excluded_sources"),
        ),
        # One row per VIEW producer recorded against this attempt's own
        # target generation, or [] when none is recorded. Never a
        # fabricated zero: an empty list means the deployment recorded no
        # per-producer result for this attempt, and the console says so in
        # those words. Per-SOURCE results are not served at all -- nothing
        # records a source run against a generation.
        # This attempt's own runs, its true total, and whether its own
        # list was capped -- never another attempt's.
        **_view_runs_fields(view_runs, metadata.get("target_generation_id")),
    }

    current, current_reason = counts_or_reason(generation)
    if previous_generation is None:
        # An empty baseline is a MEASUREMENT only for the first attempt this
        # branch recorded. Where the caller says the predecessor sits outside
        # the window it read, there is no baseline at all and the row says so.
        previous: tuple[int, int] | None = (
            None if previous_unavailable_reason else (0, 0)
        )
        previous_reason: str | None = previous_unavailable_reason
    else:
        previous, previous_reason = counts_or_reason(previous_generation)

    if current is None or previous is None:
        # A generation whose stamped counts are unavailable (blocked /
        # pre-stamp / compacted), or one whose previous generation's counts
        # are, yields an UNKNOWN delta carrying the reason — never a
        # fabricated zero (which zero_delta would misread as a silent
        # upstream failure) and never a 500 for the whole panel.
        row.update({
            "delta_available": False,
            "delta_unavailable_reason": current_reason or previous_reason,
            "nodes_added": None,
            "nodes_retracted": None,
            "edges_added": None,
            "edges_retracted": None,
            "zero_delta": False,
        })
    else:
        current_nodes, current_edges = current
        previous_nodes, previous_edges = previous
        node_delta = current_nodes - previous_nodes
        edge_delta = current_edges - previous_edges
        row.update({
            "delta_available": True,
            "delta_unavailable_reason": None,
            "nodes_added": max(node_delta, 0),
            "nodes_retracted": max(-node_delta, 0),
            "edges_added": max(edge_delta, 0),
            "edges_retracted": max(-edge_delta, 0),
            # zero_delta flags a publish that changed nothing — often
            # the tell for a silent upstream failure.
            "zero_delta": node_delta == 0 and edge_delta == 0,
        })
    return row


def compose_generations(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    limit: int = 12,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Recent publish history with per-generation deltas + status.

    `cursor` is the checkpoint id of the OLDEST attempt the caller already
    has -- the `next_cursor` of its previous page -- and this call answers with
    the attempts immediately older than it. It is a position in the list this
    composer has already loaded, so it costs one index lookup and no extra
    query; it is anchored on a checkpoint id rather than an offset, so an
    attempt published between two page reads shifts no row and repeats none.
    The read IS bounded. It used to call `store.list_generations`, which has
    no LIMIT and no projection and so moved every checkpoint's full metadata
    document -- 88 rows and 131 MB, 1.65 s, for a twelve-row panel on
    okg-workspace (issues #2113, #2202). It now reads a bounded window of the
    newest checkpoints through `store.recent_generations`, projecting only the
    metadata keys `compose_attempt_row` reads.

    Two consequences the payload states rather than hides:

    * `total` is counted separately, by generation-checkpoint id prefix, so a
      bounded row read still serves an exact history length. `total_basis`
      names that basis, and is `"scan_window"` when the backend cannot answer
      the count -- in which case `total` is what the window saw, no more.
    * a `cursor` page needs the window to reach past the cursor, so a cursor
      read opens the window to the reader's own cap. `window_exhausted` says
      when even that did not reach the end of the history, which is also when
      `next_cursor` may name a page this read could not confirm exists.
    """
    capped = max(1, min(int(limit), _MAX_LIMIT))
    require_console_count_rollup_enabled(generation_id=session.generation_id)
    # The database guard each per-row count read used to apply, applied once:
    # counts now come from the rows this read fetches on `conn`.
    assert_session_matches(conn, session)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    # Page one needs `capped` rows plus the one older attempt each delta is
    # measured against; a cursor page has to reach the cursor first, and where
    # it sits is not known before the read, so it opens the window to the cap.
    scan_window = None if cursor is not None else max(32, (capped + 1) * 2)
    recent = store.recent_generations(
        deployment_name=deployment_name,
        branch=session.branch_name,
        status=None,
        # The count keys ride along, so each row's delta is read from the row
        # already in hand instead of a second fetch per checkpoint (#2546).
        keys=tuple(dict.fromkeys(
            ATTEMPT_ROW_METADATA_KEYS + GENERATION_COUNT_METADATA_KEYS
        )),
        scan_window=scan_window,
    )
    # The reader answers newest first; every index below reads the history
    # ascending, as `list_generations` returned it.
    all_generations = list(reversed(recent.rows))
    total = store.count_generation_checkpoints(
        deployment_name=deployment_name, branch=session.branch_name,
    )
    total_basis = "checkpoint_id_prefix"
    if total is None:
        total = len(all_generations)
        total_basis = "scan_window"
    previous_by_id: dict[str, Any | None] = {}
    previous = None
    for generation in all_generations:
        previous_by_id[generation.checkpoint_id] = previous
        previous = generation

    count_cache: dict[str, tuple[int, int]] = {}
    # Counts are required stamped checkpoint authority. A missing/non-published
    # stamp fails loud through GenerationCountsUnavailable rather than taking the
    # former per-generation raw count(*) checkout. Mirroring the guard in
    # console_views/status.py, this panel CATCHES that per-generation exception —
    # but, being a historical list rather than a single live number, it degrades
    # the offending row to a typed unknown-delta carrying the reason instead of
    # recomputing (a raw checkout per historical generation is exactly the O(N)
    # scan the count-rollup cutover removed). #687: never a 500, never a silent 0.

    def _counts(generation: Any) -> tuple[int, int]:
        cached = count_cache.get(generation.checkpoint_id)
        if cached is not None:
            return cached
        # THIS generation's own counts, from the projected row the bounded
        # read already returned (the previous-gen read needs the previous
        # gen's own counts). The guards and per-row reasons are
        # `generation_counts`' own; it used to re-fetch each checkpoint's
        # whole metadata document to apply them -- up to 62 MB a row on
        # okg-workspace (issue #2546).
        counts = counts_from_projected_metadata(
            conn, generation.checkpoint_id, dict(generation.metadata or {}),
        )
        count_cache[generation.checkpoint_id] = counts
        return counts

    def _counts_or_reason(
        generation: Any,
    ) -> tuple[tuple[int, int] | None, str | None]:
        try:
            return _counts(generation), None
        except GenerationCountsUnavailable as exc:
            return None, exc.message

    # The window this page covers, in the ascending list. Page one ends at the
    # newest attempt; a cursor names the oldest attempt of the page before, so
    # this page ends where that one began.
    end = len(all_generations)
    if cursor is not None:
        index = next(
            (
                position
                for position, generation in enumerate(all_generations)
                if generation.checkpoint_id == cursor
            ),
            None,
        )
        if index is None:
            raise UnknownGenerationCursor(cursor)
        end = index
    start = max(0, end - capped)
    page = all_generations[start:end]

    view_runs = _view_runs_by_target(
        conn,
        deployment_name=deployment_name,
        targets=sorted({
            int(target)
            for target in (
                (generation.metadata or {}).get("target_generation_id")
                for generation in page
            )
            if isinstance(target, int) and not isinstance(target, bool)
        }),
    )

    items: list[dict[str, Any]] = []
    for generation in reversed(page):
        items.append(compose_attempt_row(
            generation,
            session=session,
            view_runs=view_runs,
            previous_generation=previous_by_id.get(generation.checkpoint_id),
            counts_or_reason=_counts_or_reason,
            # Only the OLDEST row of the scanned window has no predecessor in
            # hand, and only an exhausted window leaves the question open: a
            # window that reached the beginning of the history proves that row
            # is the first attempt, so its empty baseline is the fact rather
            # than a gap.
            previous_unavailable_reason=(
                NO_PREDECESSOR_IN_WINDOW
                if (
                    previous_by_id.get(generation.checkpoint_id) is None
                    and recent.window_exhausted
                )
                else None
            ),
        ))
    return {
        "deployment": deployment_name,
        "current_generation_id": (
            session.compat_generation_id
            if session.compat_generation_id is not None
            else session.generation_id
        ),
        "shown": len(items),
        # How many generations EXIST, against `shown`, how many this capped
        # read returned. Without it a client cannot tell a complete history
        # from the newest page of a long one. Counted by checkpoint-id prefix,
        # which costs no metadata parse -- `total_basis` names the basis so a
        # backend that cannot answer it says "scan_window" instead of letting
        # a windowed count pass as the history's length.
        "total": total,
        "total_basis": total_basis,
        # What the bounded row read looked at. `next_cursor` below is a
        # position inside this window; when the window was exhausted there may
        # be older attempts this read never saw.
        "scanned": recent.scanned,
        "scan_window": recent.scan_window,
        "window_exhausted": recent.window_exhausted,
        # Where the next page begins: the checkpoint id of the OLDEST attempt
        # on this page, when older attempts exist. Null when this page reached
        # the beginning of the history, so a client offers no "load more" that
        # can only return nothing.
        # `start > 0` means older attempts sit inside this window; an
        # EXHAUSTED window means older attempts may sit beyond it, and
        # dropping the cursor there would hide the rest of the history behind
        # a "load more" the console never draws.
        "next_cursor": (
            all_generations[start].checkpoint_id
            if page and (start > 0 or recent.window_exhausted)
            else None
        ),
        "generations": items,
    }
