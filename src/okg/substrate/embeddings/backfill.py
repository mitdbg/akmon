"""Vector-set backfill, dry run and document preview (PACT openrouter-embeddings-hnsw).

``okg db embed-backfill --set <name>`` fills one registered, active vector set
(design v3.2, "Commands and cost"):

* **Pin.** It reads the visible node rows of the latest published generation
  (the pin a search would read), never the unpublished head.
* **Documents.** Each covered text node's document is rendered from the
  set's REGISTERED row (``okg.embedding_sets``, what the graph writer uses),
  not from deployment.yaml; a difference between the two is reported as
  config drift (``okg migrate`` applies the file).
* **Links.** A node whose current content has no link to its document under
  the current template gets one (``okg.node_documents``). This is what makes
  a template change take effect: a template change writes no node rows, so
  the graph writer never links the new documents.
* **Vectors.** Every DISTINCT document of the set with no stored vector is
  embedded once and inserted into ``okg.text_embeddings`` (insert-only;
  ``seq`` from the set's counter). A document already embedded (the same
  text elsewhere, or an edit reverted) is reused and costs nothing.
* **No node rows.** The backfill writes no ``okg.graph_nodes`` row: the
  keyword index and the node history are untouched. The next publish
  records a watermark that makes the new vectors and links readable.
* **One run per set.** A per-set advisory lock is taken before anything is
  read or spent; a second run of the same set refuses with
  ``embedding_backfill_in_progress``.
* **Spend cap.** The ledger (``okg.llm_calls``, caller
  ``embeddings.backfill``) is keyed by set (``extras.set_id``). The earlier
  spend is read under the set's lock, so no other backfill of the set can
  spend between that read and this run's checks. The cap is checked before
  the backend is built (its one capability-probe call is a paid call), again
  after the probe, and before every embedding request. A request is sent
  only while the spend so far plus its characters at the highest price per
  character measured (this run's calls, or this database's earlier calls of
  the model) stays at or under the cap; a batch is cut to fit. The spend
  passes the cap only when a request costs more than that estimate (the
  probe, before any price is known, or a batch priced above the measured
  rate). A backend that reports no cost cannot be capped; the run refuses a
  cap it cannot enforce.
* **Call records.** Every answered request writes one ledger row with tokens,
  cost, provider and set id, including one whose response failed
  validation; a row that cannot be written fails the run.

``--dry-run`` makes no paid call and no write: per set and per subtype it
counts distinct missing documents and their characters, and estimates
tokens and dollars only from what this database measured (earlier priced
backfill calls of the same model). With no measurement it says so.

``okg db embed-show`` prints the exact document a node's current content
renders to under the registered set, its hash, and whether a vector exists.

The DBOS embed worker still refuses paid models: this command is the only
path that embeds documents with a paid backend.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Optional, Sequence

from okg.substrate.errors import SubstrateError


__all__ = [
    "ASSUMED_CHARS_PER_TOKEN",
    "BACKFILL_CALLER",
    "backfill_lock_key",
    "build_set_plan",
    "dry_run_backfill",
    "resolve_registered_set",
    "run_set_backfill",
    "show_node_document",
]

log = logging.getLogger(__name__)

BACKFILL_CALLER = "embeddings.backfill"
DEFAULT_WINDOW_ROWS = 512
DEFAULT_MAX_WINDOW_CHARS = 1_000_000
#: Used only when this database has no measured characters-per-token for
#: the model, and always named as an assumption in the dry-run report.
ASSUMED_CHARS_PER_TOKEN = 4


def _error(message: str, *, code: str, data: dict[str, Any], fix_hint: str) -> SubstrateError:
    err = SubstrateError(message, data=data, fix_hint=fix_hint)
    err.code = code
    return err


# --------------------------------------------------------------------------- #
# Which set, and at which pin                                                  #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ResolvedSet:
    """A configured set and its registered row."""

    configured: Any  # VectorSetConfig from deployment.yaml
    stored: Any  # VectorSetConfig as registered: what documents are built from
    drift: tuple[str, ...]
    active: bool


def _configured_set(config: Any, set_name: Optional[str]) -> Any:
    from okg.substrate.embeddings.config import EmbeddingConfigError

    sets = tuple(getattr(config, "vector_sets", ()) or ())
    if not sets:
        raise _error(
            "embeddings are disabled for this deployment",
            code="semantic_embedder_not_configured",
            data={},
            fix_hint="enable `embeddings:` with a set in deployment.yaml",
        )
    if set_name is None:
        try:
            return config.search_vector_set
        except EmbeddingConfigError as exc:
            raise _error(
                str(exc), code="embedding_set_not_configured",
                data={"set": config.search_set, "configured": [s.name for s in sets]},
                fix_hint="name a configured set with --set",
            ) from exc
    for item in sets:
        if item.name == set_name:
            return item
    raise _error(
        f"vector set {set_name!r} is not configured in deployment.yaml",
        code="embedding_set_not_configured",
        data={"set": set_name, "configured": sorted(s.name for s in sets)},
        fix_hint="name a set from deployment.yaml's embeddings.sets",
    )


def resolve_registered_set(
    conn: Any, config: Any, set_name: Optional[str], *, require_active: bool = True,
) -> ResolvedSet:
    """The configured set and its registered row, or a refusal.

    Refuses a set that ``okg migrate`` has not registered
    (``embedding_set_unregistered``) and, with ``require_active``, one that
    is inactive or whose stored identity no longer hashes to its id under
    this code (``embedding_set_inactive``).
    """
    from okg.substrate.embeddings.store import config_drift, load_set

    configured = _configured_set(config, set_name)
    loaded = load_set(conn, configured.set_id)
    if loaded is None:
        raise _error(
            f"vector set {configured.name!r} is not registered in this database",
            code="embedding_set_unregistered",
            data={"set": configured.name, "set_id": configured.set_id},
            fix_hint="run `okg migrate --deployment <name> --apply`",
        )
    stored, active = loaded
    stale = stored.set_id != configured.set_id
    if require_active and (not active or stale):
        raise _error(
            f"vector set {configured.name!r} is registered but "
            + ("stale under this code" if stale else "inactive"),
            code="embedding_set_inactive",
            data={
                "set": configured.name,
                "set_id": configured.set_id,
                "reason": "stale" if stale else "inactive",
            },
            fix_hint=(
                "run `okg migrate --deployment <name> --apply` with the set in "
                "deployment.yaml"
            ),
        )
    return ResolvedSet(
        configured, stored, tuple(config_drift(configured, stored)), bool(active),
    )


@dataclass(frozen=True)
class _Pin:
    generation_id: str
    catalog_version_id: Any
    branch_point: int
    physical_table: str


def _latest_pin(conn: Any, deployment: str) -> _Pin:
    """The latest published generation: the pin a search would read."""
    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.mcp.session import open_session
    from okg.substrate.semantic_ann import _node_segment

    session = open_session(conn, deployment_name=deployment)
    return _Pin(
        generation_id=str(session.generation_id),
        catalog_version_id=getattr(session, "catalog_version_id", None),
        branch_point=int(branch_point(conn, session)),
        physical_table=_node_segment(conn),
    )


def _visible_windows(
    conn: Any, pin: _Pin, vector_set: Any, *, window_rows: int,
) -> Iterator[list[tuple[str, str, dict, str]]]:
    """Visible text rows at the pin, in ``node_id`` windows:
    ``(node_id, subtype, attrs, row_hash)``. ``row_hash`` is computed by
    the same SQL expression the reader uses."""
    from psycopg.rows import tuple_row

    from okg.substrate.embeddings.store import row_hash_expression
    from okg.substrate.graph_store_internal.common import quote_physical_table

    filters = [
        "n.deleted = FALSE",
        "n.live_lo <= %(bp)s",
        "%(bp)s < n.live_hi",
        "n.attrs ? 'text'",
        "n.node_id > %(after)s",
    ]
    params: dict[str, Any] = {"bp": pin.branch_point, "window": int(window_rows)}
    if vector_set.subtypes is not None:
        filters.append("n.subtype = ANY(%(subtypes)s)")
        params["subtypes"] = list(vector_set.subtypes)
    statement = (
        f"SELECT n.node_id, n.subtype, n.attrs, {row_hash_expression('n')} "
        f"  FROM {quote_physical_table(pin.physical_table)} AS n "
        f" WHERE {' AND '.join(filters)} "
        " ORDER BY n.node_id LIMIT %(window)s"
    )
    after = ""
    while True:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(statement, {**params, "after": after}, prepare=False)
            rows = [
                (str(r[0]), str(r[1]), dict(r[2] or {}), str(r[3]))
                for r in cur.fetchall()
            ]
        if not rows:
            return
        after = rows[-1][0]
        yield rows


class _Renderer:
    """Renders documents for one set at one pin, caching each subtype's
    read classification (from the pin's catalog, as the reader does)."""

    def __init__(self, conn: Any, vector_set: Any, pin: _Pin) -> None:
        self.conn = conn
        self.vector_set = vector_set
        self.pin = pin
        self._classes: dict[str, Any] = {}

    def render(self, subtype: str, attrs: dict) -> Any:
        from okg.substrate.embeddings.documents import render_document
        from okg.substrate.embeddings.store import load_classifications

        if subtype not in self._classes:
            self._classes.update(load_classifications(
                self.conn, [subtype], catalog_version_id=self.pin.catalog_version_id,
            ))
        return render_document(
            self.vector_set, subtype=subtype, attrs=attrs,
            classification=self._classes.get(subtype),
        )


@dataclass
class _Window:
    nodes_by_subtype: dict[str, int]
    links: list[tuple[str, str, str, str]]
    #: doc_hash -> (document text, subtype of the first node rendering it)
    documents: dict[str, tuple[str, str]]
    omitted_fields: int
    #: template_hash -> template record, stored with the links
    templates: dict[str, Any] = field(default_factory=dict)


def _render_window(renderer: _Renderer, rows: Sequence[tuple[str, str, dict, str]]) -> _Window:
    window = _Window({}, [], {}, 0)
    for node_id, subtype, attrs, row_hash in rows:
        rendered = renderer.render(subtype, attrs)
        if rendered is None:
            continue
        window.nodes_by_subtype[subtype] = window.nodes_by_subtype.get(subtype, 0) + 1
        window.omitted_fields += len(rendered.omitted_fields)
        window.links.append((rendered.template_hash, node_id, row_hash, rendered.doc_hash))
        window.templates.setdefault(rendered.template_hash, rendered.template_record)
        window.documents.setdefault(rendered.doc_hash, (rendered.text, subtype))
    return window


# --------------------------------------------------------------------------- #
# Spend ledger (keyed by set)                                                  #
# --------------------------------------------------------------------------- #

def _prior_spend(conn: Any, *, deployment: str, set_id: str, model_id: str) -> tuple[float, str]:
    """USD that earlier backfill runs of this deployment's set spent.

    Returns ``(usd, basis)``: ``"no_prior_calls"`` when the ledger holds no
    row for the set (zero, stated rather than coalesced), else ``"ledger"``.
    An answered request with no cost makes the earlier spend unknown, so the
    cap cannot be enforced and this refuses instead of counting it as $0.
    """
    calls, spent, answered_uncosted = conn.execute(
        "SELECT count(*), sum(cost_usd), "
        "       count(*) FILTER (WHERE cost_usd IS NULL AND "
        "         (success OR error_kind = 'embedder_output_invalid')) "
        "  FROM okg.llm_calls "
        " WHERE caller = %s AND deployment_name = %s "
        "   AND extras->>'set_id' = %s",
        (BACKFILL_CALLER, deployment, set_id),
    ).fetchone()
    if int(answered_uncosted):
        raise _error(
            f"{answered_uncosted} earlier backfill call(s) for set {set_id!r} were "
            "answered without a reported cost, so the prior spend is unknown "
            "and the spend cap cannot be enforced",
            code="embedding_cost_unreported",
            data={
                "deployment": deployment,
                "set_id": set_id,
                "model_id": model_id,
                "answered_uncosted_calls": int(answered_uncosted),
            },
            fix_hint=(
                "check the provider bill for those okg.llm_calls rows "
                "(caller 'embeddings.backfill') before running again"
            ),
        )
    if int(calls) == 0:
        return 0.0, "no_prior_calls"
    return float(spent or 0.0), "ledger"


def _measured_rates(conn: Any, model_id: str) -> dict[str, Any]:
    """What this database measured for the model in earlier priced backfill
    calls (any set), or Nones: the average characters per token, USD per
    token and USD per input character (the dry run's estimates), and the
    highest USD per input character of any one call (``max_usd_per_char``,
    the spend cap's estimate)."""
    row = conn.execute(
        "SELECT count(*), sum(prompt_tokens), sum(cost_usd), "
        "       sum((extras->>'input_chars')::bigint), "
        "       max(cost_usd / NULLIF((extras->>'input_chars')::bigint, 0)) "
        "  FROM okg.llm_calls "
        " WHERE caller = %s AND model = %s AND success "
        "   AND cost_usd IS NOT NULL AND prompt_tokens > 0 "
        "   AND extras ? 'input_chars'",
        (BACKFILL_CALLER, model_id),
    ).fetchone()
    calls, tokens, cost, chars, max_per_char = (
        int(row[0]), row[1], row[2], row[3], row[4],
    )
    if calls == 0 or not tokens:
        return {"calls": 0, "chars_per_token": None, "usd_per_token": None,
                "usd_per_char": None, "max_usd_per_char": None}
    return {
        "calls": calls,
        "chars_per_token": float(chars) / float(tokens) if chars else None,
        "usd_per_token": float(cost) / float(tokens),
        "usd_per_char": float(cost) / float(chars) if chars else None,
        "max_usd_per_char": None if max_per_char is None else float(max_per_char),
    }


# --------------------------------------------------------------------------- #
# Plan and lock                                                                #
# --------------------------------------------------------------------------- #

def build_set_plan(
    vector_set: Any, *, builder: Optional[Callable[[str], Any]] = None,
) -> tuple[Any, list[Any]]:
    """The probed plan for one set's backend, and the calls its probe made.

    The backend carries the set's provider pin and returns vectors at the
    set's own width, unpadded (PACT vector-set-pgvector-widths). The
    observer is attached before the capability probe, so the probe's
    request is captured and the run records it and counts it toward the cap.
    """
    from okg.substrate.embeddings.config import build_vector_set_plan
    from okg.substrate.embeddings.identity import build_embedder

    probe_calls: list[Any] = []

    def build(spec: str) -> Any:
        if builder is not None:
            embedder = builder(spec)
        else:
            embedder = build_embedder(
                spec, provider=vector_set.provider, vector_set=True,
            )
        if hasattr(embedder, "call_observer"):
            embedder.call_observer = probe_calls.append
        return embedder

    plan = build_vector_set_plan(vector_set, builder=build)
    embedder = getattr(plan, "embedder", None)
    if embedder is not None and hasattr(embedder, "call_observer"):
        embedder.call_observer = None
    return plan, probe_calls


def backfill_lock_key(set_id: str) -> str:
    """The text whose ``hashtextextended`` is the set's advisory lock key."""
    return f"okg.embed_backfill:{set_id}"


def _take_set_lock(dsn: str, set_id: str) -> Any:
    """A connection holding the set's backfill lock, or a refusal."""
    from okg.substrate.db.connection import connect as db_connect

    conn = db_connect(dsn, autocommit=True)
    try:
        held = conn.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
            (backfill_lock_key(set_id),),
        ).fetchone()[0]
    except BaseException:
        conn.close()
        raise
    if not held:
        conn.close()
        raise _error(
            f"another backfill of vector set {set_id!r} is running",
            code="embedding_backfill_in_progress",
            data={"set_id": set_id},
            fix_hint="wait for the other `okg db embed-backfill` to finish, then rerun",
        )
    return conn


def _cut_to_set(vector_set: Any, vector: Sequence[float]) -> list[float]:
    from okg.substrate.semantic_ann import query_vector_for_set

    return query_vector_for_set(vector_set, vector)


# --------------------------------------------------------------------------- #
# The run                                                                      #
# --------------------------------------------------------------------------- #

def run_set_backfill(
    dsn: str,
    *,
    deployment: str,
    config: Any,
    set_name: Optional[str],
    max_usd: Optional[float],
    limit: Optional[int] = None,
    window_rows: int = DEFAULT_WINDOW_ROWS,
    max_window_chars: int = DEFAULT_MAX_WINDOW_CHARS,
    builder: Optional[Callable[[str], Any]] = None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
) -> dict[str, Any]:
    """Embed the missing documents of one registered set; see module doc.

    ``limit`` caps the documents embedded (a pilot). ``builder`` replaces
    the backend constructor (tests); the plan is built only when a document
    actually needs a vector, so a run with nothing to embed makes no call.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.embeddings.config import EMBEDDING_PROBE_TEXT
    from okg.substrate.embeddings.identity import is_paid_model_id
    from okg.substrate.embeddings.store import (
        link_documents,
        missing_documents,
        record_fill_completion,
        record_templates,
        write_vectors,
    )

    if window_rows < 1 or max_window_chars < 1:
        raise ValueError("window_rows and max_window_chars must be at least 1")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")

    started = time.monotonic()
    with db_connect(dsn) as conn:
        resolved = resolve_registered_set(conn, config, set_name)
        conn.rollback()
    vector_set = resolved.stored
    set_id = vector_set.set_id
    model_id = vector_set.model_id
    paid = is_paid_model_id(model_id)
    if paid and max_usd is None:
        raise _error(
            f"vector set {vector_set.name!r} embeds with paid backend {model_id!r}, "
            "so the backfill needs a spend cap",
            code="embedding_spend_cap_required",
            data={"set": vector_set.name, "model_id": model_id},
            fix_hint="pass --max-usd <dollars> (see --dry-run for the estimate)",
        )
    if not paid and max_usd is not None:
        raise _error(
            f"vector set {vector_set.name!r} embeds with {model_id!r}, which "
            "reports no per-call cost, so a spend cap cannot be enforced",
            code="embedding_spend_cap_unenforceable",
            data={"set": vector_set.name, "model_id": model_id},
            fix_hint="omit --max-usd for a backend that makes no paid calls",
        )

    lock_conn = _take_set_lock(dsn, set_id)
    run_cost = 0.0
    calls: list[Any] = []
    observer_lock = threading.Lock()  # the observer runs on request threads
    ledger: Any = None
    plan: Any = None
    counts = {
        "nodes_scanned": 0,
        "links_written": 0,
        "documents_missing": 0,
        "documents_embedded": 0,
        "documents_reused": 0,
        "windows": 0,
        "header_fields_omitted": 0,
    }
    status = "completed"
    prior = 0.0
    prior_basis = "not_capped"
    db_rate: Optional[float] = None
    pin: Optional[_Pin] = None
    fill_recorded = False

    def observe(call: Any) -> None:
        nonlocal run_cost
        with observer_lock:
            calls.append(call)
            if call.cost_usd is not None:
                # Charged even when the response then failed validation.
                run_cost += call.cost_usd
            ledger.execute(
                """
                INSERT INTO okg.llm_calls
                  (deployment_name, caller, model, prompt_tokens,
                   completion_tokens, total_tokens, cost_usd, latency_ms,
                   success, error_kind, extras)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    deployment, BACKFILL_CALLER, model_id, call.prompt_tokens,
                    0 if call.success else None, call.total_tokens,
                    call.cost_usd, call.latency_ms, call.success,
                    call.error_kind,
                    json.dumps({
                        "set_id": set_id,
                        "set": vector_set.name,
                        "provider": call.provider,
                        "inputs": call.inputs,
                        "input_chars": call.input_chars,
                        "attempts": call.attempts,
                        "answered": bool(getattr(call, "answered", call.success)),
                    }),
                ),
            )

    def cap_stop() -> Optional[str]:
        """Why the run must stop before its next paid call, or None."""
        if max_usd is None:
            return None
        # An answered request without a reported cost (valid or not) makes
        # the cap unenforceable: stop before spending more.
        if any(getattr(c, "answered", c.success) and c.cost_usd is None for c in calls):
            return "cost_unreported"
        if prior + run_cost >= max_usd:
            return "cap_reached"
        return None

    def usd_per_char() -> Optional[float]:
        """The highest price per input character measured so far: this
        run's priced calls, and the database's earlier calls of the model.
        None before any price is known."""
        rates = [
            float(c.cost_usd) / float(c.input_chars)
            for c in calls
            if c.cost_usd is not None and (c.input_chars or 0) > 0
        ]
        if db_rate is not None:
            rates.append(db_rate)
        return max(rates) if rates else None

    def fits(chars: int) -> bool:
        """Whether a request of ``chars`` input characters, at the highest
        measured price, keeps the spend at or under the cap. True when no
        cap applies or no price is known yet (the probe, on a database that
        never paid this model)."""
        if max_usd is None:
            return True
        rate = usd_per_char()
        if rate is None:
            return True
        return prior + run_cost + rate * chars <= max_usd

    def ensure_plan() -> Any:
        nonlocal plan
        if plan is not None:
            return plan
        built, probe_calls = build_set_plan(vector_set, builder=builder)
        embedder = built.embedder
        if paid:
            if not hasattr(embedder, "call_observer"):
                raise _error(
                    f"backend {model_id!r} reports no per-call cost, so the "
                    "spend cap cannot be enforced",
                    code="embedding_spend_cap_unenforceable",
                    data={"set": vector_set.name, "model_id": model_id},
                    fix_hint="use a backend that reports each call's cost",
                )
            embedder.call_observer = observe
            # The capability probe was a paid call: record and count it.
            for call in probe_calls:
                observe(call)
        plan = built
        return plan

    try:
        with db_connect(dsn) as conn:
            if paid:
                # Read under the set's lock: no other backfill of this set can
                # spend between this read and this run's checks.
                ledger = db_connect(dsn, autocommit=True)
                prior, prior_basis = _prior_spend(
                    ledger, deployment=deployment, set_id=set_id, model_id=model_id,
                )
                # The highest price per character of any earlier call, not
                # the average: a cheap call must not lower the estimate.
                db_rate = _measured_rates(ledger, model_id)["max_usd_per_char"]
            pin = _latest_pin(conn, deployment)
            conn.rollback()
            renderer = _Renderer(conn, vector_set, pin)
            embedded_here: set[str] = set()
            missing_seen: set[str] = set()
            reused_seen: set[str] = set()
            for rows in _visible_windows(conn, pin, vector_set, window_rows=window_rows):
                window = _render_window(renderer, rows)
                counts["nodes_scanned"] += sum(window.nodes_by_subtype.values())
                counts["header_fields_omitted"] += window.omitted_fields
                if window.links:
                    # Links are free (local rendering): write them first, in
                    # their own transaction, so a template change takes
                    # effect even if the spend cap stops the vectors.
                    record_templates(conn, set_id, window.templates)
                    counts["links_written"] += link_documents(conn, set_id, window.links)
                conn.commit()
                missing = sorted(
                    missing_documents(conn, set_id, window.documents) - embedded_here
                )
                conn.rollback()
                counts["documents_missing"] += len(set(missing) - missing_seen)
                missing_seen.update(missing)
                reused = set(window.documents) - missing_seen - reused_seen
                counts["documents_reused"] += len(reused)
                reused_seen.update(reused)
                position = 0
                while position < len(missing):
                    remaining = None if limit is None else limit - counts["documents_embedded"]
                    if remaining is not None and remaining <= 0:
                        status = "limit_reached"
                        break
                    stop = cap_stop()
                    if stop is not None:
                        status = stop
                        break
                    if plan is None and paid and not fits(len(EMBEDDING_PROBE_TEXT)):
                        # The probe is a paid call too.
                        status = "cap_reached"
                        break
                    current = ensure_plan()
                    # The probe may have spent: check again before the batch.
                    stop = cap_stop()
                    if stop is not None:
                        status = stop
                        break
                    # One batch within the character budget, holding only the
                    # documents whose estimated cost (characters at the highest
                    # measured price) still fits under the cap.
                    batch: list[str] = []
                    chars = 0
                    while position < len(missing) and (
                        remaining is None or len(batch) < remaining
                    ):
                        text = vector_set.document_prefix + window.documents[missing[position]][0]
                        if batch and chars + len(text) > max_window_chars:
                            break
                        if not fits(chars + len(text)):
                            break
                        batch.append(missing[position])
                        chars += len(text)
                        position += 1
                    if not batch:
                        status = "cap_reached"
                        break
                    vectors = current.embed_batch([
                        vector_set.document_prefix + window.documents[doc][0]
                        for doc in batch
                    ])
                    rows_out = [
                        (doc, _cut_to_set(vector_set, vector))
                        for doc, vector in zip(batch, vectors)
                    ]
                    write_vectors(conn, set_id, rows_out)
                    conn.commit()
                    embedded_here.update(batch)
                    counts["documents_embedded"] += len(batch)
                counts["windows"] += 1
                if on_progress is not None:
                    elapsed = time.monotonic() - started
                    on_progress({
                        **counts,
                        "run_cost_usd": round(run_cost, 6),
                        "prior_cost_usd": round(prior, 6),
                        "elapsed_seconds": round(elapsed, 1),
                        "after": rows[-1][0],
                    })
                if status != "completed":
                    break
            if status == "completed" and pin is not None:
                # Every window was read and every missing document embedded
                # (a limit, cap or error stops before this). The next
                # watermark carries the completion; search routes to the
                # set only from then on (Jason, "ehroute yes").
                # With this run's link count: a link another writer took
                # meanwhile makes the completion uncarriable (its rows are
                # not visible at this pin).
                fill_recorded = record_fill_completion(
                    conn, set_id, pin.generation_id,
                    links_written=counts["links_written"],
                )
                conn.commit()
    finally:
        if plan is not None and paid:
            plan.embedder.call_observer = None
        if ledger is not None:
            ledger.close()
        lock_conn.close()
    elapsed = time.monotonic() - started
    succeeded = [c for c in calls if c.success]
    return {
        "status": status,
        "deployment": deployment,
        "set": vector_set.name,
        "set_id": set_id,
        "model_id": model_id,
        "config_drift": list(resolved.drift),
        "generation_id": None if pin is None else pin.generation_id,
        # True when this run completed the set at its pin (no document left
        # without a vector) and recorded it: after the next publish semantic
        # search ranks the set. False for a partial run.
        "fill_completed": status == "completed",
        "fill_completion_recorded": fill_recorded,
        **counts,
        "node_rows_written": 0,
        "requests": len(succeeded),
        "failed_requests": len(calls) - len(succeeded),
        "prompt_tokens": sum(c.prompt_tokens or 0 for c in succeeded),
        "run_cost_usd": round(run_cost, 6),
        "prior_cost_usd": round(prior, 6),
        "prior_cost_basis": prior_basis,
        "max_usd": max_usd,
        "cap_overshoot_bound": (
            "none at the measured price: a request is sent only while the "
            "spend so far plus its characters at the highest price per "
            "character measured (this run or this database) stays at or under "
            "the cap; a request priced above that estimate can pass the cap "
            f"by its own excess (at most one request of {max_window_chars} "
            "characters, or the probe before any price is known)"
        ),
        "elapsed_seconds": round(elapsed, 1),
        "next_step": (
            "publish once (`okg run --deployment <name> --once`): the new "
            "generation's watermark makes these vectors and links readable"
        ),
    }


# --------------------------------------------------------------------------- #
# Worker fill (free backends)                                                  #
# --------------------------------------------------------------------------- #

def fill_free_sets(dsn: str, *, deployment: str, config: Any) -> list[dict[str, Any]]:
    """The runtime worker's fill of every configured vector set after a
    publish (Jason, lane chat 2026-10-14, "ehfill yes").

    A set with a free backend (``hash``, ``sentence-transformers``) whose
    newest completed backfill is not at the latest published generation is
    filled by :func:`run_set_backfill` itself: the same per-set lock (a
    running ``okg db embed-backfill`` makes this one ``busy``) and the same
    spend path (no cap: a free backend makes no paid call). A paid set is
    refused, never embedded: it needs ``okg db embed-backfill --max-usd``.
    Returns one report per configured set.
    """
    reports: list[dict[str, Any]] = []
    for item in tuple(getattr(config, "vector_sets", ()) or ()):
        report: dict[str, Any] = {"set": item.name, "set_id": item.set_id}
        reports.append(report)
        try:
            _fill_one_free_set(dsn, deployment=deployment, config=config,
                               item=item, report=report)
        except Exception as exc:  # noqa: BLE001 -- reported per set, logged
            # One set's failure is that set's report; the other sets are
            # still filled and the embed workflow still completes. The
            # report reaches the workflow's outcome row, which `okg doctor`
            # reads (`vector_set_fill_failed`). Caught broadly on purpose:
            # a lost connection or a missing model is this set's state for
            # this cycle, and the next cycle retries it.
            log.error(
                "vector set fill failed: deployment=%s set=%s: %s",
                deployment, item.name, exc,
            )
            report.update(
                status="error",
                error=type(exc).__name__,
                code=getattr(exc, "code", None),
                reason=str(exc),
            )
    return reports


def _fill_one_free_set(
    dsn: str, *, deployment: str, config: Any, item: Any, report: dict[str, Any],
) -> None:
    """:func:`fill_free_sets` for one set; updates ``report`` in place."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.embeddings.identity import is_paid_model_id

    if is_paid_model_id(item.model_id):
        report.update(
            status="refused_paid",
            reason=(
                f"{item.model_id} is paid; the worker makes no paid call. "
                "Run `okg db embed-backfill --deployment <name> --set "
                f"{item.name} --max-usd <usd>`"
            ),
        )
        return
    with db_connect(dsn) as conn:
        try:
            resolve_registered_set(conn, config, item.name)
        except SubstrateError as exc:
            if exc.code not in ("embedding_set_unregistered", "embedding_set_inactive"):
                raise
            report.update(status=exc.code, reason=exc.message)
            return
        pin = _latest_pin(conn, deployment)
        done = conn.execute(
            "SELECT 1 FROM okg.embedding_fill_completions "
            "WHERE set_id = %s AND generation_id = %s",
            (item.set_id, pin.generation_id),
        ).fetchone()
        if done is not None:
            conn.rollback()
            report.update(status="up_to_date", generation_id=pin.generation_id)
            return
        carried = _carry_completion_forward(conn, deployment, item.set_id, pin)
        conn.commit()
    if carried:
        report.update(status="unchanged", generation_id=pin.generation_id)
        return
    try:
        ran = run_set_backfill(
            dsn, deployment=deployment, config=config, set_name=item.name,
            max_usd=None,
        )
    except SubstrateError as exc:
        if exc.code != "embedding_backfill_in_progress":
            raise
        report.update(status="busy", reason=exc.message)
        return
    report.update(
        status=ran["status"],
        generation_id=ran["generation_id"],
        documents_embedded=ran["documents_embedded"],
        links_written=ran["links_written"],
        fill_completed=ran["fill_completed"],
    )


def _carry_completion_forward(
    conn: Any, deployment: str, set_id: str, pin: _Pin,
) -> bool:
    """Record a completed fill at ``pin`` without reading the graph, when
    nothing that could leave a document without a vector happened since
    the set's newest completed fill: the set's link counter still equals
    that completion's ``max_seq`` and the set is active, and the pin reads
    the same catalog version (a reclassification can change a template).
    Returns whether it recorded one; False means the full pass must run.

    The link counter is commit-ordered (each writer holds its row until it
    commits), so equality is exact, not a race on timestamps. The graph
    writer's every batch that writes a covered text row takes at least one
    link number, a new link's or, when every link of the batch is already
    stored (a node set back to earlier content), one number with no link
    (:func:`~okg.substrate.embeddings.store.write_node_documents`). So does
    a registration that changes the document rules or reactivates the set
    (:func:`~okg.substrate.embeddings.store.register_sets`), and a run undo,
    a branch restored to a generation or a merge (merge-back's LAND) that
    writes node rows (:func:`~okg.substrate.embeddings.store.mark_node_rows_restored`).
    The rule: any path that writes node rows outside
    :func:`~okg.substrate.embeddings.store.write_node_documents` must call
    ``mark_node_rows_restored`` in its transaction.
    A completion records ``max_seq`` -1, and never carries, when the link
    numbers taken
    since its pin's watermark are not exactly its own links (another writer
    took some: their rows are not visible at its pin), when its pin has no
    watermark, or when its own link count is unknown
    (:func:`~okg.substrate.embeddings.store.record_fill_completion`).

    Not seen by this check, so filled only by the next full pass (after any
    other change) or by ``okg db embed-backfill``: node rows written outside
    the graph writer, which take no link number. These are a row that
    becomes visible after the completion with no write (a fast-forward of a
    branch written before it) and a node row rewritten in place (known:
    ``nomos/source_lineage_repair.py``, which also leaves the new row with
    no link until a full pass).
    """
    from psycopg.rows import tuple_row

    from okg.substrate.mcp.session import open_session

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT c.generation_id FROM okg.embedding_fill_completions AS c "
            "  JOIN okg.embedding_sets AS s ON s.set_id = c.set_id "
            " WHERE c.set_id = %s AND s.active AND s.next_seq - 1 = c.max_seq "
            " ORDER BY c.completed_at DESC LIMIT 1",
            (set_id,),
        )
        row = cur.fetchone()
    if row is None:
        return False
    try:
        previous = open_session(
            conn, deployment_name=deployment, requested_generation=str(row[0]),
        )
    except SubstrateError as exc:
        # The completion's generation can no longer be pinned (compacted
        # away): its catalog is unknown, so the full pass runs.
        log.info(
            "vector set %s: full fill pass, the last completed fill's "
            "generation %s cannot be pinned: %s", set_id, row[0], exc,
        )
        return False
    if getattr(previous, "catalog_version_id", None) != pin.catalog_version_id:
        return False
    # One statement re-checks the counter and the row, so a link written
    # after the check above cannot fall under this completion.
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "INSERT INTO okg.embedding_fill_completions "
            "  (set_id, generation_id, max_seq, max_vector_seq) "
            "SELECT s.set_id, %s, s.next_seq - 1, v.next_seq - 1 "
            "  FROM okg.embedding_sets AS s "
            "  JOIN okg.embedding_vector_counters AS v ON v.set_id = s.set_id "
            "  JOIN okg.embedding_fill_completions AS c "
            "    ON c.set_id = s.set_id AND c.generation_id = %s "
            " WHERE s.set_id = %s AND s.active AND s.next_seq - 1 = c.max_seq "
            "ON CONFLICT (set_id, generation_id) DO NOTHING",
            (pin.generation_id, str(row[0]), set_id),
        )
        return cur.rowcount == 1


# --------------------------------------------------------------------------- #
# Dry run                                                                      #
# --------------------------------------------------------------------------- #

def dry_run_backfill(
    dsn: str,
    *,
    deployment: str,
    config: Any,
    set_name: Optional[str],
    max_usd: Optional[float] = None,
    window_rows: int = DEFAULT_WINDOW_ROWS,
) -> dict[str, Any]:
    """What a backfill would do, per set and per subtype. No call, no write.

    Every configured set is reported when ``set_name`` is None. A set that
    is not registered (or not active) is reported with that status and no
    counts: its documents are built from the registered row.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.embeddings.identity import is_paid_model_id
    from okg.substrate.embeddings.store import missing_documents, missing_links

    if set_name is None:
        names = [item.name for item in _configured_set_list(config)]
    else:
        names = [_configured_set(config, set_name).name]
    reports: list[dict[str, Any]] = []
    with db_connect(dsn) as conn:
        pin: Optional[_Pin] = None
        for name in names:
            try:
                resolved = resolve_registered_set(conn, config, name)
            except SubstrateError as exc:
                if exc.code not in ("embedding_set_unregistered", "embedding_set_inactive"):
                    raise
                reports.append({
                    "set": name,
                    "status": exc.code,
                    "reason": exc.message,
                    "fix_hint": exc.fix_hint,
                    **{k: v for k, v in exc.data.items() if k == "set_id"},
                })
                continue
            if pin is None:
                pin = _latest_pin(conn, deployment)
            vector_set = resolved.stored
            renderer = _Renderer(conn, vector_set, pin)
            by_subtype: dict[str, dict[str, int]] = {}
            seen_missing: set[str] = set()
            seen_documents: set[str] = set()
            nodes = links_missing = chars_missing = 0
            for rows in _visible_windows(conn, pin, vector_set, window_rows=window_rows):
                window = _render_window(renderer, rows)
                for subtype, count in window.nodes_by_subtype.items():
                    entry = by_subtype.setdefault(subtype, {
                        "nodes": 0, "links_missing": 0,
                        "documents_missing": 0, "characters_missing": 0,
                    })
                    entry["nodes"] += count
                    nodes += count
                subtype_of_node = {
                    node_id: subtype for node_id, subtype, _a, _r in rows
                }
                for _t, node_id, _r, _d in missing_links(conn, vector_set.set_id, window.links):
                    by_subtype[subtype_of_node[node_id]]["links_missing"] += 1
                    links_missing += 1
                seen_documents.update(window.documents)
                fresh = missing_documents(
                    conn, vector_set.set_id, window.documents,
                ) - seen_missing
                for doc in sorted(fresh):
                    text, subtype = window.documents[doc]
                    size = len(vector_set.document_prefix) + len(text)
                    entry = by_subtype[subtype]
                    entry["documents_missing"] += 1
                    entry["characters_missing"] += size
                    chars_missing += size
                seen_missing.update(fresh)
            model_id = vector_set.model_id
            paid = is_paid_model_id(model_id)
            rates = _measured_rates(conn, model_id) if paid else None
            # Measured rates come from this database's call ledger for the
            # model. Tokens per character vary a lot by text (C4 measured
            # 2.38 characters per token, not 4), so dollars are priced per
            # character, the count the dry run knows exactly.
            if rates and rates["chars_per_token"]:
                chars_per_token = rates["chars_per_token"]
                token_basis = (
                    f"measured from {rates['calls']} call(s): "
                    f"{chars_per_token:.2f} characters per token in earlier "
                    f"backfill calls of {model_id}"
                )
            else:
                chars_per_token = float(ASSUMED_CHARS_PER_TOKEN)
                why = (
                    "rates are measured only for paid models"
                    if not paid else
                    "this database has no priced backfill call of "
                    f"{model_id} with its token and character counts"
                )
                token_basis = (
                    f"assumed {ASSUMED_CHARS_PER_TOKEN} characters per token: "
                    f"no measured rate ({why}); real text can have fewer "
                    "characters per token, so more tokens"
                )
            tokens = int(round(chars_missing / chars_per_token))
            if not paid:
                estimated_usd: Optional[float] = 0.0
                price_source = f"{model_id} is a free backend: no paid call"
            elif rates and rates["usd_per_char"] is not None:
                estimated_usd = round(chars_missing * rates["usd_per_char"], 6)
                price_source = (
                    f"measured from {rates['calls']} call(s): "
                    f"${rates['usd_per_char']:.3e} per input character in "
                    f"earlier priced backfill calls of {model_id}"
                )
            else:
                estimated_usd = None
                price_source = (
                    f"none: the USD is unknown; the code has no price table for "
                    f"{model_id}, and this database records no earlier priced "
                    "backfill call for it; check the provider's price for the "
                    "estimated tokens"
                )
            prior, prior_basis = (
                _prior_spend(conn, deployment=deployment, set_id=vector_set.set_id,
                             model_id=model_id)
                if paid else (0.0, "free_backend")
            )
            report: dict[str, Any] = {
                "set": vector_set.name,
                "set_id": vector_set.set_id,
                "status": "active",
                "model_id": model_id,
                "config_drift": list(resolved.drift),
                "generation_id": pin.generation_id,
                "nodes": nodes,
                "documents": len(seen_documents),
                "links_missing": links_missing,
                "documents_missing": len(seen_missing),
                "characters_missing": chars_missing,
                "estimated_tokens": tokens,
                "token_basis": token_basis,
                "estimated_usd": estimated_usd,
                "price_source": price_source,
                "max_usd": max_usd,
                "prior_cost_usd": round(prior, 6),
                "prior_cost_basis": prior_basis,
                "by_subtype": dict(sorted(by_subtype.items())),
            }
            if paid and max_usd is not None:
                report["cap_remaining_usd"] = round(max(max_usd - prior, 0.0), 6)
                report["cap_reached"] = prior >= max_usd
            reports.append(report)
        conn.rollback()
    return {"dry_run": True, "deployment": deployment, "sets": reports}


def _configured_set_list(config: Any) -> tuple[Any, ...]:
    sets = tuple(getattr(config, "vector_sets", ()) or ())
    if not sets:
        _configured_set(config, None)  # raises the disabled refusal
    return sets


# --------------------------------------------------------------------------- #
# embed-show                                                                   #
# --------------------------------------------------------------------------- #

def show_node_document(
    dsn: str,
    *,
    deployment: str,
    config: Any,
    set_name: Optional[str],
    node_id: str,
) -> dict[str, Any]:
    """The document a node's visible content at the latest published
    generation renders to under the REGISTERED set, with its hashes, the
    fields left out, and whether a link and a vector exist. Reads only."""
    from psycopg.rows import tuple_row

    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.embeddings.store import row_hash_expression
    from okg.substrate.errors import NodeNotFound
    from okg.substrate.graph_store_internal.common import quote_physical_table

    with db_connect(dsn) as conn:
        resolved = resolve_registered_set(conn, config, set_name, require_active=False)
        vector_set = resolved.stored
        pin = _latest_pin(conn, deployment)
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                f"SELECT n.subtype, n.attrs, {row_hash_expression('n')} "
                f"  FROM {quote_physical_table(pin.physical_table)} AS n "
                " WHERE n.node_id = %s AND n.deleted = FALSE "
                "   AND n.live_lo <= %s AND %s < n.live_hi",
                (node_id, pin.branch_point, pin.branch_point),
            )
            row = cur.fetchone()
        if row is None:
            raise NodeNotFound(
                f"node {node_id!r} is not visible at generation {pin.generation_id}",
                data={"node_id": node_id, "generation_id": pin.generation_id},
                fix_hint=(
                    "check the node id; it must be visible at the latest "
                    "published generation"
                ),
            )
        subtype, attrs, row_hash = str(row[0]), dict(row[1] or {}), str(row[2])
        rendered = _Renderer(conn, vector_set, pin).render(subtype, attrs)
        out: dict[str, Any] = {
            "set": vector_set.name,
            "set_id": vector_set.set_id,
            "set_active": resolved.active,
            "config_drift": list(resolved.drift),
            "generation_id": pin.generation_id,
            "node_id": node_id,
            "subtype": subtype,
            "row_hash": row_hash,
        }
        if rendered is None:
            out.update(
                covered=False,
                reason=(
                    "the set does not cover this subtype"
                    if not vector_set.covers(subtype)
                    else "the node has no non-empty text body"
                ),
                document=None, doc_hash=None, has_vector=False, linked=False,
                readable_at_pin=False,
            )
            conn.rollback()
            return out
        with conn.cursor(row_factory=tuple_row) as cur:
            # Whether the link and the vector exist at all, and whether both
            # are at or below the pin's watermark (a read of the pinned
            # generation serves them; one written since needs a publish).
            cur.execute(
                """
                WITH w AS (
                  SELECT max_seq,
                         COALESCE(max_vector_seq, max_seq) AS max_vector_seq
                    FROM okg.embedding_watermarks
                   WHERE generation_id = %(g)s AND set_id = %(s)s
                ), l AS (
                  SELECT seq FROM okg.node_documents
                   WHERE set_id = %(s)s AND template_hash = %(t)s
                     AND node_id = %(n)s AND row_hash = %(r)s
                ), v AS (
                  SELECT seq FROM okg.text_embeddings
                   WHERE set_id = %(s)s AND doc_hash = %(d)s
                )
                SELECT EXISTS (SELECT 1 FROM l), EXISTS (SELECT 1 FROM v),
                       EXISTS (SELECT 1 FROM l, w WHERE l.seq <= w.max_seq)
                   AND EXISTS (SELECT 1 FROM v, w WHERE v.seq <= w.max_vector_seq)
                """,
                {"g": pin.generation_id, "s": vector_set.set_id,
                 "t": rendered.template_hash, "n": node_id, "r": row_hash,
                 "d": rendered.doc_hash},
            )
            linked, has_vector, readable = cur.fetchone()
        conn.rollback()
    out.update(
        covered=True,
        document=rendered.text,
        document_prefix=vector_set.document_prefix,
        characters=len(rendered.text),
        max_chars=vector_set.max_chars,
        reaches_max_chars=len(rendered.text) >= vector_set.max_chars,
        doc_hash=rendered.doc_hash,
        template_hash=rendered.template_hash,
        omitted_fields=list(rendered.omitted_fields),
        linked=bool(linked),
        has_vector=bool(has_vector),
        # This document's link and vector are both under the pinned
        # generation's watermark: a semantic read of it can serve them.
        readable_at_pin=bool(readable),
    )
    return out
