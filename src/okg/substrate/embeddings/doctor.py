"""The vector-set part of ``okg doctor`` (PACT openrouter-embeddings-hnsw).

For each vector set that ``deployment.yaml`` declares or this database has
registered and active, :func:`vector_set_report` states:

* whether the file and the registered row agree (registration, the stored
  identity under this code, document and index settings);
* the set's index state (``inspect_set_index``), including an index for new
  build settings that is missing or invalid while an older one serves;
* coverage at the latest published generation: visible nodes with
  non-blank text (the fill's rule; blank text gets no document), how many
  link to a document of the current template, and how many have a vector,
  all at the generation's watermark; plus vectors and links written since
  that watermark that only the next publish makes readable;
* the spend ledger for the set (``okg.llm_calls``, caller
  ``embeddings.backfill``, keyed by deployment and set): calls, dollars, and
  answered calls with no reported cost;
* the runtime worker's newest vector-set fill, from the embed workflow's
  outcome row: a set whose fill failed, or a failure outside any set.

It also counts vectors in the old node columns (``embedding`` /
``embedding_model``) at the latest published generation, by model, and flags
the models no active registered set owns. With no active set it skips that
count: semantic search then reads those columns, so nothing is left over.

Reads only. Every finding is a warning: semantic search is one search method
and the graph itself is intact, so these never fail plain doctor. The named
check ``okg doctor --check vector-sets`` exits 1 on any finding.
"""
from __future__ import annotations

from typing import Any, Optional

from psycopg.rows import dict_row, tuple_row

__all__ = ["vector_set_report"]

#: Index states a semantic read refuses on.
_UNREADY_INDEX = frozenset({"missing", "building", "invalid", "unsupported"})


def _finding(code: str, message: str, fix_hint: str, **data: Any) -> dict[str, Any]:
    return {"code": code, "message": message, "fix_hint": fix_hint, **data}


def _tables_present(conn: Any) -> bool:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT to_regclass('okg.embedding_sets') IS NOT NULL")
        return bool(cur.fetchone()[0])


def _registered_rows(conn: Any) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT s.set_id, s.name, s.backend, s.identity_json, s.documents_json, "
            "s.index_json, s.active, s.next_seq, v.next_seq AS next_vector_seq "
            "FROM okg.embedding_sets AS s "
            "LEFT JOIN okg.embedding_vector_counters AS v ON v.set_id = s.set_id "
            "ORDER BY s.set_id"
        )
        return list(cur.fetchall())


def _latest_published(conn: Any, deployment: str) -> Optional[Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return store.latest_generation(deployment_name=deployment, status="published")


def _row_counts(conn: Any, set_id: str) -> tuple[int, int]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM okg.text_embeddings WHERE set_id = %(s)s), "
            "       (SELECT count(*) FROM okg.node_documents WHERE set_id = %(s)s)",
            {"s": set_id},
        )
        vectors, links = cur.fetchone()
    return int(vectors), int(links)


def _watermark(conn: Any, generation_id: str, set_id: str) -> Optional[tuple[int, int]]:
    """``(links, vectors)`` bounds of the generation for the set, or None."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT max_seq, COALESCE(max_vector_seq, max_seq) "
            "FROM okg.embedding_watermarks "
            "WHERE generation_id = %s AND set_id = %s",
            (generation_id, set_id),
        )
        row = cur.fetchone()
    return None if row is None else (int(row[0]), int(row[1]))


#: A set whose newest completed backfill is older than this many publishes
#: (publisher watermarks recorded since it) is flagged
#: ``vector_set_fill_stale`` (Jason, lane chat 2026-10-14, "ehfill yes"):
#: nodes written since may have no vector, and a paid set is filled only
#: by hand.
FILL_STALE_PUBLISHES = 10


def _fill_state(conn: Any, set_id: str) -> dict[str, Any]:
    """The set's newest completed backfill and the publishes since it."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT generation_id, completed_at FROM okg.embedding_fill_completions "
            "WHERE set_id = %s ORDER BY completed_at DESC, max_vector_seq DESC LIMIT 1",
            (set_id,),
        )
        row = cur.fetchone()
        if row is None:
            return {"completed_at": None, "generation_id": None, "publishes_since": None}
        cur.execute(
            "SELECT count(*) FROM okg.embedding_watermarks "
            "WHERE set_id = %s AND recorded_by = 'publisher' AND recorded_at > %s",
            (set_id, row[1]),
        )
        since = int(cur.fetchone()[0])
    return {
        "completed_at": row[1].isoformat(),
        "generation_id": str(row[0]),
        "publishes_since": since,
    }


def _last_worker_fill(conn: Any, deployment: str) -> Optional[dict[str, Any]]:
    """The vector-set part of the deployment's newest embed-workflow
    outcome (``okg.workflow_operational_outcomes.details``): ``{"workflow",
    "recorded_at", "vector_sets", "vector_sets_error"}``, or None when no
    such outcome (or table) exists. The worker fill catches its failures so
    they cannot fail the node-column batch; this is where they persist."""
    from okg.substrate.runtime.workflow_operational_outcomes import (
        EMBED_WORKFLOW_NAME,
    )

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT to_regclass('okg.workflow_operational_outcomes') IS NOT NULL "
            "   AND to_regclass('okg.workflow_operational_registrations') IS NOT NULL"
        )
        if not cur.fetchone()[0]:
            return None
        cur.execute(
            "SELECT o.workflow_uuid, o.recorded_at, o.details "
            "  FROM okg.workflow_operational_outcomes AS o "
            "  JOIN okg.workflow_operational_registrations AS r USING (workflow_uuid) "
            " WHERE r.deployment_name = %s AND r.workflow_name = %s "
            " ORDER BY o.recorded_at DESC LIMIT 1",
            (deployment, EMBED_WORKFLOW_NAME),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return _worker_fill_from_details(str(row[0]), row[1].isoformat(), row[2])


def _worker_fill_from_details(
    workflow: str, recorded_at: str, details: Any,
) -> dict[str, Any]:
    """The vector-set fields of one outcome row's ``details``.
    ``truncated`` is the outcome writer's ``_truncated`` marker: it dropped
    whole keys to stay under its size bound, so ``vector_sets`` may be
    missing failures."""
    details = details if isinstance(details, dict) else {}
    return {
        "workflow": workflow,
        "recorded_at": recorded_at,
        "vector_sets": list(details.get("vector_sets") or ()),
        "vector_sets_error": details.get("vector_sets_error"),
        "truncated": bool(details.get("_truncated")),
    }


def _fill_failure_findings(last: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    """``vector_set_fill_failed`` for each failure in the newest worker
    fill: a set whose fill raised, and a failure outside any set; and
    ``vector_set_fill_report_truncated`` when the outcome row was cut to its
    size bound, so its failures cannot be read as none (review P9 SF3)."""
    if last is None:
        return []
    hint = (
        "read the runtime worker's log for the error; run `okg db "
        "embed-backfill --deployment <name> --set <set> --dry-run` to "
        "reproduce it"
    )
    findings = [] if not last.get("truncated") else [_finding(
        "vector_set_fill_report_truncated",
        "the runtime worker's last embed outcome was cut to its size bound, "
        "so its vector-set fill statuses may be incomplete; doctor cannot "
        "tell whether any fill failed",
        hint,
        workflow=last["workflow"], recorded_at=last["recorded_at"],
    )]
    findings += [
        _finding(
            "vector_set_fill_failed",
            f"the runtime worker's last fill of vector set {entry.get('set')!r} "
            f"failed: {entry.get('error')}: {entry.get('reason')}",
            hint,
            set=entry.get("set"), error=entry.get("error"),
            error_code=entry.get("code"), workflow=last["workflow"],
            recorded_at=last["recorded_at"],
        )
        for entry in last["vector_sets"]
        if isinstance(entry, dict) and entry.get("status") == "error"
    ]
    error = last["vector_sets_error"]
    if isinstance(error, dict):
        findings.append(_finding(
            "vector_set_fill_failed",
            f"the runtime worker's last vector-set fill failed before any "
            f"set: {error.get('error')}: {error.get('reason')}",
            hint,
            error=error.get("error"), error_code=error.get("code"),
            workflow=last["workflow"], recorded_at=last["recorded_at"],
        ))
    return findings


def _spend(conn: Any, *, deployment: str, set_id: str) -> dict[str, Any]:
    from okg.substrate.embeddings.backfill import BACKFILL_CALLER

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT count(*), sum(cost_usd), "
            "       count(*) FILTER (WHERE cost_usd IS NULL AND "
            "         (success OR error_kind = 'embedder_output_invalid')), "
            "       count(*) FILTER (WHERE NOT success) "
            "  FROM okg.llm_calls "
            " WHERE caller = %s AND deployment_name = %s "
            "   AND extras->>'set_id' = %s",
            (BACKFILL_CALLER, deployment, set_id),
        )
        calls, usd, uncosted, failed = cur.fetchone()
    return {
        "calls": int(calls),
        "usd": float(usd) if usd is not None else 0.0,
        "basis": "ledger" if int(calls) else "no_prior_calls",
        "answered_uncosted_calls": int(uncosted),
        "failed_calls": int(failed),
    }


def _old_columns(
    conn: Any, checkpoint: Optional[Any], owned_models: set[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Vectors in the old node columns at the published generation, by model."""
    from types import SimpleNamespace

    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.graph_store_internal.common import quote_physical_table
    from okg.substrate.semantic_ann import _registered_node_segment

    physical = _registered_node_segment(conn)
    if physical is None:
        return {"checked": False, "reason": "node table not registered"}, []
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT count(*) FROM pg_attribute "
            " WHERE attrelid = to_regclass(%s) AND NOT attisdropped "
            "   AND attname IN ('embedding', 'embedding_model')",
            (quote_physical_table(physical),),
        )
        if int(cur.fetchone()[0]) < 2:
            return {"checked": False, "reason": "no embedding columns"}, []
    if checkpoint is None:
        return {"checked": False, "reason": "nothing published"}, []
    bp = branch_point(conn, SimpleNamespace(generation_id=str(checkpoint.checkpoint_id)))
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"SELECT COALESCE(n.embedding_model, ''), count(*) "
            f"  FROM {quote_physical_table(physical)} AS n "
            " WHERE n.embedding IS NOT NULL AND n.deleted = FALSE "
            "   AND n.live_lo <= %(bp)s AND %(bp)s < n.live_hi "
            " GROUP BY 1 ORDER BY 1",
            {"bp": bp},
            prepare=False,
        )
        by_model = {str(model): int(count) for model, count in cur.fetchall()}
    unowned = {m: c for m, c in by_model.items() if m not in owned_models}
    findings = [
        _finding(
            "vector_old_column_unowned",
            f"{count} nodes at the published generation hold a vector in the "
            f"old node column for model {model or '(none)'!r}, which no active "
            "vector set uses; semantic search stops reading them once the "
            "search set holds vectors",
            "declare a set with that backend in deployment.yaml's "
            "embeddings.sets and run `okg migrate --deployment <name> --apply`, "
            "or leave them for the change that retires the old columns",
            model=model or None,
            nodes=count,
        )
        for model, count in sorted(unowned.items())
    ]
    return {
        "checked": True,
        "generation_id": str(checkpoint.checkpoint_id),
        "nodes_by_model": by_model,
        "owned_models": sorted(owned_models),
        "unowned_nodes": sum(unowned.values()),
    }, findings


def vector_set_report(
    conn: Any, *, deployment: str, config: Any, exact_coverage: bool = False,
) -> dict[str, Any]:
    """The doctor's vector-set report; see the module docstring.

    Coverage of a set with more than ``semantic_ann.COVERAGE_EXACT_MAX``
    visible text nodes is estimated from a row sample unless
    ``exact_coverage`` (``okg doctor --check vector-sets``, which has no
    statement timeout); see :func:`okg.substrate.semantic_ann.set_coverage`.

    ``conn`` is an autocommit psycopg connection (a statement that times out
    leaves the next one usable). ``config`` is the deployment's
    ``EmbeddingConfig``, or None when there is no manifest. Returns
    ``{"ok", "sets", "old_columns", "findings"}``; ``ok`` is False when there
    is any finding.
    """
    from okg.substrate.embeddings.identity import SENTENCE_TRANSFORMERS_PREFIX
    from okg.substrate.embeddings.store import _set_from_row, config_drift
    from okg.substrate.semantic_ann import inspect_set_index, set_coverage

    findings: list[dict[str, Any]] = []
    configured: dict[str, Any] = {}
    # A sentence-transformers set's id needs its model's width, so its model
    # loads here; a model that cannot load is that set's finding, and the
    # other sets are still reported. Loading a model can raise OSError and
    # Hugging Face errors as well as EmbedderModelMissing, so the catch is
    # broad, but only for that backend: no other backend loads a model for
    # its id, so an error there is a defect and propagates.
    unresolved: set[str] = set()
    for item in (getattr(config, "vector_sets", ()) or ()) if config is not None else ():
        try:
            configured[item.set_id] = item
        except Exception as exc:  # noqa: BLE001 - reported, never masked.
            if not str(item.backend).startswith(SENTENCE_TRANSFORMERS_PREFIX):
                raise
            unresolved.add(item.name)
            findings.append(_finding(
                "vector_set_probe_failed",
                f"could not load the model of vector set {item.name!r} to "
                f"compute its width and id: {type(exc).__name__}: {exc}",
                "install the set's sentence-transformers model on this host, "
                "or remove the set from deployment.yaml",
                set=item.name, backend=item.backend,
            ))
    present = _tables_present(conn)
    rows = _registered_rows(conn) if present else []
    # A registered row of an unresolved set is not reported as unconfigured.
    registered = {
        str(row["set_id"]): row for row in rows
        if str(row["set_id"]) in configured or str(row["name"]) not in unresolved
    }
    try:
        checkpoint = _latest_published(conn, deployment)
    except Exception as exc:  # noqa: BLE001 - reported, never masked.
        checkpoint = None
        findings.append(_finding(
            "vector_set_probe_failed",
            f"could not read the latest published generation: "
            f"{type(exc).__name__}: {exc}",
            "run `okg migrate --deployment <name> --apply` and publish",
        ))

    sets: list[dict[str, Any]] = []
    wanted = list(configured) + [
        set_id for set_id, row in registered.items()
        if set_id not in configured and row["active"]
    ]
    for set_id in wanted:
        conf = configured.get(set_id)
        row = registered.get(set_id)
        name = conf.name if conf is not None else str(row["name"])
        entry: dict[str, Any] = {
            "set": name,
            "set_id": set_id,
            "configured": conf is not None,
            "registered": row is not None,
        }
        sets.append(entry)
        if row is None:
            findings.append(_finding(
                "vector_set_unregistered",
                f"vector set {name!r} is in deployment.yaml but not registered "
                "in this database, so semantic search cannot read it",
                "run `okg migrate --deployment <name> --apply`",
                set=name, set_id=set_id,
            ))
            continue
        entry["active"] = bool(row["active"])
        entry["backend"] = str(row["backend"])
        try:
            stored = _set_from_row(row)
            stale = stored.set_id != set_id
        except Exception:  # noqa: BLE001 - an unreadable row is stale.
            stored, stale = None, True
        entry["stale"] = stale
        if conf is None:
            findings.append(_finding(
                "vector_set_not_configured",
                f"vector set {name!r} is registered and active but not in "
                "deployment.yaml; it still takes a document link for every "
                "node write",
                "run `okg migrate --deployment <name> --apply`, which marks it "
                "inactive (its vectors stay for older pins)",
                set=name, set_id=set_id,
            ))
        elif not entry["active"]:
            findings.append(_finding(
                "vector_set_inactive",
                f"vector set {name!r} is in deployment.yaml but registered "
                "as inactive",
                "run `okg migrate --deployment <name> --apply`",
                set=name, set_id=set_id,
            ))
        if stale or stored is None:
            findings.append(_finding(
                "vector_set_stale",
                f"vector set {name!r}'s stored identity no longer hashes to "
                "its id under this code; it takes no new links",
                "run `okg migrate --deployment <name> --apply` and backfill "
                "the set it registers",
                set=name, set_id=set_id,
            ))
            continue
        drift = config_drift(conf, stored) if conf is not None else []
        entry["config_drift"] = drift
        if drift:
            findings.append(_finding(
                "vector_set_config_drift",
                f"deployment.yaml and the registered row of vector set "
                f"{name!r} disagree on: {', '.join(drift)}; documents and "
                "reads use the registered row",
                "run `okg migrate --deployment <name> --apply`",
                set=name, set_id=set_id, drift=drift,
            ))
        try:
            entry["index"] = inspect_set_index(conn, stored)
            if entry["index"]["status"] in _UNREADY_INDEX:
                findings.append(_finding(
                    "vector_set_index_unavailable",
                    f"vector set {name!r}'s HNSW index is "
                    f"{entry['index']['status']}, so semantic search over it "
                    "refuses",
                    "wait for a build to finish, or run `okg migrate "
                    "--deployment <name> --apply`, which builds each set's "
                    "index (pgvector 0.8 or newer)",
                    set=name, set_id=set_id, status=entry["index"]["status"],
                ))
            rebuild = entry["index"].get("rebuild")
            if rebuild is not None and rebuild.get("status") != "building":
                # An older index serves reads, so search stays up; but the
                # registered settings' index is missing or invalid and no
                # build of it runs (a killed `okg migrate` leaves it invalid,
                # holding disk and slowing writes).
                findings.append(_finding(
                    "vector_set_index_rebuild_pending",
                    f"vector set {name!r}'s index for its registered build "
                    f"settings ({rebuild.get('index_name')}) is "
                    f"{rebuild.get('status')} and not building; search reads "
                    f"the older index {entry['index'].get('index_name')}",
                    "run `okg db embed-index --deployment <name> --apply`, "
                    "which builds it and then drops the older index",
                    set=name, set_id=set_id, status=rebuild.get("status"),
                    index_name=rebuild.get("index_name"),
                    serving_index=entry["index"].get("index_name"),
                ))
            vectors, links = _row_counts(conn, set_id)
            entry["vectors"] = vectors
            entry["links"] = links
            entry["max_seq"] = int(row["next_seq"]) - 1
            if row["next_vector_seq"] is None:
                raise RuntimeError(
                    "the set has no vector counter row; run `okg migrate "
                    "--deployment <name> --apply`"
                )
            entry["max_vector_seq"] = int(row["next_vector_seq"]) - 1
            entry["spend"] = _spend(conn, deployment=deployment, set_id=set_id)
            entry["fill"] = _fill_state(conn, set_id)
        except Exception as exc:  # noqa: BLE001 - reported, never masked.
            findings.append(_finding(
                "vector_set_probe_failed",
                f"could not read vector set {name!r}: {type(exc).__name__}: {exc}",
                "check the database connection and rerun doctor",
                set=name, set_id=set_id,
            ))
            continue
        fill = entry["fill"]
        if entry["active"] and not stale and (
            fill["completed_at"] is None
            or fill["publishes_since"] > FILL_STALE_PUBLISHES
        ):
            never = fill["completed_at"] is None
            findings.append(_finding(
                "vector_set_fill_stale",
                (
                    f"no backfill of vector set {name!r} has completed, so "
                    "semantic search ranks the node columns"
                ) if never else (
                    f"vector set {name!r}'s last completed backfill is "
                    f"{fill['publishes_since']} publishes old (more than "
                    f"{FILL_STALE_PUBLISHES}); nodes written since may have "
                    "no vector"
                ),
                (
                    "a free backend is filled by the runtime worker after each "
                    "publish; for a paid one run `okg db embed-backfill "
                    f"--deployment <name> --set {name} --max-usd <usd>`, then "
                    "publish"
                ),
                set=name, set_id=set_id,
                reason="never_completed" if never else "publishes_since_fill",
                publishes_since=fill["publishes_since"],
            ))
        if entry["spend"]["answered_uncosted_calls"]:
            findings.append(_finding(
                "vector_set_spend_unknown",
                f"{entry['spend']['answered_uncosted_calls']} backfill call(s) "
                f"for vector set {name!r} were answered without a reported "
                "cost, so its spend is unknown and the next paid backfill "
                "refuses",
                "check the provider bill for those okg.llm_calls rows",
                set=name, set_id=set_id,
            ))
        if checkpoint is None:
            entry["coverage"] = {"checked": False, "reason": "nothing published"}
            continue
        generation_id = str(checkpoint.checkpoint_id)
        watermark = _watermark(conn, generation_id, set_id)
        if watermark is None:
            entry["coverage"] = {
                "checked": False, "generation_id": generation_id,
                "reason": "no watermark",
            }
            findings.append(_finding(
                "vector_set_watermark_missing",
                f"the published generation {generation_id} has no watermark "
                f"for vector set {name!r}, so a semantic read of it refuses",
                "publish once more so the generation records its watermark",
                set=name, set_id=set_id, generation_id=generation_id,
            ))
            continue
        try:
            coverage = set_coverage(
                conn, stored,
                generation_id=generation_id,
                catalog_version_id=checkpoint.metadata.get("catalog_version_id"),
                max_seq=watermark[0],
                max_vector_seq=watermark[1],
                exact=exact_coverage,
            )
        except Exception as exc:  # noqa: BLE001 - reported, never masked.
            entry["coverage"] = {
                "checked": False, "generation_id": generation_id,
                "reason": f"{type(exc).__name__}: {exc}",
            }
            findings.append(_finding(
                "vector_set_probe_failed",
                f"could not count coverage of vector set {name!r}: "
                f"{type(exc).__name__}: {exc}",
                "rerun doctor; on a large graph run `okg doctor --check "
                "vector-sets`, which has no statement timeout",
                set=name, set_id=set_id,
            ))
            continue
        unpublished = (
            max(entry["max_seq"] - watermark[0], 0)
            + max(entry["max_vector_seq"] - watermark[1], 0)
        )
        entry["coverage"] = {
            "checked": True,
            "generation_id": generation_id,
            "watermark": watermark[0],
            "vector_watermark": watermark[1],
            "unpublished_writes": unpublished,
            **coverage,
        }
        if unpublished:
            findings.append(_finding(
                "vector_set_unpublished_writes",
                f"vector set {name!r} took {unpublished} link or vector "
                f"number(s) after generation {generation_id} (writes of "
                "covered node rows, including an undo, restore or merge, "
                "a backfill, or a document-rule change); readers see "
                "those writes only after the next publish",
                "publish (`okg run --deployment <name> --once`)",
                set=name, set_id=set_id, rows=unpublished,
            ))
        if coverage["method"] == "sample":
            sampled_missing = coverage["sampled_nodes"] - coverage["sampled_embedded"]
            entry["coverage"]["note"] = (
                f"estimated from {coverage['sampled_nodes']} sampled nodes with "
                "non-blank text: "
                f"at most {coverage['missing_upper_95']} without a vector at 95% "
                "confidence; `okg doctor --check vector-sets` counts exactly"
            ) if coverage["sampled_nodes"] else (
                "the sample held no node with non-blank text, so coverage is "
                "unknown; "
                "`okg doctor --check vector-sets` counts exactly"
            )
            if sampled_missing:
                estimate = coverage["text_nodes"] - coverage["estimated_embedded_nodes"]
                findings.append(_finding(
                    "vector_set_coverage_incomplete",
                    f"about {estimate} of {coverage['text_nodes']} nodes with "
                    "non-blank text at "
                    f"generation {generation_id} have no vector in set {name!r} "
                    f"(estimated: {sampled_missing} of {coverage['sampled_nodes']} "
                    "sampled nodes); semantic search cannot find them. For the "
                    "exact count run `okg doctor --check vector-sets`",
                    f"run `okg db embed-backfill --deployment <name> --set {name} "
                    "--dry-run`, then the backfill, then publish",
                    set=name, set_id=set_id, nodes=estimate, estimated=True,
                    sampled_nodes=coverage["sampled_nodes"],
                    sampled_missing=sampled_missing,
                    missing_upper_95=coverage["missing_upper_95"],
                    text_nodes=coverage["text_nodes"],
                ))
            continue
        missing = coverage["text_nodes"] - coverage["embedded_nodes"]
        if missing:
            findings.append(_finding(
                "vector_set_coverage_incomplete",
                f"{missing} of {coverage['text_nodes']} nodes with non-blank "
                f"text at generation {generation_id} have no vector in set {name!r}; semantic "
                "search cannot find them",
                f"run `okg db embed-backfill --deployment <name> --set {name} "
                "--dry-run`, then the backfill, then publish",
                set=name, set_id=set_id, nodes=missing,
                text_nodes=coverage["text_nodes"],
            ))

    owned = {
        str((row["identity_json"] or {}).get("model_id"))
        for row in rows if row["active"]
    }
    try:
        if not any(row["active"] for row in rows):
            # No active set: semantic search reads exactly these columns, so
            # their vectors are in use, not left over. Not counted either.
            old_columns, old_findings = {
                "checked": False,
                "reason": "no active vector set; semantic search reads the "
                          "old node columns",
            }, []
        else:
            old_columns, old_findings = _old_columns(conn, checkpoint, owned)
    except Exception as exc:  # noqa: BLE001 - reported, never masked.
        old_columns = {"checked": False, "reason": f"{type(exc).__name__}: {exc}"}
        old_findings = [_finding(
            "vector_set_probe_failed",
            f"could not count old-column vectors: {type(exc).__name__}: {exc}",
            "rerun doctor; on a large graph run `okg doctor --check "
            "vector-sets`, which has no statement timeout",
        )]
    findings.extend(old_findings)
    try:
        worker_fill = _last_worker_fill(conn, deployment)
    except Exception as exc:  # noqa: BLE001 - reported, never masked.
        worker_fill = {"checked": False, "reason": f"{type(exc).__name__}: {exc}"}
        findings.append(_finding(
            "vector_set_probe_failed",
            f"could not read the runtime worker's last vector-set fill: "
            f"{type(exc).__name__}: {exc}",
            "check the database connection and rerun doctor",
        ))
    else:
        findings.extend(_fill_failure_findings(worker_fill))
    return {
        "ok": not findings,
        "tables_present": present,
        "sets": sets,
        "worker_fill": worker_fill,
        "inactive_sets": sorted(
            str(row["name"]) for row in rows
            if not row["active"] and str(row["set_id"]) not in configured
        ),
        "old_columns": old_columns,
        "findings": findings,
    }
