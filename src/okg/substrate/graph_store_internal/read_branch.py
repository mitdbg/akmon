"""Read, checkout, branch, and guarded-merge boundary for Chronos graph store."""
from __future__ import annotations

from typing import Any, Iterable

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store_internal.common import (
    DEFAULT_BRANCH,
    EDGE_TABLE,
    NODE_TABLE,
)
from okg.substrate.graph_store_internal.node_history import ChronosGraphNodeHistoryMixin
from okg.substrate.merge_guard import DeleteOptIn, MergeDecision


class CheckpointMetadataUnreadable(SubstrateError):
    """A checkpoint exists but PostgreSQL cannot parse its metadata as JSON.

    The projected checkpoint reads cast the stored metadata text to ``json``.
    Python's ``json.dumps`` can write bare ``NaN`` / ``Infinity`` tokens that
    the cast refuses; this error names that, so it never reads as a missing
    checkpoint.
    """


def is_invalid_json_cast(exc: BaseException) -> bool:
    """True when PostgreSQL refused text as JSON (SQLSTATE 22P02)."""
    seen: BaseException | None = exc
    while seen is not None:
        if getattr(seen, "sqlstate", None) == "22P02":
            return True
        seen = seen.__cause__ or seen.__context__
    return False


def checkpoint_metadata_unreadable(
    checkpoint_id: str, exc: BaseException,
) -> CheckpointMetadataUnreadable:
    return CheckpointMetadataUnreadable(
        "checkpoint metadata is not JSON PostgreSQL can parse",
        data={"checkpoint_id": checkpoint_id, "error": str(exc)[:300]},
        fix_hint=(
            "the metadata may carry a NaN or Infinity token; the writer must "
            "store such a value as null or a string"
        ),
    )


class GenerationCheckpointAbsent(KeyError):
    """No checkpoint exists for the generation a single-statement read named.

    A `KeyError` so callers of `checkout_generation`'s contract keep working,
    but its own type so a caller can tell "no such checkpoint" apart from a
    `KeyError` the statement itself raised.
    """


class ChronosGraphReadBranchMixin(ChronosGraphNodeHistoryMixin):
    """Branch checkout/read operations and branch-level graph actions.

    Node version history (``node_version_history``) is a read, so it comes in
    through this mixin and keeps the root store a thin facade.
    """

    context: Any

    def ensure_branch(
        self,
        branch: str,
        *,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        existing = {info.branch_id for info in self.context.list_branches()}
        if branch not in existing:
            try:
                self.create_branch(
                    branch,
                    from_branch=from_branch,
                    metadata=metadata,
                )
            except Exception as exc:
                if type(exc).__name__ != "BranchAlreadyExistsError":
                    try:
                        self.context.db.rollback()
                    except Exception:  # noqa: BLE001 - best-effort rollback
                        pass
                    if "duplicate key value" not in str(exc):
                        raise
                try:
                    self.context.db.rollback()
                except Exception:  # noqa: BLE001 - best-effort rollback
                    pass

    def checkout(self, branch: str = DEFAULT_BRANCH):
        return self.context.checkout(branch)

    def _generation_checkpoint_scope(
        self, generation_id: int | str
    ) -> dict[str, Any]:
        """The ``deployment_name`` and ``branch`` a generation checkpoint records.

        Opening a pinned graph needs these two metadata fields. It used to get
        them from `get_generation`, which runs ``SELECT *`` and parses the
        whole metadata document: 8.7 MB for okg-workspace's pinned generation,
        62 MB for the largest, most of it a list of node ids (issue #2546). On
        the interval backend over PostgreSQL this reads the two fields in the
        database and moves only them.

        Same contract as `get_generation`, which the other backends still use:
        a compacted checkpoint raises `GenerationCompacted` before and after
        the read, and a missing one raises `BranchNotFoundError` -- after a
        compaction re-check, so a checkpoint retired mid-read still reports as
        compacted. Values arrive as the Python objects ``json.loads`` gives,
        because the fields are projected as ``json``.
        """
        context = self.context
        db = getattr(context, "db", None)
        if (
            getattr(context, "backend_name", None) != "interval"
            or getattr(db, "dialect", None) != "postgres"
        ):
            return dict(self.get_generation(generation_id).metadata or {})

        from chronos_core.branching import BranchNotFoundError
        from okg.substrate.history_read_guard import raise_if_compacted

        checkpoint = self._generation_checkpoint_id(generation_id)
        raise_if_compacted(db, checkpoint)
        try:
            row = db.execute(
                """
                SELECT m.deployment_name, m.branch
                  FROM _chronos_branch_interval_checkpoints AS t,
                       LATERAL json_to_record(t.metadata::json)
                            AS m(deployment_name json, branch json)
                 WHERE t.checkpoint_id = ?
                """,
                (checkpoint,),
            ).fetchone()
            if row is None:
                raise BranchNotFoundError(f"checkpoint:{checkpoint}")
        except Exception as exc:
            if is_invalid_json_cast(exc):
                # The row exists; PostgreSQL cannot parse its metadata (a NaN
                # or Infinity token, say). Say so rather than "not found".
                raise checkpoint_metadata_unreadable(checkpoint, exc) from exc
            raise_if_compacted(db, checkpoint)
            raise
        raise_if_compacted(db, checkpoint)
        return {"deployment_name": row["deployment_name"], "branch": row["branch"]}

    def checkout_generation(
        self,
        generation_id: int | str,
        *,
        branch: str = DEFAULT_BRANCH,
    ):
        # `generation_id == 0` is the publisher's sentinel for the current
        # in-progress working generation: source facts have been applied to
        # the branch working head but are not yet checkpointed (the extractor,
        # lens-promotion and enricher stages all run pre-checkpoint and are
        # invoked with a hardcoded `generation_id=0`, per Enricher.enrich's
        # contract that emitted facts land in the same generation as the
        # source facts). No committed `generation:0` chronos checkpoint
        # exists, so read the working branch head -- the same
        # source-facts-applied state the publisher reads elsewhere via
        # `store.checkout(branch)`. A concrete checkpoint id (or a non-zero
        # ordinal that maps to a committed checkpoint) resolves as usual.
        if str(generation_id) == "0":
            return self.checkout(branch)
        checkpoint = self._generation_checkpoint_id(generation_id)
        try:
            metadata = self._generation_checkpoint_scope(generation_id)
            deployment_name = str(metadata.get("deployment_name") or "default")
            checkpoint_branch = str(metadata.get("branch") or branch)
            from okg.substrate.history_read_guard import (
                LeasedGenerationSession,
                acquire_generation_read_lease,
                release_generation_read_lease,
            )

            lease_backend = getattr(self, "_history_read_lease_backend", None)
            pin_name = acquire_generation_read_lease(
                self.context,
                checkpoint_id=checkpoint,
                deployment_name=deployment_name,
                branch=checkpoint_branch,
                lease_backend=lease_backend,
            )
            try:
                session = self.context.checkout_checkpoint(checkpoint)
            except Exception as checkout_exc:
                if pin_name is not None:
                    try:
                        release_generation_read_lease(
                            self.context,
                            checkpoint_id=checkpoint,
                            deployment_name=deployment_name,
                            branch=checkpoint_branch,
                            pin_name=pin_name,
                            lease_backend=lease_backend,
                        )
                    except Exception as cleanup_exc:
                        cleanup_exc.add_note(
                            "history read lease cleanup also failed after "
                            f"checkpoint checkout failed: {checkout_exc!r}"
                        )
                        raise cleanup_exc from checkout_exc
                raise
            if pin_name is None:
                return session
            return LeasedGenerationSession(
                context=self.context,
                session=session,
                checkpoint_id=checkpoint,
                deployment_name=deployment_name,
                branch=checkpoint_branch,
                pin_name=pin_name,
                lease_backend=lease_backend,
            )
        except Exception as exc:
            from okg.substrate.errors import GenerationCompacted

            if isinstance(exc, (GenerationCompacted, CheckpointMetadataUnreadable)):
                raise
            raise KeyError(
                f"Chronos checkpoint {checkpoint!r} not found for branch {branch!r}"
            ) from exc

    def query_generation_once(
        self,
        generation_id: int | str,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        branch: str = DEFAULT_BRANCH,
    ) -> list[dict[str, Any]]:
        """Run ONE statement against a generation's checkpoint, writing nothing.

        `checkout_generation` takes a read lease first: it deletes expired rows
        from `okg.history_generation_pins` and inserts a pin, so the session
        survives history compaction across many queries. A single statement
        does not need that. Compaction commits its tombstone -- and removes the
        checkpoint row -- in one transaction BEFORE it deletes any interval row
        (`history_compaction._commit_tombstone_barrier`). So:

        * a tombstone already present refuses the read up front
          (`get_generation` raises `GenerationCompacted`);
        * a tombstone committed after the statement's snapshot was taken is
          caught by the re-check below, and the rows are refused rather than
          served, because the data deletes may have raced the statement.

        If the re-check finds no tombstone, none existed when the statement
        ran, so no interval row it read had been deleted. The lease is left to
        callers that hold a checkout across several queries.

        Raises `GenerationCheckpointAbsent` (a `KeyError`, as
        `checkout_generation` raises) for a checkpoint that does not exist and
        `GenerationCompacted` for one that is, or has just been, compacted. A
        query error propagates unchanged.
        """
        from okg.substrate.errors import GenerationCompacted
        from okg.substrate.history_read_guard import raise_if_compacted

        if str(generation_id) == "0":
            # The working branch head: no checkpoint, so no lease either way.
            return self.checkout(branch).query(sql, params)
        checkpoint = self._generation_checkpoint_id(generation_id)
        try:
            deployment_name = str(
                self._generation_checkpoint_scope(generation_id).get(
                    "deployment_name"
                )
                or "default"
            )
            session = self.context.checkout_checkpoint(checkpoint)
        except (GenerationCompacted, CheckpointMetadataUnreadable):
            raise
        except Exception as exc:
            raise GenerationCheckpointAbsent(
                f"Chronos checkpoint {checkpoint!r} not found for branch {branch!r}"
            ) from exc
        rows = session.query(sql, params)
        raise_if_compacted(
            self.context.db, checkpoint, deployment_name=deployment_name
        )
        return rows

    def has_generation_checkpoint(self, generation_id: int | str) -> bool:
        from okg.substrate.history_read_guard import raise_if_compacted

        checkpoint = self._generation_checkpoint_id(generation_id)
        raise_if_compacted(self.context.db, checkpoint)
        try:
            self.context.get_checkpoint(checkpoint)
        except Exception:  # noqa: BLE001 - any lookup failure means absent
            raise_if_compacted(self.context.db, checkpoint)
            return False
        raise_if_compacted(self.context.db, checkpoint)
        return True

    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        branch: str = DEFAULT_BRANCH,
    ) -> list[dict[str, Any]]:
        return self.checkout(branch).query(sql, params)

    def prune_edges(
        self,
        edge_ids: Iterable[int | str],
        *,
        branch: str = DEFAULT_BRANCH,
    ) -> int:
        ids = list(dict.fromkeys(int(e) for e in edge_ids))
        if not ids:
            return 0
        session = self.checkout(branch)
        # PostgreSQL's batch splice path loads an ON COMMIT DROP temp table
        # across several statements. Keep the fast delete and any inherited
        # tombstones in one explicit transaction so autocommit cannot drop the
        # temp table between CREATE and COPY/INSERT.
        with session.transaction():
            self._lock_edge_ranking_shared()  # type: ignore[attr-defined]
            self._lock_branch_write(branch)
            # A pruned dangling edge takes every producer's declaration of it
            # with it (item 92a), so no later write hands the row back.
            self._purge_edge_declarations(session, ids)  # type: ignore[attr-defined]
            backend = getattr(self.context, "_backend", None)
            if backend is not None and hasattr(backend, "_current_segment"):
                meta = backend._require_table(EDGE_TABLE)
                segment = backend._current_segment(branch)
                db = backend.db
                removed_from_working_segment: set[int] = set()
                for start in range(0, len(ids), 1000):
                    chunk = ids[start : start + 1000]
                    placeholders = ", ".join("?" for _ in chunk)
                    rows = db.execute(
                        f"DELETE FROM {meta.physical_name} "
                        f"WHERE deleted = FALSE AND live_lo >= ? "
                        f"AND edge_id IN ({placeholders}) RETURNING edge_id",
                        [segment.live_lo, *chunk],
                    ).fetchall()
                    removed_from_working_segment.update(
                        int(row["edge_id"]) for row in rows
                    )

                # The fast physical delete above is correct only for rows first
                # written in the still-unpublished mutable segment. A full gate
                # evaluation (for example after a catalog-version change) can also
                # find dangling edges inherited from an older published checkpoint.
                # Those need a Chronos tombstone in the current segment; merely
                # restricting DELETE to ``live_lo >= current.live_lo`` used to leave
                # every inherited offender visible and blocked the publish forever.
                inherited = [
                    edge_id for edge_id in ids
                    if edge_id not in removed_from_working_segment
                ]
                if inherited:
                    session.delete_keys(
                        EDGE_TABLE,
                        [{"edge_id": edge_id} for edge_id in inherited],
                    )
                return len(removed_from_working_segment) + len(inherited)
            session.delete_keys(EDGE_TABLE, [{"edge_id": e} for e in ids])
            return len(ids)

    def create_branch(
        self,
        branch: str,
        *,
        from_branch: str = DEFAULT_BRANCH,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        from okg.substrate.history_compaction import _history_protection_lock

        raw = getattr(self.context, "conn", None)
        if raw is None:
            self.context.create_branch(branch, from_branch=from_branch, metadata=metadata)
            return
        with _history_protection_lock(raw):
            self.context.create_branch(branch, from_branch=from_branch, metadata=metadata)

    def delete_branch(self, branch: str) -> None:
        self.context.delete_branch(branch)

    def diff(self, left: str, right: str):
        return self.context.diff(left, right)

    def merge(
        self,
        source: str,
        target: str,
        *,
        decision: MergeDecision | None = None,
        delete_opt_ins: Iterable[DeleteOptIn] = (),
    ):
        from okg.substrate.merge_guard import (
            MergeRefused,
            assert_deletions_allowed,
            assert_merge_allowed,
            is_protected_target,
            require_no_catalog_drift,
        )

        backend = getattr(self.context, "_backend", None)
        preview_fn = getattr(backend, "merge_preview", None)
        if preview_fn is None:
            raise MergeRefused(
                f"cannot verify a shared base for merge {source!r} -> {target!r}; refusing",
                data={"source": source, "target": target, "reason": "shared_base_unverifiable"},
            )
        try:
            preview = preview_fn(source, target)
        except Exception as exc:  # noqa: BLE001 - fail closed on any preview error
            raise MergeRefused(
                f"shared-base check failed for merge {source!r} -> {target!r}; refusing",
                data={"source": source, "target": target, "reason": "shared_base_check_failed"},
            ) from exc

        assert_merge_allowed(
            source, target, shared_base_exists=preview is not None, decision=decision
        )
        if is_protected_target(target):
            require_no_catalog_drift(
                self._resolve_fork_base_catalog_version(source),
                self._current_applied_catalog_version(),
                target=target,
            )
            changes = list(getattr(preview, "changes", None) or [])
            changes += list(getattr(preview, "conflicts", None) or [])
            assert_deletions_allowed(
                changes, source=source, target=target, delete_opt_ins=delete_opt_ins
            )
        return self._merge_apply_settling_ranked_edges(source, target)

    def _merge_apply_settling_ranked_edges(self, source: str, target: str):
        """Apply a merge and store the winner of every ranked edge it touched.

        A row-wise merge can combine one side's stored edge row with the other
        side's declarer rows (item 92a: the fork hands a row to code-structure
        v1 while the target rewrites code-structure's declaration to v2), so
        the stored row no longer matches any declaration. The merge's row
        changes and the winners of the edges whose declarations it changed are
        written in one transaction on the target, under the branch write fence.
        This is Chronos's own merge application (preview, policy, apply), run
        inside that transaction. A context without that application (a test
        double, or Chronos's Orpheus backend, which okg does not use) keeps
        the plain merge.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
        )

        backend = getattr(self.context, "_backend", None)
        apply_changes = getattr(self.context, "_apply_merge_changes", None)
        if not callable(apply_changes) or getattr(backend, "name", "") == "orpheus":
            return self.context.merge_apply(source, target)
        from chronos_core.branching._common import (
            MergeResult,
            _resolve_merge_changes,
        )

        session = self.checkout(target)
        with session.transaction():
            self._lock_edge_ranking_shared()  # type: ignore[attr-defined]
            self._lock_branch_write(target)  # type: ignore[attr-defined]
            preview = self.context.merge_preview(source, target)
            changes = _resolve_merge_changes(
                preview, None, None, backend=getattr(backend, "name", ""),
            )
            applied = apply_changes(session, changes)
            if any(
                change.table == NODE_TABLE and change.after is not None
                for change in changes
            ):
                # PACT openrouter-embeddings-hnsw: the merge copies node rows
                # past the graph writer's document links; content written on
                # the source branch may never have been embedded. One link
                # number per active vector set, in this transaction, keeps
                # the worker's fill from carrying its last completion past
                # them. A delete leaves no row to embed.
                from okg.substrate.embeddings.store import mark_node_rows_restored

                mark_node_rows_restored(self.context.db)
            touched = {
                int(change.key["edge_id"])
                for change in changes
                if change.table == EDGE_DECLARERS_TABLE
                and "edge_id" in (change.key or {})
            }
            self._settle_ranked_edges(session, touched)  # type: ignore[attr-defined]
        return MergeResult(source=source, target=target, applied=applied)

    def _current_applied_catalog_version(self) -> int | None:
        try:
            row = self.context.db.execute(
                "SELECT max(catalog_version_id) AS cv "
                "FROM okg.catalog_versions WHERE status = 'applied'"
            ).fetchone()
        except Exception:  # noqa: BLE001 - unreadable catalog is unresolved
            return None
        cv = (row or {}).get("cv") if hasattr(row, "get") else (row[0] if row else None)
        return int(cv) if cv is not None else None

    def resolve_fork_base_checkpoint(
        self, fork_branch: str, *, expected_deployment: str | None = None
    ):
        try:
            info = self.context.get_branch(fork_branch)
            fork_md = dict(getattr(info, "metadata", None) or {})
        except Exception:  # noqa: BLE001 - branch lookup best-effort
            return None
        base_gen_id = fork_md.get("base_generation_id")
        if not base_gen_id or fork_md.get("base_branch") != "default":
            return None
        try:
            candidate = self.get_generation(base_gen_id)
        except Exception:  # noqa: BLE001 - missing/unresolvable base
            return None
        cand_md = (
            dict(getattr(candidate, "metadata", None) or {})
            if candidate is not None else {}
        )
        cand_branch = cand_md.get("branch")
        if (
            candidate is not None
            and cand_md.get("kind") == "okg_generation"
            and (
                expected_deployment is None
                or cand_md.get("deployment_name") == expected_deployment
            )
            and cand_md.get("status") == "published"
            and (cand_branch is None or str(cand_branch) == "default")
        ):
            return candidate
        return None

    def _resolve_fork_base_catalog_version(self, source: str) -> int | None:
        checkpoint = self.resolve_fork_base_checkpoint(source)
        if checkpoint is None:
            return None
        cv = dict(getattr(checkpoint, "metadata", None) or {}).get("catalog_version_id")
        return int(cv) if cv is not None else None
