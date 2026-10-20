"""Merge safety gate (worktree-fork-merge-back: decider-boundary + fail-closed-two-way).

Two boundaries every merge into a protected branch must pass, enforced at the sole
merge entry `ChronosGraphStore.merge`:
  - fail-closed-two-way: no shared base between source and target -> refuse (chronos
    would otherwise silently two-way-overwrite).
  - decider-boundary: merge into default requires a verified HUMAN decider (named, an
    explicit accept, not fast_track) -- deny-by-default.

The pure-policy + fake-context tests pin the wiring deterministically (no DB); the
final test proves it against the REAL chronos backend (a genuine fork DOES share a
base, so the real path reaches the decider check and still refuses without one).
"""
from __future__ import annotations

import uuid

import pytest

from chronos_core.branching import RowDiff

from okg.substrate.merge_guard import (
    DeleteOptIn,
    MergeDecision,
    MergeRefused,
    assert_deletions_allowed,
    assert_merge_allowed,
    is_protected_target,
    partition_deletions,
    require_no_catalog_drift,
    require_valid_decider,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

ACCEPT = MergeDecision(decided_by="alice@example.edu", decision="accept", fast_track=False)


# --------------------------------------------------------------------------- #
# Pure policy
# --------------------------------------------------------------------------- #

def test_two_way_fallback_refused_for_any_target() -> None:
    # No shared base -> refuse regardless of target (protected or not), even with a
    # valid decider: a two-way merge overwrites rather than three-way merges.
    for target in ("default", "staging:x", "experiment:y"):
        with pytest.raises(MergeRefused) as ei:
            assert_merge_allowed("experiment:f", target, shared_base_exists=False, decision=ACCEPT)
        assert ei.value.data["reason"] == "two_way_fallback_no_shared_base"


def test_merge_into_default_requires_a_decider() -> None:
    with pytest.raises(MergeRefused) as ei:
        assert_merge_allowed("experiment:f", "default", shared_base_exists=True, decision=None)
    assert ei.value.data["reason"] == "missing_decider"


def test_fast_track_is_not_a_valid_decider() -> None:
    decision = MergeDecision(decided_by="alice@example.edu", decision="accept", fast_track=True)
    with pytest.raises(MergeRefused) as ei:
        require_valid_decider(decision, target="default")
    assert ei.value.data["reason"] == "fast_track_decider"


def test_non_accept_decision_refused() -> None:
    for verdict in ("reject", "abstain", "pending", "ACCEPTED?", ""):
        decision = MergeDecision(decided_by="alice@example.edu", decision=verdict)
        with pytest.raises(MergeRefused) as ei:
            require_valid_decider(decision, target="default")
        assert ei.value.data["reason"] == "non_accept_decision", verdict


def test_non_human_decider_refused() -> None:
    # System / agent / derived / service actors are not valid human deciders.
    for actor in (
        "_enricher:dedup", "derived_deterministic", "extracted_model", "system:cron",
        "service:publisher", "agent:claude", "agent_memory", "bot:ci", "_system", "",
        "   ", None,
    ):
        decision = MergeDecision(decided_by=actor, decision="accept")  # type: ignore[arg-type]
        with pytest.raises(MergeRefused) as ei:
            require_valid_decider(decision, target="default")
        assert ei.value.data["reason"] in {"non_human_decider", "missing_decider"}, actor


def test_valid_human_accept_passes_for_default() -> None:
    # A named human, explicit accept, not fast_track -> allowed (no raise).
    assert_merge_allowed("experiment:f", "default", shared_base_exists=True, decision=ACCEPT)
    # Case/whitespace tolerant on the decision verb.
    assert_merge_allowed(
        "experiment:f", "default", shared_base_exists=True,
        decision=MergeDecision(decided_by="bob@example.edu", decision=" Accept "),
    )


def test_non_protected_target_needs_no_decider() -> None:
    # Merging onto a working branch (staging/experiment) needs only a shared base.
    assert is_protected_target("default") is True
    assert is_protected_target("staging:m1") is False
    assert_merge_allowed("experiment:f", "staging:m1", shared_base_exists=True, decision=None)


# --------------------------------------------------------------------------- #
# Pure policy: deletion authority
# --------------------------------------------------------------------------- #

def _del(node_id: str) -> RowDiff:
    return RowDiff("okg.graph_nodes", {"node_id": node_id}, "deleted",
                   {"node_id": node_id}, None)


def _add(node_id: str) -> RowDiff:
    return RowDiff("okg.graph_nodes", {"node_id": node_id}, "added", None, {"node_id": node_id})


def test_deletion_of_default_origin_row_refused() -> None:
    with pytest.raises(MergeRefused) as ei:
        assert_deletions_allowed([_del("a"), _add("b")], source="experiment:f", target="default")
    assert ei.value.data["reason"] == "deletion_of_default_origin_row"
    assert ei.value.data["deleted_keys"] == [{"table": "okg.graph_nodes", "key": {"node_id": "a"}}]


def test_deletion_allowed_with_matching_opt_in() -> None:
    opt = DeleteOptIn("okg.graph_nodes", {"node_id": "a"})
    assert_deletions_allowed([_del("a")], source="f", target="default", delete_opt_ins=[opt])


def test_deletion_unguarded_on_working_branch() -> None:
    assert_deletions_allowed([_del("a")], source="f", target="staging:m1")  # no raise


def test_partition_deletions_buckets_unopted() -> None:
    opt = DeleteOptIn("okg.graph_nodes", {"node_id": "a"})
    allowed, quarantine = partition_deletions(
        [_del("a"), _del("b"), _add("c")], delete_opt_ins=[opt]
    )
    assert [d.key["node_id"] for d in quarantine] == ["b"]
    assert {d.key["node_id"] for d in allowed} == {"a", "c"}


# --------------------------------------------------------------------------- #
# Pure policy: catalog drift
# --------------------------------------------------------------------------- #

def test_catalog_drift_refused() -> None:
    with pytest.raises(MergeRefused) as ei:
        require_no_catalog_drift(5, 7, target="default")
    assert ei.value.data["reason"] == "catalog_drift_requires_rebase"
    assert ei.value.data["fork_base_cv"] == 5 and ei.value.data["default_current_cv"] == 7


def test_catalog_base_unrecorded_refused() -> None:
    with pytest.raises(MergeRefused) as ei:
        require_no_catalog_drift(None, 7, target="default")
    assert ei.value.data["reason"] == "catalog_base_unrecorded"


def test_catalog_no_drift_passes() -> None:
    require_no_catalog_drift(7, 7, target="default")  # no raise


def test_catalog_drift_skipped_on_working_branch() -> None:
    require_no_catalog_drift(5, 7, target="staging:m1")  # no raise


# --------------------------------------------------------------------------- #
# Integration via a fake chronos context (no DB): the ChronosGraphStore.merge wiring
# --------------------------------------------------------------------------- #

class _FakeBackend:
    name = "interval"

    def __init__(self, preview, *, raises: bool = False) -> None:
        self._preview = preview
        self._raises = raises

    def merge_preview(self, source, target):  # noqa: ANN001
        if self._raises:
            raise RuntimeError("boom")
        return self._preview


class _NoPreviewBackend:
    """A backend that exposes no merge_preview at all -> base unverifiable."""

    name = "log"


class _Box:
    def __init__(self, metadata) -> None:  # noqa: ANN001
        self.metadata = metadata


class _FakeDb:
    def __init__(self, current_cv) -> None:  # noqa: ANN001
        self._current_cv = current_cv

    def execute(self, _sql, *args, **kwargs):  # noqa: ANN001
        box = self

        class _Cur:
            def fetchone(self):  # noqa: ANN001
                return {"cv": box._current_cv}

        return _Cur()


class _FakeContext:
    # Defaults: an AUTHORITY-VALID fork base (base_branch=default; a published default
    # okg_generation) at fork_base_cv == default_current_cv == 1 -> catalog conformant, so
    # an otherwise-valid merge into default reaches merge_apply. branch_meta/checkpoint_meta
    # override the fork branch / base checkpoint metadata to exercise the authority gate.
    def __init__(
        self, backend, *, fork_base_cv=1, default_current_cv=1,
        branch_meta=None, checkpoint_meta=None,
    ) -> None:  # noqa: ANN001
        self._backend = backend
        self.db = _FakeDb(default_current_cv)
        self.merge_apply_calls: list[tuple[str, str]] = []
        self._branch_meta = (
            branch_meta if branch_meta is not None
            else {"base_branch": "default", "base_generation_id": "base-gen"}
        )
        self._checkpoint_meta = (
            checkpoint_meta if checkpoint_meta is not None
            else {
                "kind": "okg_generation", "status": "published", "branch": "default",
                "catalog_version_id": fork_base_cv,
            }
        )

    def get_branch(self, _name):  # noqa: ANN001
        return _Box(self._branch_meta)

    def get_checkpoint(self, _cp_id):  # noqa: ANN001
        return _Box(self._checkpoint_meta)

    def merge_apply(self, source, target):  # noqa: ANN001
        self.merge_apply_calls.append((source, target))
        return {"applied": 1, "source": source, "target": target}


class _FakePreview:
    def __init__(self, *, changes=(), conflicts=()) -> None:  # noqa: ANN001
        self.changes = list(changes)
        self.conflicts = list(conflicts)


def _store(backend, *, fork_base_cv=1, default_current_cv=1, branch_meta=None, checkpoint_meta=None):  # noqa: ANN001
    from okg.substrate.graph_store import ChronosGraphStore

    return ChronosGraphStore(
        _FakeContext(
            backend, fork_base_cv=fork_base_cv, default_current_cv=default_current_cv,
            branch_meta=branch_meta, checkpoint_meta=checkpoint_meta,
        ),
        owns_context=False,
    )


def test_resolve_fork_base_checkpoint_enforces_authority_gate() -> None:
    # The shared authority gate (worktree-fork-merge-back review fix): a fork's base
    # checkpoint is resolved ONLY if base_branch==default AND the checkpoint is a published
    # default okg_generation. A forged/invalid base pointer resolves to None (fail-closed).
    valid = _store(_FakeBackend(object()))
    assert valid.resolve_fork_base_checkpoint("experiment:f") is not None

    cases = [
        {"branch_meta": {"base_branch": "other", "base_generation_id": "g"}},
        {"branch_meta": {"base_branch": "default"}},  # no base_generation_id
        {"checkpoint_meta": {"kind": "snapshot", "status": "published",
                             "branch": "default", "catalog_version_id": 1}},
        {"checkpoint_meta": {"kind": "okg_generation", "status": "pending",
                             "branch": "default", "catalog_version_id": 1}},
        {"checkpoint_meta": {"kind": "okg_generation", "status": "published",
                             "branch": "experiment:x", "catalog_version_id": 1}},
    ]
    for kw in cases:
        store = _store(_FakeBackend(object()), **kw)
        assert store.resolve_fork_base_checkpoint("experiment:f") is None, kw


def test_store_merge_refuses_forged_base_generation_before_apply() -> None:
    # A drifted fork pointing base_generation_id at a NON-published checkpoint must be
    # refused at the catalog precondition (fail-closed), not silently resolved.
    store = _store(
        _FakeBackend(object()),
        checkpoint_meta={"kind": "okg_generation", "status": "pending",
                         "branch": "default", "catalog_version_id": 1},
    )
    with pytest.raises(MergeRefused) as ei:
        store.merge("experiment:f", "default", decision=ACCEPT)
    assert ei.value.data["reason"] == "catalog_base_unrecorded"
    assert store.context.merge_apply_calls == []


def test_store_merge_refuses_two_way_before_apply() -> None:
    # backend.merge_preview is None (no shared base) -> refuse, never call merge_apply.
    store = _store(_FakeBackend(None))
    with pytest.raises(MergeRefused) as ei:
        store.merge("experiment:f", "staging:x", decision=ACCEPT)
    assert ei.value.data["reason"] == "two_way_fallback_no_shared_base"
    assert store.context.merge_apply_calls == []


def test_store_merge_refuses_into_default_without_decider_before_apply() -> None:
    store = _store(_FakeBackend(object()))  # shared base exists
    with pytest.raises(MergeRefused) as ei:
        store.merge("experiment:f", "default")
    assert ei.value.data["reason"] == "missing_decider"
    assert store.context.merge_apply_calls == []


def test_store_merge_fail_closed_when_base_unverifiable() -> None:
    store = _store(_NoPreviewBackend())
    with pytest.raises(MergeRefused) as ei:
        store.merge("experiment:f", "default", decision=ACCEPT)
    assert ei.value.data["reason"] == "shared_base_unverifiable"
    assert store.context.merge_apply_calls == []


def test_store_merge_fail_closed_when_preview_raises() -> None:
    store = _store(_FakeBackend(None, raises=True))
    with pytest.raises(MergeRefused) as ei:
        store.merge("experiment:f", "default", decision=ACCEPT)
    assert ei.value.data["reason"] == "shared_base_check_failed"
    assert store.context.merge_apply_calls == []


def test_store_merge_allows_valid_decided_merge() -> None:
    # Shared base + valid human decider -> merge_apply is invoked.
    store = _store(_FakeBackend(object()))
    result = store.merge("experiment:f", "default", decision=ACCEPT)
    assert result["applied"] == 1
    assert store.context.merge_apply_calls == [("experiment:f", "default")]


def test_store_merge_allows_working_branch_without_decider() -> None:
    store = _store(_FakeBackend(object()))
    store.merge("experiment:f", "staging:m1")
    assert store.context.merge_apply_calls == [("experiment:f", "staging:m1")]


def test_store_merge_refuses_unopted_deletion_before_apply() -> None:
    # preview carries a deletion of a default-origin row -> refuse without an opt-in.
    store = _store(_FakeBackend(_FakePreview(changes=[_del("base")])))
    with pytest.raises(MergeRefused) as ei:
        store.merge("experiment:f", "default", decision=ACCEPT)
    assert ei.value.data["reason"] == "deletion_of_default_origin_row"
    assert store.context.merge_apply_calls == []


def test_store_merge_allows_deletion_with_opt_in() -> None:
    store = _store(_FakeBackend(_FakePreview(changes=[_del("base")])))
    store.merge(
        "experiment:f", "default", decision=ACCEPT,
        delete_opt_ins=[DeleteOptIn("okg.graph_nodes", {"node_id": "base"})],
    )
    assert store.context.merge_apply_calls == [("experiment:f", "default")]


def test_store_merge_refuses_catalog_drift_before_apply() -> None:
    # fork built on catalog 1, default now on catalog 2 -> rebase required.
    store = _store(_FakeBackend(object()), fork_base_cv=1, default_current_cv=2)
    with pytest.raises(MergeRefused) as ei:
        store.merge("experiment:f", "default", decision=ACCEPT)
    assert ei.value.data["reason"] == "catalog_drift_requires_rebase"
    assert store.context.merge_apply_calls == []


# --------------------------------------------------------------------------- #
# Real chronos backend: a genuine fork SHARES a base, so the real path reaches the
# decider check and still refuses without one (proves the gate against the runner,
# not just the fake wiring).
# --------------------------------------------------------------------------- #

@pytest.mark.postgres
def test_real_fork_merge_into_default_refused_without_decider(clean_runtime: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    sfx = uuid.uuid4().hex[:8]
    base = f"n:{sfx}:base"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(base, "person", {}, {"id": base}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        store.create_branch(fork, from_branch="default", metadata={"base_branch": "default"})

        before = list(store.checkout(branch="default").query(
            "SELECT node_id FROM okg.graph_nodes", {}
        ))
        # Real backend.merge_preview finds a shared base (fork is a child of default),
        # so the gate reaches the decider check -> refuses for lack of a decider, NOT
        # the two-way path.
        with pytest.raises(MergeRefused) as ei:
            store.merge(fork, "default")
        assert ei.value.data["reason"] == "missing_decider", ei.value.data

        after = list(store.checkout(branch="default").query(
            "SELECT node_id FROM okg.graph_nodes", {}
        ))
        assert len(after) == len(before), "default must be unchanged by a refused merge"


_HUMAN = MergeDecision(decided_by="alice@example.edu", decision="accept", fast_track=False)


def _publish_default(conn, dsn: str, *, base_nodes: list[str]) -> str:
    """Seed a catalog + base nodes on default and publish; return the generation id (a
    checkpoint whose metadata carries catalog_version_id, so the fork's base catalog
    resolves)."""
    from okg.substrate.consistency import TransformConsistencyGate
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import seed_catalog

    seed_catalog(conn, subtypes=["person"], narrowings=[])
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [NodeFact(n, "person", {}, {"id": n}, {"v": 1}) for n in base_nodes],
        source="default", strict_endpoints=True,
    )
    out = Publisher(dsn, "default", consistency_gates=[TransformConsistencyGate()], catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out
    return out["generation_id"]


@pytest.mark.postgres
def test_real_fork_merge_refuses_deletion_of_inherited_row(clean_runtime: str) -> None:
    # A fork retracts an INHERITED (default-origin) row, then merges into default with a
    # valid decider. The merge passes the decider + catalog checks and is refused at the
    # deletion gate (the three-way "deleted" change is a default-origin retraction).
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    sfx = uuid.uuid4().hex[:8]
    base = f"n:{sfx}:base"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        base_gen = _publish_default(conn, clean_runtime, base_nodes=[base])
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        # Retract the inherited node on the fork (op="R" -> a "deleted" three-way change).
        # The retraction is source-scoped (_delete_keys_for_source), so it must use the
        # inherited row's source ("default") to actually tombstone it on the fork.
        store.apply_facts(
            [NodeFact(base, "person", {}, {"id": base}, {"v": 2}, op="R")],
            branch=fork, source="default", strict_endpoints=True,
        )
        before = list(store.checkout(branch="default").query(
            "SELECT node_id FROM okg.graph_nodes", {}))
        with pytest.raises(MergeRefused) as ei:
            store.merge(fork, "default", decision=_HUMAN)
        assert ei.value.data["reason"] == "deletion_of_default_origin_row", ei.value.data
        after = list(store.checkout(branch="default").query(
            "SELECT node_id FROM okg.graph_nodes", {}))
        assert len(after) == len(before), "default unchanged by a refused deletion-merge"


@pytest.mark.postgres
def test_real_fork_merge_refuses_when_base_catalog_unresolvable(clean_runtime: str) -> None:
    # A fork created WITHOUT base_generation_id cannot prove catalog conformance; with a
    # valid decider it passes the decider check and is refused at the catalog precondition.
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    sfx = uuid.uuid4().hex[:8]
    base = f"n:{sfx}:base"
    fork = f"experiment:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _publish_default(conn, clean_runtime, base_nodes=[base])
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(fork, from_branch="default", metadata={"base_branch": "default"})
        store.apply_facts(
            [NodeFact(f"n:{sfx}:fk", "person", {}, {"id": f"n:{sfx}:fk"}, {"v": 1})],
            branch=fork, source="fork.src", strict_endpoints=True,
        )
        with pytest.raises(MergeRefused) as ei:
            store.merge(fork, "default", decision=_HUMAN)
        assert ei.value.data["reason"] == "catalog_base_unrecorded", ei.value.data
