from __future__ import annotations

import importlib.util
import itertools
import sys
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "scripts/check_formal_dbos_epoch_supersession.py"
MODEL = ROOT / "formal/dbos_epoch_supersession/DbosEpochSupersession.tla"


def _checker():
    spec = importlib.util.spec_from_file_location("dbos_epoch_checker", CHECKER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _current_binding() -> dict[str, object]:
    return {
        "lifecycle_activation_epoch": 2,
        "activation_receipt_digest": "activation-v2",
        "database_writer_epoch": 3,
        "dbos_application_version": "application-v2",
        "worktree_write_capability_epoch": 2,
        "binding_epoch": 2,
        "published_generation": 11,
    }


def _operation(**overrides: object) -> dict[str, object]:
    value = {
        **_current_binding(),
        "operation_id": "operation-a",
        "operation_digest": "sha256:operation-a",
        "equivalence_class": "source:alpha",
        "expected_parent": 11,
        "capsule_digest": "sha256:capsule-a",
        "body_running": True,
        "superseded": False,
    }
    value.pop("published_generation")
    value.update(overrides)
    return value


def test_epoch_binding_registry_is_complete() -> None:
    checker = _checker()
    checker.validate_boundary_registry()
    assert set(checker.BOUNDARY_REGISTRY) == {
        "enqueue",
        "dequeue",
        "body_entry",
        "schedule",
        "poller",
        "compactor",
        "lease",
        "mutation",
        "receipt_transition",
        "publish",
    }
    text = MODEL.read_text(encoding="utf-8")
    assert "CurrentApplication" in text
    assert "CurrentEpoch" in text
    assert "CurrentReceipt" in text
    assert "EpochBindingComplete ==" in text
    assert set(checker.REQUIRED_BINDING_FIELDS) == {
        "lifecycle_activation_epoch",
        "activation_receipt_digest",
        "database_writer_epoch",
        "dbos_application_version",
        "worktree_write_capability_epoch",
        "binding_epoch",
        "operation_id",
        "operation_digest",
        "equivalence_class",
        "expected_parent",
        "capsule_digest",
    }
    assert checker.BOUNDARY_REGISTRY_PATH.is_file()
    assert set(checker.BOUNDARY_REGISTRY_DOCUMENT["tla_mapping"]) == set(
        checker.REQUIRED_BINDING_FIELDS
    )
    serialized = checker.BOUNDARY_REGISTRY_PATH.read_text(encoding="utf-8")
    for alias in (
        "lifecycle_epoch",
        "activation_receipt",
        "worktree_capability",
        "branch_binding_epoch",
    ):
        assert f"- {alias}\n" not in serialized
        assert f"  {alias}:" not in serialized
    for boundary in checker.BOUNDARY_REGISTRY_DOCUMENT["boundaries"]:
        assert boundary["binding_profile"] == "full"
        assert boundary["equivalence_fields"] == [
            "deployment",
            "operation_kind",
            "logical_target",
        ]
        assert boundary["expected_parent_field"] == "expected_parent"


def test_superseded_body_cannot_mutate() -> None:
    checker = _checker()
    current = _current_binding()
    assert checker.mutation_admitted(_operation(), current)
    for field in checker.AUTHORITY_BINDING_FIELDS:
        stale = _operation()
        stale[field] = f"stale-{field}"
        assert not checker.mutation_admitted(stale, current)
    for field in checker.REQUIRED_BINDING_FIELDS:
        incomplete = _operation()
        incomplete.pop(field)
        assert not checker.mutation_admitted(incomplete, current)
    assert not checker.mutation_admitted(_operation(expected_parent=10), current)
    for running, superseded in itertools.product((False, True), repeat=2):
        admitted = checker.mutation_admitted(
            _operation(body_running=running, superseded=superseded), current
        )
        assert admitted is (running and not superseded)


def test_single_current_recovery_per_equivalence_class() -> None:
    checker = _checker()
    current = _current_binding()
    rows = [
        _operation(
            operation_id="old",
            equivalence_class="source:alpha",
            dbos_application_version="application-v1",
            lifecycle_activation_epoch=1,
            activation_receipt_digest="activation-v1",
        ),
        _operation(operation_id="current-b", equivalence_class="source:alpha"),
        _operation(operation_id="current-a", equivalence_class="source:alpha"),
        _operation(operation_id="current-c", equivalence_class="source:beta"),
    ]
    selected = checker.select_current_recoveries(rows, current)
    assert set(selected) == {"source:alpha", "source:beta"}
    assert selected["source:alpha"]["operation_id"] == "current-a"
    assert selected["source:beta"]["operation_id"] == "current-c"


def test_postgres_transaction_cannot_commit_across_epoch_advance() -> None:
    checker = _checker()
    current = _current_binding()
    transaction = {"state": "open", **current}
    transaction["dbos_application_version"] = "application-v1"
    transaction["lifecycle_activation_epoch"] = 1
    transaction["activation_receipt_digest"] = "activation-v1"
    assert checker.postgres_transaction_commit_admitted(transaction, current) is False
    exact = {"state": "open", **current}
    assert checker.postgres_transaction_commit_admitted(exact, current) is True
    for field in checker.AUTHORITY_BINDING_FIELDS:
        stale = dict(exact)
        stale[field] = f"stale-{field}"
        assert checker.postgres_transaction_commit_admitted(stale, current) is False

    text = MODEL.read_text(encoding="utf-8")
    assert "AdvanceActivationEpoch ==" in text
    assert "RefuseStaleTransactionCommit ==" in text
    assert "NoStaleTransactionCommitAcrossEpochAdvance ==" in text


def test_direct_producer_registry_is_exhaustive() -> None:
    checker = _checker()
    assert checker.discover_direct_producer_paths() == set(
        checker.DIRECT_PRODUCER_REGISTRY
    )
    assert all(checker.DIRECT_PRODUCER_REGISTRY.values())


def test_unregistered_producer_or_mutation_fails_inventory_validation() -> None:
    checker = _checker()
    registered = checker.discover_mutation_inventory()
    synthetic = checker.discover_file_inventory(
        path="src/okg/substrate/runtime/unregistered_boundary.py",
        source="""
def unregistered_enqueue(client, options):
    return client.enqueue(options)

def unregistered_sql(cur):
    cur.execute("UPDATE workflow_state SET status = 'running'")

def unregistered_chronos(writer):
    writer.apply_chronos_update()

def unregistered_nomos(writer):
    writer.persist_nomos_decision()
""",
    )
    assert {category for entry in synthetic for category in entry["categories"]} >= {
        "dbos_direct_producer",
        "sql_mutation",
        "chronos_graph_write",
        "nomos_apply",
    }
    with pytest.raises(ValueError, match="unregistered"):
        checker.validate_discovered_inventory(
            [*registered, *synthetic],
            registered,
        )


def test_read_only_helpers_are_not_classified_as_mutations() -> None:
    checker = _checker()
    discovered = {
        (entry["path"], entry["entrypoint"])
        for entry in checker.discover_mutation_inventory()
    }
    assert (
        "src/okg/substrate/deployment_upgrade.py",
        "_directory_inode_headroom",
    ) not in discovered
    synthetic = checker.discover_file_inventory(
        path="src/okg/substrate/runtime/read_only_probe.py",
        source="""
def read_only_probe(cur):
    cur.execute("SELECT status, checkpoint FROM workflow_state")
    return cur.fetchone()
""",
    )
    assert synthetic == []


VOCABULARY_PROSE = (
    "chronos nomos cursor watermark checkpoint session lease lock receipt "
    "publish enqueue start_workflow apply write persist INSERT INTO t "
    "UPDATE t DELETE FROM t CREATE TABLE t TRUNCATE"
)


def test_comment_and_docstring_vocabulary_does_not_classify() -> None:
    """Prose carries no boundary signal, however complete its vocabulary."""
    checker = _checker()
    source = f'''
def prose_only(value):
    """{VOCABULARY_PROSE}"""
    # {VOCABULARY_PROSE}
    return len(value)  # {VOCABULARY_PROSE}
'''
    assert (
        checker.discover_file_inventory(
            path="src/okg/substrate/runtime/prose_only.py", source=source
        )
        == []
    )


def test_prose_vocabulary_does_not_add_categories_to_real_boundary() -> None:
    checker = _checker()
    bare = """
def chronos_boundary(store, cur):
    cur.execute("INSERT INTO node_facts (id) VALUES (%s)")
    store.chronos_writer.apply_batch()
"""
    documented = f'''
def chronos_boundary(store, cur):
    """{VOCABULARY_PROSE}"""
    # {VOCABULARY_PROSE}
    cur.execute("INSERT INTO node_facts (id) VALUES (%s)")
    store.chronos_writer.apply_batch()
'''
    path = "src/okg/substrate/runtime/chronos_boundary.py"
    plain = checker.discover_file_inventory(path=path, source=bare)
    assert [entry["categories"] for entry in plain] == [
        ["chronos_graph_write", "sql_mutation"]
    ]
    assert checker.discover_file_inventory(path=path, source=documented) == plain


def test_code_level_vocabulary_still_classifies() -> None:
    """Identifiers, decorators and SQL literals keep their categories."""
    checker = _checker()
    entries = checker.discover_file_inventory(
        path="src/okg/substrate/runtime/coded_boundary.py",
        source="""
def enqueue_boundary(client, options):
    return client.enqueue(options)


@dbos.workflow()
def decorated_boundary():
    return None


def nomos_boundary(engine):
    engine.apply_nomos_decision()


def checkpoint_boundary(cur):
    cur.execute("UPDATE source_cursor SET checkpoint = %s")


def lease_boundary(conn):
    conn.acquire_publish_lease()


def annotated_boundary(store) -> ChronosBatch:
    store.apply_batch()


def imported_boundary(store):
    from okg.substrate.chronos import helper

    store.apply_batch(helper)


def literal_boundary(cur, row):
    cur.execute(statement)
    row.update({"receipt_digest": "published"})


def executed_lock_boundary(cur):
    cur.execute("LOCK TABLE dbos.workflow_status IN SHARE ROW EXCLUSIVE MODE")
    cur.connection.commit()
""",
    )
    categories = {entry["entrypoint"]: entry["categories"] for entry in entries}
    assert categories["enqueue_boundary"] == ["dbos_direct_producer"]
    assert categories["decorated_boundary"] == ["dbos_decorated_entry"]
    assert categories["nomos_boundary"] == ["nomos_apply"]
    assert categories["checkpoint_boundary"] == [
        "progress_state",
        "sql_mutation",
    ]
    assert categories["lease_boundary"] == ["authority_transition"]
    # A return annotation, an import path, a dict-key or status literal, and an
    # executed non-DML lock are all code, and each names its subsystem only
    # there. The old raw-source scan caught them incidentally; the code scan
    # has to reach them deliberately.
    assert categories["annotated_boundary"] == ["chronos_graph_write"]
    assert categories["imported_boundary"] == ["chronos_graph_write"]
    assert categories["executed_lock_boundary"] == ["authority_transition"]
    # A bare word in a dict key or a status value is NOT treated as code: it
    # reads the same as prose and classified read-only helpers. Such a boundary
    # is registered explicitly instead.
    assert "literal_boundary" not in categories


def test_sql_literals_are_still_read_but_other_strings_are_not() -> None:
    checker = _checker()
    assert (
        checker.discover_file_inventory(
            path="src/okg/substrate/runtime/logged.py",
            source="""
def logged(log, store):
    log.info("publishing the chronos checkpoint receipt to nomos")
    store.write_blob()
""",
        )
        == []
    )
    sql = checker.discover_file_inventory(
        path="src/okg/substrate/runtime/sql_literal.py",
        source="""
def sql_literal(cur):
    cur.execute("DELETE FROM workflow_state WHERE id = %s")
""",
    )
    assert [entry["categories"] for entry in sql] == [["sql_mutation"]]


def test_boundary_inventory_is_drift_free_under_the_code_scan() -> None:
    """The committed inventory equals what the code-only scan discovers."""
    checker = _checker()
    inventory = yaml.safe_load(
        (ROOT / "formal/dbos_epoch_supersession/boundary-inventory.yaml").read_text(
            encoding="utf-8"
        )
    )
    checker.validate_discovered_inventory(
        checker.discover_mutation_inventory(), inventory["entries"]
    )


def test_shared_and_exclusive_epoch_fence_is_real() -> None:
    checker = _checker()
    binding = {
        "deployment": "okg-workspace",
        "lifecycle_activation_epoch": 2,
        "activation_receipt_digest": "activation-v2",
    }
    fence = checker.EpochFence(binding=tuple(sorted(binding.items())))
    shared = checker.acquire_shared_fence(fence, holder="workflow-a", binding=binding)
    assert shared is not None
    assert (
        checker.acquire_shared_fence(shared, holder="workflow-b", binding=binding)
        is not None
    )
    assert (
        checker.acquire_exclusive_fence(
            shared, owner="activation-controller", binding=binding
        )
        is None
    )
    exclusive = checker.acquire_exclusive_fence(
        fence, owner="activation-controller", binding=binding
    )
    assert exclusive is not None
    assert (
        checker.acquire_shared_fence(exclusive, holder="workflow-a", binding=binding)
        is None
    )
    stale = {**binding, "lifecycle_activation_epoch": 1}
    assert checker.acquire_shared_fence(fence, holder="stale", binding=stale) is None


def _explicit_entry(**overrides: object) -> dict[str, object]:
    entry = {
        "path": "src/okg/substrate/runtime/promotion_fence.py",
        "entrypoint": "acquire_promotion_fence",
        "categories": ["authority_transition"],
        "boundaries": ["lease", "publish", "receipt_transition"],
        "disposition": "implementation_required",
        "owner_change": "fence-local-okg-workflow-versions",
        "reason": "claims durable promotion ownership under a deployment lock",
        "explicit": True,
    }
    entry.update(overrides)
    return entry


def test_explicit_entry_keeps_a_prose_only_boundary_enrolled() -> None:
    """A real boundary the code scan cannot see stays enrolled deliberately."""
    checker = _checker()
    # Discovery does not produce this claim, so without the explicit entry the
    # equality gate would treat it as stale drift.
    checker.validate_discovered_inventory([], [_explicit_entry()])


def test_explicit_entry_without_a_reason_fails() -> None:
    checker = _checker()
    for empty in ("", "   "):
        with pytest.raises(ValueError, match="lacks a reason"):
            checker.validate_discovered_inventory([], [_explicit_entry(reason=empty)])


def test_explicit_entry_for_a_missing_function_fails() -> None:
    checker = _checker()
    with pytest.raises(ValueError, match="no such function"):
        checker.validate_discovered_inventory(
            [], [_explicit_entry(entrypoint="no_such_boundary_function")]
        )
    with pytest.raises(ValueError, match="no such function"):
        checker.validate_discovered_inventory(
            [], [_explicit_entry(path="src/okg/substrate/runtime/no_such_file.py")]
        )


def test_explicit_entry_stands_in_place_of_discovery() -> None:
    """An explicit entry suppresses discovery for its own function only."""
    checker = _checker()
    discovered = [
        {
            "path": "src/okg/substrate/runtime/promotion_fence.py",
            "entrypoint": "acquire_promotion_fence",
            "categories": ["progress_state"],
            "boundaries": ["mutation"],
            "disposition": "implementation_required",
            "owner_change": "fence-local-okg-workflow-versions",
            "reason": "discovered",
        }
    ]
    checker.validate_discovered_inventory(discovered, [_explicit_entry()])
    with pytest.raises(ValueError, match="boundaries do not match categories"):
        checker.validate_discovered_inventory(
            [], [_explicit_entry(boundaries=["mutation"])]
        )
