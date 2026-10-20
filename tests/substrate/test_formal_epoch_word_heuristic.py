import importlib.util
import sys
from pathlib import Path

import pytest

CHECKER = (
    Path(__file__).resolve().parents[2]
    / "scripts/check_formal_dbos_epoch_supersession.py"
)
_spec = importlib.util.spec_from_file_location("formal_epoch_checker_2839", CHECKER)
checker = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = checker
_spec.loader.exec_module(checker)
PATH = "src/okg/substrate/publisher/synthetic.py"


def _categories(source: str) -> dict[str, list[str]]:
    return {
        e["entrypoint"]: e["categories"]
        for e in checker.discover_file_inventory(path=PATH, source=source)
    }


def test_publisher_class_reference_and_builtin_containers_are_not_authority():
    source = """
def query_dirty(conn, previous):
    seen = set()
    fields = {}
    fields.update(previous)
    statement = Publisher._dirty_nodes_since_previous_sql(previous)
    with conn.cursor() as cur:
        cur.execute(statement)
        for row in cur.fetchall():
            seen.add(row[0])
    return seen, fields
"""
    assert _categories(source) == {}


def test_kwargs_and_walrus_containers_are_not_writes():
    source = """
def build(conn, **kwargs):
    kwargs.update(lease_holder="x")
    if (pending := set()):
        pending.update(kwargs)
    return pending
"""
    assert _categories(source) == {}


@pytest.mark.parametrize(
    "body",
    [
        "store.publish_generation(conn)",
        "lease_store.acquire_lease(conn, holder)",
        "receipt_store.transition(receipt, 'done')",
        "cur.execute('UPDATE okg.publisher_lease SET holder = %s', (h,))",
    ],
)
def test_real_authority_call_is_still_an_authority_transition(body):
    source = f"""
def act(conn, store, lease_store, receipt_store, receipt, holder, cur, h):
    seen = set()
    publisher = Publisher.for_connection(conn)
    {body}
    return seen, publisher
"""
    assert "authority_transition" in _categories(source)["act"]


def test_camel_case_type_naming_the_effect_still_counts():
    source = """
def acquire(self, conn):
    if not self._try(conn):
        raise PublisherLeaseHeld("busy")
    self._release_dbos_publish_lock(conn)
"""
    assert "authority_transition" in _categories(source)["acquire"]


@pytest.mark.parametrize(
    "body",
    [
        "Publisher(conn).advance(gen)",
        "pub = Publisher(conn)\n    pub.commit_generation(gen)",
    ],
)
def test_write_on_the_publisher_actor_is_an_authority_transition(body):
    source = f"""
def go(conn, gen):
    {body}
"""
    assert "authority_transition" in _categories(source)["go"]


def test_update_on_a_rebound_name_still_counts():
    source = """
def advance(conn, state):
    checkpoint = {}
    checkpoint = state.checkpoint_row(conn)
    checkpoint.update(conn)
"""
    assert "progress_state" in _categories(source)["advance"]


# Each case binds a name so that it must NOT be treated as a local builtin
# container; the lease call on it therefore still counts as an effect.
REBIND_CASES = {
    "attribute_target": """
def go(conn):
    lease_store.cache = {}
    lease_store.acquire_lease(conn)
""",
    "subscript_target": """
def go(conn):
    publish_registry["pending"] = []
    publish_registry.publish_generation(conn)
""",
    "subscript_index_name": """
def go(conn):
    buckets[receipt_store] = set()
    receipt_store.transition(conn, "done")
""",
    "tuple_target": """
def go(conn, s):
    lease = {}
    lease, other = s.lease_store, 1
    lease.acquire_lease(conn)
""",
    "walrus_rebind": """
def go(conn, s):
    lease = {}
    if (lease := s.lease_store):
        lease.acquire_lease(conn)
""",
    "nested_function_binding": """
def go(conn):
    def helper():
        lease_store = {}
        return lease_store
    lease_store.release_lease(conn)
""",
    "nested_function_parameter": """
def go(conn):
    lease = {}
    def helper(lease):
        return lease
    lease.acquire_lease(conn)
""",
    "lambda_parameter": """
def go(conn, stores):
    lease = {}
    lease.acquire_lease(conn)
    return list(map(lambda lease: lease, stores))
""",
    "nested_class_binding": """
def go(conn):
    class Holder:
        lease = None
    lease = {}
    lease.acquire_lease(conn)
    return Holder
""",
    "import_alias": """
def go(conn):
    store = {}
    from okg.leases import lease_store as store
    store.acquire_lease(conn)
""",
    "except_name": """
def go(conn):
    lease = {}
    try:
        pass
    except Exception as lease:
        pass
    lease.release_lease(conn)
""",
    "match_capture": """
def go(conn, message):
    lease = {}
    match message:
        case {"store": lease}:
            pass
    lease.acquire_lease(conn)
""",
    "global_name": """
def go(conn):
    global lease_store
    lease_store = {}
    lease_store.acquire_lease(conn)
""",
    "nonlocal_name": """
def outer(conn):
    lease = None

    def go():
        nonlocal lease
        lease = {}
        lease.acquire_lease(conn)

    return go
""",
    "module_shadowed_builtin": """
from okg.leases import set_lease as set

def go(conn, lease_holder):
    set(conn, lease_holder)
""",
}


@pytest.mark.parametrize("case", sorted(REBIND_CASES))
def test_a_rebound_name_is_not_a_builtin_container(case):
    assert "authority_transition" in _categories(REBIND_CASES[case])["go"]


def test_alter_role_is_a_sql_mutation():
    source = """
def reconcile(conn, role, password):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(role, password)
        )
"""
    assert "sql_mutation" in _categories(source)["reconcile"]
