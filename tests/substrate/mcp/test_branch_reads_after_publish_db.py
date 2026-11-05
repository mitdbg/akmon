"""Experiment-branch reads after two branch publishes (issue #3772).

AKMON's optimizer trials each change on an experiment branch and measures it
by reading that branch through ``okg mcp-serve --branch`` (``build_server(...,
branch_name=<branch>)``). Issue #3772 reported that, after a branch publish
followed by attribute observations and a second branch publish, every read
pinned to the branch refused (``incomplete interval stab rows``), and the
repair command could not target a branch.

The storage cutover removed that read path (f832fcf5884d) and the repair
command (413b5e1c4cf0). Reads now take the Chronos rows at the pin plus the
generation's overlay rows, and refuse only when the pinned generation has no
overlay marker. These tests replay the issue's sequence on a real database
and read through the branch-pinned server and the default server:

* both branch publishes are ``published`` and each writes its overlay marker;
* the branch server pins the second branch generation and serves nonzero
  rows, including the attribute value observed between the publishes and the
  graph attribute changed in the second publish;
* the default server's replies are identical before the branch existed and
  after both branch publishes, and never show the branch values;
* a branch's first publish carries forward the overlays its base generation
  serves (the silent loss the review of this issue found: the first branch
  publish carried overlays forward from nothing);
* an unmarked branch pin is refused with a hint that names the branch.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")
pytest.importorskip("mcp")

from okg.substrate.attributes import (  # noqa: E402
    OVERLAY_GENERATION_MARKER_TABLE,
    record_attribute_observation,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_FENCE = re.compile(r"⟦/?UNTRUSTED_DATA[^⟧]*⟧\n?")


def _plain(value: Any) -> Any:
    """A reply value without the untrusted-data fence around stored text."""
    if isinstance(value, str):
        return _FENCE.sub("", value).strip()
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return value


class _World:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        s = uuid.uuid4().hex[:8]
        self.branch = f"experiment:b3772-{s}"
        self.a = f"doc:a:{s}"
        self.b = f"doc:b:{s}"
        self.c = f"doc:c:{s}"
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog(conn, subtypes=[
                SubtypeSpec(name="doc", metadata={
                    "filterable_attrs": {"owner": "text", "rank": "num"},
                }),
            ])
        self.identity = bind_test_catalog_writer(dsn, "default")
        self.apply("default", [
            (self.a, {"owner": "alice", "rank": 1}),
            (self.b, {"owner": "bob", "rank": 2}),
        ])
        self.default_gen = self.publish("default")

    def apply(self, branch: str, nodes: list[tuple[str, dict[str, Any]]],
              *, revision: int = 1) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [NodeFact(node, "doc", attrs, {"id": node}, {"v": revision})
                 for node, attrs in nodes],
                branch=branch, source="default", strict_endpoints=True,
            )

    def publish(self, branch: str) -> str:
        out = Publisher(
            self.dsn, "default", branch_name=branch,
            catalog_identity=self.identity,
        ).cycle()
        assert out["status"] == "published", out
        assert not out.get("noop"), out
        return str(out["generation_id"])

    def create_branch(self) -> None:
        """The branch metadata `okg branch create` records
        (src/okg/substrate/cli/branch.py)."""
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).create_branch(
                self.branch, from_branch="default",
                metadata={
                    "kind": "okg_branch", "deployment_name": "default",
                    "branch_name": self.branch, "role": "experiment",
                    "status": "active", "base_branch": "default",
                    "base_generation_id": self.default_gen,
                    "created_by": "okg branch create",
                },
            )

    def observe(self, generation: str, node: str, key: str, value: Any) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            record_attribute_observation(
                conn, target_kind="node", target_id=node,
                generation_id=generation, attr_key=key, value=value,
                producer="test:b3772",
            )

    def generation_row(self, generation: str) -> dict[str, Any]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            published = {
                info.checkpoint_id
                for info in store.list_generations(
                    deployment_name="default", branch=self.branch,
                    status="published",
                )
            }
            marker = conn.execute(
                f"SELECT overlay_rows FROM {OVERLAY_GENERATION_MARKER_TABLE} "
                "WHERE generation_id = %s",
                (generation,),
            ).fetchone()
        return {
            "published_on_branch": generation in published,
            "marker_overlay_rows": None if marker is None else int(marker[0]),
        }


def _reads(
    dsn: str, world: _World, *, branch: str | None,
    generation_id: str | None = None,
) -> dict[str, Any]:
    """The reads an optimizer trial makes, through the MCP server the
    ``okg mcp-serve --branch`` command builds."""
    kwargs = {} if branch is None else {"branch_name": branch}
    pin = {} if generation_id is None else {"generation_id": generation_id}
    mcp = build_server(dsn=dsn, deployment_name="default", **kwargs)
    tools = mcp._tool_manager._tools
    try:
        session = tools["inspect"].fn(target={"kind": "session"})
        rows = tools["filter"].fn(
            input={"kind": "nodes"}, predicate={},
            select=["node_id", "attrs"], limit=50, **pin,
        )
        owner_carol = tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={"op": "eq", "field": "attrs.owner", "value": "carol"},
            select=["node_id"], limit=50, **pin,
        )
        node_a = tools["inspect"].fn(
            target={"kind": "node", "node_id": world.a}, **pin,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return {
        "session_generation": session["generation_id"],
        "session_branch": session.get("generation_branch_name"),
        "rows": sorted(
            (_plain(row["node_id"]), _plain(row.get("attrs")))
            for row in rows["items"]
        ),
        "owner_carol": sorted(
            _plain(row["node_id"]) for row in owner_carol["items"]
        ),
        "node_a_attrs": _plain(node_a.get("attrs")),
    }


def test_branch_reads_after_two_branch_publishes(clean_runtime: str) -> None:
    world = _World(clean_runtime)
    default_before = _reads(clean_runtime, world, branch=None)
    assert default_before["session_generation"] == world.default_gen

    world.create_branch()
    # Publish 1 on the branch: a branch-only node.
    world.apply(world.branch, [(world.c, {"owner": "cy", "rank": 3})])
    branch_gen1 = world.publish(world.branch)
    # Attribute observations keyed to publish 1, as the issue's derive step.
    world.observe(branch_gen1, world.a, "owner", "carol")
    # Publish 2 on the branch: a graph attribute change as well.
    world.apply(world.branch, [(world.b, {"owner": "bob", "rank": 20})],
                revision=2)
    branch_gen2 = world.publish(world.branch)

    for gen in (branch_gen1, branch_gen2):
        row = world.generation_row(gen)
        assert row["published_on_branch"], row
        assert row["marker_overlay_rows"] is not None, row
    assert world.generation_row(branch_gen2)["marker_overlay_rows"] >= 1

    branch = _reads(clean_runtime, world, branch=world.branch)
    assert branch["session_generation"] == branch_gen2
    assert branch["session_branch"] == world.branch
    served = dict(branch["rows"])
    assert set(served) == {world.a, world.b, world.c}
    assert served[world.a]["owner"] == "carol"   # the observed value
    assert served[world.b]["rank"] == 20          # the publish-2 graph change
    assert branch["owner_carol"] == [world.a]
    assert branch["node_a_attrs"]["owner"] == "carol"

    # The first branch generation, pinned explicitly on the branch server,
    # still reads as it was published: the observation keyed to it enters
    # only the next generation, and the graph change came later.
    first = _reads(clean_runtime, world, branch=world.branch,
                   generation_id=branch_gen1)
    first_rows = dict(first["rows"])
    assert set(first_rows) == {world.a, world.b, world.c}
    assert first_rows[world.a]["owner"] == "alice"
    assert first_rows[world.b]["rank"] == 2
    assert first["owner_carol"] == []

    default_after = _reads(clean_runtime, world, branch=None)
    assert default_after == default_before
    assert world.c not in dict(default_after["rows"])
    assert dict(default_after["rows"])[world.a]["owner"] == "alice"
    assert default_after["owner_carol"] == []


def test_branch_first_publish_carries_the_base_generations_overlays(
    clean_runtime: str,
) -> None:
    """Review of #3772: an observation the default branch already serves
    must survive into a new branch. The first branch publish has no
    published generation of its own; before the fix it carried overlays
    forward from nothing, so the branch silently served the raw graph value
    (owner alice) and its overlay marker counted 0 rows."""
    world = _World(clean_runtime)
    world.observe(world.default_gen, world.a, "owner", "dave")
    world.apply("default", [(world.b, {"owner": "bob", "rank": 5})], revision=2)
    world.default_gen = world.publish("default")
    default_before = _reads(clean_runtime, world, branch=None)
    assert dict(default_before["rows"])[world.a]["owner"] == "dave"

    world.create_branch()
    world.apply(world.branch, [(world.c, {"owner": "cy", "rank": 3})])
    branch_gen1 = world.publish(world.branch)
    assert world.generation_row(branch_gen1) == {
        "published_on_branch": True, "marker_overlay_rows": 1,
    }
    first = _reads(clean_runtime, world, branch=world.branch)
    assert first["session_generation"] == branch_gen1
    first_rows = dict(first["rows"])
    assert set(first_rows) == {world.a, world.b, world.c}
    assert first_rows[world.a]["owner"] == "dave"
    assert first["node_a_attrs"]["owner"] == "dave"

    # A branch-only observation on another node shows on the branch alone,
    # beside the carried base value. (Overriding the base-observed key on
    # the branch is not tested: an observation keyed to a later generation
    # loses to the carried snapshot unless it supersedes, and supersession
    # is not branch-scoped.)
    world.observe(branch_gen1, world.b, "owner", "erin")
    world.apply(world.branch, [(world.c, {"owner": "cy", "rank": 30})],
                revision=2)
    branch_gen2 = world.publish(world.branch)
    second = _reads(clean_runtime, world, branch=world.branch)
    assert second["session_generation"] == branch_gen2
    second_rows = dict(second["rows"])
    assert second_rows[world.b]["owner"] == "erin"
    assert second_rows[world.a]["owner"] == "dave"
    assert second_rows[world.c]["rank"] == 30
    assert second["node_a_attrs"]["owner"] == "dave"

    default_after = _reads(clean_runtime, world, branch=None)
    assert default_after == default_before
    assert dict(default_after["rows"])[world.a]["owner"] == "dave"
    assert dict(default_after["rows"])[world.b]["owner"] == "bob"
    assert world.c not in dict(default_after["rows"])


def test_unmarked_branch_pin_hint_names_the_branch(clean_runtime: str) -> None:
    """Failure case: a branch read of a generation with no overlay marker
    is refused by name, and the hint lists that branch's generations, not
    the default branch's."""
    world = _World(clean_runtime)
    world.create_branch()
    world.apply(world.branch, [(world.c, {"owner": "cy", "rank": 3})])
    branch_gen = world.publish(world.branch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        deleted = conn.execute(
            f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} "
            "WHERE generation_id = %s",
            (branch_gen,),
        ).rowcount
    assert deleted == 1
    mcp = build_server(
        dsn=clean_runtime, deployment_name="default", branch_name=world.branch,
    )
    try:
        with pytest.raises(SubstrateError) as refused:
            mcp._tool_manager._tools["filter"].fn(
                input={"kind": "nodes"},
                predicate={"op": "eq", "field": "attrs.owner", "value": "cy"},
                select=["node_id"], limit=50,
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert refused.value.code == "overlay_generation_unmaterialized"
    assert refused.value.data["cause"] == "overlay_step_not_run"
    assert (
        f"okg generation list --deployment default --branch {world.branch}"
        in (refused.value.fix_hint or "")
    ), refused.value.fix_hint
