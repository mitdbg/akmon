"""Generation inspection and fork-branch scope on the MCP read path.

Three defects, one shape: a read resolved from the wrong place and reported
the answer as if it came from the right one.

- #1436 `inspect(kind='generation')` bound the public Chronos checkpoint
  STRING to `okg.graph_generations.generation_id BIGSERIAL`. Native
  publishing stopped writing those legacy rows, so a valid id — including
  the session's own pin — raised instead of resolving.
- #1437 a fork-configured server resolved its implicit latest from
  `default` and then labelled the session with the fork it was asked for.
  Every read came from the wrong branch and the metadata said otherwise.
- #1439 `generation list` spans branches and deployments while
  `generation describe` resolved on `default` only and exposed no branch
  option, so ids straight out of list were undescribable.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")
pytest.importorskip("mcp")

from okg.substrate.errors import (  # noqa: E402
    GenerationNotFound,
    GenerationOutOfScope,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres


class _Fixture:
    """A deployment with DIFFERENT content on `default` and on a fork.

    Both heads must exist and must disagree: a fixture where the branches
    agree cannot tell a fork read apart from a default read, so it passes
    on the broken code.
    """

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.suffix = uuid.uuid4().hex[:8]
        self.fork = f"experiment:{self.suffix}"
        self.default_node = f"n:{self.suffix}:on-default"
        self.fork_node = f"n:{self.suffix}:on-fork"
        self.catalog_version_id: int = 0
        self.default_generation: str = ""
        self.fork_generation: str = ""

    def build(self) -> "_Fixture":
        identity = None
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            self.catalog_version_id = seed_catalog(conn, subtypes=["page"])
        identity = bind_test_catalog_writer(self.dsn, "default")
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn)
            store.apply_facts(
                [
                    NodeFact(
                        self.default_node, "page", {"text": "default head"},
                        {"id": self.default_node}, {"v": 1},
                    )
                ],
                source="default",
            )
        out = Publisher(self.dsn, "default", catalog_identity=identity).cycle()
        assert out["status"] == "published", out
        self.default_generation = str(out["generation_id"])
        # The writer binding clones the seeded catalog into a deployment-owned
        # version, so read the PUBLISHED catalog_version_id back rather than
        # assuming the seeded one.
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            metadata = store.get_generation(self.default_generation).metadata or {}
        self.catalog_version_id = int(metadata["catalog_version_id"])

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn)
            store.create_branch(
                self.fork,
                from_branch="default",
                metadata={
                    "base_branch": "default",
                    "base_generation_id": self.default_generation,
                },
            )
            store.apply_facts(
                [
                    NodeFact(
                        self.fork_node, "page", {"text": "fork head"},
                        {"id": self.fork_node}, {"v": 1},
                    )
                ],
                branch=self.fork,
                source="default",
            )
        out = Publisher(
            self.dsn, "default", branch_name=self.fork, catalog_identity=identity,
        ).cycle()
        assert out["status"] == "published", out
        self.fork_generation = str(out["generation_id"])
        assert self.fork_generation != self.default_generation
        return self

    def stamp_checkpoint(
        self, *, deployment_name: str, status: str, branch: str = "default",
    ) -> str:
        """Mint one bare OKG generation checkpoint with chosen scope/status."""
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn)
            return store.checkpoint_generation(
                branch=branch,
                deployment_name=deployment_name,
                catalog_version_id=self.catalog_version_id,
                status=status,
                metadata=(
                    {"blocked_reason": {"gate": "test-gate"}}
                    if status == "blocked"
                    else {}
                ),
            )


@pytest.fixture
def fixture(clean_runtime: str) -> _Fixture:
    return _Fixture(clean_runtime).build()


def _server(dsn: str, **kwargs: Any):
    mcp = build_server(dsn=dsn, deployment_name="default", **kwargs)
    return mcp


def _close(mcp: Any) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


# ---------------------------------------------------------------- #1436


def test_inspect_generation_resolves_a_native_checkpoint(
    fixture: _Fixture,
) -> None:
    """Explicitly and implicitly: a real published checkpoint inspects.

    Pre-fix both raised — the explicit id because the checkpoint STRING was
    compared against a BIGSERIAL column, the implicit one for the same
    reason on the session's own pin.
    """
    mcp = _server(fixture.dsn)
    try:
        tool = mcp._tool_manager._tools["inspect"]
        explicit = tool.fn(
            target={
                "kind": "generation",
                "generation_id": fixture.default_generation,
            }
        )
        implicit = tool.fn(target={"kind": "generation"})
    finally:
        _close(mcp)

    assert explicit["generation_id"] == fixture.default_generation
    assert explicit["deployment_name"] == "default"
    assert explicit["branch"] == "default"
    assert explicit["status"] == "published"
    assert explicit["blocked_reason"] is None
    assert explicit["catalog_version_id"] == fixture.catalog_version_id
    assert explicit["published_at"] is not None

    # The session's implicit generation IS the default head here.
    assert implicit["generation_id"] == fixture.default_generation
    assert implicit["status"] == "published"
    assert implicit["pinned_generation_id"] == fixture.default_generation


def test_inspect_generation_reports_blocked_as_state_not_error(
    fixture: _Fixture,
) -> None:
    """A blocked generation is exactly the one an operator needs to see."""
    blocked = fixture.stamp_checkpoint(
        deployment_name="default", status="blocked",
    )
    mcp = _server(fixture.dsn)
    try:
        payload = mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "generation", "generation_id": blocked}
        )
    finally:
        _close(mcp)

    assert payload["generation_id"] == blocked
    assert payload["status"] == "blocked"
    assert payload["blocked_reason"] == {"gate": "test-gate"}
    # Never published, so there is no publish boundary to report.
    assert payload["published_at"] is None
    assert payload["created_at"] is not None


def test_inspect_generation_separates_missing_from_wrong_deployment(
    fixture: _Fixture,
) -> None:
    """Absent and out-of-scope are different facts and get different codes."""
    foreign = fixture.stamp_checkpoint(
        deployment_name=f"other-{fixture.suffix}", status="published",
    )
    mcp = _server(fixture.dsn)
    try:
        tool = mcp._tool_manager._tools["inspect"]
        with pytest.raises(GenerationNotFound) as missing:
            tool.fn(
                target={
                    "kind": "generation",
                    "generation_id": f"okg:default:branch:default:gen:absent:{
                        fixture.suffix
                    }",
                }
            )
        with pytest.raises(GenerationOutOfScope) as wrong_deployment:
            tool.fn(target={"kind": "generation", "generation_id": foreign})
        with pytest.raises(GenerationOutOfScope) as wrong_branch:
            tool.fn(
                target={
                    "kind": "generation",
                    "generation_id": fixture.fork_generation,
                }
            )
    finally:
        _close(mcp)

    assert missing.value.code == "generation_not_found"
    assert wrong_deployment.value.code == "generation_out_of_scope"
    assert (
        wrong_deployment.value.data["generation_deployment_name"]
        == f"other-{fixture.suffix}"
    )
    # A fork id read from a default-branch session names the owning branch
    # rather than reporting the id as nonexistent.
    assert wrong_branch.value.data["generation_branch"] == fixture.fork


def test_inspect_generation_on_a_fork_session_reads_the_fork(
    fixture: _Fixture,
) -> None:
    """Scope follows the session: the fork head is in scope, default is not."""
    mcp = _server(fixture.dsn, branch_name=fixture.fork)
    try:
        tool = mcp._tool_manager._tools["inspect"]
        own = tool.fn(
            target={
                "kind": "generation",
                "generation_id": fixture.fork_generation,
            }
        )
        with pytest.raises(GenerationOutOfScope):
            tool.fn(
                target={
                    "kind": "generation",
                    "generation_id": fixture.default_generation,
                }
            )
    finally:
        _close(mcp)

    assert own["generation_id"] == fixture.fork_generation
    assert own["branch"] == fixture.fork


# ---------------------------------------------------------------- #1437


def test_fork_server_without_a_pin_reads_the_fork_head(
    fixture: _Fixture,
) -> None:
    """The whole bug in one assertion set.

    A fork-configured server that omits the generation pin used to resolve
    latest from `default` — so it served the default head's rows while
    `inspect(session)` claimed the fork branch.
    """
    mcp = _server(fixture.dsn, branch_name=fixture.fork)
    try:
        tool = mcp._tool_manager._tools["inspect"]
        session = tool.fn(target={"kind": "session"})
        rows = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=50,
        )
    finally:
        _close(mcp)

    assert session["branch_name"] == fixture.fork
    # The pin is the FORK's head, not default's.
    assert session["generation_id"] == fixture.fork_generation
    # ... and the session says which branch that generation came from.
    assert session["generation_branch_name"] == fixture.fork
    assert session["generation_branch_matches_session"] is True

    # Content proof, not just id proof: the fork-only node is visible.
    served = {row["node_id"] for row in rows["items"]}
    assert fixture.fork_node in served
    assert fixture.default_node in served  # inherited copy-on-write base


def test_default_server_is_unaffected_and_never_sees_fork_rows(
    fixture: _Fixture,
) -> None:
    """The default branch must not gain the fork's content."""
    mcp = _server(fixture.dsn)
    try:
        session = mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "session"}
        )
        rows = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=50,
        )
    finally:
        _close(mcp)

    assert session["generation_id"] == fixture.default_generation
    assert session["generation_branch_name"] == "default"
    served = {row["node_id"] for row in rows["items"]}
    assert fixture.default_node in served
    assert fixture.fork_node not in served


def test_fork_without_its_own_head_names_the_branch_it_actually_read(
    clean_runtime: str,
) -> None:
    """The documented read-through must still be legible.

    A fresh fork with no published head of its own reads its default-branch
    base. That is intended — but the session must not then claim the
    generation came from the fork.
    """
    suffix = uuid.uuid4().hex[:8]
    fork = f"fresh:{suffix}"
    node = f"n:{suffix}:base"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
    identity = bind_test_catalog_writer(clean_runtime, "default")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(node, "page", {"text": "base"}, {"id": node}, {"v": 1})],
            source="default",
        )
    base = str(
        Publisher(
            clean_runtime, "default", catalog_identity=identity,
        ).cycle()["generation_id"]
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base},
        )

    mcp = build_server(
        dsn=clean_runtime, deployment_name="default", branch_name=fork,
    )
    try:
        session = mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "session"}
        )
    finally:
        _close(mcp)

    assert session["branch_name"] == fork
    assert session["generation_id"] == base
    assert session["generation_branch_name"] == "default"
    assert session["generation_branch_matches_session"] is False
