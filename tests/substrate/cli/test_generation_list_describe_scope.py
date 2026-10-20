"""`okg generation list` and `okg generation describe` must agree on scope.

Issue #1439: list spanned branches and described ids `describe` could not
resolve, because describe resolved on branch `default` unconditionally and
exposed no branch option. And `--deployment`'s help said omitting it meant
"all", while the handler substituted `default` — the documented behavior
was unreachable.

The binding assertion here is round-trip: EVERY id list returns must be
describable using only what that row carries plus supported CLI options.
Asserting that one hand-picked id describes proves nothing about the rest.
"""

from __future__ import annotations

import argparse
import json
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.cli.generation import (  # noqa: E402
    _cmd_generation,
    _cmd_generation_list,
)
from okg.substrate.cli.main import _build_parser  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres


def _list(dsn: str, capsys, **kwargs) -> list[dict]:
    args = argparse.Namespace(
        command="generation", subcmd="list", dsn=dsn, json=True,
        deployment=kwargs.get("deployment"), branch=kwargs.get("branch"),
        limit=kwargs.get("limit", 50),
    )
    assert _cmd_generation_list(args, as_json=True) == 0
    return json.loads(capsys.readouterr().out)["generations"]


def _describe(dsn: str, capsys, generation_id: str, **kwargs):
    args = argparse.Namespace(
        command="generation", subcmd="describe", dsn=dsn, json=True,
        generation_id=generation_id,
        deployment=kwargs.get("deployment"), branch=kwargs.get("branch"),
    )
    rc = _cmd_generation(args, as_json=True)
    return rc, capsys.readouterr()


@pytest.fixture
def multi_scope(clean_runtime: str) -> dict:
    """Two deployments and two branches, each with its own published head."""
    suffix = uuid.uuid4().hex[:8]
    fork = f"experiment:{suffix}"
    other_deployment = f"other-{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
    identity = bind_test_catalog_writer(clean_runtime, "default")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(f"n:{suffix}:a", "page", {}, {"id": "a"}, {"v": 1})],
            source="default",
        )
    default_gen = str(
        Publisher(
            clean_runtime, "default", catalog_identity=identity,
        ).cycle()["generation_id"]
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={
                "base_branch": "default", "base_generation_id": default_gen,
            },
        )
        store.apply_facts(
            [NodeFact(f"n:{suffix}:b", "page", {}, {"id": "b"}, {"v": 1})],
            branch=fork, source="default",
        )
    fork_gen = str(
        Publisher(
            clean_runtime, "default", branch_name=fork,
            catalog_identity=identity,
        ).cycle()["generation_id"]
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        catalog_version_id = int(
            (store.get_generation(default_gen).metadata or {})[
                "catalog_version_id"
            ]
        )
        foreign_gen = store.checkpoint_generation(
            branch="default",
            deployment_name=other_deployment,
            catalog_version_id=catalog_version_id,
            status="published",
        )
    return {
        "dsn": clean_runtime,
        "fork": fork,
        "other_deployment": other_deployment,
        "default_generation": default_gen,
        "fork_generation": fork_gen,
        "foreign_generation": foreign_gen,
    }


def test_argparse_exposes_branch_on_list_and_describe() -> None:
    parser = _build_parser()
    listed = parser.parse_args(
        ["generation", "list", "--branch", "experiment:x"],
    )
    assert listed.branch == "experiment:x"
    described = parser.parse_args(
        ["generation", "describe", "gen-id", "--branch", "experiment:x"],
    )
    assert described.branch == "experiment:x"
    # Omitting --branch means "any branch", not "default".
    assert parser.parse_args(["generation", "describe", "gen-id"]).branch is None
    assert parser.parse_args(["generation", "list"]).branch is None


def test_every_listed_generation_is_describable(multi_scope, capsys) -> None:
    """The round trip, over EVERY row — not a hand-picked one."""
    rows = _list(multi_scope["dsn"], capsys)
    listed_ids = {row["generation_id"] for row in rows}
    assert multi_scope["default_generation"] in listed_ids
    assert multi_scope["fork_generation"] in listed_ids
    assert multi_scope["foreign_generation"] in listed_ids

    for row in rows:
        rc, captured = _describe(
            multi_scope["dsn"], capsys, row["generation_id"],
            deployment=row["deployment_name"], branch=row["branch"],
        )
        assert rc == 0, (
            f"listed id {row['generation_id']!r} "
            f"(deployment={row['deployment_name']!r}, "
            f"branch={row['branch']!r}) is not describable: {captured.err}"
        )
        described = json.loads(captured.out)
        assert described["generation_id"] == row["generation_id"]
        assert described["deployment_name"] == row["deployment_name"]
        assert described["branch"] == row["branch"]


def test_listed_rows_carry_the_scope_describe_needs(
    multi_scope, capsys,
) -> None:
    """A row without its branch cannot be round-tripped by construction."""
    rows = _list(multi_scope["dsn"], capsys)
    by_id = {row["generation_id"]: row for row in rows}
    assert by_id[multi_scope["fork_generation"]]["branch"] == multi_scope["fork"]
    assert by_id[multi_scope["default_generation"]]["branch"] == "default"
    assert (
        by_id[multi_scope["foreign_generation"]]["deployment_name"]
        == multi_scope["other_deployment"]
    )


def test_omitting_deployment_lists_every_deployment(
    multi_scope, capsys,
) -> None:
    """The flag's documented default, actually delivered."""
    all_rows = _list(multi_scope["dsn"], capsys)
    deployments = {row["deployment_name"] for row in all_rows}
    assert {"default", multi_scope["other_deployment"]} <= deployments

    scoped = _list(multi_scope["dsn"], capsys, deployment="default")
    assert {row["deployment_name"] for row in scoped} == {"default"}
    assert multi_scope["foreign_generation"] not in {
        row["generation_id"] for row in scoped
    }


def test_branch_filter_scopes_the_listing(multi_scope, capsys) -> None:
    fork_rows = _list(
        multi_scope["dsn"], capsys,
        deployment="default", branch=multi_scope["fork"],
    )
    assert {row["generation_id"] for row in fork_rows} == {
        multi_scope["fork_generation"]
    }
    default_rows = _list(
        multi_scope["dsn"], capsys, deployment="default", branch="default",
    )
    assert multi_scope["fork_generation"] not in {
        row["generation_id"] for row in default_rows
    }


def test_explicit_branch_still_scopes_describe(multi_scope, capsys) -> None:
    """`--branch` is a real constraint, not decoration."""
    rc, captured = _describe(
        multi_scope["dsn"], capsys, multi_scope["fork_generation"],
        deployment="default", branch="default",
    )
    assert rc != 0
    assert "not found" in captured.err.lower()

    rc, captured = _describe(
        multi_scope["dsn"], capsys, multi_scope["fork_generation"],
        deployment="default", branch=multi_scope["fork"],
    )
    assert rc == 0, captured.err
    assert json.loads(captured.out)["branch"] == multi_scope["fork"]
