#!/usr/bin/env python3
"""Publish the AKMON reviewer deployment from the bundled synthetic records.

Runs the documented bootstrap sequence (docs/start/evaluate-from-source.md)
against a local PostgreSQL database with no API keys: plan, inspect, draft,
commit, verify (which imports, checks and publishes), apply, a catalog
hand-over to the applied deployment with one republish, then a pinned lexical
search of the verified generation. Prints the head generation after the
republish with its node and edge counts (and the verified generation with its
deltas), and writes the state file the claim scripts read.

Usage:
    uv run python scripts/akmon/reviewer_quickstart.py \
        --dsn "$OKG_QUICKSTART_DSN" --workdir /tmp/akmon
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    run_main,
    DEFAULT_DEPLOYMENT_NAME,
    DSN_ENV,
    RECORDS_FIXTURE,
    REPO_ROOT,
    STATE_FILE_NAME,
    fail,
    note,
    passed,
    require_keyless,
    resolve_dsn,
    run_okg,
)


_GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "AKMON reviewer",
    "GIT_AUTHOR_EMAIL": "reviewer@example.invalid",
    "GIT_COMMITTER_NAME": "AKMON reviewer",
    "GIT_COMMITTER_EMAIL": "reviewer@example.invalid",
}


def _git(deployment: Path, *args: str) -> None:
    env = {**os.environ, **_GIT_IDENTITY}
    result = subprocess.run(
        ["git", "-C", str(deployment), *args],
        env=env, capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        fail("git", f"git {' '.join(args)} failed", output=result.stderr.strip())


def _count_published(dsn: str, generation_id: str) -> tuple[int, int]:
    """Node and edge rows readable at exactly this published generation."""
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        view = store.checkout_generation(generation_id)
        nodes = view.query("SELECT count(*) AS n FROM okg.graph_nodes")
        edges = view.query("SELECT count(*) AS n FROM okg.graph_edges")
    return int(nodes[0]["n"]), int(edges[0]["n"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", help=f"PostgreSQL URL (or set {DSN_ENV})")
    parser.add_argument(
        "--workdir",
        help="empty directory for the deployment and state file "
        "(default: a new temporary directory)",
    )
    parser.add_argument("--name", default=DEFAULT_DEPLOYMENT_NAME)
    args = parser.parse_args(argv)

    require_keyless("quickstart")
    dsn = resolve_dsn(args.dsn)
    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="akmon-reviewer-")).resolve()
    if workdir == REPO_ROOT or REPO_ROOT in workdir.parents:
        # Publishing records the checkout's exact commit and refuses while
        # the checkout has uncommitted files, which this directory would add.
        fail("setup", "--workdir must be outside the AKMON checkout", workdir=workdir)
    deployments_dir = workdir / "deployments"
    deployment = deployments_dir / args.name
    if deployment.exists():
        fail("setup", "deployment directory already exists; pass an empty --workdir",
             deployment=deployment)
    (deployment / "fixtures").mkdir(parents=True)
    shutil.copy2(RECORDS_FIXTURE, deployment / "fixtures/records.json")
    brief = workdir / "brief.md"
    brief.write_text(
        "# Sample records\n"
        "A paper and dataset record used to demonstrate import, publication, "
        "and search.\n",
        encoding="utf-8",
    )
    note(f"workdir: {workdir}")

    os.environ[DSN_ENV] = dsn
    os.environ["OKG_DEPLOYMENTS_DIR"] = str(deployments_dir)
    env = dict(os.environ)

    # A fresh PostgreSQL container has the extensions but not the AKMON
    # schema; verify refuses with schema_not_migrated until this runs.
    run_okg("migrate", ["migrate", "--dsn", dsn, "--apply", "--json"], env=env)
    run_okg("plan", [
        "deployment", "bootstrap", "plan",
        "--deployment", str(deployment),
        "--name", args.name,
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--dsn-env", DSN_ENV,
        "--apply", "--json",
    ], env=env)
    run_okg("inspect-source", [
        "deployment", "bootstrap", "inspect-source",
        "--deployment", str(deployment), "records", "--apply", "--json",
    ], env=env)
    run_okg("draft", [
        "deployment", "bootstrap", "draft",
        "--deployment", str(deployment), "--apply", "--json",
    ], env=env)

    _git(deployment, "init", "-q", "-b", "main")
    _git(deployment, "add", "-A", "--", ".")
    _git(deployment, "commit", "-q", "-m", "Stage AKMON reviewer deployment")

    verified = run_okg("verify", [
        "deployment", "bootstrap", "verify",
        "--deployment", str(deployment),
        "--dsn", dsn, "--apply", "--json",
    ], env=env)
    verification = verified.get("verification") or {}
    generation_id = verification.get("generation_id")
    node_delta = verification.get("node_row_delta")
    edge_delta = verification.get("edge_row_delta")
    if verification.get("status") != "verified" or not generation_id:
        fail("verify", "bootstrap verify did not publish", verification=verification)
    if not isinstance(node_delta, int) or node_delta <= 0:
        fail("verify", "published node delta is not positive", node_row_delta=node_delta)
    if not isinstance(edge_delta, int) or edge_delta <= 0:
        fail("verify", "published edge delta is not positive", edge_row_delta=edge_delta)

    run_okg("apply", [
        "deployment", "bootstrap", "apply",
        "--deployment", str(deployment), "--apply", "--json",
    ], env=env)
    _git(deployment, "add", "-A", "--", ".")
    _git(deployment, "commit", "-q", "-m", "Apply verified AKMON reviewer deployment")

    # `bootstrap apply` handed the catalog to the applied directory (it was
    # owned by the staged copy verify published from). Publish `default` once
    # from the applied deployment so the fork and lens claims start from a
    # generation built on that catalog.
    cycle = run_okg("publish", [
        "run", "--once", "--deployment", args.name, "--dsn", dsn,
        "--apply", "--json",
    ], env=env)
    if cycle.get("status") != "published" or not cycle.get("generation_id"):
        fail("publish", "publisher cycle from the applied deployment did not publish",
             status=cycle.get("status"))
    head = str(cycle["generation_id"])
    note(f"default republished from the applied deployment: {head}")

    described = run_okg("describe", [
        "generation", "describe", str(generation_id),
        "--deployment", args.name, "--dsn", dsn, "--json",
    ], env=env)
    if described.get("status") != "published":
        fail("describe", "generation is not published", status=described.get("status"))

    searched = run_okg("search", [
        "search", "--deployment", args.name, "--dsn", dsn,
        "--query", "salinity", "--method", "lexical", "--limit", "10",
        "--generation-id", str(generation_id), "--json",
    ], env=env)
    results = searched.get("results") or []
    if searched.get("generation_id") != generation_id or not results:
        fail("search", "pinned search returned no results for the published generation",
             generation_id=searched.get("generation_id"), results=len(results))

    verified_nodes, verified_edges = _count_published(dsn, str(generation_id))
    nodes, edges = _count_published(dsn, head)
    note(
        f"verified generation {generation_id}: nodes={verified_nodes} "
        f"edges={verified_edges}; head {head}: nodes={nodes} edges={edges}"
    )
    if nodes <= 0 or edges <= 0:
        fail("count", "published head is empty", head=head, nodes=nodes, edges=edges)
    if (nodes, edges) != (verified_nodes, verified_edges):
        fail("count", "the republish changed the graph",
             verified=f"{verified_nodes}/{verified_edges}", head=f"{nodes}/{edges}")

    state = {
        "deployment_name": args.name,
        "deployment_dir": str(deployment),
        "deployments_dir": str(deployments_dir),
        "dsn_env": DSN_ENV,
        "generation_id": head,
        "verified_generation_id": str(generation_id),
        "node_count": nodes,
        "edge_count": edges,
    }
    state_path = workdir / STATE_FILE_NAME
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    note(f"state file: {state_path}")
    note(f"search hits for 'salinity': {len(results)}")
    passed(
        "QUICKSTART",
        generation=head,
        nodes=nodes,
        edges=edges,
        verified=generation_id,
        node_delta=node_delta,
        edge_delta=edge_delta,
    )
    return 0


if __name__ == "__main__":
    run_main("quickstart", main)
