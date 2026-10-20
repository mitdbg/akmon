"""req.okg-workspace-resource-monitoring.host-nodes + .collector.

The collector parses probe output to typed metrics, writes a node_series sample
per metric via record_sample, ensures a compute_host node with a valid
latest_from_series handle, and records an unreachable host as reachable=0
without aborting the run.
"""

from __future__ import annotations

from types import MappingProxyType

import psycopg
import pytest

from okg.substrate.external_backed.collector import (
    HOST_SUBTYPE,
    Host,
    host_node_fact,
    load_hosts,
    parse_probe_output,
    run_collection,
)
from okg.substrate.external_backed.handle import parse_handle
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

_DEPLOY = "okg-workspace"


def _conn(dsn):
    return psycopg.connect(dsn, autocommit=True)


def test_parse_probe_output() -> None:
    out = parse_probe_output(
        "load1=11.93\ncpu_count=64\ndisk_used_pct=37\n"
        "disk_used_bytes=254262063104\ndisk_total_bytes=687194767360\n"
        "mem_used_pct=58.2\nmem_used_bytes=39996027904\n"
        "mem_total_bytes=68719476736\njunk\n"
    )
    assert out == {
        "load1": 11.93,
        "cpu_count": 64.0,
        "disk_used_pct": 37.0,
        "disk_used_bytes": 254_262_063_104.0,
        "disk_total_bytes": 687_194_767_360.0,
        "mem_used_pct": 58.2,
        "mem_used_bytes": 39_996_027_904.0,
        "mem_total_bytes": 68_719_476_736.0,
    }


def test_host_node_fact_handle_parses() -> None:
    fact = host_node_fact(Host("cluster01", "host:cluster01", "u@h", "Example cluster01"))
    assert fact.subtype == HOST_SUBTYPE
    handle = parse_handle(fact.attrs)
    assert handle is not None
    assert set(handle.fields) == {
        "load1",
        "cpu_count",
        "mem_used_pct",
        "mem_used_bytes",
        "mem_total_bytes",
        "disk_used_pct",
        "disk_used_bytes",
        "disk_total_bytes",
        "reachable",
    }
    assert handle.fields["load1"].current == "latest_from_series"
    assert "runner_storage" not in fact.attrs


def test_dynamic_host_metadata_is_immutable_and_graph_safe() -> None:
    host = Host(
        "runner-119",
        "host:runner-119",
        "ubuntu@192.0.2.119",
        metadata={
            "github_runner_id": 119,
            "github_runner_name": "runner-119",
            "openstack_server_id": "srv-119",
            "eligible": True,
            "busy": False,
            "quarantined": False,
            "storage_state": "healthy",
            "guard_healthy": True,
            "guard_observed_at": "2026-08-13T12:00:00+00:00",
            "filesystem_available_bytes": {
                "docker": 40 * 1024**3,
                "containerd": 40 * 1024**3,
            },
        },
    )

    assert isinstance(host.metadata, MappingProxyType)
    with pytest.raises(TypeError):
        host.metadata["storage_state"] = "replace"  # type: ignore[index]
    with pytest.raises(TypeError):
        host.metadata["filesystem_available_bytes"]["docker"] = 0  # type: ignore[index]

    fact = host_node_fact(host)
    assert fact.attrs["runner_storage"] == {
        "github_runner_id": 119,
        "github_runner_name": "runner-119",
        "openstack_server_id": "srv-119",
        "eligible": True,
        "busy": False,
        "quarantined": False,
        "storage_state": "healthy",
        "guard_healthy": True,
        "guard_observed_at": "2026-08-13T12:00:00+00:00",
        "filesystem_available_bytes": {
            "docker": 40 * 1024**3,
            "containerd": 40 * 1024**3,
        },
    }
    assert "ssh_opts" not in fact.attrs["runner_storage"]
    assert "host_target" in fact.attrs  # identity remains available to collector


def _series(conn, node_id, metric):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value FROM okg.node_series WHERE deployment_name=%s "
            "AND node_id=%s AND metric=%s ORDER BY observed_at DESC LIMIT 1",
            (_DEPLOY, node_id, metric),
        )
        row = cur.fetchone()
    return None if row is None else row[0]


def test_run_collection_writes_samples_and_node(clean_runtime: str) -> None:
    host = Host("laptop", "host:laptop", "local", "laptop")

    def fake_probe(h):
        return True, {
            "load1": 2.5,
            "cpu_count": 8.0,
            "disk_used_pct": 41.0,
            "disk_used_bytes": 410.0,
            "disk_total_bytes": 1_000.0,
            "mem_used_pct": 60.0,
            "mem_used_bytes": 600.0,
            "mem_total_bytes": 1_000.0,
        }

    with _conn(clean_runtime) as conn:
        seed_catalog_version(conn)
        out = run_collection(
            conn, deployment_name=_DEPLOY, hosts=[host], probe_fn=fake_probe
        )
        assert out["hosts"]["laptop"]["reachable"] is True
        # samples landed
        assert _series(conn, "host:laptop", "load1") == 2.5
        assert _series(conn, "host:laptop", "reachable") == 1.0
        assert _series(conn, "host:laptop", "cpu_count") == 8.0
        # compute_host node exists with a parseable handle
        from okg.substrate.graph_store import ChronosGraphStore

        rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id=%(n)s",
            {"n": "host:laptop"},
        )
    assert rows and parse_handle(rows[0]["attrs"]) is not None


def test_run_collection_persists_dynamic_runner_storage_attrs(
    clean_runtime: str,
) -> None:
    host = Host(
        "runner-119",
        "host:runner-119",
        "local",
        metadata={
            "github_runner_id": 119,
            "github_runner_name": "runner-119",
            "openstack_server_id": "srv-119",
            "eligible": True,
            "busy": True,
            "quarantined": False,
            "storage_state": "warn",
            "guard_healthy": True,
            "guard_observed_at": "2026-08-13T12:00:00+00:00",
            "filesystem_available_bytes": {"docker": 40 * 1024**3},
        },
    )

    with _conn(clean_runtime) as conn:
        seed_catalog_version(conn)
        run_collection(
            conn,
            deployment_name=_DEPLOY,
            hosts=[host],
            probe_fn=lambda _host: (True, {"disk_used_pct": 70.0}),
        )
        from okg.substrate.graph_store import ChronosGraphStore

        rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id=%(n)s",
            {"n": "host:runner-119"},
        )
    assert rows
    attrs = rows[0]["attrs"]
    assert attrs["runner_storage"]["github_runner_id"] == 119
    assert attrs["runner_storage"]["openstack_server_id"] == "srv-119"
    assert attrs["runner_storage"]["busy"] is True
    assert attrs["runner_storage"]["filesystem_available_bytes"] == {
        "docker": 40 * 1024**3
    }
    assert "ssh_opts" not in attrs["runner_storage"]


def test_unreachable_records_zero_and_run_continues(clean_runtime: str) -> None:
    up = Host("up", "host:up", "local")
    down = Host("down", "host:down", "u@bogus")

    def fake_probe(h):
        if h.name == "down":
            return False, {}
        return True, {"load1": 1.0}

    with _conn(clean_runtime) as conn:
        seed_catalog_version(conn)
        out = run_collection(
            conn, deployment_name=_DEPLOY, hosts=[up, down], probe_fn=fake_probe
        )
        # the failing host is TYPED, not silent
        assert _series(conn, "host:down", "reachable") == 0.0
        assert _series(conn, "host:down", "load1") is None  # no default
        # the reachable host's samples are still written (run continued)
        assert _series(conn, "host:up", "reachable") == 1.0
        assert _series(conn, "host:up", "load1") == 1.0
    assert out["hosts"]["down"]["reachable"] is False
    assert out["hosts"]["up"]["reachable"] is True


def test_real_config_loads() -> None:
    from okg.substrate.deployments import find_deployments_root

    hosts = load_hosts(
        find_deployments_root() / "okg-workspace" / "monitored_hosts.yaml"
    )
    names = {h.name for h in hosts}
    assert "laptop" in names
    # The real config also watches at least one remote host over ssh.
    assert any(h.target != "local" for h in hosts)
    laptop = next(h for h in hosts if h.name == "laptop")
    assert laptop.target == "local"
