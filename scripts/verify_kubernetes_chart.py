#!/usr/bin/env python3
"""Render, inspect, and optionally prove the OKG Helm chart in kind."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

import yaml


REPO = Path(__file__).resolve().parents[1]
CHART = REPO / "ops" / "kubernetes" / "okg"
DEFAULT_REPORT = REPO / ".scratch" / "kubernetes" / "chart-report.json"


class VerificationError(RuntimeError):
    pass


_MCP_ENFORCE_COUNTS_UNAVAILABLE_VISIBILITY = (
    "enforce; exact counts unavailable for a graph this size"
)


def _tool(name: str, explicit: str | None = None) -> str:
    if explicit:
        path = Path(explicit).expanduser()
        if path.is_file():
            return str(path)
        raise VerificationError(f"{name} executable not found: {path}")
    found = shutil.which(name)
    if found:
        return found
    if name == "helm":
        candidates = sorted(
            (REPO / ".scratch" / "tools").glob("helm-*/darwin-arm64/helm"),
            reverse=True,
        )
        if candidates:
            return str(candidates[0])
    raise VerificationError(f"{name} is required")


def _run(
    argv: Iterable[str],
    *,
    timeout: float = 1800,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(item) for item in argv],
        cwd=REPO,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
        env=env,
    )
    if check and proc.returncode:
        command = " ".join(str(item) for item in argv)
        output = "\n".join(
            row for row in (proc.stdout.strip(), proc.stderr.strip()) if row
        )
        raise VerificationError(
            f"command failed ({proc.returncode}): {command}\n{output}"
        )
    return proc


def _base_sets(
    deployment: str,
    *,
    set_storage_defaults: bool,
    internal_postgres: bool,
) -> list[str]:
    values = [
        "--set-string",
        f"okg.deployment={deployment}",
        "--set-string",
        "secrets.existingSecret=okg-runtime",
        "--set-string",
        "images.okg.repository=registry.example/okg",
        "--set-string",
        "postgresql.image.repository=registry.example/okg-postgres",
        "--set-string",
        "images.okg.tag=verification",
        "--set-string",
        "postgresql.image.tag=verification",
    ]
    if set_storage_defaults:
        if internal_postgres:
            values.extend(
                [
                    "--set-string",
                    "postgresql.storage.storageClassName=standard",
                ]
            )
        values.extend(
            [
                "--set-string",
                "runtime.storage.storageClassName=standard",
            ]
        )
    return values


def render_chart(
    *,
    helm: str,
    deployment: str,
    values: Path | None = None,
    internal_postgres: bool = True,
    set_values: list[str] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    common = [str(CHART)]
    if values is not None:
        common.extend(["--values", str(values)])
    common.extend(
        _base_sets(
            deployment,
            set_storage_defaults=values is None,
            internal_postgres=internal_postgres,
        )
    )
    if not internal_postgres:
        common.extend(
            [
                "--set",
                "postgresql.internal.enabled=false",
                "--set",
                "mcp.manageRole=false",
            ]
        )
    common.extend(set_values or [])
    _run([helm, "lint", "--strict", *common])
    rendered = _run([helm, "template", "verification", *common]).stdout
    documents = [
        row for row in yaml.safe_load_all(rendered) if isinstance(row, dict)
    ]
    if not documents:
        raise VerificationError("helm template produced no Kubernetes objects")
    return documents, rendered


def _objects(
    documents: list[dict[str, Any]],
    *,
    kind: str,
    component: str | None = None,
) -> list[dict[str, Any]]:
    rows = [row for row in documents if row.get("kind") == kind]
    if component is None:
        return rows
    return [
        row
        for row in rows
        if (
            (row.get("metadata") or {}).get("labels") or {}
        ).get("app.kubernetes.io/component")
        == component
    ]


def _container(workload: dict[str, Any], name: str) -> dict[str, Any]:
    containers = (
        workload["spec"]["template"]["spec"].get("containers") or []
    )
    for row in containers:
        if row.get("name") == name:
            return row
    raise VerificationError(f"container {name!r} is missing")


def _env(container: dict[str, Any], name: str) -> dict[str, Any]:
    for row in container.get("env") or []:
        if row.get("name") == name:
            return row
    raise VerificationError(
        f"container {container.get('name')!r} is missing environment {name!r}"
    )


# Decision 23a: `okg provision` exits 8 when the publish committed but held
# the publisher lock past the capacity bar. The bootstrap init container maps
# exactly that code to success through this wrapper and keeps every other code.
BOOTSTRAP_COMMAND = [
    "/bin/sh",
    "-c",
    'okg "$@"; rc=$?; if [ "$rc" -eq 8 ]; then exit 0; fi; exit "$rc"',
    "okg-bootstrap",
]


#: The only prefix a worker probe may put before `okg deployment ready`.
#: renamed-flag compat (decision 310): the probe keeps the old `--profile`
#: spelling, which an image older than the chart still understands, and
#: silences the rename warning for that one command. Remove this allowance
#: when the deprecated aliases are removed and the probe uses `--checks`.
WORKER_PROBE_ENV_PREFIX = ["env", "OKG_RENAMED_FLAG_WARNINGS=0"]


def check_worker_probe_command(command: list[Any]) -> None:
    """Refuse a worker probe that does not run `okg deployment ready`.

    Accepts the command bare, or behind exactly WORKER_PROBE_ENV_PREFIX;
    any other prefix (another variable, a shell) is refused.
    """
    tokens = [str(token) for token in command]
    if tokens[:2] == WORKER_PROBE_ENV_PREFIX:
        tokens = tokens[2:]
    if tokens[:3] != ["okg", "deployment", "ready"]:
        raise VerificationError("worker probes must execute deployment readiness")


def check_bootstrap_container(bootstrap: dict[str, Any]) -> None:
    """Refuse a bootstrap container that is not the wrapped `okg provision`."""
    if bootstrap.get("command") != BOOTSTRAP_COMMAND:
        raise VerificationError(
            "bootstrap must run okg through the wrapper that maps provision's "
            f"capacity-warning exit 8 to 0; got {bootstrap.get('command')!r}"
        )
    if (bootstrap.get("args") or [])[:1] != ["provision"]:
        raise VerificationError("bootstrap must call okg provision")


def inspect_render(
    documents: list[dict[str, Any]],
    *,
    expect_postgres: bool,
    expect_mirror: bool,
    expect_repository_sync: bool = False,
    expect_worker_secrets: bool = False,
) -> dict[str, Any]:
    if _objects(documents, kind="Secret"):
        raise VerificationError("chart must not render Secret objects")
    runtime_rows = _objects(
        documents, kind="StatefulSet", component="runtime"
    )
    if len(runtime_rows) != 1:
        raise VerificationError("chart must render exactly one runtime StatefulSet")
    runtime = runtime_rows[0]
    if runtime["spec"].get("replicas") != 1:
        raise VerificationError("runtime worker must be a singleton")
    pod_spec = runtime["spec"]["template"]["spec"]
    init = pod_spec.get("initContainers") or []
    init_names = [row.get("name") for row in init]
    expected_init_names = []
    if expect_repository_sync:
        expected_init_names.append("repository-sync")
    if expect_worker_secrets:
        expected_init_names.append("worker-secrets")
    expected_init_names.extend(["bootstrap", "runtime-bootstrap", "mcp-role"])
    if not expect_postgres:
        expected_init_names.remove("mcp-role")
    if init_names != expected_init_names:
        raise VerificationError(
            f"runtime init ordering must be {expected_init_names!r}, got {init_names!r}"
        )
    bootstrap = next(row for row in init if row.get("name") == "bootstrap")
    check_bootstrap_container(bootstrap)
    runtime_bootstrap = next(
        row for row in init if row.get("name") == "runtime-bootstrap"
    )
    if (runtime_bootstrap.get("args") or [])[:2] != ["runtime", "bootstrap"]:
        raise VerificationError(
            "runtime-bootstrap must initialize DBOS before the worker starts"
        )
    worker = _container(runtime, "worker")
    mcp = _container(runtime, "mcp")
    names = {row.get("name") for row in pod_spec.get("containers") or []}
    if ("mirror" in names) is not expect_mirror:
        raise VerificationError("mirror container presence does not match values")
    if "--allow-writes" in (mcp.get("args") or []):
        raise VerificationError("MCP must use the read-only surface")
    if "streamable-http" not in (mcp.get("args") or []):
        raise VerificationError("MCP must expose streamable HTTP")
    main_containers = pod_spec.get("containers") or []
    for container in [*init, *main_containers]:
        security = container.get("securityContext") or {}
        if (
            security.get("runAsNonRoot") is not True
            or security.get("readOnlyRootFilesystem") is not True
            or security.get("allowPrivilegeEscalation") is not False
        ):
            raise VerificationError(
                f"container {container.get('name')} lacks hardened security context"
            )
        for env in container.get("env") or []:
            if "value" in env and "postgresql://" in str(env["value"]):
                raise VerificationError("rendered Pod contains a cleartext DSN")
        if not container.get("resources"):
            raise VerificationError(
                f"container {container.get('name')!r} has no resource contract"
            )
    worker_startup = worker.get("startupProbe") or {}
    worker_readiness = worker.get("readinessProbe") or {}
    for probe in (worker_startup, worker_readiness):
        check_worker_probe_command((probe.get("exec") or {}).get("command") or [])
    for probe_name in ("startupProbe", "readinessProbe"):
        if ((mcp.get(probe_name) or {}).get("tcpSocket") or {}).get("port") != "mcp":
            raise VerificationError("MCP probes must target the named MCP port")
    if (
        (_env(mcp, "OKG_MCP_RO_DSN").get("valueFrom") or {})
        .get("secretKeyRef", {})
        .get("key")
        is None
    ):
        raise VerificationError("MCP must receive OKG_MCP_RO_DSN from a Secret")
    mcp_services = _objects(documents, kind="Service", component="mcp")
    if len(mcp_services) != 1:
        raise VerificationError("chart must render one MCP Service")
    if mcp_services[0]["spec"].get("type") != "ClusterIP":
        raise VerificationError("MCP Service must remain ClusterIP")
    mcp_port_services = [
        service
        for service in _objects(documents, kind="Service")
        if any(
            port.get("targetPort") == "mcp"
            for port in service.get("spec", {}).get("ports") or []
        )
    ]
    if mcp_port_services != mcp_services:
        raise VerificationError("only the MCP ClusterIP Service may expose MCP")
    network_policies = _objects(documents, kind="NetworkPolicy")
    expected_policy_count = 2 if expect_postgres else 1
    if len(network_policies) != expected_policy_count:
        raise VerificationError(
            f"expected {expected_policy_count} ingress policies"
        )
    runtime_policy = next(
        row for row in network_policies if row["metadata"]["name"].endswith("-runtime")
    )
    runtime_ingress = runtime_policy["spec"]["ingress"]
    if runtime_ingress != [
        {
            "from": [{"podSelector": {}}],
            "ports": [{"protocol": "TCP", "port": mcp_services[0]["spec"]["ports"][0]["port"]}],
        }
    ]:
        raise VerificationError("runtime NetworkPolicy must allow only same-namespace MCP")
    postgres_rows = _objects(
        documents, kind="StatefulSet", component="postgres"
    )
    if bool(postgres_rows) is not expect_postgres:
        raise VerificationError("Postgres topology does not match selected mode")
    if postgres_rows:
        postgres_pod_security = (
            postgres_rows[0]["spec"]["template"]["spec"].get("securityContext")
            or {}
        )
        if (
            postgres_pod_security.get("runAsUser") != 1000
            or postgres_pod_security.get("runAsGroup") != 1000
            or postgres_pod_security.get("fsGroup") != 1000
        ):
            raise VerificationError(
                "Postgres pod must make a fresh CSI volume writable by UID/GID 1000"
            )
        postgres_claims = postgres_rows[0]["spec"].get("volumeClaimTemplates") or []
        if len(postgres_claims) != 1:
            raise VerificationError("Postgres must own one persistent data claim")
        postgres_services = _objects(
            documents, kind="Service", component="postgres"
        )
        if len(postgres_services) != 1:
            raise VerificationError("internal Postgres requires one ClusterIP Service")
    elif _objects(documents, kind="Service", component="postgres"):
        raise VerificationError("external Postgres mode must omit Postgres Service")
    pvc_templates = runtime["spec"].get("volumeClaimTemplates") or []
    if len(pvc_templates) != 1:
        raise VerificationError("runtime must own one persistent data claim")
    for claim in [
        *pvc_templates,
        *(
            postgres_rows[0]["spec"].get("volumeClaimTemplates") or []
            if postgres_rows
            else []
        ),
    ]:
        claim_spec = claim.get("spec") or {}
        if (
            claim_spec.get("accessModes") != ["ReadWriteOnce"]
            or not claim_spec.get("storageClassName")
            or not (claim_spec.get("resources") or {}).get("requests", {}).get(
                "storage"
            )
        ):
            raise VerificationError("persistent claims require RWO class and size")
    # The budget is opt-in: on a single replica it blocks node drains. When a
    # deployment turns it on, it must be the one minAvailable=1 runtime budget.
    pdb_rows = _objects(documents, kind="PodDisruptionBudget")
    if len(pdb_rows) > 1 or any(
        row["spec"].get("minAvailable") != 1 for row in pdb_rows
    ):
        raise VerificationError(
            "runtime disruption budget must be one minAvailable=1 budget"
        )
    return {
        "object_count": len(documents),
        "kinds": sorted({str(row.get("kind")) for row in documents}),
        "runtime_containers": sorted(str(name) for name in names),
        "postgres": expect_postgres,
        "repository_sync": expect_repository_sync,
        "mcp_service_type": mcp_services[0]["spec"]["type"],
        "disruption_budget": bool(pdb_rows),
        "bootstrap_args": bootstrap["args"],
    }


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _docker_build_images(runtime_image: str, postgres_image: str) -> None:
    _run(
        [
            "docker",
            "build",
            "--build-arg",
            "VCS_REF=kubernetes-proof",
            "-t",
            runtime_image,
            ".",
        ],
        timeout=3600,
    )
    _run(
        ["docker", "build", "-t", postgres_image, "ops/pg"],
        timeout=3600,
    )


def _json_command(
    argv: list[str],
    *,
    timeout: float = 300,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> dict[str, Any]:
    proc = _run(argv, timeout=timeout, env=env, check=check)
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise VerificationError(
            f"command did not return JSON: {' '.join(argv)}"
        ) from exc
    if not isinstance(payload, dict):
        raise VerificationError(f"expected JSON object from {' '.join(argv)}")
    return payload


def _wait_for_json_ok(
    argv: list[str],
    *,
    timeout: float,
    interval: float = 5,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_payload: dict[str, Any] | None = None
    last_returncode: int | None = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        proc = _run(
            argv,
            timeout=min(remaining, 120),
            env=env,
            check=False,
        )
        last_returncode = proc.returncode
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            last_payload = payload
            if proc.returncode == 0 and payload.get("ok") is True:
                return payload
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))
    command = " ".join(argv)
    detail = json.dumps(
        {
            "returncode": last_returncode,
            "errors": (last_payload or {}).get("errors"),
            "warnings": (last_payload or {}).get("warnings"),
        },
        default=str,
    )
    raise VerificationError(
        f"timed out waiting for ok=true from {command}: {detail}"
    )


def _qa_metrics_are_clean_and_generation_aligned(
    metrics: dict[str, Any],
    *,
    generation_id: str,
) -> bool:
    queue_depth = metrics.get("queue_depth")
    return (
        str(metrics.get("generation_id") or "") == generation_id
        and isinstance(queue_depth, dict)
        and all(int(value or 0) == 0 for value in queue_depth.values())
        and int(metrics.get("dlq_count") or 0) == 0
        and int(metrics.get("generation_blocked_count") or 0) == 0
        and int(metrics.get("doctor_exit_code") or 0) == 0
    )


def _wait_for_clean_qa_metrics(
    argv: list[str],
    *,
    generation_id: str,
    timeout: float,
    interval: float = 5,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_payload: dict[str, Any] | None = None
    last_returncode: int | None = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        proc = _run(
            argv,
            timeout=min(remaining, 120),
            env=env,
            check=False,
        )
        last_returncode = proc.returncode
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            last_payload = payload
            if (
                proc.returncode == 0
                and _qa_metrics_are_clean_and_generation_aligned(
                    payload,
                    generation_id=generation_id,
                )
            ):
                return payload
            if (
                int(payload.get("dlq_count") or 0) != 0
                or int(payload.get("generation_blocked_count") or 0) != 0
                or int(payload.get("doctor_exit_code") or 0) != 0
            ):
                raise VerificationError(
                    "terminal QA metrics failure while waiting for queue "
                    "quiescence: "
                    f"generation={payload.get('generation_id')!r} "
                    f"queue_depth={payload.get('queue_depth')!r} "
                    f"dlq={payload.get('dlq_count')!r} "
                    f"blocked={payload.get('generation_blocked_count')!r} "
                    f"doctor_exit={payload.get('doctor_exit_code')!r}"
                )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))
    detail = {
        "returncode": last_returncode,
        "generation_id": (last_payload or {}).get("generation_id"),
        "queue_depth": (last_payload or {}).get("queue_depth"),
        "dlq_count": (last_payload or {}).get("dlq_count"),
        "generation_blocked_count": (
            last_payload or {}
        ).get("generation_blocked_count"),
        "doctor_exit_code": (last_payload or {}).get("doctor_exit_code"),
    }
    raise VerificationError(
        "timed out waiting for clean, generation-aligned QA metrics: "
        f"{detail!r}"
    )


def _latest_generation_id(payload: dict[str, Any]) -> str:
    candidates = payload.get("generations") or payload.get("items") or []
    if isinstance(candidates, list) and candidates:
        row = candidates[0]
        if isinstance(row, dict):
            value = (
                row.get("generation_id")
                or row.get("checkpoint_id")
                or row.get("id")
            )
            if value is not None:
                return str(value)
    for key in ("generation_id", "checkpoint_id", "latest_generation"):
        if payload.get(key) is not None:
            return str(payload[key])
    raise VerificationError("could not resolve latest generation id")


def _strict_nonnegative_int(value: object, *, label: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise VerificationError(f"{label} must be a non-negative integer: {value!r}")
    return value


def _projection_delta_shape(
    described: dict[str, Any],
) -> tuple[int, int, bool, str]:
    projection_deltas = described.get("projection_deltas")
    if not isinstance(projection_deltas, dict):
        raise VerificationError(
            "published generation omitted its projection delta receipt"
        )
    nodes_added = _strict_nonnegative_int(
        projection_deltas.get("nodes_added"),
        label="nodes_added",
    )
    edges_added = _strict_nonnegative_int(
        projection_deltas.get("edges_added"),
        label="edges_added",
    )
    projection_noop = projection_deltas.get("noop")
    if not isinstance(projection_noop, bool):
        raise VerificationError(
            "published generation noop receipt must be boolean: "
            f"{projection_noop!r}"
        )
    if nodes_added > 0 and edges_added > 0 and projection_noop is False:
        mode = "strict"
    elif (
        projection_noop is False
        and (
            (nodes_added == 0 and edges_added > 0)
            or (edges_added == 0 and nodes_added > 0)
        )
    ):
        mode = "partial_with_populated_predecessor"
    elif nodes_added == 0 and edges_added == 0 and projection_noop is True:
        mode = "explicit_noop_with_populated_predecessor"
    else:
        raise VerificationError(
            "published generation lacks strict, incremental, or explicit "
            "no-op projection shape: "
            f"nodes_added={nodes_added} edges_added={edges_added} "
            f"noop={projection_noop!r}"
        )
    return nodes_added, edges_added, projection_noop, mode


def _validate_live_evidence(
    *,
    generation_id: str,
    described: dict[str, Any],
    predecessor_described: dict[str, Any] | None = None,
    doctor: dict[str, Any],
    metrics: dict[str, Any],
    readiness: dict[str, Any],
    mcp_http: dict[str, Any],
) -> dict[str, Any]:
    described_generation_id = str(described.get("generation_id") or "")
    if described_generation_id != generation_id:
        raise VerificationError(
            "described generation does not match the requested generation: "
            f"requested={generation_id!r} described={described_generation_id!r}"
        )
    status = str(
        described.get("status")
        or (described.get("generation") or {}).get("status")
        or ""
    )
    if status != "published":
        raise VerificationError(f"generation status is not published: {status!r}")
    nodes_live = _strict_nonnegative_int(
        described.get("nodes_live_at_gen"),
        label="nodes_live_at_gen",
    )
    edges_live = _strict_nonnegative_int(
        described.get("edges_live_at_gen"),
        label="edges_live_at_gen",
    )
    if nodes_live <= 0 or edges_live <= 0:
        raise VerificationError(
            "published generation must contain live nodes and edges, got "
            f"nodes={nodes_live} edges={edges_live}"
        )
    if doctor.get("ok") is not True:
        raise VerificationError("doctor did not report ok=true")
    if readiness.get("ok") is not True:
        raise VerificationError("deployment readiness did not report ok=true")
    metrics_generation_id = str(metrics.get("generation_id") or "")
    queue_depth = metrics.get("queue_depth")
    if not _qa_metrics_are_clean_and_generation_aligned(
        metrics,
        generation_id=generation_id,
    ):
        raise VerificationError(
            "QA metrics are not clean and generation-aligned: "
            f"generation={metrics_generation_id!r} "
            f"queue_depth={queue_depth!r} "
            f"dlq={metrics.get('dlq_count')!r} "
            f"blocked={metrics.get('generation_blocked_count')!r} "
            f"doctor_exit={metrics.get('doctor_exit_code')!r}"
        )

    readiness_generation = readiness.get("generation") or {}
    ready_generation_id = str(readiness_generation.get("generation_id") or "")
    mcp = readiness.get("mcp") or {}
    mcp_generation_id = str(mcp.get("pinned_generation_id") or "")
    if (
        ready_generation_id != generation_id
        or mcp_generation_id != generation_id
        or mcp.get("ok") is not True
    ):
        raise VerificationError(
            "generation alignment failed: "
            f"described={generation_id!r} readiness={ready_generation_id!r} "
            f"mcp={mcp_generation_id!r} mcp_ok={mcp.get('ok')!r}"
        )
    mcp_http_generation_id = str(
        mcp_http.get("pinned_generation_id")
        or mcp_http.get("generation_id")
        or ""
    )
    mcp_http_node_sample_generation_id = str(
        mcp_http.get("node_sample_pinned_generation_id") or ""
    )
    mcp_http_edge_sample_generation_id = str(
        mcp_http.get("edge_sample_pinned_generation_id") or ""
    )
    if {
        mcp_http_generation_id,
        mcp_http_node_sample_generation_id,
        mcp_http_edge_sample_generation_id,
    } != {generation_id}:
        raise VerificationError(
            "MCP HTTP generation alignment failed: "
            f"described={generation_id!r} "
            f"inspect={mcp_http_generation_id!r} "
            f"nodes={mcp_http_node_sample_generation_id!r} "
            f"edges={mcp_http_edge_sample_generation_id!r}"
        )
    visible_node_sample_count = _strict_nonnegative_int(
        mcp_http.get("visible_node_sample_count"),
        label="visible_node_sample_count",
    )
    visible_edge_sample_count = _strict_nonnegative_int(
        mcp_http.get("visible_edge_sample_count"),
        label="visible_edge_sample_count",
    )
    if visible_node_sample_count <= 0 or visible_edge_sample_count <= 0:
        raise VerificationError(
            "MCP HTTP bounded graph samples are empty: "
            f"nodes={visible_node_sample_count} "
            f"edges={visible_edge_sample_count} payload={mcp_http!r}"
        )
    counts_are_null = (
        mcp_http.get("node_count") is None
        and mcp_http.get("edge_count") is None
    )
    counts_unavailable = (
        counts_are_null
        and mcp_http.get("truncated") is True
        and mcp_http.get("visibility")
        == _MCP_ENFORCE_COUNTS_UNAVAILABLE_VISIBILITY
    )
    if counts_are_null and not counts_unavailable:
        raise VerificationError(
            "MCP HTTP inspect omitted counts without the canonical Nomos "
            f"enforce visibility marker: {mcp_http!r}"
        )
    if not counts_unavailable:
        mcp_node_count = _strict_nonnegative_int(
            mcp_http.get("node_count"),
            label="MCP node_count",
        )
        mcp_edge_count = _strict_nonnegative_int(
            mcp_http.get("edge_count"),
            label="MCP edge_count",
        )
        if mcp_node_count <= 0:
            raise VerificationError(
                f"MCP HTTP inspect returned an empty graph: {mcp_http!r}"
            )
        if mcp_edge_count <= 0:
            raise VerificationError(
                "MCP HTTP inspect returned a graph without edges: "
                f"{mcp_http!r}"
            )
    (
        nodes_added,
        edges_added,
        projection_noop,
        delta_proof_mode,
    ) = _projection_delta_shape(described)
    predecessor_generation_id = str(
        described.get("predecessor_generation_id") or ""
    )
    predecessor_nodes_live: int | None = None
    predecessor_edges_live: int | None = None
    populated_predecessor = False
    if predecessor_generation_id:
        predecessor = predecessor_described or {}
        observed_predecessor_id = str(
            predecessor.get("generation_id") or ""
        )
        predecessor_status = str(predecessor.get("status") or "")
        predecessor_nodes = predecessor.get("nodes_live_at_gen")
        predecessor_edges = predecessor.get("edges_live_at_gen")
        current_deployment = str(described.get("deployment_name") or "")
        predecessor_deployment = str(
            predecessor.get("deployment_name") or ""
        )
        if (
            observed_predecessor_id == predecessor_generation_id
            and predecessor_generation_id != generation_id
            and predecessor_status == "published"
            and bool(current_deployment)
            and predecessor_deployment == current_deployment
            and isinstance(predecessor_nodes, int)
            and not isinstance(predecessor_nodes, bool)
            and predecessor_nodes > 0
            and isinstance(predecessor_edges, int)
            and not isinstance(predecessor_edges, bool)
            and predecessor_edges > 0
        ):
            predecessor_nodes_live = predecessor_nodes
            predecessor_edges_live = predecessor_edges
            populated_predecessor = True

    if (
        delta_proof_mode != "strict"
        and not populated_predecessor
    ):
        raise VerificationError(
            "published generation lacks a populated exact predecessor for "
            f"{delta_proof_mode}: "
            f"nodes_added={nodes_added} edges_added={edges_added} "
            f"noop={projection_noop!r} "
            f"predecessor={predecessor_generation_id!r} "
            f"predecessor_populated={populated_predecessor}"
        )
    count_integrity = readiness_generation.get("count_integrity") or {}
    if count_integrity.get("ok") is not True:
        raise VerificationError("readiness generation count integrity is not clean")

    queues = readiness.get("queues") or {}
    active_depth = int(queues.get("active_queue_depth") or 0)
    max_depth = int(queues.get("max_active_queue_depth") or 0)
    dlq_count = int(queues.get("dlq_count") or 0)
    if dlq_count > 0 or active_depth > max_depth:
        raise VerificationError(
            "runtime queue bounds failed: "
            f"active={active_depth} max={max_depth} dlq={dlq_count}"
        )
    return {
        "generation_id": generation_id,
        "metrics_generation_id": metrics_generation_id,
        "readiness_generation_id": ready_generation_id,
        "mcp_pinned_generation_id": mcp_generation_id,
        "mcp_http_pinned_generation_id": mcp_http_generation_id,
        "mcp_http_node_sample_pinned_generation_id": (
            mcp_http_node_sample_generation_id
        ),
        "mcp_http_edge_sample_pinned_generation_id": (
            mcp_http_edge_sample_generation_id
        ),
        "mcp_http_visible_node_sample_count": visible_node_sample_count,
        "mcp_http_visible_edge_sample_count": visible_edge_sample_count,
        "nodes_live_at_gen": nodes_live,
        "edges_live_at_gen": edges_live,
        "nodes_added": nodes_added,
        "edges_added": edges_added,
        "projection_noop": projection_noop,
        "delta_proof_ok": True,
        "delta_proof_mode": delta_proof_mode,
        "predecessor_generation_id": predecessor_generation_id or None,
        "predecessor_nodes_live": predecessor_nodes_live,
        "predecessor_edges_live": predecessor_edges_live,
        "active_queue_depth": active_depth,
        "max_active_queue_depth": max_depth,
        "dlq_count": dlq_count,
    }


def _mcp_proof_from_helm_test(output: str) -> dict[str, Any]:
    matches = re.findall(r"OKG_MCP_PROOF=(\{[^\n]+\})", output)
    if len(matches) != 1:
        raise VerificationError(
            f"expected one MCP HTTP proof in Helm test logs, found {len(matches)}"
        )
    try:
        payload = json.loads(matches[0])
    except json.JSONDecodeError as exc:
        raise VerificationError("MCP HTTP proof was not valid JSON") from exc
    if not isinstance(payload, dict):
        raise VerificationError("MCP HTTP proof must be a JSON object")
    return payload


def _helm_test_mcp_proof(
    *,
    helm: str,
    kubectl: str,
    release: str,
    namespace: str,
    env: dict[str, str],
) -> dict[str, Any]:
    helm_test = _run(
        [
            helm,
            "test",
            release,
            "-n",
            namespace,
            "--timeout",
            "10m",
        ],
        timeout=900,
        check=False,
        env=env,
    )
    pod = f"{release}-okg-readiness-test"
    logs: list[str] = []
    for container in ("readiness", "mcp"):
        result = _run(
            [
                kubectl,
                "-n",
                namespace,
                "logs",
                pod,
                "-c",
                container,
            ],
            check=False,
            env=env,
        )
        logs.append(
            f"[{container}]\n{result.stdout.strip()}\n{result.stderr.strip()}"
        )
    output = "\n".join(
        [
            helm_test.stdout.strip(),
            helm_test.stderr.strip(),
            *logs,
        ]
    )
    if helm_test.returncode:
        raise VerificationError(
            f"Helm test failed ({helm_test.returncode}):\n{output}"
        )
    return _mcp_proof_from_helm_test(output)


# The kind proof installs kubernetes-smoke, which declares the chart's default
# database name. The chart database and both DSNs must use this one name.
KIND_PROOF_DATABASE = "okg"


def kind_proof_database_sets() -> list[str]:
    return ["--set-string", f"postgresql.database={KIND_PROOF_DATABASE}"]


def kind_proof_dsn(user: str, password: str, *, release: str) -> str:
    return (
        f"postgresql://{user}:{password}@"
        f"{release}-okg-postgres:5432/{KIND_PROOF_DATABASE}"
    )


def _delete_kind_cluster(
    *,
    kind: str,
    cluster: str,
    env: dict[str, str],
) -> None:
    """Delete a disposable proof cluster and fail if Kind cannot clean it up."""
    _run(
        [kind, "delete", "cluster", "--name", cluster],
        env=env,
    )


def live_kind_proof(
    *,
    helm: str,
    kind: str,
    kubectl: str,
    deployment: str,
    keep_cluster: bool,
) -> dict[str, Any]:
    cluster = f"okg-kubernetes-proof-{os.getpid()}"
    namespace = "okg-proof"
    release = "proof"
    runtime_image = "okg-runtime:kubernetes-proof"
    postgres_image = "okg-pg17:kubernetes-proof"
    secret = "okg-runtime"
    password = "disposable-kubernetes-proof"
    mcp_password = "disposable-kubernetes-mcp-proof"
    writer_dsn = kind_proof_dsn("postgres", password, release=release)
    mcp_dsn = kind_proof_dsn("okg_mcp", mcp_password, release=release)
    scratch_dir = REPO / ".scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)
    kubeconfig_dir = Path(
        tempfile.mkdtemp(prefix=f"{cluster}-", dir=scratch_dir)
    )
    kubeconfig = kubeconfig_dir / "kubeconfig"
    command_env = os.environ.copy()
    command_env["KUBECONFIG"] = str(kubeconfig)
    created = False
    report: dict[str, Any] | None = None
    try:
        _docker_build_images(runtime_image, postgres_image)
        _run(
            [
                kind,
                "create",
                "cluster",
                "--name",
                cluster,
                "--kubeconfig",
                str(kubeconfig),
            ],
            timeout=900,
            env=command_env,
        )
        created = True
        _run(
            [kind, "load", "docker-image", runtime_image, "--name", cluster],
            env=command_env,
        )
        _run(
            [kind, "load", "docker-image", postgres_image, "--name", cluster],
            env=command_env,
        )
        _run(
            [kubectl, "create", "namespace", namespace],
            env=command_env,
        )
        _run(
            [
                kubectl,
                "-n",
                namespace,
                "create",
                "secret",
                "generic",
                secret,
                f"--from-literal=postgres-password={password}",
                f"--from-literal=okg-dsn={writer_dsn}",
                f"--from-literal=mcp-dsn={mcp_dsn}",
                f"--from-literal=mcp-password={mcp_password}",
            ],
            env=command_env,
        )
        helm_args = [
            helm,
            "upgrade",
            "--install",
            release,
            str(CHART),
            "--namespace",
            namespace,
            "--wait",
            "--timeout",
            "30m",
            "--set-string",
            f"okg.deployment={deployment}",
            "--set-string",
            "okg.readinessProfile=general",
            "--set-string",
            f"secrets.existingSecret={secret}",
            "--set-string",
            "images.okg.repository=okg-runtime",
            "--set-string",
            "images.okg.tag=kubernetes-proof",
            "--set-string",
            "images.okg.pullPolicy=Never",
            "--set-string",
            "postgresql.image.repository=okg-pg17",
            "--set-string",
            "postgresql.image.tag=kubernetes-proof",
            "--set-string",
            "postgresql.image.pullPolicy=Never",
            *kind_proof_database_sets(),
            "--set-string",
            "postgresql.storage.storageClassName=standard",
            "--set-string",
            "postgresql.storage.size=2Gi",
            "--set-string",
            "runtime.storage.storageClassName=standard",
            "--set-string",
            "runtime.storage.size=1Gi",
            "--set-string",
            "bootstrap.include[0]=pact",
            "--set-string",
            "postgresql.resources.requests.memory=1Gi",
            "--set-string",
            "postgresql.resources.limits.memory=2Gi",
            "--set-string",
            "runtime.resources.requests.memory=1Gi",
            "--set-string",
            "runtime.resources.limits.memory=3Gi",
        ]
        _run(helm_args, timeout=2400, env=command_env)
        pod = f"{release}-okg-runtime-0"
        exec_prefix = [kubectl, "-n", namespace, "exec", pod, "-c", "worker", "--"]
        generations = _json_command(
            [
                *exec_prefix,
                "okg",
                "generation",
                "list",
                "--deployment",
                deployment,
                "--limit",
                "1",
                "--json",
            ],
            env=command_env,
        )
        generation_id = _latest_generation_id(generations)
        described = _json_command(
            [
                *exec_prefix,
                "okg",
                "generation",
                "describe",
                generation_id,
                "--deployment",
                deployment,
                "--json",
            ],
            env=command_env,
        )
        doctor = _wait_for_json_ok(
            [
                *exec_prefix,
                "okg",
                "doctor",
                "--deployment",
                deployment,
                "--json",
            ],
            timeout=360,
            interval=10,
            env=command_env,
        )
        readiness = _json_command(
            [
                *exec_prefix,
                "okg",
                "deployment",
                "ready",
                deployment,
                "--checks",
                "general",
                "--json",
            ],
            timeout=600,
            env=command_env,
        )
        metrics = _wait_for_clean_qa_metrics(
            [
                *exec_prefix,
                "okg",
                "metrics",
                "--qa-snapshot",
                "--deployment",
                deployment,
                "--generation",
                generation_id,
                "--json",
            ],
            generation_id=generation_id,
            timeout=600,
            interval=10,
            env=command_env,
        )
        mcp_http = _helm_test_mcp_proof(
            helm=helm,
            kubectl=kubectl,
            release=release,
            namespace=namespace,
            env=command_env,
        )
        alignment = _validate_live_evidence(
            generation_id=generation_id,
            described=described,
            doctor=doctor,
            metrics=metrics,
            readiness=readiness,
            mcp_http=mcp_http,
        )
        report = {
            "schema": "okg.kubernetes_live_proof/v1",
            "ok": True,
            "cluster": cluster,
            "cluster_cleaned": False,
            "kubeconfig": str(kubeconfig) if keep_cluster else None,
            "deployment": deployment,
            "generation_id": generation_id,
            "generation": described,
            "doctor": doctor,
            "metrics": metrics,
            "readiness": readiness,
            "mcp_http": mcp_http,
            "alignment": alignment,
            "captured_at_unix": time.time(),
        }
        return report
    finally:
        try:
            if created and not keep_cluster:
                _delete_kind_cluster(
                    kind=kind,
                    cluster=cluster,
                    env=command_env,
                )
                if report is not None:
                    report["cluster_cleaned"] = True
        finally:
            if not keep_cluster:
                shutil.rmtree(kubeconfig_dir, ignore_errors=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--helm-bin")
    parser.add_argument("--kind-bin")
    parser.add_argument("--kubectl-bin")
    parser.add_argument("--deployment", default="kubernetes-smoke")
    parser.add_argument("--values", type=Path)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--live-kind", action="store_true")
    parser.add_argument("--keep-cluster", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.live_kind and args.values:
            raise VerificationError(
                "--live-kind proves the generic disposable deployment and cannot "
                "be combined with --values; verify an overlay on its target cluster"
            )
        helm = _tool("helm", args.helm_bin)
        internal_docs, _ = render_chart(
            helm=helm,
            deployment=args.deployment,
            values=args.values,
            internal_postgres=True,
        )
        external_docs, _ = render_chart(
            helm=helm,
            deployment=args.deployment,
            values=args.values,
            internal_postgres=False,
        )
        payload: dict[str, Any] = {
            "schema": "okg.kubernetes_chart_verification/v1",
            "ok": True,
            "internal": inspect_render(
                internal_docs,
                expect_postgres=True,
                expect_mirror=bool(args.values),
                expect_repository_sync=bool(args.values),
                expect_worker_secrets=bool(args.values),
            ),
            "external": inspect_render(
                external_docs,
                expect_postgres=False,
                expect_mirror=bool(args.values),
                expect_repository_sync=bool(args.values),
                expect_worker_secrets=bool(args.values),
            ),
        }
        if args.live_kind:
            payload["live"] = live_kind_proof(
                helm=helm,
                kind=_tool("kind", args.kind_bin),
                kubectl=_tool("kubectl", args.kubectl_bin),
                deployment=args.deployment,
                keep_cluster=args.keep_cluster,
            )
        _write_report(args.report, payload)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    except (OSError, subprocess.SubprocessError, VerificationError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
