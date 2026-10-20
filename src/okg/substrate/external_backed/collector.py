"""Host-metrics collector for external-backed nodes.

A scheduled, config-driven collector that probes each monitored host - a local
subprocess for ``local``, ssh (BatchMode) for ``user@host`` - parses
load/cpu/mem/disk, and writes one ``node_series`` sample per metric via
``record_sample``. Reachability is a first-class ``reachable`` sample (1.0/0.0):
an unreachable or failing host records ``reachable=0`` and the run continues for
the others - never a silent gap. It also ensures one ``compute_host`` graph node
per host carrying a ``latest_from_series`` handle, so ``inspect``-with-live
shows the current metrics.

The collector (not the read path) holds ssh access; ssh uses the operator's own
agent/config and no credentials are stored. Generic substrate code - it takes
``deployment_name`` as a parameter and never branches on it.
"""

from __future__ import annotations
from okg.substrate import fastyaml

import subprocess
from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable

import psycopg

from okg.substrate.external_backed.series import _now as _sample_clock, record_sample
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact

HOST_SUBTYPE = "compute_host"
NUMERIC_METRICS = (
    "load1",
    "cpu_count",
    "mem_used_pct",
    "mem_used_bytes",
    "mem_total_bytes",
    "disk_used_pct",
    "disk_used_bytes",
    "disk_total_bytes",
)
# Per-metric freshness TTL (seconds): load/mem/reachable move fast; cpu/disk slow.
_TTL = {
    "load1": 600,
    "mem_used_pct": 600,
    "mem_used_bytes": 600,
    "mem_total_bytes": 600,
    "reachable": 600,
    "disk_used_pct": 3600,
    "disk_used_bytes": 3600,
    "disk_total_bytes": 3600,
    "cpu_count": 86400,
}

# Portable POSIX probe: emits `key=value` lines. Linux reads /proc; macOS uses
# sysctl/vm_stat. Fed to the local or remote shell via stdin (no quoting games).
PROBE_SNIPPET = r"""
if [ -r /proc/loadavg ]; then
  L=$(awk '{print $1}' /proc/loadavg)
else
  L=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}')
fi
C=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null)
set -- $(df -Pk / | awk 'NR==2{gsub("%","",$5);printf "%s %.0f %.0f\n",$5,$2*1024,$3*1024}')
D=$1
D_TOTAL=$2
D_USED=$3
if [ -r /proc/meminfo ]; then
  M_TOTAL=$(awk '/^MemTotal:/{printf "%.0f",$2*1024}' /proc/meminfo)
  M_USED=$(awk '/^MemTotal:/{t=$2}/^MemAvailable:/{a=$2}END{if(t>0)printf "%.0f",(t-a)*1024}' /proc/meminfo)
  M=$(awk '/^MemTotal:/{t=$2}/^MemAvailable:/{a=$2}END{if(t>0)printf "%.1f",(t-a)*100/t}' /proc/meminfo)
else
  PS=$(sysctl -n hw.pagesize 2>/dev/null)
  TOT=$(sysctl -n hw.memsize 2>/dev/null)
  USED=$(vm_stat 2>/dev/null | awk -v ps="$PS" '
    /Pages active/{gsub("\\.","",$3);a=$3}
    /Pages wired down/{gsub("\\.","",$4);w=$4}
    /occupied by compressed/{gsub("\\.","",$5);c=$5}
    END{print (a+w+c)*ps}')
  M=$(awk -v t="$TOT" -v u="$USED" 'BEGIN{if(t>0)printf "%.1f",u*100/t}')
  M_TOTAL=$TOT
  M_USED=$USED
fi
printf 'load1=%s\ncpu_count=%s\ndisk_used_pct=%s\ndisk_used_bytes=%s\ndisk_total_bytes=%s\nmem_used_pct=%s\nmem_used_bytes=%s\nmem_total_bytes=%s\n' "$L" "$C" "$D" "$D_USED" "$D_TOTAL" "$M" "$M_USED" "$M_TOTAL"
"""


def _freeze_metadata(value: Any) -> Any:
    """Freeze bounded host metadata before it crosses the collector boundary."""
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_metadata(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_metadata(item) for item in value)
    return value


def _thaw_metadata(value: Any) -> Any:
    """Return JSON-compatible copies of immutable host metadata."""
    if isinstance(value, Mapping):
        return {str(key): _thaw_metadata(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_metadata(item) for item in value]
    return value


@dataclass(frozen=True)
class Host:
    name: str
    node_id: str
    target: str  # "local" | "user@host"
    label: str = ""
    # Extra ssh options (e.g. ["-i", "~/.ssh/key"]); path-like opts are
    # ~-expanded. Lets a host use a non-default identity file / jump host.
    ssh_opts: tuple[str, ...] = ()
    # Bounded, non-secret state supplied by a trusted dynamic inventory. The
    # value is recursively frozen so callers cannot mutate a fact after it has
    # been assembled; static hosts retain the empty default.
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _freeze_metadata(self.metadata))


ProbeFn = Callable[[Host], "tuple[bool, dict[str, float]]"]


def parse_probe_output(text: str) -> dict[str, float]:
    """Parse `key=value` lines into typed numeric metrics (unknown/blank skipped)."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key in NUMERIC_METRICS and value:
            try:
                out[key] = float(value)
            except ValueError:
                # Ignore malformed metric values; absent is safer than guessed.
                pass
    return out


def probe_host(host: Host, *, timeout: int = 10) -> tuple[bool, dict[str, float]]:
    """Run the probe locally or over ssh. Returns (reachable, metrics).

    Any failure (non-zero exit, timeout, ssh/auth error) => (False, {}). The
    caller records reachable=0; the run continues.
    """
    try:
        if host.target == "local":
            proc = subprocess.run(
                ["sh", "-s"],
                input=PROBE_SNIPPET,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        else:
            import os

            opts = [os.path.expanduser(o) if "/" in o else o for o in host.ssh_opts]
            proc = subprocess.run(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    f"ConnectTimeout={timeout}",
                    *opts,
                    host.target,
                    "sh",
                ],
                input=PROBE_SNIPPET,
                capture_output=True,
                text=True,
                timeout=timeout + 5,
            )
    except (subprocess.TimeoutExpired, OSError):
        return False, {}
    if proc.returncode != 0:
        return False, {}
    return True, parse_probe_output(proc.stdout)


def host_node_fact(host: Host) -> NodeFact:
    """A compute_host node carrying a latest_from_series handle over the metrics."""
    handle = {
        metric: {
            "current": "latest_from_series",
            "series": {"store": "timescaledb", "metric": metric},
            "ttl_seconds": _TTL[metric],
        }
        for metric in (*NUMERIC_METRICS, "reachable")
    }
    attrs: dict[str, Any] = {
        "label": host.label or host.name,
        "host_target": host.target,
        "_external": handle,
    }
    if host.metadata:
        # Keep fleet state separate from the stable host identity and never
        # serialize SSH options, credentials, or controller-only fields.
        attrs["runner_storage"] = _thaw_metadata(host.metadata)
    return NodeFact(
        node_id=host.node_id,
        subtype=HOST_SUBTYPE,
        attrs=attrs,
        source_record_id={"host": host.name},
        source_revision={"v": 1},
        op="I",
    )


def run_collection(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    hosts: list[Host],
    probe_fn: ProbeFn = probe_host,
    write_guard: Callable[[], AbstractContextManager[Any]] | None = None,
) -> dict[str, Any]:
    """Probe every host, write samples + reachable, ensure host nodes.

    Returns a structured per-host outcome for operator inspection.

    ``write_guard`` is entered around EVERY okg write and around none of the
    probes (issue #1338). A scheduled caller used to hold the deployment
    writer permit for the whole call, and that permit is a
    ``pg_advisory_xact_lock`` -- an open Postgres transaction -- so an ssh
    probe run measured in minutes held a transaction and its lock for that
    long. The probes themselves talk only to hosts.

    Samples are therefore BUFFERED through the probe loop and written with the
    fact batch rather than one at a time as each host answers. Writing them as
    they arrived would leave `okg.node_series` inserts committing outside the
    guard for the whole probe window, which is the ordering the whole-call
    hold used to provide and the reason to narrow it rather than drop it.
    Each sample carries the instant it was observed, so buffering does not
    move any timestamp.
    """
    outcome: dict[str, Any] = {}
    facts: list[NodeFact] = []
    samples: list[dict[str, Any]] = []
    for host in hosts:
        reachable, metrics = probe_fn(host)
        observed_at = _sample_clock()
        # Reachability is the metric VALUE (1.0/0.0). The sample status is `ok`:
        # we successfully determined reachability either way.
        samples.append({
            "node_id": host.node_id,
            "metric": "reachable",
            "value": 1.0 if reachable else 0.0,
            "observed_at": observed_at,
            "source": f"resource_monitor:{host.target}",
        })
        for metric, value in metrics.items():
            samples.append({
                "node_id": host.node_id,
                "metric": metric,
                "value": value,
                "observed_at": observed_at,
                "source": f"resource_monitor:{host.target}",
            })
        facts.append(host_node_fact(host))
        outcome[host.name] = {
            "reachable": reachable,
            "metrics": metrics,
            "node_id": host.node_id,
            "metadata": _thaw_metadata(host.metadata),
        }

    # Ensure the compute_host nodes under bounded system authority. Publisher
    # remains the only operation that can checkpoint them for readers.
    from okg.substrate.fact_application import apply_bounded_fact_batch

    with (write_guard() if write_guard is not None else nullcontext()):
        for sample in samples:
            record_sample(
                conn,
                deployment_name=deployment_name,
                node_id=sample["node_id"],
                metric=sample["metric"],
                value=sample["value"],
                observed_at=sample["observed_at"],
                source=sample["source"],
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            row = cur.fetchone()
        catalog_version_id = int(row[0]) if row and row[0] is not None else None
        apply_bounded_fact_batch(
            conn,
            ChronosGraphStore.from_connection(conn),
            facts,
            deployment_name=deployment_name,
            producer_id="system:external_backed_collector",
            producer_kind="system",
            intent="external_backed_collection",
            catalog_version_id=catalog_version_id,
            authority_scope={
                "hosts": sorted(host.name for host in hosts),
                "subtype": HOST_SUBTYPE,
                "completed_scope": True,
            },
            source="system:external_backed_collector",
            strict_endpoints=True,
        )
    return {"hosts": outcome, "host_count": len(hosts)}


def load_hosts(path: str | Path) -> list[Host]:
    """Load the monitored-hosts config (deployment-shaped)."""

    doc = fastyaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    out: list[Host] = []
    for raw in doc.get("hosts", []):
        out.append(
            Host(
                name=str(raw["name"]),
                node_id=str(raw["node_id"]),
                target=str(raw["target"]),
                label=str(raw.get("label", "")),
                ssh_opts=tuple(str(o) for o in raw.get("ssh_opts", [])),
            )
        )
    return out


__all__ = [
    "Host",
    "PROBE_SNIPPET",
    "HOST_SUBTYPE",
    "NUMERIC_METRICS",
    "parse_probe_output",
    "probe_host",
    "host_node_fact",
    "run_collection",
    "load_hosts",
]
