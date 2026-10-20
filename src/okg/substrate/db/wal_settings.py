"""Checkpoint and write-ahead-log settings for OKG's own graph Postgres.

Where these settings apply. They are passed as ``postgres -c`` flags, which
override ``ALTER SYSTEM`` and ``postgresql.conf``, by exactly these paths:

- ``okg dogfood service render --adapter docker-compose`` (``cli/dogfood.py``)
  and its tracked output
  ``deployments/okg-workspace/docker-compose.okg-workspace.dogfood.yaml``;
- the sealed-environment Postgres compose (``environment_services.py``), where
  an environment's ``database.wal`` block in
  ``ops/environments/<id>/environment.yaml`` may override any of them;
- ``src/okg/substrate/library/profiles/academic-researcher/compose.yaml``.

The two static files copy the values and
``tests/substrate/db/test_wal_settings.py`` refuses any drift.
``ops/pg/docker-compose.yaml``, the local-development database that CI also
starts through ``ops/pg/docker-compose.ci.yaml``, carries only
``wal_compression=zstd``: the operator kept the checkpoint and WAL sizing
out of the CI test databases, and the same test checks that file too. The Kubernetes
chart (``ops/kubernetes/okg/templates/postgres-statefulset.yaml``) and the CI
compose files that set their own command (``ops/pg/docker-compose.bench.yaml``,
``docker-compose.required-ci.yaml``, ``docker-compose.golden.yaml``) do not get
them. ``wal_compression=zstd`` needs a Postgres built with zstd, as the
``ops/pg`` image is; a server built without it refuses to start.

Why these values. Postgres writes a full copy of a data page to the
write-ahead log (WAL) the first time the page changes after each checkpoint.
The initdb defaults (``max_wal_size=1GB``, ``checkpoint_timeout=5min``) force a
checkpoint every gigabyte of WAL, so a graph ingest that rewrites the same
pages keeps paying for fresh full-page copies. On a build host (dev 0e6171a9c0) one
code-structure first ingest wrote 22.3 GB of WAL, almost all of it full-page
images (2.66 million), and forced 41 checkpoints in 54 minutes, with 44.4 GB of
data-file writes for 6.07 GB of database growth. Re-measured with the shipped
server flags (PACT ``lane-h-pg-wal-settings``, evidence README), the same
ingest plus publish wrote about 39 GB of WAL with the defaults, 19.0 GB with
only ``wal_compression=zstd``, 17.1-17.2 GB with the values below, and
15.9 GB with ``max_wal_size=8GB``. Data-file writes were about 46 GB with any
larger cap. 4GB keeps 93% of the 8GB WAL saving at about half the disk.

Disk bound. ``max_wal_size`` is a soft cap on the ``pg_wal`` directory:
Postgres starts a checkpoint when WAL since the last one would exceed it, and
keeps at least ``min_wal_size`` of recycled segments. It can overshoot while a
checkpoint finishes. Measured peak ``pg_wal`` for the ingest above: 4.29 GB at
4GB (twice), 6.26 and 8.17 GB at 8GB, 1.07 GB at the defaults. Plan for about
4.5 GB of WAL on top of the database itself during a large import.
"""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Mapping

GRAPH_WAL_SETTINGS: Mapping[str, str] = MappingProxyType(
    {
        # Measured: 4-7 WAL-forced checkpoints per ingest (8GB: 1), within 7%
        # of 8GB's WAL volume, at half its peak disk use.
        "max_wal_size": "4GB",
        # Keep recycled WAL segments instead of creating and deleting files.
        "min_wal_size": "1GB",
        "checkpoint_timeout": "15min",
        # Spread checkpoint writes over 90% of the interval (also the PG 17
        # default; stated so the rendered command is self-describing).
        "checkpoint_completion_target": "0.9",
        # Compress full-page images in the WAL. The ops/pg image's PG 17 is
        # built --with-zstd.
        "wal_compression": "zstd",
    }
)


_SETTING_VALUE_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def resolve_wal_settings(overrides: Any) -> Mapping[str, str]:
    """Overlay an environment's ``database.wal`` mapping on the defaults.

    Only the settings named in :data:`GRAPH_WAL_SETTINGS` may be overridden.
    An unknown name or a value that is not a plain token raises
    ``ValueError`` rather than being dropped, so a typo cannot silently
    leave a database on the defaults.
    """
    resolved = dict(GRAPH_WAL_SETTINGS)
    if overrides is None:
        return MappingProxyType(resolved)
    if not isinstance(overrides, Mapping):
        raise ValueError("must be a mapping of Postgres setting name to value")
    unknown = sorted(str(name) for name in overrides if name not in resolved)
    if unknown:
        raise ValueError(
            "unknown setting(s) " + ", ".join(unknown)
            + "; allowed: " + ", ".join(GRAPH_WAL_SETTINGS)
        )
    for name, value in overrides.items():
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError(f"{name} must be a string or number")
        text = str(value)
        if not _SETTING_VALUE_RE.fullmatch(text):
            raise ValueError(f"{name} value {text!r} is not a plain setting value")
        resolved[str(name)] = text
    return MappingProxyType(resolved)


def graph_wal_command_args(settings: Mapping[str, str] | None = None) -> list[str]:
    """Return settings as ``postgres -c name=value`` pairs (defaults if None)."""
    args: list[str] = []
    for name, value in (GRAPH_WAL_SETTINGS if settings is None else settings).items():
        args.extend(("-c", f"{name}={value}"))
    return args
