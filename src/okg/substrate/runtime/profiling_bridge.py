"""Shared helper for entering a profiling context inside a DBOS
workflow body.

Per ``restore-publisher-perf-and-profiling`` §B. The substrate's
existing ``profile_stage(...)`` hooks (24 of them in
``publisher.py``, plus others in ingest / extraction / linkers)
emit telemetry into the *currently active* profiling context.
On the CLI path (``okg run --once``, ``okg sync ...``), the CLI
establishes that context. On the DBOS-driven path the context
isn't established by anyone, so the hooks are no-ops.

This module bridges the gap: each DBOS-resident workflow body calls
:func:`workflow_profiling_context` to wrap its real work, reusing
the existing manifest-level ``profiling:`` block + ``resolve_policy``
helper. No new manifest schema, no new config.
"""
from __future__ import annotations

import contextlib
from typing import Iterator

from okg.substrate.profiling import (
    profile_stage,  # noqa: F401 — re-exported convenience
    profiling_context,
    resolve_policy,
)
from okg.substrate.profiling.context import ProfileContext


__all__ = [
    "profile_stage",
    "workflow_profiling_context",
]


@contextlib.contextmanager
def workflow_profiling_context(
    *,
    manifest,  # DeploymentManifest
    deployment: str,
    target_kind: str,
    command: str,
    profile: str | bool | None = None,
    collectors: tuple[str, ...] | list[str] = (),
    force_deep: bool = False,
) -> Iterator[ProfileContext]:
    """Enter a profiling context for a DBOS workflow body.

    Reads policy from ``manifest.profiling`` (top-level field on
    ``DeploymentManifest``, already in place since the
    ``add-system-profiling`` proposal). Substrate defaults apply
    when the manifest doesn't set the block.

    Inside the context, attempts to link the active DBOS
    workflow_uuid into the ``ProfileContext.request_path`` so the
    ``okg runtime debug-workflow`` CLI can surface the artifact
    path. Linkage failure (no DBOS context, e.g. unit test path)
    is silently tolerated.

    Parameters
    ----------
    manifest : DeploymentManifest
        The resolved deployment manifest. Its top-level ``profiling``
        attribute is read to build the ``ProfilingPolicy``.
    deployment : str
        Deployment name. Used as the artifact root scope.
    target_kind : str
        Profile target category. Used by the artifact bundle to
        identify the workflow class (``"publish"``, ``"source_sync"``,
        ``"embed"``, ``"hygiene"``, etc.).
    command : str
        Specific workflow name (e.g. ``"okg_publisher_cycle"``).
        Recorded in the bundle for cross-reference with
        ``dbos.workflow_status.name``.

    Yields
    ------
    ProfileContext
        The active profile context. Callers can mutate
        ``prof.generation_id`` etc. before the context exits.
    """
    policy = resolve_policy(
        getattr(manifest, "profiling", None),
        profile=profile,
        collectors=tuple(collectors),
    )
    with profiling_context(
        deployment=deployment,
        target_kind=target_kind,
        command=command,
        policy=policy,
        force_deep=force_deep,
    ) as prof:
        # Link DBOS workflow_uuid into the profile so the CLI's
        # `okg runtime debug-workflow` can surface the artifact path.
        try:
            from dbos import DBOS
            wf_id_attr = getattr(DBOS, "workflow_id", None)
            wf_id = (
                wf_id_attr() if callable(wf_id_attr) else wf_id_attr
            )
            if isinstance(wf_id, str) and wf_id:
                prof.request_path = wf_id
        except Exception:  # noqa: BLE001
            # Not in a workflow context (test path, CLI inlining,
            # etc.). The artifact still writes; it just won't link
            # back to a DBOS row.
            pass
        yield prof
