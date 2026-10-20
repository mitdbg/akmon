"""Source-lifecycle MCP tools: list / add / remove / status.

Extracted from ``mcp/source_admin.py`` by
``extract-mcp-source-lifecycle`` (2026-05). Four MCP tools live
here, each operating on `source_registry.yaml` for a single
deployment:

  - ``list_sources(conn, deployment)`` — enumerate configured
    sources + per-source health snapshot.
  - ``add_source(conn, deployment, short_name, params, *,
    dry_run=False)`` — wire one source and run a scoped ingest.
  - ``remove_source(conn, deployment, short_name)`` — remove
    one source registration.
  - ``source_status(conn, deployment, short_name)`` — inspect
    one source's connection state + watermark + counts.

Plus the two private helpers ``_apply_source_with_ingest``
(adds + ingests + handles rollback) and ``_scoped_ingest``
(subprocess-runs the scoped ingest CLI).

``mcp/source_admin.py`` re-exports all five names so server.py's
thin wrappers (`_source_admin.list_sources` etc.) keep working,
and so ``commit_setup`` — which calls ``add_source`` directly
— resolves it via the source_admin module attribute.
"""
from __future__ import annotations

import copy
import fcntl
import hashlib
import logging
import os
import subprocess
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import psycopg
import yaml

from okg.substrate.mcp.source_catalog import (
    SOURCE_CATALOG,
)
from okg.substrate.nomos.core import looks_secret_key
from okg.substrate.sources.redaction import redact_mapping


@contextmanager
def _registry_mutation_lock(registry_path: Path) -> Iterator[None]:
    """Serialize every MCP registry read-modify-write on this host."""
    lock_key = hashlib.sha256(str(registry_path.resolve()).encode()).hexdigest()
    lock_path = Path(tempfile.gettempdir()) / f"okg-source-registry-{lock_key}.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _atomic_write_registry(registry_path: Path, raw: Mapping[str, Any]) -> None:
    """Replace a registry atomically without exposing a truncated YAML file."""
    payload = yaml.safe_dump(dict(raw), sort_keys=False)
    mode = registry_path.stat().st_mode & 0o777
    fd, temporary_name = tempfile.mkstemp(
        dir=registry_path.parent,
        prefix=f".{registry_path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, mode)
        os.replace(temporary_path, registry_path)
        directory_fd = os.open(registry_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary_path.unlink(missing_ok=True)


_log = logging.getLogger(__name__)

#: Valid secret-reference URI prefixes (per require-secret-refs-not-literals).
_SECRET_REF_PREFIXES = ("secret://", "vault://", "keyring://", "aws-sm://")


def _param_is_secret_ref(value: Any) -> bool:
    """True if a param value is a secret reference (a secret:// URI string or a
    {'secret_ref': 'secret://...'} mapping), not a literal secret."""
    if isinstance(value, str):
        return value.startswith(_SECRET_REF_PREFIXES)
    if isinstance(value, Mapping):
        ref = value.get("secret_ref")
        return isinstance(ref, str) and ref.startswith(_SECRET_REF_PREFIXES)
    return False


def _literal_secret_param_keys(params: Mapping[str, Any] | None) -> list[str]:
    """Secret-class param keys carrying a literal value (not a secret_ref)."""
    return [
        str(key)
        for key, value in (params or {}).items()
        if looks_secret_key(str(key)) and not _param_is_secret_ref(value)
    ]


def _redact_params(params: Any) -> dict[str, Any]:
    """Redact secret-bearing params before echoing them in an MCP envelope."""
    return redact_mapping(dict(params or {}))


#: Admission statuses that mean the payload was REFUSED and contributed
#: nothing. A source stuck on one of these produces no run row, no
#: watermark, and no fact-log rows, so every other health surface reads it
#: as silence (issue #1323, `okg-workspace.latex_project`).
#:
#: `blocked` and `quarantined` are deliberately NOT here. `blocked` is stamped
#: on every pending/accepted envelope in a deployment when a publish
#: generation blocks (`admission._mark_generation_blocked`) -- those payloads
#: WERE admitted, and calling them refused would flip every actively-ingesting
#: source in the deployment to a refusal it never had. `quarantined`
#: admissions carry isolated or compensated facts, so they wrote something
#: too. Both get their own label below.
ADMISSION_REFUSAL_STATUSES = ("rejected", "failed")

#: Created but never decided, or decided but never published.
ADMISSION_UNDECIDED_STATUSES = ("pending", "accepted")

#: Every status worth counting as a non-clean outcome, for the lifetime
#: tally only. Membership here makes no claim about what was written.
ADMISSION_ADVERSE_STATUSES = (
    *ADMISSION_REFUSAL_STATUSES,
    "blocked",
    "quarantined",
)


# Defer-import shim for source_admin internals.
_counts_for: Any = None
_last_admission_outcome: Any = None
_last_run_counts: Any = None
_latest_source_progress: Any = None
_load_registry_yaml: Any = None
_preflight_status: Any = None
_repo_root_from_cwd: Any = None
_watermark_for: Any = None


def _load_source_admin_helpers() -> None:
    """Bind source_admin helpers on first call. None-guarded so
    test monkeypatches survive the first invocation."""
    global _counts_for, _last_admission_outcome
    global _last_run_counts, _latest_source_progress
    global _load_registry_yaml
    global _preflight_status, _repo_root_from_cwd
    global _watermark_for
    from okg.substrate.mcp.source_admin import (
        _counts_for as _cf,
        _last_admission_outcome as _lao,
        _last_run_counts as _lrc,
        _latest_source_progress as _lsp,
        _load_registry_yaml as _lry,
        _preflight_status as _ps,
        _repo_root_from_cwd as _rrfc,
        _watermark_for as _wf,
    )
    if _counts_for is None:
        _counts_for = _cf
    if _last_admission_outcome is None:
        _last_admission_outcome = _lao
    if _last_run_counts is None:
        _last_run_counts = _lrc
    if _latest_source_progress is None:
        _latest_source_progress = _lsp
    if _load_registry_yaml is None:
        _load_registry_yaml = _lry
    if _preflight_status is None:
        _preflight_status = _ps
    if _repo_root_from_cwd is None:
        _repo_root_from_cwd = _rrfc
    if _watermark_for is None:
        _watermark_for = _wf


def _admission_producer_id(entry: Mapping[str, Any], ownership_id: str) -> str:
    """Producer id `okg.admission_envelopes` is keyed by for this source.

    A registry entry may declare `admission_policy.producer_id` independently
    of `ownership_id`; `sources/registry.py` falls back to `ownership_id` only
    when it is absent, and `ingest/service.py` stamps envelopes with whichever
    one applies. Reading envelopes by `ownership_id` alone would return zero
    rows for every source that sets its own producer id, and report it as
    never observed -- the same false negative this change exists to remove.
    """
    policy = entry.get("admission_policy")
    if isinstance(policy, Mapping):
        declared = policy.get("producer_id")
        if declared:
            return str(declared)
    return ownership_id


def _entry_is_explicitly_dormant(entry: Mapping[str, Any]) -> bool:
    """Whether the registry DELIBERATELY turned this source off.

    Only an explicit `enabled_by_default: false` counts, mirroring
    `source_sync.source_sync_entry_is_explicitly_dormant`, which is the rule
    the source-sync scheduler applies. An ABSENT flag is runnable.

    This matters because `_preflight_status` reports `disabled` for any entry
    without an explicit `true`, and 29 of okg-workspace's 30 sources -- among
    them `latex-project`, the source issue #1323 is about -- write no flag at
    all. Letting that `disabled` outrank the admission outcome would make the
    refusal label unreachable for every source that actually runs.
    """
    return entry.get("enabled_by_default") is False


def _admission_producer_kind(entry: Mapping[str, Any]) -> str:
    """Producer kind this source's own envelopes carry.

    Used to scope the `source` match: system producers (entity merge, source
    retirement, lineage repair) stamp a registry source's ownership id in
    `source` while owning `producer_kind: "system"`, and attributing their
    verdicts to the source would misdirect the operator.
    """
    policy = entry.get("admission_policy")
    if isinstance(policy, Mapping):
        declared = policy.get("producer_kind")
        if declared:
            return str(declared)
    return "source"


def _runtime_observed(
    *,
    watermark: Any,
    last_run: Any,
    latest_progress: Any = None,
    admission: Mapping[str, Any],
) -> bool:
    """Whether this source has been seen doing anything at all.

    Shared by `list_sources` and `source_status` so the two verbs cannot
    disagree about the same source.
    """
    return bool(
        watermark
        or last_run
        or latest_progress
        or (admission.get("envelope_count") or 0) > 0
    )


def _source_status_label(
    *,
    setup_status: str,
    runtime_observed: bool,
    other_observed: bool,
    admission: Mapping[str, Any],
    explicitly_dormant: bool = False,
) -> str:
    """Status label for one registry source.

    Precedence, in order:

      1. `needs_connection` wins. It names a concrete operator action, and
         one blocked publish generation -- which stamps `blocked` on every
         pending envelope in the deployment -- must not relabel the census
         out from under it. The admission outcome is carried beside it in
         `recent_error` rather than replacing it.
      2. An EXPLICITLY dormant source (`enabled_by_default: false`) keeps
         `disabled` / `observed`. The registry deliberately turned it off.
      3. Everything else reaches the admission labels -- including a source
         `_preflight_status` calls `disabled` only because the flag is
         ABSENT, which is how 29 of okg-workspace's 30 sources are written,
         `latex-project` among them. Letting that `disabled` take precedence
         made this whole signal unreachable for every source that runs.

    The states added for issue #1323, all previously reported as an
    unremarkable `active` source with zero counts:

      - `admission_rejected` — the newest admission was refused and NOTHING
        else was ever written. It contributes nothing and writes no run row,
        so this was exactly the silence the operator saw.
      - `admission_rejected_recent` — the newest admission was refused but
        the source has watermarks, progress or run rows from before. It is
        producing; the refusal is a recent failure, not a lifetime of
        silence, and saying it "wrote no rows" would be false.
      - `publish_blocked` — admitted, then marked blocked by a publish
        generation. NOT a refusal: the payload was accepted, its facts exist,
        and the fault lies with the generation.
      - `quarantined` — admitted into isolation rather than refused.
      - `admission_pending` — created but never decided or never published,
        with nothing else ever written.
      - `never_observed` — registry-active with no watermark, no
        source-progress row, and no admission envelope. Whether supervision
        never scheduled it or every attempt died before admission, the
        operator must be told, not left to infer health.

    An unreadable admission surface downgrades to `admission_unknown` rather
    than to `active`: absence of a readable refusal is not evidence that
    there was none.
    """
    admission_status = str(admission.get("status") or "")
    if setup_status == "needs_connection":
        # Names a concrete operator action; an admission outcome must not
        # replace it. The outcome is still carried in `recent_error`.
        return setup_status
    if explicitly_dormant:
        # The registry DELIBERATELY turned this source off.
        return "observed" if runtime_observed else "disabled"
    # Everything else is runnable: either `active`, or `disabled` only
    # because `_preflight_status` reads an absent `enabled_by_default` as
    # off -- which 29 of okg-workspace's 30 sources are, `latex-project`
    # included. Suppressing the admission outcome here would make the whole
    # signal unreachable for every source that actually runs.
    if admission_status in ADMISSION_REFUSAL_STATUSES:
        return (
            "admission_rejected_recent"
            if other_observed
            else "admission_rejected"
        )
    if admission_status == "blocked":
        return "publish_blocked"
    if admission_status == "quarantined":
        return "quarantined"
    if admission.get("authority") == "unavailable":
        return "admission_unknown"
    if admission_status in ADMISSION_UNDECIDED_STATUSES and not other_observed:
        # Created but never decided or never published, and nothing else was
        # ever written. Counting this as ordinary health would make a stalled
        # source look better than the silence this change replaces.
        return "admission_pending"
    if not runtime_observed:
        return "never_observed"
    if setup_status != "active":
        # Runnable, observed, but carrying no explicit `enabled_by_default`.
        # `observed` is the label this case had before issue #1323.
        return "observed"
    return setup_status


#: Per-outcome operator guidance. Each says what actually happened, because a
#: hint that misnames the outcome sends the reader to audit the wrong thing --
#: the failure mode this change exists to remove. Keyed by
#: (status, whether the source has produced anything else), because "wrote no
#: rows" is only true for a source that never produced.
_ADMISSION_HINTS = {
    ("rejected", False): (
        "This source ran and its payload was refused at admission, so it "
        "wrote no source-run row and contributed no rows. Read the verdict "
        "in okg.admission_envelopes for this producer; "
        "okg.v_source_run_diagnostics will not show it."
    ),
    ("rejected", True): (
        "This source has produced before, and its most recent payload was "
        "refused at admission. That attempt wrote no source-run row, so it "
        "is invisible in okg.v_source_run_diagnostics; read the verdict in "
        "okg.admission_envelopes."
    ),
    ("failed", False): (
        "This source ran and its admission failed, so it wrote no "
        "source-run row and contributed no rows. Read the verdict in "
        "okg.admission_envelopes for this producer; "
        "okg.v_source_run_diagnostics will not show it."
    ),
    ("failed", True): (
        "This source has produced before, and its most recent admission "
        "failed. That attempt wrote no source-run row; read the verdict in "
        "okg.admission_envelopes."
    ),
    ("blocked", False): (
        "This source's payload was ADMITTED and then blocked by a publish "
        "generation, so its facts exist and the block is not this source's "
        "fault. Investigate the blocked generation, not the producer "
        "signature."
    ),
    ("quarantined", False): (
        "This source's payload was admitted into quarantine rather than "
        "refused, so its facts exist in isolation or compensated form. "
        "Investigate the quarantine branch."
    ),
}
_ADMISSION_HINTS[("blocked", True)] = _ADMISSION_HINTS[("blocked", False)]
_ADMISSION_HINTS[("quarantined", True)] = _ADMISSION_HINTS[
    ("quarantined", False)
]


def _admission_recent_error(
    admission: Mapping[str, Any],
    other_observed: bool = False,
) -> dict[str, Any] | None:
    """Operator-facing envelope for an adverse admission outcome, else None.

    `other_observed` selects the hint: "wrote no rows" is only true for a
    source that has produced nothing else, and asserting it for a source with
    a watermark would be the same false claim this change removes elsewhere.
    """
    status = str(admission.get("status") or "")
    if status not in ADMISSION_ADVERSE_STATUSES:
        return None
    return {
        "surface": "admission",
        "status": status,
        "refused": status in ADMISSION_REFUSAL_STATUSES,
        "producer_id": admission.get("producer_id"),
        "produced_before": bool(other_observed),
        "reason_code": admission.get("reason_code"),
        "message": admission.get("reason_message"),
        "at": admission.get("created_at"),
        "adverse_admissions": admission.get("adverse_count"),
        "agent_hint": _ADMISSION_HINTS[(status, bool(other_observed))],
    }


def list_sources(
    conn: psycopg.Connection,
    deployment: str,
) -> dict:
    """Enumerate every source in the deployment's registry.

    Returns:
      {
        "status": "ok",
        "deployment": "<name>",
        "authority": "source_registry_admin",
        "health_authority": "...",
        "sources": [
          {
            "name": "dblp_by_orcid",
            "enabled_by_default": true,
            "setup_status": "active",
            "runtime_observed": true,
            "status": "active",
            "last_ingest_at": "2026-05-21T01:23:45+00:00",
            "node_count": 1234,
            "edge_count": 5678,
            "last_admission": {...},
            "recent_error": null,
          }, ...
        ],
        "counts": {"active": 7, "needs_connection": 1, ...},
      }

    `status` is the setup state (`active`, `needs_connection`, `disabled`,
    `observed`) for every source whose setup state is not `active`. An
    otherwise-active source may instead report one of the admission states
    `_source_status_label` documents: `admission_rejected`,
    `admission_rejected_recent`, `publish_blocked`, `quarantined`,
    `admission_pending`, `admission_unknown`, or `never_observed`. The
    `counts` histogram is keyed by whichever label each source received.
    """
    _load_source_admin_helpers()
    try:
        _path, raw = _load_registry_yaml(deployment)
    except FileNotFoundError as e:
        return {
            "status": "deployment_not_found",
            "error": str(e),
            "cli_fallback": (
                f"okg install --profile academic-researcher --orcid <id> "
                f"--name {deployment}"
            ),
        }
    sources = raw.get("sources") or {}
    out_sources = []
    counts: dict[str, int] = {}
    # One memo scope for the pass, so the per-source visibility gate reads
    # the active policy bundle once rather than twice per source (subject
    # lookup + evaluation). Nothing in the pass activates a policy.
    from okg.substrate.commit_scope import read_pass_scope

    with read_pass_scope():
        visible = {
            source_id
            for source_id in sorted(sources)
            if _source_visible_for_subject(conn, deployment, source_id)
        }
    for source_id, entry in sorted(sources.items()):
        if source_id not in visible:
            continue
        ownership_id = entry.get("ownership_id") or source_id
        wm = _watermark_for(conn, ownership_id)
        c = _counts_for(conn, ownership_id)
        setup_status = _preflight_status(source_id, entry)
        last_run = _last_run_counts(conn, deployment, ownership_id)
        latest_progress = _latest_source_progress(
            conn, deployment, ownership_id,
        )
        admission = _last_admission_outcome(
            conn,
            deployment,
            ownership_id,
            _admission_producer_id(entry, ownership_id),
            _admission_producer_kind(entry),
        )
        other_observed = bool(wm or last_run or latest_progress)
        runtime_observed = _runtime_observed(
            watermark=wm,
            last_run=last_run,
            latest_progress=latest_progress,
            admission=admission,
        )
        status_label = _source_status_label(
            setup_status=setup_status,
            runtime_observed=runtime_observed,
            other_observed=other_observed,
            explicitly_dormant=_entry_is_explicitly_dormant(entry),
            admission=admission,
        )
        recent_error = _admission_recent_error(
            admission, other_observed,
        )
        count_authority = "fact_log"
        if c["node_count"] == 0 and c["edge_count"] == 0:
            count_authority = (
                "last_run" if last_run is not None else "unavailable"
            )
        counts[status_label] = counts.get(status_label, 0) + 1
        out_sources.append({
            "name": source_id,
            "source_id": source_id,
            "ownership_id": ownership_id,
            "aliases": _aliases_for_registry_source(source_id),
            "authority": "source_registry_admin",
            "setup_status": setup_status,
            "runtime_observed": runtime_observed,
            "runtime_progress": latest_progress,
            "last_admission": admission,
            "recent_error": recent_error,
            "enabled_by_default": bool(
                entry.get("enabled_by_default", False),
            ),
            "status": status_label,
            "last_ingest_at": wm.get("last_ingest_at"),
            "node_count": c["node_count"],
            "edge_count": c["edge_count"],
            "count_authority": count_authority,
            "last_run_id": last_run.get("run_id") if last_run else None,
            "last_run_nodes_appended": (
                last_run.get("nodes_appended") if last_run else None
            ),
            "last_run_edges_appended": (
                last_run.get("edges_appended") if last_run else None
            ),
        })
    return {
        "status": "ok",
        "deployment": deployment,
        "authority": "source_registry_admin",
        # The path the SERVER resolved and writes. Reported so a caller
        # proving "this call had no registry effect" digests the file the
        # server would have written, not one the caller recomputed from a
        # deployment name -- two different files can both be called
        # `source_registry.yaml`, and digesting the wrong one proves
        # nothing about the right one (PACT chat-source-admin).
        "registry_path": str(_path),
        "health_authority": (
            "Use okg.v_source_run_diagnostics for source-sync timing and "
            "performance. Do NOT use it, dogfood status, or dogfood "
            "readiness to answer 'has this source ever produced anything?': "
            "all three read only run, watermark and liveness rows, and a "
            "refused admission writes none of them, so the answer is a FALSE "
            "NEGATIVE. Join okg.admission_envelopes as well -- that is what "
            "each source's `last_admission` here reports."
        ),
        "sources": out_sources,
        "counts": counts,
    }


# ----------------------------------------------------------------
# Tool 2: add_source
# ----------------------------------------------------------------


def add_source(
    conn: psycopg.Connection,
    deployment: str,
    short_name: str,
    params: Optional[dict] = None,
    *,
    dry_run: bool = False,
) -> dict:
    """Add one source to the deployment registry.

    State-machine envelope (see proposal Decision 4.5 + envelope-
    accuracy + rollback proposals):

      - `awaiting_params`: missing operator-provided params.
      - `needs_connection`: credentialed connector not connected.
      - `ok_already_configured`: source's already enabled.
      - `ingest_complete`: wired + non-zero rows ingested.
      - `ingest_complete_no_rows`: wired + ingest produced no rows.
      - `ingest_failed`: ingest exited non-zero (registry rolled back).
      - `rollback_failed`: rollback write itself failed.
      - `activation_blocked`: source is missing a runnable probe contract.
      - `unknown_source` / `deployment_not_found`: misuse.

    When `dry_run=True`, walks the catalog + idempotency + connector
    paths but does NOT write the registry and does NOT run
    ingest. Statuses are prefixed `dry_run_*`. Per the
    `improve-add-source-operator-experience` proposal: lets
    `commit_setup` preview each entry before bulk-committing,
    and lets agents preview before asking the operator to
    confirm.
    """
    _load_source_admin_helpers()
    if short_name not in SOURCE_CATALOG:
        return _wrap_dry_run({
            "status": "unknown_source",
            "short_name": short_name,
            "valid_short_names": sorted(SOURCE_CATALOG.keys()),
            "summary": (
                f"No source named `{short_name}`."
            ),
        }, dry_run)
    source_id, kind = SOURCE_CATALOG[short_name]
    params = dict(params or {})
    try:
        path, raw = _load_registry_yaml(deployment)
    except FileNotFoundError as e:
        return _wrap_dry_run({
            "status": "deployment_not_found",
            "error": str(e),
            "cli_fallback": (
                f"okg install --profile academic-researcher --orcid <id> "
                f"--name {deployment}"
            ),
            "summary": f"No deployment `{deployment}` to add to.",
        }, dry_run)

    sources = raw.setdefault("sources", {})
    entry = sources.get(source_id) or {}
    existing_params = entry.get("params") or {}
    already_on = bool(entry.get("enabled_by_default", False))
    if already_on and (not params or existing_params == {
        **existing_params, **params,
    }):
        return _wrap_dry_run({
            "status": "ok_already_configured",
            "source_id": source_id,
            "params": _redact_params(existing_params),
            "summary": f"`{source_id}` already enabled; nothing to do.",
            "agent_hint": (
                f"Source `{source_id}` is already enabled in "
                f"deployment {deployment!r}. To re-ingest, call "
                f"`okg ingest --include {source_id} --deployment "
                f"{deployment} --inline` from a terminal."
            ),
        }, dry_run)

    # Kind-specific gating.
    if kind == "connector":
        return _wrap_dry_run({
            "status": "needs_connection",
            "source_id": source_id,
            "summary": (
                f"`{short_name}` needs a connector route."
            ),
            "agent_hint": (
                f"`{short_name}` is configured through the single "
                "setup planner. Re-run `propose_setup` so it can "
                "select a host/provider/gateway route or mark the "
                "source as `needs_connection`. Do not route this "
                "through per-source credential setup."
            ),
        }, dry_run)
    if kind == "llm" and not params.get("api_key"):
        return _wrap_dry_run({
            "status": "awaiting_params",
            "source_id": source_id,
            "missing_params": ["api_key"],
            "summary": f"`{short_name}` needs an API key.",
            "agent_hint": (
                f"`{short_name}` needs an OpenAI / Anthropic API key. Ask the "
                "operator to store the key in the secret backend and re-call "
                "`add_source` with a SECRET REFERENCE, e.g. "
                "`params={\"api_key\": \"secret://<deployment>/<name>\"}` (or "
                "`{\"api_key\": {\"secret_ref\": \"secret://...\"}}`). Never pass "
                "a raw API key literal."
            ),
        }, dry_run)

    # Refuse literal secret params: api_key-class params must be secret
    # references, never raw literals persisted into source_registry.yaml.
    literal_secret_keys = _literal_secret_param_keys(params)
    if literal_secret_keys:
        return _wrap_dry_run({
            "status": "needs_secret_ref",
            "source_id": source_id,
            "secret_param_keys": literal_secret_keys,
            "summary": (
                f"`{short_name}` secret params must be secret references, "
                "not literal values."
            ),
            "agent_hint": (
                "Pass api_key-class params as a secret reference (secret://, "
                "vault://, keyring://, aws-sm://), e.g. "
                "`params={\"api_key\": \"secret://<deployment>/<name>\"}` or "
                "`{\"api_key\": {\"secret_ref\": \"secret://...\"}}`. The raw "
                "secret is never stored in the source registry."
            ),
        }, dry_run)
    if kind == "param" and not params:
        return _wrap_dry_run({
            "status": "awaiting_params",
            "source_id": source_id,
            "summary": (
                f"`{short_name}` needs configuration params."
            ),
            "agent_hint": (
                f"`{short_name}` needs configuration params (paths "
                "or URLs). Prefer `propose_setup` narrowing kwargs "
                "when discovery can infer them; use this low-level "
                "`add_source` only after the operator gives the "
                "explicit path or URL."
            ),
        }, dry_run)

    # Filesystem + param sources with full params land here.
    new_entry = copy.deepcopy(entry)
    new_entry["enabled_by_default"] = True
    if params:
        new_entry.setdefault("params", {}).update(params)
    resolved_ownership_id = (
        new_entry.get("ownership_id")
        or f"{deployment}.{source_id}"
    )
    activation_findings = _source_activation_probe_findings(
        source_id=source_id,
        entry=new_entry,
        registry_path=path,
    )
    if activation_findings:
        return _wrap_dry_run({
            "status": "activation_blocked",
            "source_id": source_id,
            "deployment": deployment,
            "findings": activation_findings,
            "summary": (
                f"`{source_id}` cannot be enabled until its source-owned "
                "change-probe contract is complete."
            ),
            "agent_hint": (
                "Add a sound class-level change_probe_kind, bind the runtime "
                "self.change_probe, and use change_probe or always_changed for "
                "poll admission; then lint the deployment again."
            ),
        }, dry_run)

    if dry_run:
        # Don't write the registry, don't run ingest. Return
        # the would-be-success envelope shape.
        return {
            "status": "dry_run_would_ingest",
            "source_id": source_id,
            "deployment": deployment,
            "params": _redact_params(new_entry.get("params")),
            "summary": (
                f"Would wire `{source_id}` and run scoped ingest."
            ),
        }
    return _apply_source_with_ingest(
        deployment=deployment,
        source_id=source_id,
        registry_path=path,
        raw=raw,
        new_entry=new_entry,
        conn=conn,
        ownership_id=resolved_ownership_id,
    )


def _wrap_dry_run(envelope: dict, dry_run: bool) -> dict:
    """Prefix the status with `dry_run_` when caller passed
    `dry_run=True`. Used for envelopes returned before the
    write/ingest branch."""
    _load_source_admin_helpers()
    if dry_run and not envelope["status"].startswith("dry_run_"):
        envelope = {**envelope, "status": f"dry_run_{envelope['status']}"}
    return envelope


def _apply_source_with_ingest(
    *,
    deployment: str,
    source_id: str,
    registry_path: Path,
    raw: dict,
    new_entry: dict,
    conn: Optional[psycopg.Connection] = None,
    ownership_id: Optional[str] = None,
) -> dict:
    with _registry_mutation_lock(registry_path):
        current_raw = yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
        if not isinstance(current_raw, dict):
            raise ValueError("source registry root must be a mapping")
        return _apply_source_with_ingest_locked(
            deployment=deployment,
            source_id=source_id,
            registry_path=registry_path,
            raw=current_raw,
            new_entry=new_entry,
            conn=conn,
            ownership_id=ownership_id,
        )


def _apply_source_with_ingest_locked(
    *,
    deployment: str,
    source_id: str,
    registry_path: Path,
    raw: dict,
    new_entry: dict,
    conn: Optional[psycopg.Connection] = None,
    ownership_id: Optional[str] = None,
) -> dict:
    """Write `new_entry` to the registry, run scoped ingest, then
    surface an envelope that reflects the ingest's actual output —
    not just its subprocess return code.

    Per `fix-add-source-rollback-on-ingest-failure`:
    write-then-ingest-then-rollback-on-failure semantics keep the
    registry consistent with the ingest outcome. Pre-fix,
    `add_source` returned `ingest_failed` while leaving the source
    enabled — future publishes kept hitting the same broken source.

    Per `fix-source-ingest-count-attribution`: counts come from
    the SourceRunner's persisted `sync.last_run` payload
    (`_last_run_counts`) — the only accounting the runner writes
    on every path, including Chronos-native deployments where the
    legacy `node_facts`/`edge_facts` logs are empty by design. A
    pre/post `run_id` comparison proves the payload belongs to
    THIS ingest. The older `_counts_for` pre/post diff (per
    `fix-add-source-envelope-accuracy`) remains as fallback, but
    a 0/0 fallback diff is treated as "accounting unavailable",
    NOT as zero rows — pre-fix it produced false
    `ingest_complete_no_rows` ("ingested 0 rows") while the data
    actually landed. `ingest_complete_no_rows` now requires
    authoritative run counts saying zero.

    Envelope shapes:

    | rc | counts | shape |
    |---|---|---|
    | ≠0 | (any)                       | `ingest_failed` + `rollback: true` |
    | 0  | authoritative, >0           | `ingest_complete` + `nodes_added`, `edges_added` |
    | 0  | authoritative, =0           | `ingest_complete_no_rows` + `agent_hint` |
    | 0  | fallback diff, >0           | `ingest_complete` + `nodes_added`, `edges_added` |
    | 0  | unavailable / fallback =0   | `ingest_complete` without count fields |

    Snapshot granularity for rollback: per-entry. If the registry
    had no `sources[source_id]` before the call, rollback removes
    it. If the entry existed, rollback restores its prior shape
    verbatim. The rest of `source_registry.yaml` is untouched in
    both directions.
    """
    _load_source_admin_helpers()
    activation_findings = _source_activation_probe_findings(
        source_id=source_id,
        entry=new_entry,
        registry_path=registry_path,
    )
    if activation_findings:
        return {
            "status": "activation_blocked",
            "source_id": source_id,
            "deployment": deployment,
            "findings": activation_findings,
            "summary": (
                f"`{source_id}` was not enabled because its source-owned "
                "change-probe contract is incomplete."
            ),
        }
    sources = raw.setdefault("sources", {})
    # Deep-copy the snapshot so the rollback restores nested
    # structures (notably the `params` sub-dict) verbatim. A
    # shallow copy would share the inner dict with `new_entry`
    # and leak mutations into the snapshot.
    prior_entry = (
        copy.deepcopy(sources[source_id]) if source_id in sources else None
    )
    # Snapshot pre-ingest accounting (before the registry write
    # is fine — both reads hit DB tables that yaml writes don't
    # touch). Two layers:
    #   - `_last_run_counts`: the runner's persisted per-run
    #     payload. The pre-snapshot's run_id proves whether the
    #     post-snapshot belongs to THIS ingest.
    #   - `_counts_for`: legacy fact-log diff, fallback only.
    # Skipped when conn or ownership_id is absent (legacy callers).
    pre_counts: Optional[dict] = None
    pre_last: Optional[dict] = None
    if conn is not None and ownership_id:
        try:
            pre_last = _last_run_counts(conn, deployment, ownership_id)
            pre_counts = _counts_for(conn, ownership_id)
        except Exception as e:  # noqa: BLE001 — best-effort
            _log.info(
                "source_admin: pre-ingest count snapshot failed for %s: %s",
                ownership_id, e,
            )
            pre_counts = None
    sources[source_id] = new_entry
    _atomic_write_registry(registry_path, raw)

    rc = _scoped_ingest(deployment, source_id)
    if rc == 0:
        # Success path. Prefer the runner's own per-run counts;
        # they are authoritative iff a NEW run_id was persisted
        # by this ingest.
        delta_n: Optional[int] = None
        delta_e: Optional[int] = None
        authoritative = False
        if conn is not None and ownership_id:
            try:
                post_last = _last_run_counts(conn, deployment, ownership_id)
            except Exception as e:  # noqa: BLE001
                _log.info(
                    "source_admin: post-ingest last-run read failed for "
                    "%s: %s", ownership_id, e,
                )
                post_last = None
            if post_last is not None and (
                pre_last is None
                or post_last["run_id"] != pre_last["run_id"]
            ):
                delta_n = post_last["nodes_appended"]
                delta_e = post_last["edges_appended"]
                authoritative = True
            elif pre_counts is not None:
                # Fallback: legacy fact-log diff. Correct on
                # deployments that still write node_facts /
                # edge_facts; blind (always 0/0) on
                # Chronos-native ones — so a 0/0 here is
                # "unavailable", never "zero rows".
                try:
                    post_counts = _counts_for(conn, ownership_id)
                    fb_n = (
                        post_counts["node_count"]
                        - pre_counts["node_count"]
                    )
                    fb_e = (
                        post_counts["edge_count"]
                        - pre_counts["edge_count"]
                    )
                    if fb_n > 0 or fb_e > 0:
                        delta_n, delta_e = fb_n, fb_e
                except Exception as e:  # noqa: BLE001
                    _log.info(
                        "source_admin: post-ingest count snapshot failed "
                        "for %s: %s — falling back to legacy envelope",
                        ownership_id, e,
                    )
        if authoritative and delta_n == 0 and delta_e == 0:
            return {
                "status": "ingest_complete_no_rows",
                "source_id": source_id,
                "deployment": deployment,
                "params": _redact_params(new_entry.get("params")),
                "nodes_added": 0,
                "edges_added": 0,
                "summary": (
                    f"Wired `{source_id}`, but ingested 0 rows."
                ),
                "agent_hint": (
                    "The source ingested cleanly (rc=0) but emitted "
                    "zero rows (per the runner's own run accounting). "
                    "Check: (1) does the configured path "
                    "exist and contain files? (2) does the source "
                    "recognize the file format? Run the cli_fallback "
                    "to see the source's stderr. The source remains "
                    "enabled in the registry — call `remove_source` "
                    "to drop it if the config is wrong."
                ),
                "cli_fallback": (
                    f"okg ingest --include {source_id} --deployment "
                    f"{deployment} --inline"
                ),
            }
        if delta_n is not None and delta_e is not None:
            return {
                "status": "ingest_complete",
                "source_id": source_id,
                "deployment": deployment,
                "params": _redact_params(new_entry.get("params")),
                "nodes_added": delta_n,
                "edges_added": delta_e,
                "summary": (
                    f"Wired `{source_id}`: {delta_n} nodes, "
                    f"{delta_e} edges."
                ),
            }
        # Counts unavailable (conn=None, ownership_id=None, no
        # last-run payload from this ingest, and no positive
        # fallback diff): return the count-less envelope so the
        # operator is never told "0 rows" without proof.
        return {
            "status": "ingest_complete",
            "source_id": source_id,
            "deployment": deployment,
            "params": _redact_params(new_entry.get("params")),
            "summary": f"Wired `{source_id}`.",
        }

    # Failure path — restore the snapshot.
    if prior_entry is None:
        sources.pop(source_id, None)
    else:
        sources[source_id] = prior_entry
    try:
        _atomic_write_registry(registry_path, raw)
    except OSError as e:
        return {
            "status": "rollback_failed",
            "source_id": source_id,
            "returncode": rc,
            "rollback_error": str(e),
            "summary": (
                f"Couldn't wire `{source_id}` AND rollback failed."
            ),
            "fix_hint": (
                f"hand-restore the registry at {registry_path}: "
                f"set sources.{source_id} to "
                f"{prior_entry if prior_entry is not None else 'absent'}"
            ),
        }
    return {
        "status": "ingest_failed",
        "source_id": source_id,
        "returncode": rc,
        "rollback": True,
        "summary": (
            f"Couldn't wire `{source_id}`: ingest failed (rolled back)."
        ),
        "cli_fallback": (
            f"okg ingest --include {source_id} --deployment "
            f"{deployment} --inline"
        ),
    }


def _source_activation_probe_findings(
    *,
    source_id: str,
    entry: Mapping[str, Any],
    registry_path: Path,
) -> list[dict[str, Any]]:
    """Validate the exact dormant candidate before it becomes runnable."""
    from okg.substrate.deployment_lint import (
        source_activation_probe_findings,
    )

    return source_activation_probe_findings(
        source_id,
        entry,
        deployment_dir=registry_path.parent,
        registry_path=registry_path,
    )


def _scoped_ingest(deployment: str, source_id: str) -> int:
    """Run `okg ingest --include <source_id> --inline`. Returns
    the subprocess returncode."""
    _load_source_admin_helpers()
    repo_root = _repo_root_from_cwd()
    from okg.substrate.cli.child_argv import running_okg_argv
    from okg.substrate.deployment_authority import (
        deployment_authority_subprocess_env,
    )

    env = deployment_authority_subprocess_env(deployment)
    try:
        proc = subprocess.run(
            [
                *running_okg_argv(),
                "ingest",
                "--deployment", deployment,
                "--include", source_id,
                "--inline",
            ],
            cwd=repo_root,
            capture_output=True,
            timeout=300,
            env=env,
        )
        return proc.returncode
    except (subprocess.TimeoutExpired, OSError) as e:
        _log.warning(
            "source_admin: ingest spawn failed for %s: %s", source_id, e,
        )
        return 1


# ----------------------------------------------------------------
# Tool 3: remove_source
# ----------------------------------------------------------------


def remove_source(
    conn: psycopg.Connection,
    deployment: str,
    short_name: str,
) -> dict:
    """Drop one source only after its published graph retirement is proven."""
    _load_source_admin_helpers()
    if short_name not in SOURCE_CATALOG:
        return {
            "status": "unknown_source",
            "short_name": short_name,
            "valid_short_names": sorted(SOURCE_CATALOG.keys()),
        }
    source_id, _kind = SOURCE_CATALOG[short_name]
    try:
        path, raw = _load_registry_yaml(deployment)
    except FileNotFoundError as e:
        return {"status": "deployment_not_found", "error": str(e)}
    sources = raw.get("sources") or {}
    if source_id not in sources:
        return {
            "status": "ok_not_present",
            "source_id": source_id,
            "agent_hint": (
                f"`{source_id}` wasn't in the registry — nothing "
                "to remove."
            ),
        }
    from okg.substrate.errors import SubstrateError
    from okg.substrate.sources.guided_remove import (
        remove_registry_entry_after_retirement,
    )

    # One code path with `okg deployment remove-source` (issue #2477).
    try:
        removed = remove_registry_entry_after_retirement(
            conn, deployment=deployment, source_id=source_id,
        )
    except SubstrateError as exc:
        return {
            "status": "retirement_required",
            "source_id": source_id,
            "deployment": deployment,
            "error": exc.message,
            "error_code": exc.code,
            "details": exc.data,
            "agent_hint": exc.fix_hint,
        }
    retirement = removed["retirement"]
    if removed["status"] == "not_present":
        return {
            "status": "ok_not_present",
            "source_id": source_id,
            "agent_hint": f"`{source_id}` was already removed.",
        }
    return {
        "status": "ok_removed",
        "source_id": source_id,
        "deployment": deployment,
        "retirement": retirement,
        "note": "Published graph retirement verified before registry removal.",
    }


# ----------------------------------------------------------------
# Tool 4: source_status
# ----------------------------------------------------------------


def _source_name_variants(name: str) -> set[str]:
    return {name, name.replace("_", "-"), name.replace("-", "_")}


def _aliases_for_registry_source(source_id: str) -> list[str]:
    aliases = set(_source_name_variants(source_id))
    source_norm = source_id.replace("_", "-")
    for short_name, (catalog_source_id, _kind) in SOURCE_CATALOG.items():
        if catalog_source_id.replace("_", "-") == source_norm:
            aliases.add(short_name)
            aliases.update(_source_name_variants(catalog_source_id))
    return sorted(aliases)


def _registry_alias_index(sources: dict[str, Any]) -> dict[str, set[str]]:
    aliases: dict[str, set[str]] = {}
    for source_id in sources:
        for alias in _aliases_for_registry_source(source_id):
            aliases.setdefault(alias, set()).add(source_id)
    return aliases


def _resolve_registry_source(
    sources: dict[str, Any],
    requested_name: str,
) -> tuple[str, dict[str, Any], list[str]] | None:
    if requested_name in sources:
        return (
            requested_name,
            sources[requested_name],
            _aliases_for_registry_source(requested_name),
        )

    aliases = _registry_alias_index(sources)
    matches = aliases.get(requested_name) or set()
    if len(matches) == 1:
        source_id = next(iter(matches))
        return (
            source_id,
            sources[source_id],
            _aliases_for_registry_source(source_id),
        )
    if len(matches) > 1:
        return (
            "",
            {
                "status": "ambiguous_source",
                "short_name": requested_name,
                "matches": sorted(matches),
            },
            [],
        )
    return None


def source_status(
    conn: psycopg.Connection,
    deployment: str,
    short_name: str,
) -> dict:
    """Inspect one source's connection/config + ingest state."""
    _load_source_admin_helpers()
    try:
        registry_path, raw = _load_registry_yaml(deployment)
    except FileNotFoundError as e:
        return {"status": "deployment_not_found", "error": str(e)}
    sources = raw.get("sources") or {}
    resolved = _resolve_registry_source(sources, short_name)
    catalog_source_id: str | None = None
    if resolved is None and short_name in SOURCE_CATALOG:
        catalog_source_id, _kind = SOURCE_CATALOG[short_name]
        resolved = _resolve_registry_source(sources, catalog_source_id)
    if resolved is None:
        if catalog_source_id is not None:
            return {
                "status": "not_in_registry",
                "source_id": catalog_source_id,
                "agent_hint": (
                    f"`{catalog_source_id}` is not in the deployment's "
                    "registry. Run `propose_setup` so setup can evaluate "
                    "it in the prioritized plan."
                ),
            }
        return {
            "status": "unknown_source",
            "short_name": short_name,
            "valid_short_names": sorted(
                set(SOURCE_CATALOG.keys()) | set(sources.keys())
            ),
            "agent_hint": (
                "Use a source name from this deployment's registry or a "
                "non-ambiguous catalog alias."
            ),
        }
    source_id, entry, aliases = resolved
    if entry.get("status") == "ambiguous_source":
        return entry
    if not _source_visible_for_subject(conn, deployment, source_id):
        return {
            "status": "not_authorized",
            "deployment": deployment,
            "agent_hint": (
                "Nomos source-lineage policy hides this source from the "
                "current subject."
            ),
        }
    ownership_id = entry.get("ownership_id") or source_id
    wm = _watermark_for(conn, ownership_id)
    c = _counts_for(conn, ownership_id)
    last_run = _last_run_counts(conn, deployment, ownership_id)
    latest_progress = _latest_source_progress(conn, deployment, ownership_id)
    admission = _last_admission_outcome(
        conn,
        deployment,
        ownership_id,
        _admission_producer_id(entry, ownership_id),
        _admission_producer_kind(entry),
    )
    count_authority = "fact_log"
    if not c.get("count_available", True) or (
        c["node_count"] == 0 and c["edge_count"] == 0
    ):
        count_authority = "last_run" if last_run is not None else "unavailable"
    status_label = _preflight_status(source_id, entry)
    sync_status = _source_sync_status_for_source(
        conn,
        deployment=deployment,
        registry_path=registry_path,
        source_name=source_id,
    )
    return {
        "status": "ok",
        "source_id": source_id,
        "registry_name": source_id,
        "ownership_id": ownership_id,
        "aliases": aliases,
        "deployment": deployment,
        "preflight_status": status_label,
        "enabled_by_default": bool(
            entry.get("enabled_by_default", False),
        ),
        "last_ingest_at": wm.get("last_ingest_at"),
        "node_count": c["node_count"],
        "edge_count": c["edge_count"],
        "count_authority": count_authority,
        "last_run_id": last_run.get("run_id") if last_run else None,
        "last_run_nodes_appended": (
            last_run.get("nodes_appended") if last_run else None
        ),
        "last_run_edges_appended": (
            last_run.get("edges_appended") if last_run else None
        ),
        "params": _redact_params(entry.get("params")),
        "source_sync_status": sync_status,
        # A refused admission writes nothing but an envelope, so this is the
        # only surface on which such a source is distinguishable from a
        # healthy idle one (issue #1323).
        "last_admission": admission,
        "recent_error": _admission_recent_error(
            admission, bool(wm or last_run or latest_progress),
        ),
        "runtime_progress": latest_progress,
        "health_status": _source_status_label(
            setup_status=status_label,
            runtime_observed=_runtime_observed(
                watermark=wm,
                last_run=last_run,
                latest_progress=latest_progress,
                admission=admission,
            ),
            other_observed=bool(wm or last_run or latest_progress),
            explicitly_dormant=_entry_is_explicitly_dormant(entry),
            admission=admission,
        ),
    }


def _source_admin_subject(
    conn: psycopg.Connection,
    deployment: str,
    session: Any | None,
) -> str:
    """The subject the source-visibility gate presents (issue #2343).

    The subject the MCP boundary already resolved for ``session`` wins: in
    multi-user mode that is the request principal, not the server. A
    multi-user session without one is refused rather than answered as the
    server. Otherwise it is the one resolver every MCP path shares, which
    refuses by name (``mcp_principal_unresolved``, or
    ``authority_unavailable`` / ``authority_evaluation_failed`` when the
    policy cannot be read) before any decision row.
    """
    from okg.substrate.nomos.mcp_subject import require_mcp_subject
    from okg.substrate.nomos.read_filter import _assert_multi_user_subject

    subject = getattr(session, "nomos_subject", None) if session is not None else None
    if subject is not None:
        return str(subject)
    if session is not None:
        _assert_multi_user_subject(session, None)
    return require_mcp_subject(
        conn, deployment_name=deployment, surface="mcp.source_admin",
    )


def _source_visible_for_subject(
    conn: psycopg.Connection,
    deployment: str,
    source_id: str,
    *,
    session: Any | None = None,
) -> bool:
    from okg.substrate.nomos import deployment_requires_nomos_runtime

    if not deployment_requires_nomos_runtime(deployment):
        return True
    from okg.substrate.nomos.core import DecisionInput
    from okg.substrate.nomos.store import evaluate_with_active_policy

    subject = _source_admin_subject(conn, deployment, session)
    scope = f"deployment:{deployment}"
    decision = evaluate_with_active_policy(
        conn,
        deployment_name=deployment,
        scope=scope,
        decision_input=DecisionInput(
            subject=subject,
            action="graph.read.mcp",
            classification="internal",
            resource={
                "deployment": deployment,
                "scope": scope,
                "source_id": source_id,
                "source_ids": [source_id],
                "kind": "source_admin",
            },
        ),
        enforcement_point="mcp.source_admin",
        target={"kind": "source_admin", "source_id": source_id},
        performed_obligations=("audit",),
    )
    conn.commit()
    return decision.effect not in {"deny", "review", "quarantine"}


def _source_sync_status_for_source(
    conn: psycopg.Connection,
    *,
    deployment: str,
    registry_path: Path,
    source_name: str,
) -> dict[str, Any]:
    dsn = _connection_info_dsn(conn)
    if not isinstance(dsn, str) or not dsn:
        return {
            "schema": "okg.source_sync.status.v1",
            "deployment": deployment,
            "source_name": source_name,
            "status": "unavailable",
            "reason": "mcp_connection_dsn_unavailable",
        }
    try:
        from okg.substrate.deployments import resolve_deployment
        from okg.substrate.runtime.config import resolve_source_sync
        from okg.substrate.source_sync_producers import source_sync_producer_health
        from okg.substrate.source_sync_status import source_sync_status_for_source
        from okg.substrate.sources.registry import load_source_registry

        registry = load_source_registry(registry_path)
        runtime_source_sync = None
        runtime_enabled = False
        try:
            manifest = resolve_deployment(deployment)
            runtime_source_sync = resolve_source_sync(manifest.runtime)
            runtime_enabled = bool(getattr(manifest.runtime, "enabled", False))
        except Exception:  # noqa: BLE001 - source status stays best-effort.
            runtime_source_sync = None
            runtime_enabled = False
        health = source_sync_producer_health(
            dsn,
            deployment=deployment,
            registry=registry,
            runtime_source_sync=runtime_source_sync,
            runtime_enabled=runtime_enabled,
        )
        status = health.get("source_sync_status") or {}
        return source_sync_status_for_source(status, source_name)
    except Exception as exc:  # noqa: BLE001 - MCP status should stay readable.
        return {
            "schema": "okg.source_sync.status.v1",
            "deployment": deployment,
            "source_name": source_name,
            "status": "unavailable",
            "reason": "source_sync_status_error",
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _connection_info_dsn(conn: psycopg.Connection) -> str | None:
    """Return conninfo suitable for nested status reads.

    ``conn.info.dsn`` redacts the password. Most MCP reads use the live
    connection directly, but source-sync health opens its own connection from
    the conninfo string, so it must reattach the password when psycopg exposes
    one.
    """
    info = getattr(conn, "info", None)
    dsn = getattr(info, "dsn", None)
    if not isinstance(dsn, str) or not dsn:
        return None
    password = getattr(info, "password", None)
    if not password:
        return dsn
    try:
        from psycopg.conninfo import make_conninfo

        return make_conninfo(dsn, password=password)
    except Exception:  # noqa: BLE001 - preserve best-effort MCP status.
        return dsn


# ----------------------------------------------------------------
# Discover → propose → commit (intent-and-approval setup)
# ----------------------------------------------------------------
#
# Per `improve-add-source-operator-experience`: the three tools
# below let the agent run a complete `okg-setup` walkthrough in
# 2 operator turns (intent + approval) instead of 5+ "what's
# your X path?" questions. The agent runs `propose_setup`
# (which calls `discover_sources` internally), reads the plan
# summary to the operator, then calls `commit_setup` on
# approval.


# `_short_for_source_id` was a byte-equivalent duplicate of
# `_short_name_for_source_id` (line 266) — deleted post-Wave-D
# audit; all callers now point at `_short_name_for_source_id`.


# Cache + cross-cutting formatters
# (_PLANS / _stash_plan / _load_plan / _format_count_summary /
# _truncate_summary) were initially copied here by the
# source_lifecycle extraction script. They live in source_admin
# instead — commit_setup and setup_planning both need them, and
# source_admin is their natural home.
