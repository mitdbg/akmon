"""Source health matrix — `okg doctor --check source-health`.

Per `source-health-matrix`: one row per source in the academic-researcher
library joining the per-source facts that already exist but were
never joined:

  - adapter import health (module.class from source-defaults)
  - registration completeness (wizard catalog, discovery registry,
    credentialed-set membership for connector-kind sources)
  - test coverage on disk (fixture/DB test file + live test file)
  - preflight implementation, and its result where the adapter is
    constructible without operator params
  - the live-tier gate: which env vars unblock the live test and
    whether they are present right now

Import failures and registration drift are FAILURES with the facet
named (the outlook-pair-missing-from-_CREDENTIAL_SOURCE_IDS gap
class); coverage holes and absent credentials are reported facts,
not failures — a missing token is an environment state, not a bug.

The okg-workspace/code-graph source families are deliberately out
of scope here: they are deployment-bound and covered by the
per-deployment `okg doctor --check sources` instantiation check.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

from okg.substrate import fastyaml

_REPO_MARKER = "src/okg/substrate/library/profiles/academic-researcher/source-defaults"

# Live-tier gates beyond the OAuth providers (which contribute their
# client_id/client_secret env vars automatically).
_EXTRA_LIVE_GATES: dict[str, list[str]] = {
    "gmail_api": ["OKG_LIVE_GMAIL"],
    "outlook_mail_api": ["OKG_LIVE_MSGRAPH"],
    "outlook_calendar_api": ["OKG_LIVE_MSGRAPH"],
    "canvas_api": ["OKG_LIVE_CANVAS", "OKG_CANVAS_TOKEN"],
}

# source_id -> OAuth provider name (for client env gates).
_PROVIDER_BY_SOURCE: dict[str, str] = {
    "gmail_api": "gmail",
    "google_calendar_api": "google_calendar",
    "slack_api": "slack",
    "outlook_mail_api": "msgraph",
    "outlook_calendar_api": "msgraph",
}


@dataclass
class SourceHealthRow:
    source_id: str
    short_name: Optional[str]
    module: str
    cls_name: str
    import_ok: bool = False
    import_error: Optional[str] = None
    wizard_kind: Optional[str] = None
    in_wizard_catalog: bool = False
    has_discoverer: bool = False
    credentialed_connector: bool = False
    in_credentialed_set: Optional[bool] = None
    test_file: Optional[str] = None
    live_test_file: Optional[str] = None
    preflight_implemented: bool = False
    preflight_status: Optional[str] = None
    live_gate_env: list[str] = field(default_factory=list)
    live_ready: bool = False
    failures: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "short_name": self.short_name,
            "module": self.module,
            "class": self.cls_name,
            "import_ok": self.import_ok,
            "import_error": self.import_error,
            "wizard_kind": self.wizard_kind,
            "in_wizard_catalog": self.in_wizard_catalog,
            "has_discoverer": self.has_discoverer,
            "credentialed_connector": self.credentialed_connector,
            "in_credentialed_set": self.in_credentialed_set,
            "test_file": self.test_file,
            "live_test_file": self.live_test_file,
            "preflight_implemented": self.preflight_implemented,
            "preflight_status": self.preflight_status,
            "live_gate_env": list(self.live_gate_env),
            "live_ready": self.live_ready,
            "failures": list(self.failures),
        }


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / _REPO_MARKER).is_dir():
            return parent
    raise RuntimeError(f"repo root not found above {here}")


def _source_defaults_entries(root: Path) -> dict[str, dict[str, Any]]:
    entries: dict[str, dict[str, Any]] = {}
    for path in sorted((root / _REPO_MARKER).glob("*.yaml")):
        raw = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for source_id, entry in raw.items():
            if isinstance(entry, dict) and "module" in entry:
                entries[source_id] = entry
    return entries


def _test_files_for(module: str, root: Path) -> tuple[Optional[str], Optional[str]]:
    """Map a source module path to its test file by the repo's
    convention: okg.substrate.library.sources[.<pkg>].<name> ->
    tests/substrate/library/sources[/<pkg>]/test_<name>.py."""
    parts = module.split(".")
    try:
        idx = parts.index("sources")
    except ValueError:
        return None, None
    rel = parts[idx + 1:]
    if not rel:
        return None, None
    base = root / "tests" / "substrate" / "library" / "sources"
    test_path = base.joinpath(*rel[:-1], f"test_{rel[-1]}.py")
    live_path = base.joinpath(*rel[:-1], f"test_{rel[-1]}_live.py")
    # Sources whose tests live under a sibling name (discovery-only
    # coverage) still count via the corpora/discovery suites; report
    # the file only when the conventional path exists.
    return (
        str(test_path.relative_to(root)) if test_path.is_file() else None,
        str(live_path.relative_to(root)) if live_path.is_file() else None,
    )


def _live_gate_for(source_id: str) -> list[str]:
    gates = list(_EXTRA_LIVE_GATES.get(source_id, []))
    provider_name = _PROVIDER_BY_SOURCE.get(source_id)
    if provider_name is not None:
        from okg.substrate.library.oauth.providers import get_provider
        provider = get_provider(provider_name)
        gates.extend([provider.client_id_env, provider.client_secret_env])
    return gates


def assemble_source_health(
    *,
    root: Optional[Path] = None,
    env: Optional[Mapping[str, str]] = None,
    source_defaults: Optional[dict[str, dict[str, Any]]] = None,
    credentialed_ids: Optional[frozenset[str]] = None,
) -> list[SourceHealthRow]:
    """Join the per-source facts into health rows. The injectable
    kwargs exist for the failure-mode tests; production callers use
    the real registries."""
    import importlib

    from okg.substrate.library.sources.discovery.registry import DISCOVERERS
    from okg.substrate.mcp.source_catalog import (
        _CREDENTIAL_SOURCE_IDS,
        SOURCE_CATALOG,
    )

    root = root or _repo_root()
    env = env if env is not None else os.environ
    entries = (
        source_defaults
        if source_defaults is not None
        else _source_defaults_entries(root)
    )
    credentialed = (
        credentialed_ids
        if credentialed_ids is not None
        else _CREDENTIAL_SOURCE_IDS
    )
    wizard_by_source = {
        source_id: (short, kind)
        for short, (source_id, kind) in SOURCE_CATALOG.items()
    }

    rows: list[SourceHealthRow] = []
    for source_id, entry in sorted(entries.items()):
        module = str(entry.get("module") or "")
        cls_name = str(entry.get("class") or "")
        short_kind = wizard_by_source.get(source_id)
        row = SourceHealthRow(
            source_id=source_id,
            short_name=short_kind[0] if short_kind else None,
            module=module,
            cls_name=cls_name,
            wizard_kind=short_kind[1] if short_kind else None,
            in_wizard_catalog=short_kind is not None,
            has_discoverer=source_id in DISCOVERERS,
        )

        cls = None
        try:
            cls = getattr(importlib.import_module(module), cls_name)
            row.import_ok = True
        except Exception as exc:  # noqa: BLE001 — the error IS the report
            row.import_error = f"{type(exc).__name__}: {exc}"
            row.failures.append(f"import: {row.import_error}")

        row.credentialed_connector = row.wizard_kind == "connector"
        if row.credentialed_connector:
            row.in_credentialed_set = source_id in credentialed
            if not row.in_credentialed_set:
                row.failures.append(
                    "registration: connector source missing from "
                    "_CREDENTIAL_SOURCE_IDS"
                )
        if not row.in_wizard_catalog:
            row.failures.append(
                "registration: source-defaults entry has no wizard-"
                "catalog short name"
            )

        row.test_file, row.live_test_file = _test_files_for(module, root)

        if cls is not None:
            row.preflight_implemented = callable(
                getattr(cls, "preflight", None),
            )
            if row.preflight_implemented:
                try:
                    probe = cls(deployment_name="health-probe")
                    row.preflight_status = probe.preflight().status
                except TypeError:
                    # Needs operator params — preflight result is a
                    # deployment-time fact, reported as such.
                    row.preflight_status = "needs_params"
                except Exception as exc:  # noqa: BLE001 — surfaced, not raised
                    row.preflight_status = f"error: {exc}"

        row.live_gate_env = _live_gate_for(source_id)
        row.live_ready = all(env.get(var) for var in row.live_gate_env)
        rows.append(row)
    return rows


def summarize(rows: list[SourceHealthRow]) -> dict[str, Any]:
    failing = [r for r in rows if r.failures]
    return {
        "ok": not failing,
        "sources": len(rows),
        "import_failures": sum(1 for r in rows if not r.import_ok),
        "registration_drift": sum(
            1 for r in rows
            if any(f.startswith("registration:") for f in r.failures)
        ),
        "with_test_file": sum(1 for r in rows if r.test_file),
        "with_live_test": sum(1 for r in rows if r.live_test_file),
        "live_ready": sum(1 for r in rows if r.live_ready),
        "live_blocked": sorted(
            {
                var
                for r in rows
                if not r.live_ready
                for var in r.live_gate_env
                if not os.environ.get(var)
            },
        ),
        "failing_sources": [r.source_id for r in failing],
    }


def parse_runbook_matrix(text: str) -> list[dict[str, str]]:
    """Extract the per-source rows from the matrix runbook's tables:
    | source | fixture/DB test | live test | live gate (env) |."""
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        if not line.startswith("|") or set(line) <= {"|", "-", " ", ":"}:
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) >= 4 and cells[0] not in ("source", ""):
            rows.append({
                "source": cells[0].strip("`"),
                "test": cells[1].strip("`"),
                "live_test": cells[2].strip("`"),
                "live_gate": cells[3],
            })
    return rows


_ENV_VAR_RE = re.compile(r"\bOKG_[A-Z0-9_]+\b")


def runbook_env_vars(cell: str) -> set[str]:
    return set(_ENV_VAR_RE.findall(cell))
