"""LocalEnvSource — substrate adapter for the local compute env.

Wraps the domain `okg.modules.compute_env.local_env_source.LocalEnvSource`
domain logic (uname / runtime version probes + parse
`~/.claude/settings.json` for MCP servers) in the substrate
`SourceAdapter` Protocol.

State translation:
  - domain `state['probe_hash']`: sha256 over all probe outputs
    used to short-circuit a no-op run.
  - substrate cursor: same dict, round-tripped verbatim.

Profile: `reference_catalog`. The local environment is a small,
fixed catalog (one Machine + N Runtimes + M MCPServers); absence
in a fresh probe implies retraction.

Incremental short-circuit semantics (documented 2026-04-25 from
`debug-substrate-okg-self` Phase 10):

- The `probe_hash` is sha256 over the full probe output:
  hostname + os_release + arch + the runtime version dict
  + the parsed `mcpServers` from settings.json. Any change
  to ANY of these invalidates the hash and forces a full
  re-emit on the next run.
- In particular: edits to `~/.claude/settings.json`'s
  `mcpServers` block (adding / removing / renaming a server)
  WILL be picked up on the next `runner.run(adapter)` —
  the probe-hash short-circuit will NOT skip them, because
  the settings.json content is in the hash input.
- If a deployment's bootstrap finished BEFORE the user added
  an MCP server, the graph will lack the corresponding
  `mcp_server` node until the next runner.run() invocation;
  this is data-drift, not a bug.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)


class _LocalEnvStateProbe:
    """Change-probe over the local-env probe-state digest (issue #323).

    Reuses the domain's cheap ``probe_state_hash`` (hostname + os + arch +
    runtime versions + settings.json MCP servers) -- the same identity the
    source short-circuits emission on -- WITHOUT the full node/edge emit. A probe
    error (e.g. a failed subprocess) propagates -> run_probe forces a full read.
    """

    def __init__(
        self,
        *,
        settings_path: Path,
        hostname_override: Optional[str],
    ) -> None:
        self._settings_path = settings_path
        self._hostname_override = hostname_override

    def build_token(self) -> dict[str, Any]:
        from okg.substrate.library.sources._compute_env_local_emit import (
            LocalEnvSource as DomainLocalEnvSource,
        )
        domain = DomainLocalEnvSource(
            settings_path=Path(self._settings_path),
            hostname_override=self._hostname_override,
        )
        return self.token_for_probe_hash(
            domain.probe_state_hash(),
        )

    def token_for_probe_hash(self, probe_hash: str) -> dict[str, Any]:
        from okg.substrate.library.sources import _compute_env_local_emit
        from okg.substrate.library.sources.change_probe import (
            compose_token,
            emit_version_for,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        return compose_token(
            content={"kind": "version_token", "value": probe_hash},
            config=hashlib.sha256(
                json.dumps(
                    {
                        "settings_path": str(self._settings_path),
                        "hostname_override": self._hostname_override,
                    },
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest(),
            emit_version=emit_version_for(*emission_identity_targets(
                "compute_env_local",
                LocalEnvSource,
                _compute_env_local_emit,
            )),
        )

    def probe(self, *, cursor, sync_scope):
        from okg.substrate.library.sources.change_probe import (
            CURSOR_TOKEN_KEY,
            ProbeVerdict,
            tokens_equal,
        )

        prior = cursor.get(CURSOR_TOKEN_KEY) if isinstance(cursor, dict) else None
        token = self.build_token()
        changed = prior is None or not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason="local_env_changed" if changed else "local_env_unchanged",
        )


@dataclass
class LocalEnvSource:
    """Substrate adapter for local compute environment metadata."""

    repo: str
    settings_path: Path | str = Path("~/.claude/settings.json")
    hostname_override: Optional[str] = None

    name: str = "compute_env_local"
    profile: str = "reference_catalog"
    probe_short_circuit_safe = True

    # Incremental-source contract (issue #323): the local env is a small fixed
    # catalog probed live (hostname + os + arch + runtime versions +
    # settings.json MCP servers). Its emission is a function of that probe state
    # -- NO wall-clock input -- so the cheap `probe_state_hash` digest the source
    # already short-circuits on is the sound runner-gate probe.
    change_probe_kind = "reference_catalog"

    def __post_init__(self) -> None:
        if isinstance(self.settings_path, str):
            self.settings_path = Path(self.settings_path).expanduser()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        return _LocalEnvStateProbe(
            settings_path=Path(self.settings_path),
            hostname_override=self.hostname_override,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        from okg.substrate.library.sources._compute_env_local_emit import (
            LocalEnvSource as DomainLocalEnvSource,
        )

        domain = DomainLocalEnvSource(
            settings_path=Path(self.settings_path),
            hostname_override=self.hostname_override,
            state=dict(cursor) if cursor and mode == "cursor" else {},
        )

        nodes = list(domain.nodes())
        edges = list(domain.edges())
        domain_state = domain.get_run_state()

        del run_id
        revision = {
            "probe_hash": str(
                domain_state.get("probe_hash")
                or domain.probe_state_hash()
            ),
        }
        completed_scope = mode in {"release_new", "scope_complete", "reconcile"}
        record_set = (
            self._record_set_from_emit(nodes, edges, revision=revision)
            if completed_scope
            else None
        )

        def _facts() -> Iterator[Any]:
            for n in nodes:
                yield NodeFact(
                    node_id=n.node_id,
                    subtype=n.subtype,
                    attrs={"label": n.label, **n.attrs},
                    source_record_id={
                        "repo": self.repo,
                        "node_id": n.node_id,
                    },
                    source_revision=revision,
                    op="I",
                )
            for e in edges:
                yield EdgeFact(
                    src=e.src,
                    dst=e.dst,
                    edge_type=e.edge_type,
                    attrs=dict(e.attrs),
                    provenance=e.provenance,
                    confidence=(e.confidence if e.confidence is not None else 1.0),
                    source_record_id={"repo": self.repo},
                    source_revision=revision,
                    op="I",
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=domain_state,
            applied_raw_token=self.change_probe.token_for_probe_hash(
                revision["probe_hash"]
            ),
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=record_set,
        )

    def _record_set_from_emit(
        self,
        nodes: list[Any],
        edges: list[Any],
        *,
        revision: dict[str, Any],
    ) -> RecordSet:
        machine = next(
            (node for node in nodes if node.subtype == "machine"),
            None,
        )
        if machine is None:
            return RecordSet()
        descendants = [
            RecordNode(
                node_id=node.node_id,
                subtype=node.subtype,
                source_record_id={
                    "repo": self.repo,
                    "node_id": node.node_id,
                },
                source_revision=revision,
            )
            for node in nodes
            if node.node_id != machine.node_id
        ]
        record_edges = [
            EdgeKey(
                src=edge.src,
                dst=edge.dst,
                edge_type=edge.edge_type,
                attrs=dict(edge.attrs),
                provenance=edge.provenance,
                confidence=(edge.confidence if edge.confidence is not None else 1.0),
                source_record_id={"repo": self.repo},
                source_revision=revision,
            )
            for edge in edges
        ]
        return RecordSet(
            records={
                machine.node_id: RecordEmission(
                    primary=RecordNode(
                        node_id=machine.node_id,
                        subtype=machine.subtype,
                        source_record_id={
                            "repo": self.repo,
                            "node_id": machine.node_id,
                        },
                        source_revision=revision,
                    ),
                    descendants=descendants,
                    edges=record_edges,
                )
            }
        )


__all__ = ["LocalEnvSource"]
