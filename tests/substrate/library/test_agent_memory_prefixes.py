"""Catalog-backed, deployment-extensible prefix registry (substrate-operability).

The registry is the single source of truth for agent-memory node-id prefix ->
catalog subtype, shared by the `record` MCP tool and the `agent_memory` source.
These tests pin: builtins resolve; a deployment extends the set without editing
substrate code; a deployment entry can override a builtin; longest prefix wins;
invalid entries are skipped; and the source resolves through the merged set.
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate.library.agent_memory_prefixes import (
    BUILTIN_RECORD_PREFIXES,
    normalize_prefixes,
    prefix_for_id,
    subtype_for_id,
)


def test_builtin_prefixes_resolve() -> None:
    prefixes = normalize_prefixes(None)
    assert subtype_for_id("claim:abc", prefixes) == "agent_claim"
    assert subtype_for_id("observation:x", prefixes) == "agent_observation"
    assert subtype_for_id("file:src/a.py", prefixes) == "source_file"
    assert subtype_for_id("totally-unknown:z", prefixes) is None


def test_deployment_prefix_extends_without_editing_builtins() -> None:
    prefixes = normalize_prefixes(
        [{"prefix": "myorg-finding:", "target_subtype": "myorg_finding"}],
    )
    # deployment-supplied prefix resolves...
    assert subtype_for_id("myorg-finding:42", prefixes) == "myorg_finding"
    # ...and the builtins still resolve.
    assert subtype_for_id("claim:abc", prefixes) == "agent_claim"
    # the deployment entry is attributed as such.
    entry = prefix_for_id("myorg-finding:42", prefixes)
    assert entry is not None
    assert entry.owner == "deployment"
    assert entry.source == "deployment"
    # `subtype` is accepted as an alias for `target_subtype`.
    alt = normalize_prefixes([{"prefix": "z:", "subtype": "zeta"}])
    assert subtype_for_id("z:1", alt) == "zeta"


def test_deployment_entry_overrides_builtin_for_same_prefix() -> None:
    prefixes = normalize_prefixes(
        [{"prefix": "claim:", "target_subtype": "myorg_claim"}],
    )
    assert subtype_for_id("claim:abc", prefixes) == "myorg_claim"
    # dedup: exactly one entry for the prefix.
    assert sum(1 for p in prefixes if p.prefix == "claim:") == 1


def test_longest_prefix_wins() -> None:
    prefixes = normalize_prefixes([
        {"prefix": "ml:", "target_subtype": "ml_generic"},
        {"prefix": "ml:model:", "target_subtype": "ml_model"},
    ])
    assert subtype_for_id("ml:model:resnet", prefixes) == "ml_model"
    assert subtype_for_id("ml:foo", prefixes) == "ml_generic"


def test_invalid_deployment_entries_skipped() -> None:
    prefixes = normalize_prefixes([{"prefix": ""}, {"target_subtype": "x"}, {}])
    assert len(prefixes) == len(BUILTIN_RECORD_PREFIXES)


def test_source_resolves_through_merged_prefixes(tmp_path: Path) -> None:
    from okg.substrate.library.sources.agent_memory import AgentMemorySource

    src = AgentMemorySource(
        repo="r",
        repo_root=tmp_path,
        record_prefixes=[
            {"prefix": "myorg-finding:", "target_subtype": "myorg_finding"},
        ],
    )
    # the source merged the deployment prefix into its registry...
    assert subtype_for_id("myorg-finding:42", src._prefixes) == "myorg_finding"
    # ...and still resolves builtins.
    assert subtype_for_id("claim:abc", src._prefixes) == "agent_claim"

    # a default source (no deployment prefixes) does NOT recognize the custom
    # prefix, but builtins still resolve.
    default = AgentMemorySource(repo="r", repo_root=tmp_path)
    assert subtype_for_id("myorg-finding:42", default._prefixes) is None
    assert subtype_for_id("claim:abc", default._prefixes) == "agent_claim"
