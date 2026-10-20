"""The opensearch-reference deployment, checked without a database.

PACT `opensearch-reference-integration` (issue #2014), req.ori.source.

Two kinds of claim live here. The deployment lints clean, which is what
`okg deployment up` needs before it will do anything. And the container
definition keeps the promises the design made about it: pinned by digest,
capped, bound to loopback, unauthenticated with no credential anywhere in
the tree.

The loopback assertion follows the precedent of
`test_compose_binds_postgres_to_loopback_only`: a literal `127.0.0.1`
rather than a parameterized host, so no environment variable can widen
the binding to 0.0.0.0.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from okg.substrate.deployment_lint import lint_deployment
from okg.substrate.sources.registry import load_source_registry


ROOT = Path(__file__).resolve().parents[4]
DEPLOYMENT = ROOT / "deployments/opensearch-reference"
COMPOSE = DEPLOYMENT / "opensearch/docker-compose.yaml"
SEED = DEPLOYMENT / "opensearch/seed.sh"
UP = DEPLOYMENT / "up.sh"

SEEDED_INDICES = (
    "app-logs-2026.09",
    "ingest-events-2026.09",
    "audit-trail-2026.09",
)


def _compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def _service() -> dict:
    return _compose()["services"]["opensearch"]


# ── the deployment itself ─────────────────────────────────────────────

def test_the_deployment_lints_without_a_blocker() -> None:
    report = lint_deployment(DEPLOYMENT)
    blockers = [
        f for f in report["findings"] if f["severity"] == "blocker"
    ]
    assert blockers == [], blockers
    assert report["ok"] is True


def test_the_registry_entry_matches_the_reference_catalog_profile() -> None:
    entry = load_source_registry(DEPLOYMENT / "source_registry.yaml")[
        "opensearch-catalog"
    ]
    assert entry.source_class == "reference_catalog"
    assert entry.record_identity_kind == "domain_key"
    assert entry.source_revision_kind == "content_hash"
    assert entry.deletion_semantics == "missing_from_completed_scope"
    assert entry.publication_mode == "published_generation"
    # The identity fields are the UUIDs, not the index name.
    assert list(entry.record_identity_fields) == [
        "namespace", "cluster_uuid", "index_uuid",
    ]


def test_every_declared_service_index_is_one_the_seed_creates() -> None:
    """The source refuses a declared index the cluster does not have, so a
    typo here would fail the documented command rather than quietly drop
    an edge. Catching it without a container is cheaper."""
    registry = yaml.safe_load(
        (DEPLOYMENT / "source_registry.yaml").read_text(encoding="utf-8"),
    )
    services = registry["sources"]["opensearch-catalog"]["params"]["services"]
    declared = {index for s in services for index in s["indices"]}
    assert declared <= set(SEEDED_INDICES), declared - set(SEEDED_INDICES)

    seed_text = SEED.read_text(encoding="utf-8")
    for index in SEEDED_INDICES:
        assert index in seed_text, index


def test_the_sync_modes_are_the_ones_this_profile_has_policies_for() -> None:
    """The runner maps (profile, mode) onto a retraction-safety policy.

    `reference_catalog` has policies for `release_new` and
    `release_unchanged` only. The registry carried `scope_complete`, the
    discovery_crawl vocabulary, so an ingest that needed to retract failed
    at exactly the point where retraction is decided -- while a run with
    nothing to retract passed, which is why only a live ingest found it.
    """
    registry = yaml.safe_load(
        (DEPLOYMENT / "source_registry.yaml").read_text(encoding="utf-8"),
    )
    sync = registry["sources"]["opensearch-catalog"]["sync"]
    catalog_modes = {"release_new", "release_unchanged"}
    assert sync["default_event_mode"] in catalog_modes
    assert sync["reconcile_mode"] in catalog_modes


def _manifest() -> dict:
    return yaml.safe_load(
        (DEPLOYMENT / "deployment.yaml").read_text(encoding="utf-8"),
    )


def test_the_deployment_loads_the_modules_the_narrowings_need() -> None:
    manifest = _manifest()
    # `service` supplies the class the logs_to narrowing's src names; the
    # cross-link rule is silently skipped without it, which would leave
    # the emitted edge with no narrowing to permit it.
    #
    # `agent_memory` supplies `agent_claim`, which is what
    # `record(kind="finding")` resolves to. The reference walk ENDS in a
    # recorded finding (req.ori.walk), and without this module enrolled the
    # write did not refuse at the tool boundary -- it failed inside Chronos
    # with a message naming nothing an operator could act on.
    assert set(manifest["modules"]) == {
        "service", "search_systems", "agent_memory",
    }


# ── the three posture declarations the live walk corrected ────────────
#
# Each of these was wrong in a way only a real MCP call showed, and each is
# asserted here so a revert reds CI rather than waiting for the next live
# run. The reasons are in `deployments/opensearch-reference/deployment.yaml`
# beside the values and in this change's design.md section 17a.

def test_the_record_write_is_reachable_at_all() -> None:
    """`deny` here made every `record` a `nomos.default_deny`.

    `nomos/store.py` emits the `mcp.record` allow rule ONLY for `review`,
    and only for a subject derived from `write_principals` -- so both halves
    are asserted. `review` rather than `allow` is also the tightest value
    the `public_reference_demo` class permits.
    """
    posture = _manifest()["nomos"]["deployment_posture"]
    assert posture["mcp_write_default"] == "review", posture
    assert "mcp_client" in posture["write_principals"], posture
    # The class bound this sits under, restated so a widening is visible
    # here and not only in `nomos/deployment_audit.py`.
    assert posture["deployment_class"] == "public_reference_demo"
    assert posture["live_call_default"] == "deny", posture


def test_the_response_ceiling_is_not_below_the_substrate_default() -> None:
    """A lower ceiling refuses every call that omits `max_bytes`.

    The MCP SDK materializes the tool schema's own `max_bytes` default into
    the call arguments before the Python function runs, so
    `response_bounds.py`'s `caller_set_budget` is True on every call and a
    declared deployment default is never substituted. A ceiling of 32,768
    was declared here first and every `search` that omitted `max_bytes` was
    refused `invalid_response_byte_budget` for a budget the caller never
    chose. Reported against #2075/#2010; asserted here so the manifest
    cannot drift back under it.
    """
    from okg.substrate.mcp.response_bounds import DEFAULT_RESPONSE_BYTES

    response_bytes = _manifest()["agent_surface"]["response_bytes"]
    assert response_bytes["ceiling"] >= DEFAULT_RESPONSE_BYTES, response_bytes
    assert response_bytes["ceiling"] == 65536, response_bytes
    # A declared `default` below the signature default does NOT take effect,
    # so declaring one would read as enforced and not be. Absent on purpose.
    assert "default" not in response_bytes, response_bytes


# ── the container ─────────────────────────────────────────────────────

def test_the_image_is_pinned_by_digest() -> None:
    """A tag would let the reference drift under a future reader, and the
    committed fixtures were captured from exactly this image."""
    image = _service()["image"]
    assert re.search(r"@sha256:[0-9a-f]{64}$", image), image
    assert image.startswith("opensearchproject/opensearch:")


def test_the_container_is_capped_and_single_node() -> None:
    service = _service()
    assert service["mem_limit"] == "2g"
    assert service["environment"]["discovery.type"] == "single-node"
    assert "-Xmx512m" in service["environment"]["OPENSEARCH_JAVA_OPTS"]


def test_the_port_is_bound_to_loopback_literally() -> None:
    """Literal 127.0.0.1, not a parameterized host: the binding must not
    be widenable to 0.0.0.0 by an environment variable."""
    ports = _service()["ports"]
    assert ports == ["127.0.0.1:9209:9200"], ports


def test_the_dev_container_installs_no_demo_security_config() -> None:
    env = _service()["environment"]
    assert env["DISABLE_SECURITY_PLUGIN"] == "true"
    assert env["DISABLE_INSTALL_DEMO_CONFIG"] == "true"


def test_the_container_declares_a_healthcheck_the_bring_up_can_wait_on() -> None:
    assert "healthcheck" in _service()


# ── no credentials, anywhere ──────────────────────────────────────────

@pytest.mark.parametrize("path", [COMPOSE, SEED, UP,
                                  DEPLOYMENT / "deployment.yaml",
                                  DEPLOYMENT / "source_registry.yaml"])
def test_the_reference_carries_no_credential(path: Path) -> None:
    """A local unauthenticated dev container: there is nothing to store,
    and nothing is stored."""
    text = path.read_text(encoding="utf-8")
    # Value-bearing shapes, not the bare words: `secret_handling:
    # no_secrets` is a posture declaration, not a credential.
    patterns = (
        r"(?i)pass(?:word|wd)\s*[:=]\s*\S",
        r"(?i)api[_-]?key\s*[:=]\s*\S",
        r"(?i)secret\s*[:=]\s*\S",
        r"(?i)token\s*[:=]\s*[\"']?[A-Za-z0-9._-]{12,}",
        r"(?i)authorization\s*[:=]",
        r"admin:admin",
    )
    for pattern in patterns:
        assert not re.search(pattern, text), (
            f"{path.name} matches {pattern!r}"
        )


# ── the one documented command ────────────────────────────────────────

def test_the_bring_up_script_is_executable_and_provisions() -> None:
    """No okg verb starts a container or seeds one, which is why this
    wrapper is the documented command rather than a bare verb."""
    import os
    import stat

    for script in (UP, SEED):
        mode = os.stat(script).st_mode
        assert mode & stat.S_IXUSR, f"{script.name} is not executable"

    text = UP.read_text(encoding="utf-8")
    # `okg provision`, not `okg deployment up`: the latter is the
    # post-merge refresh verb and its first step reads the deployment's
    # publish state, so it fails against the empty database a clean
    # checkout has. The live arm caught this; the assertion keeps it
    # caught.
    assert "okg provision" in text
    assert "--deployment opensearch-reference" in text
    # Never a bare `okg`: a globally installed one belongs to whichever
    # checkout installed it, and the CLI refuses to serve a deployment
    # from another tree. The live run hit exactly that.
    assert ".venv/bin/okg" in text or "uv run --project" in text
    assert "\nokg provision" not in text
    assert "docker compose" in text
    assert "seed.sh" in text
    # It waits for health rather than racing the seed.
    assert "_cluster/health" in text


def test_the_readme_and_the_script_agree_on_the_command() -> None:
    readme = (DEPLOYMENT / "README.md").read_text(encoding="utf-8")
    assert "deployments/opensearch-reference/up.sh" in readme


def test_the_down_command_removes_the_named_volume() -> None:
    """`--down` must leave nothing behind.

    The compose file declares a named volume, so a plain `down` keeps the
    index data and the next bring-up seeds on top of it instead of from
    clean -- and the documented teardown would quietly not tear down.
    """
    text = UP.read_text(encoding="utf-8")
    down_block = text[text.index('= "--down"'):text.index("command -v docker")]
    assert "docker compose -f \"$COMPOSE\" down -v" in down_block, down_block

    volumes = _compose().get("volumes") or {}
    assert "okg_opensearch_reference_data" in volumes
