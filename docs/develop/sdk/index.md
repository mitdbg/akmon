---
audience: developer
type: overview
canonical_id: deployment-sdk-readme
---
# Deployment SDK


Use `okg.deployment` to write durable Python workflows for one AKMON
deployment. First, provision the deployment database and run
`okg runtime bootstrap --deployment <name>` per the
[DBOS runtime runbook](../../operate/deployments/runtime.md).

## What the SDK gives you

- **Decorators** for declaring workflows, steps, transactions, scheduled jobs, and post-publish hooks.
- **Mutation helpers** that write to the OKG fact log and bitemporal projections at the right generation, with automatic worker-write generation allocation and idempotency guarantees.
- **Read helpers** that pin reads to a generation and never silently fall back to a default.
- **External integration**: LLM calls (`litellm`), durable wait-for-signal, workflow correlation by topic+key, on-demand republish.
- **Stream wrappers** for inter-workflow derived-edge passing.
- **Test fixtures** (`mock_dbos`, `dbos_test_runtime`) covering fast unit tests and durable integration tests.
- **Module-level declarations** for narrowing assertions and SDK version pinning.

The full public API is in [`api.md`](../../reference/sdk-api.md).

## External automation run identity

Use `/v1/deployments/{deployment}/external-automation-runs` with existing sender-constrained authentication. POST registers immutable reported identity; GET `/{record_id}` reads it; POST `/{record_id}/proposals` links a matching proposal. Mutations require JSON and `Idempotency-Key`.

Registration requires `run_id`, `automation_id`, `automation_version`, `correlation_id`, published `generation_id`, and `reported_distribution` (`name`, `version`, nullable `package_digest`). Permissions are `external_automation.register`, `external_automation.read`, and both `external_automation.link`/`external_action.read` for links. Distribution and generation references prove neither execution nor graph reads. No heartbeat, cost, budget or worker status is reported.

Malformed fields/generations return 422; authentication failures 401; policy denials 403; hidden runs 404; identity/replay conflicts or quarantine writes 409; unavailable authority 503. Existing mixed historical policy digests can make this service unavailable.

## Quickstart: a verifier workflow

The canonical example is the af_pubs candidate verifier: a scheduled workflow that picks unprocessed candidate publications, asks an LLM to judge each, and either commits a verdict or escalates to an operator when the LLM is unclear.

```python
# deployments/af_pubs/workflows/verifier.py
from okg.deployment import (
    cost_budget,
    declare_narrowings,
    declare_sdk_version,
    deployment_scheduled,
    deployment_step,
    deployment_transaction,
    deployment_workflow,
    emit_edge_fact,
    emit_node_fact,
    llm_call,
    query_at_generation,
    read_node,
    request_escalation,
)


declare_sdk_version("1.0")

declare_narrowings(
    ("Candidate", "verified_by", "Verification"),
    ("Verification", "led_to", "Annotation"),
)


@deployment_step(retries_allowed=True, max_attempts=3)
def llm_judge_candidate(candidate_id: str, prompt: str) -> tuple[dict, float]:
    """LLM call wrapped as a step so DBOS caches its output by input hash.

    Returns (verdict_dict, cost_usd).
    """
    response = llm_call(
        prompt=prompt,
        model="openai/gpt-4o-mini",
        max_tokens=400,
    )
    verdict = {"label": "verified", "rationale": response.text[:1000]}
    return verdict, response.cost_usd


@deployment_transaction()
def _commit_verdict(
    candidate_id: str,
    verdict: dict,
    cost_usd: float,
) -> None:
    """Dual-mutation: emit the verdict and update the candidate's
    `last_verified_at` attr — both land at the same worker-write
    generation."""
    emit_node_fact(
        f"verification:{candidate_id}",
        subtype="Verification",
        attrs={"label": verdict["label"], "rationale": verdict["rationale"]},
        dedupe_key=f"verify:{candidate_id}:{verdict['label']}",
    )
    emit_edge_fact(
        candidate_id, f"verification:{candidate_id}",
        edge_type="verified_by",
        attrs={"cost_usd": cost_usd},
        dedupe_key=f"verified_by:{candidate_id}",
    )


@deployment_workflow(name="verify_candidates_batch")
@deployment_scheduled("af_pubs.verifier.scheduled", default_cron="*/15 * * * *")
def verify_candidates_batch():
    """Run every 15 minutes; verify up to $5 worth of candidates."""
    candidates = query_at_generation(
        "SELECT node_id FROM okg.v_nodes "
        "WHERE subtype='Candidate' AND attrs->>'verified_at' IS NULL "
        "ORDER BY (attrs->>'priority')::int DESC NULLS LAST LIMIT 50"
    )
    with cost_budget(usd=5.00, scope="af_pubs.verifier.cycle") as budget:
        for row in candidates:
            if budget.available < 0.05:
                break  # not enough headroom for another call
            candidate_id = row["node_id"]
            candidate = read_node(candidate_id)
            verdict, cost = llm_judge_candidate(
                candidate_id, _build_prompt(candidate),
            )
            budget.consume(cost)
            if verdict["label"] == "unclear":
                decision = request_escalation(
                    subject=candidate_id,
                    reason=f"LLM uncertain on {candidate_id}",
                    decision_schema="schema:open",
                    timeout_seconds=86400,
                )
                verdict = {**decision, "via": "operator"}
            _commit_verdict(candidate_id, verdict, cost)


def _build_prompt(candidate) -> str:
    return f"Verify this candidate:\n{candidate.attrs}"
```

Wire the workflow into the manifest:

```yaml
# deployments/af_pubs/deployment.yaml
runtime:
  enabled: true
  sdk_version: "1.0"
  workflows:
    - af_pubs.workflows.verifier
  schedules:
    # Optional: override the @deployment_scheduled default_cron.
    af_pubs.verifier.scheduled: "*/30 * * * *"
```

Run the worker:

```bash
okg runtime worker --deployment af_pubs
```

The substrate's worker entry point imports `af_pubs.workflows.verifier` at boot, the SDK decorators register with DBOS, and the scheduled workflow fires on cron. Each cycle either commits verdicts at a worker-write generation or pauses on `request_escalation` until an operator responds via `okg runtime escalation respond` (see the runbook).

## Boundary: only `okg.deployment.*`

The substrate is internal to OKG. Deployment workflow modules must not import from `okg.substrate.*`; `okg lint deployments` catches the violation, including transitive imports through sibling helper modules, and runs in CI.

Look for the SDK equivalent first. The most common needs are mapped:

| Substrate symbol | SDK equivalent |
|---|---|
| `okg.substrate.publisher` writes | `emit_node_fact`, `emit_edge_fact`, `update_node_attrs` |
| `okg.substrate.runtime.escalation` | `request_escalation` |
| `okg.substrate.runtime.generation` | `worker_write_generation`, `pinned_generation`, `latest_published_generation` |
| `okg.substrate.runtime.hooks` registration | `@on_generation_published`, `@on_generation_blocked`, `@on_escalation_decided` |
| `okg.substrate.catalog` declarations | `declare_narrowings`, `declare_sdk_version` |
| Raw `psycopg.connect` | `read_node`, `query_at_generation`, and the mutation helpers inside `@deployment_transaction` |

If the SDK doesn't expose what you need, it should grow — propose the SDK change rather than reaching past the boundary.

### Legacy enricher surface

Enrichers (relationship builders) have no SDK read API yet. Until one exists, an external `enrichers:` entry may import exactly these substrate modules, and `okg lint deployments` states the entry as `legacy-enricher-surface` in its coverage instead of reporting DEPLOYMENT-001 (`LEGACY_ENRICHER_SURFACE` in `src/okg/substrate/runtime/lint.py`). Any other substrate import from an enricher, and any of these from a workflow or connector, is still a finding. The exit code and `count` contract the okg-deployments compatibility check reads are unchanged.

| Module | Names the pinned Archi wheel (3.0.0a1) imports |
|---|---|
| `okg.substrate.enrichers.base` | `EnrichResult`, `IncrementalContext` (4 enrichers) |
| `okg.substrate.enrichers.derived_edges` | `DerivedEdgeCandidate`, `insert_deterministic_edges`, `mint_edge_id` |
| `okg.substrate.library.linkers._chronos` | `query` only, from 5 modules including the alias backend |
| `okg.substrate.library.linkers.declarative` | `DeclarativeLinker` |
| `okg.substrate.alias.protocol` | `AliasMatch` |

The wheel's connectors use only `okg.deployment` names. `_chronos` stays private: its behaviour (a Chronos checkout read on the caller's connection that restores the write witness when a transaction is open) is frozen, not promoted. What the publish path actually gives a legacy enricher, proven by `tests/substrate/test_legacy_enricher_publish_semantics.py` with a neutral mirror of Archi's `JiraAffectsFromReferences` through three real cycles: the live connection in autocommit (every statement durable at once; the publisher's per-enricher savepoint only engages for a caller that passes a transaction-mode connection to `cycle(conn=…)`, and `_chronos.query` restores the witness for any in-transaction caller such as `okg enricher backfill`), `generation_id` 0 with the published generation known only to the publisher, derived edge facts ingested into the generation before checkpoint, a raising sibling that neither blocks the publish nor removes durable edges, a clean-input skip that re-reports the previous generation as a no-op reuse, and an incremental rerun scoped by the dirty node set — the legacy `okg.edge_facts` cursor carries no scope for Chronos-native writes.

Migration disposition: enrichers move when the read API can express what they do today — bulk multihop traversal over `graph_nodes`/`graph_edges` joins, resuming from a fact-log cursor, aggregation (`count`, `group by`), existence and anti-joins against live edge ids, subtype attribute and range joins (DQM run ranges), and deterministic derived edges with stable dedupe keys on the write side. Historical dispositions already landed: retracting records absent from completed scopes, and TWiki markup stripping; reproduce before reopening. Running the Archi enrichers on their own host and data remains unverified here.

## Testing

The SDK ships two pytest fixtures. Import them via `pytest_plugins`:

```python
# tests/conftest.py
pytest_plugins = ["okg.deployment.testing"]
```

### `mock_dbos` — fast unit tests

Decorators become pass-through (`@deployment_workflow` does not enroll with DBOS). SDK helpers that hit Postgres still need a real conn bound via `bind_test_context`. Use `mock_dbos.queued_response(topic, payload)` to drive `request_escalation` and assert via `mock_dbos.assert_workflow_enqueued(...)`.

```python
def test_verifier_flow(mock_dbos, postgres_conn):
    from okg.deployment._internal.context import bind_test_context

    mock_dbos.queued_response(
        "escalation:e-1", {"label": "stale"},
    )
    with bind_test_context(deployment="af_pubs", conn=postgres_conn):
        verify_candidates_batch()
    mock_dbos.assert_workflow_enqueued("verify_candidates_batch")
```

### `dbos_test_runtime` — durable integration tests

A real DBOS singleton against a test Postgres DB. Mark tests with `@pytest.mark.dbos_integration` and run them on a separate CI job — the init cycle is slow.

```python
import pytest

@pytest.mark.dbos_integration
def test_verifier_durable(dbos_test_runtime):
    # Real DBOS workflow / step / recv semantics.
    ...
```

## What's NOT in the SDK

- Schedule / queue tuning beyond the manifest's `runtime.schedules` and `runtime.dbos_pool_size`. Tune them in `deployment.yaml`.
- Bulk-import paths. The substrate's `okg ingest` / source-runner still owns large initial backfills; the SDK's mutation helpers are for workflow-driven incremental writes.
- Workflow-level retry policies. Step-level retries are on by `@deployment_step(retries_allowed=True)`. Workflow-level retry happens via DBOS recovery — set `max_recovery_attempts` on the `@deployment_workflow` decorator.

## Where things live

- Public SDK source: `src/okg/deployment/`. Public API is re-exported from `okg.deployment`; everything under `okg.deployment._internal` is implementation detail.
- Substrate runtime layer (what the SDK builds on): `src/okg/substrate/runtime/`. Not for deployment-author import.
- API reference: [Deployment SDK API](../../reference/sdk-api.md).
- Runtime operation: [Deployment runtime](../../operate/deployments/runtime.md).
