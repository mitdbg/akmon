---
audience: developer
type: reference
canonical_id: deployment-sdk-api
---
# `okg.deployment` API reference


`okg.deployment.__all__` enumerates the symbols documented here.

Tour: [`README.md`](../develop/sdk/index.md).

## Extension SDK

### `EXTENSION_SDK_VERSION`

The extension protocol version, pinned exactly in package metadata before the broker admits code.

### `ExtensionKind`

The extension-kind literal: connector, extractor, enricher, verifier, materializer, tool or resolver.

### `ExtensionRequest(extension_id, binding_id, capabilities, payload={})`

Frozen request to an admitted extension: verified identities, granted capabilities, sanitized payload.

### `ExtensionNode(node_id, subtype, attrs={})`

Frozen node proposed by an extension; the substrate validates and publishes it.

### `ExtensionEdge(src, dst, edge_type, attrs={})`

Frozen edge proposed by an extension; it crosses admission and narrowing first.

### `ExtensionBatch(nodes=(), edges=(), observations={})`

Proposed nodes, edges and observations, within the broker's output-size limit.

## Decorators

### `extension_entrypoint(extension_id, *, kind, capabilities=())`

Marks an out-of-process entry point. The broker checks id, kind and capabilities against admitted metadata before passing an `ExtensionRequest`.

### `deployment_workflow(name=None, *, max_recovery_attempts=50, validate_args=True)`

Wraps `DBOS.workflow()`, recording `okg_sdk_version` and the source SHA-256 as `deployment_module_version`. Resuming with either changed marks it `cancelled_due_to_version_mismatch`.

### `deployment_step(name=None, *, retries_allowed=False, max_attempts=3, interval_seconds=1.0, backoff_rate=2.0)`

Wraps `DBOS.step`. Retries default off; when enabled, DBOS caches output by input hash.

### `deployment_transaction(name=None, *, isolation_level="SERIALIZABLE")`

Wraps `DBOS.transaction`; mutation helpers use its `sql_session` and one worker-write generation.

### `deployment_scheduled(schedule_id, *, default_cron, automatic_backfill=True, guard_body=True)`

Registers a durable scheduled workflow (a plain DBOS workflow plus a schedule row the worker applies at start, not `DBOS.scheduled`). `runtime.schedules.<schedule_id>` in the manifest overrides `default_cron`. With `guard_body=True` the body runs under the deployment's writer permit; set it to `False` only for a wrapper that just enqueues a child workflow that takes the permit itself.

### `on_generation_published(deployment, *, workflow_name=None)`

Runs after `status='published'` commits. The `okg-<deployment>` queue serializes hooks per deployment; failures do not roll back publication. Adds `@deployment_workflow` when needed.

### `on_generation_blocked(deployment, *, workflow_name=None)`

As above, when the publisher's cycle blocks instead.

### `on_escalation_decided(deployment, *, workflow_name=None)`

Registers a hook for an escalation becoming `decided`, but no code path runs
these hooks yet: the publisher dispatches only `generation_published` and
`generation_blocked` hooks. Extra keyword arguments such as
`subject_subtype` are accepted and ignored.

## Mutation helpers

### `emit_node_fact(node_id, subtype, attrs, *, dedupe_key, op="I", source_record_id=None, source_revision=None) -> fact_id`

Writes `okg.node_facts` at the worker-write generation. `op='I'` shows the node, `op='R'` retracts it; `source` becomes `_deployment:<deployment>:<workflow_id>`. Outside `@deployment_transaction` it raises `MissingContextError`.

### `emit_edge_fact(src, dst, edge_type, attrs, *, dedupe_key, provenance="derived_deterministic", confidence=1.0, op="I", source_record_id=None, source_revision=None) -> fact_id`

Inserts an edge fact into the Chronos graph, `edge_id` being `uuid5` over `(src, edge_type, dst)`. The publish gate reports an invalid relationship as `narrowing_violation`.

### `update_node_attrs(node_id, new_attrs, *, dedupe_key=None) -> fact_id`

Reads the node's current attributes, merges `new_attrs` over them (a shallow merge: a top-level key replaces the old value) and writes the result through `emit_node_fact`. It raises `ValueError` when the node does not exist. There is no concurrency check: when two updates to the same node run at once, the later write wins and can drop keys the earlier one added. When `dedupe_key` is omitted it is derived from the node id and the merged attributes, so repeating the same update in the same step is idempotent.

### `request_escalation(*, subject, reason, view=None, decision_schema="schema:open", timeout_seconds=86400.0) -> dict`

Pauses for an operator decision, written idempotently by `(workflow_id, step_id)`; returns it or raises `EscalationTimeoutError`.

`decision_schema`: `"schema:open"` accepts any JSON object, `"schema:literal:<urlencoded-json-schema>"` an embedded JSON Schema.

### `set_progress(key, value)`

Fire-and-forget event via `DBOS.set_event`. NOT a durable counter — use `cost_budget` or a deployment table.

### `cost_budget(*, usd, scope)`

Context manager over `okg.deployment_cost_caps` + `okg.deployment_cost_ledger`:

- `__enter__`: UPSERT the cap.
- `.consume(usd, *, consumption_id=None)`: idempotent on `(scope, consumption_id)`, defaulting to the DBOS step_id; over cap raises `BudgetExhaustedError`.
- `.consumed` / `.cap` / `.available` / `.exhausted`: live Postgres reads.
- `.extend(extra_usd)`: atomically raise the cap.

Recovery sees cumulative consumption durably.

## Read helpers

### `read_node(node_id, *, pin=None) -> NodeRow | None`

Reads one node at the pinned generation, else None. `pin=None` resolves the worker-write generation, then `okg.pin_generation`, then latest published.

### `query_at_generation(sql, params=(), *, pin=None) -> list[dict]`

Runs a SELECT at `pin` via `set_config()`. Postgres roles enforce read-only access; the SDK parses no SQL.

### `list_published_generations(deployment, *, since_generation_id=None, limit=100) -> list[GenerationRow]`

Publisher history, newest first.

### `publisher_generation() -> int`

Valid only inside the publisher cycle; raises in deployment-workflow contexts.

### `pinned_generation() -> int`

Resolves `okg.pin_generation` if set, else `latest_published_generation(deployment)`.

### `latest_published_generation(deployment) -> int`

Max `generation_id` with `status='published'` for `deployment`; raises when none are published.

### `worker_write_generation() -> int`

Returns the transaction's worker-write generation, allocated and cached on first call.

## External integration

### `llm_call(prompt, *, model, seed=None, budget=None, response_schema=None, max_tokens=None) -> LLMResponse`

`litellm.completion` wrapper. Returns `LLMResponse` only on a finite, nonnegative LiteLLM price; any other raises `LLMPricingError`. With a `budget`, every settled cost, zero included, goes through `budget.consume(cost_usd)`; `BudgetExhaustedError` propagates. Not cached — wrap in `@deployment_step` for retry idempotency.

### `wait_for_signal(topic, *, timeout_seconds, payload_schema=None) -> Any`

Durable wait for a non-HITL callback. The topic is auto-prefixed `deployment:<name>:`, so deployments cannot cross-receive.

### `correlate_workflow(*, topic, correlation_key) -> None`

UPSERT into `okg.deployment_correlations` so a handler finds the waiting workflow by `(deployment, topic, correlation_key)`.

### `find_workflow_by_correlation(*, deployment, topic, correlation_key) -> str | None`

Reverse lookup: the registered `workflow_id`, or None.

### `request_republish(*, reason, branch_name="default", force_enrichers=False) -> str`

Enqueues `okg_publisher_cycle`, returning its workflow id. `branch_name` and `force_enrichers` are workflow arguments; nonblank `reason` is logged. Unregistered DBOS raises `RuntimeError`. `SUCCESS` means the cycle ran, not that it published.

### `write_derived_edges_stream(*, stream_key, edges)`

Producer-side: write `DerivedEdgeSpec` instances to a DBOS stream named `deployment:<name>:edges:<stream_key>`, closing it when the iterator is exhausted.

### `consume_derived_edges_stream(*, producer_workflow_id, stream_key) -> Iterator[DerivedEdgeSpec]`

Consumer-side: iterate until close.

### `DerivedEdgeSpec(src, dst, edge_type, attrs={}, dedupe_key="", confidence=1.0, provenance="derived_deterministic")`

Shipped on the stream.

### `LLMResponse(text, parsed, cost_usd, input_tokens, output_tokens, model, pricing_status="settled")`

Returned by `llm_call`. `input_tokens` and `output_tokens` are `int | None`: missing usage stays unknown rather than zero.

### `LLMPricingError`

Raised when a completed LLM call has no valid price. `pricing_status` is `"unsettled"`; `requested_model`, `resolved_model`, `input_tokens`, `output_tokens`, `reason` and `guidance` keep the context, chaining the original exception.

## Connector surface

Source-runtime aliases: a connector imports no `okg.substrate`. Facts: `NodeFact`, `EdgeFact`, `ProgressMarker`. Deletion authority: `RecordSet`, `RecordNode`, `RecordEmission`, `EdgeKey`, `RecordAuthorityStream`. Health: `ConnectorHealth`, `HEALTH_STATUSES` (incl. `partial`). Run modes: `COMPLETED_SCOPE_RUN_MODES`. Probes: `ContentHashProbe`, `MutableApiProbe`, `file_content_items`. Preflight: `PreflightResult`, `file_preflight`, `credential_preflight`, `http_preflight`, `dependency_preflight`. Redaction: `redact`, `redact_endpoint`, `redact_mapping`.

Each is the substrate object, so the runner's `isinstance` checks hold; `ProbeVerdict` and `ChangeProbeError` are its change-probe types. Resolved on first access.

Images: `ImageRef` builds image values. `ImageRef.file(path)` and `ImageRef.url(url)` return plain JSON dicts for a fact's attrs, as the attr value or a list element: `{"okg_image": "file", "path"}` with a relative POSIX path (no absolute path, `..`, `.` segment, empty segment, backslash or NUL), or `{"okg_image": "url", "url"}` with an https URL that has a host and no credentials. A bad path or URL raises `image_ref_invalid`. okg's ingest renders file values under `ConnectorRun.image_root` and adds `png` or a named `error`; a fact that already carries either is refused as `image_value_forged`. Only a connector's ingest run renders: a SDK workflow write (`emit_node_fact`, `emit_edge_fact`, `update_node_attrs`) or an enricher may carry a bare `file` or `url` reference, but its `file` reference is never rendered and reads as not ingested. The same holds for a `file` reference on an edge, from any writer, because ingest renders node attrs only. Agent record writes (MCP `record` and `propose`, `okg admission submit`) refuse a `file` reference with `image_ref_invalid`, because file images come only from a connector's ingest run; a `url` reference is accepted there. Poros export sends each image value as its bare reference, without `png` or `error`, so the receiver accepts it; a received `file` reference reads as not ingested.

## Connector protocol

`Connector` is the contract a connector implements: `name`, `profile`, `run(ctx)`.

`ConnectorContext` carries `run_id`, `mode`, `sync_scope`, `checkpoint`, `config`, `deadline`, `since_progress`, `change_probe_verdict`, `authoritative_replay`, `metrics`, and `cancelled()`, true on request or past the deadline. `secret_ref(name)` returns a credential's environment-variable name, never its value; an undeclared name raises `KeyError`. `ConnectorMetrics` counts.

`ctx.mode` is the source profile's own run mode. When the caller passes no explicit `--mode`, a whole-scope run arrives as `release_new` for a `reference_catalog` source, `reconcile` for `mutable_api` and `scope_complete` for `discovery_crawl`; an explicit `--mode` is not rewritten. `COMPLETED_SCOPE_RUN_MODES` is the set of modes whose run covers the whole scope, for every profile. A connector that read its whole selected input sets `completed_scope=True` when `ctx.mode in COMPLETED_SCOPE_RUN_MODES`; it must not test a hard-coded list such as `{"scope_complete", "reconcile"}`. A `reference_catalog` connector with that list never claims a complete scope, so it never retracts a record that disappears and never records a reconcile time.

A run that appends nothing and does not claim a complete scope is accepted as a no-op (`incomplete_scope_noop`) only when the source has history: a stored cursor, a recorded reconcile time, or an earlier admission of the source that reached a published generation. The no-op saves no cursor, and the verdict's `source_history` field names the signal that matched. The cost: once a source that keeps no cursor has published, an empty but healthy rerun is accepted as a no-op, and because such a source never records a reconcile time, nothing flags a reader that silently starts reading nothing. Sources that keep a cursor already accepted the same trade-off. A brand-new source, or one whose only runs failed or were never published, is refused as `partial`, because an empty first run may be a reader that reads nothing. A connector that never claims a complete scope, so that it never deletes, can return a content-identifying `Checkpoint`; it is stored when a run appends. Such a source never records a completed scope, so `okg deployment ready --profile dogfood` reports a `required_for_baseline: true` source of this kind as `missing`, a blocker. A source kept non-deleting must not be baseline-required.

`Checkpoint(next_cursor=None)` is opaque resume state, persisted and handed back unread.

`ConnectorRun` narrows `SourceRun` to `facts`, `completed_scope`, `next_checkpoint`, `health`, `record_set`, `run_mode`, `record_authority_stream`, `record_set_replacement_keys` and `image_root`, and refuses `partial` health with `completed_scope`. `image_root` is the directory file image values resolve against; it reaches the runner as `SourceRun.image_root`, also on a signal, and is never written to a fact or the graph. `ConnectorWarning` and `RejectedItem` count into run metrics on a completed run; a signal records `connector.signal`.

`ConnectorError` bases the only sanctioned early exits — `Cancelled`, `Deadline`, `RetryAfter`, `RateLimited` — each ending the run as a bounded partial advancing no checkpoint; others propagate. `RetryAfter(seconds=)` is bounded to 0-86400; `run` returning `None` raises.

`ConnectorAdapter(connector, *, config, secrets, deadline, cancel)` presents a `Connector` to the runner as a source adapter, collecting facts first because the runner reads the next cursor before walking them.

Change probe: the adapter class the registry constructs declares a class attribute `change_probe_kind` (for example `"content_hash"`) and sets `self.change_probe` to a probe such as `ContentHashProbe(content_items=..., config=..., emit_targets=...)` from `okg.deployment`, which hashes the input and the emitting functions. `okg deployment add-source` refuses an adapter without one as "not runnable here". The conformance kit's `change_probe` check fails an adapter without one. `examples/json-record-connector`'s `RecordSource` is a working example.

## Connector registration

`ConnectorRegistration` is a package's frozen per-connector declaration: type, version, `module`/`class`, config JSON schema, `auth_modes` from `AUTH_MODES`, `capabilities` from `CONNECTOR_CAPABILITIES`, `output_signature`, `sync`, `required_modules` as strict `owner:name` ids, `SdkVersionRange(min_version, max_version_exclusive)`, and an optional deprecation note. Unknown vocabulary, a malformed signature or sync block, an unqualified module id or an invalid schema refuse at construction.

`registration_registry_entry(registration, *, sdk_version=None, allow_deprecated=False)` returns the strict `source_registry.yaml` entry, admitting identically to a hand-written one; what the registry cannot model rides in `configuration_hints.okg_connector`. It refuses an unsatisfied SDK window, and a deprecation without the flag. `ConnectorRegistrationError` is the base of all nine codes, including the malformed-asset and non-release-bound refusals a declaration is read back through at install time.

Discovery is declaration: `as_asset_payload()` ships a registration as a package-v2 `connector` asset. No entry points, no import scan.

## Conformance kit

`okg.deployment.conformance` certifies one connector:

```
pytest -p okg.deployment.conformance --pyargs okg.deployment.conformance \
    --okg-connector mypackage:CASE --okg-conformance-receipt receipt.json
```

Point it at a `ConnectorCase`. It runs one test per check, skipping nothing, and writes a JSON receipt of each verdict, the SDK version and the dataset digests. The receipt names every declared check: one that never ran reads `fail` with detail `did not run`, and a nonzero pytest exit fails the verdict, so a connector that raises can never certify. A bad reference refuses `connector_conformance_case_invalid`.

## Module-level declarations

### `declare_narrowings(*triples)`

At module scope, declare each `(parent_subtype, edge_type, child_subtype)` triple as belonging in the active catalog. Accepted today, with no boot-time check.

### `declare_sdk_version(version)`

Declare the SDK version this module targets. The manifest loader enforces `runtime.sdk_version` from `deployment.yaml`; this declaration is not enforced.

## Versioning

`okg.deployment.__version__` is authoritative. A newer `runtime.sdk_version` major needs explicit acceptance; workflows cannot resume with different recorded SDK or module versions.

## Test fixtures

### `okg.deployment.testing.mock_dbos`

pytest fixture. Decorators pass through; functions run sync:
- `mock_dbos.queued_response(topic, payload)` — queue a `DBOS.recv()` response.
- `mock_dbos.assert_workflow_enqueued(name, *, queue_name=None, args=None)` — assert an enqueue.
- `mock_dbos.cost_usd_consumed(scope) -> float` — the live cost-ledger total.

SDK helpers' SQL writes still need real Postgres, via `bind_test_context`.

### `okg.deployment.testing.dbos_test_runtime`

pytest fixture. Real DBOS singleton on the test DSN (`OKG_TEST_DSN`, default `postgres://postgres:okg@localhost:5433/okg_test`). Mark tests `@pytest.mark.dbos_integration`; CI runs them nightly.

## Boundary lint

`okg lint deployments` flags forbidden `okg.substrate.*` use in workflow modules and same-package imports. `DEPLOYMENT-001` reports the location and replacement helper; any violation exits nonzero.

The scan also covers the connectors a source registry names (`module`/`class`) and the classes under `enrichers:`, exempting modules that resolve inside `okg` as substrate. Each finding names the registry key or enricher class that pulled the module in. An unresolvable entry is `DEPLOYMENT-002`, an unreadable registry `DEPLOYMENT-003`. Modules are parsed, never imported.
