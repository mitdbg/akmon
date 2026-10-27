---
audience: operator
type: how-to
canonical_id: live-relations
section: operations
visibility: primary
status: active
owner: documentation
---
# Live relations

A **live relation** lets an agent ask an external system a question while it
reads the graph, instead of only reading what an earlier import stored. The
graph keeps the map — which cluster exists, which index holds what, which
machine runs what — and the relation fetches the current answer from the system
itself.

This page takes one from declaration to a working call and then shows what the
call left behind, using the two example deployments: the Glances CI fixture,
read over `GET`, which is in the repository under
`tests/substrate/e2e/fixtures/live-http-ci/`; and the OpenSearch reference,
searched over `POST`, described in
[OpenSearch reference deployment](deployments/opensearch-reference.md). Every
field of the `http` binding is in the
[live HTTP binding reference](../reference/live-http-bindings.md), which this
page does not repeat. The second built-in adapter, `kind: mcp`, calls one named
tool on an MCP server; its binding is described in full below, in
[A `kind: mcp` binding](#a-kind-mcp-binding).

## What it does not do

It is not a tool registry: an agent cannot add one, and the set is fixed by the
subtype and the deployment manifest. It does not proxy another MCP server: the
`mcp` adapter binds one relation to one named tool, with arguments the operator
wrote, and an agent cannot reach any other tool on that server through it. The
two adapters that ship are `kind: http` (JSON over HTTP) and `kind: mcp`; a
deployment cannot add a third, because the policy gate authorizes a relation,
not the code that serves it. And a `GET` takes no
arguments at all: it has no body and query-string arguments are not built yet, so a `GET` binding whose relation declares **any** argument is
refused at deployment start with `live_adapter_config_invalid` naming `body`.
The bound on such an answer is an operator literal in the request path.

## 1. Declare what may be asked

The declaration says *what question exists*. It lives on a subtype, in the
LinkML class annotations, under `okg_live_relations`. This is the OpenSearch
reference's `documents` relation, **abridged** — its `query` and `time_window`
arguments are omitted here and both are required parts of that declaration:

```yaml
okg_live_relations:
  documents:
    action: external.opensearch.search
    principal_mode: service
    sample_policy: inline
    retention_class: warm
    deadline_seconds: 10
    args:   # abridged: `query` and `time_window` omitted
      index: {type: string, required: true, maxLength: 128,
              pattern: '^[a-z0-9][a-z0-9._\-]{0,127}$'}
      limit: {type: integer, default: 10, minimum: 1, maximum: 100}
```

- The node does not narrow the call. It is the gate's resource and is recorded
  on the sample, but the adapter is handed `node_id` and ignores it: `index`
  above is an ordinary argument, bounded only by its own declaration.
- `action` is the name the policy engine decides on. Required, no default,
  dotted lower-case tokens — a typo would match no rule and deny the relation
  forever, so it is refused at load.
- `principal_mode: service` means the call runs as the runtime, not as whoever
  asked. It is the only mode served today; `caller` is refused at deployment
  start rather than quietly downgraded.
- `description` (optional, at most 400 characters) is the text `inspect`
  shows beside the arguments: what the answer means and what it is not.
- Every argument is `required` **or** carries a `default`. Neither is refused: a
  missing value would arrive as null, and a null time window is a full scan.
- An argument is a `string`, an `integer`, a `number` or a `boolean`. A `string`
  takes `enum`, `pattern` and `maxLength`; `integer` and `number` take `enum`,
  `minimum` and `maximum`; a `boolean` takes only `enum`, because the type is
  the bound. A `pattern` without a `maxLength` is refused, and a constraint the
  type does not take is refused rather than ignored. A `pattern` must be
  anchored at both ends (`^...$` or `\A...\z`, with any alternation inside a
  group): an unanchored pattern matches a substring, so it is refused at load
  with `live_relation_arg_schema_invalid`, naming the relation and argument.

**Where to put the declaration.** A module's `schema.yaml` gives the relation to
every deployment that enrols that module; a deployment's own `schemas/*.yaml`
gives it to that deployment only, through the same composer path and with an
identical compiled result.

Prefer the deployment-local file when only one deployment can answer. `machine`
is a shared subtype that several deployments enrol, so a host-telemetry
relation on the library class would be advertised by `inspect` everywhere and
an agent would be refused on its first call. The CI fixture declares its two
relations in its own `schemas/machine_live_relations.yaml`, **abridged** below:
the real file also carries `transient` and the `okg_searchable_text_fields`
annotations.

```yaml
classes:
  Machine:
    annotations:
      archetype: resource   # mandatory; see below
      okg_live_relations:
        top_processes:
          action: external.host.read_telemetry
          principal_mode: service
          sample_policy: inline
          retention_class: hot
          deadline_seconds: 10
          args: {}          # a GET may declare none
```

`archetype` is not optional: the composer skips a class whose annotations lack
one, silently and with no refusal, so the subtype is never declared and the
relation simply does not exist. Repeat the library module's value.

`retention_class: hot` there is deliberate: those rows carry program names,
usernames and command lines, and seven days is enough to prove the call ran.

## 2. Bind the relation to a system

The binding says *which system answers and how the request is shaped*. It goes
in the deployment manifest under `agent_surface.live_bindings`, keyed by the
relation name.

```yaml
agent_surface:
  live_bindings:
    top_processes:
      kind: http
      config:
        base_url: http://127.0.0.1:61208
        allowed_hosts: ["127.0.0.1:61208"]
        allow_non_global: true
        method: GET
        path: /api/4/processlist/top/5
        rows_path: "."
        max_rows: 8
        max_response_bytes: 1048576
      secret_refs: []   # Glances is loopback and unauthenticated
```

The split is load-bearing. A binding accepts `kind`, `config`, `secret_refs` and
`service_principal` and nothing else — it cannot raise a relation's
`principal_mode`, so a deployment cannot widen a relation's authority without
touching the ontology that declared it, and the declaration cannot name a host.
Both directions are checked at startup: a declared relation with no binding
fails `live_relation_unbound`, a binding naming a relation nothing declares
fails `live_binding_undeclared_relation`.

`kind` must be one of `sql`, `command`, `http`, `mcp` or `python`; anything
else fails `live_relation_adapter_kind_unknown`. Only `http` and `mcp` have a
runner, so the other three are accepted as names and then fail
`live_adapter_unavailable`.

### A `kind: mcp` binding

An `mcp` binding names exactly one tool on exactly one MCP server, reached
over streamable HTTP. This is the shape of the test-only deployment in
`tests/substrate/e2e/fixtures/live-mcp-example/`, abridged to one relation:

```yaml
live_bindings:
  items:
    kind: mcp
    upstream_credential: personal
    config:
      server_url: http://127.0.0.1:5952/mcp
      allowed_hosts: ["127.0.0.1:5952"]
      allow_non_global: true
      tool: catalog_list_items
      arguments:
        name_pattern: {$arg: name_pattern}
        detail: {$arg: detail}
      rows_path: "."
      auth_header: Authorization
    secret_refs:
      - env:OKG_MCP_EXAMPLE_GATE_AUTH
```

| `config` field | required | what it does |
| --- | --- | --- |
| `server_url` | yes | `http` or `https`, an IP literal, a port and the endpoint path; no credentials, query or fragment |
| `tool` | yes | the one tool this relation calls |
| `arguments` | yes | the tool's arguments as a JSON object, written by the operator |
| `rows_path` | yes | where the rows sit in the tool's result: a dotted path such as `items`, or `.` for the root. A list there is the rows; under a named path, one object is one row (see below) |
| `row_fields` | no | the keys kept from each row; a row missing one is refused |
| `max_rows` | no | a cut by count, after the rows are read |
| `max_response_bytes` | no | the most bytes the whole session may receive: default 8 MiB, allowed 1 KiB to 32 MiB |
| `allowed_hosts` | no | narrows the allowlist; without it, the `server_url` address is the only one dialled |
| `allow_non_global` | no | `true` to dial a loopback, private or link-local address |
| `auth_header` | no | the header NAME that carries the secret; never a value |

An unknown `config` key is refused at start, naming the relation and the field.

**Caller values reach the tool only through slots.** A slot is
`{"$arg": "<name>"}` in value position inside `arguments`, and the caller's
validated value replaces it, typed. Substitution is one pass, so a caller value
that itself looks like a slot reaches the tool as a plain string. A slot may not
be a key, may not sit inside a string, and may not appear in `server_url`,
`tool`, `rows_path` or any other field. Every declared argument must be slotted
somewhere and every slot must name a declared argument. A slotted `string`,
`integer` or `number` argument must carry a constraint (`enum`, or `pattern`
with `maxLength`, or `minimum` and `maximum`), or the start fails with
`live_adapter_arg_unconstrained`.

**Egress.** The rules are the `http` adapter's. A loopback address such as
`127.0.0.1` needs `allow_non_global: true`, and a host outside `allowed_hosts`
is refused at start with `live_adapter_host_refused`. Redirects are never
followed. The bearer token lives only in an environment variable named by
`secret_refs` and is sent in the `auth_header` header; it never appears in
`config`, an error, a sample or a log line, and a reply that contains it is
refused rather than repeated.

**One call, bounded.** Each call opens one MCP session, initializes, calls the
tool once with the substituted arguments and closes the session, all inside the
relation's `deadline_seconds`; there is no retry. The byte cap is counted on the
wire across the whole session, so a reply over `max_response_bytes` is refused
before it is assembled or parsed. The rows then meet the same response budget as
any live relation (section 4): a prefix that fits plus a digest of the full set.
A refusal for a reply over the cap is itself bounded and lists the relation's
arguments under `narrow_with`, so the caller can ask a narrower question.

**Where the rows come from.** When the tool's result carries
`structuredContent`, the rows are read there at `rows_path`. Without it, the
text blocks are the answer:

- one text block is parsed as JSON and read at `rows_path`;
- several text blocks are one row each, in the order sent, and each must hold
  exactly one JSON object. `rows_path` must then be `.`, because there is no
  one document to follow a path into;
- no block at all, at the root, is no rows.

What `rows_path` may land on:

- **A list** is the rows. With `truncated: true` beside it (an MCP server at its
  `limit`) the reply says where the tool stopped and how to ask for more,
  never "all shown"; a `truncated` that is not true, false or null is refused.
- **An object, under a named path** such as `data`, is one row. Many MCP tools
  answer with one object: a tool returning `{"status": "ok", "data": {...}}`
  is read as one row with `rows_path: data`. An empty object `{}` there is no
  rows, not one empty row.
- **A wrapper object** is refused `result_unusable`: wherever `rows_path`
  lands on an object with exactly one key whose value is a list, such as
  `{"items": [...]}`, the binding stopped one level short, and the refusal
  names the path to set (`rows_path: data.items`).
- **An object at the root (`.`)** is one row only when it comes from a single
  text block. A `structuredContent` object at the root is refused, with its
  keys named: structured content is always an object, and FastMCP wraps a
  typed list as `{"result": [...]}`, so set `rows_path` to the key that holds
  the rows or the one object.
- **Anything else** (a number, a string, `null`) is refused
  `result_unusable`.

FastMCP tools that return an untyped list send one text block per item, and a
one-item list as one bare object.

A `rows_path` that lands on a wrapper with more than one key, such as
`{"status": "ok", "items": [...]}`, reads it as ONE row. Point `rows_path` at
the list, and set `row_fields` on any binding that can land on an object, so
such a wrapper is refused `result_unusable`.

**Limits.** A 401, 403 or connection failure on the client's background `GET`
stream, or a session close that reaches the deadline, reports
`upstream_unavailable` or `timeout` even when the tool result arrived.

**Errors.** Every call-time failure is `live_adapter_error`; its `failure` field
names the case:

| what happened | `failure` |
| --- | --- |
| the server is unreachable, refuses the token, answers 4xx or 5xx, or does not know the tool | `upstream_unavailable` |
| the tool returned `isError: true` (its text comes back as a short fenced excerpt) | `upstream_unavailable` |
| an `isError` text saying "empty set" | `upstream_empty_result` |
| an `isError` text whose first sentence says "not found" or "does not exist" | `upstream_not_found` |
| an `isError` text whose first sentence says "considered invalid", "is invalid" or "are invalid", "does not match schema", "validation error for", or "invalid argument", "invalid parameter" or "invalid expression" | `upstream_rejected_arguments` |
| a 3xx | `redirect_refused` |
| the session received more than `max_response_bytes` | `response_over_cap` |
| the reply is not an MCP reply, is compressed, or contains the token | `response_unusable` |
| the result has the wrong shape: no `rows_path`, neither a list nor an object at `rows_path`, a wrapper object or a root `structuredContent` object, non-JSON text, or the wrong number or kind of content blocks | `result_unusable` |
| the deadline ran out (the excerpt names the unanswered step) | `timeout` (a state) |

**Whose credential.** The credential the upstream system sees, such as a grid
proxy, lives in the MCP server's own container; the deployment holds only the
token for the server's gate. Nothing on this side can inspect that upstream
credential, so the binding must declare it: `upstream_credential: service` for a service account, or `personal`
for one person's credential. A missing value is refused at start. `personal` is
admitted only when the deployment's `contract.lifecycle` is `prototype` or
`demo`:

```yaml
contract:
  lifecycle: prototype
```

Under any other lifecycle, or none, the server refuses to start with
`live_adapter_personal_credential_refused`, naming the relation and the
lifecycle it saw. This is a declared rule that the server build enforces, not
something it detects: a false `service` declaration is the operator's responsibility.

## 3. Authorize the call

**Nomos** is the policy engine that decides each call. The live gate reads one
file beside the deployment manifest, `nomos_external_probe.json`, and nothing
else. The `nomos.deployment_posture` block in the manifest does not reach it.

A live call is refused while the deployment's access rules are on. When
`nomos.runtime_enforcement` is `audit` or `enforce`, or the server cannot tell
which it is, `expand(live=...)` fails with `live_refused_under_access_rules`
before this gate is asked and before anything leaves the process. So does
`expand(next=...)` on a live answer kept earlier. The gate decides as the
deployment's service principal, not as the reader, so it cannot stand in for
the reader's access rules. To serve live calls, run the deployment with
`runtime_enforcement: off`. The process-wide operator override
`OKG_NOMOS_RUNTIME_ENFORCEMENT=off` also turns access rules off for this
check, so a server started with it serves live calls even when the manifest
declares `audit` or `enforce`; no caller can set it.

```json
{
  "policy_bundle_id": "live-http-ci.live-relations", "policy_version": "1",
  "default_effect": "deny",
  "scope": {"deployment": "live-http-ci"},
  "rules": [{
    "rule_id": "allow_host_telemetry_for_the_runtime_service",
    "effect": "allow", "priority": 100,
    "actions": ["external.host.read_telemetry"],
    "subjects": ["service:okg-runtime"],
    "match": {"deployment": "live-http-ci", "subtype": "machine",
              "relation": ["cpu_per_core", "top_processes"]},
    "reason_code": "live_http_ci.host_telemetry_allowed"
  }]
}
```

Read it deny-first, and note that the two denials differ. No bundle on disk
denies with `nomos.default_deny.no_active_bundle`; a bundle that loaded and
matched no rule denies with `nomos.default_deny`. Any effect that is not
exactly `allow` is a deny. `match` names the relations, not only the action, so
a third relation reusing `external.host.read_telemetry` arrives denied.

The **subject** is the identity the call acts as. Under `principal_mode:
service` it is the binding's `service_principal` when the operator declared one,
and `service:okg-runtime` otherwise. Both are non-empty, so under the only mode
served today the subject always resolves; an unresolved one (a future caller
mode) would be denied with `live_principal_unresolved` and `adapter_ran:
false`, without consulting the bundle.

The bundle compiles once, at server start, so **editing it on disk takes effect
on the next restart** — a restart, not a stale allow: the gate is consulted on
every call and there is no result cache on this path.

## 4. What the agent sees

`inspect` publishes the relation — on the subtype and on a node of that subtype
— under `live_relations`, with its `description` and each argument's type, default and bounds, so the
agent is told the limits rather than meeting them as refusals.
`expand(live={relation, args})` asks it; rows come back under `external_live`,
beside the pinned graph rows and never merged into them:

```json
{
  "schema": "okg.external_live.v1", "relation": "top_processes",
  "state": "ok", "trust": "untrusted", "source": "http",
  "query": {"note": "the validated arguments"},
  "args_fingerprint": "...", "pinned_generation_id": "...",
  "rows": [], "policy": "inline"
}
```

`state` is one of `ok`, `denied`, `error` or `timeout`, and only `ok` carries
rows. A live failure never throws the graph answer away — the pinned rows beside
it still answer the graph question.

Every string **and every key** in `rows` is fenced, because whoever controls a
document upstream controls its field names too. `neutralised_keys` lists the
keys that were altered; it appears only beside inline rows, and only when at
least one key was altered, so its absence means "none", not "not checked".
Fencing marks external text so the read path does not treat it as
instructions; it reduces prompt injection and does not eliminate it.

**Size.** The rows share the call's one response budget with the graph rows:
`max_bytes`, or the deployment's `agent_surface.response_bytes` default when
the caller omits it. The bytes counted are the tool result the agent is sent,
fence markers and all. `expand` pages the graph rows first, leaving room for
a reserve: the largest envelope the live call can return that cannot shrink
further -- the no-rows digest form, or a full 1 KiB error body, whichever is
bigger. It then runs the adapter **once** into whatever room is left. When the rows do not fit, the envelope
carries the longest prefix that does, in the order the external system
returned them, plus a digest of the full set: `truncated: true`,
`truncation_reason: response_byte_budget`, `rows_returned`, `budget_bytes`,
`summary.row_count`, and a `refetch` block. With no room for even one row,
`policy` is `digest` and `rows_returned` is 0. Fencing inflates the rows by
an amount that depends on how many strings they hold, not on their total size,
so no count of `max_rows` predicts it; ask for a larger `max_bytes` instead.
If not even one graph row fits beside that reserve (or, for a `path` or
`depth>1` expand, which is never paged, the whole graph result does not), the
call is refused `response_byte_budget_exceeded` **before** the adapter runs;
the refusal names the reserve's size in `reserve_bytes`, and the sample
records it with status `error` and `refused_before_adapter` (a call the gate
also denied keeps status `denied` and still carries
`refused_before_adapter`). An
envelope that still would not fit becomes `state: error` with
`failure: response_bound_failed`, never an over-budget answer.

On `timeout` the error body carries `cut_by`: `dispatcher` when the dispatcher
gave up on the worker thread, `adapter` when the adapter cut its own call. Both
are `timeout` — which one wins is a race of microseconds — and only the
dispatcher's arm leaves a worker thread running.

**A cut answer goes on by `next`.** An answer cut to fit is kept, and the
reply names one call, `expand(next="<token>")`: the following rows, every
field whole, with no second upstream call. After a field cut it starts again
at row 1. Every `ok` answer is kept and named by its handle; see
[live handles](../reference/mcp-results.md#live-handles). It is kept 600
seconds for the calling reader and session only. Limits: 50,000 rows and
8 MiB per set, 64 sets and 32 MiB per reader and session, 1,024 sets and
128 MiB per process.

## 5. What is retained, and where

A **sample** is the record of one call, written after the adapter returns, in
its own short transaction; a call that cannot write its sample fails, because
an external call with no record is what the sample policy exists to prevent.
`sample_policy` decides what the row holds, `retention_class` how long it lives
and which table it lands in.

| `retention_class` | table | kept |
| --- | --- | --- |
| `hot` | `okg.call_sample_hot` | 7 days |
| `warm` | `okg.call_sample_warm` | 90 days |
| `cold` | `okg.call_sample_cold` | 730 days |

| `sample_policy` | what the row holds |
| --- | --- |
| `none` | nothing is written at all |
| `metadata` | principal, decision, status, the argument fingerprint |
| `inline` | the above plus the shaped rows, up to 16 KiB |
| `digest` | the above plus a sha256, a summary and a re-fetch pointer — and **no rows** |

`warm` and `metadata` are the defaults. A `digest` row stores the hash and the
summary and keeps no rows; the database refuses one that tries. The stored
payload on an `inline` row is the **shaped** one — the same fenced bytes the
agent received — not the adapter's raw return.

**Argument values and the re-fetch pointer.** The row carries the canonical
fingerprint of the arguments, so two calls can be recognised as the same
question afterwards. It does not carry the values. A `digest` or `metadata`
row's re-fetch pointer holds the call's source, the argument *names* — its
shape — and that fingerprint, and the envelope's own `refetch` block shows the
same thing rather than a second copy. The sample store can copy a named
argument's value in, and no live relation can ask it to: there is no
declaration key for that allow-list and every live relation registers with it
empty. Rows written before this rule existed do hold the values verbatim, for the
rest of their retention window.

The 16 KiB cap is a **storage** rule and decides nothing about what the
caller receives. The row stores the rows the caller received -- all of them,
or the prefix -- when they measure 16 KiB or less by the store's own encoding,
and a digest otherwise: `policy` is what was stored, `declared_policy` is what
the relation asked for, and they differ exactly when that happened. The row's
`attrs.caller_received` says what the caller got (`rows`, `rows_prefix` or
`digest`) and `attrs.caller_rows_sha256` hashes exactly those rows, so a
digest row still names the bytes the agent saw. A caller that received only
a digest gets a digest row, whatever the size.

Read a row back with SQL, against the table for its retention class:

```sql
SELECT observed_at, principal, decision, status, policy, declared_policy,
       payload_bytes, attrs
FROM okg.call_sample_hot
WHERE deployment_name = 'live-http-ci' AND relation = 'top_processes'
ORDER BY observed_at DESC LIMIT 5;
```

`attrs` carries `action`, `reason_code`, `adapter_ran`, `adapter_kind` and the
fingerprint. Read `adapter_ran` first after an incident: it says whether the
external system was contacted. No sample table is part of a published graph.

## 6. Troubleshooting by error code

In the order an operator meets them. Every code marked **start** stops the
deployment from starting; the rest reach a caller at call time.

| code | when | what it means |
| --- | --- | --- |
| `live_relation_action_missing` | start | a declared relation has no `action` |
| `live_relation_arg_schema_invalid` | start | an argument is unbounded, untyped, or both required and defaulted |
| `live_relation_principal_mode_invalid` | start | `principal_mode` is neither `service` nor `caller` |
| `live_relation_caller_mode_unsupported` | start | `principal_mode: caller`; declare `service` |
| `live_relation_sample_policy_invalid` | start | an unknown `sample_policy` or `retention_class` |
| `live_relation_invalid` | start | the annotation's shape, a bad deadline, or a stored declaration from another version |
| `live_relation_unbound` | start | a subtype declares it and no binding serves it |
| `live_binding_undeclared_relation` | start | a binding names a relation nothing declares |
| `live_relation_adapter_kind_unknown` | start | `kind` is outside `sql`, `command`, `http`, `mcp`, `python` |
| `live_adapter_unavailable` | start | the kind is one of those five and has no runner — today, anything but `http` and `mcp` |
| `live_adapter_config_invalid` | start | a named `config` field cannot produce a legal request |
| `live_adapter_arg_unconstrained` | start | the body slots an argument carrying no constraint |
| `live_adapter_host_refused` | start | the address is not an allowlisted IP literal, or is non-global without the opt-in |
| `live_adapter_secret_missing` | start | `secret_refs` names an environment variable that is not set |
| `live_adapter_personal_credential_refused` | start | an `mcp` binding declares a `personal` credential and `contract.lifecycle` is not `prototype` or `demo` |
| `live_relations_unbound_deployment` | call | the call named a relation on a deployment that binds none |
| `live_relation_undeclared` | call | the node's subtype does not declare that relation |
| `live_arguments_invalid` | call | an unknown key, an explicit null, or a value outside its bounds. A value that fails only because it lacks the leading `/` its pattern requires is refused with "name must start with '/'; did you mean '/<value>'?" (the one-line hint says to send '/' plus the value you gave) |
| `live_adapter_error` | call | the upstream refused, redirected, overflowed a cap, or answered unusably |
| `live_concurrency_exhausted` | call | too many live calls in flight; retry |
| `live_sample_write_failed` | call | the call ran and its record could not be written |

**The code and the envelope spell two of these differently, so grep for both.**
An adapter failure is the registered code `live_adapter_error`, and the
envelope's `error.reason` beside it reads `live_relation_adapter_error`. A cut
call is `state: "timeout"` with `error.reason` `live_relation_timeout` — that
reason is neither a state nor a registered code; `timeout` is the state.

A denial's reason says which of three things happened.
`nomos.default_deny.no_active_bundle`: no bundle on disk. `nomos.default_deny`:
a bundle loaded and no rule matched — check `match` and the subject before the
file. `nomos.gate_error`: the bundle could not be read or compiled, an outage
of the gate rather than a policy answer.

`live_principal_unresolved` is not a raised error and is unreachable under the
only mode served today; seeing it means the dispatcher is wrong, not the
manifest.

After changing a declaration, recompose the catalog with `okg catalog load`;
after changing a binding or the policy bundle, restart the server.

Next: the [live HTTP binding reference](../reference/live-http-bindings.md) for
every `config` field, the egress rules and the residual risk; the
[MCP tool surface](../reference/mcp.md) for how `inspect` and `expand` reach
it; [external deployments](deployments/external.md) for the other way a
deployment reads a system it does not own.
