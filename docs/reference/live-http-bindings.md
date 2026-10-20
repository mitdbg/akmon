---
audience: operator
type: reference
canonical_id: live-http-bindings
normative: true
---

# Live HTTP bindings

A **live relation** lets an agent ask an external system a question during a
read, instead of only reading what the graph already stored. The `http`
binding is how you point one at an HTTP API.

This page is the operator reference for that binding: every field, every
refusal and what each one protects, what a call retains and for how long, and
the residual risk you keep.

Read it before you write one. The binding turns an agent's arguments into
outbound network requests, which makes it the most security-sensitive block
in a deployment manifest.

## What it is

You declare the relation on a subtype, with its arguments and their bounds.
You bind it in the deployment manifest to an adapter. The substrate ships one
adapter, `kind: http`. It takes a JSON body you wrote, fills in typed holes
with the agent's validated arguments, makes one request, and returns rows.

The adapter knows nothing about the system it is calling. It copies your JSON
tree and substitutes values. You know what the fields mean, because you chose
the index and the field names.

```yaml
agent_surface:
  live_bindings:
    documents:
      kind: http
      config:
        base_url: http://127.0.0.1:9200
        allowed_hosts: ["127.0.0.1:9200"]
        allow_non_global: true
        method: POST
        path: /logs-*/_search
        body:
          size: {$arg: limit}
          query:
            bool:
              must:
                - query_string: {query: {$arg: query}}
                - range: {"@timestamp": {gte: {$arg: time_window}}}
        rows_path: hits.hits
        max_rows: 200
      secret_refs: ["env:OPENSEARCH_TOKEN"]
```

Everything below is checked when the server starts. A binding that could
never produce a legal request fails the build, naming the relation and the
field. It is not discovered on an agent's first call.

## The config fields

| field | required | what it is |
| --- | --- | --- |
| `base_url` | yes | scheme, IP literal and port. Nothing else. |
| `allowed_hosts` | yes | the addresses this binding may dial, as `ip` or `ip:port`. |
| `allow_non_global` | no (false) | permits loopback, private and link-local addresses. |
| `method` | no (`GET`) | `GET` or `POST`. |
| `path` | yes | the request path, starting with `/`. |
| `content_type` | no (`application/json`) | the request body's media type. |
| `auth_header` | no | the name of the one header the secret is sent in. |
| `body` | required for `POST` | your JSON document, with `{$arg: name}` slots. |
| `rows_path` | yes | dotted path to the list of rows, or `.` for the body itself. |
| `max_rows` | yes | 1–10,000. Cuts the answer by count. |
| `row_fields` | no | list of top-level keys each row keeps; others are dropped. A row that is not an object, or lacks a named key, is `response_unusable`. Use it when an upstream row carries fields of unbounded size, such as a process command line. |
| `max_response_bytes` | no (8 MiB) | 1 KiB–32 MiB. Caps the decoded response. |

**An unknown key is refused, not ignored.** A key that was quietly dropped is
how you come to believe a limit is set when it is not.

## Arguments: the `$arg` slot

`{"$arg": "name"}` is a slot. At call time the slot **object** is replaced by
the argument's validated value. A string argument becomes a JSON string, in
the position you chose. That is the whole language.

There is no templating, no conditionals, no iteration and no computation.

**Slots are legal only in value position.** Each of these is refused when the
server starts:

- a slot used as an object **key**
- a slot anywhere in `path`, `base_url`, `method` or a header
- `{"$arg": "q", "default": 5}` — a slot with sibling keys
- `{"$arg": {"name": "q"}}` — a slot whose value is not a plain name
- `"prefix {\"$arg\": \"q\"}"` — a slot spelled inside a string
- a slot naming an argument the relation does not declare
- a declared argument that no slot uses

The last one matters more than it looks: an argument `inspect` advertises and
the body never uses is a knob that does nothing, and an agent only finds out
by getting the same answer for every value.

Substitution is **single-pass**. A substituted value is never re-scanned, so
an argument whose value happens to be the text `{"$arg": "other"}` stays that
text. There is no escape for a literal `$arg` key: a body that needs one
cannot be written here, which is the price of the grammar having exactly one
reading.

**Why it is shaped this way.** Your body is configuration — reviewed, in the
manifest, yours. The argument values come from an agent and are the only
untrusted part. Restricting untrusted input to typed values in positions you
chose means an agent can change *what is asked* and never *what shape the
request has*. A general template loses that, and with it any ability to say
an argument cannot reach a `script` field.

**`GET` with any declared argument is refused.** A `GET` has no body, and
query-string arguments are not supported in this version, so there is nowhere
to put one. A relation that takes arguments uses `POST`.

## Arguments must be constrained

The adapter refuses, at startup and by name, any binding whose body slots an
argument carrying no constraint:

| type | needs |
| --- | --- |
| `string` | `enum`, or `pattern` **and** `maxLength` |
| `integer`, `number` | `enum`, or `minimum` **and** `maximum` |
| `boolean` | nothing — the type is the constraint |

**This catches an absent constraint, not a weak one.** `pattern: "^.*$"` with a
`maxLength` satisfies the rule and still admits `*:*` into a query string, or
`now-10y` into a range filter that scans a decade of an index. Whether a
pattern is tight enough is a fact about the upstream query language and your
index, not about the request shape, so the adapter cannot judge it.

**You own the quality of the constraint. This rule only guarantees there is
one to review.** It is written here plainly because an operator who believes
the check is stronger than it is writes a weaker pattern than they otherwise
would.

**Patterns must be anchored.** `pattern` is applied as a *search*, as it is
in JSON Schema. An unanchored `[A-Za-z0-9 ]{1,60}` would accept `!!bad!!`,
because the substring `bad` matches — it restricts nothing about the alphabet,
and only `maxLength` bounds anything. So a declaration whose pattern is not
anchored at both ends is refused at load with
`live_relation_arg_schema_invalid`, naming the relation and argument. Write
`^[A-Za-z0-9 ]{1,60}$`, or `\A...\z`; put any alternation inside a group
(`^(?:a|b)$`); and use `\A`/`\z` rather than `^`/`$` under the multi-line
flag.

Prefer `enum` wherever the set is knowable — for a time window, an
aggregation field or an interval it almost always is. It is also published by
`inspect`, so the agent is told the choices instead of guessing and being
refused:

```yaml
time_window:
  type: string
  enum: ["now-15m", "now-1h", "now-24h", "now-7d"]
field:
  type: string
  enum: ["@timestamp", "event.created"]
limit:
  type: integer
  minimum: 1
  maximum: 200
```

## Where it may connect

**Hosts are IP literals. Hostnames are refused in this version.** A hostname
is checked before the socket opens, and DNS can answer differently for the
check than for the connection. With an IP literal the URL's host *is* the
address, so there is no gap to race. Hostnames return when the connection can
be pinned to the address that was checked; that work is not built yet.

Addresses are canonicalised before they are compared, so every spelling
collapses to one value first. `::ffff:127.0.0.1` is 127.0.0.1. `0177.0.0.1`,
`2130706433` and `127.0.0.1.` are refused outright rather than quietly
resolved. A `base_url` with a path, query, fragment or `user:password@` is
refused.

**Ports are exact.** A bare `127.0.0.1` means that address on the scheme's
default port and **not** any port. `127.0.0.1:9200` in the allowlist does not
allow `127.0.0.1:5432`. An allowlist whose entries covered every port would
authorise the admin interface next door to the one you meant.

**IPv6 must be bracketed**, in `base_url` and in every allowlist entry:
`[::1]` or `[::1]:9200`. An unbracketed entry is refused by name, because
`::1:9200` is both a valid address and exactly what someone writes meaning
`::1` on port 9200, and either guess authorises a host nobody asked for.

**Non-global addresses need the opt-in.** Loopback, private, link-local
(including `169.254.169.254`), multicast, reserved and unspecified addresses
are refused unless the binding sets `allow_non_global: true`. Set it when you
mean it: one visible line in the manifest is exactly the fact a reviewer
should see.

**Redirects are never followed.** Any 3xx is an error. The `Location` target
is never requested and never echoed, because a redirect is how a credentialed
request gets re-sent somewhere no allowlist covers.

**Proxy environment is ignored.** The session sets `trust_env=False`, so
`HTTP_PROXY` cannot silently route a gated call through an unchecked host.
The same flag also disables `REQUESTS_CA_BUNDLE` — if the upstream needs a
private CA, install it in the system trust store.

**TLS verification is not configurable.** There is no `verify: false`
anywhere. A system whose certificate cannot be verified is a deployment
problem to fix, and an off switch in config is how that becomes permanent.

## Secrets

`secret_refs` takes exactly one entry, of the form `env:NAME`. Nothing else
is accepted — no file paths, no vault URIs, no bare names, and no inline
`KEY=VALUE`. One scheme means one place a credential can live.

`auth_header` carries a header **name**, never a value. A literal credential
there is refused by name. There is no second way to configure a secret, and
therefore no second way to leak one.

The value is read once, when the server starts. A missing variable fails the
build, and the error names the **reference**, never the value. It is then
held in a closure — not on the binding, not in `config` — so a `repr()` in a
traceback cannot print it. No argument, and no part of an argument, can reach
any header.

## Limits and the deadline

The response is bounded **while it is read**, not after.

- **The byte cap counts decoded bytes.** A small compressed body that decodes
  to gigabytes is refused at the cap. Counting wire bytes would admit it.
- **Over the cap is a refusal, not a truncation.** A half-read JSON body is
  not a smaller answer.
- **Parsing is strict**, because `json.loads` is not. `NaN`, `Infinity` and
  `-Infinity` are refused — Python accepts them and writes them straight back
  out, so they would reach the agent and the stored row as text no JSON
  reader can parse. So are duplicate object keys (which value wins is a
  parser choice, not a fact about the document), a non-UTF-8 body, a leading
  byte-order mark, an integer literal past Python's digit limit, and a
  document whose arrays and objects nest more than 256 levels deep. The
  depth is measured before parsing, because a nesting bomb small enough to
  pass the byte cap (20,000 levels is about 40 KB) would otherwise exhaust
  the parser's stack. That refusal is reported as `response_over_cap`.
- **`rows_path` must resolve to a list.** Absent, or present and not a list,
  is an error — never an empty result. "The shape changed" must not read as
  "no rows": an agent told zero rows stops looking.
- **`max_rows` cuts by count**; the response budget then cuts by bytes.
  Rows that do not fit the caller's `max_bytes` (or the deployment default)
  come back as the longest prefix that fits plus a digest of the full set,
  not as a refusal. The call is refused, before the adapter runs, only
  when the graph rows leave no room for even the live relation's reserve
  (see the live relations guide). Fencing adds bytes per string, so the fenced size is
  what counts, not the upstream body's.
- **The 24-level depth cap and the node cap are charged on the rows you get
  back**, after `rows_path` and after `max_rows` — not on the whole
  document. A wide endpoint would otherwise fail however small a `max_rows`
  you set, and fail only sometimes, depending on how much the upstream had
  to report that second. The whole document is bounded by the decoded-byte
  cap and by the 256-level parse-depth limit above.

### Binding an endpoint that returns a bare array

Some APIs answer a `GET` with a top-level JSON array rather than an object —
Glances does this for its per-CPU and process-list endpoints. Write
`rows_path: "."` for those: it means **the response body is itself the list
of rows**.

```yaml
kind: http
config:
  base_url: http://127.0.0.1:61208
  allowed_hosts: ["127.0.0.1:61208"]
  allow_non_global: true
  method: GET
  path: /api/4/percpu
  rows_path: "."
  max_rows: 32
```

`.` is the only spelling accepted for the root. An empty string is refused,
because `""` is what an absent or mistyped key looks like and a config file
should not spell "the whole document" the same way it spells "I forgot to
fill this in". `$`, `..` and a leading or trailing dot are refused too.

The "must resolve to a list" rule still applies: if that endpoint ever
answers with an object instead, the call is an error rather than no rows.

A `GET` still cannot take arguments — it has no body and no query parameters
here, so a relation that declares any is refused at load.

### The deadline, stated honestly

The relation's `deadline_seconds` is enforced twice: once by the dispatcher,
which abandons the worker thread, and once **inside** the adapter, which is
the one that matters. An abandoned thread keeps its concurrency slot until
its own I/O returns, and there are only eight slots in the process.

Two mechanisms compose, and neither alone is a bound:

| upstream behaviour | what cuts it |
| --- | --- |
| never accepts the connection | the connect timeout, set from the remaining budget |
| silent after its headers | the per-read socket timeout, likewise from the budget |
| one byte per second | a watchdog that breaks the socket when the budget is spent |

The third row needs the watchdog and is not covered by the other two. A
per-read timeout is a quiet period that a drip resets forever. A check
between chunks does not help either, because the client fills a 64 KiB
buffer before it yields a chunk — at 4 KiB a second that is one read lasting
sixteen seconds, and the check never gets to run. Measured at 15.1 seconds
against a 2-second deadline before the watchdog was added, and 2.1 seconds
after.

The watchdog calls `socket.shutdown()`, not just `close()`: closing a
response returns its connection to the pool but does not interrupt a read
already blocked in `recv`.

Whatever exception the broken connection produces, a call whose budget has
expired is reported as `timeout`. Reporting the transport's view of it would
send you to inspect a healthy upstream.

## Errors

| what happened | `failure` | state |
| --- | --- | --- |
| connection refused, TLS failure, non-2xx | `upstream_unavailable` | error |
| any 3xx | `redirect_refused` | error |
| over the byte cap, document nested over 256 levels, or rows over the depth or node cap | `response_over_cap` | error |
| not JSON, unparsable, or `rows_path` missing | `response_unusable` | error |
| the budget ran out | `timeout` | `timeout` — always; see below |

An adapter never passes a network exception's own message through: that text
routinely carries the URL it was dialling, credentials included. Each class
has a fixed message.

Error data carries the relation, the failure class, that fixed message, the
HTTP status where there was one, and at most a 256-byte excerpt of the
upstream body — fenced, and with any `user:password@` in it stripped. **It
does not carry the URL.** The address is in your manifest; repeating it in
every error payload would publish it into logs and envelopes for no reader
who does not already have it.

An exception nobody predicted falls to a backstop that reports the exception
**type name** and no message text at all.

A call cut at its deadline is always `state: "timeout"`. Two mechanisms can
cut it — the dispatcher gives up on the worker thread, or the adapter cuts its
own call — and which one wins is a race of microseconds, so the state no
longer depends on it. The error body names the winner in `cut_by`
(`dispatcher` or `adapter`); only the dispatcher's arm leaves a worker thread
running, and only that arm logs `okg.live_relation.adapter_abandoned`.

An adapter that raises Python's `TimeoutError` is reporting **its own**
failure, not the deadline: that is `state: "error"` with `failure:
"adapter_raised"`, and nothing is logged as abandoned. An adapter that means
the deadline raises `LiveAdapterError("timeout")`.

## What a live call retains

This is the part you cannot see from the manifest, and the part to check
before calling a binding safe. It is read off the code, not the intention.

**Never stored:** the rendered request body, and the agent's argument
**values**. Only a canonical fingerprint of the arguments is kept. The
envelope returned to the caller does carry the validated arguments, but that
is the answer to the caller's own call, not a stored record.

**Stored, by the relation's `sample_policy`:**

| policy | what the row holds |
| --- | --- |
| `none` | no row at all |
| `metadata` | identity, decision, status and the args fingerprint — **no payload** |
| `inline` | the above, plus the **shaped** rows, up to 16 KiB |
| `digest` | the above, plus a sha256, a 512-character summary and a re-fetch pointer |

Under `inline` and `digest` the payload stored is the **shaped** one — fenced
and key-neutralised, the same bytes the agent received — not the adapter's
raw return. The 16 KiB cap applies to what is **stored**, measured by the
store's own encoding, and not to what the agent receives: an `inline` payload
over it degrades to `digest`, the row records that it did, and
`attrs.caller_received` records whether the agent got the rows, a prefix, or
only the digest.

**Retention is by class**: `hot` 7 days, `warm` 90 days, `cold` 730 days.
`warm` is the default, so an `inline` sample of an upstream document sits in
the database for 90 days.

Choose `metadata` for a log-like source unless you need the content. It keeps
the decision and the fingerprint — enough to audit that a call happened and
with what shape of arguments — and retains none of the upstream document.

A failed call still writes its row. A rollback cannot un-call an external
system; it can only erase the evidence that it was called.

## The residual risk

**Content returned by an external system is untrusted text that an agent will
read.** The adapter fences every string and every key it returns, and records
which keys it altered, because whoever controls a document in the upstream
system controls its field names as well as its values. Fencing marks the
text; it does not make it safe.

Two things follow. Point a binding only at a system whose contents you would
be willing to show an agent. And keep the relation's `action` behind a Nomos
gate that authorises it narrowly — the gate decides whether a call may happen
at all, and it is the only thing that does.

## See also

- [MCP tool surface](mcp.md) — how `expand(live=...)` reaches this adapter.
- [Security](security.md) — the deployment-wide trust boundaries.
- [External deployments](../operate/deployments/external.md) — external-backed
  fields, the other way a deployment reads a system it does not own.
