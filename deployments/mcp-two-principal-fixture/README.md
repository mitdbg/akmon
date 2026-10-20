# mcp-two-principal-fixture

This fixture declares the authenticated, per-user MCP identity contract for
repeatable pull-request proof. It is not an operated deployment.

`okg mcp-serve --transport streamable-http --auth-token-env VAR
--multi-user-principals` refuses to start unless the deployment it serves:

1. enables Nomos runtime enforcement (`nomos.runtime_enforcement: audit` here),
   so resolved principals govern reads;
2. declares `nomos.principal_mapping`, naming the header that carries the
   signed-in user; and
3. admits that mapping's `principal_kind` in
   `nomos.deployment_posture.read_principals`.

Other fixture deployments leave enforcement off. `deployment.yaml` records
this fixture’s mapping and delegation risk.

## What uses it

- `tests/substrate/mcp/test_two_principal_fixture_deployment.py` -- hermetic;
  asserts the manifest declares all three, read through the same functions the
  server calls at start.
- `tests/substrate/mcp/test_multi_user_http_e2e.py` -- starts `okg mcp-serve`
  drives two principals through real subprocesses. Rotation replaces the
  bearer environment value and restarts the server; clients reconnect. The
  old bearer then receives 401. Any bearer holder can still assert either
  principal: rotation does not identify the fronting application.
- `tests/substrate/mcp/_two_principal_fixture.py` -- the database side: the
  identity provider, two subject bindings, the grants and the policy bundle,
  written through the public Nomos helpers. Its constants must match
  `deployment.yaml`; the hermetic test above enforces that.

## No database is declared

`postgres.dsn` interpolates `${OKG_DSN}` and no `database:` is named: the
proof creates and drops a disposable database per run and passes that DSN
explicitly. Nothing here is meant to persist.
