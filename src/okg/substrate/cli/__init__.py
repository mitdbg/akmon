"""okg substrate CLI surface.

Entry point: `okg` (installed as a console script).

v1 subcommands:

  okg migrate          - apply v1 schema
  okg doctor           - verify extensions, roles, DSN
  okg run --once <src> - synchronous one-shot (M1 skeleton)
  okg status           - deployment status (M1 skeleton)

Unknown subcommands fall through to the legacy agent CLI
(okg ask ...) so this dispatcher is backward-compatible.
"""
