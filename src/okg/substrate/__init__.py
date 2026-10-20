"""okg.substrate — the Postgres-era OKG substrate.

Ships alongside the existing pipeline (src/okg/ingest, src/okg/core,
src/okg/persistence) through M6. Old code keeps working; new code
lands here. Final cutover retires the old packages.

Public surface:

- okg.substrate.types        — PhaseAResult, GateResult, ViolatorRow,
                                PublisherLockHeld, etc.
                                (docs/develop/architecture/pipeline.md is authoritative)
- okg.substrate.errors       — Exception classes; 1:1 with
                                docs/reference/error_codes.yaml
- okg.substrate.db.migrate   — okg migrate implementation
- okg.substrate.db.doctor    — okg doctor implementation
- okg.substrate.publisher    — Publisher class (Phase A/B/C)
- okg.substrate.cli.main     — `okg` CLI entry point
"""
