# OKG local lifecycle controller

`okg-local-controller` is the candidate-independent lifecycle kernel for one local
OKG deployment. It owns a signed installation identity, an external SQLite saga
ledger, sandboxed candidate probes, trusted in-process effect/target ports, and the
three-action activation protocol (authorize, publish, observe/finalize).

The package deliberately has no `okg` import. A supervisor supplies trusted
in-process implementations for product-specific upgrade, service, and Chronos
publication behavior and pins their adapter-set digest in every transition.
The v1 wire schema retains the field name `port_manifest_digest`; in this narrowed
library it is the opaque adapter-set digest, not a subprocess command manifest.

Candidate execution requires the platform's trusted sandbox: `sandbox-exec` on
macOS or Bubblewrap (`bwrap`) on Linux. If that boundary is unavailable, the
launcher refuses the probe; it never falls back to an unsandboxed process.

This distribution does **not** install or repair itself and does not expose a
standalone lifecycle CLI or subprocess adapter transport. The rescued dual-slot
bootstrap depended on the managed release's Python runtime. The rescued subprocess
transport used replayable shared-secret authorizations. Both were excluded until a
supervisor-owned runtime closure and a verification-only, single-use process authority
can be designed and proved independently.

The repository PACT at
`pact/changes/externalize-local-okg-lifecycle-controller/` is the normative behavior
and verification contract.
