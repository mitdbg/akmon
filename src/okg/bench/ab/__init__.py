"""okg-bench: the Claude Code +/- OKG A/B benchmarking framework.

PACT: okg-bench-ab-framework. Protocol: marketing/ab-experiment-protocol.md
(branch claude/akmon-open-source-marketing-68a32a).

One CLI (``okg-bench``), a benchmark-adapter interface (SWE-bench Verified,
SWE-QA, GitGoodBench, RegMiner), arm drivers that run headless Claude Code
under the operator's subscription, and a leakage-audited corpus builder that
constructs an OKG deployment from a repo's git+GitHub history up to a
machine-checkable cutoff.

Heavy dependencies (matplotlib for charts, psycopg for the corpus builder)
are imported lazily by the modules that need them; the CLI and adapters are
stdlib-only.
"""

SCHEMA_VERSION = "okg-bench-ab.v1"
