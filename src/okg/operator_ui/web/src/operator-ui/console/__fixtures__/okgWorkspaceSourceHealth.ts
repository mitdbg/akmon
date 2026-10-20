// Every source row of okg-workspace's live `console/producers` answer,
// captured 2026-09-22 on generation okg:okg-workspace:branch:default:gen:20260908T045336779290Z:8c64bbc20bb1,
// BEFORE PACT console-producer-health-from-recorded-outcome (issue #2205).
//
// Only the fields the row state is decided from are kept: the name, the
// kind, `health.status` and the convergence `workflow.state` with its
// recorded reason. Nothing is edited. Twenty-four of these rows carry
// `health.status: "healthy"` with no fresh watermark and no recorded run
// behind the word; after the change the read serves `unknown` for them.
export const OKG_WORKSPACE_SOURCES_BEFORE = [
  {"name": "code-structure", "kind": "source", "health": {"status": "stale"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_stale"}},
  {"name": "pact", "kind": "source", "health": {"status": "stale"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_stale"}},
  {"name": "openspec", "kind": "source", "health": {"status": "stale"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_stale"}},
  {"name": "git-files", "kind": "source", "health": {"status": "stale"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_stale"}},
  {"name": "agent-sessions", "kind": "source", "health": {"status": "missing"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_missing"}},
  {"name": "coordination-config", "kind": "source", "health": {"status": "stale"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_stale"}},
  {"name": "agent-memory", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "agent-runtime-security-monitor", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "bib-references", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "branch-dispositions", "kind": "source", "health": {"status": "degraded"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "chat-sessions", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "code-data-flow", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "code-lsp-resolution", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "codeql-data-flow", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "codex-sessions", "kind": "source", "health": {"status": "missing"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_missing"}},
  {"name": "compute-env-local", "kind": "source", "health": {"status": "degraded"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "coordination-native", "kind": "source", "health": {"status": "stale"}, "workflow": {"state": "blocked", "reason": "completed_scope_watermark_stale"}},
  {"name": "coordination-packets", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "doc-corpus", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "git-history", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "github-issues", "kind": "source", "health": {"status": "degraded"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "instrumentation-log", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "interfaces", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "junit-xml", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "latex-project", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "latex-project-doc-corpus", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "package-manifests", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "papers-git-files", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "profile-measurements", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "security-analysis", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "self-play-reports", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "semgrep-data-flow", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "test-reports", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "fresh", "reason": "source_not_required_for_readiness"}},
  {"name": "_system:native_views", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "unsupported", "reason": "source_not_in_convergence_status"}},
  {"name": "system:external_backed_collector", "kind": "source", "health": {"status": "healthy"}, "workflow": {"state": "unsupported", "reason": "source_not_in_convergence_status"}},
] as const;
