"""invariants.yaml loader + SQL-write detection (M5.1).

An invariant is a SQL query that RETURNS VIOLATOR ROWS. Empty
result = pass; any rows = fail; the rows themselves form the
sample in `invariant_summary` on the blocked generation (M5.2).

Load-time validation:
  - `name` is a non-empty identifier.
  - `severity ∈ {error, warn}`.
  - `scope ∈ {source, cross}`.
  - `source` is required when `scope='source'` and forbidden
    when `scope='cross'`.
  - `sql` body contains NO write-shaped statements (INSERT,
    UPDATE, DELETE, TRUNCATE, CREATE, DROP, ALTER, GRANT,
    REVOKE, COPY, MERGE, CALL, LISTEN, NOTIFY, VACUUM, ANALYZE,
    REFRESH MATERIALIZED VIEW, DO, BEGIN, COMMIT, ROLLBACK,
    SAVEPOINT, RELEASE, SET, RESET). Regex-based detection
    that strips SQL comments first.
  - Optional `timeout_ms` (positive integer).
  - Optional `requires_sources` (non-empty list of source registry
    names). When a named source is off in the deployment (not
    registered, or `enabled_by_default: false`), the publish gate
    skips the invariant and reports it as skipped (issue #3624):
    an invariant about data only that source writes cannot hold
    while the source is off, and it must not block unrelated content.
    `load_deployment_invariants` refuses a name the deployment's
    source registry does not know, so a typo cannot skip a check.

Write detection is a first-line defense; the Phase B evaluator
also runs each invariant inside a `SET LOCAL TRANSACTION READ
ONLY` transaction so any write that slips past the regex aborts
at the DB.

SCOPE AT M5.1: SQL invariants only. Python invariants are
mentioned in design.md but defer to a follow-up (they need a
registration mechanism + security story for arbitrary user
code — out of scope for this stride).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError


__all__ = [
    "DEFAULT_TIMEOUT_MS",
    "InvariantSpec",
    "discover_invariant_files",
    "install_invariants",
    "load_deployment_invariants",
    "load_invariants",
    "sql_contains_writes",
]


DEFAULT_TIMEOUT_MS = 5000  # per design.md Phase B — 5s default per-invariant


# Keywords that signal a write or a transaction-control
# statement. Case-insensitive. Matched at word boundaries so
# e.g. "UPDATED_AT" in a comment or column name doesn't trip.
# The list is deliberately over-broad — a legitimate SELECT
# doesn't need any of these. False positives are a feature.
_WRITE_KEYWORDS = (
    "INSERT", "UPDATE", "DELETE", "TRUNCATE",
    "CREATE", "DROP", "ALTER",
    "GRANT", "REVOKE",
    "COPY", "MERGE", "CALL",
    "LISTEN", "NOTIFY",
    "VACUUM", "ANALYZE",
    "REFRESH",    # REFRESH MATERIALIZED VIEW
    "DO",         # anonymous PL/pgSQL block
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
    "SET", "RESET",
    "LOCK",
)

_WRITE_RE = re.compile(
    r"\b(?:" + "|".join(_WRITE_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# Strip `-- line comments` and `/* block comments */` before
# scanning. Crude but good enough for this check; we don't need
# a full SQL parser.
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
# String literals (single-quoted; PG also allows $$ dollar
# quotes, but invariants shouldn't need them; detect and reject
# dollar-quotes below).
_STRING_RE = re.compile(r"'(?:[^']|'')*'")


@dataclass(frozen=True)
class InvariantSpec:
    """One loaded invariant, ready to install into `okg.invariants`.

    Two kinds:

    - ``kind='sql'`` (default): row source is ``sql_body``,
      evaluated inside the Phase B gate. Existing behavior.
    - ``kind='llm_judge'`` (per ``add-semantic-llm-judge-invariants``):
      row source is ``candidates_sql`` (returns
      ``(candidate_id TEXT, payload JSONB)``); each row is judged
      by ``judge_model`` against ``judge_prompt``; verdicts in
      ``fire_on`` are recorded as fires. Runs OUTSIDE the gate
      via ``okg run --include-llm-invariants``.

    Field-set per kind:

    | field           | sql      | llm_judge |
    |-----------------|----------|-----------|
    | sql_body        | required | forbidden |
    | candidates_sql  | forbidden| required  |
    | judge_prompt    | forbidden| required  |
    | judge_model     | forbidden| required  |
    | sample_cap      | forbidden| required  |
    | fire_on         | forbidden| optional (default ['inconsistent']) |
    """

    name: str
    severity: str           # 'error' | 'warn'
    scope: str              # 'source' | 'cross'
    source: Optional[str]   # required iff scope='source'
    sql_body: Optional[str]  # required iff kind='sql'
    timeout_ms: Optional[int]  # None → deployment default
    # Optional deployment-level audit annotation. When the SQL
    # body reads an attr produced by a regex/heuristic extractor
    # rather than a structured catalog field, the YAML may carry
    # a `precision_audit_ref:` mapping pointing at the audit doc
    # that established the extractor's precision floor. The
    # substrate validates the shape but does not gate installs on
    # the contents — deployments enforce content-level rules
    # (e.g. af_pubs's PDF-derived attr gating).
    precision_audit_ref: Optional[dict[str, Any]] = None
    # LLM-judge fields (per `add-semantic-llm-judge-invariants`).
    # All None when kind='sql'; all required when kind='llm_judge'.
    kind: str = "sql"                                # 'sql' | 'llm_judge'
    candidates_sql: Optional[str] = None
    judge_prompt: Optional[str] = None               # path string
    judge_model: Optional[str] = None
    sample_cap: Optional[int] = None
    fire_on: Optional[tuple[str, ...]] = None        # default ('inconsistent',)
    # Spend admission for the judge lane (coherent-parallelism /
    # model-call-admission). Both-or-neither: a cap without an estimate
    # cannot reserve, an estimate without a cap bounds nothing. Both
    # None leaves the lane spend-UNBOUNDED, which the runner reports
    # via `ModelCallSpend.enabled` — never silently defaulted.
    judge_spend_cap_usd: Optional[float] = None
    judge_spend_estimate_usd: Optional[float] = None
    # Concept scope (per `add-concept-scoped-invariants`). When
    # non-None, the runner injects a `scope` CTE before the user's
    # row-source SQL that filters nodes to the named Concept's
    # neighborhood. Shape:
    #   {"concept_name": str, "min_confidence": float,
    #    "hops": int, "hop_edges": tuple[str, ...]}
    # `hops` defaults to 1 (direct mentions only); `hop_edges`
    # defaults to ('mentions',) and is only consulted when hops > 1.
    concept_scope: Optional[dict[str, Any]] = None
    # Sources this invariant's data comes from (issue #3624). None =
    # always evaluated. Set = skipped, and reported as skipped, while any
    # of them is off in the deployment.
    requires_sources: Optional[tuple[str, ...]] = None


def sql_contains_writes(sql: str) -> Optional[str]:
    """Return the first write-keyword hit (uppercased), or None.

    Strips comments and single-quoted strings first so the keyword
    check only sees SQL tokens, not text content. Rejects
    dollar-quoted strings outright (invariants shouldn't need them).
    """
    if "$$" in sql or re.search(r"\$[a-zA-Z_]*\$", sql):
        return "DOLLAR_QUOTE"

    # Strip comments then strings.
    stripped = _BLOCK_COMMENT_RE.sub(" ", sql)
    stripped = _LINE_COMMENT_RE.sub(" ", stripped)
    stripped = _STRING_RE.sub("''", stripped)

    m = _WRITE_RE.search(stripped)
    if m:
        return m.group(0).upper()
    return None


_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_VERDICT_LITERALS = ("consistent", "inconsistent", "uncertain")
_LLM_JUDGE_KEYS = (
    "candidates_sql", "judge_prompt", "judge_model", "sample_cap",
    "fire_on", "judge_spend_cap_usd", "judge_spend_estimate_usd",
)


def _reject_dropped_graph_relations(
    sql: str, *, where: str, name: str, field: str,
) -> None:
    """Refuse a body bound to a relation the Chronos cutover DROPPED.

    ``okg.nodes_live`` / ``okg.edges_live`` are not merely unmaintained:
    ``retire_legacy_graph_storage`` issues DROP TABLE for both on every
    migrate, so the read fails at publish time with undefined_table and no
    checkout can repair it. Refuse at authoring time and name the maintained
    replacement, which the publish gate now reads through a pinned checkout.
    """
    from okg.substrate.catalog.invariant_graph_read import (
        DROPPED_GRAPH_RELATIONS,
        dropped_graph_relations_in,
    )

    offenders = dropped_graph_relations_in(sql)
    if not offenders:
        return
    offender = offenders[0]
    replacement = DROPPED_GRAPH_RELATIONS[offender]
    err = SubstrateError(
        f"{where}: invariant {name!r} reads {offender}, which the Chronos "
        f"cutover dropped; the read fails at publish time with "
        f"undefined_table. Use {replacement} instead.",
        data={
            "where": where,
            "name": name,
            "field": field,
            "offending_relation": offender,
            "replacement": replacement,
            "offending_relations": offenders,
        },
        fix_hint=(
            f"replace {offender} with {replacement}; the publish gate reads "
            "the maintained graph views through a pinned Chronos checkout"
        ),
    )
    err.code = "invariant_body_dropped_relation"
    raise err


def _validate_single(
    body: dict[str, Any], *, where: str,
    yaml_dir: Optional[Path] = None,
) -> InvariantSpec:
    if not isinstance(body, dict):
        raise SubstrateError(
            f"{where}: invariant entry must be a mapping; got "
            f"{type(body).__name__}",
            data={"where": where},
        )
    allowed = {
        "name", "severity", "scope", "source", "sql", "timeout_ms",
        "description", "precision_audit_ref", "kind",
        "concept_scope", "requires_sources",
        *_LLM_JUDGE_KEYS,
    }
    extras = set(body) - allowed
    if extras:
        raise SubstrateError(
            f"{where}: unknown keys {sorted(extras)}; allowed: "
            f"{sorted(allowed)}",
            data={"where": where, "extras": sorted(extras)},
        )

    # `kind` must come first because per-kind required-key sets diverge.
    kind = body.get("kind", "sql")
    if kind not in ("sql", "llm_judge"):
        raise SubstrateError(
            f"{where}: kind must be 'sql' or 'llm_judge'; got {kind!r}",
            data={"where": where, "kind": kind},
        )

    if kind == "sql":
        common_required = ("name", "severity", "scope", "sql")
    else:
        # llm_judge: forbid `sql`, require the judge fields. The check
        # is done in two passes so the operator gets a complete error
        # list per YAML reload, not a one-at-a-time peelback.
        common_required = (
            "name", "severity", "scope",
            "candidates_sql", "judge_prompt", "judge_model", "sample_cap",
        )
    for required in common_required:
        if required not in body:
            raise SubstrateError(
                f"{where}: missing required key {required!r} "
                f"(kind={kind})",
                data={"where": where, "missing": required, "kind": kind},
            )

    name = body["name"]
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise SubstrateError(
            f"{where}: name must match ^[a-z_][a-z0-9_]*$; got "
            f"{name!r}",
            data={"where": where, "name": name},
        )

    severity = body["severity"]
    if severity not in ("error", "warn"):
        raise SubstrateError(
            f"{where}: severity must be 'error' or 'warn'; got "
            f"{severity!r}",
            data={"where": where, "severity": severity},
        )

    scope = body["scope"]
    if scope not in ("source", "cross"):
        raise SubstrateError(
            f"{where}: scope must be 'source' or 'cross'; got "
            f"{scope!r}",
            data={"where": where, "scope": scope},
        )

    source = body.get("source")
    if scope == "source" and not source:
        raise SubstrateError(
            f"{where}: scope='source' requires a source name",
            data={"where": where},
        )
    if scope == "cross" and source is not None:
        raise SubstrateError(
            f"{where}: scope='cross' invariants must not set "
            f"`source`; got {source!r}",
            data={"where": where, "source": source},
        )

    # SQL body validation differs by kind.
    sql_body: Optional[str] = None
    candidates_sql: Optional[str] = None
    judge_prompt: Optional[str] = None
    judge_model: Optional[str] = None
    sample_cap: Optional[int] = None
    fire_on_tuple: Optional[tuple[str, ...]] = None
    judge_spend_cap: Optional[float] = None
    judge_spend_estimate: Optional[float] = None

    if kind == "sql":
        # Forbid llm_judge keys on sql kind so a typo doesn't silently
        # land an llm_judge field in a sql invariant row.
        for forbidden in _LLM_JUDGE_KEYS:
            if forbidden in body:
                raise SubstrateError(
                    f"{where}: kind='sql' invariant must not set "
                    f"{forbidden!r}; that field belongs to "
                    f"kind='llm_judge'",
                    data={
                        "where": where, "name": name,
                        "offending_field": forbidden,
                    },
                )
        sql_body = body["sql"]
        if not isinstance(sql_body, str) or not sql_body.strip():
            raise SubstrateError(
                f"{where}: sql must be a non-empty string",
                data={"where": where},
            )
        write = sql_contains_writes(sql_body)
        if write is not None:
            err = SubstrateError(
                f"{where}: invariant body contains write-shaped token "
                f"{write!r}; invariants must be read-only",
                data={
                    "where": where, "name": name,
                    "offending_keyword": write,
                },
                fix_hint=(
                    "remove write/DDL/transaction-control statements from "
                    "the invariant body; invariants are SELECT queries "
                    "that return violator rows"
                ),
            )
            err.code = "invariant_body_writes"
            raise err
        _reject_dropped_graph_relations(
            sql_body, where=where, name=name, field="sql",
        )
        sql_body = sql_body.strip()
    else:
        # kind='llm_judge'. `sql` is forbidden — the row source is
        # `candidates_sql`. Loud failure on partial fields is the
        # whole point of the proposal: missing sample_cap MUST cause
        # a hard reject, no silent default.
        if "sql" in body:
            raise SubstrateError(
                f"{where}: kind='llm_judge' invariant must not set "
                f"`sql`; the row source is `candidates_sql`",
                data={"where": where, "name": name},
            )
        candidates_sql = body["candidates_sql"]
        if (not isinstance(candidates_sql, str)
                or not candidates_sql.strip()):
            raise SubstrateError(
                f"{where}: candidates_sql must be a non-empty string",
                data={"where": where, "name": name},
            )
        write = sql_contains_writes(candidates_sql)
        if write is not None:
            err = SubstrateError(
                f"{where}: candidates_sql contains write-shaped token "
                f"{write!r}; row sources must be read-only",
                data={
                    "where": where, "name": name,
                    "offending_keyword": write,
                },
                fix_hint=(
                    "candidates_sql is a SELECT that returns "
                    "(candidate_id TEXT, payload JSONB)"
                ),
            )
            err.code = "invariant_body_writes"
            raise err
        _reject_dropped_graph_relations(
            candidates_sql, where=where, name=name, field="candidates_sql",
        )
        candidates_sql = candidates_sql.strip()

        judge_prompt_raw = body["judge_prompt"]
        if (not isinstance(judge_prompt_raw, str)
                or not judge_prompt_raw.strip()):
            raise SubstrateError(
                f"{where}: judge_prompt must be a non-empty path string",
                data={"where": where, "name": name},
            )
        # Resolve relative paths against the YAML's directory so
        # operators can write `judge_prompt: prompts/foo.md` and have
        # the loader find it. yaml_dir is None when validating an
        # in-memory body (tests / direct API).
        judge_prompt_path = Path(judge_prompt_raw)
        if not judge_prompt_path.is_absolute() and yaml_dir is not None:
            judge_prompt_path = yaml_dir / judge_prompt_path
        if yaml_dir is not None and not judge_prompt_path.exists():
            raise SubstrateError(
                f"{where}: judge_prompt {judge_prompt_raw!r} resolves to "
                f"{judge_prompt_path} which does not exist",
                data={
                    "where": where, "name": name,
                    "judge_prompt": str(judge_prompt_path),
                },
                fix_hint=(
                    "create the prompt file or correct the relative path"
                ),
            )
        judge_prompt = str(judge_prompt_path)

        judge_model = body["judge_model"]
        if (not isinstance(judge_model, str)
                or not judge_model.strip()):
            raise SubstrateError(
                f"{where}: judge_model must be a non-empty model string",
                data={"where": where, "name": name},
            )
        judge_model = judge_model.strip()

        sample_cap_raw = body["sample_cap"]
        if (not isinstance(sample_cap_raw, int)
                or isinstance(sample_cap_raw, bool)
                or sample_cap_raw <= 0):
            raise SubstrateError(
                f"{where}: sample_cap must be a positive integer; got "
                f"{sample_cap_raw!r}",
                data={"where": where, "name": name,
                      "sample_cap": sample_cap_raw},
            )
        sample_cap = sample_cap_raw

        fire_on_raw = body.get("fire_on", ["inconsistent"])
        if (not isinstance(fire_on_raw, list)
                or not fire_on_raw
                or not all(isinstance(v, str) for v in fire_on_raw)):
            raise SubstrateError(
                f"{where}: fire_on must be a non-empty list of verdict "
                f"strings; got {fire_on_raw!r}",
                data={"where": where, "name": name,
                      "fire_on": fire_on_raw},
            )
        for v in fire_on_raw:
            if v not in _VERDICT_LITERALS:
                raise SubstrateError(
                    f"{where}: fire_on contains unknown verdict "
                    f"{v!r}; allowed: {list(_VERDICT_LITERALS)}",
                    data={"where": where, "name": name, "verdict": v},
                )
        fire_on_tuple = tuple(fire_on_raw)

        # Spend admission declaration. Both-or-neither, mirroring the
        # extractor lanes: a cap alone cannot reserve (no per-call
        # estimate), an estimate alone bounds nothing. Absent both,
        # the judge lane runs spend-unbounded and the runner reports
        # that — a partial declaration is a config error, not a
        # silently-ignored half-wish.
        cap_present = "judge_spend_cap_usd" in body
        est_present = "judge_spend_estimate_usd" in body
        if cap_present != est_present:
            raise SubstrateError(
                f"{where}: judge_spend_cap_usd and "
                f"judge_spend_estimate_usd must be declared together "
                f"(got only "
                f"{'judge_spend_cap_usd' if cap_present else 'judge_spend_estimate_usd'!s})",
                data={"where": where, "name": name},
                fix_hint=(
                    "declare both (cap bounds the invariant's total "
                    "judge spend; the estimate is reserved per call "
                    "and should be an upper bound), or neither"
                ),
            )
        for money_key in ("judge_spend_cap_usd", "judge_spend_estimate_usd"):
            if money_key in body:
                mv = body[money_key]
                if (not isinstance(mv, (int, float))
                        or isinstance(mv, bool) or float(mv) <= 0):
                    raise SubstrateError(
                        f"{where}: {money_key} must be a positive "
                        f"number; got {mv!r}",
                        data={"where": where, "name": name,
                              money_key: mv},
                    )
        judge_spend_cap = (
            float(body["judge_spend_cap_usd"]) if cap_present else None
        )
        judge_spend_estimate = (
            float(body["judge_spend_estimate_usd"]) if est_present else None
        )

    timeout_ms = body.get("timeout_ms")
    if timeout_ms is not None:
        if not isinstance(timeout_ms, int) or timeout_ms <= 0:
            raise SubstrateError(
                f"{where}: timeout_ms must be a positive integer; "
                f"got {timeout_ms!r}",
                data={"where": where, "timeout_ms": timeout_ms},
            )

    audit_ref = body.get("precision_audit_ref")
    if audit_ref is not None:
        if not isinstance(audit_ref, dict):
            raise SubstrateError(
                f"{where}: precision_audit_ref must be a mapping; "
                f"got {type(audit_ref).__name__}",
                data={"where": where},
            )
        ar_extras = set(audit_ref) - {"doc", "precision", "generation_id"}
        if ar_extras:
            raise SubstrateError(
                f"{where}: precision_audit_ref has unknown keys "
                f"{sorted(ar_extras)}; allowed: doc, precision, "
                f"generation_id",
                data={"where": where, "extras": sorted(ar_extras)},
            )
        for required in ("doc", "precision", "generation_id"):
            if required not in audit_ref:
                raise SubstrateError(
                    f"{where}: precision_audit_ref missing required "
                    f"key {required!r}",
                    data={"where": where, "missing": required},
                )
        if not isinstance(audit_ref["doc"], str) or not audit_ref["doc"]:
            raise SubstrateError(
                f"{where}: precision_audit_ref.doc must be a "
                f"non-empty string",
                data={"where": where},
            )
        prec = audit_ref["precision"]
        if not isinstance(prec, (int, float)) or not (0.0 <= prec <= 1.0):
            raise SubstrateError(
                f"{where}: precision_audit_ref.precision must be a "
                f"float in [0.0, 1.0]; got {prec!r}",
                data={"where": where, "precision": prec},
            )
        gen_id = audit_ref["generation_id"]
        if not isinstance(gen_id, int) or gen_id <= 0:
            raise SubstrateError(
                f"{where}: precision_audit_ref.generation_id must be "
                f"a positive integer; got {gen_id!r}",
                data={"where": where, "generation_id": gen_id},
            )

    # Concept scope (per `add-concept-scoped-invariants`). Optional
    # for both kinds. When set, every required key is enforced
    # loudly (project P0 rule against silent defaults).
    concept_scope_raw = body.get("concept_scope")
    concept_scope_dict: Optional[dict[str, Any]] = None
    if concept_scope_raw is not None:
        if not isinstance(concept_scope_raw, dict):
            raise SubstrateError(
                f"{where}: concept_scope must be a mapping; got "
                f"{type(concept_scope_raw).__name__}",
                data={"where": where, "name": name},
            )
        cs_extras = set(concept_scope_raw) - {
            "concept_name", "min_confidence", "hops", "hop_edges",
        }
        if cs_extras:
            raise SubstrateError(
                f"{where}: concept_scope has unknown keys "
                f"{sorted(cs_extras)}; allowed: concept_name, "
                f"min_confidence, hops, hop_edges",
                data={"where": where, "name": name,
                      "extras": sorted(cs_extras)},
            )
        cn = concept_scope_raw.get("concept_name")
        if not isinstance(cn, str) or not cn.strip():
            raise SubstrateError(
                f"{where}: concept_scope.concept_name must be a "
                f"non-empty string; got {cn!r}",
                data={"where": where, "name": name},
            )
        if "min_confidence" not in concept_scope_raw:
            raise SubstrateError(
                f"{where}: concept_scope is set but min_confidence "
                f"is missing (no silent default; project P0 rule)",
                data={"where": where, "name": name},
                fix_hint=(
                    "add `min_confidence: <float>` to concept_scope; "
                    "typical starting value 0.5"
                ),
            )
        mc = concept_scope_raw["min_confidence"]
        if (not isinstance(mc, (int, float))
                or isinstance(mc, bool)
                or not (-1.0 <= float(mc) <= 1.0)):
            raise SubstrateError(
                f"{where}: concept_scope.min_confidence must be a "
                f"number in [-1, 1]; got {mc!r}",
                data={"where": where, "name": name,
                      "min_confidence": mc},
            )
        hops = concept_scope_raw.get("hops", 1)
        if (not isinstance(hops, int) or isinstance(hops, bool)
                or hops <= 0):
            raise SubstrateError(
                f"{where}: concept_scope.hops must be a positive "
                f"integer; got {hops!r}",
                data={"where": where, "name": name, "hops": hops},
            )
        hop_edges = concept_scope_raw.get("hop_edges", ["mentions"])
        if (not isinstance(hop_edges, list) or not hop_edges
                or not all(isinstance(e, str) and e
                           for e in hop_edges)):
            raise SubstrateError(
                f"{where}: concept_scope.hop_edges must be a "
                f"non-empty list of edge-type strings; got "
                f"{hop_edges!r}",
                data={"where": where, "name": name,
                      "hop_edges": hop_edges},
            )
        concept_scope_dict = {
            "concept_name": cn.strip(),
            "min_confidence": float(mc),
            "hops": int(hops),
            "hop_edges": tuple(hop_edges),
        }

    requires_sources: Optional[tuple[str, ...]] = None
    if "requires_sources" in body and kind != "sql":
        # The judge runner does not apply it, so accepting it would be a
        # declaration nothing honors.
        raise SubstrateError(
            f"{where}: requires_sources applies to sql invariants only; "
            f"got kind={kind!r}",
            data={"where": where, "name": name, "kind": kind},
        )
    if "requires_sources" in body:
        raw_sources = body["requires_sources"]
        if (not isinstance(raw_sources, list) or not raw_sources
                or not all(isinstance(x, str) and x.strip()
                           for x in raw_sources)):
            raise SubstrateError(
                f"{where}: requires_sources must be a non-empty list of "
                f"source names; got {raw_sources!r}",
                data={"where": where, "name": name,
                      "requires_sources": raw_sources},
            )
        cleaned = [x.strip() for x in raw_sources]
        if len(set(cleaned)) != len(cleaned):
            raise SubstrateError(
                f"{where}: requires_sources lists a source twice: "
                f"{cleaned!r}",
                data={"where": where, "name": name,
                      "requires_sources": cleaned},
            )
        requires_sources = tuple(cleaned)

    return InvariantSpec(
        name=name,
        severity=severity,
        scope=scope,
        source=source,
        sql_body=sql_body,
        timeout_ms=timeout_ms,
        precision_audit_ref=dict(audit_ref) if audit_ref else None,
        kind=kind,
        candidates_sql=candidates_sql,
        judge_prompt=judge_prompt,
        judge_model=judge_model,
        sample_cap=sample_cap,
        fire_on=fire_on_tuple,
        judge_spend_cap_usd=judge_spend_cap,
        judge_spend_estimate_usd=judge_spend_estimate,
        concept_scope=concept_scope_dict,
        requires_sources=requires_sources,
    )


def load_invariants(path: str | Path) -> list[InvariantSpec]:
    """Parse `invariants.yaml`, validate each entry, return the
    resolved list.

    Expected shape:
      invariants:
        - name: no_orphan_edges
          severity: error
          scope: cross
          sql: |
            SELECT ...
        - name: every_ticket_has_reporter
          severity: error
          scope: source
          source: jira
          timeout_ms: 2000
          sql: |
            SELECT ...

    Duplicate names raise.
    """
    p = Path(path)
    with p.open("r") as f:
        doc = fastyaml.safe_load(f) or {}

    if not isinstance(doc, dict):
        raise SubstrateError(
            f"invariants {p}: top-level must be a mapping; got "
            f"{type(doc).__name__}",
            data={"path": str(p)},
        )
    extras = set(doc) - {"invariants"}
    if extras:
        raise SubstrateError(
            f"invariants {p}: unknown top-level keys "
            f"{sorted(extras)}; allowed: invariants",
            data={"path": str(p), "extras": sorted(extras)},
        )
    items = doc.get("invariants") or []
    if not isinstance(items, list):
        raise SubstrateError(
            f"invariants {p}: 'invariants' must be a list; got "
            f"{type(items).__name__}",
            data={"path": str(p)},
        )

    yaml_dir = p.parent
    out: list[InvariantSpec] = []
    seen_names: set[str] = set()
    for i, body in enumerate(items):
        spec = _validate_single(
            body, where=f"invariants[{i}]", yaml_dir=yaml_dir,
        )
        if spec.name in seen_names:
            raise SubstrateError(
                f"invariants {p}: duplicate invariant name "
                f"{spec.name!r}",
                data={"path": str(p), "name": spec.name},
            )
        seen_names.add(spec.name)
        out.append(spec)
    return out


def discover_invariant_files(deployment_root: str | Path) -> list[Path]:
    """Every invariant file a deployment ships, in load order.

    A deployment carries invariants in two places: the root
    `invariants.yaml` (a profile's bundle copied verbatim by
    `okg init`, or the scaffold marker `invariants: []`) and the
    `invariants/` directory (per-concern bundles). Catalog
    composition, the wiring resolver, and bootstrap validation all
    read the same list from here so the set the publisher evaluates
    is the set the operator can see. The root file comes first, then
    the directory's `*.yaml` sorted by name.
    """
    root = Path(deployment_root)
    files: list[Path] = []
    root_file = root / "invariants.yaml"
    if root_file.is_file():
        files.append(root_file)
    inv_dir = root / "invariants"
    if inv_dir.is_dir():
        files.extend(sorted(inv_dir.glob("*.yaml")))
    return files


def load_deployment_invariants(
    deployment_root: str | Path,
) -> list[InvariantSpec]:
    """Load and merge every invariant file of a deployment.

    Files come from `discover_invariant_files`. A name that appears
    in more than one file is kept once when the loaded specs are
    identical (the LLM bootstrap writes the same bundle to both the
    root file and `invariants/bootstrap.yaml`); two files that
    disagree about a name raise `SubstrateError` naming both, since
    silently picking one would install a gate the operator did not
    review. Equality is by loaded fields, so an ``llm_judge`` spec
    whose relative ``judge_prompt`` resolves against each file's own
    directory reads as a conflict; keep such a bundle in one place.
    """
    merged: dict[str, tuple[InvariantSpec, Path]] = {}
    for path in discover_invariant_files(deployment_root):
        for spec in load_invariants(path):
            prior = merged.get(spec.name)
            if prior is None:
                merged[spec.name] = (spec, path)
                continue
            prior_spec, prior_path = prior
            if prior_spec == spec:
                continue
            raise SubstrateError(
                f"invariants: {spec.name!r} is declared differently in "
                f"{prior_path} and {path}; keep one definition",
                data={
                    "name": spec.name,
                    "paths": [str(prior_path), str(path)],
                },
            )
    _check_required_sources_registered(deployment_root, merged)
    return [spec for spec, _path in merged.values()]


def _check_required_sources_registered(
    deployment_root: str | Path,
    merged: dict[str, tuple[InvariantSpec, Path]],
) -> None:
    """Refuse a `requires_sources` name the source registry does not know.

    An unknown name would read as "source off" at publish and skip the
    check for good, so a typo must fail here, where the operator sees it.
    A deployment with no `source_registry.yaml` has nothing to check
    against; the gate then cannot tell either, evaluates the invariant as
    usual and lists it under `unchecked_source_requirements`.

    A source counts as off only while its entry stays in the registry with
    `enabled_by_default: false`; removing the entry is refused here.
    """
    wanting = {
        name: (spec, path) for name, (spec, path) in merged.items()
        if spec.requires_sources
    }
    if not wanting:
        return
    registry_path = Path(deployment_root) / "source_registry.yaml"
    if not registry_path.is_file():
        return
    from okg.substrate.sources.registry import load_source_registry

    known = set(load_source_registry(registry_path))
    for name, (spec, path) in sorted(wanting.items()):
        unknown = [s for s in spec.requires_sources or () if s not in known]
        if unknown:
            raise SubstrateError(
                f"invariants: {name!r} in {path} requires sources "
                f"{unknown} that {registry_path} does not register",
                data={
                    "name": name,
                    "path": str(path),
                    "unknown_sources": unknown,
                    "registry": str(registry_path),
                },
                fix_hint=(
                    "use the source names as written in "
                    "source_registry.yaml"
                ),
            )


def install_invariants(
    conn: psycopg.Connection,
    specs: Iterable[InvariantSpec],
    *,
    catalog_version_id: int,
) -> int:
    """Install or replace the invariant rows for a catalog version.

    Within one catalog version, invariant rows are immutable
    (PRIMARY KEY is (name, catalog_version_id)). This function
    does a clean REPLACE by deleting the existing rows at
    `catalog_version_id` and inserting the new set. Runs inside
    one transaction.

    Returns the number of rows inserted.
    """
    specs = list(specs)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.invariants "
                "WHERE catalog_version_id = %s",
                (catalog_version_id,),
            )
            for s in specs:
                # Concept scope is stored as JSONB so the frozen
                # tuple of hop_edges round-trips as a JSON list.
                concept_scope_payload = None
                if s.concept_scope is not None:
                    cs = dict(s.concept_scope)
                    if isinstance(cs.get("hop_edges"), tuple):
                        cs["hop_edges"] = list(cs["hop_edges"])
                    import json as _json
                    concept_scope_payload = _json.dumps(cs)
                cur.execute(
                    """
                    INSERT INTO okg.invariants
                      (name, severity, scope, source, sql_body,
                       timeout_ms, kind,
                       candidates_sql, judge_prompt, judge_model,
                       sample_cap, fire_on,
                       judge_spend_cap_usd, judge_spend_estimate_usd,
                       concept_scope, requires_sources,
                       catalog_version_id)
                    VALUES (%s, %s, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s,
                            %s::jsonb, %s,
                            %s)
                    """,
                    (
                        s.name, s.severity, s.scope, s.source,
                        s.sql_body, s.timeout_ms, s.kind,
                        s.candidates_sql, s.judge_prompt, s.judge_model,
                        s.sample_cap,
                        list(s.fire_on) if s.fire_on else None,
                        s.judge_spend_cap_usd, s.judge_spend_estimate_usd,
                        concept_scope_payload,
                        list(s.requires_sources) if s.requires_sources else None,
                        catalog_version_id,
                    ),
                )
    return len(specs)
