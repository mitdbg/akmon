"""`review_as` substrate MCP primitive.

Per S-A7-R5:

Inputs:
  persona: str — Persona slug (or `persona:<slug>`)
  target_node_id: str — node id critiqued
  request_type: str | None — overrides Persona.default_request_type
  llm_call: Callable[[str, dict], LLMResponse] — caller-supplied
    LLM-completion callable; the substrate uses it to invoke the
    deployment's configured LLM (existing `llm_calls` path).

Side effects:
  - One row in okg.llm_calls (handled by the caller's llm_call
    callable or via record_llm_call passthrough).
  - One Critique NodeFact (deterministic id including the
    mcp_call_log run_id).
  - One Critique -> target_node `critiques` EdgeFact.

Returns:
  ReviewAsResult { critique_id, persona, verdict, severity,
                   text, target_node_id, generation_id }

The actual LLM client is injected so the unit tests can pass a
stub. The MCP server bootstrap wires a real callable that pipes
through the deployment's configured LLM.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.library.personas import (
    PersonaSeed,
    compute_prompt_hash,
    load_seed_personas,
)
from okg.substrate.mcp.framework import (
    PreJoinToolError,
    ResolverRegistry,
    execute_pre_join_tool,
)
from okg.substrate.mcp.framework.schema import PreJoinToolSpec
from okg.substrate.mcp.session import (
    Session,
    assert_session_matches,
)


SOURCE = "substrate.review_as"
_VALID_VERDICTS = frozenset({"approve", "conditional", "reject"})
_VALID_SEVERITIES = frozenset({"info", "minor", "major", "blocker"})


__all__ = [
    "PersonaUnknown",
    "PersonaPromptMissing",
    "ReviewAsResult",
    "build_review_neighborhood_bundle",
    "parse_persona_response",
    "review_as",
]


class PersonaUnknown(SubstrateError):
    """Raised when `review_as(persona=X, ...)` cannot find X in
    the registry. The wire code matches S-A7-R5's contract."""

    code = "persona_unknown"


class PersonaPromptMissing(SubstrateError):
    """Raised when a registered Persona's system_prompt_ref does
    not resolve to a readable file at call time. The wire code
    matches S-A7-R5's contract."""

    code = "persona_prompt_missing"


@dataclass(frozen=True)
class ReviewAsResult:
    critique_id: str
    persona: str
    target_node_id: str
    verdict: str
    severity: str
    text: str
    confidence: Optional[float]
    request_type: str
    generation_id: int


def review_as(
    conn: psycopg.Connection,
    session: Session,
    *,
    persona: str,
    target_node_id: str,
    llm_call: Callable[[str, dict[str, Any]], dict[str, Any]],
    pre_join_tools: dict[str, PreJoinToolSpec],
    resolvers: ResolverRegistry,
    request_type: Optional[str] = None,
    repo_root: Optional[Path] = None,
    observed_at: Optional[datetime] = None,
    call_run_id: Optional[uuid.UUID] = None,
) -> ReviewAsResult:
    """Run ``persona`` against ``target_node_id`` and emit one
    Critique node + critiques edge.

    `llm_call(system_prompt, user_payload) -> { verdict?, severity?,
    text?, confidence? }`. The caller is responsible for whichever
    LLM client is in use; the substrate's MCP server bootstrap wires
    a real callable. Parse failures emit a Critique with
    verdict=conditional, severity=info, text=raw + parse note (per
    S-A7-R5 task 5.6).
    """
    assert_session_matches(conn, session)
    persona_seed = _resolve_persona(persona, repo_root=repo_root)
    prompt_text = _read_prompt(persona_seed.system_prompt_ref, repo_root)

    # Re-hash the prompt; the freshness check (R2) records the
    # hash on the Critique node's source attrs so historical
    # bitemporal queries can recover the prompt content used.
    prompt_hash = compute_prompt_hash(
        _resolve_prompt_path(persona_seed.system_prompt_ref, repo_root)
    )

    # Assemble target neighborhood via the persona's pre-join tool.
    tool_name = persona_seed.pre_join_tool_for_input
    spec = pre_join_tools.get(tool_name)
    if spec is None:
        raise PreJoinToolError(
            f"review_as: persona {persona_seed.slug!r} references "
            f"unregistered pre-join tool {tool_name!r}",
            data={
                "persona": persona_seed.slug,
                "tool": tool_name,
            },
        )
    neighborhood = execute_pre_join_tool(
        spec,
        conn=conn,
        session=session,
        params={"target_node_id": target_node_id},
        resolvers=resolvers,
    )

    effective_request_type = (
        request_type or persona_seed.default_request_type
    )
    user_payload = {
        "target_node_id": target_node_id,
        "request_type": effective_request_type,
        "neighborhood": neighborhood,
    }

    # Issue the LLM call. Parse failures fall through to a
    # well-formed Critique with verdict=conditional so the audit
    # trail still lands (per task 5.6).
    raw_response = llm_call(prompt_text, user_payload)
    parsed, parse_error = parse_persona_response(raw_response)

    when = (observed_at or datetime.now(timezone.utc))
    run_id = call_run_id or uuid.uuid4()
    critique_id = _compute_critique_id(
        persona_slug=persona_seed.slug,
        target_node_id=target_node_id,
        run_id=run_id,
    )

    critique_attrs = {
        "critique_id": critique_id,
        "persona": persona_seed.slug,
        "target_node_ref": target_node_id,
        "request_type": effective_request_type,
        "verdict": parsed["verdict"],
        "severity": parsed["severity"],
        "text": parsed["text"],
        "source_subtype": "review_as",
        "persona_prompt_content_hash": prompt_hash,
        "created_at": when.isoformat(),
        "last_modified_at": when.isoformat(),
    }
    if parsed.get("confidence") is not None:
        critique_attrs["confidence"] = parsed["confidence"]
    if parse_error:
        critique_attrs["response_parse_error"] = parse_error

    _emit_critique(
        conn=conn,
        critique_id=critique_id,
        target_node_id=target_node_id,
        attrs=critique_attrs,
        observed_at=when,
        run_id=run_id,
    )

    return ReviewAsResult(
        critique_id=critique_id,
        persona=persona_seed.slug,
        target_node_id=target_node_id,
        verdict=critique_attrs["verdict"],
        severity=critique_attrs["severity"],
        text=critique_attrs["text"],
        confidence=critique_attrs.get("confidence"),
        request_type=effective_request_type,
        generation_id=session.generation_id,
    )


# ---- response parsing ----


def parse_persona_response(
    raw: Any,
) -> tuple[dict[str, Any], Optional[str]]:
    """Parse the structured response a persona prompt is expected
    to honor: `{ verdict, severity, text, confidence? }`.

    Returns (parsed_dict, parse_error_or_None). On a parse
    failure, the parsed_dict is filled with safe defaults
    (verdict=conditional, severity=info, text=raw-or-str(raw))
    per S-A7-R5 task 5.6 — the audit trail still lands.
    """
    if not isinstance(raw, dict):
        return {
            "verdict": "conditional",
            "severity": "info",
            "text": str(raw) if raw is not None else "",
        }, "response is not a JSON object"

    verdict = raw.get("verdict")
    severity = raw.get("severity")
    text = raw.get("text")
    confidence = raw.get("confidence")

    errors: list[str] = []
    if verdict not in _VALID_VERDICTS:
        errors.append(
            f"verdict {verdict!r} not in "
            f"{sorted(_VALID_VERDICTS)}"
        )
    if severity not in _VALID_SEVERITIES:
        errors.append(
            f"severity {severity!r} not in "
            f"{sorted(_VALID_SEVERITIES)}"
        )
    if not isinstance(text, str) or not text:
        errors.append("text must be a non-empty string")
    if confidence is not None and not isinstance(confidence, (int, float)):
        errors.append("confidence must be a number")

    if errors:
        return {
            "verdict": "conditional",
            "severity": "info",
            "text": (
                str(raw) + "\n\n[response_parse_error: "
                + "; ".join(errors) + "]"
            ),
        }, "; ".join(errors)

    parsed: dict[str, Any] = {
        "verdict": verdict,
        "severity": severity,
        "text": text,
    }
    if confidence is not None:
        parsed["confidence"] = float(confidence)
    return parsed, None


# ---- helpers ----


def build_review_neighborhood_bundle(
    target_node_id: str,
    *,
    conn: psycopg.Connection,
    session: Session,
    pre_join_tools: dict[str, PreJoinToolSpec],
    resolvers: ResolverRegistry,
    tool_name: str,
) -> dict[str, Any]:
    """Run a persona's pre_join_tool_for_input against a target.
    Exposed for tests that want to validate the neighborhood
    bundle without running a real LLM call.
    """
    spec = pre_join_tools.get(tool_name)
    if spec is None:
        raise PreJoinToolError(
            f"build_review_neighborhood_bundle: unregistered "
            f"pre_join tool {tool_name!r}",
            data={"tool": tool_name},
        )
    return execute_pre_join_tool(
        spec,
        conn=conn,
        session=session,
        params={"target_node_id": target_node_id},
        resolvers=resolvers,
    )


def _resolve_persona(
    persona: str, *, repo_root: Optional[Path] = None,
) -> PersonaSeed:
    """Find the persona seed by slug. The substrate's MCP server
    bootstrap normally caches the registry; this fallback re-loads
    the canonical 4 each call. Deployment-registered personas can
    be passed via a caller-side dict in a future extension."""
    slug = persona.split(":", 1)[1] if persona.startswith("persona:") else persona
    for seed in load_seed_personas(repo_root=repo_root):
        if seed.slug == slug:
            return seed
    raise PersonaUnknown(
        f"persona {persona!r} is not in the registry",
        data={"persona": persona, "slug": slug},
        fix_hint="check src/okg/substrate/library/personas/*.yaml "
                 "or the deployment's personas_dir.",
    )


def _resolve_prompt_path(
    system_prompt_ref: str, repo_root: Optional[Path],
) -> Path:
    p = Path(system_prompt_ref)
    if not p.is_absolute():
        if repo_root is None:
            from okg.substrate.library.personas import SEEDS_DIR
            repo_root = SEEDS_DIR.parents[4]
        p = (repo_root / system_prompt_ref).resolve()
    return p


def _read_prompt(
    system_prompt_ref: str, repo_root: Optional[Path],
) -> str:
    p = _resolve_prompt_path(system_prompt_ref, repo_root)
    if not p.is_file():
        raise PersonaPromptMissing(
            f"persona prompt at {p} does not exist",
            data={"path": str(p)},
        )
    return p.read_text(encoding="utf-8")


def _compute_critique_id(
    *,
    persona_slug: str,
    target_node_id: str,
    run_id: uuid.UUID,
) -> str:
    """Per D8: identity is persona_slug + target_short_hash +
    run_id. Re-reviews emit distinct Critiques (one row per call);
    in-call retries collapse via wire-level dedup.
    """
    short = hashlib.sha256(target_node_id.encode("utf-8")).hexdigest()[:12]
    return f"critique:{persona_slug}:{short}:{run_id.hex[:8]}"


def _emit_critique(
    *,
    conn: psycopg.Connection,
    critique_id: str,
    target_node_id: str,
    attrs: dict[str, Any],
    observed_at: datetime,
    run_id: uuid.UUID,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.images.values import check_incoming
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    # Image values (#3046): critique attrs carry model output; refuse a
    # forged or malformed image value before anything is written.
    check_incoming(attrs)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source,
               source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (%s, 'critique', %s::jsonb, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, 'I')
            ON CONFLICT (dedupe_key, observed_at) DO NOTHING
            """,
            (
                critique_id,
                json.dumps(attrs, sort_keys=True, default=str),
                SOURCE,
                json.dumps({
                    "critique_id": critique_id,
                    "target_node_id": target_node_id,
                }),
                json.dumps({"observed_at": observed_at.isoformat()}),
                observed_at,
                f"{SOURCE}|node|{critique_id}",
                str(run_id),
            ),
        )
        edge_id = _mint_edge_id(
            src=critique_id, dst=target_node_id, edge_type="critiques",
        )
        cur.execute(
            """
            INSERT INTO okg.edge_facts
              (edge_id, src, dst, edge_type, attrs, provenance,
               confidence, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s, 'critiques', '{}'::jsonb,
                    'extracted_model', 1.0, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, 'I')
            ON CONFLICT (dedupe_key, observed_at) DO NOTHING
            """,
            (
                edge_id, critique_id, target_node_id,
                SOURCE,
                json.dumps({
                    "src": critique_id, "dst": target_node_id,
                    "edge_type": "critiques",
                }),
                json.dumps({"observed_at": observed_at.isoformat()}),
                observed_at,
                f"{SOURCE}|edge|{critique_id}|critiques|{target_node_id}",
                str(run_id),
            ),
        )

    ChronosGraphStore.from_connection(conn).apply_facts([
        NodeFact(
            critique_id,
            "critique",
            attrs,
            {
                "critique_id": critique_id,
                "target_node_id": target_node_id,
            },
            {"observed_at": observed_at.isoformat()},
        ),
        EdgeFact(
            critique_id,
            target_node_id,
            "critiques",
            provenance="extracted_model",
            source_record_id={
                "src": critique_id,
                "dst": target_node_id,
                "edge_type": "critiques",
            },
            source_revision={"observed_at": observed_at.isoformat()},
        ),
    ])


def _mint_edge_id(*, src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)
