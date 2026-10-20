# okg + pact agent skills bundle

The usage-grounded essential set of agent skills for working with the OKG and
PACT (per the `refine-agent-skills` PACT). This directory is the **source of
truth**; `okg pact install` ships it into a coding agent's skill location.

## The skills (full agent ↔ OKG lifecycle)

| Skill | Lifecycle mode | What it does |
|---|---|---|
| `agent-workflow` | operate agents | coordinate PACT-scoped work, report status, handle handoffs, and roll up subagents |
| `pact` | manage changes | author → check → prove → complete a PACT (the full loop) |
| `okg-query` | read | query the graph for durable context (search / trace / psql / MCP operators) |
| `okg-provision` | provision / health | stand up + verify a graph (provision → preflight → publish → integrity → metrics) |
| `okg-add-source` | populate | wire a data source (`okg deployment add-source` preview → `--apply`, or discover → propose → commit) |
| `okg-record` | write-back | record findings/observations back (the `record` / `propose` operators) |
| `okg-troubleshoot` | diagnose | diagnose a blocked / silent-failure publish |
| `okg-ontology-design` | extend schema | add a subtype/edge/narrowing + recompose the catalog |

`tests/pact/test_agent_skills_bundle.py` pins this set exactly. A new skill
must be an actual agent entry point, not a doc-only shortcut or speculative
surface.

## Skill-location convention

- **Bundle (here):** `src/okg/substrate/pact/skills/<skill>/SKILL.md` — tracked,
  the install source. Each is a self-contained Claude Code skill (frontmatter
  `name` + `description`) composing existing okg verbs / AKMON operators; no
  invented tools, and never the non-existent `okg query` CLI.
- **`okg pact install --agent claude-code`** → copies the bundle to
  `.claude/skills/<skill>/SKILL.md` + a marker-delimited section in `CLAUDE.md`.
  This repository tracks its installed copy, which must match the bundle byte
  for byte (`tests/pact/test_agent_surface_consistency.py`); edit the bundle,
  never the copy.
- **`okg pact install --agent codex`** → copies the bundle to
  `.agents/skills/<skill>/SKILL.md` (the portable, cross-tool convention) + a
  marker-delimited section in `AGENTS.md`.
- **Repo-local skills** specific to working in THIS repo (`design-invariants`,
  `report-okg-bug`) are hand-maintained and tracked under `.agents/skills/`
  (single source; no byte-identical copies elsewhere).

The install is a thin placer — it copies files + a stub; the deep, tailored
setup is driven by an agent running the skills.
