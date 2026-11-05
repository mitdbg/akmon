"""Run one proposal end to end on an experiment branch (issue #4282).

1. Build the proposer's context from a diagnosis (``okg.optimizer.diagnose``
   label and waste records) and the served surface.
2. Run the model loop (``loop.propose``) until one change is accepted.
3. Write the change into the deployment on a git branch named after the
   experiment branch, and commit it (publish refuses uncommitted inputs).
4. ``okg branch create <branch> --role experiment``; for a ``link`` change,
   ``okg catalog load --apply`` and ``okg catalog ownership claim --force``
   (the narrowing is catalog content).
5. ``okg run --once --apply --inline --branch <branch>`` from the deployment
   root (rule paths are relative to it).
6. Check that the publish did something: a ``link`` change that added no
   relationship is recorded ``no_effect``, never as a success.
7. Return the deployment to its base commit (and, after a ``link`` change,
   its base catalog), so the next trial starts from the same place.
8. Write ``proposal.json``: the change, files, diff digest, branch,
   generation, publish outcome, model calls and dollars.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from .apply import Proposal, apply
from .loop import Context

SCHEMA = "okg.akb.proposal_run.v1"


def okg_bin() -> str:
    return str(Path(sys.executable).with_name("okg"))


def _run(argv: Sequence[str], *, cwd: Path, env: Mapping[str, str], timeout: float = 7200) -> tuple[int, str]:
    proc = subprocess.run(list(argv), cwd=str(cwd), env=dict(env), capture_output=True, text=True,  # noqa: S603
                          timeout=timeout, check=False)
    return proc.returncode, (proc.stdout or "")  # the JSON report is on stdout; logs go to stderr


def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, check=True,  # noqa: S603,S607
                          timeout=120).stdout.strip()


def _with_password(dsn: str, env: Mapping[str, str]) -> str:
    """The DSN with the lane copy's password from ``env`` (PGPASSWORD) folded in."""
    from okg.substrate.db import dsn as dsn_module

    password = env.get("PGPASSWORD")
    return dsn_module.parse(dsn).with_params(password=password).render() if password else dsn


# ── context ────────────────────────────────────────────────────────────


def _jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def build_context(root: Path, *, labels: Path | None, waste: Path | None, surface: Mapping[str, Any] | None,
                  objectives: Path | None = None, steer_classes: Iterable[str] = ("c", "f", "g"),
                  max_rows: int = 25) -> Context:
    """The proposer's reading: objectives, diagnosis counts, steering and hint rows, waste, served text,
    readable files; and the diagnosis ids a proposal may target."""
    rows = [r for r in _jsonl(labels) if r.get("status") == "labelled"]
    steer = set(steer_classes)
    steering = [r for r in rows if r["confidence"] == "high" and r["cls"] in steer]
    hints = [r for r in rows if r not in steering]

    def mid(r: Mapping[str, Any]) -> str:
        return f"{r['question_id']}/{r['try']}/{r['fact_id']}"

    def row(r: Mapping[str, Any]) -> str:
        return (f"- id {mid(r)} class {r['cls']} stage {r['stage']} confidence {r['confidence']} "
                f"call {r.get('call')} quote {json.dumps(r.get('quote', ''))[:220]} note {r.get('note', '')[:200]}")

    lines: list[str] = []
    if objectives is not None and Path(objectives).is_file():
        lines += ["# Objectives (what this deployment is optimized for)",
                  Path(objectives).read_text(encoding="utf-8")[:6_000], ""]
    lines += ["# Diagnosis of missed facts",
              f"labelled misses: {len(rows)}; by class {dict(Counter(r['cls'] for r in rows))}; "
              f"by stage {dict(Counter(r['stage'] for r in rows))}",
              f"steering rows (high confidence, class in {sorted(steer)}): {len(steering)}; hints: {len(hints)}", "",
              "## Steering rows"]
    lines += [row(r) for r in steering[:max_rows]]
    lines += ["", "## Hint rows (lower confidence or classes a, b, d, e: weigh them less)"]
    lines += [row(r) for r in hints[:max_rows]]
    targets = {mid(r) for r in rows}
    wrows = _jsonl(waste)
    if wrows:
        kinds: Counter[str] = Counter()
        for w in wrows:
            kinds.update(w.get("by_kind") or {})
        lines += ["", "# Wasted calls (by rule; target a wasted call by its question/try id)",
                  f"tries {len(wrows)}; calls {sum(w['calls'] for w in wrows)}; "
                  f"wasted {sum(w['wasted_calls'] for w in wrows)}; by kind {dict(kinds)}"]
        shown = 0
        for w in wrows:
            if w.get("items"):
                targets.add(f"{w['question_id']}/{w['try']}")
            for item in w.get("items") or ():
                if item["kind"] in ("tool_error", "abandoned_refusal", "repeated_search", "timeout") and shown < max_rows:
                    lines.append(f"- {w['question_id']}/{w['try']} {item['kind']} call {item['call']}: "
                                 f"{item['evidence'][:200]}")
                    shown += 1
    if surface:
        lines += ["", "# Served instructions (head)", str(surface.get("instructions", ""))[:6_000], "",
                  "# Served tools"]
        for tool in surface.get("tools") or ():
            props = sorted(((tool.get("input_schema") or {}).get("properties") or {}))
            lines.append(f"## {tool['name']} (arguments: {', '.join(props)})\n{str(tool.get('description', ''))[:1_500]}")
    readable = {"deployment.yaml": root / "deployment.yaml"}
    for path in sorted((root / "schemas").rglob("*.yaml")):
        readable[str(path.relative_to(root))] = path
    lines += ["", "# Files you may read", *sorted(readable)]
    return Context(summary="\n".join(lines), readable=readable, target_ids=frozenset(targets), surface=surface)


def _graph(dsn: str, env: Mapping[str, str], branch: str) -> tuple[Any, Any]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore

    conn = db_connect(_with_password(dsn, env), autocommit=True, connect_timeout=20, application_name="okg-optimizer-propose")
    conn.execute("SET statement_timeout = '120s'")
    return conn, ChronosGraphStore.from_connection(conn).checkout(branch)


def _rows(result: Any) -> list[Any]:
    rows = getattr(result, "rows", result)
    return [tuple(r.values()) if isinstance(r, Mapping) else tuple(r) for r in rows]


def graph_facts(dsn: str, env: Mapping[str, str], *, branch: str = "default", types: int = 60,
                fields: int = 25, sample: int = 300) -> str:
    """The record types that hold data on ``branch``, their counts and their most common fields."""
    conn, graph = _graph(dsn, env, branch)
    try:
        counts = _rows(graph.query(
            "SELECT subtype, count(*) FROM okg.graph_nodes GROUP BY subtype ORDER BY 2 DESC LIMIT :n", {"n": types}))
        lines = ["# Record types with data (type id as stored: count; common fields)"]
        for subtype, n in counts:
            keys = _rows(graph.query(
                "SELECT k, count(*) FROM (SELECT attrs FROM okg.graph_nodes WHERE subtype = :s LIMIT :m) t, "
                "jsonb_object_keys(t.attrs) k GROUP BY k ORDER BY 2 DESC LIMIT :f", {"s": subtype, "m": sample,
                                                                                     "f": fields}))
            lines.append(f"- {subtype}: {n}; fields {', '.join(str(k) for k, _ in keys)}")
        return "\n".join(lines)
    finally:
        conn.close()


def link_check_for(dsn: str, env: Mapping[str, str], *, branch: str = "default") -> Any:
    """A check that refuses a cross-link rule which would link nothing in this graph.

    It counts records of each endpoint type that carry the endpoint field
    and, for the ``exact`` matcher, the pairs of different records whose
    values are equal. Zero records, or zero pairs, refuses with the counts.
    """

    def check(rule: Mapping[str, Any]) -> str | None:
        src, dst = rule.get("src") or {}, rule.get("dst") or {}
        s_type, s_field = src.get("subtype"), src.get("field")
        d_type, d_field = dst.get("subtype"), dst.get("field")
        if not all(isinstance(x, str) and x for x in (s_type, s_field, d_type, d_field)):
            return "the data check needs src and dst with one subtype and one field each"
        conn, graph = _graph(dsn, env, branch)
        try:
            def count(subtype: str, field: str) -> int:
                return int(_rows(graph.query(
                    "SELECT count(*) FROM okg.graph_nodes WHERE subtype = :s AND attrs ? :f",
                    {"s": subtype, "f": field}))[0][0])

            n_src, n_dst = count(s_type, s_field), count(d_type, d_field)
            if not n_src or not n_dst:
                return (f"the rule would link nothing: {n_src} {s_type} records carry {s_field!r}, "
                        f"{n_dst} {d_type} records carry {d_field!r}; pick types and fields from the data list")
            if rule.get("matcher") == "exact":
                # EXISTS, not a count: one common value shared by many records would make a
                # count of every pair run past the statement timeout.
                pairs = bool(_rows(graph.query(
                    "SELECT EXISTS (SELECT 1 FROM okg.graph_nodes a JOIN okg.graph_nodes b "
                    "ON a.attrs ->> :sf = b.attrs ->> :df AND a.node_id <> b.node_id "
                    "WHERE a.subtype = :s AND b.subtype = :d)", {"s": s_type, "sf": s_field, "d": d_type,
                                                                  "df": d_field}))[0][0])
                if not pairs:
                    return (f"the rule would link nothing: {n_src} {s_type}.{s_field} and {n_dst} "
                            f"{d_type}.{d_field} values exist but no two different records share a value")
            return None
        except Exception as exc:  # noqa: BLE001 - the model gets the reason, the run goes on
            return f"the data check failed: {type(exc).__name__}: {str(exc)[:200]}"
        finally:
            conn.close()

    return check


async def read_surface(run_file: Path, log_dir: Path) -> dict[str, Any]:
    """Start the run file's server once and read its instructions and tools ($0, no model call)."""
    from okg.optimizer.replay.config import load_config
    from okg.optimizer.replay.serve import mcp_connect, probe_surface, start_server

    config = load_config(Path(run_file))
    async with start_server(config, Path(log_dir) / "surface.log", mcp_connect) as handle:
        surface = await probe_surface(mcp_connect, handle.url, handle.headers)
    return {"instructions": surface.instructions,
            "tools": [{"name": t.name, "description": t.description, "input_schema": t.input_schema}
                      for t in surface.tools]}


# ── apply, publish, restore ────────────────────────────────────────────


def catalog_state(dsn: str, deployment: str, env: Mapping[str, str]) -> dict[str, Any] | None:
    """The newest applied catalog version of ``deployment``: id and fingerprints."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(_with_password(dsn, env), connect_timeout=20, application_name="okg-optimizer-propose") as conn:
        row = conn.execute(
            "SELECT catalog_version_id, catalog_fingerprint, schema_fingerprint FROM okg.catalog_versions "
            "WHERE deployment_name = %s AND status = 'applied' ORDER BY catalog_version_id DESC LIMIT 1",
            (deployment,),
        ).fetchone()
    if row is None:
        return None
    return {"catalog_version_id": int(row[0]), "catalog_fingerprint": row[1], "schema_fingerprint": row[2]}


#: Paths a trial may create in the deployment (cleaned on restore).
OPTIMIZER_PATHS = ("surface_text.yaml", "optimizer_rules", "schemas/bridges/optimizer_*")

_OWNERSHIP_FIELDS = ("owner_fingerprint", "schema_fingerprint", "catalog_fingerprint", "deployment_root_hash",
                     "manifest_fingerprint", "status")


def ownership_state(dsn: str, deployment: str, env: Mapping[str, str]) -> dict[str, Any] | None:
    """The deployment's write-ownership row (the fields a restore must bring back)."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(_with_password(dsn, env), connect_timeout=20, application_name="okg-optimizer-propose") as conn:
        row = conn.execute(
            f"SELECT {', '.join(_OWNERSHIP_FIELDS)} FROM okg.deployment_write_ownership WHERE deployment_name = %s",
            (deployment,),
        ).fetchone()
    return None if row is None else dict(zip(_OWNERSHIP_FIELDS, row))


def rule_edges(dsn: str, env: Mapping[str, str], branch: str, rule: Mapping[str, Any]) -> int:
    """Edges of the rule's type between its two endpoint types, on ``branch``."""
    conn, graph = _graph(dsn, env, branch)
    try:
        return int(_rows(graph.query(
            "SELECT count(*) FROM okg.graph_edges e JOIN okg.graph_nodes s ON s.node_id = e.src "
            "JOIN okg.graph_nodes d ON d.node_id = e.dst "
            "WHERE e.edge_type = :t AND ((s.subtype = :a AND d.subtype = :b) OR (s.subtype = :b AND d.subtype = :a))",
            {"t": rule["edge"], "a": rule["src"]["subtype"], "b": rule["dst"]["subtype"]}))[0][0])
    finally:
        conn.close()


def _runtime_enabled(root: Path) -> bool:
    from okg.substrate import fastyaml

    raw = fastyaml.safe_load((root / "deployment.yaml").read_text(encoding="utf-8")) or {}
    runtime = raw.get("runtime") if isinstance(raw, dict) else None
    return bool(isinstance(runtime, dict) and runtime.get("enabled"))


def publish_trial(root: Path, proposal: Proposal, *, deployment: str, dsn: str, env: Mapping[str, str],
                  answer_key: Sequence[str] = (), surface: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Steps 3-7 for an accepted proposal; returns the record's publish part.

    Lane copies only: the trial changes deployment-wide state (the working
    tree, and for a link change the catalog and write ownership) for its
    duration. It refuses a deployment whose runtime worker is enabled, and
    holds an exclusive lock file in the deployment root, so two trials never
    overlap.
    """
    import fcntl

    root = Path(root)
    if _runtime_enabled(root):
        raise RuntimeError(f"{root}: runtime.enabled is true; trials run only on a lane copy with no worker")
    if _git(root, "--no-optional-locks", "status", "--porcelain"):
        raise RuntimeError(f"{root} has uncommitted changes; a trial starts from a clean base")
    lock = (root / ".git" / "okg-optimizer.lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        lock.close()
        raise RuntimeError(f"{root}: another optimizer trial holds the lock") from exc
    base = _git(root, "rev-parse", "HEAD")
    ref = subprocess.run(["git", "-C", str(root), "symbolic-ref", "-q", "--short", "HEAD"],  # noqa: S603,S607
                         capture_output=True, text=True, check=False).stdout.strip()
    out: dict[str, Any] = {"base_commit": base, "base_ref": ref or None, "branch": proposal.branch}
    if proposal.kind == "link":
        out["catalog_before"] = catalog_state(dsn, deployment, env)
        out["ownership_before"] = ownership_state(dsn, deployment, env)
    try:
        _git(root, "checkout", "-q", "-b", f"exp/{proposal.branch}")
        files = apply(root, proposal, answer_key=answer_key, surface=surface)
        _git(root, "add", "-A")
        _git(root, "-c", "user.name=okg optimizer", "-c", "user.email=optimizer@okg.invalid",
             "commit", "-q", "-m", f"optimizer proposal for {proposal.branch} ({proposal.kind})")
        out["files"] = files
        out["commit"] = _git(root, "rev-parse", "HEAD")
        if proposal.kind == "text":
            # The layer is read from the deployment root at server start: serving this branch's
            # text needs this commit checked out (#4283 does that for the trial replay).
            out["serve_commit"] = out["commit"]
        out["diff_digest"] = "sha256:" + hashlib.sha256(_git(root, "diff", base, "HEAD").encode()).hexdigest()
        okg = okg_bin()
        steps: list[dict[str, Any]] = []
        out["steps"] = steps

        def step(name: str, *argv: str) -> int:
            rc, stdout, _err = _run3([okg, *argv], cwd=root, env=env)
            steps.append({"step": name, "rc": rc})
            return rc

        if step("branch_create", "--json", "branch", "create", proposal.branch, "--role", "experiment",
                "--deployment", deployment, "--dsn", dsn) != 0:
            out["outcome"] = "branch_create_failed"
            return out
        if proposal.kind == "link":
            # The same count on the base branch first: a rule may re-derive edges that already exist,
            # and only the new ones are its effect.
            out["rule_edges_base"] = rule_edges(dsn, env, "default", proposal.change["rule"])
            if step("catalog_load", "catalog", "load", "--deployment", deployment, "--dsn", dsn, "--apply") != 0 or \
                    step("ownership_claim", "catalog", "ownership", "claim", "--deployment", deployment,
                         "--dsn", dsn, "--force", "--json") != 0:
                out["outcome"] = "catalog_failed"
                return out
        rc, stdout, _err = _run3([okg, "--json", "run", "--once", "--apply", "--inline", "--branch", proposal.branch,
                                  "--deployment", deployment, "--dsn", dsn], cwd=root, env=env)
        steps.append({"step": "publish", "rc": rc})
        report = _last_json(stdout)
        out["generation_id"] = (report or {}).get("generation_id")
        out["status"] = (report or {}).get("status")
        out["edges_added_cycle"] = _first_int(report, ("edges_added",))
        failures = (report or {}).get("enricher_failures")
        out["enricher_failures"] = len(failures) if isinstance(failures, (list, dict)) else (failures or 0)
        if rc != 0 or out["status"] != "published":
            out["outcome"] = "publish_failed" if rc != 3 else "publish_blocked"
        elif out["enricher_failures"]:
            out["outcome"] = "enricher_failed"
        elif proposal.kind == "link":
            # Counted by the rule's own edge type between its endpoint types, not the cycle total.
            out["rule_edges"] = rule_edges(dsn, env, proposal.branch, proposal.change["rule"])
            out["rule_edges_new"] = out["rule_edges"] - out["rule_edges_base"]
            out["outcome"] = "published" if out["rule_edges_new"] > 0 else "no_effect"
        else:
            out["outcome"] = "published"
        return out
    except Exception as exc:  # noqa: BLE001 - recorded, and the restore below still runs
        out["outcome"] = "trial_error"
        out["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
        return out
    finally:
        try:
            _git(root, "reset", "-q", "--hard")
            # Only the paths the optimizer writes (the base was verified clean, so any
            # untracked file there came from this trial, also from an apply that failed midway).
            _git(root, "clean", "-fdq", "--", *OPTIMIZER_PATHS)
            _git(root, "checkout", "-q", ref or base)
            out["restored_tree"] = _git(root, "rev-parse", "HEAD") == base and not _git(root, "--no-optional-locks", "status", "--porcelain")
        except Exception as exc:  # noqa: BLE001
            out["restored_tree"] = False
            out["restore_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        if proposal.kind == "link":
            try:
                rc, _o, _e = _run3([okg_bin(), "catalog", "load", "--deployment", deployment, "--dsn", dsn,
                                    "--apply"], cwd=root, env=env)
                rc2, _o, _e = _run3([okg_bin(), "catalog", "ownership", "claim", "--deployment", deployment,
                                     "--dsn", dsn, "--force", "--json"], cwd=root, env=env)
                out["restored_catalog"] = rc == 0 and rc2 == 0
                after, owner_after = catalog_state(dsn, deployment, env), ownership_state(dsn, deployment, env)
                before, owner_before = out.get("catalog_before"), out.get("ownership_before")
                out["catalog_after"], out["ownership_after"] = after, owner_after
                # Verified only when the newest applied catalog AND the write-ownership row are the
                # base ones again, and both restore commands succeeded.
                out["catalog_restored_verified"] = bool(
                    out["restored_catalog"] and before and after and owner_before and owner_after
                    and after["catalog_fingerprint"] == before["catalog_fingerprint"]
                    and after["schema_fingerprint"] == before["schema_fingerprint"]
                    and owner_after == owner_before)
            except Exception as exc:  # noqa: BLE001
                out["restored_catalog"] = False
                out["catalog_restored_verified"] = False
                out["catalog_restore_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


def _run3(argv: Sequence[str], *, cwd: Path, env: Mapping[str, str], timeout: float = 7200) -> tuple[int, str, str]:
    """(exit code, stdout, stderr): the JSON report is read from stdout only."""
    rc, text = _run(argv, cwd=cwd, env=env, timeout=timeout)
    if isinstance(text, tuple):  # tests may return (stdout, stderr)
        return rc, text[0], text[1]
    return rc, text, ""


def _last_json(text: str) -> dict[str, Any] | None:
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict):
                return value
    return None


def _first_int(report: Mapping[str, Any] | None, keys: Sequence[str]) -> int | None:
    if not report:
        return None
    stack: list[Any] = [report]
    while stack:
        value = stack.pop()
        if isinstance(value, Mapping):
            for key in keys:
                if isinstance(value.get(key), int):
                    return int(value[key])
            stack.extend(value.values())
    return None


def proposal_record(result: Mapping[str, Any], publish: Mapping[str, Any] | None) -> dict[str, Any]:
    proposal = result.get("proposal")
    return {
        "schema": SCHEMA,
        "status": result["status"],
        "proposal": None if proposal is None else {
            "kind": proposal.kind, "branch": proposal.branch, "change": proposal.change, "stage": proposal.stage,
            "purpose": proposal.purpose, "targets": list(proposal.targets),
            "expected_effect": proposal.expected_effect, "rationale": proposal.rationale},
        "model": {"turns": result.get("turns"), "usd": result.get("usd"), "refusals": result.get("refusals", []),
                  **({"error": result["error"]} if result.get("error") else {})},
        "publish": dict(publish) if publish is not None else None,
    }


def server_env(extra: Mapping[str, str]) -> dict[str, str]:
    env = dict(os.environ)
    env.update(extra)
    env.pop("OPENAI_API_KEY", None)  # the publisher never needs it
    return env


__all__ = ["SCHEMA", "build_context", "okg_bin", "proposal_record", "publish_trial", "read_surface", "server_env"]
