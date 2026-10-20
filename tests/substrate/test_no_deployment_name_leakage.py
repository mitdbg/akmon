"""Substrate must not branch on deployment names at runtime.

Per `deployment-qa` capability (proposal `qa-stabilize-main`):
deployment-specific behavior MUST flow through pluggability surfaces
(source registry, enricher registry, view registry, invariant kind,
alias backend, embedder), never an `if deployment == "af_pubs":`
branch.

The disease this test exists to prevent is **runtime branching on a
hardcoded deployment name** — an `if` / `elif` / ternary / `match` / `==`
that compares a runtime value to a literal deployment name in order to
select deployment-specific behavior. A mere *mention* of a deployment
name is not the disease: command examples (`okg ... --deployment
okg-workspace`), `.env.example`, demo/federation URLs
(`https://example.invalid/`), microbench shell snippets, metric/check-id
labels (`doctor.okg_workspace`), error/help text, docstrings/comments,
and declared data are all benign — they carry no control flow. (The fleet
lists that used to be package constants are now repository configuration,
`deployment_fleet` in .okg/coordination.yaml; see
tests/substrate/test_deployment_fleet_config.py.)

So this test walks `src/okg/substrate/**/*.py` with the `ast` module and
flags a deployment-name literal only when it is used as a **branch
condition**:

  (a) a comparison operand — `... == "af_pubs"`, `"af_pubs" != ...`,
      `deployment in ("af_pubs", ...)`, `deployment is "af_pubs"`, and
      their negations. The literal must equal a deployment name exactly
      (so a URL/path/command string that merely *contains* the name is
      not a comparison against it).
  (b) a `match` / `case` pattern value — `case "af_pubs":`.

Everything else — list/tuple/dict elements, call arguments, keyword
values, returns, assignments, f-string parts, docstrings, comments — is
allowed, because none of those branch on the name. The test fails iff
there is at least one class-(a)/(b) hit.

To prove the guard still bites, temporarily add an
`if deployment_name == "okg-workspace":` branch to any scanned module and
re-run: this test fails on it.
"""
from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

DEPLOYMENT_NAMES = (
    "af_pubs",
    "okg-workspace",
    "okg_workspace",
    "cms",
    "memex",
    "intersystems",
    "example",
)
DEPLOYMENT_NAME_SET = frozenset(DEPLOYMENT_NAMES)

REPO_ROOT = Path(__file__).resolve().parents[2]
SUBSTRATE_ROOT = REPO_ROOT / "src" / "okg" / "substrate"
TEMPLATE_ROOT = SUBSTRATE_ROOT / "library" / "templates"

# Comparison operators that express a branch on equality/membership with a
# deployment-name literal. Ordering / identity-with-non-literal operators
# (`<`, `>`, etc.) never appear against a name literal, so equality and
# membership are the meaningful set.
_BRANCHING_CMP_OPS = (ast.Eq, ast.NotEq, ast.Is, ast.IsNot, ast.In, ast.NotIn)


def _is_deployment_name_literal(node: ast.AST) -> bool:
    """True iff ``node`` is a str Constant equal to a deployment name."""
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value in DEPLOYMENT_NAME_SET
    )


def _iter_comparison_leaks(tree: ast.AST):
    """Yield (lineno, name) for deployment literals used as a branch.

    A leak is a deployment-name literal that is a direct operand of an
    equality/membership comparison, or the value of a ``match``/``case``
    pattern. Those are the shapes that branch on deployment identity.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            if not any(
                isinstance(op, _BRANCHING_CMP_OPS) for op in node.ops
            ):
                continue
            for operand in (node.left, *node.comparators):
                # A bare literal operand: `deployment == "af_pubs"`.
                if _is_deployment_name_literal(operand):
                    yield operand.lineno, operand.value
                # A membership operand collection:
                # `deployment in ("af_pubs", "cms")`. The names live in
                # the tuple/list/set elements, not as a direct operand.
                elif isinstance(operand, (ast.Tuple, ast.List, ast.Set)):
                    for elt in operand.elts:
                        if _is_deployment_name_literal(elt):
                            yield elt.lineno, elt.value
        elif isinstance(node, ast.MatchValue):
            if _is_deployment_name_literal(node.value):
                yield node.value.lineno, node.value.value


def test_substrate_has_no_deployment_name_runtime_branches() -> None:
    leaks: list[tuple[Path, int, str]] = []
    counts = Counter()

    for path in sorted(SUBSTRATE_ROOT.rglob("*.py")):
        if TEMPLATE_ROOT in path.parents:
            continue
        try:
            source = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        # Cheap pre-filter: skip files that cannot contain any name.
        if not any(name in source for name in DEPLOYMENT_NAMES):
            continue
        tree = ast.parse(source, filename=str(path))
        for lineno, name in _iter_comparison_leaks(tree):
            counts["leak"] += 1
            rel = path.relative_to(REPO_ROOT)
            leaks.append((rel, lineno, name))

    # Always print the count so a green run still shows the guard ran.
    print(
        f"\nLeakage scan: deployment-name runtime branches={counts['leak']} "
        f"(scanned {SUBSTRATE_ROOT})"
    )

    if leaks:
        lines = [
            f"  {p}:{ln}  branches on deployment name {name!r}"
            for p, ln, name in leaks
        ]
        raise AssertionError(
            "Substrate runtime branch on a deployment name (forbidden "
            "by deployment-qa capability — route deployment-specific "
            "behavior through a pluggability surface, not an "
            "`if deployment == \"...\":` branch):\n" + "\n".join(lines)
        )
