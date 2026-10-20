"""Chronos read-surface audit for shipped profile SQL and Python readers."""
from __future__ import annotations

from pathlib import Path

from tests.mirror_scope import is_public_copy
from tests.private_deployments import names


REPO_ROOT = Path(__file__).resolve().parents[2]
AUDIT_ROOTS = (
    REPO_ROOT / "src/okg/substrate/library/profiles",
    (
        REPO_ROOT
        / "src/okg/substrate/library/ontologies/reasoning"
        / "derived_artifacts/queries"
    ),
)
AUDIT_SUFFIXES = {".sql", ".yaml", ".yml"}
FORBIDDEN_TOKENS = (
    "okg.nodes_live",
    "okg.edges_live",
    "valid_to_gen",
    ".parent_id",
    ".child_id",
    ".edge_archetype",
)

# Python readers audited for the dead pre-chronos graph tables only.
# The catalog-column tokens above (".parent_id", ".edge_archetype", ...)
# appear legitimately in Python (dataclass attributes, CTE aliases), so
# the Python sweep refuses just the dropped table/column names.
PY_AUDIT_ROOTS = (
    REPO_ROOT / "src/okg/viz",
    REPO_ROOT / "src/okg/bench",
    REPO_ROOT / "experiments/beaver/okg",
    *(REPO_ROOT / "deployments" / name for name in names("retired_prototype")),
)
PY_FORBIDDEN_TOKENS = (
    "okg.nodes_live",
    "okg.edges_live",
    "valid_to_gen",
    "valid_from_gen",
)


def _audit_files(roots, suffixes) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        # A missing root would silently shrink coverage to zero files —
        # the audit must fail loud if a swept tree is moved or renamed.
        assert root.is_dir(), f"audit root vanished: {root}"
        files.extend(
            path
            for path in root.rglob("*")
            if path.is_file() and path.suffix in suffixes
        )
    return sorted(files)


def _offenders(roots, suffixes, tokens) -> list[str]:
    offenders: list[str] = []
    for path in _audit_files(roots, suffixes):
        text = path.read_text()
        for token in tokens:
            if token in text:
                rel = path.relative_to(REPO_ROOT)
                offenders.append(f"{rel}: contains {token!r}")
    return offenders


def test_shipped_profile_sql_uses_chronos_view_surfaces() -> None:
    offenders = _offenders(AUDIT_ROOTS, AUDIT_SUFFIXES, FORBIDDEN_TOKENS)
    assert not offenders, "\n".join(offenders)


def test_python_readers_do_not_reference_dead_live_tables() -> None:
    """okg.nodes_live / okg.edges_live were emptied by the Chronos cutover
    and later dropped; any reference under these roots is a dead read path.
    Read the graph through ChronosGraphStore.checkout_generation against
    okg.graph_nodes / okg.graph_edges instead."""
    # The public copy leaves out this repository's own experiment and lab
    # deployment. There it audits every root that exists, and the package
    # roots under src/, which always ship, must be among them. A root that
    # does ship is therefore always audited.
    roots = PY_AUDIT_ROOTS
    if is_public_copy():
        roots = tuple(root for root in PY_AUDIT_ROOTS if root.is_dir())
        package_roots = {r for r in PY_AUDIT_ROOTS if r.is_relative_to(REPO_ROOT / "src")}
        assert package_roots and package_roots <= set(roots), package_roots - set(roots)
    offenders = _offenders(roots, {".py"}, PY_FORBIDDEN_TOKENS)
    assert not offenders, "\n".join(offenders)
