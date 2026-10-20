"""Deployment names a repository's tooling acts on are configuration.

The kept fleet (`okg up --all`), the Nomos audits' required deployments, the
strict PACT smoke deployment and the extra protected databases used to be
package constants naming the development repository's private deployments.
They are now the `deployment_fleet` block of the checkout's
`.okg/coordination.yaml` (#2823). A checkout without it -- the public copy,
any other user's repository -- gets an empty fleet, and each consumer refuses
or reports the empty list; nothing falls back to a built-in name.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from okg.substrate.deployment_default import DeploymentFleet, deployment_fleet
from okg.substrate.errors import SubstrateError

REPO_ROOT = Path(__file__).resolve().parents[2]


def _config(root: Path, body: str) -> Path:
    path = root / ".okg" / "coordination.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return root


def test_no_configuration_is_an_empty_fleet(tmp_path: Path) -> None:
    assert deployment_fleet(tmp_path) == DeploymentFleet()
    _config(tmp_path, "graph_projection:\n  deployment: x\n")
    assert deployment_fleet(tmp_path) == DeploymentFleet()


def test_the_block_is_read_in_full(tmp_path: Path) -> None:
    root = _config(
        tmp_path,
        "deployment_fleet:\n"
        "  kept: [a, b]\n"
        "  nomos_required: [c]\n"
        "  nomos_required_when_present: [d]\n"
        "  strict_pact_smoke:\n"
        "    e:\n"
        "      source_family: e_repo\n"
        "  protected_databases: [f]\n",
    )
    fleet = deployment_fleet(root)
    assert fleet == DeploymentFleet(
        source=str(root / ".okg" / "coordination.yaml"),
        kept=("a", "b"),
        nomos_required=("c",),
        nomos_required_when_present=("d",),
        strict_pact_smoke={"e": "e_repo"},
        protected_databases=("f",),
    )


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ("deployment_fleet: [a]\n", "must be a mapping"),
        ("deployment_fleet:\n  surprise: [a]\n", "must be a mapping"),
        ("deployment_fleet:\n  kept: a\n", "kept must be a list"),
        ("deployment_fleet:\n  kept: ['']\n", "kept must be a list"),
        ("deployment_fleet:\n  strict_pact_smoke: {e: e_repo}\n", "strict_pact_smoke"),
        ("deployment_fleet:\n  strict_pact_smoke: []\n", "strict_pact_smoke"),
        ("deployment_fleet:\n  strict_pact_smoke: false\n", "strict_pact_smoke"),
        (
            "deployment_fleet:\n  strict_pact_smoke:\n    e: {source_family: ''}\n",
            "strict_pact_smoke",
        ),
    ],
)
def test_a_malformed_block_is_loud_not_empty(
    tmp_path: Path, body: str, fragment: str
) -> None:
    root = _config(tmp_path, body)
    with pytest.raises(SubstrateError) as raised:
        deployment_fleet(root)
    assert fragment in str(raised.value)


def _configured_names() -> set[str]:
    config = REPO_ROOT / ".okg" / "coordination.yaml"
    if not config.is_file():
        pytest.skip(
            ".okg/coordination.yaml is this repository's own configuration and "
            "does not ship in the public copy, whose package names no fleet"
        )
    fleet = deployment_fleet(REPO_ROOT)
    names = {
        *fleet.kept,
        *fleet.nomos_required,
        *fleet.nomos_required_when_present,
        *fleet.strict_pact_smoke,
        *fleet.protected_databases,
    }
    assert names, "the development repository must configure its fleet"
    return names


#: Shipped uses of a configured name that stay, each for a stated reason:
#: the readiness profile named after the CI authority graph (a product
#: profile, like `workspace-graph`); the MCP server-name exception table
#: (removing it would re-attribute this repository's own session history);
#: and a v4 PACT affected-area label in the high-risk set (a manifest label,
#: not a deployment reference).
_RETAINED = {
    "src/okg/substrate/cli/deployment_readiness.py": 1,
    "src/okg/substrate/library/sources/_mcp_tool_telemetry.py": 2,
    "src/okg/substrate/pact/validation.py": 1,
}


def test_no_shipped_code_names_a_configured_deployment() -> None:
    """Package code carries none of the configured names as data (#2823).

    A string constant equal to a configured name is a hard-coded private
    deployment; docstrings, comments and identifiers that merely contain a
    name (view ids, check ids) are not scanned.
    """
    names = _configured_names()
    found: dict[str, int] = {}
    for path in sorted((REPO_ROOT / "src").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = {
            id(node.body[0].value)
            for node in ast.walk(tree)
            if isinstance(
                node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            )
            and node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
        }
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in docstrings
                and node.value in names
            ):
                rel = path.relative_to(REPO_ROOT).as_posix()
                found[rel] = found.get(rel, 0) + 1
    assert found == _RETAINED
