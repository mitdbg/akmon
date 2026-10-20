"""Every identifier the live-relations guide names must exist in the code.

The page teaches an operator to write a declaration, a binding and a policy
bundle. A field name, an error code or a table that the page spells and the
substrate does not have is worse than no page at all: the operator types it,
the deployment refuses, and the refusal names something the guide never
mentioned. So the page's own backticked vocabulary is checked against
`error_codes.yaml` and the source, here, hermetically.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "docs/operate/live-relations.md"
ERROR_CODES = ROOT / "docs/reference/error_codes.yaml"
SCHEMA_SQL = ROOT / "src/okg/substrate/db/schema.sql"

#: Where a documented name is allowed to come from.
SOURCE_FILES = (
    "src/okg/substrate/external_backed/live_relations.py",
    "src/okg/substrate/external_backed/live_dispatch.py",
    "src/okg/substrate/external_backed/http_adapter.py",
    "src/okg/substrate/external_backed/mcp_adapter.py",
    "src/okg/substrate/external_backed/_http_guard.py",
    "src/okg/substrate/external_backed/call_sample.py",
    "src/okg/substrate/catalog/modules_compose.py",
    "src/okg/substrate/catalog/deployment_product_composition.py",
    "src/okg/substrate/mcp/server.py",
    "src/okg/substrate/mcp/tools.py",
    "src/okg/substrate/nomos/store.py",
    "src/okg/substrate/nomos/identity_store.py",
    "src/okg/substrate/db/schema.sql",
)

#: Names that belong to the two WORKED EXAMPLES rather than to the substrate's
#: vocabulary: relation names, argument names and the fixture's own labels. An
#: operator chooses these; the substrate never spells them. Listed explicitly
#: so the check below stays exhaustive over everything else.
EXAMPLE_NAMES = frozenset({
    "Machine", "documents", "top_processes", "cpu_per_core",
    "index", "limit", "note",
    # The `kind: mcp` example: its relation and the tool's own argument names.
    "datasets", "primary_ds_name", "detail",
})

FENCE_RE = re.compile(r"```(yaml|json|sql)\n(.*?)```", re.DOTALL)
INLINE_RE = re.compile(r"`([^`\n]+)`")

#: The Glances CI fixture the page quotes. Its three snippets are compared
#: against these files on disk, so the comparison runs in every checkout.
FIXTURE_DIR = ROOT / "tests/substrate/e2e/fixtures/live-http-ci"


def _page_text() -> str:
    return PAGE.read_text(encoding="utf-8")


def _source_blob() -> str:
    return "\n".join(
        (ROOT / relative).read_text(encoding="utf-8") for relative in SOURCE_FILES
    )


def _registered_codes() -> set[str]:
    raw = yaml.safe_load(ERROR_CODES.read_text(encoding="utf-8"))
    codes: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            code = node.get("code")
            if isinstance(code, str):
                codes.add(code)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(raw)
    return codes


def _mapping_keys(node: object, into: set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            into.add(str(key))
            _mapping_keys(value, into)
    elif isinstance(node, list):
        for value in node:
            _mapping_keys(value, into)


def _declared_keys() -> set[str]:
    keys: set[str] = set()
    for language, block in FENCE_RE.findall(_page_text()):
        if language == "yaml":
            _mapping_keys(yaml.safe_load(block), keys)
        elif language == "json":
            _mapping_keys(json.loads(block), keys)
    return keys


def _live_tokens() -> set[str]:
    tokens: set[str] = set()
    for span in INLINE_RE.findall(_page_text()):
        tokens.update(re.findall(r"\blive_[a-z0-9_]+\b", span))
    return tokens


def test_the_guide_is_present_with_operator_front_matter() -> None:
    text = _page_text()
    front = yaml.safe_load(re.match(r"\A---\n(.*?)\n---\n", text, re.DOTALL).group(1))
    assert front["audience"] == "operator"
    assert front["type"] == "how-to"
    assert front["canonical_id"] == "live-relations"


def test_every_live_error_code_the_guide_names_is_registered() -> None:
    codes = _registered_codes()
    blob = _source_blob()
    # `live_relations` / `live_bindings` are the annotation and manifest keys,
    # not codes; they are checked against the source instead.
    unregistered = sorted(
        token for token in _live_tokens()
        if token not in codes and token not in blob
    )
    assert not unregistered, unregistered
    # And the page must actually be naming codes, so a future edit that
    # deleted the troubleshooting tables does not pass this vacuously.
    assert len(_live_tokens() & codes) >= 15


def test_a_fabricated_code_would_be_caught() -> None:
    # The check above is only worth running if it bites. A code that is
    # neither registered nor spelled in the source must not pass.
    assert "live_relation_does_not_exist" not in _registered_codes()
    assert "live_relation_does_not_exist" not in _source_blob()


def test_every_field_the_snippets_declare_exists_in_the_code() -> None:
    blob = _source_blob()
    missing = sorted(
        key for key in _declared_keys()
        if key not in EXAMPLE_NAMES and key not in blob
    )
    assert not missing, missing


def test_the_sample_tables_and_their_retention_match_the_schema() -> None:
    schema = SCHEMA_SQL.read_text(encoding="utf-8")
    text = _page_text()
    for table, days in (("hot", 7), ("warm", 90), ("cold", 730)):
        qualified = f"okg.call_sample_{table}"
        assert f"CREATE TABLE IF NOT EXISTS {qualified} (" in schema
        assert f"`{qualified}`" in text
        assert re.search(
            rf"add_retention_policy\('{re.escape(qualified)}',\s+INTERVAL '{days} days'",
            schema,
        ), qualified
        assert f"| {days} days |" in text


def test_the_states_and_policies_match_the_dispatcher() -> None:
    from okg.substrate.external_backed.call_sample import (
        INLINE_MAX_BYTES,
        RETENTION_CLASSES,
        SAMPLE_POLICIES,
    )
    from okg.substrate.external_backed.live_dispatch import LIVE_STATES
    from okg.substrate.external_backed.live_relations import (
        ANNOTATION_KEY,
        PRINCIPAL_MODES,
    )

    text = _page_text()
    for state in LIVE_STATES:
        assert f"`{state}`" in text, state
    for policy in SAMPLE_POLICIES:
        assert f"| `{policy}` |" in text, policy
    for retention in RETENTION_CLASSES:
        assert f"| `{retention}` |" in text, retention
    for mode in PRINCIPAL_MODES:
        assert f"`{mode}`" in text, mode
    assert f"`{ANNOTATION_KEY}`" in text
    assert INLINE_MAX_BYTES == 16 * 1024 and "16 KiB" in text


def test_the_gate_reads_the_bundle_filename_the_guide_names() -> None:
    from okg.substrate.external_backed.gating import BUNDLE_FILENAME
    from okg.substrate.external_backed.live_dispatch import (
        DEFAULT_SERVICE_PRINCIPAL,
        GATE_DENY_REASON,
        GATE_ERROR_REASON,
        PRINCIPAL_UNRESOLVED_REASON,
    )

    text = _page_text()
    for name in (
        BUNDLE_FILENAME, DEFAULT_SERVICE_PRINCIPAL, GATE_DENY_REASON,
        GATE_ERROR_REASON, PRINCIPAL_UNRESOLVED_REASON,
    ):
        assert f"`{name}`" in text, name


def _table_rows(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or set(stripped) <= set("|- "):
            continue
        rows.append([cell.strip() for cell in stripped.strip("|").split("|")])
    return rows


def _troubleshooting_codes() -> set[str]:
    """The first column of the error-code table, as bare code names."""
    section = _page_text().split("## 6. Troubleshooting by error code", 1)[1]
    codes: set[str] = set()
    for cells in _table_rows(section):
        match = re.fullmatch(r"`(live_[a-z0-9_]+)`", cells[0])
        if match:
            codes.add(match.group(1))
    return codes


def test_the_troubleshooting_table_lists_only_registered_codes() -> None:
    """Stricter than the source grep: these must be REGISTERED codes.

    A `live_*` name that merely appears somewhere in the source passes the
    broad check above. The troubleshooting table promises an operator a code
    they can look up in `error_codes.yaml`, so every row is held to that.
    """
    listed = _troubleshooting_codes()
    assert len(listed) >= 18, sorted(listed)
    unregistered = sorted(listed - _registered_codes())
    assert not unregistered, unregistered


def _fixture_file(relative: str) -> str:
    """One fixture file, read from the tree.

    A missing file FAILS the test: the page quotes this fixture, so a
    checkout without it is a page quoting something that is not there.
    """
    path = FIXTURE_DIR / relative
    assert path.is_file(), f"the page quotes {path.relative_to(ROOT)}, which is missing"
    return path.read_text(encoding="utf-8")


def _assert_subset(quoted: object, shipped: object, path: str) -> None:
    """Every leaf the page quotes must equal the shipped file's leaf.

    A subset, because both page snippets are marked abridged. Extra keys in
    the shipped file are fine; a key the page spells differently, or a value
    it gets wrong, is not.
    """
    if isinstance(quoted, dict):
        assert isinstance(shipped, dict), path
        for key, value in quoted.items():
            assert key in shipped, f"{path}.{key} is not in the shipped fixture"
            _assert_subset(value, shipped[key], f"{path}.{key}")
    elif isinstance(quoted, list):
        assert quoted == shipped, path
    else:
        assert quoted == shipped, f"{path}: page {quoted!r} != fixture {shipped!r}"


def _page_blocks() -> dict[str, object]:
    """The page's fenced snippets, keyed by their distinguishing top-level key."""
    blocks: dict[str, object] = {}
    for language, block in FENCE_RE.findall(_page_text()):
        if language == "yaml":
            parsed = yaml.safe_load(block)
        elif language == "json":
            parsed = json.loads(block)
        else:
            continue
        if isinstance(parsed, dict):
            for marker in ("classes", "agent_surface", "policy_bundle_id"):
                if marker in parsed:
                    blocks[marker] = parsed
    return blocks


def test_the_fixture_declaration_snippet_matches_the_shipped_fixture() -> None:
    shipped = yaml.safe_load(_fixture_file("schemas/machine_live_relations.yaml"))
    quoted = _page_blocks()["classes"]
    _assert_subset(quoted["classes"], shipped["classes"], "classes")


def test_the_fixture_binding_snippet_matches_the_shipped_deployment() -> None:
    shipped = yaml.safe_load(_fixture_file("deployment.yaml"))
    quoted = _page_blocks()["agent_surface"]
    _assert_subset(
        quoted["agent_surface"], shipped["agent_surface"], "agent_surface",
    )


def test_the_policy_bundle_snippet_matches_the_shipped_bundle() -> None:
    shipped = json.loads(_fixture_file("nomos_external_probe.json"))
    _assert_subset(_page_blocks()["policy_bundle_id"], shipped, "bundle")
