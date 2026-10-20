"""Refuse a new private DSN parser, masker or placeholder expander under src/okg.

`okg.substrate.db.dsn` is the one implementation of DSN parsing, rewriting,
`${VAR}` expansion and password masking (issues #1980, #2230, #2040, #2285).
Before it existed, several private copies disagreed: one expander treated an
empty variable as unset and another as set, one masker ate the lines after
`password=`, another showed half a password containing `:`. This test stops a
new copy from appearing while the old ones are moved onto the module.

It is a ratchet, the same shape as `test_no_raw_psycopg_connect.py`.
`ALLOWED_PRIVATE_DSN_FILES` records every file that still had a private copy
when the module landed. The test fails two ways:

* a file NOT on the list (and not permanently exempt) matches -- a new
  private copy; call `okg.substrate.db.dsn` instead;
* a file ON the list no longer matches -- it has been migrated, so remove it
  from the list. That keeps the list shrinking only.

`PERMANENT_EXEMPTIONS` is separate and every entry carries a reason: generic
secret maskers that are not DSN code, the chat app's own DSN handling, and
`${VAR}` users whose values are not DSNs. It has a stale check too: an exempt
file the scan no longer flags must leave the map, so the map cannot hide a
file that later gains a real DSN masker under an old reason.

What counts, found by walking each file's AST (so a call split over lines is
still seen):

1. an import of `psycopg.conninfo`, or a call to `conninfo_to_dict` or
   `make_conninfo`;
2. a pattern given to `compile/sub/subn/search/match/fullmatch/findall/
   finditer/split` of `re` or `regex` (under any alias, or imported by name
   with `from re import compile`; positional or `pattern=`) that contains
   `\\$\\{` (placeholder expansion), `password` or `passfile` (masking), or
   both `://` and `@` (URL userinfo). The pattern may be a literal, a
   constant anywhere in the file, an f-string or a `+` concatenation;
3. `urlsplit`/`urlparse` called on an argument whose names mention `dsn`,
   `conninfo`, `database_url`, `db_url`, `pg_url`, `postgres` or a
   connection string;
4. a function whose name pairs a masking or parsing verb (redact, mask,
   scrub, hide, sanitize, censor, parse, expand, interpolate) with a DSN word
   (`dsn`, `conninfo`, `connection_string`, `conn_str`, `database_url`,
   `db_url`): `_redact_dsn`, `_scrub_dsn`, `mask_connection_string`, ...; or
   is exactly `redact`/`_redact` or `_interpolate_env`;
5. `re.sub`/`re.subn` whose replacement writes a mask (`***` or
   `redacted`), the shape of `bootstrap._redact_dsn`'s `":***@"`;
6. `urlsplit(...).password` (or a name assigned from `urlsplit`) passed as
   a call argument or into an f-string, the shape of a hand-written masker.
   A truthiness test that refuses credentials is not flagged.

What the guard cannot see. A heuristic scan does not find code that rebuilds
a DSN from `conn.info`, compares DSN strings, or rewrites a DSN with string
methods. The design inventory lists nine such migration sites; an empty
allowlist does not prove they moved. Each migration pull request checks the
ones it owns by hand (`MIGRATION_SITES_THE_GUARD_CANNOT_SEE`).

Scope is `src/okg`. `src/chronos_core` is vendored from mitdbg/chronos and
must not import `okg`, so it is not walked.
"""

from __future__ import annotations

import ast
import functools
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
OKG_ROOT = REPO_ROOT / "src" / "okg"

#: The module every DSN operation belongs in.
DSN_MODULE = "src/okg/substrate/db/dsn.py"

FIX_HINT = (
    "use the shared DSN module instead of a private copy:\n"
    "    from okg.substrate.db import dsn\n"
    "    dsn.parse(value) / dsn.try_parse(value) / dsn.form_of(value)\n"
    "    dsn.require_url_form(value, source=...)\n"
    "    dsn.expand(template, lookup) / dsn.require_expanded(template, source=...)\n"
    "    dsn.redact(value) / dsn.redact_text(text) / dsn.redact_argv(argv)\n"
    "    dsn.parse(value).with_dbname(...) / .with_params(...) / .to_libpq_env()"
)

# Files that still held a private DSN parser, masker or expander when the
# module landed (inventory: the Sprint 14 DSN-module design, sections 1.1-1.5,
# plus what this scan found at origin/dev 400b2932c). Shrink only -- never add
# a file here to let a new copy through. Each migration pull request removes
# the files it moves onto `okg.substrate.db.dsn`.
ALLOWED_PRIVATE_DSN_FILES: frozenset[str] = frozenset(
    {
        # ALLOWLIST-START
        "src/okg/substrate/cli/_db_bootstrap_errors.py",
        "src/okg/substrate/cli/_env_helpers.py",
        "src/okg/substrate/cli/deployment_inventory.py",
        "src/okg/substrate/cli/deployment_readiness.py",
        "src/okg/substrate/cli/dogfood.py",
        "src/okg/substrate/cli/environment.py",
        "src/okg/substrate/cli/init.py",
        "src/okg/substrate/cli/main.py",
        "src/okg/substrate/cli/nomos_rollout_audit.py",
        # Migration target: credential-shaped text detection -> dsn.form_of / redact_text.
        "src/okg/substrate/console_views/served_text.py",
        "src/okg/substrate/db/readiness.py",
        "src/okg/substrate/deployment_apply.py",
        "src/okg/substrate/deployment_authority.py",
        "src/okg/substrate/deployment_env_doctor.py",
        "src/okg/substrate/deployment_lint.py",
        "src/okg/substrate/deployment_promotion_operations.py",
        "src/okg/substrate/deployment_upgrade.py",
        "src/okg/substrate/deployments/__init__.py",
        "src/okg/substrate/deployments/environment.py",
        "src/okg/substrate/environment_services.py",
        "src/okg/substrate/exchange/admission.py",
        "src/okg/substrate/exchange/harness.py",
        "src/okg/substrate/ingest/adapter_factory.py",
        "src/okg/substrate/ingest/service.py",
        "src/okg/substrate/library/agent_runtimes/coordination_ssh.py",
        "src/okg/substrate/library/profiles/academic-researcher/cli/install.py",
        "src/okg/substrate/llm/tracking.py",
        "src/okg/substrate/mcp/connector_ingest.py",
        "src/okg/substrate/mcp/server.py",
        "src/okg/substrate/mcp/source_lifecycle.py",
        "src/okg/substrate/mcp/surface_fleet.py",
        "src/okg/substrate/qa/complete_reference.py",
        "src/okg/substrate/runtime/admission_workflows.py",
        "src/okg/substrate/runtime/bootstrap.py",
        "src/okg/substrate/sources/registry.py",
        "src/okg/substrate/support/bug_report.py",
        "src/okg/viz/coordination_adapters.py",
        # ALLOWLIST-END
    }
)

# Never migrated: not DSN code, or deliberately outside the DSN program.
# Every entry needs a reason; the test refuses an empty one.
PERMANENT_EXEMPTIONS: dict[str, str] = {
    DSN_MODULE: "the shared DSN module itself",
    # The chat app manages its own chat-database DSN; out of the DSN program
    # by operator decision on question 9 of the Sprint 14 DSN-module design.
    "src/okg/substrate/chat/instance.py": "chat app DSN: rewrites the chat database host alias",
    "src/okg/substrate/chat/sync.py": "chat app: generic secret masker for chat sync payloads",
    "src/okg/substrate/deployments/chat_block.py": "chat app: refuses credential-shaped keys in the chat block",
    # Generic secret maskers: they mask known values, whole URLs or
    # secret-named keys, and do not parse a DSN.
    "src/okg/substrate/extension_broker.py": "generic secret masker for extension broker output",
    "src/okg/substrate/external_backed/_http_guard.py": "generic URL credential and token masker for HTTP errors",
    "src/okg/substrate/library/agent_runtimes/launcher.py": "generic secret-literal detector for agent launch arguments",
    "src/okg/substrate/library/agent_runtimes/secret_ssh.py": "generic secret masker for SSH agent runtimes",
    "src/okg/substrate/library/enrichers/extraction/base.py": "generic secret-field masker for extraction failures",
    "src/okg/substrate/library/extraction/pdf_table_vlm.py": "generic sensitive-key masker for VLM requests",
    "src/okg/substrate/library/sources/_git_graph_history_emit.py": "generic secret masker for commit diff text",
    "src/okg/substrate/library/sources/_git_helpers.py": "generic git remote credential masker",
    "src/okg/substrate/library/sources/_source_urls.py": "git remote URL parser (ssh/https), not a DSN",
    "src/okg/substrate/library/sources/_transcript_redaction.py": "generic credential-URL masker for session transcripts",
    "src/okg/substrate/library/sources/agent_runtime_security_monitor.py": "generic secret masker for runtime security events",
    "src/okg/substrate/library/sources/security_analysis.py": "generic high-entropy token masker for scanner findings",
    "src/okg/substrate/mcp/response_bounds.py": "generic sensitive-key masker for MCP error payloads",
    "src/okg/substrate/nomos/runtime_enforcement.py": "generic secret-looking-text detector",
    "src/okg/substrate/sources/redaction.py": "generic token layer (auth headers, *TOKEN*/*PASSWORD* assignments); its DSN masking is dsn.redact_text",
    "src/okg/substrate/pact/agent_install.py": "agent install config: credential detection and ${VAR} references, not a DSN",
    "src/okg/substrate/pact/coordination_operations.py": "coordination credential references (${VAR} or credential_ref), not a DSN",
    "src/okg/substrate/views/read_policy.py": "view read policy: refuses secret literals in view SQL",
    # `${VAR}` users whose values are not DSNs.
    "src/okg/substrate/catalog/profile.py": "${VAR} in catalog profiles, not a DSN",
    "src/okg/substrate/deployment_profile_explain.py": "${VAR} in profile explanations, not a DSN",
    "src/okg/substrate/deployments/sync_root.py": "${VAR} in sync-root paths, which refuse secret variables",
    "src/okg/substrate/library/sources/upload.py": "${VAR} in upload source paths, not a DSN",
    "src/okg/substrate/publisher/checkpoint_attestations.py": "${VAR} in checkpoint attestation config, not a DSN",
    "src/okg/substrate/runtime/openshell_policy.py": "${VAR} in OpenShell policy templates, not a DSN",
    # Not DSN code; flagged once the guard resolved patterns held in constants.
    "src/okg/viz/endpoints.py": "reader path filter: a constant regex naming secret-looking file names",
}

# Migration sites from the design inventory that no rule above can see: they
# rebuild a DSN from connection info, compare DSN strings, or rewrite one with
# string methods. The allowlist emptying does not prove these moved; the
# migration pull request that owns each file checks it by hand.
MIGRATION_SITES_THE_GUARD_CANNOT_SEE: tuple[str, ...] = (
    "src/okg/deployment/_internal/external.py",
    "src/okg/substrate/catalog/ownership.py",
    "src/okg/substrate/cli/doctor.py",
    "src/okg/substrate/cli/dsn_authority.py",
    "src/okg/substrate/deployment_agreement.py",
    "src/okg/substrate/graph_store_internal/common.py",
    "src/okg/substrate/runtime/escalation_response.py",
    "src/okg/substrate/runtime/release_authority.py",
    "src/okg/substrate/runtime/worker.py",
)

_RE_FUNCS = frozenset(
    {
        "compile",
        "sub",
        "subn",
        "search",
        "match",
        "fullmatch",
        "findall",
        "finditer",
        "split",
    }
)
_REGEX_MODULES = frozenset({"re", "regex"})
_URL_FUNCS = frozenset({"urlsplit", "urlparse"})
_CONNINFO_FUNCS = frozenset({"conninfo_to_dict", "make_conninfo"})
_DSN_ARG_TOKENS = (
    "dsn",
    "conninfo",
    "database_url",
    "db_url",
    "pg_url",
    "postgres",
    "connection_string",
    "conn_str",
    "connstr",
)
_DSN_WORDS = (
    r"(?:dsn|conninfo|connection_?string|conn_?str|database_?url|db_?url|pg_?url)"
)
_VERBS = (
    r"(?:redact|mask|scrub|hide|sanitiz|censor|strip_password|pars|expand"
    r"|interpolat|placeholder)"
)
_FUNC_NAME_RE = re.compile(
    r"^_?redact$"
    rf"|{_DSN_WORDS}.*{_VERBS}"
    rf"|{_VERBS}.*{_DSN_WORDS}"
    r"|^_?interpolate_env(?:_vars)?$",
    re.IGNORECASE,
)


# Text prefilter: a file that fails it cannot match any rule, so it is not
# parsed. A shortcut in cost only -- every rule needs its prefilter's text in
# the source: rules 1, 3, 4 and 6 name a function, module or attribute, and
# rules 2 and 5 need an import of `re` or `regex` (the pattern itself may be
# assembled from pieces, so its text is not required).
_CANDIDATE_NAMES = re.compile(
    rf"conninfo|urlsplit|urlparse|\.password\b|def\s+\w*(?:redact|{_DSN_WORDS}"
    r"|interpolate_env)",
    re.IGNORECASE,
)
_CANDIDATE_REGEX_IMPORT = re.compile(
    r"^\s*(?:import\s+(?:re|regex)\b|from\s+(?:re|regex)\s+import\b)", re.MULTILINE
)


def _is_candidate(source: str) -> bool:
    return bool(
        _CANDIDATE_NAMES.search(source) or _CANDIDATE_REGEX_IMPORT.search(source)
    )


def _regex_literal_is_dsn_handling(pattern: str) -> bool:
    lowered = pattern.lower()
    return (
        "\\$\\{" in pattern
        or "password" in lowered
        or "passfile" in lowered
        or ("://" in pattern and "@" in pattern)
    )


def _call_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _names_in(node: ast.AST) -> list[str]:
    names: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Name):
            names.append(child.id)
        elif isinstance(child, ast.Attribute):
            names.append(child.attr)
        elif isinstance(child, ast.arg):
            names.append(child.arg)
    return names


def _string_constants(tree: ast.AST) -> dict[str, str]:
    """Every ``NAME = <string expression>`` in the file, resolved.

    A pattern held in a constant (``PAT = r"password=(\\S+)"``), built by
    ``+`` or an f-string, is resolved so the regex rule sees its text.
    Resolved twice so a constant built from another constant is seen.
    """
    assignments: list[tuple[str, ast.expr]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                assignments.append((target.id, node.value))
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.value is not None
        ):
            assignments.append((node.target.id, node.value))
    constants: dict[str, str] = {}
    for _ in range(2):
        for name, value in assignments:
            text = _string_value(value, constants)
            if text is not None:
                constants[name] = text
    return constants


def _string_value(node: ast.AST | None, constants: dict[str, str]) -> str | None:
    """The text of a string expression, or ``None`` when it is not one.

    Unknown parts of an f-string or a ``+`` chain count as empty, so
    ``"pass" + "word=" + x`` still shows ``password=``.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.JoinedStr):
        return "".join(_string_value(part, constants) or "" for part in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _string_value(node.left, constants)
        right = _string_value(node.right, constants)
        if left is None and right is None:
            return None
        return (left or "") + (right or "")
    return None


def _writes_a_mask(replacement: str | None) -> bool:
    return replacement is not None and (
        "***" in replacement or "redacted" in replacement.lower()
    )


def _regex_callables(tree: ast.AST) -> tuple[set[str], dict[str, str]]:
    """Module aliases of ``re``/``regex``, and names imported from them
    (local name -> the function's real name)."""
    modules: set[str] = set()
    functions: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in _REGEX_MODULES:
                    modules.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module in _REGEX_MODULES:
            for alias in node.names:
                if alias.name in _RE_FUNCS:
                    functions[alias.asname or alias.name] = alias.name
    return modules, functions


def _regex_call(
    node: ast.Call, modules: set[str], functions: dict[str, str]
) -> str | None:
    """The ``re`` function a call reaches (``re.sub``, ``regex.compile``,
    a ``from re import compile`` name), or ``None``."""
    func = node.func
    if (
        isinstance(func, ast.Attribute)
        and func.attr in _RE_FUNCS
        and isinstance(func.value, ast.Name)
        and func.value.id in modules
    ):
        return func.attr
    if isinstance(func, ast.Name) and func.id in functions:
        return functions[func.id]
    return None


def _argument(node: ast.Call, index: int, keyword: str) -> ast.expr | None:
    if len(node.args) > index:
        return node.args[index]
    for item in node.keywords:
        if item.arg == keyword:
            return item.value
    return None


def _url_split_names(tree: ast.AST) -> set[str]:
    """Names assigned from a ``urlsplit``/``urlparse`` call."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Call)
            and _call_name(node.value.func) in _URL_FUNCS
        ):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return names


def _value_positions(tree: ast.AST) -> set[int]:
    """``id`` of every node used as a call argument or an f-string value.

    ``urlsplit(url).password`` in a truthiness test only refuses credentials;
    passed to ``str.replace``, ``quote`` or an f-string it is being masked or
    rebuilt, which is DSN handling.
    """
    positions: set[int] = set()
    for node in ast.walk(tree):
        values: list[ast.AST] = []
        if isinstance(node, ast.Call):
            values = [*node.args, *(item.value for item in node.keywords)]
        elif isinstance(node, ast.FormattedValue):
            values = [node.value]
        for value in values:
            # `urlsplit(x).password or ""` passed on is the same idiom.
            positions.update(id(child) for child in ast.walk(value))
    return positions


def private_dsn_findings(source: str, filename: str = "<source>") -> list[str]:
    """Every private-DSN-handling idiom in ``source``, as ``line: rule`` strings."""
    if not _is_candidate(source):
        return []
    tree = ast.parse(source, filename=filename)
    modules, functions = _regex_callables(tree)
    constants = _string_constants(tree) if (modules or functions) else {}
    split_names = _url_split_names(tree)
    value_positions = _value_positions(tree) if ".password" in source else set()

    findings: list[str] = []
    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "psycopg.conninfo":
                    findings.append(f"{line}: imports psycopg.conninfo")
        elif isinstance(node, ast.ImportFrom):
            if node.module == "psycopg.conninfo" or (
                node.module == "psycopg"
                and any(a.name == "conninfo" for a in node.names)
            ):
                findings.append(f"{line}: imports psycopg.conninfo")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _FUNC_NAME_RE.search(node.name):
                findings.append(f"{line}: defines {node.name}()")
        elif (
            isinstance(node, ast.Attribute)
            and node.attr == "password"
            and id(node) in value_positions
        ):
            value = node.value
            if (
                isinstance(value, ast.Call) and _call_name(value.func) in _URL_FUNCS
            ) or (isinstance(value, ast.Name) and value.id in split_names):
                findings.append(f"{line}: passes urlsplit().password on")
        elif isinstance(node, ast.Call):
            name = _call_name(node.func)
            regex_function = _regex_call(node, modules, functions)
            if name in _CONNINFO_FUNCS:
                findings.append(f"{line}: calls {name}")
            elif name in _URL_FUNCS and node.args:
                arg_names = [n.lower() for n in _names_in(node.args[0])]
                if any(tok in n for n in arg_names for tok in _DSN_ARG_TOKENS):
                    findings.append(f"{line}: {name}() on a DSN")
            elif regex_function is not None:
                pattern = _string_value(_argument(node, 0, "pattern"), constants)
                if pattern is not None and _regex_literal_is_dsn_handling(pattern):
                    findings.append(f"{line}: re.{regex_function}() with a DSN pattern")
                elif regex_function in {"sub", "subn"}:
                    replacement = _string_value(_argument(node, 1, "repl"), constants)
                    if _writes_a_mask(replacement):
                        findings.append(f"{line}: re.{regex_function}() writing a mask")
            elif (
                name in {"sub", "subn"}
                and isinstance(node.func, ast.Attribute)
                and _writes_a_mask(_string_value(_argument(node, 0, "repl"), constants))
            ):
                # `RX = re.compile(...)` then `RX.sub("***", text)`: the
                # pattern may hold no DSN text, but the replacement is a mask.
                findings.append(f"{line}: compiled pattern .{name}() writing a mask")
    return sorted(set(findings), key=lambda f: int(f.split(":", 1)[0]))


@functools.lru_cache(maxsize=1)
def _tree_findings() -> dict[str, list[str]]:
    """Map every file under src/okg with a finding to its findings."""
    result: dict[str, list[str]] = {}
    for path in sorted(OKG_ROOT.rglob("*.py")):
        found = private_dsn_findings(path.read_text(encoding="utf-8"), str(path))
        if found:
            result[path.relative_to(REPO_ROOT).as_posix()] = found
    return result


def test_no_new_private_dsn_handling() -> None:
    """No file outside the module, the allowlist and the exemptions may match."""
    offenders = {
        name: found
        for name, found in _tree_findings().items()
        if name not in ALLOWED_PRIVATE_DSN_FILES and name not in PERMANENT_EXEMPTIONS
    }
    assert not offenders, (
        "private DSN parsing, masking or placeholder expansion outside "
        f"{DSN_MODULE}:\n"
        + "\n".join(
            f"  {name}\n    " + "\n    ".join(found)
            for name, found in offenders.items()
        )
        + f"\n{FIX_HINT}"
    )


def test_allowlist_has_no_stale_entries() -> None:
    """A migrated file must leave the allowlist, so the list only shrinks."""
    findings = _tree_findings()
    stale = sorted(name for name in ALLOWED_PRIVATE_DSN_FILES if name not in findings)
    assert not stale, (
        "these files no longer hold private DSN handling; remove them from "
        f"ALLOWED_PRIVATE_DSN_FILES: {stale}"
    )


def test_permanent_exemptions_have_no_stale_entries() -> None:
    """An exempt file the scan no longer flags must leave the map.

    Otherwise the whole-file exemption outlives its reason, and a real DSN
    masker added to that file later passes under the old reason.
    """
    findings = _tree_findings()
    stale = sorted(name for name in PERMANENT_EXEMPTIONS if name not in findings)
    assert not stale, (
        "these exempt files no longer match any rule; remove them from "
        f"PERMANENT_EXEMPTIONS: {stale}"
    )


def test_stale_exemption_turns_the_check_red(monkeypatch: pytest.MonkeyPatch) -> None:
    module = __import__(__name__, fromlist=["_tree_findings"])
    exempt = sorted(PERMANENT_EXEMPTIONS)[0]
    shrunk = {k: v for k, v in _tree_findings().items() if k != exempt}
    monkeypatch.setattr(module, "_tree_findings", lambda: shrunk)
    with pytest.raises(AssertionError, match=re.escape(exempt)):
        test_permanent_exemptions_have_no_stale_entries()


def test_migration_sites_the_guard_cannot_see_are_real_and_unflagged() -> None:
    """The hand-checked list names real files the scan does not already cover."""
    missing = [
        name
        for name in MIGRATION_SITES_THE_GUARD_CANNOT_SEE
        if not (REPO_ROOT / name).is_file()
    ]
    assert not missing, f"renamed or removed migration sites: {missing}"
    covered = set(MIGRATION_SITES_THE_GUARD_CANNOT_SEE) & (
        ALLOWED_PRIVATE_DSN_FILES | set(PERMANENT_EXEMPTIONS)
    )
    assert not covered, f"already covered by the scan: {sorted(covered)}"


def test_allowlist_and_exemptions_name_real_files() -> None:
    names = ALLOWED_PRIVATE_DSN_FILES | set(PERMANENT_EXEMPTIONS)
    missing = sorted(name for name in names if not (REPO_ROOT / name).is_file())
    assert not missing, f"allowlisted paths that do not exist: {missing}"


def test_allowlist_and_exemptions_do_not_overlap() -> None:
    assert not (ALLOWED_PRIVATE_DSN_FILES & set(PERMANENT_EXEMPTIONS))


def test_every_permanent_exemption_has_a_reason() -> None:
    blank = sorted(
        name for name, reason in PERMANENT_EXEMPTIONS.items() if not reason.strip()
    )
    assert not blank, f"permanent exemptions without a reason: {blank}"


def test_allowlist_is_sorted_one_path_per_line() -> None:
    """One sorted path per line, so parallel removals rarely conflict."""
    text = Path(__file__).read_text(encoding="utf-8")
    block = text.split("# ALLOWLIST-START", 1)[1].split("# ALLOWLIST-END", 1)[0]
    listed = re.findall(r'^\s*"([^"]+)",\s*$', block, flags=re.MULTILINE)
    assert listed == sorted(listed)
    assert set(listed) == set(ALLOWED_PRIVATE_DSN_FILES)


@pytest.mark.parametrize(
    ("source", "rule"),
    [
        ("import psycopg.conninfo\n", "imports psycopg.conninfo"),
        ("from psycopg.conninfo import conninfo_to_dict\n", "imports psycopg.conninfo"),
        ("from psycopg import conninfo\n", "imports psycopg.conninfo"),
        (
            "import psycopg\npsycopg.conninfo.make_conninfo(dsn, dbname='x')\n",
            "calls make_conninfo",
        ),
        ("conninfo_to_dict(\n    value,\n)\n", "calls conninfo_to_dict"),
        (
            "import re\nre.compile(r'\\$\\{([A-Z_]+)\\}')\n",
            "re.compile() with a DSN pattern",
        ),
        ("import re\nre.sub(r':[^:@]+@', ':***@', dsn)\n", "re.sub() writing a mask"),
        (
            "import re\nre.sub(r'(password=)\\S+', r'\\1***', text)\n",
            "re.sub() with a DSN pattern",
        ),
        (
            "import re as regex\nregex.search(r'passfile=(\\S+)', s)\n",
            "re.search() with a DSN pattern",
        ),
        (
            "import re\nre.findall('postgresql://([^@]+)@', s)\n",
            "re.findall() with a DSN pattern",
        ),
        ("from urllib.parse import urlsplit\nurlsplit(dsn)\n", "urlsplit() on a DSN"),
        (
            "import urllib.parse\nurllib.parse.urlparse(self.database_url)\n",
            "urlparse() on a DSN",
        ),
        (
            "from urllib.parse import urlsplit\nurlsplit(str(base_dsn))\n",
            "urlsplit() on a DSN",
        ),
        ("def _redact_dsn(value):\n    return value\n", "defines _redact_dsn()"),
        ("def _redact(value):\n    return value\n", "defines _redact()"),
        (
            "def _interpolate_env(value):\n    return value\n",
            "defines _interpolate_env()",
        ),
        ("def parse_dsn(value):\n    return value\n", "defines parse_dsn()"),
    ],
)
def test_seeded_violation_is_found(source: str, rule: str) -> None:
    """Each idiom the guard exists for is caught, including split calls."""
    found = private_dsn_findings(source)
    assert any(rule in finding for finding in found), found


# Review of PR 2309, S5: each of these slipped past the guard before.
GUARD_EVASIONS = {
    "constant-pattern": (
        "import re\nPAT = r'password=(\\S+)'\nre.compile(PAT)\n",
        "re.compile() with a DSN pattern",
    ),
    "constant-built-from-constant": (
        "import re\nKEY = 'password'\nPAT = KEY + r'=(\\S+)'\nre.sub(PAT, 'x', s)\n",
        "re.sub() with a DSN pattern",
    ),
    "concatenation": (
        "import re\nre.search('pass' + 'word=(\\\\S+)', s)\n",
        "re.search() with a DSN pattern",
    ),
    "f-string": (
        "import re\nre.compile(rf'{key}|password=(\\S+)')\n",
        "re.compile() with a DSN pattern",
    ),
    "class-constant": (
        "import re\nclass M:\n    PAT = r'://[^@]+@'\n    RX = re.compile(PAT)\n",
        "re.compile() with a DSN pattern",
    ),
    "from-re-import-compile": (
        "from re import compile\ncompile(r'passfile=(\\S+)')\n",
        "re.compile() with a DSN pattern",
    ),
    "from-re-import-sub-alias": (
        "from re import sub as rsub\nrsub(r':[^:@]+@', ':***@', dsn)\n",
        "re.sub() writing a mask",
    ),
    "regex-module": (
        "import regex\nregex.compile(r'password=(\\S+)')\n",
        "re.compile() with a DSN pattern",
    ),
    "pattern-keyword": (
        "import re\nre.compile(pattern=r'password=(\\S+)')\n",
        "re.compile() with a DSN pattern",
    ),
    "mask-replacement-keyword": (
        "import re\nre.sub(r':[^:@]+@', repl=':***@', string=dsn)\n",
        "re.sub() writing a mask",
    ),
    "urlsplit-password-replace": (
        "from urllib.parse import urlsplit\n"
        "def show(url_value):\n"
        "    return url_value.replace(urlsplit(url_value).password, '***')\n",
        "passes urlsplit().password on",
    ),
    "urlsplit-name-password-fstring": (
        "from urllib.parse import urlsplit\n"
        "def show(url):\n"
        "    parts = urlsplit(url)\n"
        "    return f'{parts.scheme}://{parts.username}:{parts.password}'\n",
        "passes urlsplit().password on",
    ),
    "urlsplit-on-db-url": (
        "from urllib.parse import urlsplit\nurlsplit(db_url)\n",
        "urlsplit() on a DSN",
    ),
    "urlsplit-on-postgres-url": (
        "from urllib.parse import urlsplit\nurlsplit(postgres_url)\n",
        "urlsplit() on a DSN",
    ),
    "scrub-name": ("def _scrub_dsn(v):\n    return v\n", "defines _scrub_dsn()"),
    "mask-connection-string": (
        "def mask_connection_string(v):\n    return v\n",
        "defines mask_connection_string()",
    ),
    "hide-conninfo": (
        "def _hide_conninfo(v):\n    return v\n",
        "defines _hide_conninfo()",
    ),
    "sanitize-database-url": (
        "def sanitize_database_url(v):\n    return v\n",
        "defines sanitize_database_url()",
    ),
    "private-mask-dsn": (
        "def _mask_the_dsn(v):\n    return v\n",
        "defines _mask_the_dsn()",
    ),
}


# Re-review of PR 2309, S4: compile a pattern with no DSN text, then `.sub`
# a mask through the compiled object.
GUARD_EVASIONS["compiled-constant-then-sub"] = (
    "import re\n"
    "RX = re.compile(r'(?<=:)[^:@]+(?=@)')\n"
    "def show(text):\n"
    "    return RX.sub('***', text)\n",
    "compiled pattern .sub() writing a mask",
)
GUARD_EVASIONS["compiled-then-subn-redacted-keyword"] = (
    "import re\n"
    "_RX = re.compile(r'(?<=:)[^:@]+(?=@)')\n"
    "MASKED = '<redacted>'\n"
    "_RX.subn(repl=MASKED, string=s)\n",
    "compiled pattern .subn() writing a mask",
)


@pytest.mark.parametrize(
    ("source", "rule"), list(GUARD_EVASIONS.values()), ids=list(GUARD_EVASIONS)
)
def test_guard_catches_the_review_2309_evasions(source: str, rule: str) -> None:
    found = private_dsn_findings(source)
    assert any(rule in finding for finding in found), found


def test_planted_compiled_sub_masker_turns_the_ratchet_red(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-review of PR 2309, S4: the compile-then-sub idiom fails the ratchet."""
    module = __import__(__name__, fromlist=["_tree_findings"])
    seeded = dict(_tree_findings())
    name = "src/okg/substrate/planted_compiled_masker.py"
    seeded[name] = private_dsn_findings(GUARD_EVASIONS["compiled-constant-then-sub"][0])
    assert seeded[name], "the planted compile-then-sub masker was not found"
    monkeypatch.setattr(module, "_tree_findings", lambda: seeded)
    with pytest.raises(AssertionError, match="planted_compiled_masker.py"):
        test_no_new_private_dsn_handling()


def test_planted_evasion_file_turns_the_ratchet_red(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A file using only the evasions (constant pattern, urlsplit().password)
    fails the ratchet itself, not just the unit rule."""
    module = __import__(__name__, fromlist=["_tree_findings"])
    seeded = dict(_tree_findings())
    seeded["src/okg/substrate/planted_masker.py"] = private_dsn_findings(
        "import re\n"
        "from urllib.parse import urlsplit\n"
        "_PW = r'pass' + 'word=([^ ]+)'\n"
        "def show(url_value):\n"
        "    text = re.sub(_PW, 'password=x', url_value)\n"
        "    return text.replace(urlsplit(text).password or '', 'x')\n"
    )
    assert len(seeded["src/okg/substrate/planted_masker.py"]) == 2
    monkeypatch.setattr(module, "_tree_findings", lambda: seeded)
    with pytest.raises(AssertionError, match="planted_masker.py"):
        test_no_new_private_dsn_handling()


@pytest.mark.parametrize(
    "source",
    [
        "from okg.substrate.db import dsn\ndsn.redact(value)\n",
        "from urllib.parse import urlsplit\nurlsplit(url)\n",
        "import re\nre.compile(r'^[A-Z_]+$')\n",
        "import re\nre.sub(pattern_from_elsewhere, '', text)\n",
        "import re\nRX = re.compile(r'\\s+')\nRX.sub(' ', text)\n",
        "def redact_token(value):\n    return value\n",
        # Refusing credentials in a URL is not masking a DSN.
        (
            "from urllib.parse import urlsplit\n"
            "parts = urlsplit(url)\n"
            "if parts.username or parts.password:\n    raise ValueError('x')\n"
        ),
        "import re\nPAT = r'^[a-z]+$'\nre.compile(PAT)\n",
        "def parse_manifest(value):\n    return value\n",
    ],
)
def test_clean_source_is_not_flagged(source: str) -> None:
    assert private_dsn_findings(source) == []


def test_seeded_violation_turns_the_ratchet_red(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A new file with a private masker fails the ratchet test itself."""
    module = __import__(__name__, fromlist=["_tree_findings"])
    seeded = dict(_tree_findings())
    seeded["src/okg/substrate/new_helper.py"] = private_dsn_findings(
        "import re\ndef mask(dsn):\n    return re.sub(r':[^:@]+@', ':***@', dsn)\n"
    )
    monkeypatch.setattr(module, "_tree_findings", lambda: seeded)
    with pytest.raises(AssertionError, match="new_helper.py"):
        test_no_new_private_dsn_handling()


def test_migrated_file_turns_the_stale_check_red(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing the last private copy from an allowlisted file fails until the entry goes."""
    if not ALLOWED_PRIVATE_DSN_FILES:
        pytest.skip("allowlist is empty: nothing left to migrate")
    module = __import__(__name__, fromlist=["_tree_findings"])
    migrated = sorted(ALLOWED_PRIVATE_DSN_FILES)[0]
    shrunk = {k: v for k, v in _tree_findings().items() if k != migrated}
    monkeypatch.setattr(module, "_tree_findings", lambda: shrunk)
    with pytest.raises(AssertionError, match=re.escape(migrated)):
        test_allowlist_has_no_stale_entries()
