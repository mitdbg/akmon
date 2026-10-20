"""No test may fall back to a database DSN of its own (issue #1913).

The defect this holds shut: a test reads `OKG_TEST_DSN`, and when it is
unset it uses a DSN of its own instead of skipping. Every such fallback in
this repository named the shared `okg-pg` Compose service on 127.0.0.1:5433,
and the callers behind them TRUNCATE, DROP and CREATE. On a maintainer
workstation with that service up, a forgotten variable therefore destroyed
the database another session was using and reported green.

**Why there are two scans.** The first version of this guard looked only for
a DSN literal used as the default ARGUMENT of an environment lookup. Run
against the four defective files on `dev` it caught two and missed two --
including `tests/substrate/conftest.py`, the file the issue was written
about -- because their fallback was a module constant returned at the end of
a resolver, not a default argument. A fallback has many shapes.
`_defaulted_dsn_findings` covers the ones with an explicit default;
`_escaping_dsn_findings` covers the rest by asking a different question: does
a DSN literal LEAVE a function that supplies a DSN?

The second scan is scoped to DSN surfaces -- `conftest.py` files, functions
whose name ends in `dsn`, and module constants named `*DSN*` -- with a named
allowlist, rather than to every string in the tree. Scanning every string
would name 51 files, nearly all of them passing a fixed DSN into a pure
function as test input, and a guard that cries wolf is a guard that gets an
exclusion added to it.

**What this guard does NOT catch.** It is a lint, not a proof, and reading it
as complete is how the first version stayed narrow for a whole review cycle.
Known gaps, all of which would need a reviewer rather than this file:

* a helper whose name does not end in `dsn`, outside a `conftest.py`,
  returning a DSN literal -- the scope rule never looks at it;
* a DSN assembled from parts, e.g. `make_conninfo(host=..., port=5433, ...)`
  or a settings dict `{"host": ..., "port": 5433}`, since no string literal
  in it looks like a DSN;
* a fallback that reaches the code through an import, a fixture parameter or
  a config file rather than a literal in the file being scanned.

The tree-wide behavioural runs are what actually prove the property: with
both variables cleared and `PGHOST` pointed at a nonexistent socket
directory, every affected test must skip and none may error.

Callers that need the test database call
`tests.substrate.conftest.require_okg_test_dsn`, which returns the DSN or
skips.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from tests.pytest.tracked_files import tracked_files


_TESTS_ROOT = Path(__file__).resolve().parents[1]

#: Environment variables that name the database these fixtures may destroy.
#: A default for any of them is a fallback, whatever the literal says.
_TEST_DSN_ENV_VARS = frozenset({"OKG_TEST_DSN", "OKG_TEST_DSN_TEMPLATE"})

#: The shared project instance. A default pointing here is a fallback even
#: when the variable has some other name -- that is how
#: `ISSUE1305_LIFECYCLE_ADMIN_DSN` reached it.
_SHARED_INSTANCE_MARKER = ":5433/"

#: A DSN literal naming the shared instance or a test database.
_DSN_LITERAL = re.compile(r"postgres(?:ql)?://[^\s\"']*(?::5433/|/okg_test)")

#: Deliberate, reviewed exceptions to the default-argument scan. Each is a
#: LIVE-deployment fixture already gated behind an explicit opt-in
#: (`OKG_RUN_LIVE_DEPLOYMENT_TESTS=1`) that reads rather than writes, so an
#: unset test DSN still skips it.
_ALLOWED_DEFAULTS: frozenset[tuple[str, str]] = frozenset(
    {
        ("tests/views/conftest.py", "OKG_DSN"),
    }
)

#: Deliberate, reviewed exceptions to the escaping-literal scan: named
#: constants that are test INPUT to a pure function, never a DSN anything
#: connects with. Listed one by one so adding another is a visible edit.
#:
#: Keyed on ``(path, the literal's own text)``, NOT on a line number
#: (issue #1971). The first version keyed on ``(path, line)``, so inserting
#: any line above an allowlisted literal moved it, the entry stopped
#: matching, and the guard reddened a pull request that changed nothing
#: relevant -- with the natural repair under time pressure being to widen
#: the guard rather than re-key the entry. The literal's text moves with the
#: literal, and it pins the exact DSN allowed rather than a position.
_ALLOWED_ESCAPING: frozenset[tuple[str, str]] = frozenset(
    {
        # A documentation literal the quickstart doc test compares against.
        (
            "tests/e2e/quickstart_extract.py",
            "postgresql://postgres:okg@127.0.0.1:5433/<db-name>",
        ),
        # The DSN docs/operate/install.md prints in step 3. The install
        # journey matches it on the page and replaces it with the lane's
        # own database before running anything; nothing connects to it.
        (
            "tests/e2e/install_journey.py",
            "postgresql://postgres:okg@127.0.0.1:5433/okg",
        ),
        # Parser fixtures for the schema-destructive DSN helpers.
        (
            "tests/substrate/test_schema_destructive_dsn.py",
            "postgresql://okg:secret@127.0.0.1:5433/okg_test_gw1",
        ),
        (
            "tests/substrate/test_schema_destructive_dsn.py",
            "postgresql://u:p@h1:5433,h2:5434/okg_test",
        ),
        # A stub DSN passed to an agent-runtime argument builder.
        (
            "tests/substrate/library/agent_runtimes/test_agent_runtimes.py",
            "postgres://postgres:okg@localhost:32773/okg_test",
        ),
        # live_okg_dsn: opt-in, read-only against an operator's own graph.
        (
            "tests/views/conftest.py",
            "postgres://postgres:okg@localhost:5433/okg",
        ),
    }
)


def _is_dsn_literal(node: ast.AST) -> bool:
    """A string, or an f-string, that spells out a shared-instance DSN."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return bool(_DSN_LITERAL.search(node.value))
    if isinstance(node, ast.JoinedStr):
        # f"postgres://{host}:{port}/okg_test" has no matchable literal, so
        # judge it on the fragments the author DID write.
        text = "".join(
            part.value
            for part in node.values
            if isinstance(part, ast.Constant) and isinstance(part.value, str)
        )
        return "postgres://" in text and ("okg_test" in text or ":5433" in text)
    return False


def _dsn_literal_text(node: ast.AST) -> str | None:
    """The DSN text the author wrote, or ``None`` if this is not one.

    This is the allowlist key: it travels with the literal through every
    edit above it, and it names the exact DSN a reviewer signed off on.
    """
    if not _is_dsn_literal(node):
        return None
    if isinstance(node, ast.Constant):
        return str(node.value)
    if isinstance(node, ast.JoinedStr):
        return "".join(
            part.value
            for part in node.values
            if isinstance(part, ast.Constant) and isinstance(part.value, str)
        )
    return None


def _string_constants(node: ast.AST) -> str:
    return " ".join(
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    )


def _module_constants(tree: ast.Module) -> dict[str, ast.expr]:
    """Module-level ``NAME = <value>`` bindings, so a Name default resolves."""
    constants: dict[str, ast.expr] = {}
    for statement in tree.body:
        if isinstance(statement, ast.Assign):
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = statement.value
        elif isinstance(statement, ast.AnnAssign):
            if isinstance(statement.target, ast.Name) and statement.value:
                constants[statement.target.id] = statement.value
    return constants


def _env_var_name(node: ast.AST) -> str | None:
    """The variable name in an ``os.environ.get(NAME, ...)``-style lookup."""
    if isinstance(node, ast.Call) and node.args:
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            return first.value
    if isinstance(node, ast.Subscript):
        index = node.slice
        if isinstance(index, ast.Constant) and isinstance(index.value, str):
            return index.value
    return None


def _is_env_lookup(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"get", "getenv"}
    )


def _is_env_setdefault(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "setdefault"
    )


def _defaulted_dsn_findings(source: str, rel: str) -> list[str]:
    """Every environment lookup whose default is a DSN."""
    findings: list[str] = []
    tree = ast.parse(source, filename=rel)
    constants = _module_constants(tree)

    def _resolve(node: ast.expr) -> ast.expr:
        """A Name default resolves to its module-level constant."""
        if isinstance(node, ast.Name) and node.id in constants:
            return constants[node.id]
        return node

    def _report(lookup: ast.AST, default: ast.expr, lineno: int) -> None:
        resolved = _resolve(default)
        name = _env_var_name(lookup)
        literal = _string_constants(resolved)
        if "postgres://" not in literal and "postgresql://" not in literal:
            return
        offending = (
            name in _TEST_DSN_ENV_VARS or _SHARED_INSTANCE_MARKER in literal
        )
        if not offending or (rel, name) in _ALLOWED_DEFAULTS:
            return
        findings.append(
            f"{rel}:{lineno} falls back to a database DSN when {name!r} is "
            "unset; call tests.substrate.conftest.require_okg_test_dsn instead"
        )

    for node in ast.walk(tree):
        if _is_env_setdefault(node) and len(node.args) == 2:
            if _env_var_name(node) in _TEST_DSN_ENV_VARS:
                findings.append(
                    f"{rel}:{node.lineno} calls setdefault on "
                    f"{_env_var_name(node)!r}; a test must not invent the "
                    "database it is about to destroy"
                )
        elif _is_env_lookup(node) and len(node.args) == 2:
            _report(node, node.args[1], node.lineno)
        elif isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            head = node.values[0]
            if _is_env_lookup(head) or isinstance(head, ast.Subscript):
                for fallback in node.values[1:]:
                    _report(head, fallback, node.lineno)
    return findings


class _EscapingVisitor(ast.NodeVisitor):
    """Find DSN literals that LEAVE a DSN-supplying scope.

    This is the half that catches a fallback written as a module constant, a
    bare return, a ``try/except KeyError`` rebind, an ``if not dsn:`` rebind
    or a ternary -- none of which is a default argument.
    """

    def __init__(self, rel: str, constants: dict[str, ast.expr]) -> None:
        self.rel = rel
        self.constants = constants
        self.is_conftest = Path(rel).name == "conftest.py"
        self.stack: list[str] = []
        self.findings: list[str] = []
        #: Allowlisted literals actually met in this source, so a stale
        #: entry cannot sit in the allowlist unnoticed.
        self.suppressed: list[tuple[str, str]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def _supplies_a_dsn(self, names: list[str]) -> bool:
        if self.stack and self.stack[-1].startswith("test_"):
            # A literal bound inside a test case is input to that case.
            return False
        if self.stack:
            if self.is_conftest:
                return True
            return any(
                function.lower().endswith("dsn")
                or ("dsn" in function.lower() and "resolve" in function.lower())
                for function in self.stack
            )
        return self.is_conftest or any("DSN" in name for name in names)

    def _resolve(self, node: ast.expr) -> ast.expr:
        """A returned Name resolves to its module-level constant.

        `return DEFAULT_TEST_DSN` is the original defect. Following the name
        catches it after someone renames the constant or moves the resolver
        out of a `conftest.py`.
        """
        if isinstance(node, ast.Name) and node.id in self.constants:
            return self.constants[node.id]
        return node

    def _check(self, value: ast.expr, lineno: int, names: list[str]) -> None:
        candidates: list[ast.expr] = [self._resolve(value)]
        if isinstance(value, ast.BoolOp):
            candidates = [self._resolve(item) for item in value.values]
        elif isinstance(value, ast.IfExp):
            candidates = [
                self._resolve(value.body), self._resolve(value.orelse)
            ]
        texts = [
            text
            for text in (_dsn_literal_text(item) for item in candidates)
            if text is not None
        ]
        if not texts:
            return
        if not self._supplies_a_dsn(names):
            return
        offending = [
            text for text in texts if (self.rel, text) not in _ALLOWED_ESCAPING
        ]
        if not offending:
            self.suppressed.extend((self.rel, text) for text in texts)
            return
        where = ".".join(self.stack) or "<module>"
        self.findings.append(
            f"{self.rel}:{lineno} {where} supplies a hardcoded database DSN "
            f"({offending[0]!r}); call "
            "tests.substrate.conftest.require_okg_test_dsn instead"
        )

    def visit_Return(self, node: ast.Return) -> None:
        if node.value is not None:
            self._check(node.value, node.lineno, [])
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        self._check(node.value, node.lineno, names)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """`DSN: str = "postgres://..."` is the same binding with a type."""
        if node.value is not None:
            names = (
                [node.target.id] if isinstance(node.target, ast.Name) else []
            )
            self._check(node.value, node.lineno, names)
        self.generic_visit(node)


def _scan_escaping(source: str, rel: str) -> _EscapingVisitor:
    tree = ast.parse(source, filename=rel)
    visitor = _EscapingVisitor(rel, _module_constants(tree))
    visitor.visit(tree)
    return visitor


def _escaping_dsn_findings(source: str, rel: str) -> list[str]:
    return _scan_escaping(source, rel).findings


def _escaping_allowlist_hits(source: str, rel: str) -> list[tuple[str, str]]:
    """Which allowlist entries this source actually uses."""
    return _scan_escaping(source, rel).suppressed


def _dsn_fallback_findings(source: str, rel: str) -> list[str]:
    return _defaulted_dsn_findings(source, rel) + _escaping_dsn_findings(
        source, rel
    )


# --------------------------------------------------------------------------
# The shapes a fallback can take. Nine of these were missed by the
# default-argument scan alone; each is written the way real code writes it.
# --------------------------------------------------------------------------
_SHARED = "postgres://postgres:okg@127.0.0.1:5433/okg_test"

_FALLBACK_SHAPES: dict[str, str] = {
    "default-argument": (
        "import os\n"
        "def _dsn():\n"
        f'    return os.environ.get("OKG_TEST_DSN", "{_SHARED}")\n'
    ),
    "getenv-default": (
        "import os\n"
        "def _dsn():\n"
        f'    return os.getenv("OKG_TEST_DSN", "{_SHARED}")\n'
    ),
    "or-literal": (
        "import os\n"
        "def _dsn():\n"
        f'    return os.environ.get("OKG_TEST_DSN") or "{_SHARED}"\n'
    ),
    "nested-default": (
        "import os\n"
        "def _dsn():\n"
        '    return os.environ.get(\n'
        '        "OKG_TEST_DSN",\n'
        f'        os.environ.get("OKG_DSN", "{_SHARED}"),\n'
        "    )\n"
    ),
    # --- the nine the default-argument scan alone did not see ---
    "trailing-literal-return": (
        "import os\n"
        "def _dsn():\n"
        '    if explicit := os.environ.get("OKG_TEST_DSN"):\n'
        "        return explicit\n"
        f'    return "{_SHARED}"\n'
    ),
    "module-constant-return": (
        "import os\n"
        f'DEFAULT_TEST_DSN = "{_SHARED}"\n'
        "def _dsn():\n"
        '    if explicit := os.environ.get("OKG_TEST_DSN"):\n'
        "        return explicit\n"
        "    return DEFAULT_TEST_DSN\n"
    ),
    "module-constant-default-argument": (
        "import os\n"
        f'DEFAULT_TEST_DSN = "{_SHARED}"\n'
        "def _dsn():\n"
        '    return os.environ.get("OKG_TEST_DSN", DEFAULT_TEST_DSN)\n'
    ),
    "try-except-keyerror": (
        "import os\n"
        "def _dsn():\n"
        "    try:\n"
        '        return os.environ["OKG_TEST_DSN"]\n'
        "    except KeyError:\n"
        f'        dsn = "{_SHARED}"\n'
        "    return dsn\n"
    ),
    "if-not-rebind": (
        "import os\n"
        "def _dsn():\n"
        '    dsn = os.environ.get("OKG_TEST_DSN")\n'
        "    if not dsn:\n"
        f'        dsn = "{_SHARED}"\n'
        "    return dsn\n"
    ),
    "ternary": (
        "import os\n"
        "def _dsn():\n"
        '    env = os.environ.get("OKG_TEST_DSN")\n'
        f'    dsn = env if env else "{_SHARED}"\n'
        "    return dsn\n"
    ),
    "module-constant-only": f'TEST_DSN = "{_SHARED}"\n',
    "fstring-from-parts": (
        "import os\n"
        "def _dsn():\n"
        '    host = os.environ.get("PGHOST", "127.0.0.1")\n'
        '    port = os.environ.get("PGPORT", "5433")\n'
        '    return f"postgres://postgres:okg@{host}:{port}/okg_test"\n'
    ),
    "annotated-module-constant": (
        f'TEST_DSN: str = "{_SHARED}"\n'
    ),
    "renamed-constant-outside-a-conftest": (
        "import os\n"
        f'_FALLBACK = "{_SHARED}"\n'
        "def resolve_test_dsn():\n"
        '    if explicit := os.environ.get("OKG_TEST_DSN"):\n'
        "        return explicit\n"
        "    return _FALLBACK\n"
    ),
    "environ-setdefault": (
        "import os\n"
        f'os.environ.setdefault("OKG_TEST_DSN", "{_SHARED}")\n'
    ),
}


#: Shapes that must be caught even in an ordinary test module, not only in a
#: `conftest.py`. The rest are scanned as conftest sources, which is where a
#: resolver normally lives.
_NON_CONFTEST_SHAPES = frozenset({"renamed-constant-outside-a-conftest"})


@pytest.mark.parametrize("shape", sorted(_FALLBACK_SHAPES))
def test_the_guard_detects_every_fallback_shape(shape: str) -> None:
    rel = (
        f"tests/synthetic/{shape}/test_module.py"
        if shape in _NON_CONFTEST_SHAPES
        else f"tests/synthetic/{shape}/conftest.py"
    )

    findings = _dsn_fallback_findings(_FALLBACK_SHAPES[shape], rel)

    assert findings, f"{shape} slipped through the guard"


def test_the_guard_accepts_the_required_resolver() -> None:
    findings = _dsn_fallback_findings(
        "from tests.substrate.conftest import require_okg_test_dsn\n"
        "def _dsn():\n"
        "    return require_okg_test_dsn()\n",
        "tests/synthetic/required/conftest.py",
    )

    assert findings == []


def test_the_guard_leaves_a_test_case_input_alone() -> None:
    """A fixed DSN passed into a pure function is not a fallback."""
    findings = _dsn_fallback_findings(
        "def test_parses_a_url_dsn():\n"
        f'    dsn = "{_SHARED}"\n'
        '    assert dsn.startswith("postgres://")\n',
        "tests/synthetic/input/test_parser.py",
    )

    assert findings == []


#: The two fallbacks on `dev` that the default-argument scan alone missed,
#: verbatim. Kept as source rather than read from git so the regression
#: survives a shallow CI checkout.
_DEV_MISSED_FALLBACKS = {
    "tests/substrate/conftest.py": (
        "import os\n"
        "DEFAULT_TEST_DSN = (\n"
        '    "postgres://postgres:okg@127.0.0.1:5433/okg_test"\n'
        ")\n"
        "def _resolve_okg_test_dsn() -> str:\n"
        '    if explicit := os.environ.get("OKG_TEST_DSN"):\n'
        "        return explicit\n"
        '    if template := os.environ.get("OKG_TEST_DSN_TEMPLATE"):\n'
        '        workerid = os.environ.get("PYTEST_XDIST_WORKER", "master")\n'
        "        return template.format(workerid=workerid)\n"
        "    return DEFAULT_TEST_DSN\n"
    ),
    "tests/pact/conftest.py": (
        "import os\n"
        "def _resolve_test_dsn() -> str:\n"
        '    if explicit := os.environ.get("OKG_TEST_DSN"):\n'
        "        return explicit\n"
        '    if template := os.environ.get("OKG_TEST_DSN_TEMPLATE"):\n'
        '        workerid = os.environ.get("PYTEST_XDIST_WORKER", "master")\n'
        "        return template.format(workerid=workerid)\n"
        '    return "postgres://postgres:okg@127.0.0.1:5433/okg_test"\n'
    ),
}


@pytest.mark.parametrize("rel", sorted(_DEV_MISSED_FALLBACKS))
def test_the_guard_catches_the_original_defect(rel: str) -> None:
    """The regression: these are the files issue #1913 was written about."""
    findings = _dsn_fallback_findings(_DEV_MISSED_FALLBACKS[rel], rel)

    assert findings, f"{rel} is the original defect and must be caught"


#: The files the escaping-literal allowlist names, read from the allowlist
#: itself so this can never drift from it.
_ALLOWLISTED_PATHS = sorted({rel for rel, _ in _ALLOWED_ESCAPING})


@pytest.mark.parametrize("rel", _ALLOWLISTED_PATHS)
def test_an_allowlisted_literal_survives_a_line_insertion(rel: str) -> None:
    """Issue #1971: an edit ABOVE an allowlisted literal must not red it.

    Run against the line-keyed allowlist this replaced, three comment lines
    inserted at the top of `test_schema_destructive_dsn.py` reported both of
    its allowlisted literals as fallbacks.
    """
    path = _TESTS_ROOT.parent / rel
    source = path.read_text(encoding="utf-8")

    assert _dsn_fallback_findings(source, rel) == []

    shifted = "# an unrelated line inserted above the literal\n" * 3 + source

    assert _dsn_fallback_findings(shifted, rel) == []


@pytest.mark.parametrize("rel", _ALLOWLISTED_PATHS)
def test_a_new_literal_in_an_allowlisted_file_is_still_caught(
    rel: str,
) -> None:
    """The allowlist covers named literals, not whole files."""
    path = _TESTS_ROOT.parent / rel
    source = path.read_text(encoding="utf-8")
    added = f'\nUNREVIEWED_DSN = "{_SHARED}"\n'

    findings = _dsn_fallback_findings(source + added, rel)

    assert findings, f"a new DSN literal in {rel} slipped through"
    assert _SHARED in findings[0], findings


def test_every_allowlist_entry_is_live() -> None:
    """No entry may name a literal that no longer exists.

    A stale entry is an exception nobody can see the subject of, and under
    the old line keying it was also how a moved literal quietly kept some
    OTHER literal allowed.
    """
    hits: set[tuple[str, str]] = set()

    for path in tracked_files(_TESTS_ROOT, "*.py"):
        rel = path.relative_to(_TESTS_ROOT.parent).as_posix()
        source = path.read_text(encoding="utf-8")
        hits.update(_escaping_allowlist_hits(source, rel))

    assert hits == set(_ALLOWED_ESCAPING)


def test_no_test_falls_back_to_a_database_dsn() -> None:
    """The whole tree, not a list of files."""
    findings: list[str] = []
    scanned = 0

    for path in tracked_files(_TESTS_ROOT, "*.py"):
        rel = path.relative_to(_TESTS_ROOT.parent).as_posix()
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:  # pragma: no cover
            pytest.fail(f"{rel} is unreadable: {exc}")
        scanned += 1
        findings.extend(_dsn_fallback_findings(source, rel))

    assert scanned > 100, f"scanned only {scanned} files; the walk is broken"
    assert findings == [], "\n".join(findings)
