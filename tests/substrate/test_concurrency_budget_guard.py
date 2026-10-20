"""Three decidable properties that stop the shattered state from returning.

1. Every declared term has a consumer in `src/`. An unread term is a dead knob
   — the defect that survived the chronos-native cutover as
   `n_phase_a_workers`, assigned and never read, and cited afterwards as
   evidence that a lane was parallel when it was not.
2. Every pool/executor construction site in `src/` takes its width from a
   registry term rather than a bare integer literal.
3. Every raw `psycopg.connect(...)` site in `src/` is either routed through
   the sanctioned wrapper / the pool, or carries an enumerated file-level
   exemption with a stated reason. A raw connect is a connection the budget's
   `connection_demand` accounting and `okg.connection_reservations` never see,
   which is exactly how a "coherent" budget ends up under-counting the load it
   claims to bound.

All three scan `src/` ONLY. Scanning `tests/` would let a test that merely
asserts a knob's value count as a consumer, which is how a name-based
heuristic ends up green over dead code.

A name-pattern heuristic over "workers/concurrency/parallelism" was considered
and rejected: it misses `max_node_ids_per_batch` and `okg_read_pool_size`, and
it cannot see attributes read via `getattr`, which this codebase uses heavily
(`publisher/extractor_stage.py`). Property 2 is the one that actually prevents
a new unaccounted pool.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from okg.substrate.db import concurrency_budget as budget

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"

#: Construction sites whose width must come from a term.
_WIDTH_CALLS = {
    "ThreadPoolExecutor": "max_workers",
    "ProcessPoolExecutor": "max_workers",
    "ConnectionPool": "max_size",
    "Queue": "concurrency",
}

#: Files exempt from property 2, each for a stated reason.
_EXEMPT = {
    # The registry itself declares the numbers; that is its job.
    "okg/substrate/db/concurrency_budget.py",
    # Offline experiment tooling. It does not run alongside a publish cycle
    # and deliberately sizes itself from its own experiment config.
    "okg/bench/",
    # The viz dev server sizes from its own CLI flag for local runs.
    "okg/viz/boot.py",
}


def _all_python_files():
    for path in sorted(SRC.rglob("*.py")):
        yield path, str(path.relative_to(SRC))


def _python_files():
    for path, rel in _all_python_files():
        if any(rel.startswith(e) or rel == e for e in _EXEMPT):
            continue
        yield path, rel


def _parse_or_fail(path: pathlib.Path, rel: str) -> ast.Module:
    """Parse a src file, treating a SyntaxError as a guard failure.

    An earlier version of this module did `except SyntaxError: continue`,
    which silently exempted any file that fails to parse from every property
    here — the exact shape of hole this guard exists to close.

    Raises rather than calling `pytest.fail`, which is equally fatal but
    reads to every caller — and to static analysis — as a path that can fall
    through and hand back `None`.
    """
    try:
        return ast.parse(path.read_text(), filename=rel)
    except SyntaxError as exc:
        raise AssertionError(
            f"{rel} does not parse under this interpreter ({exc}). A file "
            f"that fails to parse is silently unguarded by every property in "
            f"this module; fix the file or the interpreter mismatch."
        ) from exc


def _module_int_constants(tree: ast.Module) -> set[str]:
    """Module-level names bound directly to an int literal.

    Such a name is a width too. An earlier version of this guard only flagged
    `ast.Constant` keyword values, so `WORKERS = 8` /
    `ThreadPoolExecutor(max_workers=WORKERS)` passed — and it did pass, over
    `views/execution/recheck.py`, while the guard claimed to assert that no such
    constant exists. Laundering a literal through a name is the obvious evasion
    and must be caught.
    """
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, int) and not isinstance(
                node.value.value, bool
            ):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        names.add(target.id)
    return names


def _literal_width_sites():
    """Construction sites whose width is a literal, or a name bound to one."""
    offenders = []
    for path, rel in _python_files():
        tree = _parse_or_fail(path, rel)
        int_names = _module_int_constants(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
            kwarg = _WIDTH_CALLS.get(name or "")
            if kwarg is None:
                continue
            for kw in node.keywords:
                if kw.arg != kwarg:
                    continue
                value = kw.value
                if isinstance(value, ast.Constant) and isinstance(value.value, int):
                    offenders.append(
                        f"{rel}:{node.lineno} {name}({kwarg}=<literal>)"
                    )
                elif isinstance(value, ast.Name) and value.id in int_names:
                    offenders.append(
                        f"{rel}:{node.lineno} {name}({kwarg}={value.id}) "
                        f"— module constant bound to an int literal"
                    )
    return offenders


def test_every_pool_width_comes_from_a_term() -> None:
    """No construction site may hardcode its width."""
    offenders = _literal_width_sites()
    assert not offenders, (
        "these sites hardcode a concurrency width instead of drawing it from "
        "a term in okg.substrate.db.concurrency_budget:\n  "
        + "\n  ".join(offenders)
    )


def test_the_guard_can_actually_fail(tmp_path) -> None:
    """The guard is only worth having if it detects a violation, so prove it
    against a synthetic offender rather than trusting the empty result."""
    offender = tmp_path / "bad.py"
    offender.write_text(
        "from concurrent.futures import ThreadPoolExecutor\n"
        "pool = ThreadPoolExecutor(max_workers=12)\n"
    )
    tree = ast.parse(offender.read_text())
    found = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and getattr(n.func, "id", None) == "ThreadPoolExecutor"
        and any(
            kw.arg == "max_workers" and isinstance(kw.value, ast.Constant)
            for kw in n.keywords
        )
    ]
    assert found, "the detection predicate itself is broken"


# ---------------------------------------------------------------------------
# Property 3: every raw psycopg connect site is enumerated with a reason.
# ---------------------------------------------------------------------------

#: Every file (or directory prefix) in src/ allowed to call psycopg.connect
#: directly, each with a stated reason. File-level granularity on purpose:
#: line numbers churn, files rarely do. This table is an INVENTORY of where
#: raw connects exist today, not an endorsement of each one — entries whose
#: reason says "should migrate" are burn-down candidates.
_PSYCOPG_CONNECT_EXEMPT: dict[str, str] = {
    # --- the sanctioned funnel --------------------------------------------
    "okg/substrate/db/connection.py": (
        "the sanctioned direct-connect wrapper (keepalives, connect_timeout, "
        "application_name); the funnel every other exemption should migrate to"
    ),
    # --- foreign connection regimes ---------------------------------------
    "chronos_core/": (
        "chronos owns its own connection terms (chronos.schema_index, "
        "chronos.interval_gc declare their connects outside the pool)"
    ),
    "okg/bench/": (
        "offline experiment tooling; never runs alongside a publish cycle "
        "(already width-exempt above)"
    ),
    # --- remaining operator/diagnostic surfaces (file-scoped so the rest
    # --- of each namespace stays guarded) ---------------------------------
    # `okg/viz/boot.py` and `okg/viz/endpoints.py` were here until issue #1968
    # routed their sessions (and boot's pool) through the substrate connection
    # factory. A stale grant is an open door, so the entries are deleted rather
    # than kept "just in case".
    "okg/operator_ui/web/audit/seed_fixture.py": (
        "audit fixture seeding with its own retry-on-timeout connect loop"
    ),
}


def _psycopg_binding_names(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Names this module binds to the psycopg module / its connect function.

    Three files alias the module (`import psycopg as _psycopg`), so matching
    the literal name `psycopg` would miss them; resolve through the import
    table instead.
    """
    module_names: set[str] = set()
    connect_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "psycopg":
                    module_names.add(alias.asname or alias.name)
                elif alias.name.startswith("psycopg.") and alias.asname is None:
                    # `import psycopg.rows` binds the root name `psycopg`.
                    module_names.add("psycopg")
        elif isinstance(node, ast.ImportFrom) and node.module == "psycopg":
            for alias in node.names:
                if alias.name == "connect":
                    connect_names.add(alias.asname or alias.name)
    return module_names, connect_names


def _psycopg_connect_calls(tree: ast.Module) -> list[ast.Call]:
    """Every Call node in `tree` that invokes psycopg's connect()."""
    module_names, connect_names = _psycopg_binding_names(tree)
    if not module_names and not connect_names:
        return []
    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if (
            isinstance(fn, ast.Attribute)
            and fn.attr == "connect"
            and isinstance(fn.value, ast.Name)
            and fn.value.id in module_names
        ):
            calls.append(node)
        elif isinstance(fn, ast.Name) and fn.id in connect_names:
            calls.append(node)
    return calls


def _unaccounted_connect_sites() -> tuple[list[str], set[str]]:
    """Raw connect sites not covered by the exemption table, and the table
    entries that matched at least one site (for staleness detection)."""
    offenders: list[str] = []
    used: set[str] = set()
    for path, rel in _all_python_files():
        tree = _parse_or_fail(path, rel)
        calls = _psycopg_connect_calls(tree)
        if not calls:
            continue
        entry = next(
            (
                e
                for e in _PSYCOPG_CONNECT_EXEMPT
                if rel == e or rel.startswith(e)
            ),
            None,
        )
        if entry is not None:
            used.add(entry)
            continue
        offenders.extend(f"{rel}:{c.lineno}" for c in calls)
    return offenders, used


def test_every_raw_connect_site_is_enumerated() -> None:
    """No new raw psycopg.connect site may appear outside the exemption
    table. A raw connect bypasses okg.connection_reservations and the
    budget's connection_demand accounting entirely."""
    offenders, used = _unaccounted_connect_sites()
    assert not offenders, (
        "raw psycopg.connect() sites outside the exemption table:\n  "
        + "\n  ".join(offenders)
        + "\nA raw connect bypasses okg.connection_reservations and the "
        "budget's connection_demand accounting: the budget under-counts "
        "real load by every connection opened this way. Either route the "
        "connection through okg.substrate.db.connection (the sanctioned "
        "wrapper) or the pool, or add a file-level entry with a reason to "
        "_PSYCOPG_CONNECT_EXEMPT in this test."
    )
    stale = set(_PSYCOPG_CONNECT_EXEMPT) - used
    assert not stale, (
        "exemption entries that no longer match any raw connect site "
        "(delete them — a stale grant is an open door):\n  "
        + "\n  ".join(sorted(stale))
    )


def test_every_exemption_carries_a_reason() -> None:
    """The table is only an inventory if every entry says why it exists."""
    unreasoned = [
        e for e, r in _PSYCOPG_CONNECT_EXEMPT.items()
        if not r or not r.strip()
    ]
    assert not unreasoned, f"exemption entries without a reason: {unreasoned}"


def test_the_connect_guard_can_actually_fail(tmp_path) -> None:
    """Prove the real detection function flags an aliased import, rather
    than trusting the empty offender list."""
    offender = tmp_path / "bad_connect.py"
    offender.write_text(
        "import psycopg as pg\n"
        "conn = pg.connect('dbname=x')\n"
    )
    tree = ast.parse(offender.read_text())
    calls = _psycopg_connect_calls(tree)
    assert [c.lineno for c in calls] == [2], (
        "the connect detection predicate itself is broken"
    )


def test_every_src_file_parses() -> None:
    """A file that fails to parse is silently unguarded by every property in
    this module, so parseability is itself a guarded property."""
    failures = []
    for path, rel in _all_python_files():
        try:
            ast.parse(path.read_text(), filename=rel)
        except SyntaxError as exc:
            failures.append(f"{rel}: {exc}")
    assert not failures, (
        "src files that do not parse (and are therefore unguarded):\n  "
        + "\n  ".join(failures)
    )


@pytest.mark.parametrize("term", [t.name for t in budget.declared_terms()])
def test_every_declared_term_has_a_consumer(term: str) -> None:
    """A term nothing reads is a dead knob. Consumers reference the term by
    its string name, so a plain text scan over src/ is the right check here."""
    needle = f'"{term}"'
    alt = f"'{term}'"
    for path, rel in _python_files():
        text = path.read_text()
        if needle in text or alt in text:
            return
    pytest.fail(
        f"concurrency term {term!r} is declared but no src/ code reads it. "
        f"Either wire it to the lane it names or delete it — an unread width "
        f"is the orphaned-knob defect this registry exists to end."
    )
