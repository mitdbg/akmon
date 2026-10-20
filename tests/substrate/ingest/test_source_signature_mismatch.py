"""Proving — and recovering from — a stale source adapter (issue #945).

The incident: ``CooperAliasSource.__init__`` gained a ``dsn`` parameter
84 minutes after the worker booted. The worker held the old class, so
every cycle raised ``TypeError: ... got an unexpected keyword argument
'dsn'`` — 177 times over ~36 hours — against a file on disk that
accepted ``dsn`` the whole time.

These tests build that exact situation with real files: write a class,
import it, rewrite the file so the definitions differ, and check that
the substrate proves the difference (rather than inferring it from a
clock) and re-imports rather than failing the cycle.
"""
from __future__ import annotations

import importlib
import os
import sys
import textwrap
import time
from pathlib import Path

import pytest

from okg.substrate.ingest import source_construction as sc


@pytest.fixture
def module_dir(tmp_path: Path):
    root = tmp_path / "adapters"
    root.mkdir()
    sys.path.insert(0, str(root))
    created: list[str] = []

    def make(name: str, body: str) -> str:
        (root / f"{name}.py").write_text(
            textwrap.dedent(body), encoding="utf-8",
        )
        created.append(name)
        return name

    def rewrite(
        name: str,
        body: str,
        *,
        mtime_ns: int | None = None,
    ) -> None:
        path = root / f"{name}.py"
        path.write_text(
            textwrap.dedent(body), encoding="utf-8",
        )
        # Set an explicit nanosecond identity and invalidate the finder cache
        # without relying on a sleep. Tests that assert import-time ordering
        # supply their observed import time below.
        if mtime_ns is None:
            mtime_ns = time.time_ns()
        os.utime(path, ns=(mtime_ns, mtime_ns))
        importlib.invalidate_caches()

    yield type("Fixture", (), {
        "root": root,
        "make": staticmethod(make),
        "rewrite": staticmethod(rewrite),
    })

    for name in created:
        sys.modules.pop(name, None)
    sys.path.remove(str(root))


_WITHOUT_DSN = """
    class AliasSource:
        def __init__(self, source_name=None):
            self.source_name = source_name
"""

_WITH_DSN = """
    class AliasSource:
        def __init__(self, source_name=None, dsn=None):
            self.source_name = source_name
            self.dsn = dsn
"""

_WITHOUT_DSN_SAME_IDENTITY = """
    class AliasSource:
        def __init__(self, source_name=None, unused=None):
            self.source_name = source_name
            self.unused = unused
"""

_WITH_DSN_SAME_IDENTITY = """
    class AliasSource:
        def __init__(self, source_name=None, dsn=None   ):
            self.source_name = source_name
            self.dsn = dsn  # xx
"""


def _construct(module_name: str, **params):
    module = sys.modules[module_name]
    return module.AliasSource(**params)


def test_stale_definition_is_proven_by_comparison(module_dir) -> None:
    """The loaded class rejects `dsn`; the file accepts it. That is proof.

    No timestamps are consulted: the two definitions are read and
    compared, so the verdict cannot invert on an ordinary edit-then-
    import loop or on a deploy that preserves mtimes.
    """
    name = module_dir.make("alias_stale", _WITHOUT_DSN)
    __import__(name)
    # The incident: the file gains the parameter; the loaded class does not.
    module_dir.rewrite(name, _WITH_DSN)

    with pytest.raises(TypeError) as raised:
        _construct(name, dsn="postgresql:///x")

    proof = sc.prove_stale_definition(
        module_name=name,
        class_name="AliasSource",
        arguments=["dsn"],
        direction="adapter_rejected_argument",
    )
    assert proof["status"] == "stale"
    assert proof["basis"] == "definition_comparison"
    assert "dsn" in proof["on_disk_parameters"]
    assert "dsn" not in proof["loaded_parameters"]

    classified = sc.classify_source_construction_failure(
        exc=raised.value,
        stage="construct",
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "postgresql:///x"},
    )
    assert classified["error_kind"] == sc.SOURCE_ADAPTER_SIGNATURE_MISMATCH
    assert classified["cause"] == "stale_loaded_code"
    assert "restart the runtime worker" in classified["message"]


def test_genuine_mismatch_is_not_called_staleness(module_dir) -> None:
    """The file rejects `dsn` too — that is a real disagreement."""
    name = module_dir.make("alias_genuine", _WITHOUT_DSN)
    __import__(name)

    with pytest.raises(TypeError) as raised:
        _construct(name, dsn="postgresql:///x")

    proof = sc.prove_stale_definition(
        module_name=name,
        class_name="AliasSource",
        arguments=["dsn"],
        direction="adapter_rejected_argument",
    )
    assert proof["status"] == "current"

    classified = sc.classify_source_construction_failure(
        exc=raised.value,
        stage="construct",
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "x"},
    )
    assert classified["cause"] == "registry_adapter_mismatch"
    assert "source registry" in classified["message"]
    assert "restart" not in classified["message"]


def test_uninspectable_definition_is_undetermined(module_dir) -> None:
    """A class this cannot statically compare reports undetermined.

    It must name both possibilities rather than defaulting to either —
    a wrong confident answer is what cost 36 hours in #945.
    """
    name = module_dir.make("alias_factory", """
        def _make():
            class AliasSource:
                def __init__(self, source_name=None):
                    self.source_name = source_name
            return AliasSource

        AliasSource = _make()
    """)
    __import__(name)

    with pytest.raises(TypeError) as raised:
        _construct(name, dsn="x")

    classified = sc.classify_source_construction_failure(
        exc=raised.value,
        stage="construct",
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "x"},
    )
    assert classified["cause"] == "undetermined"
    assert "undetermined" in classified["message"]
    # Both causes named, neither asserted.
    assert "stale code" in classified["message"]
    assert "genuinely disagree" in classified["message"]


def test_proven_stale_module_is_reimported_and_retried(module_dir) -> None:
    """Recovery: the 177 failures become zero without an operator."""
    name = module_dir.make("alias_recover", _WITHOUT_DSN)
    __import__(name)
    module_dir.rewrite(name, _WITH_DSN)

    with pytest.raises(TypeError):
        _construct(name, dsn="postgresql:///x")

    recovery = sc.retry_construction_after_eviction(
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "postgresql:///x"},
    )
    assert recovery["recovered"] is True
    assert recovery["adapter"].dsn == "postgresql:///x"
    assert name in recovery["evicted_modules"]


def test_recovered_module_is_not_reported_stale_forever(module_dir) -> None:
    """The self-heal must not permanently falsify the detection half.

    The import observer stamps a module's FIRST import and never
    overwrites, so a re-imported module would otherwise keep the stamp
    of the copy that was thrown away and be reported
    loaded-then-modified for the rest of the process's life.
    """
    from okg.substrate.runtime import code_identity

    code_identity.clear_worker_code_identity()
    try:
        code_identity.record_worker_code_identity(
            deployment="d", extra_roots=[module_dir.root],
        )
        name = module_dir.make("alias_stamp", _WITHOUT_DSN)
        __import__(name)
        imported_at = code_identity.module_import_time(name)
        assert imported_at is not None

        loaded_at = code_identity.module_import_time(name)
        assert loaded_at is not None
        module_dir.rewrite(
            name,
            _WITH_DSN,
            mtime_ns=max(time.time_ns(), int(loaded_at * 1_000_000_000) + 1),
        )
        report = code_identity.worker_code_identity_report()
        assert name in {
            row["module"] for row in report["modified_modules"]
        }

        recovery = sc.retry_construction_after_eviction(
            module_name=name,
            class_name="AliasSource",
            params={"dsn": "x"},
        )
        assert recovery["recovered"] is True

        report = code_identity.worker_code_identity_report()
        assert name not in {
            row["module"] for row in report["modified_modules"]
        }
    finally:
        code_identity.clear_worker_code_identity()


def test_recovery_reimports_same_size_same_mtime_source(module_dir) -> None:
    """A proven stale adapter must not be resurrected from its old `.pyc`."""
    assert len(_WITHOUT_DSN_SAME_IDENTITY) == len(_WITH_DSN_SAME_IDENTITY)
    for attempt in range(10):
        name = module_dir.make(
            f"alias_same_identity_{attempt}", _WITHOUT_DSN_SAME_IDENTITY,
        )
        __import__(name)
        source = module_dir.root / f"{name}.py"
        original_mtime_ns = source.stat().st_mtime_ns

        module_dir.rewrite(
            name,
            _WITH_DSN_SAME_IDENTITY,
            mtime_ns=original_mtime_ns,
        )

        # This is the old recovery path: finder-cache invalidation plus a
        # normal re-import. Timestamp-and-size validation accepts old bytecode.
        sys.modules.pop(name)
        importlib.invalidate_caches()
        stale_reimport = importlib.import_module(name)
        assert not hasattr(stale_reimport.AliasSource(), "dsn")

        proof = sc.prove_stale_definition(
            module_name=name,
            class_name="AliasSource",
            arguments=["dsn"],
            direction="adapter_rejected_argument",
        )
        assert proof["status"] == "stale"

        recovery = sc.retry_construction_after_eviction(
            module_name=name,
            class_name="AliasSource",
            params={"dsn": "postgresql:///x"},
        )
        assert recovery["recovered"] is True, recovery.get("reason")
        assert recovery["adapter"].dsn == "postgresql:///x"


def test_recovery_derives_bytecode_cache_from_source_not_module_metadata(
    module_dir,
    monkeypatch,
) -> None:
    """Module metadata cannot redirect recovery deletion outside its source cache."""
    deleted: list[Path] = []
    original_unlink = Path.unlink

    def record_unlink(path: Path, *args, **kwargs):
        deleted.append(path)
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", record_unlink)
    for redirect_kind in ("direct", "symlink"):
        name = module_dir.make(
            f"alias_cache_redirect_{redirect_kind}", _WITHOUT_DSN_SAME_IDENTITY,
        )
        original = importlib.import_module(name)
        source = module_dir.root / f"{name}.py"
        real_cache = Path(importlib.util.cache_from_source(str(source)))
        assert real_cache.is_file()
        sentinel = module_dir.root / f"must-not-delete-{redirect_kind}.pyc"
        sentinel.write_text("sentinel", encoding="utf-8")
        redirect = sentinel
        if redirect_kind == "symlink":
            redirect = module_dir.root / "cache-alias.pyc"
            redirect.symlink_to(sentinel)
        original.__cached__ = str(redirect)
        original_mtime_ns = source.stat().st_mtime_ns
        module_dir.rewrite(
            name,
            _WITH_DSN_SAME_IDENTITY,
            mtime_ns=original_mtime_ns,
        )

        recovery = sc.retry_construction_after_eviction(
            module_name=name,
            class_name="AliasSource",
            params={"dsn": "postgresql:///x"},
        )

        assert real_cache in deleted
        assert redirect not in deleted
        assert sentinel.read_text(encoding="utf-8") == "sentinel"
        assert recovery["recovered"] is True, recovery.get("reason")
        assert recovery["adapter"].dsn == "postgresql:///x"


def test_recovery_uses_import_visible_symlink_source_cache(
    module_dir,
    monkeypatch,
) -> None:
    """A symlink import is cached under the symlink filename, not its target."""
    name = module_dir.make("alias_symlink_source", _WITHOUT_DSN_SAME_IDENTITY)
    source = module_dir.root / f"{name}.py"
    backing_dir = module_dir.root / "backing"
    backing_dir.mkdir()
    backing = backing_dir / "adapter.py"
    source.rename(backing)
    source.symlink_to(backing)

    imported = importlib.import_module(name)
    assert Path(imported.__file__) == source
    assert source.is_symlink()
    import_visible_cache = Path(importlib.util.cache_from_source(str(source)))
    target_cache = Path(importlib.util.cache_from_source(str(backing)))
    assert import_visible_cache.is_file()
    assert not target_cache.exists()
    original_mtime_ns = backing.stat().st_mtime_ns

    module_dir.rewrite(
        name,
        _WITH_DSN_SAME_IDENTITY,
        mtime_ns=original_mtime_ns,
    )

    # Finder invalidation still accepts the import-visible stale bytecode when
    # the symlink target rewrite preserves source size and mtime.
    sys.modules.pop(name)
    importlib.invalidate_caches()
    stale_reimport = importlib.import_module(name)
    assert not hasattr(stale_reimport.AliasSource(), "dsn")

    deleted: list[Path] = []
    original_unlink = Path.unlink

    def record_unlink(path: Path, *args, **kwargs):
        deleted.append(path)
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", record_unlink)
    recovery = sc.retry_construction_after_eviction(
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "postgresql:///x"},
    )

    assert import_visible_cache in deleted
    assert source.is_symlink()
    assert recovery["recovered"] is True, recovery.get("reason")
    assert recovery["adapter"].dsn == "postgresql:///x"


def test_recovery_unlinks_cache_alias_without_deleting_its_target(
    module_dir,
    monkeypatch,
) -> None:
    """The derived cache filename may be an alias; its target is not disposable."""
    name = module_dir.make("alias_cache_file", _WITHOUT_DSN_SAME_IDENTITY)
    importlib.import_module(name)
    source = module_dir.root / f"{name}.py"
    cache_alias = Path(importlib.util.cache_from_source(str(source)))
    assert cache_alias.is_file()
    sentinel = module_dir.root / "cache-target-sentinel.pyc"
    cache_alias.rename(sentinel)
    cache_alias.symlink_to(sentinel)
    assert cache_alias.is_symlink()
    original_mtime_ns = source.stat().st_mtime_ns

    module_dir.rewrite(
        name,
        _WITH_DSN_SAME_IDENTITY,
        mtime_ns=original_mtime_ns,
    )

    # A normal re-import follows the cache alias and resurrects its stale target.
    sys.modules.pop(name)
    importlib.invalidate_caches()
    stale_reimport = importlib.import_module(name)
    assert not hasattr(stale_reimport.AliasSource(), "dsn")

    deleted: list[Path] = []
    original_unlink = Path.unlink

    def record_unlink(path: Path, *args, **kwargs):
        deleted.append(path)
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", record_unlink)
    recovery = sc.retry_construction_after_eviction(
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "postgresql:///x"},
    )

    assert cache_alias in deleted
    assert sentinel not in deleted
    assert sentinel.is_file()
    assert not cache_alias.is_symlink()
    assert recovery["recovered"] is True, recovery.get("reason")
    assert recovery["adapter"].dsn == "postgresql:///x"


def test_dataclass_adapter_can_be_proven_stale(module_dir) -> None:
    """A @dataclass source adapter is a plausible shape; cover it.

    Without this the on-disk comparison finds no literal `__init__`,
    falls back to mtime, and the recovery never fires for dataclass
    adapters.
    """
    name = module_dir.make("alias_dc", """
        from dataclasses import dataclass
        from typing import ClassVar

        @dataclass
        class AliasSource:
            source_name: str | None = None
            last_run_stats: ClassVar[dict] = {}
    """)
    __import__(name)
    module_dir.rewrite(name, """
        from dataclasses import dataclass
        from typing import ClassVar

        @dataclass
        class AliasSource:
            source_name: str | None = None
            dsn: str | None = None
            last_run_stats: ClassVar[dict] = {}
    """)

    proof = sc.prove_stale_definition(
        module_name=name,
        class_name="AliasSource",
        arguments=["dsn"],
        direction="adapter_rejected_argument",
    )
    assert proof["status"] == "stale"
    assert proof["basis"] == "definition_comparison"
    # ClassVar is not an init parameter and must not be reported as one.
    assert "last_run_stats" not in proof["on_disk_parameters"]

    recovery = sc.retry_construction_after_eviction(
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "postgresql:///x"},
    )
    assert recovery["recovered"] is True


def test_second_recovery_of_a_healed_module_does_not_reevict(
    module_dir,
) -> None:
    """Once healed, a repeat recovery reuses the fresh class object."""
    name = module_dir.make("alias_repeat", _WITHOUT_DSN)
    __import__(name)
    module_dir.rewrite(name, _WITH_DSN)

    first = sc.retry_construction_after_eviction(
        module_name=name, class_name="AliasSource", params={"dsn": "x"},
    )
    assert first["recovered"] is True
    assert first["evicted_modules"] == [name]

    second = sc.retry_construction_after_eviction(
        module_name=name, class_name="AliasSource", params={"dsn": "x"},
    )
    assert second["recovered"] is True
    # Already fresh — no second eviction, so no second class object.
    assert second["evicted_modules"] == []
    assert sys.modules[name].AliasSource is type(second["adapter"])


def test_concurrent_recoveries_yield_one_class_object(module_dir) -> None:
    """Genuinely concurrent recoveries must not fork the class identity.

    Sources run in a thread pool and two of them can name the same
    adapter module. Without serialisation, both evict and both import,
    and the two adapters end up instances of different class objects
    with the same qualified name.
    """
    import threading

    name = module_dir.make("alias_threads", _WITHOUT_DSN)
    __import__(name)
    module_dir.rewrite(name, _WITH_DSN)

    barrier = threading.Barrier(6)
    results: list[dict] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        outcome = sc.retry_construction_after_eviction(
            module_name=name, class_name="AliasSource", params={"dsn": "x"},
        )
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads), "deadlock"

    assert len(results) == 6
    assert all(row["recovered"] for row in results), results
    classes = {id(type(row["adapter"])) for row in results}
    assert len(classes) == 1, "concurrent recoveries forked the class object"


def test_requiredness_difference_is_recovered_not_skipped(
    module_dir,
) -> None:
    """Parameter NAMES equal, requiredness different — still stale.

    The freshness short-circuit compares both axes. Comparing names
    alone would report the module already fresh, skip the eviction the
    proof had just established was needed, and restore the #945 failure
    mode on a case the code proved stale.
    """
    name = module_dir.make("alias_required_axis", """
        class AliasSource:
            def __init__(self, *, source_name=None, dsn):
                self.dsn = dsn
    """)
    __import__(name)
    module_dir.rewrite(name, """
        class AliasSource:
            def __init__(self, *, source_name=None, dsn=None):
                self.dsn = dsn
    """)

    proof = sc.prove_stale_definition(
        module_name=name,
        class_name="AliasSource",
        arguments=["dsn"],
        direction="adapter_requires_argument",
    )
    assert proof["status"] == "stale"

    recovery = sc.retry_construction_after_eviction(
        module_name=name, class_name="AliasSource", params={},
    )
    assert recovery["recovered"] is True, recovery.get("reason")
    assert recovery["evicted_modules"] == [name]


@pytest.mark.parametrize(
    "body",
    [
        # Inherited fields are real init parameters and are not resolved
        # statically — answering from this class's own fields alone would
        # report `current` for a definition that is actually stale.
        """
        from dataclasses import dataclass

        class Base:
            pass

        @dataclass
        class AliasSource(Base):
            source_name: str | None = None
        """,
        # `field(init=False)` is declared but is NOT an init parameter.
        """
        from dataclasses import dataclass, field

        @dataclass
        class AliasSource:
            source_name: str | None = None
            cached: dict = field(default_factory=dict, init=False)
        """,
        # The KW_ONLY sentinel is a marker, not a parameter.
        """
        from dataclasses import dataclass, KW_ONLY

        @dataclass
        class AliasSource:
            source_name: str | None = None
            _: KW_ONLY
            other: int = 1
        """,
    ],
    ids=["inherited", "field-init-false", "kw-only-sentinel"],
)
def test_dataclass_shapes_this_cannot_read_exactly(module_dir, body) -> None:
    """Report the parameter set exactly, or report nothing.

    A wrong set makes the caller assert `registry_adapter_mismatch`
    ("change the registry or the adapter") for a process that is in fact
    serving stale code — the confidently-wrong answer.
    """
    import dataclasses
    import inspect as _inspect

    name = module_dir.make("alias_dc_shape", body)
    module = __import__(name)

    path = Path(module.__file__)
    parsed = sc.on_disk_init_parameters(path, "AliasSource")
    if parsed is None:
        return  # Refused — the honest answer.

    # If it DID answer, the answer must match the real generated
    # __init__, not merely be close.
    real = {
        parameter
        for parameter in _inspect.signature(module.AliasSource).parameters
    }
    assert parsed["all"] == real, (
        f"parsed {sorted(parsed['all'])} != real {sorted(real)}"
    )
    assert dataclasses.is_dataclass(module.AliasSource)


def test_recovery_failure_is_reported_not_claimed(module_dir) -> None:
    """A re-import that still fails must not report a recovery."""
    name = module_dir.make("alias_norecover", _WITHOUT_DSN)
    __import__(name)

    recovery = sc.retry_construction_after_eviction(
        module_name=name,
        class_name="AliasSource",
        params={"dsn": "x"},
    )
    assert recovery["recovered"] is False
    assert "TypeError" in recovery["reason"]


def test_missing_required_argument_is_also_classified(module_dir) -> None:
    name = module_dir.make("alias_required", """
        class AliasSource:
            def __init__(self, dsn):
                self.dsn = dsn
    """)
    __import__(name)

    with pytest.raises(TypeError) as raised:
        _construct(name)

    classified = sc.classify_source_construction_failure(
        exc=raised.value,
        stage="construct",
        module_name=name,
        class_name="AliasSource",
        params={},
    )
    assert classified["error_kind"] == sc.SOURCE_ADAPTER_SIGNATURE_MISMATCH
    assert classified["direction"] == "adapter_requires_argument"


def test_unrelated_failures_are_left_alone(module_dir) -> None:
    """Classification must never reinterpret an unrelated error."""
    assert sc.classify_source_construction_failure(
        exc=ValueError("boom"),
        stage="construct",
        module_name="x",
        class_name="Y",
    ) is None
    assert sc.classify_source_construction_failure(
        exc=TypeError("unsupported operand type(s) for +: 'int' and 'str'"),
        stage="construct",
        module_name="x",
        class_name="Y",
    ) is None
    # Same TypeError, but raised while RUNNING rather than constructing.
    assert sc.classify_source_construction_failure(
        exc=TypeError("__init__() got an unexpected keyword argument 'dsn'"),
        stage="run",
        module_name="x",
        class_name="Y",
    ) is None


def test_admission_verdict_carries_error_kind(monkeypatch) -> None:
    """The classification reaches the admission envelope, not just the log.

    `okg.admission_events` is where an operator looks after the fact;
    #945 cost 36 hours partly because the row there said only
    ``TypeError``. This asserts the verdict payload that
    `transition_envelope` is handed.
    """
    from okg.substrate.ingest import service as ingest_service

    captured: dict = {}

    class _Conn:
        def transaction(self):
            class _Txn:
                def __enter__(self):
                    return None

                def __exit__(self, *exc):
                    return False

            return _Txn()

    class _Pool:
        # `pooled_connection` sizes the general-admission bound from the
        # pool's width, so a stub pool has to declare one.
        max_size = 8
        name = "stub-pool"

        def connection(self):
            class _Ctx:
                def __enter__(self):
                    return _Conn()

                def __exit__(self, *exc):
                    return False

            return _Ctx()

    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool", lambda dsn: _Pool()
    )
    monkeypatch.setattr(
        ingest_service,
        "transition_envelope",
        lambda conn, **kwargs: captured.update(kwargs),
    )

    ingest_service._fail_source_admission(
        dsn="postgresql:///unused",
        admission_id="adm-1",
        error="source_adapter_signature_mismatch: ...",
        workflow_ref={"workflow_id": "w"},
        diagnosis={
            "error_kind": sc.SOURCE_ADAPTER_SIGNATURE_MISMATCH,
            "cause": "stale_loaded_code",
        },
    )

    assert captured["status"] == "failed"
    assert captured["verdict"]["error_kind"] == (
        sc.SOURCE_ADAPTER_SIGNATURE_MISMATCH
    )
    assert captured["verdict"]["error_diagnosis"]["cause"] == (
        "stale_loaded_code"
    )


def test_eviction_is_scoped_to_the_failing_subtree(module_dir) -> None:
    """A sibling module is NOT evicted.

    Narrow scope is deliberate: evicting live modules wholesale churns
    class identity under running code. The cost is that a stale
    definition in a sibling produces an honest failed retry instead of a
    claimed recovery.
    """
    parent = module_dir.root / "pkg_scope"
    parent.mkdir()
    (parent / "__init__.py").write_text("", encoding="utf-8")
    (parent / "child.py").write_text("VALUE = 1\n", encoding="utf-8")
    module_dir.make("sibling_scope", "VALUE = 1\n")
    __import__("pkg_scope.child")
    __import__("sibling_scope")

    evicted = sc.evict_module_tree("pkg_scope")
    try:
        assert "pkg_scope" in evicted
        assert "pkg_scope.child" in evicted
        assert "sibling_scope" not in evicted
        assert "sibling_scope" in sys.modules
    finally:
        for name in list(sys.modules):
            if name.startswith("pkg_scope"):
                sys.modules.pop(name, None)
        sys.modules.pop("sibling_scope", None)
