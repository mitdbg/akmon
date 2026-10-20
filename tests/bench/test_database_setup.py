from types import SimpleNamespace

import okg.bench.orchestrator as orchestrator


def test_setup_db_uses_safe_recreation_helper(monkeypatch):
    calls = []
    exp = SimpleNamespace(
        id="safe-clone-test",
        db_name="okg_bench_safe_clone_test",
        dsn="postgresql://postgres:okg@localhost:5433/okg_bench_safe_clone_test",
        deployment="safe-clone-test",
        deployments_root="/tmp",
        deployment_dir="/tmp/okg-bench-safe-clone-test",
    )

    monkeypatch.setattr(
        orchestrator,
        "safe_recreate_database",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    monkeypatch.setattr(orchestrator, "_psql", lambda *_a, **_k: "")
    monkeypatch.setattr(orchestrator, "_write_deployment_files", lambda *_a: None)
    monkeypatch.setattr(orchestrator, "_run", lambda *_a, **_k: None)

    orchestrator.setup_db(exp)

    assert calls == [
        (
            (exp.dsn,),
            {
                "expected_database": exp.db_name,
                "admin_dsn": orchestrator.ADMIN_DSN,
            },
        )
    ]
