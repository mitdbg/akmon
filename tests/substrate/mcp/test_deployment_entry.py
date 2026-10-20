from __future__ import annotations

from okg.substrate.mcp.deployment_entry import _preflight


def test_preflight_checks_read_side_generation_view(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc) -> None:
            return None

        def execute(self, query: str, params: tuple[str, ...]) -> None:
            captured["query"] = query
            captured["params"] = params

        def fetchone(self) -> tuple[int]:
            return (1,)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc) -> None:
            return None

        def cursor(self) -> FakeCursor:
            return FakeCursor()

    def connect(dsn: str, *, connect_timeout: int) -> FakeConnection:
        captured["dsn"] = dsn
        captured["connect_timeout"] = connect_timeout
        return FakeConnection()

    # `_preflight` now opens its session through okg's connection factory, not
    # `psycopg.connect`, so that the session carries the search_path pin
    # (issue #1968) as well as the keepalives and timeouts. Patch the factory.
    import okg.substrate.db.connection as connection_module

    monkeypatch.setattr(connection_module, "connect", connect)

    _preflight("postgres://example/cms", "cms")

    assert captured["dsn"] == "postgres://example/cms"
    assert captured["connect_timeout"] == 5
    assert captured["params"] == ("cms",)
    assert "FROM okg.v_generations" in str(captured["query"])
    assert "okg.graph_generations" not in str(captured["query"])
