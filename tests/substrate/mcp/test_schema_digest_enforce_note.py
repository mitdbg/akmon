"""#3666: the schema-digest resource's enforce-mode note points at a read an
agent can take within the default reply budget.

It pointed at inspect(target={'kind':'ontology'}), which is not paged and
measured larger than the default 65,536-byte budget on a real deployment.
No database: the enforce branch returns before any digest query.
"""

from __future__ import annotations

import json

from okg.substrate.mcp import schema_digest_resource
from okg.substrate.nomos import runtime_enforcement


class _Pool:
    def __init__(self) -> None:
        self.returned = 0

    def getconn(self) -> object:
        return object()

    def putconn(self, _conn: object) -> None:
        self.returned += 1


def test_the_enforce_note_points_at_a_paged_read(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime_enforcement, "deployment_nomos_enforcement_mode",
        lambda _deployment: "enforce",
    )
    pool = _Pool()
    payload = json.loads(schema_digest_resource.build_digest_payload(
        pool=pool,
        deployment_name="demo",
        digest_cache={},
        resolve_latest_fn=lambda _conn, use_cache=False: ("gen-1", 7),
    ))
    note = payload["note"]
    assert "'kind':'ontology'" not in note
    assert "inspect(target={'kind':'subtype','subtype':<type>})" in note
    assert "in pages" in note
    assert pool.returned == 1
