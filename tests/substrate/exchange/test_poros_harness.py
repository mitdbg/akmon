from __future__ import annotations

import pytest

from okg.substrate.exchange.harness import (
    PorosDatabaseIdentity,
    PorosHarnessError,
    assert_distinct_database_identities,
)


def test_two_okg_harness_rejects_same_database_identity() -> None:
    sender = PorosDatabaseIdentity(
        dsn="postgres://example/sender",
        database="okg",
        server_addr="127.0.0.1",
        server_port="5432",
        user="postgres",
    )
    receiver = PorosDatabaseIdentity(
        dsn="postgres://example/receiver",
        database="okg",
        server_addr="127.0.0.1",
        server_port="5432",
        user="postgres",
    )

    with pytest.raises(PorosHarnessError, match="requires separate"):
        assert_distinct_database_identities(sender, receiver)


def test_two_okg_harness_accepts_distinct_database_identities() -> None:
    sender = PorosDatabaseIdentity(
        dsn="postgres://example/sender",
        database="okg_poros_sender",
        server_addr="127.0.0.1",
        server_port="5432",
        user="postgres",
    )
    receiver = PorosDatabaseIdentity(
        dsn="postgres://example/receiver",
        database="okg_poros_receiver",
        server_addr="127.0.0.1",
        server_port="5432",
        user="postgres",
    )

    assert_distinct_database_identities(sender, receiver)
