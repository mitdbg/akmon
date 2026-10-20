"""Issue #2838: runner-reserved record keys are never scope-independent.

Database-free. The runner-level refusal runs against a real database in
``tests/substrate/test_source_runner_record_set.py``.
"""
from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.runner import (
    SourceRunner,
    freeze_replacement_keys,
    refuse_runner_reserved_replacement_keys,
)
from okg.substrate.library.sources.base import (
    RecordEmission,
    RecordNode,
    RecordSet,
    record_key_is_runner_reserved,
    record_key_is_scope_independent,
)


# Every `__<name>__` record key a source or the runner constructs today
# (grep of src/okg/substrate for `__<name>__:` spellings, 2026-09-28).
SOURCE_SYNTHETIC_KEYS = (
    "__branch__:main",  # git_graph_files.py branch record
    "__cap__:pact",  # pact.py capability record key
    "__cap__:pact:node:cap:pact",  # pact.py capability fact key
)
RUNNER_RESERVED_KEYS = (
    "__chronos_retained__:node:sha256:abc",  # retained-graph bootstrap
    "__chronos_retained__:edge:sha256:abc",
    "__stream__:node:sha256:abc",  # streamed footprint
    "__stream__:edge:sha256:abc",
)


@pytest.mark.parametrize("key", SOURCE_SYNTHETIC_KEYS)
def test_source_synthetic_keys_stay_scope_independent(key: str) -> None:
    assert record_key_is_scope_independent(key)
    assert not record_key_is_runner_reserved(key)


@pytest.mark.parametrize("key", RUNNER_RESERVED_KEYS)
def test_runner_reserved_keys_are_not_scope_independent(key: str) -> None:
    assert record_key_is_runner_reserved(key)
    assert not record_key_is_scope_independent(key)


@pytest.mark.parametrize(
    "key",
    (
        "__init__.py",
        "b.md",
        "src/__chronos_retained__:node:x",
        "__chronos_retained_extra__:node:x",
        "__streamx__:node:x",
    ),
)
def test_lookalike_keys_are_not_runner_reserved(key: str) -> None:
    assert not record_key_is_runner_reserved(key)


def test_refusal_names_every_reserved_key_and_its_reason() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        refuse_runner_reserved_replacement_keys(
            source_name="test.source",
            replacement_keys={
                "b.md",
                "__branch__:main",
                "__stream__:edge:sha256:abc",
                "__chronos_retained__:node:sha256:abc",
            },
        )

    data = excinfo.value.data
    assert data["reason_code"] == "scoped_replacement_retained_key_forbidden"
    assert data["source_name"] == "test.source"
    assert data["reserved_replacement_keys"] == [
        "__chronos_retained__:node:sha256:abc",
        "__stream__:edge:sha256:abc",
    ]
    assert data["reserved_replacement_key_count"] == 2


def test_refusal_passes_legitimate_replacement_keys() -> None:
    refuse_runner_reserved_replacement_keys(
        source_name="test.source",
        replacement_keys=["b.md", *SOURCE_SYNTHETIC_KEYS],
    )


def test_merge_carries_runner_generated_retained_keys_forward() -> None:
    """The runner's own retained records sit in the PRIOR, not the keys.

    The retained-graph bootstrap adds its adopted records to the prior set;
    a replacement slice that declares only its own key leaves them intact.
    """
    retained_key = "__chronos_retained__:node:sha256:legacy"
    prior = RecordSet(records={
        retained_key: RecordEmission(primary=RecordNode("doc:legacy", "c")),
        "b.md": RecordEmission(primary=RecordNode("doc:b", "c")),
    })

    merged = SourceRunner._record_set_with_prior(
        prior,
        current=RecordSet(),
        replacement_keys={"b.md", "__branch__:main"},
    )

    assert set(merged.records) == {retained_key}


def test_freeze_replacement_keys_reads_a_generator_once() -> None:
    keys = (key for key in ("b.md", "a.md"))

    frozen = freeze_replacement_keys(keys)

    assert frozen == frozenset({"a.md", "b.md"})
    assert list(keys) == []
    assert freeze_replacement_keys(None) is None
    assert freeze_replacement_keys([]) == frozenset()


RETAINED_LEGACY_KEY = "__chronos_retained__:node:sha256:legacy"


def _merged_prior_runner() -> SourceRunner:
    # The merged-prior check reads only source_name; no database needed.
    runner = SourceRunner.__new__(SourceRunner)
    runner.source_name = "test.source"
    return runner


def _prior_with_retained_record() -> RecordSet:
    return RecordSet(records={
        RETAINED_LEGACY_KEY: RecordEmission(
            primary=RecordNode("doc:legacy", "concept"),
        ),
        "b.md": RecordEmission(primary=RecordNode("doc:b", "concept")),
    })


def test_merged_claim_altering_a_retained_record_is_refused() -> None:
    """A merged claim may keep a reserved key but not rewrite its record."""
    declared = RecordSet(records={
        RETAINED_LEGACY_KEY: RecordEmission(
            primary=RecordNode("doc:hijacked", "concept"),
        ),
        "b.md": RecordEmission(primary=RecordNode("doc:b", "concept")),
    })

    with pytest.raises(SubstrateError) as excinfo:
        _merged_prior_runner()._verify_scoped_merged_record_set_covers_prior(
            sync_scope={"scope_keys": ["b.md"]},
            prior_record_set=_prior_with_retained_record(),
            declared_record_set=declared,
        )

    data = excinfo.value.data
    assert data["reason_code"] == (
        "scoped_merged_record_set_altered_outside_scope"
    )
    assert data["altered_prior_record_keys"] == [RETAINED_LEGACY_KEY]


def test_merged_claim_dropping_a_retained_record_is_refused() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        _merged_prior_runner()._verify_scoped_merged_record_set_covers_prior(
            sync_scope={"scope_keys": ["b.md"]},
            prior_record_set=_prior_with_retained_record(),
            declared_record_set=RecordSet(records={
                "b.md": RecordEmission(
                    primary=RecordNode("doc:b", "concept"),
                ),
            }),
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_merged_record_set_incomplete"
    )
    assert excinfo.value.data["uncovered_prior_record_keys"] == [
        RETAINED_LEGACY_KEY,
    ]


def test_merged_claim_carrying_a_retained_record_unchanged_passes() -> None:
    """In-scope b.md may change; the retained record must not."""
    _merged_prior_runner()._verify_scoped_merged_record_set_covers_prior(
        sync_scope={"scope_keys": ["b.md"]},
        prior_record_set=_prior_with_retained_record(),
        declared_record_set=RecordSet(records={
            RETAINED_LEGACY_KEY: RecordEmission(
                primary=RecordNode("doc:legacy", "concept"),
            ),
            "b.md": RecordEmission(primary=RecordNode("doc:b2", "concept")),
        }),
    )
