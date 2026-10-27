from __future__ import annotations

import itertools
import warnings

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from okg.substrate.embeddings import (
    EmbeddingConfig,
    EmbeddingConfigDeprecationWarning,
    EmbeddingConfigError,
    parse_embedding_config,
)


_ABSENT = object()
_SYMBOLIC_STATES = {
    "absent": _ABSENT,
    "disabled": {"enabled": False},
    "enabled": {"enabled": True, "backend": "hash"},
    "malformed": "not-a-mapping",
    "unknown-key": {"enabled": False, "surprise": True},
}
_UNKNOWN_KEY = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz_",
    min_size=1,
    max_size=16,
).map(lambda suffix: f"x_{suffix}")
# The first unrestricted st.text() draw in a process builds Hypothesis's
# Unicode table (about 0.2 s on an idle lab host, cached on disk afterwards).
# On a fresh checkout under a loaded parallel run that one draw passed the
# 1 s too_slow health check and failed the test before any example ran. The
# check measures the host, not the property; every assertion still runs.
_FIRST_TEXT_DRAW_MAY_BE_SLOW = settings(suppress_health_check=[HealthCheck.too_slow])


@pytest.mark.parametrize(
    ("singular_name", "plural_name"),
    list(itertools.product(_SYMBOLIC_STATES, repeat=2)),
)
def test_exhaustive_symbolic_spelling_state_model(
    singular_name: str,
    plural_name: str,
) -> None:
    """Cover all 25 equivalence-class pairs for the two spellings."""
    singular = _SYMBOLIC_STATES[singular_name]
    plural = _SYMBOLIC_STATES[plural_name]
    kwargs: dict[str, object] = {}
    if singular is not _ABSENT:
        kwargs["singular"] = singular
    if plural is not _ABSENT:
        kwargs["plural"] = plural

    invalid = {"malformed", "unknown-key"}
    valid_conflict = (
        singular_name in {"enabled", "disabled"}
        and plural_name in {"enabled", "disabled"}
        and singular_name != plural_name
    )
    if singular_name in invalid or plural_name in invalid or valid_conflict:
        with pytest.raises(EmbeddingConfigError):
            parse_embedding_config(**kwargs)
        return

    expects_warning = singular_name != "absent"
    context = (
        pytest.warns(EmbeddingConfigDeprecationWarning)
        if expects_warning
        else warnings.catch_warnings(record=True)
    )
    with context as caught:
        if not expects_warning:
            warnings.simplefilter("always")
        actual = parse_embedding_config(**kwargs)
    if not expects_warning:
        assert not caught
    expected_name = plural_name if plural_name != "absent" else singular_name
    expected = {
        "absent": EmbeddingConfig(),
        "disabled": EmbeddingConfig(enabled=False),
        "enabled": EmbeddingConfig(enabled=True, backend="hash"),
    }[expected_name]
    assert actual == expected


@given(
    st.sampled_from(["singular", "plural"]),
    st.dictionaries(
        keys=_UNKNOWN_KEY,
        values=st.one_of(st.none(), st.booleans(), st.integers(), st.text()),
        min_size=1,
        max_size=4,
    )
)
@_FIRST_TEXT_DRAW_MAY_BE_SLOW
def test_every_unknown_key_is_rejected(
    spelling: str,
    unknown: dict[str, object],
) -> None:
    with pytest.raises(EmbeddingConfigError, match="unknown"):
        parse_embedding_config(**{spelling: {"enabled": False, **unknown}})


@given(
    st.sampled_from(["singular", "plural"]),
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        st.floats(),
        st.text(),
        st.lists(st.integers()),
    ).filter(lambda value: not isinstance(value, dict))
)
@_FIRST_TEXT_DRAW_MAY_BE_SLOW
def test_every_non_mapping_block_is_rejected(
    spelling: str,
    value: object,
) -> None:
    with pytest.raises(EmbeddingConfigError, match="mapping"):
        parse_embedding_config(**{spelling: value})


@given(
    st.sampled_from(["singular", "plural"]),
    st.sampled_from([None, 0, 1, "true", [], {}]),
)
def test_every_non_boolean_enabled_value_is_rejected(
    spelling: str,
    value: object,
) -> None:
    with pytest.raises(EmbeddingConfigError, match="enabled must be a boolean"):
        parse_embedding_config(**{spelling: {"enabled": value}})


@given(
    st.sampled_from(["singular", "plural"]),
    st.sampled_from([None, False, 0, 1, [], {}]),
)
def test_every_non_string_reason_value_is_rejected(
    spelling: str,
    value: object,
) -> None:
    if value is None:
        # None is the only admitted non-string sentinel for an omitted reason.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", EmbeddingConfigDeprecationWarning)
            actual = parse_embedding_config(
                **{spelling: {"enabled": False, "reason": value}}
            )
        assert actual.reason is None
        return
    with pytest.raises(EmbeddingConfigError, match="reason must be a string"):
        parse_embedding_config(
            **{spelling: {"enabled": False, "reason": value}}
        )


@given(st.booleans(), st.sampled_from(["hash", " hash "]))
def test_normalization_is_idempotent(enabled: bool, backend: str) -> None:
    block = {"enabled": enabled, "backend": backend}
    first = parse_embedding_config(plural=block)
    second = parse_embedding_config(
        plural={
            "enabled": first.enabled,
            "backend": first.backend,
        }
    )
    assert second == first
