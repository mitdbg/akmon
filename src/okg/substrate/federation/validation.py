"""Shared validation primitives for the federation package.

Federation and mailbox modules each raise their own exception type
(`FederationValidationError`, `MailboxValidationError`) and want their own
message wording. These helpers centralize the predicates so the validation
rule lives in one place; callers parameterize the exception class.

Each consumer module keeps a tiny private wrapper (``_required_str`` etc.)
that fixes its exception type once at the top of the file, so call sites
stay terse.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def required_str(
    raw: Mapping[str, Any],
    key: str,
    *,
    exc_type: type[Exception],
    message: str | None = None,
) -> str:
    """Return ``raw[key]`` if it is a non-empty string; otherwise raise."""
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise exc_type(message or f"missing required string field: {key}")
    return value


def required_str_among(
    raw: Mapping[str, Any],
    keys: Sequence[str],
    *,
    exc_type: type[Exception],
    message: str | None = None,
) -> str:
    """Return the first ``raw[k]`` that is a non-empty string; otherwise raise."""
    for key in keys:
        value = raw.get(key)
        if isinstance(value, str) and value:
            return value
    label = " or ".join(keys)
    raise exc_type(message or f"missing required string field: {label}")


def optional_str(
    raw: Mapping[str, Any],
    key: str,
    *,
    exc_type: type[Exception],
) -> str | None:
    """Return ``raw[key]`` as a non-empty string if present, else ``None``."""
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise exc_type(f"{key} must be a non-empty string")
    return value


def required_int(
    raw: Mapping[str, Any],
    key: str,
    *,
    exc_type: type[Exception],
) -> int:
    """Return ``raw[key]`` if it is an int; otherwise raise."""
    value = raw.get(key)
    if not isinstance(value, int):
        raise exc_type(f"{key} must be an integer")
    return value


def positive_int(
    value: Any,
    field: str,
    *,
    exc_type: type[Exception],
) -> int:
    """Return ``value`` if it is a positive int; otherwise raise."""
    if not isinstance(value, int) or value <= 0:
        raise exc_type(f"{field} must be a positive integer")
    return value


def required_mapping(
    value: Any,
    field: str,
    *,
    exc_type: type[Exception],
) -> Mapping[str, Any]:
    """Return ``value`` if it is a Mapping; otherwise raise."""
    if not isinstance(value, Mapping):
        raise exc_type(f"{field} must be an object")
    return value


def required_nonempty_mapping(
    raw: Mapping[str, Any],
    key: str,
    *,
    exc_type: type[Exception],
) -> Mapping[str, Any]:
    """Return a copy of ``raw[key]`` if it is a non-empty Mapping; otherwise raise."""
    value = raw.get(key)
    if not isinstance(value, Mapping) or not value:
        raise exc_type(f"{key} must be a non-empty object")
    return dict(value)


def optional_mapping(
    raw: Mapping[str, Any],
    key: str,
    *,
    exc_type: type[Exception],
) -> Mapping[str, Any]:
    """Return ``raw[key]`` as a dict if present; else return an empty dict."""
    value = raw.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise exc_type(f"{key} must be an object")
    return dict(value)
