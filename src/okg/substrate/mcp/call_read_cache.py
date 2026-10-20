"""Reads that one MCP tool call may reuse across its own reruns.

The byte-budget wrapper (`response_bounds._fit_to_budget`) serves an
over-budget page by running the operator again at a smaller `limit`, pinned
to the first run's generation. Those reruns change only `limit` and page
shaping, so a read whose SQL and parameters do not depend on `limit` returns
the same rows every time. Lexical search's BM25 reads are such reads; the
filtered fold's passage read cost seconds per run on the comp-ops copy and
ran once per rerun (PACT mcp-filtered-passage-cost).

`call_read_cache()` opens one cache for one tool call; `cached_read` returns
the rows a read with the same key already returned inside it, or runs the
read. Outside an open cache every read runs, as before. The key must name
everything the read's result depends on: the statement, its parameters
(which carry the pin's branch point), and the pinned session. Callers mutate
rows, so the cache keeps its own copy and hands out copies.

Only stored rows are cached. Nomos decisions, the effective-attribute
overlay and every other per-run step still run on every rerun.
"""
from __future__ import annotations

import contextlib
import copy
from contextvars import ContextVar
from typing import Any, Callable, Hashable, Iterator, Mapping, Optional, TypeVar

T = TypeVar("T")

_CACHE: ContextVar[Optional[dict[Hashable, Any]]] = ContextVar(
    "okg_mcp_call_read_cache", default=None,
)


@contextlib.contextmanager
def call_read_cache() -> Iterator[None]:
    """One cache for the duration of one tool call (nested calls share it)."""
    if _CACHE.get() is not None:
        yield
        return
    token = _CACHE.set({})
    try:
        yield
    finally:
        _CACHE.reset(token)


def freeze(value: Any) -> Hashable:
    """A hashable, order-independent form of a statement parameter."""
    if isinstance(value, Mapping):
        return tuple(sorted((str(k), freeze(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(freeze(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(repr(freeze(v)) for v in value))
    if isinstance(value, Hashable):
        return (type(value).__name__, value)
    return (type(value).__name__, repr(value))


def cached_read(key: Hashable, read: Callable[[], T]) -> T:
    """`read()`'s result, run at most once per key inside one open cache."""
    cache = _CACHE.get()
    if cache is None:
        return read()
    if key not in cache:
        result = read()
        cache[key] = copy.deepcopy(result)
        return result
    return copy.deepcopy(cache[key])
