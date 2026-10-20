"""Adapter registry. A fifth benchmark = one import + one dict entry."""

from __future__ import annotations

from ..errors import AbBenchError
from .base import BenchmarkAdapter
from .ctibench import CtiBenchAdapter
from .finauditing import FinAuditingAdapter
from .gitgoodbench import GitGoodBenchAdapter
from .housing_qa import HousingQaAdapter
from .itbench import ItBenchAdapter
from .phantomwiki import PhantomWikiAdapter
from .regminer import RegMinerAdapter
from .scifact import SciFactAdapter
from .stark import StarkPrimeAdapter
from .swe_qa import SweQaAdapter
from .swebench_verified import SweBenchVerifiedAdapter

_ADAPTERS: dict[str, type[BenchmarkAdapter]] = {
    cls.name: cls
    for cls in (
        SweBenchVerifiedAdapter,
        SweQaAdapter,
        GitGoodBenchAdapter,
        RegMinerAdapter,
        CtiBenchAdapter,
        FinAuditingAdapter,
        HousingQaAdapter,
        SciFactAdapter,
        ItBenchAdapter,
        # PACT okg-bench-graph-benchmarks. Both are graph-native: the answer
        # unit is a traversal, not a passage, so a null result here would be
        # informative rather than ambiguous. PhantomWiki additionally has
        # structurally zero contamination — regenerate at a new seed and the
        # universe has never existed.
        PhantomWikiAdapter,
        StarkPrimeAdapter,
    )
}

# FreshQA was CUT (operator decision, 2026-07-28). It is the only benchmark in
# the set whose CORPUS CONTENT was not determined by its own released data —
# which documents constitute the dated snapshot was a standing human decision,
# so the corpus could never be reproduced from the benchmark alone. It also
# loaded 4 instances from an operator-exported sheet rather than a fetchable
# dataset, and its freshness claim needs TWO dated snapshots to mean anything.
# The adapter module is deleted rather than left unregistered, so nothing can
# import a benchmark that is not in the registry.


def adapter_names() -> list[str]:
    return sorted(_ADAPTERS)


def get_adapter(name: str) -> BenchmarkAdapter:
    if name not in _ADAPTERS:
        raise AbBenchError(
            f"unknown benchmark {name!r}; known: {', '.join(adapter_names())}"
        )
    return _ADAPTERS[name]()
