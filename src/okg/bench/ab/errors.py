"""Loud, typed failures. No silent fallbacks anywhere in this package (P0)."""

from __future__ import annotations


class AbBenchError(RuntimeError):
    """Base error for the okg-bench framework. Always fatal, never defaulted."""


class DatasetError(AbBenchError):
    """A dataset could not be fetched, was malformed, or is not cached."""


class ManifestMismatch(AbBenchError):
    """A resume was attempted under a manifest that no longer matches."""


class CorpusBuildError(AbBenchError):
    """The corpus builder could not produce a leakage-clean deployment."""


class LeakageViolation(AbBenchError):
    """The leakage audit found post-cutoff facts in a published generation."""


class ArmDriverError(AbBenchError):
    """An arm driver could not run a task (missing binary, bad config, ...)."""


class ScoringError(AbBenchError):
    """A scorer failed; the task is recorded as unscored, never defaulted."""


class ComparisonRefused(AbBenchError):
    """Two runs cannot be compared, or can be but not over enough pairs.

    Always names the field that differs (or is missing). A refusal is NOT a
    result: the CLI exits non-zero so a script cannot read one as the other.
    """
