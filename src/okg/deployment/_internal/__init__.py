"""Implementation modules for the `okg.deployment` SDK.

Underscore prefix is the convention boundary: anything under
``okg.deployment._internal`` is implementation detail and not part of
the SDK's stability contract. The ``okg lint deployments`` boundary
check (Phase 2 §6.3) treats imports from this package as substrate-
internal and disallows them from deployment workflow modules.

Public symbols are re-exported from ``okg.deployment.__init__`` and
listed in its ``__all__``.
"""
