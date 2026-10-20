"""PACT v5: a git-native change manifest, receipts, and one check.

This package has no dependency on ``okg``. Everything it needs is the
checkout, git metadata, PyYAML, and pydantic, so it can be extracted to its
own repository unchanged. ``okg`` imports ``pact``; never the reverse.
"""

from pact.schema import Manifest, PACT_VERSION

__all__ = ["Manifest", "PACT_VERSION"]
