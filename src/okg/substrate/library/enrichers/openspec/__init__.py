"""Substrate-shipped enrichers for the openspec module.

- ``default_project.DefaultProjectMembershipEnricher`` — back-fill
  `OpenSpecChange member_of Project` edges for changes lacking an
  explicit `project:` frontmatter declaration. Per
  `cleanup-personal-okg-data-quality` C1 item 3.
"""

from okg.substrate.library.enrichers.openspec.default_project import (
    DefaultProjectMembershipEnricher,
)

__all__ = ["DefaultProjectMembershipEnricher"]
