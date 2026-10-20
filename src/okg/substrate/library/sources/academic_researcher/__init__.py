"""Source adapters for the ``academic-researcher`` profile.

The profile's ``source-defaults/*.yaml`` name these modules by dotted path
(``okg.substrate.library.sources.academic_researcher.<module>``), and
``library/linkers/academic_researcher_collaboration.py`` imports
``candidate_review`` from here. This file makes the directory a regular
package, so ``packages.find`` ships the modules as code; ``prompts/`` ships
as package data.
"""
