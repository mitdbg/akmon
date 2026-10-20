"""academic_researcher ontology module — lab-management subtypes for PIs.

Provides 11 subtypes covering the lab-management dimension of a
professor's personal-OKG: Advisee (refines person.Person), Lab
(refines person.Team), Grant, Deadline, Committee, Review, Course,
TalkEngagement, RecommendationLetter, Meeting, ActionItem.

Loaded by the `academic-researcher` profile by default. Opt-in for any
other deployment that wants typed lab-management surfaces.

See `module.yaml` for the composition story and `schema.yaml` for
the subtype + narrowing declarations.
"""
