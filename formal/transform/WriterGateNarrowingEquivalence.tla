---- MODULE WriterGateNarrowingEquivalence ----
EXTENDS FiniteSets

\* Writer-path NarrowingChecker and TransformConsistencyGate must accept the
\* same edge triples when they read the same catalog version. Both
\* implementations are strict only when a catalog is applied and that catalog
\* has at least one narrowing row; otherwise narrowing enforcement is inactive
\* and every tuple passes this check.
\*
\* Both surfaces admit a declared (parent, edge, child) narrowing triple via a
\* descendant-closure arm UNION a RAW-rows arm (the declared triple itself). The
\* raw arm is what keeps writer and gate equivalent when the reflexive
\* subtype_ancestors self-row is absent -- if the gate used the closure arm
\* alone (BuggyGateOmitsRawRows) an incomplete closure (ReflexiveClosureComplete
\* = FALSE) would make it reject a declared triple the writer accepts. The main
\* config keeps the closure complete and the gate's raw arm on, so the raw arm is
\* redundant and the surfaces agree; the witness config drops the reflexive row
\* and the gate's raw arm to refute the equivalence by name.

\* When FALSE, drop the reflexive self-row for work_item from the closure,
\* modelling an incompletely-populated subtype_ancestors hierarchy.
CONSTANT ReflexiveClosureComplete

\* When TRUE, the gate omits the raw-rows arm the writer and DB trigger use --
\* the pre-fix divergence. FALSE on the main config (the gate matches the
\* writer).
CONSTANT BuggyGateOmitsRawRows

CatalogSubtypes == {
  "entity",
  "work_item",
  "ticket",
  "bug",
  "person",
  "engineer",
  "team"
}

AliasLabels == {"human"}

Subtypes == CatalogSubtypes \cup AliasLabels

EdgeTypes == {"assigned_to", "blocks"}

CatalogVersions == {"cv.primary", "cv.secondary"}

PrimaryNarrowingRows == {
  <<"work_item", "assigned_to", "person">>,
  <<"ticket", "blocks", "ticket">>
}

SecondaryNarrowingRows ==
  PrimaryNarrowingRows \cup {<<"team", "assigned_to", "engineer">>}

NarrowingRowsFor(catalogVersion) ==
  IF catalogVersion = "cv.primary"
  THEN PrimaryNarrowingRows
  ELSE SecondaryNarrowingRows

AliasRows == {<<"human", "person", "subtype">>}

SubtypeAncestorRows == {
  <<"entity", "entity">>,
  <<"work_item", "work_item">>,
  <<"work_item", "entity">>,
  <<"ticket", "ticket">>,
  <<"ticket", "work_item">>,
  <<"ticket", "entity">>,
  <<"bug", "bug">>,
  <<"bug", "ticket">>,
  <<"bug", "work_item">>,
  <<"bug", "entity">>,
  <<"person", "person">>,
  <<"person", "entity">>,
  <<"engineer", "engineer">>,
  <<"engineer", "person">>,
  <<"engineer", "entity">>,
  <<"team", "team">>,
  <<"team", "entity">>
}

\* The closure the surfaces actually read. On the main config it is the complete
\* SubtypeAncestorRows; the witness drops work_item's reflexive self-row so the
\* declared (work_item, ...) triple is admitted only by the raw-rows arm.
EffectiveSubtypeAncestorRows ==
  IF ReflexiveClosureComplete
  THEN SubtypeAncestorRows
  ELSE SubtypeAncestorRows \ {<<"work_item", "work_item">>}

VARIABLES
  srcSubtype,
  edgeType,
  dstSubtype,
  catalogApplied,
  narrowingRowsPresent,
  writerCatalogVersion,
  gateCatalogVersion

vars == <<
  srcSubtype,
  edgeType,
  dstSubtype,
  catalogApplied,
  narrowingRowsPresent,
  writerCatalogVersion,
  gateCatalogVersion
>>

Init ==
  /\ srcSubtype \in Subtypes
  /\ edgeType \in EdgeTypes
  /\ dstSubtype \in Subtypes
  /\ catalogApplied \in BOOLEAN
  /\ narrowingRowsPresent \in BOOLEAN
  /\ writerCatalogVersion \in CatalogVersions
  /\ gateCatalogVersion \in CatalogVersions

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ srcSubtype \in Subtypes
  /\ edgeType \in EdgeTypes
  /\ dstSubtype \in Subtypes
  /\ catalogApplied \in BOOLEAN
  /\ narrowingRowsPresent \in BOOLEAN
  /\ writerCatalogVersion \in CatalogVersions
  /\ gateCatalogVersion \in CatalogVersions

NarrowingActive == catalogApplied /\ narrowingRowsPresent

SameCatalogVersion == writerCatalogVersion = gateCatalogVersion

WriterNarrowingRows ==
  IF narrowingRowsPresent
  THEN NarrowingRowsFor(writerCatalogVersion)
  ELSE {}

GateNarrowingRows ==
  IF narrowingRowsPresent
  THEN NarrowingRowsFor(gateCatalogVersion)
  ELSE {}

WriterDescendants(parent) ==
  {subtype \in CatalogSubtypes : <<subtype, parent>> \in EffectiveSubtypeAncestorRows}

WriterTriplesForRow(row) ==
  WriterDescendants(row[1]) \X {row[2]} \X WriterDescendants(row[3])

\* Closure arm UNION raw-rows arm: the writer (catalog/narrowings.py) and the DB
\* trigger both admit the declared narrowing triple directly, so it is accepted
\* even when the reflexive self-row is missing from the closure.
WriterAllowedTriples ==
  (UNION {WriterTriplesForRow(row) : row \in WriterNarrowingRows})
    \cup WriterNarrowingRows

GateAncestorRows(parent) ==
  {row \in EffectiveSubtypeAncestorRows : row[2] = parent}

GateAllowedSurfaceRows ==
  UNION {
    {<<row[2], src_row[1], dst_row[1]>> :
      src_row \in GateAncestorRows(row[1]),
      dst_row \in GateAncestorRows(row[3])} :
    row \in GateNarrowingRows
  }

GateClosureTriples == {
  <<surface_row[2], surface_row[1], surface_row[3]>> :
    surface_row \in GateAllowedSurfaceRows
}

\* The fixed gate (narrowing_allowed_binding) is closure UNION raw, matching the
\* writer; BuggyGateOmitsRawRows models the pre-fix closure-only gate.
GateAllowedTriples ==
  GateClosureTriples
    \cup (IF BuggyGateOmitsRawRows THEN {} ELSE GateNarrowingRows)

WriterAcceptsTuple(src, edge, dst) ==
  \/ ~NarrowingActive
  \/ <<src, edge, dst>> \in WriterAllowedTriples

GateAcceptsTuple(src, edge, dst) ==
  \/ ~NarrowingActive
  \/ <<src, edge, dst>> \in GateAllowedTriples

WriterGateNarrowingEquivalence ==
  SameCatalogVersion =>
    WriterAcceptsTuple(srcSubtype, edgeType, dstSubtype) =
      GateAcceptsTuple(srcSubtype, edgeType, dstSubtype)

AllTuplesEquivalent ==
  SameCatalogVersion =>
    \A src \in Subtypes, edge \in EdgeTypes, dst \in Subtypes:
      WriterAcceptsTuple(src, edge, dst) = GateAcceptsTuple(src, edge, dst)

CatalogVersionMismatchCanDisagree ==
  (~SameCatalogVersion /\ NarrowingActive) =>
    \E src \in Subtypes, edge \in EdgeTypes, dst \in Subtypes:
      ~(WriterAcceptsTuple(src, edge, dst) = GateAcceptsTuple(src, edge, dst))

InactiveCatalogPermitsAll ==
  ~NarrowingActive =>
    \A src \in Subtypes, edge \in EdgeTypes, dst \in Subtypes:
      /\ WriterAcceptsTuple(src, edge, dst)
      /\ GateAcceptsTuple(src, edge, dst)

AliasRowsDoNotCanonicalizeNarrowing ==
  NarrowingActive =>
    \A alias_row \in AliasRows:
      /\ ~WriterAcceptsTuple("work_item", "assigned_to", alias_row[1])
      /\ ~GateAcceptsTuple("work_item", "assigned_to", alias_row[1])
      /\ WriterAcceptsTuple("work_item", "assigned_to", alias_row[2])
      /\ GateAcceptsTuple("work_item", "assigned_to", alias_row[2])

AllowedTriplesUseCatalogSubtypesOnly ==
  /\ WriterAllowedTriples \subseteq CatalogSubtypes \X EdgeTypes \X CatalogSubtypes
  /\ GateAllowedTriples \subseteq CatalogSubtypes \X EdgeTypes \X CatalogSubtypes

RepresentativeRefinementCases ==
  NarrowingActive =>
    /\ WriterAcceptsTuple("bug", "assigned_to", "engineer")
    /\ GateAcceptsTuple("bug", "assigned_to", "engineer")
    /\ WriterAcceptsTuple("bug", "blocks", "ticket")
    /\ GateAcceptsTuple("bug", "blocks", "ticket")
    /\ ~WriterAcceptsTuple("person", "assigned_to", "engineer")
    /\ ~GateAcceptsTuple("person", "assigned_to", "engineer")

WriterGateNarrowingContract ==
  /\ TypeOK
  /\ WriterGateNarrowingEquivalence
  /\ AllTuplesEquivalent
  /\ CatalogVersionMismatchCanDisagree
  /\ InactiveCatalogPermitsAll
  /\ AliasRowsDoNotCanonicalizeNarrowing
  /\ AllowedTriplesUseCatalogSubtypesOnly
  /\ RepresentativeRefinementCases

====
