--------------------------- MODULE EntityMerge ---------------------------
EXTENDS FiniteSets, Naturals, TLC

(***************************************************************************
Finite two-merge refinement model for entity-merge remediation.

The mutable branch is represented by ``branchToken``.  It is deliberately a
whole-branch mutation token: every external write changes it, even when the
write is outside the merge incident set.  A plan captures that token and all
policy inputs; stage/apply refuses if any captured value changed.  The second
merge is planned only after the first commit, so its digest and write keys are
instance-derived from the evolved branch rather than from the published
baseline.

The model abstracts PostgreSQL/Chronos rows, but retains the safety boundaries
that the runtime must refine: pair-scoped candidate-record projection,
semantic/confidence refusal, provenance audit union, Nomos enforce refusal,
transaction-private staging, atomic commit, and stale-plan refusal preserving
the already-approved prefix.
***************************************************************************)

CONSTANTS Survivor, RetireeA, RetireeB, Other, Chunk, Enricher

NodeUniverse == {Survivor, RetireeA, RetireeB, Other}
Retirees == {RetireeA, RetireeB}
EdgeKinds == {"in", "out"}
TokenValues == 0..4
CheckpointValues == 0..3
CatalogValues == 1..3
NomosModes == {"off", "audit", "enforce"}
SemanticValues == {"same", "different"}
ConfidenceValues == {"ok", "weaker"}
AttrAtoms == {"base", "retiree-a", "retiree-b"}
AttrValues == SUBSET AttrAtoms
WriteKinds == {"edges", "survivor-attrs", "retired-node", "receipt"}
NoPlan == "no-plan"
NoRecord == [chunk |-> Chunk, enricher |-> Enricher, node |-> Other]

InitialLive == NodeUniverse
InitialEdges == {
    [src |-> Other,    dst |-> Survivor, kind |-> "in"],
    [src |-> Survivor, dst |-> Other,    kind |-> "out"],
    [src |-> Other,    dst |-> RetireeA, kind |-> "in"],
    [src |-> RetireeA, dst |-> Other,    kind |-> "out"],
    [src |-> Other,    dst |-> RetireeB, kind |-> "in"],
    [src |-> RetireeB, dst |-> Other,    kind |-> "out"]
}

CandidateRecordUniverse == {
    [chunk |-> Chunk, enricher |-> Enricher, node |-> RetireeA],
    [chunk |-> Chunk, enricher |-> Enricher, node |-> RetireeB],
    [chunk |-> Chunk, enricher |-> Enricher, node |-> Survivor],
    [chunk |-> Chunk, enricher |-> Enricher, node |-> Other],
    [chunk |-> Chunk, enricher |-> Enricher, node |-> RetireeA,
     span |-> 12],
    [enricher |-> Enricher, node |-> RetireeA],
    [chunk |-> Chunk, node |-> RetireeA],
    [chunk |-> Chunk, enricher |-> Enricher, node |-> RetireeA,
     owner |-> [node |-> RetireeA]],
    [chunk |-> Chunk, enricher |-> Enricher,
     owner |-> [node |-> RetireeA]]
}

GoodRecord(r) == [chunk |-> Chunk, enricher |-> Enricher, node |-> r]
GoodRecordWithSpan(r) == [chunk |-> Chunk, enricher |-> Enricher, node |-> r,
                         span |-> 12]
BadNoChunk == [enricher |-> Enricher, node |-> RetireeA]
BadNoEnricher == [chunk |-> Chunk, node |-> RetireeA]
WrongPairRecord == [chunk |-> Chunk, enricher |-> Enricher, node |-> Other]
NestedRecord == [chunk |-> Chunk, enricher |-> Enricher, node |-> RetireeA,
                 owner |-> [node |-> RetireeA]]
NestedProjected == [chunk |-> Chunk, enricher |-> Enricher, node |-> Survivor,
                    owner |-> [node |-> RetireeA]]
NestedOnlyRecord == [chunk |-> Chunk, enricher |-> Enricher,
                     owner |-> [node |-> RetireeA]]

RecordShape(record) ==
    \/ record \in [chunk: {Chunk}, enricher: {Enricher}, node: NodeUniverse]
    \/ record \in [chunk: {Chunk}, enricher: {Enricher}, node: NodeUniverse,
                    span: {12}]
    \/ record \in [chunk: {Chunk}, enricher: {Enricher}, node: NodeUniverse,
                    owner: [node: NodeUniverse]]

CandidatePairShape(record, retiree) ==
    RecordShape(record) /\ record.node = retiree

NormalizeCandidateRecord(record, survivor, retiree) ==
    IF CandidatePairShape(record, retiree)
    THEN [record EXCEPT !.node = survivor]
    ELSE record

(***************************************************************************
The projection is intentionally narrow.  It rewrites only the top-level node
field when chunk, enricher, and node are all present and the node is exactly
the retiree in this merge pair.  Extra fields are preserved and remain
identity-bearing.  Missing fields, nested-only lookalikes, and a node from
another pair remain byte-for-byte unchanged.
***************************************************************************)
CandidateProjectionInvariant ==
    /\ NormalizeCandidateRecord(GoodRecord(RetireeA), Survivor, RetireeA)
         = GoodRecord(Survivor)
    /\ NormalizeCandidateRecord(GoodRecordWithSpan(RetireeA), Survivor, RetireeA)
         = GoodRecordWithSpan(Survivor)
    /\ NormalizeCandidateRecord(GoodRecord(RetireeB), Survivor, RetireeA)
         = GoodRecord(RetireeB)
    /\ NormalizeCandidateRecord(BadNoChunk, Survivor, RetireeA) = BadNoChunk
    /\ NormalizeCandidateRecord(BadNoEnricher, Survivor, RetireeA)
         = BadNoEnricher
    /\ NormalizeCandidateRecord(WrongPairRecord, Survivor, RetireeA)
         = WrongPairRecord
    /\ NormalizeCandidateRecord(NestedRecord, Survivor, RetireeA)
         = NestedProjected
    /\ NormalizeCandidateRecord(NestedOnlyRecord, Survivor, RetireeA)
         = NestedOnlyRecord

RehomeOne(edge, retiree) ==
    [src |-> IF edge.src = retiree THEN Survivor ELSE edge.src,
     dst |-> IF edge.dst = retiree THEN Survivor ELSE edge.dst,
     kind |-> edge.kind]

RehomeSet(edges, retiree) == {RehomeOne(edge, retiree) : edge \in edges}

FoldEdges(retirees) ==
    LET afterA ==
            IF RetireeA \in retirees
            THEN RehomeSet(InitialEdges, RetireeA)
            ELSE InitialEdges
    IN IF RetireeB \in retirees
       THEN RehomeSet(afterA, RetireeB)
       ELSE afterA

FoldAttrs(retirees) ==
    {"base"}
    \cup (IF RetireeA \in retirees THEN {"retiree-a"} ELSE {})
    \cup (IF RetireeB \in retirees THEN {"retiree-b"} ELSE {})

FoldLive(retirees) == InitialLive \ retirees

ProvenanceAtoms == {
    [retiree |-> r, source_record_id |-> GoodRecord(Survivor),
     lineage |-> "survivor-lineage"] : r \in Retirees
} \cup {
    [retiree |-> r, source_record_id |-> GoodRecord(r),
     lineage |-> "retiree-lineage"] : r \in Retirees
}

ProvenanceUnion(retiree) == {
    [retiree |-> retiree, source_record_id |-> GoodRecord(Survivor),
     lineage |-> "survivor-lineage"],
    [retiree |-> retiree, source_record_id |-> GoodRecord(retiree),
     lineage |-> "retiree-lineage"]
}

SemanticOK(retiree, modes) == modes[retiree] = "same"
ConfidenceOK(retiree, modes) == modes[retiree] = "ok"

PlanContent(retiree, attrs, records, semantic, confidence, nomos) ==
    [survivor |-> Survivor,
     retiree |-> retiree,
     survivor_attrs |-> attrs,
     candidate_record |-> records[retiree],
     semantic |-> semantic[retiree],
     confidence |-> confidence[retiree],
     nomos_mode |-> nomos]

PlanDigest(retiree, token, checkpoint, catalog, attrs, records,
           semantic, confidence, nomos) ==
    [survivor |-> Survivor,
     retiree |-> retiree,
     branch_token |-> token,
     published_checkpoint |-> checkpoint,
     catalog_version |-> catalog,
     content |-> PlanContent(retiree, attrs, records, semantic,
                              confidence, nomos)]

PlanUniverse == {
    [survivor |-> Survivor,
     retiree |-> r,
     branch_token |-> t,
     published_checkpoint |-> p,
     catalog_version |-> c,
     content |-> [survivor |-> Survivor,
                   retiree |-> r,
                   survivor_attrs |-> a,
                   candidate_record |-> record,
                   semantic |-> sem,
                   confidence |-> conf,
                   nomos_mode |-> mode]]
    : r \in Retirees,
      t \in TokenValues,
      p \in CheckpointValues,
      c \in CatalogValues,
      a \in AttrValues,
      record \in CandidateRecordUniverse,
      sem \in SemanticValues,
      conf \in ConfidenceValues,
      mode \in NomosModes
}

WriteKeyUniverse == {
    [plan |-> plan, kind |-> kind] : plan \in PlanUniverse,
                                      kind \in WriteKinds
}

VARIABLES
    phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
    branchToken, publishedCheckpoint, catalogVersion, nomosMode,
    candidateRecords, semanticModes, confidenceModes,
    plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
    plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
    plannedSemantic, plannedConfidence, plannedDigest, planHistory,
    pending, fault, published

vars == <<
    phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
    branchToken, publishedCheckpoint, catalogVersion, nomosMode,
    candidateRecords, semanticModes, confidenceModes,
    plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
    plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
    plannedSemantic, plannedConfidence, plannedDigest, planHistory,
    pending, fault, published
>>

Init ==
    /\ phase = "ready"
    /\ live = InitialLive
    /\ edges = InitialEdges
    /\ survivorAttrs = {"base"}
    /\ receipts = {}
    /\ audit = [r \in Retirees |-> {}]
    /\ writeKeys = {}
    /\ branchToken = 0
    /\ publishedCheckpoint = 0
    /\ catalogVersion = 1
    /\ nomosMode = "off"
    /\ candidateRecords = [r \in Retirees |-> GoodRecord(r)]
    /\ semanticModes = [r \in Retirees |-> "same"]
    /\ confidenceModes = [r \in Retirees |-> "ok"]
    /\ plannedRetiree = NoPlan
    /\ plannedBranchToken = 0
    /\ plannedPublishedCheckpoint = 0
    /\ plannedCatalogVersion = 1
    /\ plannedNomosMode = "off"
    /\ plannedCandidateRecord = NoRecord
    /\ plannedSemantic = "same"
    /\ plannedConfidence = "ok"
    /\ plannedDigest = NoPlan
    /\ planHistory = {}
    /\ pending = FALSE
    /\ fault = FALSE
    /\ published = FALSE

PlanAllowed(retiree) ==
    /\ retiree \notin receipts
    /\ CandidatePairShape(candidateRecords[retiree], retiree)
    /\ SemanticOK(retiree, semanticModes)
    /\ ConfidenceOK(retiree, confidenceModes)
    /\ nomosMode # "enforce"

CurrentPlanValid ==
    /\ plannedRetiree \in Retirees
    /\ plannedRetiree \notin receipts
    /\ branchToken = plannedBranchToken
    /\ publishedCheckpoint = plannedPublishedCheckpoint
    /\ catalogVersion = plannedCatalogVersion
    /\ nomosMode = plannedNomosMode
    /\ candidateRecords[plannedRetiree] = plannedCandidateRecord
    /\ semanticModes[plannedRetiree] = plannedSemantic
    /\ confidenceModes[plannedRetiree] = plannedConfidence
    /\ CandidatePairShape(plannedCandidateRecord, plannedRetiree)
    /\ plannedNomosMode # "enforce"
    /\ SemanticOK(plannedRetiree, semanticModes)
    /\ ConfidenceOK(plannedRetiree, confidenceModes)
    /\ ~fault

Plan(retiree, nextPhase) ==
    /\ phase = IF retiree = RetireeA THEN "ready" ELSE "appliedA"
    /\ PlanAllowed(retiree)
    /\ plannedRetiree' = retiree
    /\ plannedBranchToken' = branchToken
    /\ plannedPublishedCheckpoint' = publishedCheckpoint
    /\ plannedCatalogVersion' = catalogVersion
    /\ plannedNomosMode' = nomosMode
    /\ plannedCandidateRecord' = candidateRecords[retiree]
    /\ plannedSemantic' = semanticModes[retiree]
    /\ plannedConfidence' = confidenceModes[retiree]
    /\ plannedDigest' = PlanDigest(
           retiree, branchToken, publishedCheckpoint, catalogVersion,
           survivorAttrs, candidateRecords, semanticModes, confidenceModes,
           nomosMode)
    /\ planHistory' = planHistory \cup {PlanDigest(
           retiree, branchToken, publishedCheckpoint, catalogVersion,
           survivorAttrs, candidateRecords, semanticModes, confidenceModes,
           nomosMode)}
    /\ phase' = nextPhase
    /\ UNCHANGED <<
         live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         pending, fault, published
         >>

PlanA == Plan(RetireeA, "plannedA")
PlanB == Plan(RetireeB, "plannedB")

Stage ==
    /\ phase \in {"plannedA", "plannedB"}
    /\ ~pending
    /\ CurrentPlanValid
    /\ pending' = TRUE
    /\ phase' = IF phase = "plannedA" THEN "stagedA" ELSE "stagedB"
    /\ UNCHANGED <<
         live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         fault, published
         >>

Commit(retiree, nextPhase) ==
    /\ phase = IF retiree = RetireeA THEN "stagedA" ELSE "stagedB"
    /\ pending
    /\ ~fault
    /\ CurrentPlanValid
    /\ branchToken < 4
    /\ live' = FoldLive(receipts \cup {retiree})
    /\ edges' = FoldEdges(receipts \cup {retiree})
    /\ survivorAttrs' = FoldAttrs(receipts \cup {retiree})
    /\ receipts' = receipts \cup {retiree}
    /\ audit' = [audit EXCEPT ![retiree] = ProvenanceUnion(retiree)]
    /\ writeKeys' = writeKeys \cup {
         [plan |-> plannedDigest, kind |-> kind] : kind \in WriteKinds}
    /\ branchToken' = branchToken + 1
    /\ pending' = FALSE
    /\ phase' = nextPhase
    /\ UNCHANGED <<
         publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         fault, published
         >>

CommitA == Commit(RetireeA, "appliedA")
CommitB == Commit(RetireeB, "complete")

Rollback ==
    /\ phase \in {"stagedA", "stagedB"}
    /\ pending
    /\ fault
    /\ pending' = FALSE
    /\ phase' = "refused"
    /\ UNCHANGED <<
         live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         fault, published
         >>

Refuse ==
    /\ phase \in {"ready", "plannedA", "stagedA", "appliedA", "plannedB", "stagedB"}
    /\ IF phase = "ready"
       THEN ~PlanAllowed(RetireeA)
       ELSE IF phase = "appliedA"
            THEN ~PlanAllowed(RetireeB)
            ELSE ~CurrentPlanValid
    /\ phase' = "refused"
    /\ pending' = FALSE
    /\ UNCHANGED <<
         live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         fault, published
         >>

ExternalWriter ==
    /\ phase \in {"plannedA", "plannedB"}
    /\ branchToken < 4
    /\ branchToken' = branchToken + 1
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault, published
         >>

CatalogDrift ==
    /\ phase \in {"plannedA", "plannedB"}
    /\ catalogVersion < 3
    /\ catalogVersion' = catalogVersion + 1
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault, published
         >>

PublishedDrift ==
    /\ phase \in {"plannedA", "plannedB"}
    /\ publishedCheckpoint < 3
    /\ publishedCheckpoint' = publishedCheckpoint + 1
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault, published
         >>

NomosEnforceDrift ==
    /\ phase \in {"ready", "plannedA", "appliedA", "plannedB"}
    /\ nomosMode # "enforce"
    /\ nomosMode' = "enforce"
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault, published
         >>

CandidateShapeDrift ==
    /\ phase \in {"plannedA", "plannedB"}
    /\ plannedRetiree \in Retirees
    /\ branchToken < 4
    /\ candidateRecords' = [candidateRecords EXCEPT ![plannedRetiree] = BadNoChunk]
    /\ branchToken' = branchToken + 1
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         publishedCheckpoint, catalogVersion, nomosMode,
         semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault, published
         >>

SemanticDrift ==
    /\ phase \in {"plannedA", "plannedB"}
    /\ plannedRetiree \in Retirees
    /\ semanticModes' = [semanticModes EXCEPT ![plannedRetiree] = "different"]
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault, published
         >>

ConfidenceDrift ==
    /\ phase \in {"plannedA", "plannedB"}
    /\ plannedRetiree \in Retirees
    /\ confidenceModes' = [confidenceModes EXCEPT ![plannedRetiree] = "weaker"]
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault, published
         >>

InjectFault ==
    /\ phase \in {"plannedA", "stagedA", "plannedB", "stagedB"}
    /\ ~fault
    /\ fault' = TRUE
    /\ UNCHANGED <<
         phase, live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, publishedCheckpoint, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, published
         >>

Publish ==
    /\ phase = "complete"
    /\ receipts = Retirees
    /\ published' = TRUE
    /\ phase' = "published"
    /\ publishedCheckpoint' = branchToken
    /\ UNCHANGED <<
         live, edges, survivorAttrs, receipts, audit, writeKeys,
         branchToken, catalogVersion, nomosMode,
         candidateRecords, semanticModes, confidenceModes,
         plannedRetiree, plannedBranchToken, plannedPublishedCheckpoint,
         plannedCatalogVersion, plannedNomosMode, plannedCandidateRecord,
         plannedSemantic, plannedConfidence, plannedDigest, planHistory,
         pending, fault
         >>

Next ==
    PlanA \/ PlanB \/ Stage \/ CommitA \/ CommitB \/ Rollback \/ Refuse
    \/ ExternalWriter \/ CatalogDrift \/ PublishedDrift
    \/ NomosEnforceDrift \/ CandidateShapeDrift \/ SemanticDrift
    \/ ConfidenceDrift \/ InjectFault \/ Publish

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"ready", "plannedA", "stagedA", "appliedA",
                  "plannedB", "stagedB", "complete", "published", "refused"}
    /\ live \subseteq NodeUniverse
    /\ edges \subseteq [src: NodeUniverse, dst: NodeUniverse, kind: EdgeKinds]
    /\ survivorAttrs \in AttrValues
    /\ receipts \subseteq Retirees
    /\ audit \in [Retirees -> SUBSET ProvenanceAtoms]
    /\ writeKeys \subseteq WriteKeyUniverse
    /\ branchToken \in TokenValues
    /\ publishedCheckpoint \in CheckpointValues
    /\ catalogVersion \in CatalogValues
    /\ nomosMode \in NomosModes
    /\ candidateRecords \in [Retirees -> CandidateRecordUniverse]
    /\ semanticModes \in [Retirees -> SemanticValues]
    /\ confidenceModes \in [Retirees -> ConfidenceValues]
    /\ plannedRetiree \in Retirees \cup {NoPlan}
    /\ plannedBranchToken \in TokenValues
    /\ plannedPublishedCheckpoint \in CheckpointValues
    /\ plannedCatalogVersion \in CatalogValues
    /\ plannedNomosMode \in NomosModes
    /\ plannedCandidateRecord \in CandidateRecordUniverse
    /\ plannedSemantic \in SemanticValues
    /\ plannedConfidence \in ConfidenceValues
    /\ IF plannedRetiree = NoPlan
       THEN plannedDigest = NoPlan
       ELSE plannedDigest \in PlanUniverse
    /\ planHistory \subseteq PlanUniverse
    /\ pending \in BOOLEAN
    /\ fault \in BOOLEAN
    /\ published \in BOOLEAN

NoDangling ==
    \A edge \in edges: edge.src \in live /\ edge.dst \in live

BatchFold ==
    /\ live = FoldLive(receipts)
    /\ edges = FoldEdges(receipts)
    /\ survivorAttrs = FoldAttrs(receipts)

PrefixPreserved ==
    RetireeA \in receipts =>
        /\ RetireeA \notin live
        /\ RetireeA \in receipts

ReceiptCoupled ==
    /\ \A retiree \in receipts: audit[retiree] = ProvenanceUnion(retiree)
    /\ Cardinality(writeKeys) = 4 * Cardinality(receipts)

PlanIdentityInjective ==
    /\ \A p \in planHistory:
         p.survivor = Survivor
         /\ p.retiree \in Retirees
         /\ p.content.survivor = Survivor
    /\ \A p, q \in planHistory:
         p # q =>
             p.retiree # q.retiree
             \/ p.branch_token # q.branch_token
             \/ p.published_checkpoint # q.published_checkpoint
             \/ p.catalog_version # q.catalog_version
             \/ p.content # q.content

WholeBranchTokenPinned ==
    phase \in {"stagedA", "stagedB"}
    => plannedBranchToken = branchToken

StagingIsPrivate ==
    phase \in {"stagedA", "stagedB"}
    =>
        /\ pending
        /\ BatchFold

RefusalPreservesPrefix ==
    phase = "refused" =>
        /\ BatchFold
        /\ pending = FALSE

CandidatePairSafety ==
    \A retiree \in receipts: CandidatePairShape(candidateRecords[retiree], retiree)

SemanticConfidenceSafety ==
    \A retiree \in receipts:
        /\ semanticModes[retiree] = "same"
        /\ confidenceModes[retiree] = "ok"

NomosEnforceNoAlias ==
    nomosMode = "enforce" => RetireeB \notin receipts

PublishedCoupling ==
    published =>
        /\ phase = "published"
        /\ receipts = Retirees
        /\ publishedCheckpoint = branchToken

=============================================================================
