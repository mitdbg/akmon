------------------- MODULE SourceRetirementLiveness -------------------
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS A, B, EmptySource, SharedSource,
          DA, DAny, DAll, DChain, DCycle1, DCycle2, DUnrelated,
          Legacy, Orphan, Curated, Chunk1, Chunk2,
          EdgeA, EdgeB, EdgeShared, EdgeDA, EdgeDAnyA, EdgeDAnyB

SourceNodes == {A, B, EmptySource, SharedSource}
DerivedNodes == {
    DA, DAny, DAll, DChain, DCycle1, DCycle2, DUnrelated, Legacy, Orphan
}
CuratedNodes == {Curated}
BaseLive == SourceNodes \cup (DerivedNodes \ {Legacy, Orphan}) \cup CuratedNodes
AllChunks == {Chunk1, Chunk2}

AllEdges == {EdgeA, EdgeB, EdgeShared, EdgeDA, EdgeDAnyA, EdgeDAnyB}

EdgeSrc == [edge \in AllEdges |->
    CASE edge = EdgeA -> A
      [] edge = EdgeB -> B
      [] edge = EdgeShared -> SharedSource
      [] edge = EdgeDA -> DA
      [] edge \in {EdgeDAnyA, EdgeDAnyB} -> DAny
]

EdgeDst == [edge \in AllEdges |->
    CASE edge \in {EdgeA, EdgeB, EdgeShared} -> Curated
      [] edge = EdgeDA -> A
      [] edge = EdgeDAnyA -> A
      [] edge = EdgeDAnyB -> B
]

EdgesWithin(edges, nodes) == {
    edge \in edges: EdgeSrc[edge] \in nodes /\ EdgeDst[edge] \in nodes
}

SupportPolicy == [node \in DerivedNodes |->
    CASE node = DAny -> "any_member"
      [] node = DCycle1 -> "any_member"
      [] node \in {Legacy, Orphan} -> "missing"
      [] OTHER -> "all_members"
]

SupportMembers == [node \in DerivedNodes |->
    CASE node = DA -> {A}
      [] node = DAny -> {A, B}
      [] node = DAll -> {A, B}
      [] node = DChain -> {DA}
      [] node = DCycle1 -> {A, DCycle2}
      [] node = DCycle2 -> {DCycle1}
      [] node = DUnrelated -> {B}
      [] node = Legacy -> {A}
      [] node = Orphan -> {}
]

KnownPolicy(node) == SupportPolicy[node] \in {"any_member", "all_members"}

SupportStep(node, grounded) ==
    /\ KnownPolicy(node)
    /\ IF SupportPolicy[node] = "any_member"
          THEN SupportMembers[node] \cap grounded # {}
          ELSE SupportMembers[node] \subseteq grounded

RECURSIVE Grounded(_, _)
Grounded(live, depth) ==
    IF depth = 0
    THEN live \ DerivedNodes
    ELSE LET prior == Grounded(live, depth - 1)
         IN prior \cup {
             node \in (live \cap DerivedNodes): SupportStep(node, prior)
         }

GroundedLive(live) == Grounded(live, Cardinality(DerivedNodes))
Unsupported(live) == (live \cap DerivedNodes) \ GroundedLive(live)
Malformed(live) == {node \in (live \cap DerivedNodes): ~KnownPolicy(node)}

AffectedFrontier(target) ==
    IF target = A
    THEN {DA, DAny, DAll, DChain, DCycle1, DCycle2, Legacy}
    ELSE {}

CleanupSet(live, target) ==
    (Unsupported(live) \cap AffectedFrontier(target)) \ Malformed(live)

VARIABLES implementationStage, retirementTarget, requestedDeployment,
          applyDeployment, identityAmbiguous, sharedSourceCollision, phase,
          live, liveEdges, baselineSnapshot, baselineEdges,
          publishedSnapshot, publishedEdges, latestPublished, registryRevision, branchHead,
          footprintVersion, policyVersion, plan, intentState, chunkReceipts,
          terminalReceipt, registryActive, graphEffects, replayCount, refusalCode

vars == <<implementationStage, retirementTarget, requestedDeployment,
          applyDeployment, identityAmbiguous, sharedSourceCollision, phase,
          live, liveEdges, baselineSnapshot, baselineEdges,
          publishedSnapshot, publishedEdges, latestPublished, registryRevision, branchHead,
          footprintVersion, policyVersion, plan, intentState, chunkReceipts,
          terminalReceipt, registryActive, graphEffects, replayCount, refusalCode>>

NodeOwners(node) ==
    CASE node = A -> {A}
      [] node = B -> {B}
      [] node = EmptySource -> {EmptySource}
      [] node = SharedSource ->
            IF sharedSourceCollision THEN {A, B} ELSE {B}

RetirementFootprint(target) ==
    {node \in SourceNodes: NodeOwners(node) = {target}}

RetirementEdges(target) == {
    edge \in AllEdges: EdgeSrc[edge] \in RetirementFootprint(target)
}

EmptyPlan == [
    deployment |-> "",
    source |-> A,
    registry_revision |-> 0,
    branch_head |-> 0,
    footprint_version |-> 0,
    policy_version |-> 0,
    frontier |-> {}
]

CurrentPlan == [
    deployment |-> requestedDeployment,
    source |-> retirementTarget,
    registry_revision |-> registryRevision,
    branch_head |-> branchHead,
    footprint_version |-> footprintVersion,
    policy_version |-> policyVersion,
    frontier |-> AffectedFrontier(retirementTarget) \cap live
]

FreshPlan == plan = CurrentPlan

\* Finite representative for PR2's graph-row producer collision. The shared
\* boolean models an ownership collision when retiring A; on EmptySource in
\* PR2 it models a different producer attempting to replace the same derived
\* node id. The cases are disjoint and both must refuse before graph effects.
ProducerNodeCollision ==
    implementationStage = "pr2" /\ retirementTarget = EmptySource
        /\ sharedSourceCollision

Init ==
    /\ implementationStage \in {"pr1", "pr2"}
    /\ retirementTarget \in {A, EmptySource}
    /\ requestedDeployment = "disposable"
    /\ applyDeployment \in {"disposable", "other"}
    /\ identityAmbiguous \in BOOLEAN
    /\ sharedSourceCollision \in BOOLEAN
    /\ phase = "requested"
    /\ live \in {BaseLive, BaseLive \cup {Legacy}}
    /\ liveEdges = AllEdges
    /\ baselineSnapshot = live
    /\ baselineEdges = liveEdges
    /\ publishedSnapshot = {}
    /\ publishedEdges = {}
    /\ latestPublished = 0
    /\ registryRevision = 0
    /\ branchHead = 0
    /\ footprintVersion = 0
    /\ policyVersion = 0
    /\ plan = EmptyPlan
    /\ intentState = "none"
    /\ chunkReceipts = {}
    /\ terminalReceipt = FALSE
    /\ registryActive = TRUE
    /\ graphEffects = 0
    /\ replayCount = 0
    /\ refusalCode = "none"

Plan ==
    /\ phase = "requested"
    /\ phase' = "planned"
    /\ plan' = CurrentPlan
    /\ refusalCode' = "none"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, intentState,
                    chunkReceipts, terminalReceipt, registryActive, graphEffects,
                    replayCount>>

ChangeRegistry ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ registryRevision' = registryRevision + 1
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, phase, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, branchHead,
                    footprintVersion, policyVersion, plan, intentState,
                    chunkReceipts, terminalReceipt, registryActive,
                    graphEffects, replayCount, refusalCode>>

ChangeBranch ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ branchHead' = branchHead + 1
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, phase, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    footprintVersion, policyVersion, plan, intentState,
                    chunkReceipts, terminalReceipt, registryActive,
                    graphEffects, replayCount, refusalCode>>

ChangeFootprint ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ footprintVersion' = footprintVersion + 1
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, phase, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, policyVersion, plan, intentState, chunkReceipts,
                    terminalReceipt, registryActive, graphEffects, replayCount, refusalCode>>

ChangePolicy ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ policyVersion' = policyVersion + 1
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, phase, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, plan, intentState,
                    chunkReceipts, terminalReceipt, registryActive,
                    graphEffects, replayCount, refusalCode>>

RefuseStale ==
    /\ phase = "planned"
    /\ ~FreshPlan
    /\ phase' = "stale_refused"
    /\ refusalCode' = "stale_plan"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount>>

RefuseIdentity ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ identityAmbiguous
    /\ phase' = "identity_refused"
    /\ refusalCode' = "ambiguous_source_identity"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount>>

RefuseDeployment ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ applyDeployment # plan.deployment
    /\ phase' = "deployment_refused"
    /\ refusalCode' = "cross_deployment_plan"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount>>

RefuseSharedSource ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ retirementTarget = A
    /\ sharedSourceCollision
    /\ phase' = "collision_refused"
    /\ refusalCode' = "shared_source_ownership_unresolved"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount>>

RefuseProducerNodeCollision ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ ProducerNodeCollision
    /\ phase' = "producer_collision_refused"
    /\ refusalCode' = "derived_node_producer_collision"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount>>

RefuseSupport ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ implementationStage = "pr1"
    /\ plan.frontier # {}
    /\ phase' = "support_refused"
    /\ refusalCode' = "derived_liveness_required"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount>>

BeginApply ==
    /\ phase = "planned"
    /\ FreshPlan
    /\ applyDeployment = plan.deployment
    /\ ~identityAmbiguous
    /\ ~(retirementTarget = A /\ sharedSourceCollision)
    /\ ~ProducerNodeCollision
    /\ (implementationStage = "pr2" \/ plan.frontier = {})
    /\ phase' = "applying"
    /\ intentState' = "recorded"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    chunkReceipts, terminalReceipt, registryActive,
                    graphEffects, replayCount, refusalCode>>

ApplyChunk(chunk) ==
    /\ phase = "applying"
    /\ chunk \in AllChunks \ chunkReceipts
    /\ chunkReceipts' = chunkReceipts \cup {chunk}
    /\ graphEffects' = graphEffects + 1
    /\ live' = IF chunk = Chunk1
                THEN live \ RetirementFootprint(retirementTarget)
                ELSE live
    /\ liveEdges' = IF chunk = Chunk2
                     THEN liveEdges \ RetirementEdges(retirementTarget)
                     ELSE liveEdges
    /\ phase' = IF chunkReceipts \cup {chunk} = AllChunks
                 THEN "source_retracted" ELSE "applying"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead,
                    footprintVersion, policyVersion, plan, intentState,
                    terminalReceipt, registryActive, replayCount, refusalCode>>

RetryAppliedChunk ==
    /\ phase = "applying"
    /\ chunkReceipts # {}
    /\ UNCHANGED vars

Crash ==
    /\ phase = "applying"
    /\ intentState = "recorded"
    /\ phase' = "recovering"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

Resume ==
    /\ phase = "recovering"
    /\ intentState = "recorded"
    /\ phase' = "applying"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

CrashAfterGraph ==
    /\ phase = "source_retracted"
    /\ phase' = "recovering_graph"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

ResumeAfterGraph ==
    /\ phase = "recovering_graph"
    /\ phase' = "source_retracted"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

RecomputeSupport ==
    /\ phase = "source_retracted"
    /\ implementationStage = "pr2"
    /\ phase' = "support_recomputed"
    /\ LET cleaned == live \ CleanupSet(live, retirementTarget)
       IN /\ live' = cleaned
          /\ liveEdges' = EdgesWithin(liveEdges, cleaned)
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead,
                    footprintVersion, policyVersion, plan, intentState,
                    chunkReceipts, terminalReceipt, registryActive,
                    graphEffects, replayCount, refusalCode>>

SkipSupport ==
    /\ phase = "source_retracted"
    /\ implementationStage = "pr1"
    /\ plan.frontier = {}
    /\ phase' = "support_recomputed"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

InjectUnsupported ==
    /\ phase = "support_recomputed"
    /\ implementationStage = "pr2"
    /\ Orphan \notin live
    /\ live' = live \cup {Orphan}
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, phase, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

CrashAfterSupport ==
    /\ phase = "support_recomputed"
    /\ ~terminalReceipt
    /\ phase' = "recovering_support"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

ResumeAfterSupport ==
    /\ phase = "recovering_support"
    /\ phase' = "support_recomputed"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

RecordTerminalReceipt ==
    /\ phase = "support_recomputed"
    /\ Unsupported(live) = {}
    /\ chunkReceipts = AllChunks
    /\ terminalReceipt' = TRUE
    /\ intentState' = "terminal"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, phase, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    chunkReceipts, registryActive, graphEffects, replayCount, refusalCode>>

CrashAfterTerminal ==
    /\ phase = "support_recomputed"
    /\ terminalReceipt
    /\ phase' = "recovering_terminal"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

ResumeAfterTerminal ==
    /\ phase = "recovering_terminal"
    /\ phase' = "support_recomputed"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

CheckSuccess ==
    /\ phase = "support_recomputed"
    /\ terminalReceipt
    /\ Unsupported(live) = {}
    /\ phase' = "checked"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount, refusalCode>>

CheckBlocked ==
    /\ phase = "support_recomputed"
    /\ Unsupported(live) # {}
    /\ phase' = "blocked"
    /\ refusalCode' = IF Malformed(live) # {}
                       THEN "missing_support_policy"
                       ELSE "unsupported_derived_node"
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, replayCount>>

Publish ==
    /\ phase = "checked"
    /\ terminalReceipt
    /\ phase' = "published"
    /\ latestPublished' = 1
    /\ publishedSnapshot' = live
    /\ publishedEdges' = liveEdges
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, live, liveEdges, baselineSnapshot, baselineEdges,
                    registryRevision, branchHead, footprintVersion,
                    policyVersion, plan, intentState, chunkReceipts,
                    terminalReceipt, registryActive, graphEffects, replayCount, refusalCode>>

DisableRegistry ==
    /\ phase = "published"
    /\ terminalReceipt
    /\ registryActive' = FALSE
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment, applyDeployment, identityAmbiguous,
                    sharedSourceCollision, phase, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished, registryRevision,
                    branchHead, footprintVersion, policyVersion, plan,
                    intentState, chunkReceipts, terminalReceipt,
                    graphEffects, replayCount, refusalCode>>

RepeatedApply ==
    /\ phase = "published"
    /\ terminalReceipt
    /\ replayCount = 0
    /\ replayCount' = 1
    /\ UNCHANGED <<implementationStage, retirementTarget, requestedDeployment,
                    applyDeployment, identityAmbiguous, sharedSourceCollision,
                    phase, live, liveEdges, baselineSnapshot, baselineEdges,
                    publishedSnapshot, publishedEdges, latestPublished,
                    registryRevision, branchHead, footprintVersion, policyVersion,
                    plan, intentState, chunkReceipts, terminalReceipt,
                    registryActive, graphEffects, refusalCode>>

Noop == UNCHANGED vars

Next == Plan \/ ChangeRegistry \/ ChangeBranch \/ ChangeFootprint \/
        ChangePolicy \/ RefuseStale \/ RefuseIdentity \/ RefuseDeployment \/ RefuseSharedSource \/
        RefuseProducerNodeCollision \/
        RefuseSupport \/ BeginApply \/ (\E chunk \in AllChunks: ApplyChunk(chunk)) \/
        RetryAppliedChunk \/ Crash \/ Resume \/ CrashAfterGraph \/ ResumeAfterGraph \/
        RecomputeSupport \/ SkipSupport \/ InjectUnsupported \/
        CrashAfterSupport \/ ResumeAfterSupport \/ RecordTerminalReceipt \/
        CrashAfterTerminal \/ ResumeAfterTerminal \/ CheckSuccess \/
        CheckBlocked \/ Publish \/ DisableRegistry \/ RepeatedApply \/ Noop

TypeOK ==
    /\ implementationStage \in {"pr1", "pr2"}
    /\ retirementTarget \in {A, EmptySource}
    /\ requestedDeployment \in {"disposable"}
    /\ applyDeployment \in {"disposable", "other"}
    /\ identityAmbiguous \in BOOLEAN
    /\ sharedSourceCollision \in BOOLEAN
    /\ phase \in {"requested", "planned", "stale_refused",
                   "identity_refused", "deployment_refused", "collision_refused",
                   "producer_collision_refused",
                   "support_refused", "applying", "recovering",
                   "source_retracted", "recovering_graph",
                   "support_recomputed", "recovering_support",
                   "recovering_terminal", "checked", "blocked", "published"}
    /\ live \subseteq SourceNodes \cup DerivedNodes \cup CuratedNodes
    /\ liveEdges \subseteq AllEdges
    /\ baselineSnapshot \subseteq SourceNodes \cup DerivedNodes \cup CuratedNodes
    /\ baselineEdges \subseteq AllEdges
    /\ publishedSnapshot \subseteq SourceNodes \cup DerivedNodes \cup CuratedNodes
    /\ publishedEdges \subseteq AllEdges
    /\ latestPublished \in {0, 1}
    /\ registryRevision \in Nat
    /\ branchHead \in Nat
    /\ footprintVersion \in Nat
    /\ policyVersion \in Nat
    /\ chunkReceipts \subseteq AllChunks
    /\ intentState \in {"none", "recorded", "terminal"}
    /\ terminalReceipt \in BOOLEAN
    /\ registryActive \in BOOLEAN
    /\ graphEffects \in Nat
    /\ replayCount \in {0, 1}

UnrelatedContentPreserved ==
    (SourceNodes \ RetirementFootprint(retirementTarget)) \cup CuratedNodes
        \subseteq live

PR1FrontierRefused ==
    implementationStage = "pr1" /\ AffectedFrontier(retirementTarget) # {} =>
        phase \notin {"applying", "recovering", "source_retracted",
                      "support_recomputed", "checked", "blocked", "published"}

StalePlanHasNoEffect ==
    phase = "stale_refused" =>
        /\ intentState = "none"
        /\ chunkReceipts = {}
        /\ graphEffects = 0
        /\ registryActive

IdentityRefusalHasNoEffect ==
    phase \in {"identity_refused", "deployment_refused", "collision_refused",
               "producer_collision_refused",
               "support_refused"} =>
        /\ intentState = "none"
        /\ chunkReceipts = {}
        /\ graphEffects = 0
        /\ registryActive

ProducerCollisionHasNoEffect ==
    phase = "producer_collision_refused" =>
        /\ ProducerNodeCollision
        /\ intentState = "none"
        /\ chunkReceipts = {}
        /\ graphEffects = 0
        /\ live = baselineSnapshot
        /\ registryActive

ChunkReplayIdempotent == graphEffects = Cardinality(chunkReceipts)

RepeatedApplyIdempotent == replayCount = 1 =>
    /\ graphEffects = Cardinality(chunkReceipts)
    /\ publishedSnapshot = live
    /\ publishedEdges = liveEdges

TerminalReceiptComplete == terminalReceipt => chunkReceipts = AllChunks

ReceiptBeforeRegistryDisable == ~registryActive => terminalReceipt

BlockedPublishInvisible == phase = "blocked" => latestPublished = 0

PublishedSupportSound == phase = "published" => Unsupported(live) = {}

NoDanglingPublishedEdges == phase = "published" =>
    publishedEdges = EdgesWithin(publishedEdges, publishedSnapshot)

PublishedPointerAtomic == latestPublished = 1 => phase = "published"

HistoricalSnapshotRetained ==
    /\ baselineSnapshot \subseteq BaseLive \cup {Legacy}
    /\ baselineEdges = AllEdges

PinnedHistoryPreserved ==
    latestPublished = 1 /\ retirementTarget = A =>
        /\ A \in baselineSnapshot
        /\ A \notin publishedSnapshot
        /\ EdgeA \in baselineEdges
        /\ EdgeA \notin publishedEdges

AnyAndAllSemantics ==
    phase = "published" /\ retirementTarget = A =>
        /\ DAny \in publishedSnapshot
        /\ DAll \notin publishedSnapshot
        /\ DA \notin publishedSnapshot
        /\ DChain \notin publishedSnapshot
        /\ DCycle1 \notin publishedSnapshot
        /\ DCycle2 \notin publishedSnapshot
        /\ DUnrelated \in publishedSnapshot

MissingPolicyNeverDefaults ==
    Legacy \in live /\ Legacy \in AffectedFrontier(retirementTarget) =>
        phase \notin {"checked", "published"}

\* Temporal closure: every state on every behavior remains inside the explicit
\* recovery/refusal/terminal state space described by TypeOK. Eventual runtime
\* completion depends on worker scheduling and is an integration obligation.
RecoveryStateRemainsExplicit == []TypeOK

Spec == Init /\ [][Next]_vars

=======================================================================
