---- MODULE AttributeState ----
EXTENDS Naturals, FiniteSets

CONSTANTS
    Attrs,
    NodeAttrs,
    EdgeAttrs,
    Values,
    Observations,
    NoObservation,
    ReadSurfaces,
    NoReadSurface,
    PromotableAttrs,
    ReservedAttrs,
    BuggyViewMaintenanceSkipsCopy

Statuses == {"absent", "candidate", "accepted", "rejected", "superseded", "stale"}
ObservationRefs == Observations \cup {NoObservation}

DefaultAttr == CHOOSE attr \in Attrs: TRUE
DefaultValue == CHOOSE value \in Values: TRUE

DefaultObservation == [
    attr |-> DefaultAttr,
    value |-> DefaultValue,
    status |-> "absent",
    raw |-> FALSE,
    external |-> FALSE,
    published |-> FALSE,
    supersedes |-> NoObservation
]

ObservationRecord == [
    attr: Attrs,
    value: Values,
    status: Statuses,
    raw: BOOLEAN,
    external: BOOLEAN,
    published: BOOLEAN,
    supersedes: ObservationRefs
]

ReadRecord == [
    surface: ReadSurfaces \cup {NoReadSurface},
    attr: Attrs,
    obs: ObservationRefs
]

Observation(targetAttr, attrValue, attrStatus, isRaw, isExternal, isPublished, supersededObs) ==
    [
        attr |-> targetAttr,
        value |-> attrValue,
        status |-> attrStatus,
        raw |-> isRaw,
        external |-> isExternal,
        published |-> isPublished,
        supersedes |-> supersededObs
    ]

NoRead == [
    surface |-> NoReadSurface,
    attr |-> DefaultAttr,
    obs |-> NoObservation
]

VARIABLES history, effective, pinnedEffective, pinOpen, externalLive, promoted, retained, read,
          viewRow, viewFresh

vars == <<history, effective, pinnedEffective, pinOpen, externalLive, promoted, retained, read,
          viewRow, viewFresh>>

\* viewRow models the maintained overlay validity ranges (okg.attribute_overlay_ranges): a
\* materialized projection distinct from the ground-truth `effective` function.
\* Effective-changing transitions leave it behind (viewFresh=FALSE); the lazy
\* MaintainView step copies `effective` into it and re-marks it fresh. Reads
\* resolve off viewRow, so a view that lags truth is a representable, witnessable
\* state -- not conflated with truth as in the pre-strengthening model.

Known(obs) == history[obs].status # "absent"
Materialized(obs) == Known(obs) /\ history[obs].published /\ ~history[obs].external
Visible(obs) == Materialized(obs) /\ history[obs].status = "accepted"
TerminalStatuses == {"rejected", "superseded", "stale"}

TypeOK ==
    /\ NoObservation \notin Observations
    /\ NoReadSurface \notin ReadSurfaces
    /\ PromotableAttrs \subseteq Attrs
    /\ ReservedAttrs \subseteq Attrs
    /\ history \in [Observations -> ObservationRecord]
    /\ effective \in [Attrs -> ObservationRefs]
    /\ pinnedEffective \in [Attrs -> ObservationRefs]
    /\ pinOpen \in BOOLEAN
    /\ externalLive \in [Attrs -> ObservationRefs]
    /\ promoted \subseteq Observations
    /\ retained \subseteq Observations
    /\ read \in ReadRecord
    /\ viewRow \in [Attrs -> ObservationRefs]
    /\ viewFresh \in BOOLEAN

NodeEdgeSymmetry ==
    /\ NodeAttrs \subseteq Attrs
    /\ EdgeAttrs \subseteq Attrs
    /\ NodeAttrs # {}
    /\ EdgeAttrs # {}
    /\ NodeAttrs \cap EdgeAttrs = {}

Init ==
    /\ history = [obs \in Observations |-> DefaultObservation]
    /\ effective = [attr \in Attrs |-> NoObservation]
    /\ pinnedEffective = [attr \in Attrs |-> NoObservation]
    /\ pinOpen = FALSE
    /\ externalLive = [attr \in Attrs |-> NoObservation]
    /\ promoted = {}
    /\ retained = {}
    /\ read = NoRead
    /\ viewRow = [attr \in Attrs |-> NoObservation]
    /\ viewFresh = TRUE

RecordRaw(obs, attr, value) ==
    /\ obs \in Observations
    /\ attr \in Attrs
    /\ value \in Values
    /\ ~Known(obs)
    /\ history' = [history EXCEPT ![obs] =
        Observation(attr, value, "accepted", TRUE, FALSE, TRUE, NoObservation)]
    /\ effective' =
        IF effective[attr] = NoObservation
        THEN [effective EXCEPT ![attr] = obs]
        ELSE IF history[effective[attr]].raw
        THEN [effective EXCEPT ![attr] = obs]
        ELSE effective
    /\ retained' = retained \cup {obs}
    /\ viewFresh' = FALSE
    /\ UNCHANGED <<pinnedEffective, pinOpen, externalLive, promoted, read, viewRow>>

RecordCandidate(obs, attr, value) ==
    /\ obs \in Observations
    /\ attr \in Attrs
    /\ value \in Values
    /\ ~Known(obs)
    /\ history' = [history EXCEPT ![obs] =
        Observation(attr, value, "candidate", FALSE, FALSE, TRUE, NoObservation)]
    /\ retained' = retained \cup {obs}
    /\ UNCHANGED <<effective, pinnedEffective, pinOpen, externalLive, promoted, read,
                   viewRow, viewFresh>>

AcceptCandidate(obs) ==
    /\ obs \in Observations
    /\ Known(obs)
    /\ history[obs].status = "candidate"
    /\ history[obs].published
    /\ ~history[obs].external
    /\ LET attr == history[obs].attr IN
        /\ IF attr \in ReservedAttrs
           THEN TRUE
           ELSE IF effective[attr] = NoObservation
                THEN TRUE
                ELSE history[effective[attr]].raw
        /\ history' = [history EXCEPT ![obs].status = "accepted"]
        /\ effective' =
            IF attr \in ReservedAttrs
            THEN effective
            ELSE [effective EXCEPT ![attr] = obs]
    /\ retained' = retained \cup {obs}
    /\ viewFresh' = FALSE
    /\ UNCHANGED <<pinnedEffective, pinOpen, externalLive, promoted, read, viewRow>>

RejectCandidate(obs) ==
    /\ obs \in Observations
    /\ Known(obs)
    /\ history[obs].status = "candidate"
    /\ history' = [history EXCEPT ![obs].status = "rejected"]
    /\ retained' = retained \cup {obs}
    /\ UNCHANGED <<effective, pinnedEffective, pinOpen, externalLive, promoted, read,
                   viewRow, viewFresh>>

SupersedeOverlay(old, new, value) ==
    /\ old \in Observations
    /\ new \in Observations
    /\ old # new
    /\ value \in Values
    /\ Visible(old)
    /\ ~history[old].raw
    /\ ~Known(new)
    /\ LET attr == history[old].attr IN
        /\ effective[attr] = old
        /\ history' = [history EXCEPT ![old].status = "superseded", ![new] = Observation(attr, value, "accepted", FALSE, FALSE, TRUE, old)]
        /\ effective' = [effective EXCEPT ![attr] = new]
    /\ promoted' = promoted \ {old}
    /\ retained' = retained \cup {old, new}
    /\ viewFresh' = FALSE
    /\ UNCHANGED <<pinnedEffective, pinOpen, externalLive, read, viewRow>>

InvalidateEffective(obs) ==
    /\ obs \in Observations
    /\ Visible(obs)
    /\ ~history[obs].raw
    /\ LET attr == history[obs].attr IN
        /\ history' = [history EXCEPT ![obs].status = "stale"]
        /\ effective' = IF effective[attr] = obs THEN [effective EXCEPT ![attr] = NoObservation] ELSE effective
        /\ promoted' = promoted \ {obs}
    /\ retained' = retained \cup {obs}
    /\ viewFresh' = FALSE
    /\ UNCHANGED <<pinnedEffective, pinOpen, externalLive, read, viewRow>>

HydrateExternalLive(obs, attr, value) ==
    /\ obs \in Observations
    /\ attr \in Attrs
    /\ value \in Values
    /\ ~Known(obs)
    /\ history' = [history EXCEPT ![obs] =
        Observation(attr, value, "candidate", FALSE, TRUE, FALSE, NoObservation)]
    /\ externalLive' = [externalLive EXCEPT ![attr] = obs]
    /\ retained' = retained \cup {obs}
    /\ UNCHANGED <<effective, pinnedEffective, pinOpen, promoted, read, viewRow, viewFresh>>

MaterializeDerived(obs, attr, value) ==
    /\ obs \in Observations
    /\ attr \in Attrs
    /\ value \in Values
    /\ ~Known(obs)
    /\ IF attr \in ReservedAttrs
       THEN TRUE
       ELSE IF effective[attr] = NoObservation
            THEN TRUE
            ELSE history[effective[attr]].raw
    /\ history' = [history EXCEPT ![obs] =
        Observation(attr, value, "accepted", FALSE, FALSE, TRUE, NoObservation)]
    /\ effective' =
        IF attr \in ReservedAttrs
        THEN effective
        ELSE [effective EXCEPT ![attr] = obs]
    /\ retained' = retained \cup {obs}
    /\ viewFresh' = FALSE
    /\ UNCHANGED <<pinnedEffective, pinOpen, externalLive, promoted, read, viewRow>>

PromoteToUserGraph(obs) ==
    /\ obs \in Observations
    /\ Visible(obs)
    /\ history[obs].attr \in PromotableAttrs
    /\ promoted' = promoted \cup {obs}
    /\ retained' = retained \cup {obs}
    /\ UNCHANGED <<history, effective, pinnedEffective, pinOpen, externalLive, read,
                   viewRow, viewFresh>>

PrunableExternalLive(obs) ==
    /\ obs \in Observations
    /\ Known(obs)
    /\ history[obs].external
    /\ ~history[obs].published
    /\ history[obs].status = "candidate"
    /\ obs \notin promoted
    /\ \A attr \in Attrs: effective[attr] # obs

PruneRetention(obs) ==
    /\ obs \in retained
    /\ PrunableExternalLive(obs)
    /\ LET attr == history[obs].attr IN
        /\ retained' = retained \ {obs}
        /\ externalLive' =
            IF externalLive[attr] = obs
            THEN [externalLive EXCEPT ![attr] = NoObservation]
            ELSE externalLive
    /\ UNCHANGED <<history, effective, pinnedEffective, pinOpen, promoted, read,
                   viewRow, viewFresh>>

\* MaintainView is the per-publish overlay-range sync (sync_overlay_ranges_for_generation).
\* Positive: copy the ground-truth `effective` projection into the maintained
\* view and mark it fresh. Under the BuggyViewMaintenanceSkipsCopy toggle it
\* claims freshness without copying, so a view that already lags truth stays
\* stale while reads believe it current -- the modeled production hazard.
MaintainView ==
    /\ viewRow' = IF BuggyViewMaintenanceSkipsCopy THEN viewRow ELSE effective
    /\ viewFresh' = TRUE
    /\ UNCHANGED <<history, effective, pinnedEffective, pinOpen, externalLive,
                   promoted, retained, read>>

ReadSurface(surface, attr) ==
    /\ surface \in ReadSurfaces
    /\ attr \in Attrs
    \* Reads resolve off the maintained view (viewRow), not ground-truth
    \* `effective`; the pin snapshots the view for a stable read.
    /\ pinnedEffective' = IF pinOpen THEN pinnedEffective ELSE viewRow
    /\ pinOpen' = TRUE
    /\ LET pinned == IF pinOpen THEN pinnedEffective ELSE viewRow IN
        read' = [surface |-> surface, attr |-> attr, obs |-> pinned[attr]]
    /\ UNCHANGED <<history, effective, externalLive, promoted, retained,
                   viewRow, viewFresh>>

Next ==
    \/ \E obs \in Observations, attr \in Attrs, value \in Values:
        RecordRaw(obs, attr, value)
    \/ \E obs \in Observations, attr \in Attrs, value \in Values:
        RecordCandidate(obs, attr, value)
    \/ \E obs \in Observations:
        AcceptCandidate(obs)
    \/ \E obs \in Observations:
        RejectCandidate(obs)
    \/ \E old \in Observations, new \in Observations, value \in Values:
        SupersedeOverlay(old, new, value)
    \/ \E obs \in Observations:
        InvalidateEffective(obs)
    \/ \E obs \in Observations, attr \in Attrs, value \in Values:
        HydrateExternalLive(obs, attr, value)
    \/ \E obs \in Observations, attr \in Attrs, value \in Values:
        MaterializeDerived(obs, attr, value)
    \/ \E obs \in Observations:
        PromoteToUserGraph(obs)
    \/ \E obs \in Observations:
        PruneRetention(obs)
    \/ MaintainView
    \/ \E surface \in ReadSurfaces, attr \in Attrs:
        ReadSurface(surface, attr)

Spec == Init /\ [][Next]_vars

EffectiveKnown ==
    \A attr \in Attrs:
        IF effective[attr] = NoObservation
        THEN TRUE
        ELSE Known(effective[attr])

EffectiveMatchesAttribute ==
    \A attr \in Attrs:
        IF effective[attr] = NoObservation
        THEN TRUE
        ELSE history[effective[attr]].attr = attr

EffectiveOnlyAcceptedProjection ==
    \A attr \in Attrs:
        IF effective[attr] = NoObservation
        THEN TRUE
        ELSE Visible(effective[attr])

PinnedSnapshotKnown ==
    \A attr \in Attrs:
        IF pinnedEffective[attr] = NoObservation
        THEN TRUE
        ELSE Known(pinnedEffective[attr])

PinnedSnapshotOnlyMaterializedProjection ==
    \A attr \in Attrs:
        IF pinnedEffective[attr] = NoObservation
        THEN TRUE
        ELSE Materialized(pinnedEffective[attr])

PinnedSnapshotNeverUsesExternalLive ==
    \A attr \in Attrs:
        pinnedEffective[attr] = NoObservation \/ externalLive[attr] # pinnedEffective[attr]

RejectedNeverEffective ==
    \A obs \in Observations:
        history[obs].status = "rejected" =>
            \A attr \in Attrs: effective[attr] # obs

SupersededNeverEffective ==
    \A obs \in Observations:
        history[obs].status = "superseded" =>
            \A attr \in Attrs: effective[attr] # obs

StaleNeverEffective ==
    \A obs \in Observations:
        history[obs].status = "stale" =>
            \A attr \in Attrs: effective[attr] # obs

ExternalLiveMatchesAttribute ==
    \A attr \in Attrs:
        IF externalLive[attr] = NoObservation
        THEN TRUE
        ELSE
            /\ Known(externalLive[attr])
            /\ history[externalLive[attr]].attr = attr
            /\ history[externalLive[attr]].external

ExternalLiveNeverEffective ==
    \A attr \in Attrs:
        externalLive[attr] = NoObservation \/ effective[attr] # externalLive[attr]

RawSourceObservationImmutable ==
    \A obs \in Observations:
        history[obs].raw =>
            /\ history[obs].status = "accepted"
            /\ history[obs].published
            /\ ~history[obs].external
            /\ history[obs].supersedes = NoObservation

SupersededKeepsHistory ==
    \A obs \in Observations:
        history[obs].status = "superseded" =>
            \E replacement \in Observations:
                Known(replacement) /\ history[replacement].supersedes = obs

AcceptedOverlayIsEffectiveOrSuperseded ==
    \A obs \in Observations:
        /\ Known(obs)
        /\ history[obs].status = "accepted"
        /\ ~history[obs].raw
        /\ ~history[obs].external
        /\ history[obs].attr \notin ReservedAttrs
        => \/ effective[history[obs].attr] = obs
           \/ \E replacement \in Observations:
                Known(replacement) /\ history[replacement].supersedes = obs

ReservedEffectiveOnlyRaw ==
    \A attr \in ReservedAttrs:
        effective[attr] = NoObservation \/ history[effective[attr]].raw

ReservedPinnedEffectiveOnlyRaw ==
    \A attr \in ReservedAttrs:
        pinnedEffective[attr] = NoObservation \/ history[pinnedEffective[attr]].raw

OneEffectivePerAttribute ==
    \A attrA \in Attrs:
        \A attrB \in Attrs:
            /\ effective[attrA] # NoObservation
            /\ effective[attrA] = effective[attrB]
            => attrA = attrB

PromotedOnlyAcceptedMaterialized ==
    \A obs \in promoted:
        Visible(obs)

PromotedOnlyPromotable ==
    \A obs \in promoted:
        history[obs].attr \in PromotableAttrs

TerminalObservationRetained ==
    \A obs \in Observations:
        history[obs].status \in TerminalStatuses => Known(obs)

ReadSurfaceCoverage ==
    ReadSurfaces # {}

LastReadUsesEffective ==
    IF read.surface = NoReadSurface
    THEN TRUE
    ELSE read.obs = pinnedEffective[read.attr]

LastReadOnlyAcceptedProjection ==
    IF read.surface = NoReadSurface \/ read.obs = NoObservation
    THEN TRUE
    ELSE Materialized(read.obs)

LastReadNeverUsesExternalLive ==
    IF read.surface = NoReadSurface \/ read.obs = NoObservation
    THEN TRUE
    ELSE externalLive[read.attr] # read.obs

\* The maintained view, once marked fresh by MaintainView, equals the
\* ground-truth effective projection. Load-bearing: the BuggyViewMaintenanceSkipsCopy
\* toggle makes MaintainView set viewFresh without copying, so a view that lags
\* truth is served as current -- a reachable stale-view TRACE refutes this by
\* name (Init has no drift; only a maintenance-after-change sequence exposes it).
ViewMatchesEffectiveWhenFresh ==
    viewFresh => \A attr \in Attrs: viewRow[attr] = effective[attr]

\* Even while lagging, the view only ever holds materialized observations copied
\* from `effective` (never external-live or unknown rows). Holds positive and
\* under the copy-skip toggle, so it is not the witness target.
ViewOnlyHoldsMaterialized ==
    \A attr \in Attrs:
        viewRow[attr] = NoObservation \/ Materialized(viewRow[attr])

ViewMatchesAttribute ==
    \A attr \in Attrs:
        viewRow[attr] = NoObservation \/ history[viewRow[attr]].attr = attr

ViewNeverUsesExternalLive ==
    \A attr \in Attrs:
        viewRow[attr] = NoObservation \/ externalLive[attr] # viewRow[attr]

RetainedObservationsKnown ==
    \A obs \in retained:
        Known(obs)

AuditCriticalObservation(obs) ==
    /\ obs \in Observations
    /\ Known(obs)
    /\ \/ history[obs].raw
       \/ history[obs].status \in TerminalStatuses
       \/ obs \in promoted
       \/ \E attr \in Attrs: effective[attr] = obs

AuditCriticalRetained ==
    \A obs \in Observations:
        AuditCriticalObservation(obs) => obs \in retained

UnretainedOnlyNonAudit ==
    \A obs \in Observations:
        /\ Known(obs)
        /\ obs \notin retained
        => ~AuditCriticalObservation(obs)

AttributeReadSurfaceSafety ==
    /\ ReadSurfaceCoverage
    /\ PinnedSnapshotKnown
    /\ PinnedSnapshotOnlyMaterializedProjection
    /\ PinnedSnapshotNeverUsesExternalLive
    /\ LastReadUsesEffective
    /\ LastReadOnlyAcceptedProjection
    /\ LastReadNeverUsesExternalLive
    /\ ReservedPinnedEffectiveOnlyRaw

AttributeRetentionSafety ==
    /\ RetainedObservationsKnown
    /\ AuditCriticalRetained
    /\ UnretainedOnlyNonAudit

AttributePromotionLifecycleSafety ==
    /\ PromotedOnlyAcceptedMaterialized
    /\ PromotedOnlyPromotable

AttributeConflictSafety ==
    /\ RawSourceObservationImmutable
    /\ AcceptedOverlayIsEffectiveOrSuperseded
    /\ OneEffectivePerAttribute
    /\ ReservedEffectiveOnlyRaw

AttributeViewMaintenanceSafety ==
    /\ ViewMatchesEffectiveWhenFresh
    /\ ViewOnlyHoldsMaterialized
    /\ ViewMatchesAttribute
    /\ ViewNeverUsesExternalLive

AttributeStateSafety ==
    /\ TypeOK
    /\ NodeEdgeSymmetry
    /\ EffectiveKnown
    /\ EffectiveMatchesAttribute
    /\ EffectiveOnlyAcceptedProjection
    /\ PinnedSnapshotKnown
    /\ PinnedSnapshotOnlyMaterializedProjection
    /\ PinnedSnapshotNeverUsesExternalLive
    /\ RejectedNeverEffective
    /\ SupersededNeverEffective
    /\ StaleNeverEffective
    /\ ExternalLiveMatchesAttribute
    /\ ExternalLiveNeverEffective
    /\ RawSourceObservationImmutable
    /\ SupersededKeepsHistory
    /\ AcceptedOverlayIsEffectiveOrSuperseded
    /\ ReservedEffectiveOnlyRaw
    /\ ReservedPinnedEffectiveOnlyRaw
    /\ OneEffectivePerAttribute
    /\ PromotedOnlyAcceptedMaterialized
    /\ PromotedOnlyPromotable
    /\ TerminalObservationRetained
    /\ AttributeReadSurfaceSafety
    /\ AttributeRetentionSafety
    /\ AttributePromotionLifecycleSafety
    /\ AttributeConflictSafety
    /\ AttributeViewMaintenanceSafety

====
