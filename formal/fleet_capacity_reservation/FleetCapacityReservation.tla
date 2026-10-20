--------------------- MODULE FleetCapacityReservation ---------------------
EXTENDS FiniteSets, Naturals, TLC

Consumers == {"deployment-a", "deployment-b"}
ReservationIds == {"reservation-a", "reservation-b"}
Capacity == 4
StandingReserve == 1
CurrentActivationEpoch == 2
CurrentActivationReceipt == "activation-v2"
ReservationVector == [workers |-> 1, disk |-> 1]
ZeroVector == [workers |-> 0, disk |-> 0]

DomainOf(consumer) == "postgres-cluster-a"
ResourceDomains == {DomainOf(consumer) : consumer \in Consumers}
ObservedDomainDemand == Cardinality(ResourceDomains)

VARIABLES reservations, consumed, crashed, reconciled, releasedReservations,
          chargedReservations, reservationBinding, committedDemand,
          permitState, permitReservation, permitBound, permitConsumed,
          permitChargeCount, permitReconciled, permitEpoch, permitReceipt,
          permitExpiresAt, activationEpoch, activationReceipt, logicalTime

vars == <<reservations, consumed, crashed, reconciled, releasedReservations,
          chargedReservations, reservationBinding, committedDemand,
          permitState, permitReservation, permitBound, permitConsumed,
          permitChargeCount, permitReconciled, permitEpoch, permitReceipt,
          permitExpiresAt, activationEpoch, activationReceipt, logicalTime>>

HeldReservations == reservations \ consumed
ActiveCommittedReservations == consumed \ releasedReservations
HeldDemand == Cardinality(HeldReservations)
TotalDemand == ObservedDomainDemand + StandingReserve
               + Cardinality(ActiveCommittedReservations) + HeldDemand

Init ==
    /\ reservations = {}
    /\ consumed = {}
    /\ crashed = {}
    /\ reconciled = {}
    /\ releasedReservations = {}
    /\ chargedReservations = {}
    /\ reservationBinding =
         [reservation \in ReservationIds |->
            [epoch |-> 0, receipt |-> "none", vector |-> ZeroVector]]
    /\ committedDemand = 0
    /\ permitState = "absent"
    /\ permitReservation = "none"
    /\ permitBound = ZeroVector
    /\ permitConsumed = ZeroVector
    /\ permitChargeCount = 0
    /\ permitReconciled = FALSE
    /\ permitEpoch = 0
    /\ permitReceipt = "none"
    /\ permitExpiresAt = 0
    /\ activationEpoch = 1
    /\ activationReceipt = "activation-v1"
    /\ logicalTime = 0

Acquire(reservation) ==
    /\ reservation \in ReservationIds
    /\ reservation \notin reservations \cup reconciled
    /\ TotalDemand + 1 <= Capacity
    /\ reservations' = reservations \cup {reservation}
    /\ reservationBinding' = [reservationBinding EXCEPT
          ![reservation] = [epoch |-> activationEpoch,
                            receipt |-> activationReceipt,
                            vector |-> ReservationVector]]
    /\ UNCHANGED <<consumed, crashed, reconciled, releasedReservations,
                    chargedReservations, committedDemand, permitState,
                    permitReservation, permitBound, permitConsumed,
                    permitChargeCount, permitReconciled, permitEpoch,
                    permitReceipt, permitExpiresAt, activationEpoch,
                    activationReceipt, logicalTime>>

Consume(reservation) ==
    /\ reservation \in HeldReservations
    /\ reservation \notin crashed
    /\ reservation # permitReservation
    /\ consumed' = consumed \cup {reservation}
    /\ committedDemand' = committedDemand + 1
    /\ UNCHANGED <<reservations, crashed, reconciled, releasedReservations,
                    chargedReservations, reservationBinding, permitState,
                    permitReservation, permitBound, permitConsumed,
                    permitChargeCount, permitReconciled, permitEpoch,
                    permitReceipt, permitExpiresAt, activationEpoch,
                    activationReceipt, logicalTime>>

Crash(reservation) ==
    /\ reservation \in HeldReservations
    /\ reservation \notin crashed
    /\ crashed' = crashed \cup {reservation}
    /\ UNCHANGED <<reservations, consumed, reconciled, releasedReservations,
                    chargedReservations, reservationBinding, committedDemand,
                    permitState, permitReservation, permitBound, permitConsumed,
                    permitChargeCount, permitReconciled, permitEpoch,
                    permitReceipt, permitExpiresAt, activationEpoch,
                    activationReceipt, logicalTime>>

ReconcileCrash(reservation) ==
    /\ reservation \in crashed
    /\ reservation \notin reconciled
    /\ reservations' = reservations \ {reservation}
    /\ crashed' = crashed \ {reservation}
    /\ reconciled' = reconciled \cup {reservation}
    /\ UNCHANGED <<consumed, releasedReservations, chargedReservations,
                    reservationBinding, committedDemand, permitState,
                    permitReservation, permitBound, permitConsumed,
                    permitChargeCount, permitReconciled, permitEpoch,
                    permitReceipt, permitExpiresAt, activationEpoch,
                    activationReceipt, logicalTime>>

ConvertHeldReservationToPermit(reservation) ==
    /\ permitState = "absent"
    /\ reservation \in HeldReservations
    /\ reservation \notin crashed
    /\ reservationBinding[reservation].epoch = activationEpoch
    /\ reservationBinding[reservation].receipt = activationReceipt
    /\ reservationBinding[reservation].vector = ReservationVector
    /\ logicalTime < 2
    /\ consumed' = consumed \cup {reservation}
    /\ committedDemand' = committedDemand + 1
    /\ chargedReservations' = chargedReservations \cup {reservation}
    /\ permitState' = "issued"
    /\ permitReservation' = reservation
    /\ permitBound' = reservationBinding[reservation].vector
    /\ permitConsumed' = ZeroVector
    /\ permitChargeCount' = permitChargeCount + 1
    /\ permitReconciled' = FALSE
    /\ permitEpoch' = reservationBinding[reservation].epoch
    /\ permitReceipt' = reservationBinding[reservation].receipt
    /\ permitExpiresAt' = 2
    /\ UNCHANGED <<reservations, crashed, reconciled, releasedReservations,
                    reservationBinding, activationEpoch, activationReceipt,
                    logicalTime>>

ConsumePermitWorker ==
    /\ permitState = "issued"
    /\ logicalTime < permitExpiresAt
    /\ permitEpoch = activationEpoch
    /\ permitReceipt = activationReceipt
    /\ permitConsumed.workers < permitBound.workers
    /\ permitConsumed' = [permitConsumed EXCEPT !.workers = @ + 1]
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    releasedReservations, chargedReservations,
                    reservationBinding, committedDemand, permitState,
                    permitReservation, permitBound, permitChargeCount,
                    permitReconciled, permitEpoch, permitReceipt,
                    permitExpiresAt, activationEpoch, activationReceipt,
                    logicalTime>>

ConsumePermitDisk ==
    /\ permitState = "issued"
    /\ logicalTime < permitExpiresAt
    /\ permitEpoch = activationEpoch
    /\ permitReceipt = activationReceipt
    /\ permitConsumed.disk < permitBound.disk
    /\ permitConsumed' = [permitConsumed EXCEPT !.disk = @ + 1]
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    releasedReservations, chargedReservations,
                    reservationBinding, committedDemand, permitState,
                    permitReservation, permitBound, permitChargeCount,
                    permitReconciled, permitEpoch, permitReceipt,
                    permitExpiresAt, activationEpoch, activationReceipt,
                    logicalTime>>

ReleasePermit ==
    /\ permitState = "issued"
    /\ ~permitReconciled
    /\ permitReservation \in ActiveCommittedReservations
    /\ permitState' = "released"
    /\ permitReconciled' = TRUE
    /\ releasedReservations' = releasedReservations \cup {permitReservation}
    /\ committedDemand' = committedDemand - 1
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    chargedReservations, reservationBinding, permitReservation,
                    permitBound, permitConsumed, permitChargeCount, permitEpoch,
                    permitReceipt, permitExpiresAt, activationEpoch,
                    activationReceipt, logicalTime>>

CrashPermit ==
    /\ permitState = "issued"
    /\ ~permitReconciled
    /\ permitState' = "crashed"
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    releasedReservations, chargedReservations,
                    reservationBinding, committedDemand, permitReservation,
                    permitBound, permitConsumed, permitChargeCount,
                    permitReconciled, permitEpoch, permitReceipt,
                    permitExpiresAt, activationEpoch, activationReceipt,
                    logicalTime>>

ReconcilePermit ==
    /\ permitState \in {"expired", "revoked", "crashed"}
    /\ ~permitReconciled
    /\ permitReservation \in ActiveCommittedReservations
    /\ permitState' = "reconciled"
    /\ permitReconciled' = TRUE
    /\ releasedReservations' = releasedReservations \cup {permitReservation}
    /\ committedDemand' = committedDemand - 1
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    chargedReservations, reservationBinding, permitReservation,
                    permitBound, permitConsumed, permitChargeCount, permitEpoch,
                    permitReceipt, permitExpiresAt, activationEpoch,
                    activationReceipt, logicalTime>>

Tick ==
    /\ logicalTime < 2
    /\ logicalTime' = logicalTime + 1
    /\ permitState' = IF permitState = "issued" /\ logicalTime' >= permitExpiresAt
                       THEN "expired" ELSE permitState
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    releasedReservations, chargedReservations,
                    reservationBinding, committedDemand, permitReservation,
                    permitBound, permitConsumed, permitChargeCount,
                    permitReconciled, permitEpoch, permitReceipt,
                    permitExpiresAt, activationEpoch, activationReceipt>>

AdvanceActivation ==
    /\ activationEpoch = 1
    /\ activationEpoch' = CurrentActivationEpoch
    /\ activationReceipt' = CurrentActivationReceipt
    /\ permitState' = IF permitState = "issued" THEN "revoked" ELSE permitState
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    releasedReservations, chargedReservations,
                    reservationBinding, committedDemand, permitReservation,
                    permitBound, permitConsumed, permitChargeCount,
                    permitReconciled, permitEpoch, permitReceipt,
                    permitExpiresAt, logicalTime>>

RotateActivationReceipt ==
    /\ activationReceipt = "activation-v1"
    /\ activationReceipt' = CurrentActivationReceipt
    /\ permitState' = IF permitState = "issued" THEN "revoked" ELSE permitState
    /\ UNCHANGED <<reservations, consumed, crashed, reconciled,
                    releasedReservations, chargedReservations,
                    reservationBinding, committedDemand, permitReservation,
                    permitBound, permitConsumed, permitChargeCount,
                    permitReconciled, permitEpoch, permitReceipt,
                    permitExpiresAt, activationEpoch, logicalTime>>

Noop == UNCHANGED vars

Next == (\E reservation \in ReservationIds :
          Acquire(reservation) \/ Consume(reservation) \/ Crash(reservation)
          \/ ReconcileCrash(reservation)
          \/ ConvertHeldReservationToPermit(reservation))
        \/ ConsumePermitWorker \/ ConsumePermitDisk \/ ReleasePermit
        \/ CrashPermit \/ ReconcilePermit \/ Tick
        \/ AdvanceActivation \/ RotateActivationReceipt \/ Noop

ReservationLifecycleTyped ==
    /\ reservations \subseteq ReservationIds
    /\ consumed \subseteq reservations
    /\ crashed \subseteq HeldReservations
    /\ reconciled \subseteq ReservationIds
    /\ releasedReservations \subseteq consumed
    /\ chargedReservations \subseteq consumed
    /\ reservationBinding \in
         [ReservationIds -> [epoch : 0..CurrentActivationEpoch,
                             receipt : {"none", "activation-v1", CurrentActivationReceipt},
                             vector : [workers : 0..1, disk : 0..1]]]
    /\ committedDemand \in 0..Cardinality(ReservationIds)

ConcurrentReservationsNeverOversubscribe == TotalDemand <= Capacity

CrashedReservationReconcilesAtMostOnce ==
    /\ reconciled \cap reservations = {}
    /\ reconciled \cap crashed = {}

ConsumedReservationsRemainCommitted ==
    committedDemand = Cardinality(ActiveCommittedReservations)

ResourceDomainsDeduplicateSharedCapacity ==
    /\ Cardinality(Consumers) = 2
    /\ Cardinality(ResourceDomains) = 1
    /\ ObservedDomainDemand = 1

StandingPermitLifecycleTyped ==
    /\ permitState \in {"absent", "issued", "expired", "revoked", "crashed",
                         "released", "reconciled"}
    /\ permitReservation \in {"none"} \cup ReservationIds
    /\ permitBound \in [workers : 0..1, disk : 0..1]
    /\ permitConsumed \in [workers : 0..1, disk : 0..1]
    /\ permitChargeCount \in 0..1
    /\ permitReconciled \in BOOLEAN
    /\ permitEpoch \in 0..CurrentActivationEpoch
    /\ permitReceipt \in {"none", "activation-v1", CurrentActivationReceipt}
    /\ permitExpiresAt \in 0..2
    /\ activationEpoch \in {1, CurrentActivationEpoch}
    /\ activationReceipt \in {"activation-v1", CurrentActivationReceipt}
    /\ logicalTime \in 0..2

StandingPermitConsumptionBounded ==
    /\ permitConsumed.workers <= permitBound.workers
    /\ permitConsumed.disk <= permitBound.disk

StandingPermitDoesNotOutliveAuthority ==
    permitState = "issued" =>
      /\ permitEpoch = activationEpoch
      /\ permitReceipt = activationReceipt
      /\ logicalTime < permitExpiresAt

StandingPermitCannotExpand ==
    permitState # "absent" =>
      /\ permitReservation \in chargedReservations
      /\ permitBound = reservationBinding[permitReservation].vector
      /\ permitEpoch = reservationBinding[permitReservation].epoch
      /\ permitReceipt = reservationBinding[permitReservation].receipt

PermitConvertsHeldReservationAtomically ==
    permitState # "absent" =>
      /\ permitReservation \in reservations
      /\ permitReservation \in consumed
      /\ permitReservation \in chargedReservations
      /\ permitBound = ReservationVector
      /\ permitChargeCount = 1

PermitResourceChargeExactlyOnce ==
    permitChargeCount = Cardinality(chargedReservations)

PermitTerminalReconciliationAtMostOnce ==
    /\ permitReconciled =>
         /\ permitState \in {"released", "reconciled"}
         /\ permitReservation \in releasedReservations
    /\ permitState \in {"expired", "revoked", "crashed"} => ~permitReconciled

Spec == Init /\ [][Next]_vars

=============================================================================
