---- MODULE DerivationParity ----
EXTENDS Integers, FiniteSets

CONSTANTS
    Items,
    InitialBase,
    BuggyIncrementalMissesClose

VARIABLES accumulated, base, kept, openContribution

vars == <<accumulated, base, kept, openContribution>>

Deltas == {0 - 1, 0, 1}

\* base/inputs live in the tight range; kept and its open contribution get
\* headroom so a missed-close divergence stays type-valid and TLC reports the
\* parity invariant violated (not TypeOK swallowing the counterexample first).
BaseRange == (0 - 2)..2
KeptRange == (0 - 6)..6

\* Batch recompute path: the derived value is a pure function of the accumulated
\* input stream, recomputed from scratch -- never folded from kept's own prior
\* state. This is what makes base and kept two INDEPENDENT sequences rather than
\* the same delta applied to both (the pre-strengthening x=x tautology).
Recompute(item) == InitialBase + accumulated[item]

TypeOK ==
    /\ Items # {}
    /\ accumulated \in [Items -> BaseRange]
    /\ base \in [Items -> BaseRange]
    /\ kept \in [Items -> KeptRange]
    /\ openContribution \in [Items -> KeptRange]

Init ==
    /\ accumulated = [item \in Items |-> 0]
    /\ base = [item \in Items |-> InitialBase]
    /\ kept = [item \in Items |-> InitialBase]
    /\ openContribution = [item \in Items |-> 0]

\* One accepted input delta for one item.
\*
\*  * base is RECOMPUTED from the accumulated input stream (batch path).
\*  * kept is maintained by an INDEPENDENT interval fold: CLOSE the currently
\*    open contribution (subtract it) and OPEN a new interval carrying the new
\*    accumulated input (add it). Correct maintenance keeps kept = base.
\*
\* Under BuggyIncrementalMissesClose the fold OPENS the new interval without
\* CLOSING the old one, so the prior contribution is double-counted and kept
\* diverges from the recompute -- reachable only after a second delta (the first
\* delta closes a zero contribution), i.e. a genuine multi-step trace.
ApplyDelta(item, delta) ==
    /\ item \in Items
    /\ delta \in Deltas
    /\ LET newAccum   == accumulated[item] + delta
           newInput   == InitialBase + newAccum
           closed     == IF BuggyIncrementalMissesClose
                         THEN 0
                         ELSE openContribution[item]
           newKept    == kept[item] - closed + newAccum
       IN
        /\ newAccum \in BaseRange
        /\ newInput \in BaseRange
        /\ newKept \in KeptRange
        /\ accumulated' = [accumulated EXCEPT ![item] = newAccum]
        /\ base' = [base EXCEPT ![item] = newInput]
        /\ kept' = [kept EXCEPT ![item] = newKept]
        /\ openContribution' = [openContribution EXCEPT ![item] = newAccum]

Next ==
    \/ \E item \in Items, delta \in Deltas:
        ApplyDelta(item, delta)
    \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars

\* The kept incremental materialization visible to readers equals the batch
\* recompute over the accumulated base truth after every accepted delta.
IncrementalEqualsBatchForLinearDerivation == kept = base

\* The batch path is genuinely a recompute over the accumulated stream, not a
\* mirror of kept: base always equals Recompute. Holds in both configs (the bug
\* corrupts only the incremental fold), so it is not the witness target -- it
\* pins the independence of the two sequences.
BaseIsRecompute == \A item \in Items: base[item] = Recompute(item)

DeterministicLinearSafety ==
    /\ TypeOK
    /\ BaseIsRecompute
    /\ IncrementalEqualsBatchForLinearDerivation

====
