------------------------- MODULE FactBatchRecovery -------------------------
EXTENDS Naturals, TLC

VARIABLES receipt, graphApplied, markerAdvanced, terminal, published

vars == <<receipt, graphApplied, markerAdvanced, terminal, published>>

Init ==
    /\ receipt = "none"
    /\ graphApplied = FALSE
    /\ markerAdvanced = FALSE
    /\ terminal = "pending"
    /\ published = FALSE

Prepare ==
    /\ terminal = "pending"
    /\ receipt = "none"
    /\ receipt' = "prepared"
    /\ UNCHANGED <<graphApplied, markerAdvanced, terminal, published>>

ApplyGraph ==
    /\ receipt = "prepared"
    /\ graphApplied' = TRUE
    /\ UNCHANGED <<receipt, markerAdvanced, terminal, published>>

CommitEvidence ==
    /\ receipt = "prepared"
    /\ graphApplied
    /\ receipt' = "applied"
    /\ markerAdvanced' = TRUE
    /\ UNCHANGED <<graphApplied, terminal, published>>

RecoverGraph ==
    /\ receipt = "prepared"
    /\ graphApplied' = TRUE
    /\ UNCHANGED <<receipt, markerAdvanced, terminal, published>>

Accept ==
    /\ terminal = "pending"
    /\ receipt = "applied"
    /\ terminal' = "accepted"
    /\ UNCHANGED <<receipt, graphApplied, markerAdvanced, published>>

BeginCompensation ==
    /\ terminal = "pending"
    /\ receipt = "applied"
    /\ receipt' = "compensating"
    /\ UNCHANGED <<graphApplied, markerAdvanced, terminal, published>>

FinishCompensation ==
    /\ receipt = "compensating"
    /\ receipt' = "compensated"
    /\ graphApplied' = FALSE
    /\ markerAdvanced' = FALSE
    /\ UNCHANGED <<terminal, published>>

Reject ==
    /\ terminal = "pending"
    /\ receipt \in {"none", "compensated"}
    /\ terminal' = "rejected"
    /\ UNCHANGED <<receipt, graphApplied, markerAdvanced, published>>

Fail ==
    /\ terminal = "pending"
    /\ receipt \in {"none", "compensated"}
    /\ terminal' = "failed"
    /\ UNCHANGED <<receipt, graphApplied, markerAdvanced, published>>

Publish ==
    /\ terminal = "accepted"
    /\ receipt = "applied"
    /\ graphApplied
    /\ published' = TRUE
    /\ UNCHANGED <<receipt, graphApplied, markerAdvanced, terminal>>

Noop == UNCHANGED vars

Next ==
    Prepare \/ ApplyGraph \/ CommitEvidence \/ RecoverGraph \/ Accept \/
    BeginCompensation \/ FinishCompensation \/ Reject \/ Fail \/ Publish \/ Noop

MarkerNeverAhead == markerAdvanced => receipt \in {"applied", "compensating"}
TerminalAcceptanceSafe == terminal = "accepted" => receipt = "applied"
TerminalFailureCompensated ==
    terminal \in {"rejected", "failed"} => receipt \in {"none", "compensated"}
PublishedOnlyAfterAcceptance ==
    published => terminal = "accepted" /\ receipt = "applied" /\ graphApplied
PreparedBlocksTerminal ==
    receipt = "prepared" => terminal = "pending"
CompensatingBlocksTerminal ==
    receipt = "compensating" => terminal = "pending"

Spec == Init /\ [][Next]_vars

=============================================================================
