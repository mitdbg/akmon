---- MODULE QualificationGate ----
EXTENDS Naturals, FiniteSets

\* Blocking-policy state machine for the consistency publish gate's finding
\* arm (src/okg/substrate/consistency/gates.py). The model crosses the finding
\* lifecycle (absent -> open via an observed event -> resolved -> reopened)
\* with the declarative blocking policy (never / new_findings [+ severity floor]
\* / any_open) and a deployment blocks_publish override, across a sequence of
\* publish ATTEMPTS where a blocked attempt is retried on the next cycle.
\*
\* SCOPE: finding-blocking ONLY. Operational blocks (timeout / error /
\* incomplete coverage), which the engine ALWAYS treats as blocking regardless
\* of policy, are deliberately out of scope -- this model reasons about the
\* clean-evaluation-with-open-findings case the policy governs.
\*
\* The load-bearing engine fact is the bounded-event-growth invariant
\* (_newly_opened_this_run docstring): a still-open UNCHANGED finding emits NO
\* re-observed event on a later run. ``observedThisCycle`` is that signal --
\* set TRUE only for the finding a cycle just opened/reopened, and FALSE for a
\* finding that stays open across a cycle without change. That single fact is
\* what makes new_findings a ONE-SHOT ratchet.

CONSTANTS
  Findings,                            \* subset of {"f_high","f_med","f_low"}
  MaxCycles,                           \* bound on publish attempts (Nat)
  BlockOn,                             \* "never" | "new_findings" | "any_open"
  MinSeverityRank,                     \* severity floor rank: 0 (low) | 1 | 2
  BuggyConsultsNewlyOpenedForStanding  \* falsifiability toggle (see below)

Statuses == {"absent", "open", "resolved"}
Outcomes == {"pending", "published", "blocked"}
Phases == {"act", "gate"}
OverrideValues == {"unset", "force", "advisory"}

\* Severity lattice low < medium < high, keyed on the finding vocabulary. The
\* engine's block_on_min_severity is case-folded rank (gates.py _SEVERITY_RANK);
\* here each finding carries a fixed rank and the floor filters at/above it.
SeverityRank(f) ==
  CASE f = "f_high" -> 2
    [] f = "f_med"  -> 1
    [] f = "f_low"  -> 0
    [] OTHER        -> 0

VARIABLES
  status,             \* [Findings -> Statuses]: per-finding lifecycle
  observedThisCycle,  \* [Findings -> BOOLEAN]: emitted an 'observed' event this run
  cycle,              \* 0..MaxCycles: which publish attempt
  phase,              \* "act" (mutate the ledger) then "gate" (decide)
  outcome,            \* "pending" | "published" | "blocked": last gate verdict
  override            \* rigid per-behavior deployment blocks_publish override

vars == <<status, observedThisCycle, cycle, phase, outcome, override>>

OpenFindings(st) == {f \in Findings : st[f] = "open"}

\* Findings this run OPENED and left open -- the _newly_opened_this_run set:
\* an 'observed' event this cycle on a currently-open finding. A pre-existing
\* open backlog has observedThisCycle = FALSE (bounded-event-growth), so it is
\* NOT here -- exactly why new_findings publishes over it.
NewlyObservedOpen(st, obs) == {f \in OpenFindings(st) : obs[f]}

\* The severity-floored newly-opened set the new_findings policy blocks on
\* (gates.py: _newly_opened_this_run(..., block_on_min_severity)).
NewFindingsBlockingSet(st, obs) ==
  {f \in NewlyObservedOpen(st, obs) : SeverityRank(f) >= MinSeverityRank}

\* Concord's STANDING qualification predicate (spec section 9.10): a finding is
\* mandatory iff it is open AND qualifies at the declared severity floor. This
\* is POLICY-INDEPENDENT (the paper's infinite-cost class) -- it does not decay
\* when a still-open finding stops re-emitting an observed event. A deployment
\* ``advisory`` override demotes everything (blocks_publish=False => nothing
\* mandatory); ``force`` and ``unset`` leave the floored-open set standing.
StandingMandatoryOpen(st, ov) ==
  IF ov = "advisory"
  THEN {}
  ELSE {f \in OpenFindings(st) : SeverityRank(f) >= MinSeverityRank}

\* The gate's finding-blocking decision (gates.py evaluate_consistency_publish_gate
\* blocks_publish=None branch, finding-only). An explicit deployment override
\* wins both ways; else, with an open finding, the declarative policy governs.
FindingBlocks(st, obs, ov) ==
  IF ov = "force" THEN TRUE
  ELSE IF ov = "advisory" THEN FALSE
  ELSE IF OpenFindings(st) = {} THEN FALSE
  ELSE IF BlockOn = "never" THEN FALSE
  ELSE IF BlockOn = "any_open"
       \* any_open blocks on ANY open finding. The falsifiability toggle models
       \* a gate that consults the newly-opened count (the wrong count) instead
       \* of the open count -- silently degrading any_open into new_findings.
       THEN (IF BuggyConsultsNewlyOpenedForStanding
             THEN NewlyObservedOpen(st, obs) # {}
             ELSE TRUE)
  ELSE IF BlockOn = "new_findings"
       THEN NewFindingsBlockingSet(st, obs) # {}
  ELSE TRUE  \* unknown policy fails safe (blocks)

Init ==
  /\ status = [f \in Findings |-> "absent"]
  /\ observedThisCycle = [f \in Findings |-> FALSE]
  /\ cycle = 0
  /\ phase = "act"
  /\ outcome = "pending"
  /\ override \in OverrideValues

\* A cycle opens/reopens a finding (a new 'observed' event), resolves an open
\* one, or changes nothing. Only the opened finding is observed this cycle; a
\* finding that stays open is NOT re-observed (bounded-event-growth).
OpenOne(f) ==
  /\ status[f] \in {"absent", "resolved"}
  /\ status' = [status EXCEPT ![f] = "open"]
  /\ observedThisCycle' = [g \in Findings |-> g = f]

ResolveOne(f) ==
  /\ status[f] = "open"
  /\ status' = [status EXCEPT ![f] = "resolved"]
  /\ observedThisCycle' = [g \in Findings |-> FALSE]

NoChange ==
  /\ status' = status
  /\ observedThisCycle' = [g \in Findings |-> FALSE]

Act ==
  /\ phase = "act"
  /\ cycle < MaxCycles
  /\ \/ \E f \in Findings : OpenOne(f)
     \/ \E f \in Findings : ResolveOne(f)
     \/ NoChange
  /\ phase' = "gate"
  /\ outcome' = "pending"       \* a fresh attempt is in flight
  /\ UNCHANGED <<cycle, override>>

Gate ==
  /\ phase = "gate"
  /\ outcome' = IF FindingBlocks(status, observedThisCycle, override)
                THEN "blocked"
                ELSE "published"
  /\ phase' = "act"
  /\ cycle' = cycle + 1          \* blocked attempts are retried next cycle
  /\ UNCHANGED <<status, observedThisCycle, override>>

Done ==
  /\ phase = "act"
  /\ cycle = MaxCycles
  /\ UNCHANGED vars

Next == Act \/ Gate \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ status \in [Findings -> Statuses]
  /\ observedThisCycle \in [Findings -> BOOLEAN]
  /\ cycle \in 0..MaxCycles
  /\ phase \in Phases
  /\ outcome \in Outcomes
  /\ override \in OverrideValues

\* ---- Headline standing invariant (any_open) -------------------------------
\* No generation publishes while a mandatory finding is open. ``outcome`` is
\* only "published" in the post-Gate state, where ``status`` is exactly what
\* the gate decided over, so this relates the publish verdict to the finding
\* state at decision time. HOLDS under any_open (the paper's standing
\* qualification); VIOLATED under new_findings by the one-shot ratchet.
NoPublishWhileMandatoryOpen ==
  (outcome = "published") => (StandingMandatoryOpen(status, override) = {})

\* ---- Smoke / sanity invariants --------------------------------------------
\* Under never (with no override), findings never block; only operational
\* results would (out of scope), so the finding arm never reaches "blocked".
NeverPolicyNeverBlocksOnFindings ==
  (BlockOn = "never" /\ override = "unset") => (outcome # "blocked")

\* A deployment ``force`` override blocks every decided attempt; ``advisory``
\* publishes every decided attempt (finding-only scope). These mirror the
\* engine's explicit-override-wins-both-ways contract.
OverrideForceBlocksWhenDecided ==
  (override = "force" /\ outcome # "pending") => (outcome = "blocked")

OverrideAdvisoryPublishesWhenDecided ==
  (override = "advisory" /\ outcome # "pending") => (outcome = "published")

====
