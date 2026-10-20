import Std.Tactic

/- Candidate FIFO maintenance contract. Units are charged design operations,
not seconds. This file does not import or prove Concord's rule evaluator. -/
namespace ConcordBounded

def allowance (burst tokens refill : Nat) := min burst (tokens + refill)
def nextTokens (burst tokens refill arrivals : Nat) := allowance burst tokens refill - arrivals
def nextBacklog (backlog arrivals capacity : Nat) := backlog + arrivals - capacity

-- The actual admission and service recurrence preserves a burst bound.
theorem burst_invariant (burst tokens refill arrivals backlog capacity : Nat)
    (initial : backlog + tokens ≤ burst)
    (admitted : arrivals ≤ allowance burst tokens refill)
    (service : refill ≤ capacity) :
    nextBacklog backlog arrivals capacity +
      nextTokens burst tokens refill arrivals ≤ burst := by
  unfold nextBacklog nextTokens allowance at *
  omega

-- Before serving, even a burst of new jobs fits within B+R units.
theorem admitted_ahead_bound (burst tokens refill arrivals backlog : Nat)
    (initial : backlog + tokens ≤ burst)
    (admitted : arrivals ≤ allowance burst tokens refill) :
    backlog + arrivals ≤ burst + refill := by
  unfold allowance at admitted
  omega

-- Pending queues contain remaining work, not original job estimates.
theorem queue_count_le_work (jobs : List Nat)
    (positive : ∀ job ∈ jobs, 1 ≤ job) : jobs.length ≤ jobs.sum := by
  induction jobs with
  | nil => simp
  | cons job jobs ih =>
    have head : 1 ≤ job := positive job (by simp)
    have tail : ∀ rest ∈ jobs, 1 ≤ rest := by
      intro rest member
      exact positive rest (by simp [member])
    have bound := ih tail
    simp only [List.length_cons, List.sum_cons]
    omega

-- A concrete FIFO queue contains one generation tag per charged work unit.
-- Service removes a prefix. A tag remains until that generation's final unit.
def fifoStep (queue arrivals : List Nat) (capacity : Nat) :=
  (queue ++ arrivals).drop capacity

theorem fifo_work_recurrence (queue arrivals : List Nat) (capacity : Nat) :
    (fifoStep queue arrivals capacity).length =
      nextBacklog queue.length arrivals.length capacity := by
  simp [fifoStep, nextBacklog]

theorem fifo_burst_invariant (queue arrivals : List Nat)
    (burst tokens refill capacity : Nat)
    (initial : queue.length + tokens ≤ burst)
    (admitted : arrivals.length ≤ allowance burst tokens refill)
    (service : refill ≤ capacity) :
    (fifoStep queue arrivals capacity).length +
      nextTokens burst tokens refill arrivals.length ≤ burst := by
  rw [fifo_work_recurrence]
  exact burst_invariant burst tokens refill arrivals.length queue.length capacity initial admitted service

-- Later generations are appended behind an existing target. This decomposition
-- proves their arrival cannot reset or increase that target's remaining prefix.
theorem fifo_prefix_step (ahead behind arrivals : List Nat) (capacity : Nat) :
    fifoStep (ahead ++ behind) arrivals capacity =
      ahead.drop capacity ++ (behind ++ arrivals).drop (capacity - ahead.length) := by
  simp only [fifoStep, List.append_assoc]
  exact List.drop_append

theorem fifo_prefix_length (ahead : List Nat) (capacity : Nat) :
    (ahead.drop capacity).length = ahead.length - capacity := by
  exact List.length_drop

-- Count every pending version, including no-op versions, with a positive cost.
theorem version_lag_bound (pending remaining tokens burst : Nat)
    (positive_charge : pending ≤ remaining)
    (invariant : remaining + tokens ≤ burst) : pending ≤ burst := by
  omega

-- FIFO property: later arrivals cannot add to a target's remaining ahead.
-- Each round actually serves capacity units or empties that ahead.
def drain (capacity : Nat) : Nat → Nat → Nat
  | 0, ahead => ahead
  | steps + 1, ahead => drain capacity steps (ahead - capacity)

theorem drain_bound (capacity steps ahead : Nat) :
    drain capacity steps ahead = ahead - steps * capacity := by
  induction steps generalizing ahead with
  | zero => simp [drain]
  | succ steps ih =>
    simp only [drain, ih, Nat.succ_mul]
    omega

theorem fifo_deadline (capacity steps ahead : Nat)
    (enough : ahead ≤ steps * capacity) : drain capacity steps ahead = 0 := by
  rw [drain_bound]
  omega

-- A simple general bound requiring no division: at most B+R service rounds.
theorem admitted_job_deadline (burst tokens refill arrivals backlog capacity : Nat)
    (initial : backlog + tokens ≤ burst)
    (admitted : arrivals ≤ allowance burst tokens refill)
    (positive_service : 1 ≤ capacity) :
    drain capacity (burst + refill) (backlog + arrivals) = 0 := by
  apply fifo_deadline
  have bound := admitted_ahead_bound burst tokens refill arrivals backlog initial admitted
  have multiply : burst + refill ≤ (burst + refill) * capacity := by
    have := Nat.mul_le_mul_left (burst + refill) positive_service
    simpa using this
  omega

-- Every service round, not only a finite exploration, preserves the invariant.
def evolve (burst refill capacity : Nat) (arrival : Nat → Nat) : Nat → Nat × Nat
  | 0 => (0, burst)
  | n + 1 =>
      let previous := evolve burst refill capacity arrival n
      (nextBacklog previous.1 (arrival n) capacity,
       nextTokens burst previous.2 refill (arrival n))

theorem all_rounds (burst refill capacity : Nat) (arrival : Nat → Nat)
    (service : refill ≤ capacity)
    (admission : ∀ n, arrival n ≤ allowance burst (evolve burst refill capacity arrival n).2 refill) :
    ∀ n, (evolve burst refill capacity arrival n).1 +
      (evolve burst refill capacity arrival n).2 ≤ burst := by
  intro n
  induction n with
  | zero => simp [evolve]
  | succ n ih =>
    exact burst_invariant burst _ refill (arrival n) _ capacity ih (admission n) service

-- Exact checkpoints carry graph, rule, and answer identity. Pending work never
-- mutates one. The equality is a commit obligation, not a proof of evaluation.
structure Identity where
  graph : Nat
  rules : Nat
  answers : Nat
  deriving DecidableEq
structure Checkpoint (Result : Type) where
  identity : Identity
  result : Result

def Exact {Result : Type} (full : Identity → Result) (cp : Checkpoint Result) :=
  cp.result = full cp.identity

def commit {Result : Type} (identity : Identity) (candidate : Result) : Checkpoint Result :=
  ⟨identity, candidate⟩

theorem exact_commit {Result : Type} (full : Identity → Result)
    (identity : Identity) (candidate : Result) (evaluated : candidate = full identity) :
    Exact full (commit identity candidate) := evaluated

-- No global graph size appears in the charged-operation ledger. Enumerating
-- these finite lists by indexes, and complete dependency coverage, are separate
-- obligations; this is an accounting identity, not a query-complexity proof.
structure Ledger where
  changed : List Nat
  traced : List Nat
  evaluated : List Nat
  retained : List Nat
  retried : List Nat

def cost (ledger : Ledger) := 1 + ledger.changed.length + ledger.traced.length +
  ledger.evaluated.length + ledger.retained.length + ledger.retried.length

theorem positive_cost (ledger : Ledger) : 1 ≤ cost ledger := by
  unfold cost
  omega

#print axioms fifo_work_recurrence
#print axioms fifo_burst_invariant
#print axioms fifo_prefix_step
#print axioms fifo_prefix_length
#print axioms queue_count_le_work
#print axioms burst_invariant
#print axioms all_rounds
#print axioms fifo_deadline
#print axioms admitted_job_deadline
#print axioms version_lag_bound
#print axioms exact_commit
#print axioms positive_cost
end ConcordBounded
