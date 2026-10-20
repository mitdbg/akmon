import Std.Tactic
namespace ConcordRecovery
structure Identity where
  graph : Nat
  rules : Nat
  answers : Nat
  deriving DecidableEq
structure Journal where
  certified : Identity
  certifiedValue : Nat
  target : Identity
  total : Nat
  completed : Nat
  effects : List (Nat × Nat)
  running : Bool
  deriving DecidableEq
-- E is a fixed deterministic evaluator for pinned identity and unit ordinal.
-- It is not a provider action: durable effect+ordinal append is atomic by design.
def canonical (E : Identity → Nat → Nat) (identity : Identity) (count : Nat) :=
  (List.range count).map fun ordinal => (ordinal, E identity ordinal)
def Valid (E : Identity → Nat → Nat) (state : Journal) :=
  state.completed ≤ state.total ∧ state.effects = canonical E state.target state.completed

def step (E : Identity → Nat → Nat) (state : Journal) : Journal :=
  if state.running = true ∧ state.completed < state.total then
    {state with completed := state.completed + 1, effects := state.effects ++ [(state.completed, E state.target state.completed)]}
  else state

def crash (state : Journal) : Journal := {state with running := false}
def resume (state : Journal) : Journal := {state with running := true}
def finish (state : Journal) : Journal :=
  if state.completed = state.total then
    {state with certified := state.target, certifiedValue := (state.effects.map Prod.snd).sum}
  else state

theorem step_valid (E : Identity → Nat → Nat) (state : Journal) (valid : Valid E state) :
    Valid E (step E state) := by
  obtain ⟨bound, ledger⟩ := valid
  unfold step
  split <;> rename_i h
  · constructor
    · simp only; omega
    · simp only
      rw [ledger]
      simp [canonical, List.range_succ, List.map_append]
  · exact ⟨bound, ledger⟩

theorem step_preserves_checkpoint (E : Identity → Nat → Nat) (state : Journal) :
    (step E state).certified = state.certified ∧
      (step E state).certifiedValue = state.certifiedValue ∧ (step E state).target = state.target := by
  unfold step
  split <;> exact ⟨rfl, rfl, rfl⟩
theorem progress_monotone (E : Identity → Nat → Nat) (state : Journal) :
    state.completed ≤ (step E state).completed := by
  unfold step
  split <;> simp <;> omega

theorem ordinal_ledger (E : Identity → Nat → Nat) (state : Journal) (valid : Valid E state) :
    state.effects.map Prod.fst = List.range state.completed := by
  rw [valid.2]
  simp [canonical, List.map_map, Function.comp_def]

theorem range_unique (n : Nat) : (List.range n).Nodup := by
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.range_succ, List.nodup_append]
    constructor
    · exact ih
    constructor
    · simp
    · intro a member b last
      have smaller : a < n := List.mem_range.mp member
      have same : b = n := by simpa using last
      omega

theorem unique_effects (E : Identity → Nat → Nat) (state : Journal) (valid : Valid E state) :
    (state.effects.map Prod.fst).Nodup := by
  rw [ordinal_ledger E state valid]
  exact range_unique state.completed

theorem crash_resume_preserves (state : Journal) :
    (resume (crash state)).effects = state.effects ∧
    (resume (crash state)).completed = state.completed ∧
    (resume (crash state)).certified = state.certified ∧
    (resume (crash state)).certifiedValue = state.certifiedValue ∧
    (resume (crash state)).target = state.target := by
  exact ⟨rfl, rfl, rfl, rfl, rfl⟩
theorem retry_valid (E : Identity → Nat → Nat) (state : Journal) (valid : Valid E state) :
    Valid E (resume (crash state)) := valid

theorem stopped_worker_no_step (E : Identity → Nat → Nat) (state : Journal) :
    step E (crash state) = crash state := by simp [step, crash]

theorem retry_step_unique_effects (E : Identity → Nat → Nat) (state : Journal) (valid : Valid E state) :
    ((step E (resume (crash state))).effects.map Prod.fst).Nodup := by
  exact unique_effects E _ (step_valid E _ (retry_valid E state valid))

theorem unfinished_not_certified (state : Journal) (unfinished : state.completed ≠ state.total) :
    finish state = state := by simp [finish, unfinished]
theorem finish_exact (E : Identity → Nat → Nat) (state : Journal) (valid : Valid E state)
    (done : state.completed = state.total) :
    (finish state).certified = state.target ∧
      (finish state).certifiedValue = ((canonical E state.target state.total).map Prod.snd).sum := by
  simp [finish, done, valid.2]
theorem finish_idempotent (state : Journal) : finish (finish state) = finish state := by
  unfold finish
  split <;> rename_i h
  · simp [h]
  · simp
#print axioms step_valid
#print axioms step_preserves_checkpoint
#print axioms progress_monotone
#print axioms unique_effects
#print axioms crash_resume_preserves
#print axioms retry_valid
#print axioms unfinished_not_certified
#print axioms finish_exact
#print axioms finish_idempotent
#print axioms stopped_worker_no_step
#print axioms retry_step_unique_effects
end ConcordRecovery
