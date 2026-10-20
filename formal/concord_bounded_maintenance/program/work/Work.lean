import Std.Tactic
namespace ConcordWork
abbrev Graph := Nat → List Nat
abbrev Cache := Nat → Nat

def full (g : Graph) (s : Nat) := (g s).sum
-- Functional sparse writes. Evaluation reads exactly each selected group's cells.
def maintain (old : Cache) (g : Graph) (touched : List Nat) (s : Nat) :=
  if s ∈ touched then full g s else old s
-- Explicit operation units: record reads, group evaluation, sparse write, trace
-- reads, index edits, answer root reads/deletions, retry effects, commit.
def work (g : Graph) (touched trace indexEdits roots deleted retries : List Nat) :=
  (touched.map (fun s => (g s).length + 2)).sum + trace.length + indexEdits.length +
    roots.length + deleted.length + retries.length + 1

theorem locality_result (a b : Graph) (old : Cache) (touched : List Nat)
    (same : ∀ s ∈ touched, a s = b s) :
    maintain old a touched = maintain old b touched := by
  funext s
  unfold maintain full
  split <;> rename_i h
  · rw [same s h]
  · rfl

theorem locality_work (a b : Graph) (touched trace edits roots deleted retries : List Nat)
    (same : ∀ s ∈ touched, a s = b s) :
    work a touched trace edits roots deleted retries =
      work b touched trace edits roots deleted retries := by
  unfold work
  have equal : touched.map (fun s => (a s).length + 2) =
      touched.map (fun s => (b s).length + 2) := by
    apply List.map_congr_left
    intro s member
    rw [same s member]
  rw [equal]

theorem maintained_exact (old new : Graph) (touched : List Nat)
    (outside : ∀ s, s ∉ touched → old s = new s) :
    maintain (full old) new touched = full new := by
  funext s
  unfold maintain
  split <;> rename_i h
  · rfl
  · unfold full
    rw [outside s h]

-- The sparse evaluator executes selected groups and materializes only deltas.
def runGroups (g : Graph) : List Nat → List (Nat × Nat) × Nat
  | [] => ([], 0)
  | key :: rest =>
      let tail := runGroups g rest
      ((key, full g key) :: tail.1, (g key).length + 2 + tail.2)

theorem run_groups_cost (g : Graph) (keys : List Nat) :
    (runGroups g keys).2 = (keys.map (fun key => (g key).length + 2)).sum := by
  induction keys with
  | nil => rfl
  | cons key rest ih => simp [runGroups, ih, Nat.add_assoc]

-- Membership-based dedup may be quadratic in references, not graph size.
-- Charge the full candidate tail scan even when it stops early.
def dedupBudget : List Nat → Nat
  | [] => 0
  | _ :: rest => rest.length + 1 + dedupBudget rest

theorem dedup_budget (keys : List Nat) : dedupBudget keys ≤ keys.length * keys.length := by
  induction keys with
  | nil => simp [dedupBudget]
  | cons key rest ih =>
    simp only [dedupBudget, List.length_cons, Nat.succ_mul, Nat.mul_succ]
    omega

-- End-to-end abstract batch: materialize sparse group outputs, consume trace
-- and index-edit operation streams, enumerate answer roots, deduplicate them,
-- look up retained answers, retire obsolete entries, execute retry effects,
-- then commit. Reference extraction operations belong in rootOps separately.
def batchCost (g : Graph) (keys trace edits rootOps refs retired retries : List Nat) :=
  (runGroups g keys).2 + trace.length + edits.length + rootOps.length +
    dedupBudget refs + refs.length + retired.length + retries.length + 1

theorem batch_bound (g : Graph) (keys trace edits rootOps refs retired retries : List Nat) :
    batchCost g keys trace edits rootOps refs retired retries ≤
      (keys.map (fun key => (g key).length + 2)).sum + trace.length + edits.length +
      rootOps.length + refs.length * refs.length + refs.length + retired.length + retries.length + 1 := by
  unfold batchCost
  rw [run_groups_cost]
  have budget := dedup_budget refs
  omega

theorem batch_unrelated_independence (a b : Graph)
    (keys trace edits rootOps refs retired retries : List Nat)
    (same : ∀ s ∈ keys, a s = b s) :
    batchCost a keys trace edits rootOps refs retired retries =
      batchCost b keys trace edits rootOps refs retired retries := by
  unfold batchCost
  rw [run_groups_cost, run_groups_cost]
  have equal : keys.map (fun s => (a s).length + 2) = keys.map (fun s => (b s).length + 2) := by
    apply List.map_congr_left
    intro s member
    rw [same s member]
  rw [equal]

-- The work equation is an executable abstract-operation bound, not a CPU bound:
-- cached indexed group/trace access must avoid hidden global scans/copies.
#print axioms locality_result
#print axioms locality_work
#print axioms maintained_exact
#print axioms run_groups_cost
#print axioms dedup_budget
#print axioms batch_bound
#print axioms batch_unrelated_independence
end ConcordWork
