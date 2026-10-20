import Uncertainty
namespace ConcordCleanup
open ConcordUncertainty
-- Root expressions include certified, active and queued pinned inputs, even
-- expressions that currently produce no finding. New jobs install roots first.
structure Roots where
  checkpoint : List Expr
  unfinished : List Expr

def references (roots : Roots) := roots.checkpoint.flatMap deps ++ roots.unfinished.flatMap deps

def unique : List Nat → List Nat
  | [] => []
  | key :: rest => let tail := unique rest
                  if key ∈ tail then tail else key :: tail

theorem mem_unique (keys : List Nat) (key : Nat) : key ∈ unique keys ↔ key ∈ keys := by
  induction keys generalizing key with
  | nil => simp [unique]
  | cons head tail ih =>
    simp only [unique]
    split <;> rename_i h
    · simp only [List.mem_cons]
      rw [ih key]
      have found := (ih head).mp h
      constructor
      · exact Or.inr
      · intro member
        cases member with
        | inl equal => simpa [equal] using found
        | inr member => exact member
    · simp [List.mem_cons, ih]

theorem unique_length (keys : List Nat) : (unique keys).length ≤ keys.length := by
  induction keys with
  | nil => simp [unique]
  | cons head tail ih =>
    simp only [unique]
    split <;> simp only [List.length_cons] <;> omega

def clean (roots : Roots) (store : Store) (key : Nat) :=
  if key ∈ unique (references roots) then store key else none

def retained (roots : Roots) (store : Store) := (unique (references roots)).filterMap fun key =>
  (store key).map fun answer => (key, answer)

theorem checkpoint_ref (roots : Roots) (expression : Expr) (key : Nat)
    (root : expression ∈ roots.checkpoint) (member : key ∈ deps expression) : key ∈ references roots := by
  simp only [references, List.mem_append, List.mem_flatMap]
  exact Or.inl ⟨expression, root, member⟩
theorem unfinished_ref (roots : Roots) (expression : Expr) (key : Nat)
    (root : expression ∈ roots.unfinished) (member : key ∈ deps expression) : key ∈ references roots := by
  simp only [references, List.mem_append, List.mem_flatMap]
  exact Or.inr ⟨expression, root, member⟩
theorem preserves_needed (roots : Roots) (store : Store) (key : Nat)
    (needed : key ∈ references roots) : clean roots store key = store key := by
  simp [clean, mem_unique, needed]

theorem preserves_root_evaluation (roots : Roots) (store : Store) (expression : Expr)
    (live : expression ∈ roots.checkpoint ∨ expression ∈ roots.unfinished) :
    evaluate (approximate (clean roots store)) expression = evaluate (approximate store) expression := by
  apply evaluation_local
  intro key member
  have referenced : key ∈ references roots := by
    cases live with
    | inl h => exact checkpoint_ref roots expression key h member
    | inr h => exact unfinished_ref roots expression key h member
  unfold approximate
  rw [preserves_needed roots store key referenced]

theorem preserves_root_marked (roots : Roots) (store : Store) (expression : Expr)
    (live : expression ∈ roots.checkpoint ∨ expression ∈ roots.unfinished) :
    Marked (clean roots store) expression ↔ Marked store expression := by
  have reference : ∀ key ∈ deps expression, key ∈ references roots := by
    intro key member
    cases live with
    | inl h => exact checkpoint_ref roots expression key h member
    | inr h => exact unfinished_ref roots expression key h member
  constructor
  · rintro ⟨key, member, missing⟩
    rw [preserves_needed roots store key (reference key member)] at missing
    exact ⟨key, member, missing⟩
  · rintro ⟨key, member, missing⟩
    refine ⟨key, member, ?_⟩
    rw [preserves_needed roots store key (reference key member)]
    exact missing

theorem cleanup_idempotent (roots : Roots) (store : Store) :
    clean roots (clean roots store) = clean roots store := by
  funext key
  unfold clean
  split <;> simp_all

theorem retained_bound (roots : Roots) (store : Store) :
    (retained roots store).length ≤ (unique (references roots)).length := by
  exact List.length_filterMap_le _ _

theorem expression_reference_bound (expressions : List Expr) (perExpression : Nat)
    (limited : ∀ expression ∈ expressions, (deps expression).length ≤ perExpression) :
    (expressions.flatMap deps).length ≤ expressions.length * perExpression := by
  induction expressions with
  | nil => simp
  | cons expression rest ih =>
    have head := limited expression (by simp)
    have tail := ih (by intro e member; exact limited e (by simp [member]))
    simp only [List.flatMap_cons, List.length_append, List.length_cons, Nat.succ_mul]
    omega

theorem storage_bound (roots : Roots) (store : Store) (maxRefs : Nat)
    (checkpointCap : ∀ e ∈ roots.checkpoint, (deps e).length ≤ maxRefs)
    (pendingCap : ∀ e ∈ roots.unfinished, (deps e).length ≤ maxRefs) :
    (retained roots store).length ≤
      (roots.checkpoint.length + roots.unfinished.length) * maxRefs := by
  have retainedCount := retained_bound roots store
  have uniqueCount := unique_length (references roots)
  have checkpointCount := expression_reference_bound roots.checkpoint maxRefs checkpointCap
  have pendingCount := expression_reference_bound roots.unfinished maxRefs pendingCap
  simp only [references, List.length_append] at uniqueCount retainedCount
  rw [Nat.add_mul]
  omega

#print axioms mem_unique
#print axioms unique_length
#print axioms preserves_root_evaluation
#print axioms cleanup_idempotent
#print axioms preserves_root_marked
#print axioms retained_bound
#print axioms storage_bound
end ConcordCleanup
