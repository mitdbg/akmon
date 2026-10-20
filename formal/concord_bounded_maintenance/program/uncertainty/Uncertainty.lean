import Std.Tactic
namespace ConcordUncertainty
inductive Expr where
  | literal : Nat → Expr
  | answer : Nat → Expr
  | add : Expr → Expr → Expr
  deriving DecidableEq
abbrev Store := Nat → Option Nat

def deps : Expr → List Nat
  | .literal _ => []
  | .answer key => [key]
  | .add left right => deps left ++ deps right

def evaluate (answers : Nat → Nat) : Expr → Nat
  | .literal n => n
  | .answer key => answers key
  | .add left right => evaluate answers left + evaluate answers right

def approximate (store : Store) (key : Nat) := (store key).getD 0

def Marked (store : Store) (expression : Expr) : Prop :=
  ∃ key ∈ deps expression, store key = none

def Completes (store : Store) (answers : Nat → Nat) : Prop :=
  ∀ key value, store key = some value → answers key = value

theorem evaluation_local (expression : Expr) (a b : Nat → Nat)
    (same : ∀ key ∈ deps expression, a key = b key) :
    evaluate a expression = evaluate b expression := by
  induction expression with
  | literal n => rfl
  | answer key => exact same key (by simp [deps])
  | add left right ihl ihr =>
    simp only [evaluate]
    rw [ihl (by intro key member; exact same key (by simp [deps, member])),
        ihr (by intro key member; exact same key (by simp [deps, member]))]

theorem unmarked_sound (store : Store) (expression : Expr)
    (unmarked : ¬ Marked store expression) (answers : Nat → Nat)
    (completion : Completes store answers) :
    evaluate (approximate store) expression = evaluate answers expression := by
  apply evaluation_local
  intro key member
  cases present : store key with
  | none => exact False.elim (unmarked ⟨key, member, present⟩)
  | some value =>
    unfold approximate
    rw [present]
    simp only [Option.getD_some]
    exact (completion key value present).symm

theorem mark_add (store : Store) (left right : Expr) :
    Marked store (.add left right) ↔ Marked store left ∨ Marked store right := by
  simp only [Marked, deps, List.mem_append]
  constructor
  · rintro ⟨key, member, missing⟩
    cases member with
    | inl h => exact Or.inl ⟨key, h, missing⟩
    | inr h => exact Or.inr ⟨key, h, missing⟩
  · intro h
    cases h with
    | inl h => obtain ⟨key, member, missing⟩ := h; exact ⟨key, Or.inl member, missing⟩
    | inr h => obtain ⟨key, member, missing⟩ := h; exact ⟨key, Or.inr member, missing⟩

theorem irrelevant_missing_does_not_mark (store : Store) (expression : Expr)
    (complete_dependencies : ∀ key ∈ deps expression, (store key).isSome = true) :
    ¬ Marked store expression := by
  rintro ⟨key, member, missing⟩
  have present := complete_dependencies key member
  simp [missing] at present

#print axioms evaluation_local
#print axioms unmarked_sound
#print axioms mark_add
#print axioms irrelevant_missing_does_not_mark
end ConcordUncertainty
