import Std.Tactic
namespace ConcordUpdates
abbrev Graph := Nat → List Nat
abbrev Findings := Nat → Option Nat

def full (g : Graph) (s : Nat) : Option Nat :=
  if (g s).isEmpty then none else some (g s).sum

def write (g : Graph) (key : Nat) (values : List Nat) (s : Nat) :=
  if s = key then values else g s

def refresh (old : Findings) (new : Graph) (source target s : Nat) :=
  if s = source ∨ s = target then full new s else old s

-- A row move uses the actual old and new group: delete source old, add target new.
def move (g : Graph) (source target value : Nat) : Graph :=
  if source = target ∨ value ∉ g source then g
  else write (write g source ((g source).erase value)) target (value :: g target)

def delete (g : Graph) (group value : Nat) : Graph :=
  write g group ((g group).erase value)

theorem move_outside (g : Graph) (source target value s : Nat)
    (notFrom : s ≠ source) (notTo : s ≠ target) : move g source target value s = g s := by
  unfold move
  split
  · rfl
  · simp [write, notFrom, notTo]

theorem move_exact (g : Graph) (source target value : Nat) :
    refresh (full g) (move g source target value) source target = full (move g source target value) := by
  funext s
  unfold refresh
  split <;> rename_i h
  · rfl
  · have notFrom : s ≠ source := by omega
    have notTo : s ≠ target := by omega
    unfold full
    rw [move_outside g source target value s notFrom notTo]

theorem delete_exact (g : Graph) (group value : Nat) :
    refresh (full g) (delete g group value) group group = full (delete g group value) := by
  funext s
  unfold refresh delete full write
  split <;> rename_i h
  · rfl
  · have different : s ≠ group := by omega
    simp [different]

theorem deleted_last_finding (g : Graph) (group value : Nat)
    (only : g group = [value]) :
    refresh (full g) (delete g group value) group group group = none := by
  simp [refresh, delete, write, full, only]

-- Same-group and absent-value requests are explicit no-ops.
theorem moving_to_new_group (g : Graph) (source target value : Nat) (different : source ≠ target) (present : value ∈ g source) :
    move g source target value source = (g source).erase value ∧
      move g source target value target = value :: g target := by
  simp [move, write, different, present]
theorem move_noop (g : Graph) (source target value : Nat)
    (noop : source = target ∨ value ∉ g source) : move g source target value = g := by
  simp [move, noop]

theorem erase_count (xs : List Nat) (value : Nat) (present : value ∈ xs) :
    (xs.erase value).length + 1 = xs.length := by
  induction xs with
  | nil => simp at present
  | cons head tail ih =>
    by_cases same : head = value
    · subst head
      simp
    · have tailMember : value ∈ tail := by simpa [List.mem_cons, Ne.symm same] using present
      have shorter := ih tailMember
      simp only [List.erase_cons, beq_iff_eq, same, ite_false, List.length_cons]
      omega

theorem erase_sum (xs : List Nat) (value : Nat) (present : value ∈ xs) :
    (xs.erase value).sum + value = xs.sum := by
  induction xs with
  | nil => simp at present
  | cons head tail ih =>
    by_cases same : head = value
    · subst head
      simp [Nat.add_comm]
    · have tailMember : value ∈ tail := by simpa [List.mem_cons, Ne.symm same] using present
      have smaller := ih tailMember
      simp only [List.erase_cons, beq_iff_eq, same, ite_false, List.sum_cons]
      omega

theorem move_preserves_value (g : Graph) (source target value : Nat)
    (different : source ≠ target) (present : value ∈ g source) :
    (move g source target value source).sum + (move g source target value target).sum =
      (g source).sum + (g target).sum := by
  obtain ⟨oldGroup, newGroup⟩ := moving_to_new_group g source target value different present
  rw [oldGroup, newGroup, List.sum_cons]
  have mass := erase_sum (g source) value present
  omega

theorem move_preserves_count (g : Graph) (source target value : Nat)
    (different : source ≠ target) (present : value ∈ g source) :
    (move g source target value source).length +
      (move g source target value target).length = (g source).length + (g target).length := by
  obtain ⟨oldGroup, newGroup⟩ := moving_to_new_group g source target value different present
  rw [oldGroup, newGroup, List.length_cons]
  have count := erase_count (g source) value present
  omega

#print axioms move_exact
#print axioms delete_exact
#print axioms deleted_last_finding
#print axioms moving_to_new_group
#print axioms move_noop
#print axioms move_preserves_count
#print axioms move_preserves_value
end ConcordUpdates
