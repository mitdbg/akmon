import Bounds
namespace ConcordProgress
open ConcordBounded
abbrev Graph := Nat → List Nat
-- Settlement covers the entire indexed dependency group.
def Settled (history : Nat → Graph) (targetRound subject : Nat) :=
  ∀ later, targetRound ≤ later → history later subject = history targetRound subject
inductive Event where
  | work : Nat → Event
  | commit : Nat → Nat → Nat → Event  -- version, subject, evaluated result
  deriving DecidableEq
-- The final operation of the target evaluates its pinned group and commits it.
def marker (history : Nat → Graph) (version subject : Nat) :=
  Event.commit version subject (history version subject).sum

def targetPrefix (history : Nat → Graph) (version subject : Nat) (earlier : List Event) :=
  earlier ++ (history version subject).map Event.work ++ [marker history version subject]
-- Executing FIFO charged units emits their immutable commit events. New work
-- only appends behind this prefix. Read the committed target result from the log.
def readTarget (history : Nat → Graph) (version subject : Nat) (executed : List Event) : Option Nat :=
  if marker history version subject ∈ executed then some (history version subject).sum else none

theorem prefix_emitted (ahead future : List Event) (served : Nat) (enough : ahead.length ≤ served) :
    ∀ event ∈ ahead, event ∈ (ahead ++ future).take served := by
  rw [List.take_append, List.take_of_length_le enough]
  intro event member
  exact List.mem_append_left _ member

theorem settled_subject_catches_up (history : Nat → Graph)
    (version subject burst tokens refill arrivals backlog capacity later : Nat)
    (earlier future : List Event)
    (prefixCharge : (targetPrefix history version subject earlier).length ≤ backlog + arrivals)
    (initial : backlog + tokens ≤ burst)
    (admitted : arrivals ≤ allowance burst tokens refill)
    (positive : 1 ≤ capacity)
    (settled : Settled history version subject) (afterTarget : version ≤ later) :
    readTarget history version subject
      ((targetPrefix history version subject earlier ++ future).take ((burst + refill) * capacity)) =
        some (history later subject).sum := by
  have bounded := admitted_ahead_bound burst tokens refill arrivals backlog initial admitted
  have multiplied : burst + refill ≤ (burst + refill) * capacity := by
    have := Nat.mul_le_mul_left (burst + refill) positive
    simpa using this
  have prefixBound : (targetPrefix history version subject earlier).length ≤
      (burst + refill) * capacity := by omega
  have emitted := prefix_emitted (targetPrefix history version subject earlier) future
    ((burst + refill) * capacity) prefixBound (marker history version subject)
    (by simp [targetPrefix])
  simp only [readTarget, emitted, ite_true]
  rw [settled later afterTarget]

#print axioms prefix_emitted
#print axioms settled_subject_catches_up
end ConcordProgress
