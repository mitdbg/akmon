import Pairs
namespace ConcordPairLevel
open ConcordPairs
/-
Pair-level UPDATE (decision of 10-01): UPDATE re-checks only the affected
pairs, not whole buckets. The bucket-level model and its theorems stay in
Pairs.lean; this module adds the pair-level store and step.

Stored per rule: claims by bucket, unplaced IDs, one stored row per ordered
claim-ID pair (left.id < right.id) of every known bucket, and a request index
from a request to the pairs whose test read it. A stored row also carries its
two claims; this stands for a lookup of the claims by ID, which a re-test
needs. The rule-change branch takes a full pass and is out of scope.

Step for removed R, added Ad, arrived D, new answers A1:
1. Each removed claim x with known bucket b: for every claim z in the current
   claims of b, delete the stored pair (x, z) and its index entries; then drop
   x from the claims of b. Unknown: drop x.id from unplaced.
2. Each added claim y with known bucket b, in list order: for every claim z in
   the current claims of b (earlier-added claims included), test the ordered
   pair under A1, store the row, add its index entries; then insert y.
   Unknown: add y.id to unplaced.
3. Each entry (q, p) of the OLD index with q in D: if p was not tested in 2
   and p still exists, re-test p under A1 and replace its row and entries.

Deleting the pairs (x, z) for z in the current claims of x's bucket is the
same as deleting every stored pair containing x.id when the state is exact;
it also makes the number of deletions countable.
This is a proved model of the design, not a proof of production code.
-/

abbrev Key := Nat × Nat

structure Row where
  l : Claim
  r : Claim
  out : Outcome
  reqs : List Request
deriving DecidableEq, Repr

-- The row a pair test under answers A produces.
def rowOf (rule : Rule) (A : Answers) (l r : Claim) : Row :=
  ⟨l, r, rule.test l r A, rule.req l r⟩

structure PState where
  claimsByBucket : Nat → List Claim
  unplaced : List Nat
  pairOut : Key → Option Row
  pairIndex : Request → List Key

-- A step in progress: the state, the pairs tested in phase 2, and counters.
structure Run where
  st : PState
  tested : List Key
  tests : Nat
  deletes : Nat

-- Claim IDs are keys of a claim list.
def Keys (cs : List Claim) : Prop := ∀ x ∈ cs, ∀ y ∈ cs, x.id = y.id → x = y

-- A removed claim's ID names no remaining old claim. Implied by R ⊆ c0 and
-- Keys c0 (removedIds_of_subset).
def RemovedIds (c0 R : List Claim) : Prop := ∀ x ∈ R, ∀ y ∈ c0, y.id = x.id → y ∈ R

-- A stored row is right for claims cs and answers A: its two claims share a
-- bucket of cs, are ordered by ID, sit at their key, and hold the test result
-- and requests under A.
def Good (rule : Rule) (cs : List Claim) (A : Answers) (k : Key) (row : Row) : Prop :=
  (∃ b, row.l ∈ members cs b ∧ row.r ∈ members cs b) ∧ row.l.id < row.r.id ∧
    k = (row.l.id, row.r.id) ∧ row = rowOf rule A row.l row.r

-- The index is complete and sound for the stored rows.
def IndexOK (st : PState) : Prop :=
  ∀ k q, k ∈ st.pairIndex q ↔ ∃ row, st.pairOut k = some row ∧ q ∈ row.reqs

-- The stored state equals the from-scratch state of claims cs and answers A.
structure PExact (rule : Rule) (st : PState) (cs : List Claim) (A : Answers) : Prop where
  claims : ∀ b x, x ∈ st.claimsByBucket b ↔ x ∈ members cs b
  unplaced : ∀ i, i ∈ st.unplaced ↔ i ∈ unknownIds cs
  stored : ∀ k row, st.pairOut k = some row → Good rule cs A k row
  every : ∀ b l r, l ∈ members cs b → r ∈ members cs b → l.id < r.id →
    st.pairOut (l.id, r.id) = some (rowOf rule A l r)
  index : IndexOK st

/- Primitive store operations. -/

def delPair (st : PState) (k : Key) : PState :=
  ⟨st.claimsByBucket, st.unplaced, fun k' => if k' = k then none else st.pairOut k',
    fun q => (st.pairIndex q).filter (fun k' => k' ≠ k)⟩

def putPair (st : PState) (k : Key) (row : Row) : PState :=
  ⟨st.claimsByBucket, st.unplaced, fun k' => if k' = k then some row else st.pairOut k',
    fun q => (st.pairIndex q).filter (fun k' => k' ≠ k) ++ (if q ∈ row.reqs then [k] else [])⟩

def setBucket (st : PState) (b : Nat) (ms : List Claim) : PState :=
  ⟨fun b' => if b' = b then ms else st.claimsByBucket b', st.unplaced, st.pairOut, st.pairIndex⟩

def setUnplaced (st : PState) (u : List Nat) : PState :=
  ⟨st.claimsByBucket, u, st.pairOut, st.pairIndex⟩

def withSt (r : Run) (st : PState) : Run := ⟨st, r.tested, r.tests, r.deletes⟩

-- The ordered pair of two claims and its key.
def orient (y z : Claim) : Claim × Claim := if y.id < z.id then (y, z) else (z, y)

def keyOf (y z : Claim) : Key := ((orient y z).1.id, (orient y z).2.id)

/- Phase 1: removals. -/

def delPairsOf (x : Claim) : List Claim → Run → Run
  | [], r => r
  | z :: zs, r => delPairsOf x zs
      (if z.id = x.id then r else ⟨delPair r.st (keyOf x z), r.tested, r.tests, r.deletes + 1⟩)

def removeOne (r : Run) (x : Claim) : Run :=
  match x.bucket with
  | none => withSt r (setUnplaced r.st (r.st.unplaced.filter (fun i => i ≠ x.id)))
  | some b =>
    withSt (delPairsOf x (r.st.claimsByBucket b) r)
      (setBucket (delPairsOf x (r.st.claimsByBucket b) r).st b
        ((r.st.claimsByBucket b).filter (fun z => z ≠ x)))

/- Phase 2: additions, in list order. -/

def testPairsOf (rule : Rule) (A1 : Answers) (y : Claim) : List Claim → Run → Run
  | [], r => r
  | z :: zs, r => testPairsOf rule A1 y zs
      (if z.id = y.id then r else
        ⟨putPair r.st (keyOf y z) (rowOf rule A1 (orient y z).1 (orient y z).2),
          keyOf y z :: r.tested, r.tests + 1, r.deletes⟩)

def addOne (rule : Rule) (A1 : Answers) (r : Run) (y : Claim) : Run :=
  match y.bucket with
  | none => withSt r (setUnplaced r.st (r.st.unplaced ++ [y.id]))
  | some b =>
    withSt (testPairsOf rule A1 y (r.st.claimsByBucket b) r)
      (setBucket (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st b
        (r.st.claimsByBucket b ++ [y]))

/- Phase 3: arrived answers through the old index. -/

def retest (rule : Rule) (A1 : Answers) (row : Row) : Row := rowOf rule A1 row.l row.r

def retestOne (rule : Rule) (A1 : Answers) (r : Run) (k : Key) : Run :=
  if k ∈ r.tested then r else
    match r.st.pairOut k with
    | none => r
    | some row => ⟨putPair r.st k (retest rule A1 row), r.tested, r.tests + 1, r.deletes⟩

def retestKeys (rule : Rule) (A1 : Answers) : List Key → Run → Run
  | [], r => r
  | k :: ks, r => retestKeys rule A1 ks (retestOne rule A1 r k)

def phase1 (st : PState) (R : List Claim) : Run := R.foldl removeOne ⟨st, [], 0, 0⟩

def phase2 (rule : Rule) (A1 : Answers) (r : Run) (Ad : List Claim) : Run :=
  Ad.foldl (addOne rule A1) r

def pairStep (rule : Rule) (st : PState) (R Ad : List Claim) (D : List Request) (A1 : Answers) :
    Run :=
  retestKeys rule A1 (D.flatMap st.pairIndex) (phase2 rule A1 (phase1 st R) Ad)

-- The derived per-bucket view that findings, issues and COMPONENTS read: the
-- stored rows of the pairs of the bucket's current claims.
def view (st : PState) (b : Nat) : BucketOut :=
  ⟨(pairs (st.claimsByBucket b)).filterMap (fun p =>
      (st.pairOut (p.1.id, p.2.id)).map (fun row => (p.1.id, p.2.id, row.out))),
    (pairs (st.claimsByBucket b)).flatMap (fun p =>
      ((st.pairOut (p.1.id, p.2.id)).map Row.reqs).getD [])⟩

def emptyState : PState := ⟨fun _ => [], [], fun _ => none, fun _ => []⟩

/- Basic facts. -/

theorem removedIds_of_subset (c0 R : List Claim) (sub : ∀ x ∈ R, x ∈ c0) (keys : Keys c0) :
    RemovedIds c0 R := by
  intro x hx y hy e
  rw [keys y hy x (sub x hx) e]
  exact hx

theorem removedIds_unknown (c0 R : List Claim) (h : RemovedIds c0 R) : UnknownIdsRemoved c0 R := by
  intro x hx y hy _ _ e
  exact h x hx y hy e.symm

theorem delPair_index (st : PState) (k : Key) (h : IndexOK st) : IndexOK (delPair st k) := by
  intro k' q
  show k' ∈ (st.pairIndex q).filter (fun k'' => k'' ≠ k) ↔
    ∃ row, (if k' = k then none else st.pairOut k') = some row ∧ q ∈ row.reqs
  rw [List.mem_filter, decide_eq_true_eq, h k' q]
  by_cases e : k' = k
  · simp [e]
  · simp [e]

theorem putPair_index (st : PState) (k : Key) (row : Row) (h : IndexOK st) :
    IndexOK (putPair st k row) := by
  intro k' q
  show k' ∈ (st.pairIndex q).filter (fun k'' => k'' ≠ k) ++ (if q ∈ row.reqs then [k] else []) ↔
    ∃ row', (if k' = k then some row else st.pairOut k') = some row' ∧ q ∈ row'.reqs
  rw [List.mem_append, List.mem_filter, decide_eq_true_eq, h k' q]
  by_cases e : k' = k
  · subst e
    by_cases hq : q ∈ row.reqs <;> simp [hq]
  · by_cases hq : q ∈ row.reqs <;> simp [e, hq]

theorem orient_cases (y z : Claim) : orient y z = (y, z) ∨ orient y z = (z, y) := by
  unfold orient
  split
  · exact Or.inl rfl
  · exact Or.inr rfl

theorem orient_lt (y z : Claim) (ne : z.id ≠ y.id) : (orient y z).1.id < (orient y z).2.id := by
  unfold orient
  split
  · assumption
  · rename_i h
    show z.id < y.id
    omega

theorem keyOf_lt (x z : Claim) (lt : x.id < z.id) : keyOf x z = (x.id, z.id) := by
  simp [keyOf, orient, lt]

theorem keyOf_gt (x z : Claim) (gt : z.id < x.id) : keyOf x z = (z.id, x.id) := by
  have h : ¬ x.id < z.id := by omega
  simp [keyOf, orient, h]

theorem keyOf_has (x z : Claim) : (keyOf x z).1 = x.id ∨ (keyOf x z).2 = x.id := by
  unfold keyOf
  rcases orient_cases x z with h | h <;> rw [h]
  · exact Or.inl rfl
  · exact Or.inr rfl

-- With keyed claims, a written key names exactly the pair it was written for.
theorem orient_eq (c1 : List Claim) (keys : Keys c1) (y z l r : Claim) (hy : y ∈ c1)
    (hz : z ∈ c1) (hl : l ∈ c1) (hr : r ∈ c1) (ek : keyOf y z = (l.id, r.id)) :
    (orient y z).1 = l ∧ (orient y z).2 = r := by
  have e1 : (orient y z).1.id = l.id := congrArg Prod.fst ek
  have e2 : (orient y z).2.id = r.id := congrArg Prod.snd ek
  have m1 : (orient y z).1 ∈ c1 := by
    rcases orient_cases y z with h | h <;> rw [h]
    · exact hy
    · exact hz
  have m2 : (orient y z).2 ∈ c1 := by
    rcases orient_cases y z with h | h <;> rw [h]
    · exact hz
    · exact hy
  exact ⟨keys _ m1 l hl e1, keys _ m2 r hr e2⟩

theorem new_good (rule : Rule) (c1 : List Claim) (A1 : Answers) (y z : Claim) (b : Nat)
    (hy : y ∈ members c1 b) (hz : z ∈ members c1 b) (ne : z.id ≠ y.id) :
    Good rule c1 A1 (keyOf y z) (rowOf rule A1 (orient y z).1 (orient y z).2) := by
  refine ⟨⟨b, ?_, ?_⟩, orient_lt y z ne, rfl, rfl⟩
  · show (orient y z).1 ∈ members c1 b
    rcases orient_cases y z with h | h <;> rw [h]
    · exact hy
    · exact hz
  · show (orient y z).2 ∈ members c1 b
    rcases orient_cases y z with h | h <;> rw [h]
    · exact hz
    · exact hy

theorem unknownIds_snoc_none (cs : List Claim) (x : Claim) (h : x.bucket = none) (i : Nat) :
    i ∈ unknownIds (cs ++ [x]) ↔ i ∈ unknownIds cs ∨ i = x.id := by
  rw [mem_unknownIds, mem_unknownIds]
  constructor
  · rintro ⟨c, hc, hn, hi⟩
    rcases List.mem_append.1 hc with hc | hc
    · exact Or.inl ⟨c, hc, hn, hi⟩
    · rw [List.mem_singleton] at hc
      rw [hc] at hi
      exact Or.inr hi.symm
  · rintro (⟨c, hc, hn, hi⟩ | hi)
    · exact ⟨c, List.mem_append_left _ hc, hn, hi⟩
    · exact ⟨x, List.mem_append_right _ (List.mem_singleton.2 rfl), h, hi.symm⟩

theorem unknownIds_snoc_some (cs : List Claim) (x : Claim) (b : Nat) (h : x.bucket = some b)
    (i : Nat) : i ∈ unknownIds (cs ++ [x]) ↔ i ∈ unknownIds cs := by
  rw [mem_unknownIds, mem_unknownIds]
  constructor
  · rintro ⟨c, hc, hn, hi⟩
    rcases List.mem_append.1 hc with hc | hc
    · exact ⟨c, hc, hn, hi⟩
    · rw [List.mem_singleton] at hc
      rw [hc, h] at hn
      cases hn
  · rintro ⟨c, hc, hn, hi⟩
    exact ⟨c, List.mem_append_left _ hc, hn, hi⟩

theorem mem_members_snoc (cs : List Claim) (y : Claim) (b b' : Nat) (hb : y.bucket = some b)
    (z : Claim) : z ∈ members (cs ++ [y]) b' ↔ z ∈ members cs b' ∨ (b' = b ∧ z = y) := by
  rw [mem_members, mem_members, List.mem_append, List.mem_singleton]
  constructor
  · rintro ⟨hz | hz, hz'⟩
    · exact Or.inl ⟨hz, hz'⟩
    · rw [hz, hb] at hz'
      exact Or.inr ⟨(Option.some.inj hz').symm, hz⟩
  · rintro (⟨hz, hz'⟩ | ⟨e, hz⟩)
    · exact ⟨Or.inl hz, hz'⟩
    · rw [hz, hb, e]
      exact ⟨Or.inr rfl, rfl⟩

theorem mem_members_snoc_none (cs : List Claim) (y : Claim) (hb : y.bucket = none) (b : Nat)
    (z : Claim) : z ∈ members (cs ++ [y]) b ↔ z ∈ members cs b := by
  rw [members_append, members_unplaced [y] b (fun x hx => by rw [List.mem_singleton.1 hx]; exact hb),
    List.append_nil]

/- Phase 1 lemmas. -/

theorem delPairsOf_same (x : Claim) : ∀ (zs : List Claim) (r : Run),
    (delPairsOf x zs r).st.claimsByBucket = r.st.claimsByBucket ∧
    (delPairsOf x zs r).st.unplaced = r.st.unplaced ∧
    (delPairsOf x zs r).tested = r.tested ∧ (delPairsOf x zs r).tests = r.tests
  | [], _ => ⟨rfl, rfl, rfl, rfl⟩
  | z :: zs, r => by
    simp only [delPairsOf]
    split
    · exact delPairsOf_same x zs r
    · exact delPairsOf_same x zs _

theorem delPairsOf_index (x : Claim) : ∀ (zs : List Claim) (r : Run),
    IndexOK r.st → IndexOK (delPairsOf x zs r).st
  | [], _, h => h
  | z :: zs, r, h => by
    simp only [delPairsOf]
    split
    · exact delPairsOf_index x zs r h
    · exact delPairsOf_index x zs _ (delPair_index r.st _ h)

theorem delPairsOf_deletes (x : Claim) : ∀ (zs : List Claim) (r : Run),
    (delPairsOf x zs r).deletes ≤ r.deletes + zs.length
  | [], _ => Nat.le_refl _
  | z :: zs, r => by
    simp only [delPairsOf, List.length_cons]
    split
    · have := delPairsOf_deletes x zs r
      omega
    · have := delPairsOf_deletes x zs ⟨delPair r.st (keyOf x z), r.tested, r.tests, r.deletes + 1⟩
      simp only at this
      omega

theorem delPairsOf_out (x : Claim) (k : Key) (row : Row) : ∀ (zs : List Claim) (r : Run),
    (delPairsOf x zs r).st.pairOut k = some row ↔
      r.st.pairOut k = some row ∧ ∀ z ∈ zs, z.id ≠ x.id → keyOf x z ≠ k
  | [], r => by simp [delPairsOf]
  | z :: zs, r => by
    simp only [delPairsOf]
    rw [delPairsOf_out x k row zs]
    split
    · rename_i h
      constructor
      · rintro ⟨h1, h2⟩
        refine ⟨h1, fun z' hz' => ?_⟩
        rcases List.mem_cons.1 hz' with e | hz'
        · rw [e]
          intro ne
          exact absurd h ne
        · exact h2 z' hz'
      · rintro ⟨h1, h2⟩
        exact ⟨h1, fun z' hz' => h2 z' (List.mem_cons_of_mem _ hz')⟩
    · rename_i h
      show (if k = keyOf x z then none else r.st.pairOut k) = some row ∧ _ ↔ _
      constructor
      · rintro ⟨h1, h2⟩
        by_cases e : k = keyOf x z
        · rw [ite_eq_left e] at h1
          cases h1
        · rw [ite_eq_right e] at h1
          refine ⟨h1, fun z' hz' => ?_⟩
          rcases List.mem_cons.1 hz' with e' | hz'
          · rw [e']
            intro _ e''
            exact e e''.symm
          · exact h2 z' hz'
      · rintro ⟨h1, h2⟩
        have e : ¬ k = keyOf x z := fun e => h2 z (List.mem_cons_self ..) h e.symm
        rw [ite_eq_right e]
        exact ⟨h1, fun z' hz' => h2 z' (List.mem_cons_of_mem _ hz')⟩

theorem removeOne_counts (r : Run) (x : Claim) :
    (removeOne r x).tested = r.tested ∧ (removeOne r x).tests = r.tests := by
  cases hb : x.bucket with
  | none =>
    simp only [removeOne, hb]
    exact ⟨rfl, rfl⟩
  | some b =>
    simp only [removeOne, hb]
    have := delPairsOf_same x (r.st.claimsByBucket b) r
    exact ⟨this.2.2.1, this.2.2.2⟩

-- Invariant of phase 1 after removing the prefix Rd.
structure P1Inv (S0 : PState) (c0 Rd : List Claim) (st : PState) : Prop where
  claims : ∀ b z, z ∈ st.claimsByBucket b ↔ z ∈ members c0 b ∧ z ∉ Rd
  unplaced : ∀ i, i ∈ st.unplaced ↔ i ∈ S0.unplaced ∧ i ∉ unknownIds Rd
  stored : ∀ k row, st.pairOut k = some row →
    S0.pairOut k = some row ∧ ∃ b, row.l ∈ st.claimsByBucket b ∧ row.r ∈ st.claimsByBucket b
  kept : ∀ k row, S0.pairOut k = some row →
    (∀ x ∈ Rd, row.l.id ≠ x.id ∧ row.r.id ≠ x.id) → st.pairOut k = some row
  index : IndexOK st

theorem p1_init (rule : Rule) (A0 : Answers) (S0 : PState) (c0 : List Claim)
    (exact : PExact rule S0 c0 A0) : P1Inv S0 c0 [] S0 := by
  refine ⟨fun b z => ?_, fun i => ?_, fun k row h => ?_, fun _ _ h _ => h, exact.index⟩
  · rw [exact.claims b z]
    exact ⟨fun h => ⟨h, List.not_mem_nil⟩, fun h => h.1⟩
  · rw [exact.unplaced i]
    have : i ∉ unknownIds ([] : List Claim) := by simp [unknownIds]
    exact ⟨fun h => ⟨h, this⟩, fun h => h.1⟩
  · obtain ⟨⟨b, hl, hr⟩, _⟩ := exact.stored k row h
    exact ⟨h, b, (exact.claims b _).2 hl, (exact.claims b _).2 hr⟩

theorem removeOne_inv (rule : Rule) (A0 : Answers) (S0 : PState) (c0 Rd : List Claim)
    (x : Claim) (r : Run) (exact : PExact rule S0 c0 A0) (inv : P1Inv S0 c0 Rd r.st) :
    P1Inv S0 c0 (Rd ++ [x]) (removeOne r x).st := by
  cases hb : x.bucket with
  | none =>
    simp only [removeOne, hb]
    refine ⟨fun b z => ?_, fun i => ?_, inv.stored, fun k row h0 hx => ?_, inv.index⟩
    · show z ∈ r.st.claimsByBucket b ↔ _
      rw [inv.claims b z]
      constructor
      · rintro ⟨hm, hn⟩
        refine ⟨hm, fun h => ?_⟩
        rcases List.mem_append.1 h with h | h
        · exact hn h
        · rw [List.mem_singleton] at h
          have hz := ((mem_members c0 b z).1 hm).2
          rw [h, hb] at hz
          cases hz
      · rintro ⟨hm, hn⟩
        exact ⟨hm, fun h => hn (List.mem_append_left _ h)⟩
    · show i ∈ r.st.unplaced.filter (fun i => i ≠ x.id) ↔ _
      rw [List.mem_filter, decide_eq_true_eq, inv.unplaced, unknownIds_snoc_none Rd x hb]
      constructor
      · rintro ⟨⟨h1, h2⟩, h3⟩
        exact ⟨h1, fun h => h.elim h2 h3⟩
      · rintro ⟨h1, h2⟩
        exact ⟨⟨h1, fun h => h2 (Or.inl h)⟩, fun h => h2 (Or.inr h)⟩
    · exact inv.kept k row h0 (fun x' hx' => hx x' (List.mem_append_left _ hx'))
  | some b =>
    simp only [removeOne, hb]
    have same := delPairsOf_same x (r.st.claimsByBucket b) r
    refine ⟨fun b' z => ?_, fun i => ?_, fun k row h => ?_, fun k row h0 hx => ?_, ?_⟩
    · show z ∈ (if b' = b then (r.st.claimsByBucket b).filter (fun z => z ≠ x)
          else (delPairsOf x (r.st.claimsByBucket b) r).st.claimsByBucket b') ↔ _
      by_cases e : b' = b
      · rw [ite_eq_left e, List.mem_filter, decide_eq_true_eq, ← e, inv.claims b' z]
        constructor
        · rintro ⟨⟨hm, hn⟩, hx⟩
          refine ⟨hm, fun h => ?_⟩
          rcases List.mem_append.1 h with h | h
          · exact hn h
          · exact hx (List.mem_singleton.1 h)
        · rintro ⟨hm, hn⟩
          exact ⟨⟨hm, fun h => hn (List.mem_append_left _ h)⟩,
            fun h => hn (List.mem_append_right _ (List.mem_singleton.2 h))⟩
      · rw [ite_eq_right e, same.1, inv.claims b' z]
        constructor
        · rintro ⟨hm, hn⟩
          refine ⟨hm, fun h => ?_⟩
          rcases List.mem_append.1 h with h | h
          · exact hn h
          · rw [List.mem_singleton] at h
            have hz := ((mem_members c0 b' z).1 hm).2
            rw [h, hb] at hz
            exact e (Option.some.inj hz).symm
        · rintro ⟨hm, hn⟩
          exact ⟨hm, fun h => hn (List.mem_append_left _ h)⟩
    · show i ∈ (delPairsOf x (r.st.claimsByBucket b) r).st.unplaced ↔ _
      rw [same.2.1, inv.unplaced, unknownIds_snoc_some Rd x b hb]
    · have h' : (delPairsOf x (r.st.claimsByBucket b) r).st.pairOut k = some row := h
      obtain ⟨h1, h2⟩ := (delPairsOf_out x k row _ r).1 h'
      obtain ⟨h0, b'', hl, hr⟩ := inv.stored k row h1
      obtain ⟨_, lt, ek, _⟩ := exact.stored k row h0
      refine ⟨h0, b'', ?_⟩
      show row.l ∈ (if b'' = b then (r.st.claimsByBucket b).filter (fun z => z ≠ x)
          else (delPairsOf x (r.st.claimsByBucket b) r).st.claimsByBucket b'') ∧
        row.r ∈ (if b'' = b then (r.st.claimsByBucket b).filter (fun z => z ≠ x)
          else (delPairsOf x (r.st.claimsByBucket b) r).st.claimsByBucket b'')
      by_cases e : b'' = b
      · rw [ite_eq_left e]
        rw [e] at hl hr
        rw [List.mem_filter, List.mem_filter, decide_eq_true_eq, decide_eq_true_eq]
        refine ⟨⟨hl, fun lx => ?_⟩, ⟨hr, fun rx => ?_⟩⟩
        · have ne : row.r.id ≠ x.id := by rw [← lx]; omega
          have kx : keyOf x row.r = k := by
            rw [ek, keyOf_lt x row.r (by rw [← lx]; exact lt), lx]
          exact h2 row.r hr ne kx
        · have ne : row.l.id ≠ x.id := by rw [← rx]; omega
          have kx : keyOf x row.l = k := by
            rw [ek, keyOf_gt x row.l (by rw [← rx]; exact lt), rx]
          exact h2 row.l hl ne kx
      · rw [ite_eq_right e, same.1]
        exact ⟨hl, hr⟩
    · have old := inv.kept k row h0 (fun x' hx' => hx x' (List.mem_append_left _ hx'))
      obtain ⟨_, _, ek, _⟩ := exact.stored k row h0
      show (delPairsOf x (r.st.claimsByBucket b) r).st.pairOut k = some row
      refine (delPairsOf_out x k row _ r).2 ⟨old, fun z _ _ kz => ?_⟩
      have hxx := hx x (List.mem_append_right _ (List.mem_singleton.2 rfl))
      rw [ek] at kz
      rcases keyOf_has x z with h | h
      · rw [kz] at h
        exact hxx.1 h
      · rw [kz] at h
        exact hxx.2 h
    · exact delPairsOf_index x _ r inv.index

theorem phase1_inv (rule : Rule) (A0 : Answers) (S0 : PState) (c0 : List Claim)
    (exact : PExact rule S0 c0 A0) : ∀ (R Rd : List Claim) (r : Run),
    P1Inv S0 c0 Rd r.st → P1Inv S0 c0 (Rd ++ R) (R.foldl removeOne r).st
  | [], Rd, r, h => by
    simp only [List.append_nil, List.foldl_nil]
    exact h
  | x :: R, Rd, r, h => by
    rw [List.foldl_cons]
    have := phase1_inv rule A0 S0 c0 exact R (Rd ++ [x]) (removeOne r x)
      (removeOne_inv rule A0 S0 c0 Rd x r exact h)
    rw [List.append_assoc, List.singleton_append] at this
    exact this

theorem phase1_counts : ∀ (R : List Claim) (r : Run),
    (R.foldl removeOne r).tested = r.tested ∧ (R.foldl removeOne r).tests = r.tests
  | [], _ => ⟨rfl, rfl⟩
  | x :: R, r => by
    rw [List.foldl_cons]
    have h1 := phase1_counts R (removeOne r x)
    have h2 := removeOne_counts r x
    exact ⟨h1.1.trans h2.1, h1.2.trans h2.2⟩

/- Phase 2 lemmas. -/

theorem testPairsOf_same (rule : Rule) (A1 : Answers) (y : Claim) : ∀ (zs : List Claim) (r : Run),
    (testPairsOf rule A1 y zs r).st.claimsByBucket = r.st.claimsByBucket ∧
    (testPairsOf rule A1 y zs r).st.unplaced = r.st.unplaced ∧
    (testPairsOf rule A1 y zs r).deletes = r.deletes
  | [], _ => ⟨rfl, rfl, rfl⟩
  | z :: zs, r => by
    simp only [testPairsOf]
    split
    · exact testPairsOf_same rule A1 y zs r
    · exact testPairsOf_same rule A1 y zs _

theorem testPairsOf_index (rule : Rule) (A1 : Answers) (y : Claim) : ∀ (zs : List Claim) (r : Run),
    IndexOK r.st → IndexOK (testPairsOf rule A1 y zs r).st
  | [], _, h => h
  | z :: zs, r, h => by
    simp only [testPairsOf]
    split
    · exact testPairsOf_index rule A1 y zs r h
    · exact testPairsOf_index rule A1 y zs _ (putPair_index r.st _ _ h)

theorem testPairsOf_tests (rule : Rule) (A1 : Answers) (y : Claim) : ∀ (zs : List Claim) (r : Run),
    (testPairsOf rule A1 y zs r).tests ≤ r.tests + zs.length
  | [], _ => Nat.le_refl _
  | z :: zs, r => by
    simp only [testPairsOf, List.length_cons]
    split
    · have := testPairsOf_tests rule A1 y zs r
      omega
    · have := testPairsOf_tests rule A1 y zs
        ⟨putPair r.st (keyOf y z) (rowOf rule A1 (orient y z).1 (orient y z).2),
          keyOf y z :: r.tested, r.tests + 1, r.deletes⟩
      simp only at this
      omega

theorem testPairsOf_old (rule : Rule) (A1 : Answers) (y : Claim) (k : Key) :
    ∀ (zs : List Claim) (r : Run), (∀ z ∈ zs, z.id ≠ y.id → keyOf y z ≠ k) →
      (testPairsOf rule A1 y zs r).st.pairOut k = r.st.pairOut k
  | [], _, _ => rfl
  | z :: zs, r, h => by
    simp only [testPairsOf]
    rw [testPairsOf_old rule A1 y k zs _ (fun z' hz' => h z' (List.mem_cons_of_mem _ hz'))]
    split
    · rfl
    · rename_i ne
      show (if k = keyOf y z then _ else r.st.pairOut k) = r.st.pairOut k
      rw [ite_eq_right (fun e => h z (List.mem_cons_self ..) ne e.symm)]

theorem testPairsOf_any (rule : Rule) (A1 : Answers) (y : Claim) (k : Key) (row : Row) :
    ∀ (zs : List Claim) (r : Run), (testPairsOf rule A1 y zs r).st.pairOut k = some row →
      r.st.pairOut k = some row ∨ ∃ z ∈ zs, z.id ≠ y.id ∧ keyOf y z = k ∧
        row = rowOf rule A1 (orient y z).1 (orient y z).2
  | [], _, h => Or.inl h
  | z :: zs, r, h => by
    simp only [testPairsOf] at h
    rcases testPairsOf_any rule A1 y k row zs _ h with h' | ⟨z', hz', ne, ek, er⟩
    · split at h'
      · exact Or.inl h'
      · rename_i ne
        change (if k = keyOf y z then some _ else r.st.pairOut k) = some row at h'
        by_cases e : k = keyOf y z
        · rw [ite_eq_left e] at h'
          exact Or.inr ⟨z, List.mem_cons_self .., ne, e.symm, (Option.some.inj h').symm⟩
        · rw [ite_eq_right e] at h'
          exact Or.inl h'
    · exact Or.inr ⟨z', List.mem_cons_of_mem _ hz', ne, ek, er⟩

theorem testPairsOf_written (rule : Rule) (A1 : Answers) (y : Claim) (k : Key) :
    ∀ (zs : List Claim) (r : Run), (∃ z ∈ zs, z.id ≠ y.id ∧ keyOf y z = k) →
      ∃ z ∈ zs, z.id ≠ y.id ∧ keyOf y z = k ∧
        (testPairsOf rule A1 y zs r).st.pairOut k =
          some (rowOf rule A1 (orient y z).1 (orient y z).2)
  | [], _, h => by
    obtain ⟨_, hz, _⟩ := h
    exact absurd hz List.not_mem_nil
  | z :: zs, r, h => by
    simp only [testPairsOf]
    by_cases t : ∃ z' ∈ zs, z'.id ≠ y.id ∧ keyOf y z' = k
    · obtain ⟨z', hz', ne, ek, eo⟩ := testPairsOf_written rule A1 y k zs _ t
      exact ⟨z', List.mem_cons_of_mem _ hz', ne, ek, eo⟩
    · obtain ⟨z', hz', ne, ek⟩ := h
      rcases List.mem_cons.1 hz' with e | hz'
      · rw [e] at ne ek
        refine ⟨z, List.mem_cons_self .., ne, ek, ?_⟩
        rw [testPairsOf_old rule A1 y k zs _ (fun z'' hz'' ne'' ek'' => t ⟨z'', hz'', ne'', ek''⟩)]
        rw [ite_eq_right ne]
        show (if k = keyOf y z then _ else _) = _
        rw [ite_eq_left ek.symm]
      · exact absurd ⟨z', hz', ne, ek⟩ t

theorem testPairsOf_tested (rule : Rule) (A1 : Answers) (y : Claim) (k : Key) :
    ∀ (zs : List Claim) (r : Run), k ∈ (testPairsOf rule A1 y zs r).tested →
      k ∈ r.tested ∨ ∃ z ∈ zs, z.id ≠ y.id ∧ keyOf y z = k
  | [], _, h => Or.inl h
  | z :: zs, r, h => by
    simp only [testPairsOf] at h
    rcases testPairsOf_tested rule A1 y k zs _ h with h' | ⟨z', hz', ne, ek⟩
    · split at h'
      · exact Or.inl h'
      · rename_i ne
        change k ∈ keyOf y z :: r.tested at h'
        rcases List.mem_cons.1 h' with e | h'
        · exact Or.inr ⟨z, List.mem_cons_self .., ne, e.symm⟩
        · exact Or.inl h'
    · exact Or.inr ⟨z', List.mem_cons_of_mem _ hz', ne, ek⟩

-- Invariant of phase 2 after adding the prefix Dn.
structure P2Inv (rule : Rule) (A0 A1 : Answers) (S0 : PState) (c0 R c1 Dn : List Claim)
    (r : Run) : Prop where
  claims : ∀ b z, z ∈ r.st.claimsByBucket b ↔ (z ∈ members c0 b ∧ z ∉ R) ∨ z ∈ members Dn b
  unplaced : ∀ i, i ∈ r.st.unplaced ↔ (i ∈ S0.unplaced ∧ i ∉ unknownIds R) ∨ i ∈ unknownIds Dn
  stored : ∀ k row, r.st.pairOut k = some row →
    (S0.pairOut k = some row ∧ row.l ∈ c1 ∧ row.r ∈ c1) ∨ Good rule c1 A1 k row
  tested : ∀ k ∈ r.tested, ∃ row, r.st.pairOut k = some row ∧ Good rule c1 A1 k row
  pairs : ∀ b l r', l ∈ r.st.claimsByBucket b → r' ∈ r.st.claimsByBucket b → l.id < r'.id →
    r.st.pairOut (l.id, r'.id) = some (rowOf rule A1 l r') ∨
    (l ∉ Dn ∧ r' ∉ Dn ∧ S0.pairOut (l.id, r'.id) = some (rowOf rule A0 l r') ∧
      r.st.pairOut (l.id, r'.id) = some (rowOf rule A0 l r'))
  index : IndexOK r.st

theorem claims_mem (c0 R c1 Dn : List Claim) (cbb : Nat → List Claim)
    (claims : ∀ b z, z ∈ cbb b ↔ (z ∈ members c0 b ∧ z ∉ R) ∨ z ∈ members Dn b)
    (kept1 : ∀ z ∈ c0, z ∉ R → z ∈ c1) (dn1 : ∀ z ∈ Dn, z ∈ c1) :
    ∀ b z, z ∈ cbb b → z ∈ c1 ∧ z.bucket = some b := by
  intro b z h
  rcases (claims b z).1 h with ⟨hm, hn⟩ | hm
  · have := (mem_members c0 b z).1 hm
    exact ⟨kept1 z this.1 hn, this.2⟩
  · have := (mem_members Dn b z).1 hm
    exact ⟨dn1 z this.1, this.2⟩

theorem p2_init (rule : Rule) (A0 A1 : Answers) (S0 : PState) (c0 R c1 : List Claim) (r : Run)
    (exact : PExact rule S0 c0 A0) (ids : RemovedIds c0 R) (kept1 : ∀ z ∈ c0, z ∉ R → z ∈ c1)
    (p1 : P1Inv S0 c0 R r.st) (none_tested : r.tested = []) :
    P2Inv rule A0 A1 S0 c0 R c1 [] r := by
  refine ⟨fun b z => ?_, fun i => ?_, fun k row h => ?_, fun k hk => ?_,
    fun b l r' hl hr lt => ?_, p1.index⟩
  · rw [p1.claims b z]
    have : z ∉ members ([] : List Claim) b := by simp [members]
    exact ⟨Or.inl, fun h => h.elim id (fun h' => absurd h' this)⟩
  · rw [p1.unplaced i]
    have : i ∉ unknownIds ([] : List Claim) := by simp [unknownIds]
    exact ⟨Or.inl, fun h => h.elim id (fun h' => absurd h' this)⟩
  · obtain ⟨h0, b, hl, hr⟩ := p1.stored k row h
    obtain ⟨hl0, hlR⟩ := (p1.claims b _).1 hl
    obtain ⟨hr0, hrR⟩ := (p1.claims b _).1 hr
    exact Or.inl ⟨h0, kept1 _ ((mem_members c0 b _).1 hl0).1 hlR,
      kept1 _ ((mem_members c0 b _).1 hr0).1 hrR⟩
  · rw [none_tested] at hk
    exact absurd hk List.not_mem_nil
  · obtain ⟨hl0, hlR⟩ := (p1.claims b l).1 hl
    obtain ⟨hr0, hrR⟩ := (p1.claims b r').1 hr
    have h0 := exact.every b l r' hl0 hr0 lt
    have h1 := p1.kept _ _ h0 (fun x hx =>
      ⟨fun e => hlR (ids x hx l ((mem_members c0 b l).1 hl0).1 e),
        fun e => hrR (ids x hx r' ((mem_members c0 b r').1 hr0).1 e)⟩)
    exact Or.inr ⟨List.not_mem_nil, List.not_mem_nil, h0, h1⟩

theorem addOne_inv (rule : Rule) (A0 A1 : Answers) (S0 : PState) (c0 R c1 Dn : List Claim)
    (y : Claim) (r : Run) (keys : Keys c1) (kept1 : ∀ z ∈ c0, z ∉ R → z ∈ c1)
    (dn1 : ∀ z ∈ Dn, z ∈ c1) (y1 : y ∈ c1) (inv : P2Inv rule A0 A1 S0 c0 R c1 Dn r) :
    P2Inv rule A0 A1 S0 c0 R c1 (Dn ++ [y]) (addOne rule A1 r y) := by
  have mem := claims_mem c0 R c1 Dn _ inv.claims kept1 dn1
  cases hb : y.bucket with
  | none =>
    simp only [addOne, hb]
    refine ⟨fun b z => ?_, fun i => ?_, inv.stored, inv.tested, fun b l r' hl hr lt => ?_,
      inv.index⟩
    · show z ∈ r.st.claimsByBucket b ↔ _
      rw [inv.claims b z, mem_members_snoc_none Dn y hb b z]
    · show i ∈ r.st.unplaced ++ [y.id] ↔ _
      rw [List.mem_append, List.mem_singleton, inv.unplaced i, unknownIds_snoc_none Dn y hb i]
      exact or_assoc
    · rcases inv.pairs b l r' hl hr lt with h | ⟨h1, h2, h3, h4⟩
      · exact Or.inl h
      · have notin : ∀ z, z ∈ r.st.claimsByBucket b → z ∉ Dn → z ∉ Dn ++ [y] := by
          intro z hz hn h
          rcases List.mem_append.1 h with h | h
          · exact hn h
          · rw [List.mem_singleton] at h
            have := (mem b z hz).2
            rw [h, hb] at this
            cases this
        exact Or.inr ⟨notin l hl h1, notin r' hr h2, h3, h4⟩
  | some b =>
    simp only [addOne, hb]
    have same := testPairsOf_same rule A1 y (r.st.claimsByBucket b) r
    have y1b : y ∈ members c1 b := (mem_members c1 b y).2 ⟨y1, hb⟩
    -- The new claims of every bucket.
    have claims' : ∀ b' z, z ∈ (if b' = b then r.st.claimsByBucket b ++ [y]
        else (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.claimsByBucket b') ↔
        (z ∈ members c0 b' ∧ z ∉ R) ∨ z ∈ members (Dn ++ [y]) b' := by
      intro b' z
      by_cases e : b' = b
      · rw [ite_eq_left e, List.mem_append, List.mem_singleton, ← e, inv.claims b' z,
          mem_members_snoc Dn y b b' hb z]
        constructor
        · rintro ((h | h) | h)
          · exact Or.inl h
          · exact Or.inr (Or.inl h)
          · exact Or.inr (Or.inr ⟨e, h⟩)
        · rintro (h | h | ⟨_, h⟩)
          · exact Or.inl (Or.inl h)
          · exact Or.inl (Or.inr h)
          · exact Or.inr h
      · rw [ite_eq_right e, same.1, inv.claims b' z, mem_members_snoc Dn y b b' hb z]
        constructor
        · rintro (h | h)
          · exact Or.inl h
          · exact Or.inr (Or.inl h)
        · rintro (h | h | ⟨e', _⟩)
          · exact Or.inl h
          · exact Or.inr h
          · exact absurd e' e
    have mem' := claims_mem c0 R c1 (Dn ++ [y]) _ claims' kept1 (fun z hz => by
      rcases List.mem_append.1 hz with h | h
      · exact dn1 z h
      · rw [List.mem_singleton.1 h]
        exact y1)
    refine ⟨claims', fun i => ?_, fun k row h => ?_, fun k hk => ?_,
      fun b'' l r' hl hr lt => ?_, ?_⟩
    · show i ∈ (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.unplaced ↔ _
      rw [same.2.1, inv.unplaced i, unknownIds_snoc_some Dn y b hb i]
    · have h' : (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.pairOut k = some row := h
      rcases testPairsOf_any rule A1 y k row _ r h' with h0 | ⟨z, hz, ne, ek, er⟩
      · exact inv.stored k row h0
      · right
        rw [er, ← ek]
        have hz1 := mem b z hz
        exact new_good rule c1 A1 y z b y1b ((mem_members c1 b z).2 hz1) ne
    · have hk' : k ∈ (testPairsOf rule A1 y (r.st.claimsByBucket b) r).tested := hk
      have written : (∃ z ∈ r.st.claimsByBucket b, z.id ≠ y.id ∧ keyOf y z = k) →
          ∃ row, (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.pairOut k = some row ∧
            Good rule c1 A1 k row := by
        intro w
        obtain ⟨z, hz, ne, ek, eo⟩ := testPairsOf_written rule A1 y k _ r w
        refine ⟨_, eo, ?_⟩
        rw [← ek]
        exact new_good rule c1 A1 y z b y1b ((mem_members c1 b z).2 (mem b z hz)) ne
      rcases testPairsOf_tested rule A1 y k _ r hk' with old | w
      · by_cases w : ∃ z ∈ r.st.claimsByBucket b, z.id ≠ y.id ∧ keyOf y z = k
        · exact written w
        · obtain ⟨row, e, g⟩ := inv.tested k old
          refine ⟨row, ?_, g⟩
          show (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.pairOut k = some row
          rw [testPairsOf_old rule A1 y k _ r (fun z hz ne ek => w ⟨z, hz, ne, ek⟩)]
          exact e
      · exact written w
    · have hl1 := mem' b'' l hl
      have hr1 := mem' b'' r' hr
      show (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.pairOut (l.id, r'.id) =
          some (rowOf rule A1 l r') ∨
        (l ∉ Dn ++ [y] ∧ r' ∉ Dn ++ [y] ∧ S0.pairOut (l.id, r'.id) = some (rowOf rule A0 l r') ∧
          (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.pairOut (l.id, r'.id) =
            some (rowOf rule A0 l r'))
      by_cases w : ∃ z ∈ r.st.claimsByBucket b, z.id ≠ y.id ∧ keyOf y z = (l.id, r'.id)
      · obtain ⟨z, hz, ne, ek, eo⟩ := testPairsOf_written rule A1 y _ _ r w
        obtain ⟨o1, o2⟩ := orient_eq c1 keys y z l r' y1 (mem b z hz).1 hl1.1 hr1.1 ek
        left
        rw [eo, o1, o2]
      · have keep := testPairsOf_old rule A1 y (l.id, r'.id) _ r
          (fun z hz ne ek => w ⟨z, hz, ne, ek⟩)
        -- Neither claim is y, so both were already in the bucket.
        have lny : l ≠ y := by
          intro ly
          have eb : b'' = b := by
            have := hl1.2
            rw [ly, hb] at this
            exact (Option.some.inj this).symm
          rw [eb] at hr
          have hr' : r' ∈ r.st.claimsByBucket b ++ [y] := by
            have h' : r' ∈ (if b = b then r.st.claimsByBucket b ++ [y]
              else (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.claimsByBucket b) := hr
            rwa [ite_eq_left rfl] at h'
          rcases List.mem_append.1 hr' with hr' | hr'
          · apply w
            refine ⟨r', hr', ?_, ?_⟩
            · rw [← ly]
              omega
            · rw [← ly]
              exact keyOf_lt l r' lt
          · rw [List.mem_singleton] at hr'
            rw [ly, hr'] at lt
            exact Nat.lt_irrefl _ lt
        have rny : r' ≠ y := by
          intro ry
          have eb : b'' = b := by
            have := hr1.2
            rw [ry, hb] at this
            exact (Option.some.inj this).symm
          rw [eb] at hl
          have hl' : l ∈ r.st.claimsByBucket b ++ [y] := by
            have h' : l ∈ (if b = b then r.st.claimsByBucket b ++ [y]
              else (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.claimsByBucket b) := hl
            rwa [ite_eq_left rfl] at h'
          rcases List.mem_append.1 hl' with hl' | hl'
          · apply w
            refine ⟨l, hl', ?_, ?_⟩
            · rw [← ry]
              omega
            · rw [← ry]
              exact keyOf_gt r' l lt
          · exact lny (List.mem_singleton.1 hl')
        have old : ∀ z, z ∈ (if b'' = b then r.st.claimsByBucket b ++ [y]
            else (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.claimsByBucket b'') →
            z ≠ y → z ∈ r.st.claimsByBucket b'' := by
          intro z hz zy
          by_cases e : b'' = b
          · rw [ite_eq_left e] at hz
            rcases List.mem_append.1 hz with hz | hz
            · rw [e]
              exact hz
            · exact absurd (List.mem_singleton.1 hz) zy
          · rw [ite_eq_right e, same.1] at hz
            exact hz
        have notin : ∀ z, z ∉ Dn → z ≠ y → z ∉ Dn ++ [y] := by
          intro z hn zy h
          rcases List.mem_append.1 h with h | h
          · exact hn h
          · exact zy (List.mem_singleton.1 h)
        rw [keep]
        rcases inv.pairs b'' l r' (old l hl lny) (old r' hr rny) lt with h | ⟨h1, h2, h3, h4⟩
        · exact Or.inl h
        · exact Or.inr ⟨notin l h1 lny, notin r' h2 rny, h3, h4⟩
    · exact testPairsOf_index rule A1 y _ r inv.index

theorem phase2_inv (rule : Rule) (A0 A1 : Answers) (S0 : PState) (c0 R c1 : List Claim)
    (keys : Keys c1) (kept1 : ∀ z ∈ c0, z ∉ R → z ∈ c1) :
    ∀ (L Dn : List Claim) (r : Run), (∀ z ∈ Dn ++ L, z ∈ c1) →
      P2Inv rule A0 A1 S0 c0 R c1 Dn r →
      P2Inv rule A0 A1 S0 c0 R c1 (Dn ++ L) (L.foldl (addOne rule A1) r)
  | [], Dn, r, _, h => by
    simp only [List.append_nil, List.foldl_nil]
    exact h
  | y :: L, Dn, r, sub, h => by
    rw [List.foldl_cons]
    have step := addOne_inv rule A0 A1 S0 c0 R c1 Dn y r keys kept1
      (fun z hz => sub z (List.mem_append_left _ hz))
      (sub y (List.mem_append_right _ (List.mem_cons_self ..))) h
    have sub' : ∀ z ∈ Dn ++ [y] ++ L, z ∈ c1 := by
      rw [List.append_assoc, List.singleton_append]
      exact sub
    have := phase2_inv rule A0 A1 S0 c0 R c1 keys kept1 L (Dn ++ [y]) _ sub' step
    rw [List.append_assoc, List.singleton_append] at this
    exact this

/- Phase 3 lemmas. -/

theorem retestOne_same (rule : Rule) (A1 : Answers) (r : Run) (k : Key) :
    (retestOne rule A1 r k).st.claimsByBucket = r.st.claimsByBucket ∧
    (retestOne rule A1 r k).st.unplaced = r.st.unplaced ∧
    (retestOne rule A1 r k).tested = r.tested ∧ (retestOne rule A1 r k).deletes = r.deletes ∧
    (retestOne rule A1 r k).tests ≤ r.tests + 1 ∧
    (IndexOK r.st → IndexOK (retestOne rule A1 r k).st) := by
  unfold retestOne
  split
  · exact ⟨rfl, rfl, rfl, rfl, Nat.le_succ _, id⟩
  · cases r.st.pairOut k with
    | none => exact ⟨rfl, rfl, rfl, rfl, Nat.le_succ _, id⟩
    | some row => exact ⟨rfl, rfl, rfl, rfl, Nat.le_refl _, putPair_index r.st k _⟩

theorem retestKeys_same (rule : Rule) (A1 : Answers) : ∀ (ks : List Key) (r : Run),
    (retestKeys rule A1 ks r).st.claimsByBucket = r.st.claimsByBucket ∧
    (retestKeys rule A1 ks r).st.unplaced = r.st.unplaced ∧
    (retestKeys rule A1 ks r).tested = r.tested ∧ (retestKeys rule A1 ks r).deletes = r.deletes ∧
    (retestKeys rule A1 ks r).tests ≤ r.tests + ks.length ∧
    (IndexOK r.st → IndexOK (retestKeys rule A1 ks r).st)
  | [], _ => ⟨rfl, rfl, rfl, rfl, Nat.le_refl _, id⟩
  | k :: ks, r => by
    have h1 := retestOne_same rule A1 r k
    have h2 := retestKeys_same rule A1 ks (retestOne rule A1 r k)
    refine ⟨h2.1.trans h1.1, h2.2.1.trans h1.2.1, h2.2.2.1.trans h1.2.2.1,
      h2.2.2.2.1.trans h1.2.2.2.1, ?_, fun h => h2.2.2.2.2.2 (h1.2.2.2.2.2 h)⟩
    have := h2.2.2.2.2.1
    have := h1.2.2.2.2.1
    show (retestKeys rule A1 ks (retestOne rule A1 r k)).tests ≤ r.tests + (ks.length + 1)
    omega

-- Phase 3 re-tests exactly the visited keys that phase 2 did not test.
theorem retestKeys_out (rule : Rule) (A1 : Answers) (k : Key) : ∀ (ks : List Key) (r : Run),
    (retestKeys rule A1 ks r).st.pairOut k =
      (r.st.pairOut k).map (fun row => if k ∈ ks ∧ k ∉ r.tested then retest rule A1 row else row)
  | [], r => by
    cases h : r.st.pairOut k <;> simp [retestKeys, h]
  | k0 :: ks, r => by
    show (retestKeys rule A1 ks (retestOne rule A1 r k0)).st.pairOut k = _
    rw [retestKeys_out rule A1 k ks, (retestOne_same rule A1 r k0).2.2.1]
    unfold retestOne
    by_cases t : k0 ∈ r.tested
    · rw [ite_eq_left t]
      by_cases e : k = k0
      · subst e
        cases r.st.pairOut k <;> simp [t]
      · cases r.st.pairOut k <;> simp [e]
    · rw [ite_eq_right t]
      cases h0 : r.st.pairOut k0 with
      | none =>
        by_cases e : k = k0
        · subst e
          rw [h0]
          rfl
        · cases r.st.pairOut k <;> simp [e]
      | some row0 =>
        show ((putPair r.st k0 (retest rule A1 row0)).pairOut k).map _ = _
        by_cases e : k = k0
        · subst e
          simp [putPair, h0, t, retest, rowOf]
        · simp [putPair, e]

theorem stable_row (rule : Rule) (reads : ReadsDeclared rule) (S0 : PState) (A0 A1 : Answers)
    (D : List Request) (answers : AnswerChange A0 A1 D) (idx : IndexOK S0) (k : Key)
    (l r : Claim) (h : S0.pairOut k = some (rowOf rule A0 l r))
    (notV : k ∉ D.flatMap S0.pairIndex) : rowOf rule A0 l r = rowOf rule A1 l r := by
  have agree : ∀ q ∈ rule.req l r, A0 q = A1 q := by
    intro q hq
    apply answers
    intro hD
    exact notV (List.mem_flatMap.2 ⟨q, hD, (idx k q).2 ⟨_, h, hq⟩⟩)
  unfold rowOf
  rw [reads l r A0 A1 agree]

theorem claims_c1 (c0 R Ad c1 : List Claim) (change : ClaimChange c0 R Ad c1) (b : Nat)
    (z : Claim) : ((z ∈ members c0 b ∧ z ∉ R) ∨ z ∈ members Ad b) ↔ z ∈ members c1 b := by
  rw [mem_members, mem_members, mem_members, change z]
  constructor
  · rintro (⟨⟨h0, hb⟩, hR⟩ | ⟨hA, hb⟩)
    · exact ⟨Or.inl ⟨h0, hR⟩, hb⟩
    · exact ⟨Or.inr hA, hb⟩
  · rintro ⟨⟨h0, hR⟩ | hA, hb⟩
    · exact Or.inl ⟨⟨h0, hb⟩, hR⟩
    · exact Or.inr ⟨hA, hb⟩

theorem phase3_exact (rule : Rule) (reads : ReadsDeclared rule) (S0 : PState)
    (c0 R Ad c1 : List Claim) (A0 A1 : Answers) (D : List Request)
    (exact : PExact rule S0 c0 A0) (change : ClaimChange c0 R Ad c1)
    (answers : AnswerChange A0 A1 D) (ids : RemovedIds c0 R) (r : Run)
    (inv : P2Inv rule A0 A1 S0 c0 R c1 Ad r) :
    PExact rule (retestKeys rule A1 (D.flatMap S0.pairIndex) r).st c1 A1 := by
  have same := retestKeys_same rule A1 (D.flatMap S0.pairIndex) r
  have out := retestKeys_out rule A1
  refine ⟨fun b z => ?_, fun i => ?_, fun k row' h => ?_, fun b l r' hl hr lt => ?_,
    same.2.2.2.2.2 inv.index⟩
  · rw [same.1, inv.claims b z]
    exact claims_c1 c0 R Ad c1 change b z
  · rw [same.2.1, inv.unplaced i]
    have u := newUnplaced_exact ⟨fun _ => [], S0.unplaced, fun _ => ⟨[], []⟩, fun _ => []⟩
      c0 R Ad c1 exact.unplaced change (removedIds_unknown c0 R ids) i
    simp only [newUnplaced, List.mem_append, List.mem_filter, decide_eq_true_eq] at u
    exact u
  · rw [out] at h
    cases h2 : r.st.pairOut k with
    | none =>
      rw [h2] at h
      cases h
    | some row =>
      rw [h2] at h
      have e := Option.some.inj h
      rw [← e]
      dsimp only
      rcases inv.stored k row h2 with ⟨h0, hl1, hr1⟩ | g
      · obtain ⟨⟨b, ml, mr⟩, lt, ek, er⟩ := exact.stored k row h0
        have ml1 : row.l ∈ members c1 b := (mem_members c1 b _).2 ⟨hl1, ((mem_members c0 b _).1 ml).2⟩
        have mr1 : row.r ∈ members c1 b := (mem_members c1 b _).2 ⟨hr1, ((mem_members c0 b _).1 mr).2⟩
        by_cases cnd : k ∈ D.flatMap S0.pairIndex ∧ k ∉ r.tested
        · rw [ite_eq_left cnd]
          exact ⟨⟨b, ml1, mr1⟩, lt, ek, rfl⟩
        · rw [ite_eq_right cnd]
          by_cases tk : k ∈ r.tested
          · obtain ⟨row2, e2, g2⟩ := inv.tested k tk
            rw [h2] at e2
            rw [Option.some.inj e2]
            exact g2
          · have notV : k ∉ D.flatMap S0.pairIndex := fun hv => cnd ⟨hv, tk⟩
            have h0' : S0.pairOut k = some (rowOf rule A0 row.l row.r) := by
              rw [← er]
              exact h0
            have hs := stable_row rule reads S0 A0 A1 D answers exact.index k row.l row.r h0' notV
            exact ⟨⟨b, ml1, mr1⟩, lt, ek, er.trans hs⟩
      · have rt : retest rule A1 row = row := g.2.2.2.symm
        split
        · rw [rt]
          exact g
        · exact g
  · have hl0 := (inv.claims b l).2 ((claims_c1 c0 R Ad c1 change b l).2 hl)
    have hr0 := (inv.claims b r').2 ((claims_c1 c0 R Ad c1 change b r').2 hr)
    rw [out]
    rcases inv.pairs b l r' hl0 hr0 lt with h | ⟨_, _, h3, h4⟩
    · rw [h]
      show some (if _ then retest rule A1 (rowOf rule A1 l r') else rowOf rule A1 l r') = _
      split <;> rfl
    · rw [h4]
      show some (if _ then retest rule A1 (rowOf rule A0 l r') else rowOf rule A0 l r') = _
      split
      · rfl
      · rename_i cnd
        by_cases tk : (l.id, r'.id) ∈ r.tested
        · obtain ⟨row2, e2, g2⟩ := inv.tested _ tk
          rw [h4] at e2
          have := g2.2.2.2
          rw [← Option.some.inj e2] at this
          rw [this]
          rfl
        · have notV : (l.id, r'.id) ∉ D.flatMap S0.pairIndex := fun hv => cnd ⟨hv, tk⟩
          rw [stable_row rule reads S0 A0 A1 D answers exact.index _ l r' h3 notV]

/- Theorem 1: one step keeps the whole pair-level state exact. -/

theorem pair_step_exact (rule : Rule) (reads : ReadsDeclared rule) (st : PState)
    (c0 R Ad c1 : List Claim) (A0 A1 : Answers) (D : List Request)
    (exact : PExact rule st c0 A0) (change : ClaimChange c0 R Ad c1)
    (answers : AnswerChange A0 A1 D) (ids : RemovedIds c0 R) (keys : Keys c1) :
    PExact rule (pairStep rule st R Ad D A1).st c1 A1 := by
  have kept1 : ∀ z ∈ c0, z ∉ R → z ∈ c1 := fun z h0 hR => (change z).2 (Or.inl ⟨h0, hR⟩)
  have ad1 : ∀ z ∈ ([] : List Claim) ++ Ad, z ∈ c1 := by
    rw [List.nil_append]
    exact fun z h => (change z).2 (Or.inr h)
  have p1 := phase1_inv rule A0 st c0 exact R [] ⟨st, [], 0, 0⟩ (p1_init rule A0 st c0 exact)
  rw [List.nil_append] at p1
  have p2init := p2_init rule A0 A1 st c0 R c1 (phase1 st R) exact ids kept1 p1
    (phase1_counts R ⟨st, [], 0, 0⟩).1
  have p2 := phase2_inv rule A0 A1 st c0 R c1 keys kept1 Ad [] (phase1 st R) ad1 p2init
  rw [List.nil_append] at p2
  exact phase3_exact rule reads st c0 R Ad c1 A0 A1 D exact change answers ids _ p2

-- The derived per-bucket view of an exact state is the full reference check.
theorem pair_view_exact (rule : Rule) (st : PState) (cs : List Claim) (A : Answers)
    (e : PExact rule st cs A) (b : Nat) : SameOut (view st b) (full rule cs A b) := by
  have pmem : ∀ p, p ∈ pairs (st.claimsByBucket b) ↔ p ∈ pairs (members cs b) := by
    intro p
    obtain ⟨l, r⟩ := p
    rw [mem_pairs, mem_pairs, e.claims b l, e.claims b r]
  have stored : ∀ p ∈ pairs (members cs b),
      st.pairOut (p.1.id, p.2.id) = some (rowOf rule A p.1 p.2) := by
    intro p hp
    obtain ⟨l, r⟩ := p
    obtain ⟨hl, hr, lt⟩ := (mem_pairs _ l r).1 hp
    exact e.every b l r hl hr lt
  constructor
  · intro t
    simp only [view, full, bucketOut, results, List.mem_filterMap, List.mem_map]
    constructor
    · rintro ⟨p, hp, ht⟩
      have hp' := (pmem p).1 hp
      rw [stored p hp'] at ht
      exact ⟨p, hp', Option.some.inj ht⟩
    · rintro ⟨p, hp, ht⟩
      refine ⟨p, (pmem p).2 hp, ?_⟩
      rw [stored p hp, ← ht]
      rfl
  · intro q
    simp only [view, full, bucketOut, requests, List.mem_flatMap]
    constructor
    · rintro ⟨p, hp, hq⟩
      have hp' := (pmem p).1 hp
      rw [stored p hp'] at hq
      exact ⟨p, hp', hq⟩
    · rintro ⟨p, hp, hq⟩
      refine ⟨p, (pmem p).2 hp, ?_⟩
      rw [stored p hp]
      exact hq

theorem empty_exact (rule : Rule) (A : Answers) : PExact rule emptyState [] A := by
  refine ⟨fun b x => ?_, fun i => ?_, fun k row h => ?_, fun b l r hl _ _ => ?_, fun k q => ?_⟩
  · simp [emptyState, members]
  · simp [emptyState, unknownIds]
  · cases h
  · simp [members] at hl
  · simp [emptyState]

/- Any number of steps. -/

def pairRun (rule : Rule) : PState → List Change → PState
  | st, [] => st
  | st, ch :: rest =>
    pairRun rule (pairStep rule st ch.removed ch.added ch.arrived ch.answers).st rest

-- Each change lists every changed answer, removes no ID a remaining old claim
-- carries, and leaves keyed claims.
def PChainOK : List Claim → Answers → List Change → Prop
  | _, _, [] => True
  | cs, A, ch :: rest => AnswerChange A ch.answers ch.arrived ∧ RemovedIds cs ch.removed ∧
      Keys (applyChange cs ch.removed ch.added) ∧
      PChainOK (applyChange cs ch.removed ch.added) ch.answers rest

theorem pair_runs (rule : Rule) (reads : ReadsDeclared rule) (changes : List Change) :
    ∀ (st : PState) (cs : List Claim) (A : Answers), PExact rule st cs A → PChainOK cs A changes →
      PExact rule (pairRun rule st changes) (claimsAfter cs changes) (answersAfter A changes) := by
  induction changes with
  | nil => intro st cs A e _; exact e
  | cons ch rest ih =>
    intro st cs A e ok
    exact ih _ _ _ (pair_step_exact rule reads st cs ch.removed ch.added _ A ch.answers ch.arrived e
      (applyChange_change cs ch.removed ch.added) ok.1 ok.2.1 ok.2.2.1) ok.2.2.2

-- Starting from the empty store (the first run adds every claim), every
-- bucket's view after any run equals the full reference check.
theorem pair_runs_view (rule : Rule) (reads : ReadsDeclared rule) (A : Answers)
    (changes : List Change) (ok : PChainOK [] A changes) :
    ∀ b, SameOut (view (pairRun rule emptyState changes) b)
      (full rule (claimsAfter [] changes) (answersAfter A changes) b) :=
  pair_view_exact rule _ _ _ (pair_runs rule reads changes emptyState [] A (empty_exact rule A) ok)

/- Theorem 2: cost. -/

-- The size of a claim's bucket in a claims-by-bucket map (0 for Unknown).
def bucketSize (cbb : Nat → List Claim) (x : Claim) : Nat :=
  match x.bucket with
  | some b => (cbb b).length
  | none => 0

theorem sumMap_le {α : Type} (xs : List α) (f g : α → Nat) (le : ∀ x ∈ xs, f x ≤ g x) :
    (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x rest ih =>
    simp only [List.map_cons, List.sum_cons]
    have head := le x (List.mem_cons_self ..)
    have tail := ih (fun y hy => le y (List.mem_cons_of_mem _ hy))
    omega

theorem removeOne_cost (r : Run) (x : Claim) :
    (removeOne r x).deletes ≤ r.deletes + bucketSize r.st.claimsByBucket x ∧
    ∀ b, ((removeOne r x).st.claimsByBucket b).length ≤ (r.st.claimsByBucket b).length := by
  unfold bucketSize
  cases hb : x.bucket with
  | none =>
    simp only [removeOne, hb]
    exact ⟨Nat.le_add_right _ _, fun _ => Nat.le_refl _⟩
  | some b =>
    simp only [removeOne, hb]
    refine ⟨delPairsOf_deletes x _ r, fun b' => ?_⟩
    show (if b' = b then (r.st.claimsByBucket b).filter (fun z => z ≠ x)
      else (delPairsOf x (r.st.claimsByBucket b) r).st.claimsByBucket b').length ≤ _
    by_cases e : b' = b
    · rw [ite_eq_left e, e]
      exact List.length_filter_le _ _
    · rw [ite_eq_right e, (delPairsOf_same x _ r).1]
      exact Nat.le_refl _

theorem phase1_deletes : ∀ (R : List Claim) (r : Run),
    (R.foldl removeOne r).deletes ≤ r.deletes + (R.map (bucketSize r.st.claimsByBucket)).sum
  | [], _ => by simp
  | x :: R, r => by
    rw [List.foldl_cons]
    have ih := phase1_deletes R (removeOne r x)
    have h := removeOne_cost r x
    have shrink := sumMap_le R (bucketSize (removeOne r x).st.claimsByBucket)
      (bucketSize r.st.claimsByBucket) (fun y _ => by
        unfold bucketSize
        split
        · exact h.2 _
        · exact Nat.le_refl _)
    simp only [List.map_cons, List.sum_cons]
    omega

theorem addOne_cost (rule : Rule) (A1 : Answers) (r : Run) (y : Claim) :
    (addOne rule A1 r y).tests ≤ r.tests + bucketSize (addOne rule A1 r y).st.claimsByBucket y ∧
    (addOne rule A1 r y).deletes = r.deletes ∧
    ∀ b, (r.st.claimsByBucket b).length ≤ ((addOne rule A1 r y).st.claimsByBucket b).length := by
  unfold bucketSize
  cases hb : y.bucket with
  | none =>
    simp only [addOne, hb]
    exact ⟨Nat.le_add_right _ _, rfl, fun _ => Nat.le_refl _⟩
  | some b =>
    simp only [addOne, hb]
    have same := testPairsOf_same rule A1 y (r.st.claimsByBucket b) r
    have t := testPairsOf_tests rule A1 y (r.st.claimsByBucket b) r
    refine ⟨?_, same.2.2, fun b' => ?_⟩
    · show _ ≤ r.tests + (if b = b then r.st.claimsByBucket b ++ [y]
        else (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.claimsByBucket b).length
      rw [ite_eq_left rfl, List.length_append]
      exact Nat.le_trans t (Nat.add_le_add_left (Nat.le_add_right _ _) _)
    · show _ ≤ (if b' = b then r.st.claimsByBucket b ++ [y]
        else (testPairsOf rule A1 y (r.st.claimsByBucket b) r).st.claimsByBucket b').length
      by_cases e : b' = b
      · rw [ite_eq_left e, List.length_append, e]
        exact Nat.le_add_right _ _
      · rw [ite_eq_right e, same.1]
        exact Nat.le_refl _

theorem phase2_mono (rule : Rule) (A1 : Answers) : ∀ (L : List Claim) (r : Run) (b : Nat),
    (r.st.claimsByBucket b).length ≤ ((L.foldl (addOne rule A1) r).st.claimsByBucket b).length ∧
    (L.foldl (addOne rule A1) r).deletes = r.deletes
  | [], _, _ => ⟨Nat.le_refl _, rfl⟩
  | y :: L, r, b => by
    rw [List.foldl_cons]
    have ih := phase2_mono rule A1 L (addOne rule A1 r y) b
    have h := addOne_cost rule A1 r y
    exact ⟨Nat.le_trans (h.2.2 b) ih.1, ih.2.trans h.2.1⟩

theorem phase2_tests (rule : Rule) (A1 : Answers) : ∀ (L : List Claim) (r : Run),
    (L.foldl (addOne rule A1) r).tests ≤
      r.tests + (L.map (bucketSize (L.foldl (addOne rule A1) r).st.claimsByBucket)).sum
  | [], _ => by simp
  | y :: L, r => by
    rw [List.foldl_cons]
    have ih := phase2_tests rule A1 L (addOne rule A1 r y)
    have h1 := (addOne_cost rule A1 r y).1
    have h3 : bucketSize (addOne rule A1 r y).st.claimsByBucket y ≤
        bucketSize (L.foldl (addOne rule A1) (addOne rule A1 r y)).st.claimsByBucket y := by
      unfold bucketSize
      split
      · exact (phase2_mono rule A1 L (addOne rule A1 r y) _).1
      · exact Nat.le_refl _
    simp only [List.map_cons, List.sum_cons]
    omega

-- Pair tests: at most the size (after the step) of each added claim's bucket,
-- plus one per visited old-index entry of an arrived request. Deletions: at
-- most the old size of each removed claim's bucket. No other bucket's size
-- enters either bound.
theorem pair_cost_bound (rule : Rule) (st : PState) (R Ad : List Claim) (D : List Request)
    (A1 : Answers) :
    (pairStep rule st R Ad D A1).tests ≤
      (Ad.map (bucketSize (pairStep rule st R Ad D A1).st.claimsByBucket)).sum +
        (D.map (fun q => (st.pairIndex q).length)).sum ∧
    (pairStep rule st R Ad D A1).deletes ≤ (R.map (bucketSize st.claimsByBucket)).sum := by
  have c1 := phase1_counts R ⟨st, [], 0, 0⟩
  have d1 := phase1_deletes R ⟨st, [], 0, 0⟩
  have t2 := phase2_tests rule A1 Ad (phase1 st R)
  have m2 := phase2_mono rule A1 Ad (phase1 st R) 0
  have s3 := retestKeys_same rule A1 (D.flatMap st.pairIndex) (phase2 rule A1 (phase1 st R) Ad)
  have len : (D.flatMap st.pairIndex).length = (D.map (fun q => (st.pairIndex q).length)).sum :=
    List.length_flatMap ..
  have c1t : (phase1 st R).tests = 0 := c1.2
  have d1' : (phase1 st R).deletes ≤ (R.map (bucketSize st.claimsByBucket)).sum := by
    have : (phase1 st R).deletes ≤ 0 + (R.map (bucketSize st.claimsByBucket)).sum := d1
    omega
  refine ⟨?_, ?_⟩
  · show (retestKeys rule A1 (D.flatMap st.pairIndex) (phase2 rule A1 (phase1 st R) Ad)).tests ≤
      (Ad.map (bucketSize (retestKeys rule A1 (D.flatMap st.pairIndex)
        (phase2 rule A1 (phase1 st R) Ad)).st.claimsByBucket)).sum + _
    rw [s3.1]
    have t2' : (phase2 rule A1 (phase1 st R) Ad).tests ≤
        0 + (Ad.map (bucketSize (phase2 rule A1 (phase1 st R) Ad).st.claimsByBucket)).sum := by
      rw [← c1t]
      exact t2
    have := s3.2.2.2.2.1
    omega
  · show (retestKeys rule A1 (D.flatMap st.pairIndex) (phase2 rule A1 (phase1 st R) Ad)).deletes ≤ _
    rw [s3.2.2.2.1]
    show (Ad.foldl (addOne rule A1) (phase1 st R)).deletes ≤ _
    rw [m2.2]
    exact d1'

-- One added claim costs at most n pair tests, where n is the size of its
-- bucket after the step (not n * n); one removed claim costs at most n
-- deletions, n its bucket's old size, and no pair tests.
theorem pair_cost_single (rule : Rule) (st : PState) (y : Claim) (b : Nat) (A1 : Answers)
    (hb : y.bucket = some b) :
    (pairStep rule st [] [y] [] A1).tests ≤ (st.claimsByBucket b).length + 1 ∧
    (pairStep rule st [y] [] [] A1).deletes ≤ (st.claimsByBucket b).length ∧
    (pairStep rule st [y] [] [] A1).tests = 0 := by
  have add := (pair_cost_bound rule st [] [y] [] A1).1
  have rem := pair_cost_bound rule st [y] [] [] A1
  have final : (pairStep rule st [] [y] [] A1).st.claimsByBucket b = st.claimsByBucket b ++ [y] := by
    show (addOne rule A1 ⟨st, [], 0, 0⟩ y).st.claimsByBucket b = _
    simp only [addOne, hb]
    show (if b = b then st.claimsByBucket b ++ [y] else _) = _
    rw [ite_eq_left rfl]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, bucketSize, hb, final,
    List.length_append, List.length_singleton] at add rem
  exact ⟨by omega, by omega, by omega⟩

/- Counterexamples (theorem 3). -/

-- (a) Testing an added claim only against the claims present before the
-- step misses the pair between two claims added in the same batch.
def addOneOld (rule : Rule) (A1 : Answers) (base : PState) (r : Run) (y : Claim) : Run :=
  match y.bucket with
  | none => withSt r (setUnplaced r.st (r.st.unplaced ++ [y.id]))
  | some b =>
    withSt (testPairsOf rule A1 y (base.claimsByBucket b) r)
      (setBucket (testPairsOf rule A1 y (base.claimsByBucket b) r).st b
        (r.st.claimsByBucket b ++ [y]))

def stepOldMembers (rule : Rule) (st : PState) (R Ad : List Claim) (D : List Request)
    (A1 : Answers) : Run :=
  retestKeys rule A1 (D.flatMap st.pairIndex)
    (Ad.foldl (addOneOld rule A1 (phase1 st R).st) (phase1 st R))

theorem batch_pair_needed :
    ¬ ∀ (rule : Rule) (st : PState) (c0 R Ad c1 : List Claim) (A0 A1 : Answers)
      (D : List Request), ReadsDeclared rule → PExact rule st c0 A0 → ClaimChange c0 R Ad c1 →
      AnswerChange A0 A1 D → RemovedIds c0 R → Keys c1 →
      PExact rule (stepOldMembers rule st R Ad D A1).st c1 A1 := by
  intro h
  have e := h judged emptyState [] [] [x0, x1] (applyChange [] [] [x0, x1]) noAnswers noAnswers []
    judged_reads (empty_exact judged noAnswers) (applyChange_change [] [] [x0, x1])
    (fun _ _ => rfl) (fun _ hx => absurd hx List.not_mem_nil) (by unfold Keys; decide)
  have m := e.every 0 x0 x1 (by decide) (by decide) (by decide)
  exact absurd m (by decide)

-- (b) Not deleting a removed claim's pairs leaves its stored row: here the
-- violation (0, 1) stays after claim 1 is removed.
def removeKeep (r : Run) (x : Claim) : Run :=
  match x.bucket with
  | none => withSt r (setUnplaced r.st (r.st.unplaced.filter (fun i => i ≠ x.id)))
  | some b => withSt r (setBucket r.st b ((r.st.claimsByBucket b).filter (fun z => z ≠ x)))

def stepKeepRemoved (rule : Rule) (st : PState) (R Ad : List Claim) (D : List Request)
    (A1 : Answers) : Run :=
  retestKeys rule A1 (D.flatMap st.pairIndex) (phase2 rule A1 (R.foldl removeKeep ⟨st, [], 0, 0⟩) Ad)

-- The exact state of claims 0 and 1 under answers A, built by one step from
-- the empty store.
def twoClaims (A : Answers) : PState :=
  (pairStep judged emptyState [] [x0, x1] [] A).st

theorem twoClaims_exact (A : Answers) :
    PExact judged (twoClaims A) (applyChange [] [] [x0, x1]) A :=
  pair_step_exact judged judged_reads emptyState [] [] [x0, x1] _ A A [] (empty_exact judged A)
    (applyChange_change [] [] [x0, x1]) (fun _ _ => rfl) (fun _ hx => absurd hx List.not_mem_nil)
    (by unfold Keys; decide)

theorem removed_pairs_needed :
    ¬ ∀ (rule : Rule) (st : PState) (c0 R Ad c1 : List Claim) (A0 A1 : Answers)
      (D : List Request), ReadsDeclared rule → PExact rule st c0 A0 → ClaimChange c0 R Ad c1 →
      AnswerChange A0 A1 D → RemovedIds c0 R → Keys c1 →
      PExact rule (stepKeepRemoved rule st R Ad D A1).st c1 A1 := by
  intro h
  have e := h judged (twoClaims answer01) (applyChange [] [] [x0, x1]) [x1] []
    (applyChange (applyChange [] [] [x0, x1]) [x1] []) answer01 answer01 [] judged_reads
    (twoClaims_exact answer01) (applyChange_change _ [x1] []) (fun _ _ => rfl)
    (by unfold RemovedIds; decide) (by unfold Keys; decide)
  have stale : (stepKeepRemoved judged (twoClaims answer01) [x1] [] [] answer01).st.pairOut (0, 1) =
      some ⟨x0, x1, .violation 1, [1]⟩ := by decide
  obtain ⟨⟨b, _, hr⟩, _⟩ := e.stored (0, 1) _ stale
  exact absurd ((mem_members _ _ _).1 hr).1 (by decide)

-- (c) Ignoring arrived answers leaves a stale Pending: the answer to (0, 1)
-- arrives and the stored row stays Pending.
def stepIgnoreArrivals (rule : Rule) (st : PState) (R Ad : List Claim) (_D : List Request)
    (A1 : Answers) : Run :=
  phase2 rule A1 (phase1 st R) Ad

theorem arrivals_needed :
    ¬ ∀ (rule : Rule) (st : PState) (c0 R Ad c1 : List Claim) (A0 A1 : Answers)
      (D : List Request), ReadsDeclared rule → PExact rule st c0 A0 → ClaimChange c0 R Ad c1 →
      AnswerChange A0 A1 D → RemovedIds c0 R → Keys c1 →
      PExact rule (stepIgnoreArrivals rule st R Ad D A1).st c1 A1 := by
  intro h
  have e := h judged (twoClaims noAnswers) (applyChange [] [] [x0, x1]) [] []
    (applyChange (applyChange [] [] [x0, x1]) [] []) noAnswers answer01 [1] judged_reads
    (twoClaims_exact noAnswers) (applyChange_change _ [] []) answer01_change
    (fun _ hx => absurd hx List.not_mem_nil) (by unfold Keys; decide)
  have m := e.every 0 x0 x1 (by decide) (by decide) (by decide)
  have pending : (stepIgnoreArrivals judged (twoClaims noAnswers) [] [] [1] answer01).st.pairOut
      (0, 1) = some ⟨x0, x1, .pending, [1]⟩ := by decide
  rw [show x0.id = 0 from rfl, show x1.id = 1 from rfl, pending] at m
  exact absurd m (by decide)

-- RemovedIds is needed: removing a claim that is not in c0 but shares ID 1
-- with the remaining claim 1 deletes the live pair (0, 1).
def x1other : Claim := ⟨1, some 0, 0⟩

theorem removed_ids_needed :
    ¬ ∀ (rule : Rule) (st : PState) (c0 R Ad c1 : List Claim) (A0 A1 : Answers)
      (D : List Request), ReadsDeclared rule → PExact rule st c0 A0 → ClaimChange c0 R Ad c1 →
      AnswerChange A0 A1 D → Keys c1 → PExact rule (pairStep rule st R Ad D A1).st c1 A1 := by
  intro h
  have e := h judged (twoClaims noAnswers) (applyChange [] [] [x0, x1]) [x1other] []
    (applyChange (applyChange [] [] [x0, x1]) [x1other] []) noAnswers noAnswers [] judged_reads
    (twoClaims_exact noAnswers) (applyChange_change _ [x1other] []) (fun _ _ => rfl)
    (by unfold Keys; decide)
  have m := e.every 0 x0 x1 (by decide) (by decide) (by decide)
  exact absurd m (by decide)

-- Keys is needed for any exact state to exist: two buckets with pairs (0, 1)
-- of different claims would need two rows under one key.
def y0 : Claim := ⟨0, some 1, 0⟩
def y1 : Claim := ⟨1, some 1, 0⟩

theorem exact_needs_keys : ¬ ∃ st, PExact judged st [x0, x1, y0, y1] noAnswers := by
  rintro ⟨st, e⟩
  have h1 := e.every 0 x0 x1 (by decide) (by decide) (by decide)
  have h2 := e.every 1 y0 y1 (by decide) (by decide) (by decide)
  have h3 := h1.symm.trans h2
  exact absurd h3 (by decide)

end ConcordPairLevel
