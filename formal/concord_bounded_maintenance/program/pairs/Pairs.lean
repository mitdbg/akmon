import Std.Tactic
namespace ConcordPairs
/-
Model of the pair-rule core of Algorithm 2 (concord-design.tex), revised: the
buckets that UPDATE re-checks are computed from the removed claims, the added
claims, and a request index over changed answers. There is no full-pass
fallback for answer changes. A rule change takes a full pass and is out of
scope for these theorems. This is a proved model of the design, not a proof of
production code.
-/

-- A claim: identity, bucket (none = Unknown, not placed in any bucket), value.
structure Claim where
  id : Nat
  bucket : Option Nat
  value : Nat
deriving DecidableEq, Repr

abbrev Request := Nat
-- An answer snapshot. none = Missing.
abbrev Answers := Request → Option Nat

-- check_pair outcomes (Algorithm 2 lines 32-41).
inductive Outcome where
  | noViolation
  | violation (c : Nat)
  | pending
  | failed
deriving DecidableEq, Repr

-- A pair rule: the requests one pair test reads (they depend on the rule and
-- the two claims, not on answers), and the test itself.
structure Rule where
  req : Claim → Claim → List Request
  test : Claim → Claim → Answers → Outcome

-- Read discipline: a test reads only the answers to its declared requests.
def ReadsDeclared (rule : Rule) : Prop :=
  ∀ l r (A A' : Answers), (∀ q ∈ rule.req l r, A q = A' q) →
    rule.test l r A = rule.test l r A'

-- indexed_claims(claims, bucket) (line 30).
def members (cs : List Claim) (b : Nat) : List Claim :=
  cs.filter (fun c => c.bucket = some b)

-- The pairs of a bucket with left.id < right.id (line 31).
def pairs (ms : List Claim) : List (Claim × Claim) :=
  ms.flatMap (fun l => (ms.filter (fun r => l.id < r.id)).map (fun r => (l, r)))

-- One bucket's stored output: (left id, right id, outcome) per pair, and the
-- union of the requests its pair tests read (bucket_requests, line 34).
structure BucketOut where
  results : List (Nat × Nat × Outcome)
  requests : List Request
deriving DecidableEq, Repr

def results (rule : Rule) (ms : List Claim) (A : Answers) : List (Nat × Nat × Outcome) :=
  (pairs ms).map (fun p => (p.1.id, p.2.id, rule.test p.1 p.2 A))

def requests (rule : Rule) (ms : List Claim) : List Request :=
  (pairs ms).flatMap (fun p => rule.req p.1 p.2)

def bucketOut (rule : Rule) (cs : List Claim) (A : Answers) (b : Nat) : BucketOut :=
  ⟨results rule (members cs b) A, requests rule (members cs b)⟩

-- The full reference check: every bucket, every pair. A bucket with no
-- members has empty output, which models "no output stored".
def full (rule : Rule) (cs : List Claim) (A : Answers) : Nat → BucketOut :=
  fun b => bucketOut rule cs A b

-- Stored state: the old output of every bucket. Its requests fields are the
-- stored request lists the index is built from.
structure State where
  out : Nat → BucketOut

-- The request index must list every bucket whose stored output read q.
-- Extra buckets are allowed; they only cost extra work.
def IndexComplete (old : State) (index : Request → List Nat) : Prop :=
  ∀ b q, q ∈ (old.out b).requests → b ∈ index q

-- The known buckets of some claims (Unknown claims contribute none).
def knownBuckets (cs : List Claim) : List Nat := cs.filterMap (fun c => c.bucket)

-- Revised lines 16-20: buckets of removed and added claims, plus every bucket
-- the index lists for a changed answer.
def affected (R Ad : List Claim) (D : List Request) (index : Request → List Nat) : List Nat :=
  knownBuckets R ++ knownBuckets Ad ++ D.flatMap index

-- Lines 21-41: replace the output of each chosen bucket; keep the rest.
def refresh (rule : Rule) (old : State) (c1 : List Claim) (A1 : Answers)
    (chosen : List Nat) (b : Nat) : BucketOut :=
  if b ∈ chosen then bucketOut rule c1 A1 b else old.out b

def update (rule : Rule) (old : State) (c1 : List Claim) (A1 : Answers)
    (R Ad : List Claim) (D : List Request) (index : Request → List Nat) : Nat → BucketOut :=
  refresh rule old c1 A1 (affected R Ad D index)

-- New claims are the old ones minus the removed ones plus the added ones.
def ClaimChange (c0 R Ad c1 : List Claim) : Prop :=
  ∀ x, x ∈ c1 ↔ (x ∈ c0 ∧ x ∉ R) ∨ x ∈ Ad

-- Every answer outside D is unchanged.
def AnswerChange (A0 A1 : Answers) (D : List Request) : Prop :=
  ∀ q, q ∉ D → A0 q = A1 q

-- The canonical new claim list an implementation builds.
def applyChange (c0 R Ad : List Claim) : List Claim :=
  c0.filter (fun x => x ∉ R) ++ Ad

-- Two bucket outputs hold the same rows and the same requests, ignoring order
-- and duplicates.
def SameOut (o o' : BucketOut) : Prop :=
  (∀ t, t ∈ o.results ↔ t ∈ o'.results) ∧ (∀ q, q ∈ o.requests ↔ q ∈ o'.requests)

-- A bucket that no claim of cs is in has no members. (Proved by recursion:
-- the library's filter_eq_nil_iff depends on Classical.choice.)
theorem members_empty (cs : List Claim) (b : Nat) (absent : ∀ x ∈ cs, x.bucket ≠ some b) :
    members cs b = [] := by
  induction cs with
  | nil => rfl
  | cons x rest ih =>
    unfold members
    rw [List.filter_cons_of_neg (p := fun c : Claim => decide (c.bucket = some b))
      (fun h => absent x (List.mem_cons_self ..) (of_decide_eq_true h))]
    exact ih (fun y hy => absent y (List.mem_cons_of_mem _ hy))

theorem mem_affected (R Ad : List Claim) (D : List Request) (index : Request → List Nat)
    (b : Nat) :
    b ∈ affected R Ad D index ↔ (∃ x ∈ R, x.bucket = some b) ∨
      (∃ x ∈ Ad, x.bucket = some b) ∨ ∃ q ∈ D, b ∈ index q := by
  simp [affected, knownBuckets]

theorem mem_pairs (ms : List Claim) (l r : Claim) :
    (l, r) ∈ pairs ms ↔ l ∈ ms ∧ r ∈ ms ∧ l.id < r.id := by
  simp only [pairs, List.mem_flatMap, List.mem_map, List.mem_filter, decide_eq_true_eq,
    Prod.mk.injEq]
  constructor
  · rintro ⟨a, ha, c, ⟨hc, hlt⟩, rfl, rfl⟩
    exact ⟨ha, hc, hlt⟩
  · rintro ⟨hl, hr, hlt⟩
    exact ⟨l, hl, r, ⟨hr, hlt⟩, rfl, rfl⟩

-- Theorem 2: outside the affected buckets the members are the same set.
theorem members_unchanged_outside (c0 R Ad c1 : List Claim) (D : List Request)
    (index : Request → List Nat) (b : Nat)
    (change : ClaimChange c0 R Ad c1) (outside : b ∉ affected R Ad D index) :
    ∀ x, x ∈ members c1 b ↔ x ∈ members c0 b := by
  intro x
  rw [mem_affected] at outside
  simp only [members, List.mem_filter, decide_eq_true_eq]
  constructor
  · rintro ⟨hx, hb⟩
    rcases (change x).1 hx with ⟨h0, _⟩ | hAd
    · exact ⟨h0, hb⟩
    · exact absurd (Or.inr (Or.inl ⟨x, hAd, hb⟩)) outside
  · rintro ⟨h0, hb⟩
    exact ⟨(change x).2 (Or.inl ⟨h0, fun hR => outside (Or.inl ⟨x, hR, hb⟩)⟩), hb⟩

-- The same lemma as list equality for the canonical new claim list.
theorem members_apply_outside (c0 R Ad : List Claim) (D : List Request)
    (index : Request → List Nat) (b : Nat) (outside : b ∉ affected R Ad D index) :
    members (applyChange c0 R Ad) b = members c0 b := by
  rw [mem_affected] at outside
  have noAd : members Ad b = [] :=
    members_empty Ad b (fun x hx hb => outside (Or.inr (Or.inl ⟨x, hx, hb⟩)))
  unfold applyChange
  simp only [members, List.filter_append] at noAd ⊢
  rw [noAd, List.append_nil, List.filter_filter]
  apply List.filter_congr
  intro x _
  cases hb : decide (x.bucket = some b)
  · rfl
  · have notR : x ∉ R := fun hR => outside (Or.inl ⟨x, hR, of_decide_eq_true hb⟩)
    simp [notR]

-- Outside the affected buckets every request a stored output read is unchanged.
theorem stable_answers (old : State) (R Ad : List Claim) (A0 A1 : Answers)
    (D : List Request) (index : Request → List Nat) (b : Nat)
    (indexed : IndexComplete old index) (answers : AnswerChange A0 A1 D)
    (outside : b ∉ affected R Ad D index) :
    ∀ q ∈ (old.out b).requests, A0 q = A1 q := by
  intro q hq
  apply answers
  intro hD
  apply outside
  rw [mem_affected]
  exact Or.inr (Or.inr ⟨q, hD, indexed b q hq⟩)

theorem bucketOut_congr (rule : Rule) (reads : ReadsDeclared rule) (c0 c1 : List Claim)
    (A0 A1 : Answers) (b : Nat) (same : members c0 b = members c1 b)
    (agree : ∀ q ∈ requests rule (members c0 b), A0 q = A1 q) :
    bucketOut rule c0 A0 b = bucketOut rule c1 A1 b := by
  unfold bucketOut
  rw [← same]
  congr 1
  unfold results
  apply List.map_congr_left
  intro p hp
  rw [reads p.1 p.2 A0 A1 (fun q hq => agree q (List.mem_flatMap.2 ⟨p, hp, hq⟩))]

-- Theorem 1 (sets): under the spec's change hypotheses, every bucket of the
-- updated state holds the same rows and requests as the full reference check.
theorem update_exact (rule : Rule) (reads : ReadsDeclared rule) (old : State)
    (c0 R Ad c1 : List Claim) (A0 A1 : Answers) (D : List Request)
    (index : Request → List Nat)
    (stored : old.out = full rule c0 A0) (change : ClaimChange c0 R Ad c1)
    (answers : AnswerChange A0 A1 D) (indexed : IndexComplete old index) :
    ∀ b, SameOut (update rule old c1 A1 R Ad D index b) (full rule c1 A1 b) := by
  intro b
  unfold update refresh
  split
  · exact ⟨fun _ => Iff.rfl, fun _ => Iff.rfl⟩
  · rename_i outside
    have agree := stable_answers old R Ad A0 A1 D index b indexed answers outside
    have mem := members_unchanged_outside c0 R Ad c1 D index b change outside
    rw [stored] at agree ⊢
    have pmem : ∀ p, p ∈ pairs (members c0 b) ↔ p ∈ pairs (members c1 b) := by
      intro p
      obtain ⟨l, r⟩ := p
      rw [mem_pairs, mem_pairs, mem l, mem r]
    have tests : ∀ p ∈ pairs (members c0 b), rule.test p.1 p.2 A0 = rule.test p.1 p.2 A1 :=
      fun p hp => reads p.1 p.2 A0 A1 (fun q hq => agree q (List.mem_flatMap.2 ⟨p, hp, hq⟩))
    constructor
    · intro t
      simp only [full, bucketOut, results, List.mem_map]
      constructor
      · rintro ⟨p, hp, rfl⟩
        exact ⟨p, (pmem p).1 hp, by rw [tests p hp]⟩
      · rintro ⟨p, hp, rfl⟩
        have hp0 := (pmem p).2 hp
        exact ⟨p, hp0, by rw [tests p hp0]⟩
    · intro q
      simp only [full, bucketOut, requests, List.mem_flatMap]
      constructor
      · rintro ⟨p, hp, hq⟩
        exact ⟨p, (pmem p).1 hp, hq⟩
      · rintro ⟨p, hp, hq⟩
        exact ⟨p, (pmem p).2 hp, hq⟩

-- Exactness of a choice of re-checked buckets, as function equality, for the
-- canonical new claim list.
def ExactFor (choose : List Claim → List Claim → List Request → (Request → List Nat) → List Nat) :
    Prop :=
  ∀ (rule : Rule) (old : State) (c0 R Ad : List Claim) (A0 A1 : Answers) (D : List Request)
    (index : Request → List Nat),
    ReadsDeclared rule → old.out = full rule c0 A0 → AnswerChange A0 A1 D →
    IndexComplete old index →
    refresh rule old (applyChange c0 R Ad) A1 (choose R Ad D index) =
      full rule (applyChange c0 R Ad) A1

-- Theorem 1 (lists): with the canonical new claim list, the updated state is
-- exactly the full reference check, row order included.
theorem update_exact_lists : ExactFor affected := by
  intro rule old c0 R Ad A0 A1 D index reads stored answers indexed
  funext b
  unfold refresh
  split
  · rfl
  · rename_i outside
    have agree := stable_answers old R Ad A0 A1 D index b indexed answers outside
    rw [stored] at agree ⊢
    exact bucketOut_congr rule reads c0 _ A0 A1 b
      (members_apply_outside c0 R Ad D index b outside).symm agree

theorem members_append (a a' : List Claim) (b : Nat) :
    members (a ++ a') b = members a b ++ members a' b := List.filter_append ..

theorem members_unplaced (U : List Claim) (b : Nat) (unplaced : ∀ x ∈ U, x.bucket = none) :
    members U b = [] :=
  members_empty U b (fun x hx hb => by rw [unplaced x hx] at hb; cases hb)

theorem knownBuckets_unplaced (U : List Claim) (unplaced : ∀ x ∈ U, x.bucket = none) :
    knownBuckets U = [] := by
  simp only [knownBuckets, List.filterMap_eq_nil_iff]
  exact unplaced

-- Theorem 3: Unknown claims among the removed or added claims add no affected
-- bucket and change no bucket's output; moving one claim between Unknown and a
-- known bucket affects exactly that known bucket.
theorem unplaced_never_affects (rule : Rule) (cs R Ad U U' : List Claim) (A : Answers)
    (D : List Request) (index : Request → List Nat)
    (unplacedU : ∀ x ∈ U, x.bucket = none) (unplacedU' : ∀ x ∈ U', x.bucket = none) :
    affected (R ++ U) (Ad ++ U') D index = affected R Ad D index ∧
      (∀ b, bucketOut rule (cs ++ U) A b = bucketOut rule cs A b) ∧
      (∀ b, bucketOut rule (cs ++ U') A b = bucketOut rule cs A b) := by
  refine ⟨?_, ?_, ?_⟩
  · unfold affected
    simp only [knownBuckets, List.filterMap_append]
    rw [show U.filterMap (fun c => c.bucket) = [] from knownBuckets_unplaced U unplacedU,
      show U'.filterMap (fun c => c.bucket) = [] from knownBuckets_unplaced U' unplacedU']
    simp
  · intro b
    unfold bucketOut
    rw [members_append, members_unplaced U b unplacedU, List.append_nil]
  · intro b
    unfold bucketOut
    rw [members_append, members_unplaced U' b unplacedU', List.append_nil]

theorem unknown_move_affects_one (x x' : Claim) (b : Nat) (index : Request → List Nat)
    (unknown : x.bucket = none) (known : x'.bucket = some b) :
    affected [x] [x'] [] index = [b] ∧ affected [x'] [x] [] index = [b] := by
  simp [affected, knownBuckets, unknown, known]

-- Theorem 4: a claim moved from bucket b to b' (old version removed, new
-- version added) puts both buckets in the affected set.
theorem move_refreshes_both (R Ad : List Claim) (D : List Request) (index : Request → List Nat)
    (x x' : Claim) (b b' : Nat) (removed : x ∈ R) (added : x' ∈ Ad)
    (from_b : x.bucket = some b) (to_b' : x'.bucket = some b') :
    b ∈ affected R Ad D index ∧ b' ∈ affected R Ad D index := by
  rw [mem_affected, mem_affected]
  exact ⟨Or.inl ⟨x, removed, from_b⟩, Or.inr (Or.inl ⟨x', added, to_b'⟩)⟩

-- Theorem 5: operation count. Per affected bucket: at most n*n pair tests,
-- delete its old rows and requests, write it. Then read R and Ad, probe the
-- index for each changed answer, and commit.
def cost (old : State) (c1 R Ad : List Claim) (D : List Request)
    (index : Request → List Nat) : Nat :=
  ((affected R Ad D index).map (fun b =>
      (members c1 b).length * (members c1 b).length + (old.out b).requests.length + 1)).sum +
    R.length + Ad.length + (D.map (fun q => (index q).length)).sum + 1

theorem cost_local (old : State) (c1 c1' R Ad : List Claim) (D : List Request)
    (index : Request → List Nat)
    (same : ∀ b ∈ affected R Ad D index, members c1 b = members c1' b) :
    cost old c1 R Ad D index = cost old c1' R Ad D index := by
  unfold cost
  have equal : (affected R Ad D index).map (fun b =>
        (members c1 b).length * (members c1 b).length + (old.out b).requests.length + 1) =
      (affected R Ad D index).map (fun b =>
        (members c1' b).length * (members c1' b).length + (old.out b).requests.length + 1) := by
    apply List.map_congr_left
    intro b member
    rw [same b member]
  rw [equal]

theorem pairs_le_square (ms : List Claim) : (pairs ms).length ≤ ms.length * ms.length := by
  have key : ∀ outer : List Claim,
      (outer.flatMap (fun l => (ms.filter (fun r => l.id < r.id)).map (fun r => (l, r)))).length ≤
        outer.length * ms.length := by
    intro outer
    induction outer with
    | nil => simp
    | cons l rest ih =>
      simp only [List.flatMap_cons, List.length_append, List.length_map, List.length_cons,
        Nat.succ_mul]
      have := List.length_filter_le (fun r => decide (l.id < r.id)) ms
      omega
  exact key ms

theorem sum_map_le (xs : List Nat) (f g : Nat → Nat) (le : ∀ x ∈ xs, f x ≤ g x) :
    (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x rest ih =>
    simp only [List.map_cons, List.sum_cons]
    have head := le x (by simp)
    have tail := ih (fun y hy => le y (by simp [hy]))
    omega

-- The pair tests UPDATE runs are bounded by the cost.
theorem update_tests_le_cost (old : State) (c1 R Ad : List Claim) (D : List Request)
    (index : Request → List Nat) :
    ((affected R Ad D index).map (fun b => (pairs (members c1 b)).length)).sum ≤
      cost old c1 R Ad D index := by
  unfold cost
  have := sum_map_le (affected R Ad D index) (fun b => (pairs (members c1 b)).length)
    (fun b => (members c1 b).length * (members c1 b).length + (old.out b).requests.length + 1)
    (fun b _ => by have := pairs_le_square (members c1 b); omega)
  omega

/- Concrete counterexamples (theorem 6). -/

-- A judged rule: a pair reads one answer, request l.id * 10 + r.id.
def judged : Rule where
  req l r := [l.id * 10 + r.id]
  test l r A := match A (l.id * 10 + r.id) with
    | none => .pending
    | some 0 => .noViolation
    | some _ => if l.value = r.value then .noViolation else .violation 1

theorem judged_reads : ReadsDeclared judged := by
  intro l r A A' agree
  have h := agree (l.id * 10 + r.id) (by simp [judged])
  show (match A (l.id * 10 + r.id) with
    | none => Outcome.pending
    | some 0 => .noViolation
    | some _ => if l.value = r.value then .noViolation else .violation 1) =
    (match A' (l.id * 10 + r.id) with
    | none => Outcome.pending
    | some 0 => .noViolation
    | some _ => if l.value = r.value then .noViolation else .violation 1)
  rw [h]

def x0 : Claim := ⟨0, some 0, 0⟩
def x1 : Claim := ⟨1, some 0, 1⟩
def x1moved : Claim := ⟨1, some 1, 1⟩
def x2 : Claim := ⟨2, some 0, 0⟩
def noAnswers : Answers := fun _ => none
-- The answer to pair (0, 1) arrives: Missing -> 1.
def answer01 : Answers := fun q => if q = 1 then some 1 else none
def oldState : State := ⟨full judged [x0, x1] noAnswers⟩

theorem old_index_complete : IndexComplete oldState (fun _ => [0]) := by
  intro b q hq
  cases b with
  | zero => simp
  | succ n =>
    have empty : members [x0, x1] (n + 1) = [] := by
      apply members_empty
      intro x hx hb
      cases hx with
      | head => exact Nat.succ_ne_zero n (Option.some.inj hb).symm
      | tail _ hx =>
        cases hx with
        | head => exact Nat.succ_ne_zero n (Option.some.inj hb).symm
        | tail _ hx => cases hx
    have hq' : q ∈ requests judged (members [x0, x1] (n + 1)) := hq
    rw [empty] at hq'
    exact absurd hq' List.not_mem_nil

theorem answer01_change : AnswerChange noAnswers answer01 [1] := by
  intro q hq
  have : q ≠ 1 := fun h => hq (by simp [h])
  simp [noAnswers, answer01, this]

-- Omitting the answer-driven buckets is not exact: the answer to (0, 1)
-- arrives and bucket 0 keeps its stale Pending row.
theorem ignoring_answers_not_exact :
    ¬ ExactFor (fun R Ad _ _ => knownBuckets R ++ knownBuckets Ad) := by
  intro h
  have e := congrFun (h judged oldState [x0, x1] [] [] noAnswers answer01 [1] (fun _ => [0])
    judged_reads rfl answer01_change old_index_complete) 0
  exact absurd e (by decide)

-- Omitting the old bucket of a moved claim is not exact: claim 1 moves from
-- bucket 0 to bucket 1 and bucket 0 keeps the pair (0, 1).
theorem new_bucket_only_not_exact :
    ¬ ExactFor (fun _ Ad D index => knownBuckets Ad ++ D.flatMap index) := by
  intro h
  have e := congrFun (h judged oldState [x0, x1] [x1] [x1moved] noAnswers noAnswers [] (fun _ => [0])
    judged_reads rfl (fun _ _ => rfl) old_index_complete) 0
  exact absurd e (by decide)

-- An index that misses a bucket breaks exactness (IndexComplete is needed).
theorem incomplete_index_not_exact :
    ¬ ∀ (rule : Rule) (old : State) (c0 R Ad : List Claim) (A0 A1 : Answers) (D : List Request)
      (index : Request → List Nat),
      ReadsDeclared rule → old.out = full rule c0 A0 → AnswerChange A0 A1 D →
      update rule old (applyChange c0 R Ad) A1 R Ad D index = full rule (applyChange c0 R Ad) A1 := by
  intro h
  have e := congrFun (h judged oldState [x0, x1] [] [] noAnswers answer01 [1] (fun _ => [])
    judged_reads rfl answer01_change) 0
  exact absurd e (by decide)

-- A test that reads an undeclared answer breaks exactness (ReadsDeclared is needed).
def peek : Rule where
  req _ _ := []
  test _ _ A := match A 7 with
    | none => .pending
    | some _ => .noViolation

def answer7 : Answers := fun q => if q = 7 then some 0 else none

theorem undeclared_read_not_exact :
    ¬ ∀ (rule : Rule) (old : State) (c0 R Ad : List Claim) (A0 A1 : Answers) (D : List Request)
      (index : Request → List Nat),
      old.out = full rule c0 A0 → AnswerChange A0 A1 D → IndexComplete old index →
      update rule old (applyChange c0 R Ad) A1 R Ad D index = full rule (applyChange c0 R Ad) A1 := by
  intro h
  have answers : AnswerChange noAnswers answer7 [7] := by
    intro q hq
    have : q ≠ 7 := fun e => hq (by simp [e])
    simp [noAnswers, answer7, this]
  have indexed : IndexComplete ⟨full peek [x0, x1] noAnswers⟩ (fun _ => []) := by
    intro b q hq
    simp [full, bucketOut, requests, peek] at hq
  have e := congrFun (h peek ⟨full peek [x0, x1] noAnswers⟩ [x0, x1] [] [] noAnswers answer7 [7]
    (fun _ => []) rfl answers indexed) 0
  exact absurd e (by decide)

-- Function equality needs a canonical claim order: under ClaimChange alone two
-- orderings of the same claim set give different row lists. That is why
-- update_exact compares outputs as sets and update_exact_lists fixes the order.
theorem claim_order_matters :
    ¬ ∀ (rule : Rule) (c0 c1 : List Claim) (A : Answers), (∀ x, x ∈ c1 ↔ x ∈ c0) →
      full rule c0 A = full rule c1 A := by
  intro h
  have e := congrFun (h judged [x0, x1, x2] [x2, x1, x0] noAnswers
    (fun x => (List.reverse_perm [x0, x1, x2]).mem_iff)) 0
  exact absurd e (by decide)

/- Stored-state UPDATE (Algorithm 2 lines 3-17, 20-44). The stored state holds
claims_by_bucket, unplaced, outputs and buckets_by_request, as in the
pseudocode, and the step maintains all four. Lists stand for sets: only
membership matters. The rule-change branch (lines 18-19) is out of scope. -/

-- Lines 4, 7 and 12: the IDs of claims whose bucket is Unknown.
def unknownIds (cs : List Claim) : List Nat :=
  (cs.filter (fun c => c.bucket = none)).map (fun c => c.id)

structure Stored where
  claimsByBucket : Nat → List Claim
  unplaced : List Nat
  out : Nat → BucketOut
  index : Request → List Nat

-- Lines 29-39 over an explicit member list.
def outOf (rule : Rule) (ms : List Claim) (A : Answers) : BucketOut :=
  ⟨results rule ms A, requests rule ms⟩

-- Lines 6-15: discard each removed claim from its bucket, then add each added
-- claim to its bucket.
def newClaimsByBucket (st : Stored) (R Ad : List Claim) (b : Nat) : List Claim :=
  (st.claimsByBucket b).filter (fun x => x ∉ members R b) ++ members Ad b

def newUnplaced (st : Stored) (R Ad : List Claim) : List Nat :=
  st.unplaced.filter (fun i => i ∉ unknownIds R) ++ unknownIds Ad

-- Lines 22-40: an affected bucket gets the output recomputed from its new
-- members (an empty member set gives the empty output, i.e. none stored);
-- every other bucket keeps its old output.
def newOut (rule : Rule) (st : Stored) (R Ad : List Claim) (A1 : Answers) (aff : List Nat)
    (b : Nat) : BucketOut :=
  if b ∈ aff then outOf rule (newClaimsByBucket st R Ad b) A1 else st.out b

-- Lines 25-26 and 41-42: for an affected bucket, drop it from the entries of
-- its old requests, then add it to the entries of its new requests. Buckets
-- are processed independently, so the set result does not depend on order.
def newIndex (rule : Rule) (st : Stored) (R Ad : List Claim) (A1 : Answers) (aff : List Nat)
    (q : Request) : List Nat :=
  (st.index q).filter (fun b => ¬ (b ∈ aff ∧ q ∈ (st.out b).requests)) ++
    aff.filter (fun b => q ∈ (newOut rule st R Ad A1 aff b).requests)

def stepOn (rule : Rule) (st : Stored) (R Ad : List Claim) (A1 : Answers) (aff : List Nat) :
    Stored :=
  ⟨newClaimsByBucket st R Ad, newUnplaced st R Ad, newOut rule st R Ad A1 aff,
    newIndex rule st R Ad A1 aff⟩

-- Lines 5-17: the affected buckets, with arrived requests looked up in the
-- OLD stored index.
def step (rule : Rule) (st : Stored) (R Ad : List Claim) (D : List Request) (A1 : Answers) :
    Stored :=
  stepOn rule st R Ad A1 (affected R Ad D st.index)

-- The stored state is exactly the from-scratch state of claims cs and answers
-- A: same bucket members, same unplaced IDs, the full reference output in
-- every bucket, and an index that is complete and sound for that output.
structure Exact (rule : Rule) (st : Stored) (cs : List Claim) (A : Answers) : Prop where
  claims : ∀ b x, x ∈ st.claimsByBucket b ↔ x ∈ members cs b
  unplaced : ∀ i, i ∈ st.unplaced ↔ i ∈ unknownIds cs
  out : ∀ b, SameOut (st.out b) (full rule cs A b)
  complete : ∀ b q, q ∈ (st.out b).requests → b ∈ st.index q
  sound : ∀ b q, b ∈ st.index q → q ∈ (st.out b).requests

-- unplaced is a set of IDs, so discarding a removed Unknown claim's ID must not
-- drop a remaining Unknown claim with the same ID. Holds when R ⊆ c0 and IDs
-- are keys in c0 (keyed_ids_remove_unknown).
def UnknownIdsRemoved (c0 R : List Claim) : Prop :=
  ∀ x ∈ R, ∀ y ∈ c0, x.bucket = none → y.bucket = none → x.id = y.id → y ∈ R

theorem keyed_ids_remove_unknown (c0 R : List Claim) (subset : ∀ x ∈ R, x ∈ c0)
    (keyed : ∀ x ∈ c0, ∀ y ∈ c0, x.id = y.id → x = y) : UnknownIdsRemoved c0 R := by
  intro x hx y hy _ _ hid
  rw [← keyed x (subset x hx) y hy hid]
  exact hx

theorem mem_members (cs : List Claim) (b : Nat) (x : Claim) :
    x ∈ members cs b ↔ x ∈ cs ∧ x.bucket = some b := by
  simp only [members, List.mem_filter, decide_eq_true_eq]

theorem mem_unknownIds (cs : List Claim) (i : Nat) :
    i ∈ unknownIds cs ↔ ∃ c, c ∈ cs ∧ c.bucket = none ∧ c.id = i := by
  simp only [unknownIds, List.mem_map, List.mem_filter, decide_eq_true_eq, and_assoc]

theorem mem_knownBuckets (cs : List Claim) (b : Nat) :
    b ∈ knownBuckets cs ↔ ∃ c, c ∈ cs ∧ c.bucket = some b := by
  simp only [knownBuckets, List.mem_filterMap]

theorem applyChange_change (c0 R Ad : List Claim) : ClaimChange c0 R Ad (applyChange c0 R Ad) := by
  intro x
  simp only [applyChange, List.mem_append, List.mem_filter, decide_eq_true_eq]

theorem sameOut_trans {o o' o'' : BucketOut} (h : SameOut o o') (h' : SameOut o' o'') :
    SameOut o o'' :=
  ⟨fun t => (h.1 t).trans (h'.1 t), fun q => (h.2 q).trans (h'.2 q)⟩

-- Outputs computed from two member lists with the same members agree as sets.
theorem outOf_congr (rule : Rule) (ms ms' : List Claim) (A : Answers)
    (same : ∀ x, x ∈ ms ↔ x ∈ ms') : SameOut (outOf rule ms A) (outOf rule ms' A) := by
  have pmem : ∀ p, p ∈ pairs ms ↔ p ∈ pairs ms' := by
    intro p
    obtain ⟨l, r⟩ := p
    rw [mem_pairs, mem_pairs, same l, same r]
  constructor
  · intro t
    simp only [outOf, results, List.mem_map]
    constructor
    · rintro ⟨p, hp, e⟩
      exact ⟨p, (pmem p).1 hp, e⟩
    · rintro ⟨p, hp, e⟩
      exact ⟨p, (pmem p).2 hp, e⟩
  · intro q
    simp only [outOf, requests, List.mem_flatMap]
    constructor
    · rintro ⟨p, hp, e⟩
      exact ⟨p, (pmem p).1 hp, e⟩
    · rintro ⟨p, hp, e⟩
      exact ⟨p, (pmem p).2 hp, e⟩

-- Lines 6-15 keep claims_by_bucket equal to the members of the new claims.
theorem newClaims_exact (st : Stored) (c0 R Ad c1 : List Claim)
    (claims : ∀ b x, x ∈ st.claimsByBucket b ↔ x ∈ members c0 b)
    (change : ClaimChange c0 R Ad c1) :
    ∀ b x, x ∈ newClaimsByBucket st R Ad b ↔ x ∈ members c1 b := by
  intro b x
  simp only [newClaimsByBucket, List.mem_append, List.mem_filter, decide_eq_true_eq, claims,
    mem_members]
  rw [change x]
  constructor
  · rintro (⟨⟨h0, hb⟩, hR⟩ | ⟨hA, hb⟩)
    · exact ⟨Or.inl ⟨h0, fun r => hR ⟨r, hb⟩⟩, hb⟩
    · exact ⟨Or.inr hA, hb⟩
  · rintro ⟨⟨h0, hR⟩ | hA, hb⟩
    · exact Or.inl ⟨⟨h0, hb⟩, fun r => hR r.1⟩
    · exact Or.inr ⟨hA, hb⟩

-- Lines 7 and 12 keep unplaced equal to the IDs of the new Unknown claims.
theorem newUnplaced_exact (st : Stored) (c0 R Ad c1 : List Claim)
    (unplaced : ∀ i, i ∈ st.unplaced ↔ i ∈ unknownIds c0)
    (change : ClaimChange c0 R Ad c1) (ids : UnknownIdsRemoved c0 R) :
    ∀ i, i ∈ newUnplaced st R Ad ↔ i ∈ unknownIds c1 := by
  intro i
  simp only [newUnplaced, List.mem_append, List.mem_filter, decide_eq_true_eq, unplaced,
    mem_unknownIds]
  constructor
  · rintro (⟨⟨y, hy, hn, hid⟩, notR⟩ | ⟨y, hy, hn, hid⟩)
    · exact ⟨y, (change y).2 (Or.inl ⟨hy, fun r => notR ⟨y, r, hn, hid⟩⟩), hn, hid⟩
    · exact ⟨y, (change y).2 (Or.inr hy), hn, hid⟩
  · rintro ⟨y, hy, hn, hid⟩
    rcases (change y).1 hy with ⟨h0, hR⟩ | hA
    · refine Or.inl ⟨⟨y, h0, hn, hid⟩, ?_⟩
      rintro ⟨x, hx, hxn, hxid⟩
      exact hR (ids x hx y h0 hxn hn (hxid.trans hid.symm))
    · exact Or.inr ⟨y, hA, hn, hid⟩

theorem mem_newIndex (rule : Rule) (st : Stored) (R Ad : List Claim) (A1 : Answers)
    (aff : List Nat) (q : Request) (b : Nat) :
    b ∈ newIndex rule st R Ad A1 aff q ↔
      (b ∈ st.index q ∧ ¬ (b ∈ aff ∧ q ∈ (st.out b).requests)) ∨
        (b ∈ aff ∧ q ∈ (newOut rule st R Ad A1 aff b).requests) := by
  simp only [newIndex, List.mem_append, List.mem_filter, decide_eq_true_eq]

-- Index maintenance for any affected list: completeness is kept outright, and
-- soundness is kept when the old index was sound.
theorem newIndex_complete (rule : Rule) (st : Stored) (R Ad : List Claim) (A1 : Answers)
    (aff : List Nat) (complete : ∀ b q, q ∈ (st.out b).requests → b ∈ st.index q) :
    ∀ b q, q ∈ (newOut rule st R Ad A1 aff b).requests → b ∈ newIndex rule st R Ad A1 aff q := by
  intro b q hq
  rw [mem_newIndex]
  by_cases h : b ∈ aff
  · exact Or.inr ⟨h, hq⟩
  · have same : newOut rule st R Ad A1 aff b = st.out b := by
      unfold newOut; split
      · contradiction
      · rfl
    rw [same] at hq
    exact Or.inl ⟨complete b q hq, fun hb => h hb.1⟩

theorem newIndex_sound (rule : Rule) (st : Stored) (R Ad : List Claim) (A1 : Answers)
    (aff : List Nat) (sound : ∀ b q, b ∈ st.index q → q ∈ (st.out b).requests) :
    ∀ b q, b ∈ newIndex rule st R Ad A1 aff q → q ∈ (newOut rule st R Ad A1 aff b).requests := by
  intro b q hb
  rcases (mem_newIndex rule st R Ad A1 aff q b).1 hb with ⟨hi, keep⟩ | ⟨_, hq⟩
  · have old := sound b q hi
    have h : b ∉ aff := fun ha => keep ⟨ha, old⟩
    have same : newOut rule st R Ad A1 aff b = st.out b := by
      unfold newOut; split
      · contradiction
      · rfl
    rw [same]
    exact old
  · exact hq

-- UPDATE preserves exactness of the whole stored state, its own indexes
-- included, so one run's output is a valid input to the next.
theorem update_step (rule : Rule) (reads : ReadsDeclared rule) (st : Stored)
    (c0 R Ad c1 : List Claim) (A0 A1 : Answers) (D : List Request)
    (exact : Exact rule st c0 A0) (change : ClaimChange c0 R Ad c1)
    (answers : AnswerChange A0 A1 D) (ids : UnknownIdsRemoved c0 R) :
    Exact rule (step rule st R Ad D A1) c1 A1 := by
  have claims1 := newClaims_exact st c0 R Ad c1 exact.claims change
  refine ⟨claims1, newUnplaced_exact st c0 R Ad c1 exact.unplaced change ids, ?_,
    newIndex_complete rule st R Ad A1 _ exact.complete,
    newIndex_sound rule st R Ad A1 _ exact.sound⟩
  intro b
  show SameOut (newOut rule st R Ad A1 (affected R Ad D st.index) b) (full rule c1 A1 b)
  unfold newOut
  split
  · exact outOf_congr rule _ (members c1 b) A1 (claims1 b)
  · rename_i outside
    have indexed : IndexComplete ⟨full rule c0 A0⟩ st.index :=
      fun b' q hq => exact.complete b' q (((exact.out b').2 q).2 hq)
    have h := update_exact rule reads ⟨full rule c0 A0⟩ c0 R Ad c1 A0 A1 D st.index rfl change
      answers indexed b
    unfold update refresh at h
    split at h
    · contradiction
    · exact sameOut_trans (exact.out b) h

-- The from-scratch state of claims cs and answers A.
def build (rule : Rule) (cs : List Claim) (A : Answers) : Stored :=
  ⟨members cs, unknownIds cs, full rule cs A,
    fun q => (knownBuckets cs).filter (fun b => q ∈ (full rule cs A b).requests)⟩

theorem build_exact (rule : Rule) (cs : List Claim) (A : Answers) :
    Exact rule (build rule cs A) cs A := by
  refine ⟨fun _ _ => Iff.rfl, fun _ => Iff.rfl, fun _ => ⟨fun _ => Iff.rfl, fun _ => Iff.rfl⟩,
    ?_, ?_⟩
  · intro b q hq
    show b ∈ (knownBuckets cs).filter (fun b => q ∈ (full rule cs A b).requests)
    rw [List.mem_filter, decide_eq_true_eq]
    refine ⟨?_, hq⟩
    have hq' : q ∈ requests rule (members cs b) := hq
    obtain ⟨⟨l, r⟩, hp, _⟩ := List.mem_flatMap.1 hq'
    have hl := (mem_members cs b l).1 ((mem_pairs _ l r).1 hp).1
    exact (mem_knownBuckets cs b).2 ⟨l, hl.1, hl.2⟩
  · intro b q hb
    have hb' : b ∈ (knownBuckets cs).filter (fun b => q ∈ (full rule cs A b).requests) := hb
    rw [List.mem_filter, decide_eq_true_eq] at hb'
    exact hb'.2

-- A run of consecutive changes: claims removed and added, requests arrived,
-- and the new answer snapshot.
structure Change where
  removed : List Claim
  added : List Claim
  arrived : List Request
  answers : Answers

def runSteps (rule : Rule) : Stored → List Change → Stored
  | st, [] => st
  | st, ch :: rest => runSteps rule (step rule st ch.removed ch.added ch.arrived ch.answers) rest

def claimsAfter : List Claim → List Change → List Claim
  | cs, [] => cs
  | cs, ch :: rest => claimsAfter (applyChange cs ch.removed ch.added) rest

def answersAfter : Answers → List Change → Answers
  | A, [] => A
  | _, ch :: rest => answersAfter ch.answers rest

-- Each change lists every changed answer, and removed Unknown IDs are not
-- shared by a remaining Unknown claim.
def ChainOK : List Claim → Answers → List Change → Prop
  | _, _, [] => True
  | cs, A, ch :: rest => AnswerChange A ch.answers ch.arrived ∧ UnknownIdsRemoved cs ch.removed ∧
      ChainOK (applyChange cs ch.removed ch.added) ch.answers rest

-- Any number of consecutive UPDATE runs keeps the stored state exact.
theorem update_runs (rule : Rule) (reads : ReadsDeclared rule) (changes : List Change) :
    ∀ (st : Stored) (cs : List Claim) (A : Answers), Exact rule st cs A → ChainOK cs A changes →
      Exact rule (runSteps rule st changes) (claimsAfter cs changes) (answersAfter A changes) := by
  induction changes with
  | nil => intro st cs A exact _; exact exact
  | cons ch rest ih =>
    intro st cs A exact ok
    exact ih _ _ _ (update_step rule reads st cs ch.removed ch.added _ A ch.answers ch.arrived exact
      (applyChange_change cs ch.removed ch.added) ok.1 ok.2.1) ok.2.2

-- Starting from a from-scratch build, every run's outputs equal the full
-- reference check of the current claims and answers.
theorem update_runs_from_build (rule : Rule) (reads : ReadsDeclared rule) (cs : List Claim)
    (A : Answers) (changes : List Change) (ok : ChainOK cs A changes) :
    ∀ b, SameOut ((runSteps rule (build rule cs A) changes).out b)
      (full rule (claimsAfter cs changes) (answersAfter A changes) b) :=
  (update_runs rule reads changes _ cs A (build_exact rule cs A) ok).out

/- Counterexamples for the stored-state step. -/

-- The variant that omits the buckets of added claims.
def stepNoAdded (rule : Rule) (st : Stored) (R Ad : List Claim) (D : List Request) (A1 : Answers) :
    Stored :=
  stepOn rule st R Ad A1 (knownBuckets R ++ D.flatMap st.index)

-- Omitting the added claims' buckets is not exact: claim 1 joins claim 0 in
-- bucket 0 and the new pair (0, 1) never appears.
theorem added_bucket_needed :
    ¬ ∀ (rule : Rule) (st : Stored) (c0 R Ad c1 : List Claim) (A0 A1 : Answers) (D : List Request),
      ReadsDeclared rule → Exact rule st c0 A0 → ClaimChange c0 R Ad c1 → AnswerChange A0 A1 D →
      UnknownIdsRemoved c0 R → Exact rule (stepNoAdded rule st R Ad D A1) c1 A1 := by
  intro h
  have e := h judged (build judged [x0] noAnswers) [x0] [] [x1] (applyChange [x0] [] [x1])
    noAnswers noAnswers [] judged_reads (build_exact judged [x0] noAnswers)
    (applyChange_change [x0] [] [x1]) (fun _ _ => rfl) (fun _ hx => absurd hx List.not_mem_nil)
  have m := ((e.out 0).1 (0, 1, .pending)).2 (by decide)
  exact absurd m (by decide)

def u5a : Claim := ⟨5, none, 0⟩
def u5b : Claim := ⟨5, none, 1⟩

-- UnknownIdsRemoved is needed: two Unknown claims share ID 5, one is removed,
-- and the set unplaced loses ID 5 although the other claim remains.
theorem unplaced_needs_unknown_ids :
    ¬ ∀ (rule : Rule) (st : Stored) (c0 R Ad c1 : List Claim) (A0 A1 : Answers) (D : List Request),
      ReadsDeclared rule → Exact rule st c0 A0 → ClaimChange c0 R Ad c1 → AnswerChange A0 A1 D →
      Exact rule (step rule st R Ad D A1) c1 A1 := by
  intro h
  have e := h judged (build judged [u5a, u5b] noAnswers) [u5a, u5b] [u5a] []
    (applyChange [u5a, u5b] [u5a] []) noAnswers noAnswers [] judged_reads
    (build_exact judged [u5a, u5b] noAnswers) (applyChange_change [u5a, u5b] [u5a] [])
    (fun _ _ => rfl)
  have m := (e.unplaced 5).2 (by decide)
  exact absurd m (by decide)

-- Remark: a rule change re-checks every bucket (full pass) and is not modelled.
#print axioms members_empty
#print axioms mem_affected
#print axioms mem_pairs
#print axioms members_unchanged_outside
#print axioms members_apply_outside
#print axioms stable_answers
#print axioms bucketOut_congr
#print axioms update_exact
#print axioms update_exact_lists
#print axioms members_append
#print axioms members_unplaced
#print axioms knownBuckets_unplaced
#print axioms unplaced_never_affects
#print axioms unknown_move_affects_one
#print axioms move_refreshes_both
#print axioms cost_local
#print axioms pairs_le_square
#print axioms sum_map_le
#print axioms update_tests_le_cost
#print axioms judged_reads
#print axioms old_index_complete
#print axioms answer01_change
#print axioms ignoring_answers_not_exact
#print axioms new_bucket_only_not_exact
#print axioms incomplete_index_not_exact
#print axioms undeclared_read_not_exact
#print axioms claim_order_matters
#print axioms keyed_ids_remove_unknown
#print axioms mem_members
#print axioms mem_unknownIds
#print axioms mem_knownBuckets
#print axioms applyChange_change
#print axioms sameOut_trans
#print axioms outOf_congr
#print axioms newClaims_exact
#print axioms newUnplaced_exact
#print axioms mem_newIndex
#print axioms newIndex_complete
#print axioms newIndex_sound
#print axioms update_step
#print axioms build_exact
#print axioms update_runs
#print axioms update_runs_from_build
#print axioms added_bucket_needed
#print axioms unplaced_needs_unknown_ids
end ConcordPairs
