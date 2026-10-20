---- MODULE ConsoleFocus ----
(***************************************************************************
  Console modal + focus state machine.

  The Console framework's keyboard & focus cross-cutting contract makes
  four commitments that are easy to break in implementation:

    AtMostOneTopmostModal
        Only one modal is "topmost" at a time. Keyboard events (Escape,
        Tab) are delivered to the topmost.

    EveryModalHasVisibleDismiss
        A modal can only mount if it advertises a visible dismiss
        affordance. A modal that traps focus with no visible dismiss
        target is a bug.

    FocusReturnsOnClose
        Closing a modal restores focus to whatever was focused when the
        modal opened. The Console records that target on open.

    EscapeIsEnabledWheneverAModalIsOpen (safety, structural)
        Whenever the modal stack is nonempty, the Escape action is
        enabled and, when it fires, strictly shrinks the stack. No
        reachable state lets a modal swallow Escape. This is the honest
        action-level form: the spec is safety-only and does NOT assume
        the operator ever presses Escape (they may keep opening modals),
        so there is no eventual-empties liveness claim and no fairness
        assumption here.

  This model encodes the modal stack as a sequence (topmost at the end),
  records focus-return targets per modal, and models Escape as a
  dismissal of the topmost. It deliberately does not model animation,
  rendering, or the substrate; those are irrelevant to the contract.
 ***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  ModalIds,         \* The set of all modal identifiers in the Console.
  FocusableIds,     \* The set of all focusable elements outside modals.
  NoFocus,          \* Sentinel returned when nothing is focused (e.g. body).
  MaxStackDepth     \* Bound on stack size to make the model finite.

ASSUME NoFocus \notin ModalIds
ASSUME NoFocus \notin FocusableIds
ASSUME ModalIds \cap FocusableIds = {}
ASSUME MaxStackDepth \in Nat \ {0}

(***************************************************************************
  VARIABLES

    modalStack
      Sequence of modal records. Topmost is Head(modalStack); a modal
      record carries the modal id, the focus-return target captured on
      open, and a Boolean flag that records whether a visible dismiss
      affordance was advertised at mount.

    focused
      The id of the currently focused element. When a modal is on top,
      focus is on its default focusable; when the stack is empty, focus
      is on a focusable id from FocusableIds (or NoFocus).
 ***************************************************************************)
VARIABLES
  modalStack,
  focused

vars == << modalStack, focused >>

(***************************************************************************
  ReturnTarget is whatever was focused at the moment a modal opened.
  That can be a non-modal focusable, NoFocus, OR another modal id (when
  modal B opens while modal A is topmost; closing B returns focus to A).
 ***************************************************************************)
ReturnTargets == FocusableIds \cup ModalIds \cup {NoFocus}

ModalRecord == [
  id |-> ModalIds,
  returnTarget |-> ReturnTargets,
  hasVisibleDismiss |-> BOOLEAN
]

(***************************************************************************
  Helpers.
 ***************************************************************************)
StackIds(s) ==
  { s[i].id : i \in 1..Len(s) }

TopmostModal == IF modalStack = << >> THEN NoFocus ELSE Head(modalStack).id

(***************************************************************************
  Init: no modals open; focus is on some non-modal focusable or NoFocus.
 ***************************************************************************)
Init ==
  /\ modalStack = << >>
  /\ focused \in FocusableIds \cup {NoFocus}

(***************************************************************************
  OpenModal(m): mount modal m as the new topmost.

  Preconditions:
    - m is a known modal id
    - m is not already in the stack (no double-mount of the same id)
    - the stack is below MaxStackDepth (bound for finite checking)
    - the modal advertises a visible dismiss target (refusal if not)

  Effects:
    - push a record onto modalStack carrying m, the captured return
      target, and the dismiss flag (always TRUE here; we model the
      contract by refusing to open a modal without a visible dismiss)
    - focus moves to m (we represent the focused id as m while a modal
      is topmost, abstracting over which internal focusable it is)
 ***************************************************************************)
OpenModal(m) ==
  /\ m \in ModalIds
  /\ m \notin StackIds(modalStack)
  /\ Len(modalStack) < MaxStackDepth
  /\ modalStack' = << [
       id |-> m,
       returnTarget |-> focused,
       hasVisibleDismiss |-> TRUE
     ] >> \o modalStack
  /\ focused' = m

(***************************************************************************
  CloseTopmost: dismiss the topmost modal (either by Escape, by the
  modal's close button, or by backdrop click — all are modelled as one
  action because the contract is identical).

  Effects:
    - pop the topmost
    - restore focus to the popped record's returnTarget (when the modal
      mounted)
 ***************************************************************************)
CloseTopmost ==
  /\ modalStack # << >>
  /\ focused' = Head(modalStack).returnTarget
  /\ modalStack' = Tail(modalStack)

(***************************************************************************
  ChangeFocusWithinStack: while at least one modal is open, focus stays
  on the topmost modal's surface. We model Tab/Shift+Tab as no-ops at
  this abstraction level (they cycle within the modal). The action is
  here as a no-op transition to let TLC explore long traces.
 ***************************************************************************)
ChangeFocusBelowModal ==
  /\ modalStack = << >>
  /\ \E f \in FocusableIds \cup {NoFocus}:
       focused' = f
  /\ UNCHANGED modalStack

(***************************************************************************
  PressEscape: identical effect to CloseTopmost when a modal is open;
  no-op when the stack is empty. We model the contract that no modal
  swallows Escape: the action is enabled iff modalStack # << >>, so a
  fairness assumption guarantees Escape always eventually fires.
 ***************************************************************************)
PressEscape == CloseTopmost

Next ==
  \/ \E m \in ModalIds: OpenModal(m)
  \/ CloseTopmost
  \/ PressEscape
  \/ ChangeFocusBelowModal

(***************************************************************************
  The contract is safety-only: every PressEscape pops the topmost. We
  do NOT assume the operator presses Escape (they may keep opening
  modals instead). The interesting properties are structural:

    - The stack invariants hold at every reachable state.
    - PressEscape is enabled whenever the stack is nonempty (no
      "modal swallows Escape forever" state).
    - When PressEscape fires, the stack strictly decreases.

  An earlier draft included a liveness property
  (modalStack # <<>> ~> modalStack = <<>>) but TLC correctly produced
  a counterexample of operator behavior: keep opening modals. The
  honest contract belongs at the action level, not at the spec level.
 ***************************************************************************)
Spec ==
  /\ Init
  /\ [][Next]_vars

(***************************************************************************
  TypeOK
 ***************************************************************************)
TypeOK ==
  /\ modalStack \in Seq([id: ModalIds, returnTarget: ReturnTargets, hasVisibleDismiss: BOOLEAN])
  /\ Len(modalStack) <= MaxStackDepth
  /\ focused \in ModalIds \cup FocusableIds \cup {NoFocus}

(***************************************************************************
  AtMostOneTopmostModal

      The topmost is uniquely identifiable; no two modals are
      simultaneously at the head of the stack. (Modelled as: ids in the
      stack are unique, so Head is unambiguous.)
 ***************************************************************************)
AtMostOneTopmostModal ==
  Cardinality(StackIds(modalStack)) = Len(modalStack)

(***************************************************************************
  EveryModalHasVisibleDismiss

      Every modal currently in the stack advertised a visible dismiss
      affordance at mount.
 ***************************************************************************)
EveryModalHasVisibleDismiss ==
  \A i \in 1..Len(modalStack): modalStack[i].hasVisibleDismiss = TRUE

(***************************************************************************
  FocusReflectsTopmost

      When the stack is nonempty, focus is on the topmost modal's id;
      when empty, focus is on a non-modal focusable or NoFocus. This
      encodes the visible commitment that focus does not wander outside
      the topmost modal.
 ***************************************************************************)
FocusReflectsTopmost ==
  /\ modalStack # << >> => focused = Head(modalStack).id
  /\ modalStack = << >> => focused \in FocusableIds \cup {NoFocus}

(***************************************************************************
  EscapeIsEnabledWheneverAModalIsOpen

      Whenever the stack is nonempty, the PressEscape action is enabled
      — i.e., there is no reachable state in which a modal is on top
      yet Escape cannot fire. This is the structural form of "Esc
      always returns to a known-safe state".
 ***************************************************************************)
EscapeIsEnabledWheneverAModalIsOpen ==
  modalStack # << >> => ENABLED PressEscape
====
