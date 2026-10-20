// The three copy affordances, rule 15: an identifier's icon, a block's "Copy",
// a page's "Copy as text".
//
// All three are one control in three sizes. Each is a real button (focusable,
// Enter copies), carries `data-copy="<what>"` and the accessible name
// "Copy <what>" (design/audit-rules.mjs holds both; an identifier's icon
// adds the value, "Copy version id efc517cac053"), says "copied" in place
// for COPIED_MS after a successful write, and says "not copied" -- with the
// error in its title -- when the clipboard refused. A capture never shows
// "copied": nothing in capture.mjs clicks, and the audit reds a view where the
// state persists.
//
// Visual restraint is the whole design: a 12 px icon at 40 per cent that
// comes up on hover, or 13 px link-colour text; nothing boxed, nothing that moves
// the text beside it.
import { useEffect, useRef, useState, type ReactElement, type ReactNode } from "react";
import { CopyOutlined } from "@operator-ui/console/components/icons";

import { copy, COPIED_MS } from "@operator-ui/console/components/copy";
import { looksLikeVersionId, shortVersion } from "@operator-ui/console/model/shared/ids";

export type CopyState =
  | { kind: "idle" }
  | { kind: "copied" }
  | { kind: "failed"; message: string };

/** The state one control is in, and the function that copies. */
export function useCopy(): { state: CopyState; trigger: (text: string) => void } {
  const [state, setState] = useState<CopyState>({ kind: "idle" });
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (timer.current) clearTimeout(timer.current); }, []);
  const trigger = (text: string) => {
    copy(text).then(
      () => {
        setState({ kind: "copied" });
        if (timer.current) clearTimeout(timer.current);
        timer.current = setTimeout(() => setState({ kind: "idle" }), COPIED_MS);
      },
      (reason: unknown) => {
        setState({
          kind: "failed",
          message: reason instanceof Error ? reason.message : String(reason),
        });
      },
    );
  };
  return { state, trigger };
}

/** The value an icon's accessible name carries, so a list of copy buttons
 *  does not read as ten identical "Copy version id" controls (audit B10).
 *  A version id is named by its 12-hex tail, the form the page prints; any
 *  other value up to 64 characters whole; a longer one by its first 24
 *  characters and an ellipsis. The button still copies the whole value. */
export function copyNameValue(text: string): string {
  if (looksLikeVersionId(text)) return shortVersion(text) ?? text;
  return text.length <= 64 ? text : `${text.slice(0, 24)}\u2026`;
}

function stateAttributes(state: CopyState, what: string, text: string, name = `Copy ${what}`) {
  return {
    "data-copy": what,
    "data-copied": state.kind === "copied" ? "true" : "false",
    "aria-label": name,
    title: state.kind === "failed" ? state.message : `Copy ${text.length > 80 ? what : text}`,
  };
}

/**
 * An identifier in monospace with its copy icon.
 *
 * `text` is what the clipboard gets -- the FULL identifier, never the 12-hex
 * short form -- and the children are what the page shows; without children
 * the text itself is shown. `reveal="row"` hides the icon until the table row
 * or list row it sits in is hovered (`.okg-console__copyrow`); headers and
 * detail lines use the default, always visible at 40 per cent.
 */
export function CopyableId({
  text, what, reveal = "always", codeClassName = "", children,
}: {
  text: string;
  what: string;
  reveal?: "always" | "row";
  codeClassName?: string;
  children?: ReactNode;
}): ReactElement {
  const { state, trigger } = useCopy();
  return (
    <span className="okg-console__copyable" data-reveal={reveal}>
      {/* `title`: the id wraps at 390 px rather than running off the edge, so
          a reader who wants it in one unbroken piece hovers for it. The
          button beside it still copies the WHOLE id, wrapped or not. */}
      {children ?? (
        <code className={`okg-console__id ${codeClassName}`.trim()} title={text}>
          {text}
        </code>
      )}
      <button
        type="button"
        className="okg-console__copy okg-console__copy--icon"
        data-state={state.kind}
        onClick={(event) => { event.stopPropagation(); trigger(text); }}
        {...stateAttributes(state, what, text, `Copy ${what} ${copyNameValue(text)}`)}
      >
        {state.kind === "copied" ? <span className="okg-console__copy-said">copied</span> : null}
        {state.kind === "failed" ? <span className="okg-console__copy-said">not copied</span> : null}
        {state.kind === "idle" ? <CopyOutlined className="okg-console__copy-glyph" aria-hidden="true" /> : null}
      </button>
      <CopyAnnouncement state={state} what={what} />
    </span>
  );
}

/** A polite live region saying what was copied, or that it was not. The
 *  word in the button changes too, but a screen reader does not announce a
 *  button's changed text, so "copied" went unheard (audit 2026-09-23). */
function CopyAnnouncement({ state, what }: { state: CopyState; what: string }): ReactElement {
  return (
    <span className="okg-console__sr" role="status" aria-live="polite" data-testid="copy-announcement">
      {state.kind === "copied" ? `Copied ${what}` : state.kind === "failed" ? `${what} not copied` : ""}
    </span>
  );
}

/**
 * A 13 px link-colour text control: "Copy" at a block's top right, "Copy as text"
 * at the far right of a page's h1 row. `text` may be a function, so a page's
 * Markdown is built when it is asked for and not on every render.
 */
export function CopyTextButton({
  text, what, label = "Copy", className = "",
}: {
  text: string | (() => string);
  what: string;
  label?: string;
  className?: string;
}): ReactElement {
  const { state, trigger } = useCopy();
  const resolve = () => (typeof text === "function" ? text() : text);
  return (
    <>
      <button
        type="button"
        className={`okg-console__copy okg-console__copy--text ${className}`.trim()}
        data-state={state.kind}
        onClick={(event) => { event.stopPropagation(); trigger(resolve()); }}
        {...stateAttributes(state, what, what)}
      >
        {state.kind === "copied" ? "copied" : state.kind === "failed" ? "not copied" : label}
      </button>
      <CopyAnnouncement state={state} what={what} />
    </>
  );
}
