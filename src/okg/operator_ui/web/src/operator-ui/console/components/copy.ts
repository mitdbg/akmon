// The one clipboard write the console makes.
//
// Rule 15 (requirements v2 section 3): "easy to copy information throughout
// the UI so we can pass it to our coding agents." Every copy control -- an
// identifier's icon, a block's "Copy", a page's "Copy as text" -- goes through
// `copy()`, and a test replaces the writer with `setClipboardWriter` so the
// text that would have reached the clipboard is asserted rather than assumed.
//
// A page with no clipboard API (an insecure origin, an old browser, a server
// render) gets an error by name, not a silent no-op: a control that says
// "copied" over an empty clipboard is the exact defect rule 13 forbids.

export type ClipboardWriter = (text: string) => Promise<void>;

/** How long a control says "copied" before it returns to its icon. */
export const COPIED_MS = 1500;

let writer: ClipboardWriter | null = null;

/** Replace the clipboard for a test, or pass null to restore the browser's. */
export function setClipboardWriter(next: ClipboardWriter | null): void {
  writer = next;
}

export async function copy(text: string): Promise<void> {
  if (writer) {
    await writer(text);
    return;
  }
  const clipboard = typeof navigator === "undefined" ? undefined : navigator.clipboard;
  if (!clipboard || typeof clipboard.writeText !== "function") {
    throw new Error(
      "The clipboard is not available on this page (navigator.clipboard is missing, which "
      + "happens off HTTPS and off localhost). Nothing was copied.",
    );
  }
  await clipboard.writeText(text);
}
