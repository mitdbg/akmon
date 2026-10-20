// Identifiers inside running text: a version id set in mono, and a long
// registry name given places to wrap.
import type { ReactElement, ReactNode } from "react";

/** "version " and the 12-hex id after it, in a sentence the model built as
 *  one string. */
const VERSION_IN_TEXT = /(version )([0-9a-f]{12})(?![0-9a-z])/g;

/**
 * A sentence with every "version <12 hex>" id set in mono, the way the page
 * prints ids everywhere else (audit B21). The words stay in the body font and
 * the text is unchanged: "Copy as text" still carries the model's string.
 */
export function VersionIds({ text }: { text: string }): ReactElement {
  const parts: ReactNode[] = [];
  let last = 0;
  for (const match of text.matchAll(VERSION_IN_TEXT)) {
    const at = (match.index ?? 0) + match[1].length;
    parts.push(text.slice(last, at));
    parts.push(<span key={at} className="okg-console__version-id">{match[2]}</span>);
    last = at + match[2].length;
  }
  parts.push(text.slice(last));
  return <>{parts}</>;
}

/**
 * A registry name with a line-break opportunity after each "_", "." and "-",
 * so at 390 "cmssw_releases" wraps as "cmssw_" / "releases" rather than at an
 * arbitrary letter (audit B16). `<wbr>` adds no character: the text, the copy
 * and the title are unchanged.
 */
export function Breakable({ text }: { text: string }): ReactElement {
  const parts = text.split(/(?<=[_.-])/);
  return (
    <>
      {parts.map((part, index) => (index === 0 ? part : [<wbr key={`b${index}`} />, part]))}
    </>
  );
}
