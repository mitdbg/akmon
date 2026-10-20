// Text as Explore prints it: a policy-hidden value as the word, a match
// marked inside its text, and a code block with its copy control.
import { CopyTextButton } from "@operator-ui/console/components/CopyControls";
import { highlight } from "@operator-ui/console/model/explore/query";
import { splitMaskedText } from "@operator-ui/console/api/searchData";
import type { ReactElement, ReactNode } from "react";

// --- shared pieces --------------------------------------------------------------

/** Text with policy-hidden values shown as the word, never as a token. */
export function Masked({ value }: { value: string }): ReactElement {
  return (
    <>
      {splitMaskedText(value).map((part, index) => (part.kind === "hidden"
        ? <span key={index} className="okg-console__hidden">hidden</span>
        : <span key={index}>{part.value}</span>))}
    </>
  );
}

export function Marked({ text, query }: { text: string; query: string }): ReactElement {
  return (
    <>
      {highlight(text, query).map((part, index) => (part.match
        ? <mark key={index} className="okg-console__mark">{part.text}</mark>
        : <span key={index}>{part.text}</span>))}
    </>
  );
}

/** A mono block with its Copy control and a 13 px line naming it. */
export function CodeBlock({
  title, text, copyWhat, testid, aside,
}: { title: string; text: string; copyWhat: string; testid: string; aside?: ReactNode }): ReactElement {
  return (
    <div className="okg-console__block" data-testid={testid}>
      <div className="okg-console__block-head">
        <span className="okg-console__block-title">{title}</span>
        <span className="okg-console__section-aside">
          {aside}
          <CopyTextButton what={copyWhat} text={text} />
        </span>
      </div>
      <pre className="okg-console__verbatim">{text}</pre>
    </div>
  );
}
