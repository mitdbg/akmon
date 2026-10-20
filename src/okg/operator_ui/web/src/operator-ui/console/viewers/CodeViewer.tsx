// The code viewer: shiki (MIT) with its JavaScript "raw" engine and the
// precompiled grammars, which is the smallest way to run shiki -- no WebAssembly
// and no regex translation at runtime; each grammar is its own lazy chunk.
// Colours come from a CSS-variables theme, so the token colours are the
// console's tokens (styles/explore.css sets --shiki-* from --okg-color-*), not a
// bundled palette. Line numbers are drawn beside every line; a language shiki
// does not know renders as plain lines with numbers, and says so.
import { useEffect, useState, type ReactElement } from "react";
import { createCssVariablesTheme, createHighlighterCore, type HighlighterCore, type ThemedToken } from "shiki/core";
import { createJavaScriptRawEngine } from "shiki/engine/javascript";

import { fetchText, reasonOf, type OnViewerState } from "@operator-ui/console/viewers/viewerState";

type Grammar = () => Promise<{ default: unknown }>;

/** The grammars this console ships, each loaded when first asked for. */
const GRAMMARS: Record<string, Grammar> = {
  rust: () => import("@shikijs/langs-precompiled/rust"),
  python: () => import("@shikijs/langs-precompiled/python"),
  typescript: () => import("@shikijs/langs-precompiled/typescript"),
  javascript: () => import("@shikijs/langs-precompiled/javascript"),
  json: () => import("@shikijs/langs-precompiled/json"),
  yaml: () => import("@shikijs/langs-precompiled/yaml"),
  toml: () => import("@shikijs/langs-precompiled/toml"),
  html: () => import("@shikijs/langs-precompiled/html"),
  bash: () => import("@shikijs/langs-precompiled/bash"),
  csv: () => import("@shikijs/langs-precompiled/csv"),
  markdown: () => import("@shikijs/langs-precompiled/markdown"),
};

export const THEME_NAME = "okg-console";

let highlighterPromise: Promise<HighlighterCore> | null = null;

function highlighter(): Promise<HighlighterCore> {
  if (!highlighterPromise) {
    highlighterPromise = createHighlighterCore({
      themes: [createCssVariablesTheme({ name: THEME_NAME, variablePrefix: "--shiki-", fontStyle: true })],
      langs: [],
      engine: createJavaScriptRawEngine(),
    });
  }
  return highlighterPromise;
}

type Line = { tokens: { text: string; style: Record<string, string> }[] };

/** The text as lines of styled tokens; plain lines when there is no
 *  grammar for the language. */
async function tokenize(text: string, language: string | null): Promise<{ lines: Line[]; highlighted: boolean }> {
  // Own property only: "constructor" or "toString" as a language name would
  // otherwise find an inherited function and fail the viewer instead of
  // falling back to plain lines.
  const grammar = language && Object.hasOwn(GRAMMARS, language) ? GRAMMARS[language] : undefined;
  if (!grammar) {
    return { lines: text.split("\n").map((line) => ({ tokens: [{ text: line, style: {} }] })), highlighted: false };
  }
  const core = await highlighter();
  if (!core.getLoadedLanguages().includes(language as string)) {
    const loaded = await grammar();
    await core.loadLanguage(loaded.default as never);
  }
  const tokens = core.codeToTokensBase(text, { lang: language as string, theme: THEME_NAME });
  const styled = (token: ThemedToken) => {
    const style: Record<string, string> = {};
    if (token.color) style.color = token.color;
    if (token.fontStyle && token.fontStyle & 1) style.fontStyle = "italic";
    if (token.fontStyle && token.fontStyle & 2) style.fontWeight = "600";
    return { text: token.content, style };
  };
  return { lines: tokens.map((line) => ({ tokens: line.map(styled) })), highlighted: true };
}

export default function CodeViewer({
  url, language, onState,
}: { url: string; language: string | null; onState: OnViewerState }): ReactElement | null {
  const [lines, setLines] = useState<Line[] | null>(null);
  useEffect(() => {
    let cancelled = false;
    setLines(null);
    onState({ kind: "pending" });
    fetchText(url)
      .then((text) => tokenize(text.replace(/\n$/, ""), language))
      .then(
        (result) => {
          if (cancelled) return;
          setLines(result.lines);
          onState({
            kind: "ready",
            note: result.highlighted ? `${language} highlighting` : (language ? `no grammar for ${language}; plain lines` : "plain text"),
          });
        },
        (error: unknown) => { if (!cancelled) onState({ kind: "failed", reason: reasonOf(error) }); },
      );
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onState is stable for one url
  }, [url, language]);
  if (lines === null) return null;
  const width = String(lines.length).length;
  return (
    <pre className="okg-console__code" data-testid="explore-content-code" data-language={language ?? "plain"}>
      {lines.map((line, index) => (
        <div className="okg-console__code-line" key={index}>
          <span className="okg-console__code-number" aria-hidden="true">{String(index + 1).padStart(width, " ")}</span>
          <span className="okg-console__code-text">
            {line.tokens.map((token, at) => <span key={at} style={token.style}>{token.text}</span>)}
            {line.tokens.length === 0 || (line.tokens.length === 1 && line.tokens[0].text === "") ? "\n" : ""}
          </span>
        </div>
      ))}
    </pre>
  );
}
