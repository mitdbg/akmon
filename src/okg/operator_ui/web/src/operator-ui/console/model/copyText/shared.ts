// The pieces every page's copy text is built from: the heading, sections,
// Markdown tables and fences.
import { utcStamp } from "@operator-ui/console/model/shared/time";

/** What every page header states. */
export interface PageContext {
  deployment: string;
  /** The full id of the version every view reads, or null before a publish. */
  version: string | null;
  /** When it was published, or null. */
  publishedAt: string | null;
  /** The clock the page was rendered against. */
  nowMs: number;
}

// --- shared pieces --------------------------------------------------------

function cell(text: string): string {
  return text.replace(/\r?\n/g, " ").replace(/\|/g, "\\|").trim();
}

export function code(text: string | null): string {
  return text ? `\`${text}\`` : "";
}

/** A Markdown table. Rows with fewer cells than the header are padded. */
export function table(headers: string[], rows: string[][]): string {
  const line = (cells: string[]) => `| ${headers.map((_, i) => cell(cells[i] ?? "")).join(" | ")} |`;
  return [
    line(headers),
    `|${headers.map(() => "---").join("|")}|`,
    ...rows.map(line),
  ].join("\n");
}

export function fence(text: string, language = ""): string {
  // A block that itself contains a fence gets a longer one.
  const longest = Math.max(2, ...[...text.matchAll(/`+/g)].map((match) => match[0].length));
  const marks = "`".repeat(longest + 1);
  return `${marks}${language}\n${text.replace(/\n$/, "")}\n${marks}`;
}

export function heading(page: string, context: PageContext): string[] {
  const version = context.version
    ? `Version read: ${code(context.version)}`
      + (context.publishedAt ? `, published ${utcStamp(context.publishedAt)}` : "")
    : "Version read: none published";
  return [
    `# ${context.deployment} · ${page}`,
    "",
    version,
    `As of ${utcStamp(new Date(context.nowMs).toISOString())}`,
  ];
}

export function section(title: string, body: string[], level = 2): string[] {
  return ["", `${"#".repeat(level)} ${title}`, "", ...body];
}

export function outcomeCell(row: { outcome: { word: string } }, stoppedOn = ""): string {
  return stoppedOn ? `${row.outcome.word} ${stoppedOn}` : row.outcome.word;
}
