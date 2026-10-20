// A foldable JSON tree in the console's mono type. Written here rather than
// taken from a package: the candidates (react-json-view-lite, MIT, 2.5 kB)
// bring their own colour classes and a second type scale, and a tree that
// folds is sixty lines. Objects and arrays fold with a triangle and print
// their size while folded; scalars print as JSON. Depth past `openDepth`
// starts folded, so a 256 KB payload is not a 10,000-row DOM on first paint.
import { useState, type ReactElement } from "react";

function isContainer(value: unknown): value is Record<string, unknown> | unknown[] {
  return typeof value === "object" && value !== null;
}

function sizeLabel(value: Record<string, unknown> | unknown[]): string {
  const count = Array.isArray(value) ? value.length : Object.keys(value).length;
  const unit = Array.isArray(value) ? "item" : "key";
  return `${count} ${count === 1 ? unit : `${unit}s`}`;
}

function Node({
  name, value, depth, openDepth, last,
}: { name: string | null; value: unknown; depth: number; openDepth: number; last: boolean }): ReactElement {
  const [open, setOpen] = useState(depth < openDepth);
  const comma = last ? "" : ",";
  const label = name !== null ? <span className="okg-console__json-key">{JSON.stringify(name)}: </span> : null;
  if (!isContainer(value)) {
    return (
      <div className="okg-console__json-row" style={{ paddingLeft: depth * 16 }}>
        {label}
        <span className={`okg-console__json-${value === null ? "null" : typeof value}`}>{JSON.stringify(value)}</span>
        {comma}
      </div>
    );
  }
  const entries: [string | null, unknown][] = Array.isArray(value)
    ? value.map((item) => [null, item])
    : Object.entries(value);
  const [openMark, closeMark] = Array.isArray(value) ? ["[", "]"] : ["{", "}"];
  return (
    <div className="okg-console__json-node" data-open={open ? "true" : "false"}>
      <div className="okg-console__json-row" style={{ paddingLeft: depth * 16 }}>
        <button
          type="button"
          className="okg-console__json-fold"
          aria-expanded={open}
          aria-label={`${open ? "Fold" : "Unfold"} ${name ?? "value"}`}
          onClick={() => setOpen((was) => !was)}
        >
          {open ? "▾" : "▸"}
        </button>
        {label}
        {openMark}
        {open ? "" : <span className="okg-console__json-size"> {sizeLabel(value)} </span>}
        {open ? "" : `${closeMark}${comma}`}
      </div>
      {open ? (
        <>
          {entries.map(([key, item], index) => (
            <Node key={key ?? index} name={key} value={item} depth={depth + 1} openDepth={openDepth} last={index === entries.length - 1} />
          ))}
          <div className="okg-console__json-row" style={{ paddingLeft: depth * 16 }}>{closeMark}{comma}</div>
        </>
      ) : null}
    </div>
  );
}

export function JsonTree({ value, openDepth = 2, testid }: { value: unknown; openDepth?: number; testid?: string }): ReactElement {
  return (
    <div className="okg-console__jsontree" data-testid={testid}>
      <Node name={null} value={value} depth={0} openDepth={openDepth} last />
    </div>
  );
}
