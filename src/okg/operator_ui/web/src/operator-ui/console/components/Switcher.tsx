// A small exclusive choice: panes, direction, span.
//
// This was Ant Design's `Segmented`, whose only addition over a row of
// buttons was a sliding ink block, and which is one of the components that
// pulled `antd` into the console chunk. The options are buttons in a group;
// the one in force says so with `aria-pressed` and a solid background, and
// Tab reaches each one.
import type { ReactElement } from "react";

export function Switcher<T extends string>({
  className = "", label, value, options, onChange,
}: {
  className?: string;
  /** The group's accessible name ("Panes", "Direction", "Span"). */
  label: string;
  value: T;
  options: readonly { value: T; label: string }[];
  onChange: (next: T) => void;
}): ReactElement {
  return (
    <div className={`okg-console__switcher ${className}`.trim()} role="group" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          className="okg-console__switch"
          aria-pressed={option.value === value}
          data-checked={option.value === value ? "true" : "false"}
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
