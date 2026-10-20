// A count chip that is also the filter on the table under it (rule 1).
//
// This was Ant Design's `Tag.CheckableTag`, which rendered a `<span>`: not
// focusable, not operable from the keyboard, and it carried the whole
// `antd` + `@rc-component` stack into the console chunk. It is a real button
// now -- Tab reaches it, Enter and Space toggle it -- and it says whether it
// is on with `aria-pressed`, not with a tint alone.
import type { ReactElement, ReactNode } from "react";

export function Chip({
  className = "", checked, onToggle, severity, label, children,
}: {
  className?: string;
  checked: boolean;
  onToggle: () => void;
  /** Drawn on the chip as `data-severity`, which the sheets colour. */
  severity?: string;
  /** The accessible name, when the visible text is not enough on its own. */
  label?: string;
  children: ReactNode;
}): ReactElement {
  return (
    <button
      type="button"
      className={`okg-console__chip ${className}`.trim()}
      aria-pressed={checked}
      aria-label={label}
      data-checked={checked ? "true" : "false"}
      {...(severity === undefined ? {} : { "data-severity": severity })}
      onClick={onToggle}
    >
      {children}
    </button>
  );
}
