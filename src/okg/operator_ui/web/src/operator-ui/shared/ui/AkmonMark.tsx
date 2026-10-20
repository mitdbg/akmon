// The AKMON mark, drawn in the console's top bar. It is what is left of the
// old operator shell's component library (OperatorShell, TopStatus and the
// rest), which PACT console-default-remove-old-views deleted with the Home and
// Health pages that used it.
/**
 * The AKMON logomark — an ancient block anvil (Greek *ákmon* = anvil, the tool
 * of Hephaestus). A period-correct silhouette: a heavy overhanging face on a
 * splayed stump, not the horned 18th-century London pattern. Fills with
 * `currentColor` so it takes the surrounding mark's ink; decorative (the brand
 * element carries the label).
 */
export function AkmonMark({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 48 48"
      className={className}
      aria-hidden="true"
      focusable="false"
    >
      <path
        fill="currentColor"
        d="M7.5 9 L40.5 9 L40.5 12.8 L36.5 12.8 L36.5 21 L31 21 L31 31 L34.5 36.5 L13.5 36.5 L17 31 L17 21 L11.5 21 L11.5 12.8 L7.5 12.8 Z"
      />
    </svg>
  );
}
