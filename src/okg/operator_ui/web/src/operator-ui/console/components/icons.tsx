// The console's nine glyphs, drawn here rather than imported.
//
// They were @ant-design/icons, which cost about 18 kB of minified JavaScript
// in the console chunk -- the icon component plus the whole icons-svg path
// table -- for eight marks. Each one below is a 24-unit SVG sized in `em`, so
// every `font-size` rule that sized the Ant Design icons still sizes these,
// and each carries `okg-icon` for the few rules that reached `.anticon`.
//
// Rule 2 (colour never carries status alone) is unchanged: these are drawn in
// `currentColor` beside a word, never alone.
import type { ReactElement, SVGProps } from "react";

type IconProps = { className?: string } & Pick<SVGProps<SVGSVGElement>, "aria-hidden">;

function Glyph({
  className = "", children, ...rest
}: IconProps & { children: ReactElement | ReactElement[] }): ReactElement {
  return (
    <svg
      className={`okg-icon ${className}`.trim()}
      viewBox="0 0 24 24"
      width="1em"
      height="1em"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      focusable="false"
      role="img"
      {...rest}
    >
      {children}
    </svg>
  );
}

/** A filled disc with the mark cut out of it. The cut is painted in the
 *  surface colour rather than punched out of the path: a hole needs opposite
 *  winding, which is easy to get wrong by hand and impossible to see in a
 *  unit test. `--okg-console-icon-cut` lets a surface that is not the page
 *  background (a dark chip) restate its own colour. */
const CUT = "var(--okg-console-icon-cut, var(--okg-color-surface, #fff))";

function Disc({ className = "", mark, ...rest }: IconProps & { mark: string }): ReactElement {
  return (
    <svg
      className={`okg-icon ${className}`.trim()}
      viewBox="0 0 24 24"
      width="1em"
      height="1em"
      focusable="false"
      role="img"
      {...rest}
    >
      <circle cx="12" cy="12" r="10" fill="currentColor" />
      <path d={mark} fill="none" stroke={CUT} strokeWidth={2.2} strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export function CheckCircleFilled(props: IconProps): ReactElement {
  return <Disc mark="M7.6 12.2 10.7 15.3 16.4 9.2" {...props} />;
}

export function CloseCircleFilled(props: IconProps): ReactElement {
  return <Disc mark="M8.6 8.6 15.4 15.4 M15.4 8.6 8.6 15.4" {...props} />;
}

export function ExclamationCircleFilled(props: IconProps): ReactElement {
  // The bar and the dot are two subpaths of one stroke: with a round cap a
  // 0.1-unit segment draws the dot, so the glyph needs no second element and
  // no fill rule. The cut is painted in the surface colour, like the others.
  return <Disc mark="M12 6.6 12 13.2 M12 16.7 12 16.8" {...props} />;
}

export function MinusCircleOutlined(props: IconProps): ReactElement {
  return (
    <Glyph {...props}>
      <circle cx="12" cy="12" r="9" />
      <path d="M8 12h8" />
    </Glyph>
  );
}

export function StopOutlined(props: IconProps): ReactElement {
  return (
    <Glyph {...props}>
      <circle cx="12" cy="12" r="9" />
      <path d="M5.9 5.9l12.2 12.2" />
    </Glyph>
  );
}

export function SyncOutlined(props: IconProps): ReactElement {
  return (
    <Glyph {...props}>
      <path d="M20 12a8 8 0 0 1-13.3 6" />
      <path d="M4 12a8 8 0 0 1 13.3-6" />
      <path d="M17.3 2.5V6h-3.5" />
      <path d="M6.7 21.5V18h3.5" />
    </Glyph>
  );
}

export function CopyOutlined(props: IconProps): ReactElement {
  return (
    <Glyph {...props}>
      <rect x="9" y="9" width="12" height="12" rx="2" />
      <path d="M15 5.5A2.5 2.5 0 0 0 12.5 3H5.5A2.5 2.5 0 0 0 3 5.5v7A2.5 2.5 0 0 0 5.5 15" />
    </Glyph>
  );
}

export function SearchOutlined(props: IconProps): ReactElement {
  return (
    <Glyph {...props}>
      <circle cx="10.5" cy="10.5" r="7" />
      <path d="M15.6 15.6 21 21" />
    </Glyph>
  );
}

export function ExportOutlined(props: IconProps): ReactElement {
  return (
    <Glyph {...props}>
      <path d="M14 4h6v6" />
      <path d="M20 4 11 13" />
      <path d="M18 14.5V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4.5" />
    </Glyph>
  );
}
