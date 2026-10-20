// One section of a console page: a 14/600 title with an optional count and
// aside, its content, and a hairline before the next. No box.
import type { Severity } from "@operator-ui/console/model/shared/status";
import type { ReactNode } from "react";

/**
 * One section. No box.
 *
 * The critique of pass 5: a tinted canvas with near-white bordered cards is six
 * rectangles outlined against a blue-grey field, and github-01 and vercel-02
 * have no cards on these pages at all. A section is a 14/600 title, its
 * content, and a hairline before the next one.
 */
export function Section({
  title, testid, aside, count = null, countSeverity = null, children,
}: {
  title: string;
  testid: string;
  aside?: ReactNode;
  /** Printed after the title as "· N". Carries the colour of the WORST state
   *  it counts: red for a failure, amber for a producer that has not run yet.
   *  A count of three sources waiting to run is not a count of three
   *  problems. With no severity the count is neutral: it is not assumed to
   *  be a failure. */
  count?: string | null;
  countSeverity?: Severity | null;
  children: ReactNode;
}) {
  return (
    <section className="okg-console__section" data-testid={testid}>
      <div className="okg-console__section-head">
        <h2 className="okg-console__section-title">
          {title}
          {count ? (
            <>
              {" \u00b7 "}
              <span
                className="okg-console__section-count"
                data-severity={countSeverity ?? undefined}
              >
                {count}
              </span>
            </>
          ) : null}
        </h2>
        {aside ?? null}
      </div>
      {children}
    </section>
  );
}
