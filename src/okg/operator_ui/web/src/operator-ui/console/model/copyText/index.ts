// Each page as plain Markdown, for "Copy as text".
//
// Rule 15: "Every page carries a 'Copy as text' control in its header that
// copies the page's facts as plain Markdown." One function per page, over the
// same model the page renders from, so the clipboard and the screen cannot
// disagree. The shape is fixed: an h1 with the deployment and the page, the
// version being read and the as-of time, then each section as a heading with
// its rows as a table or a list. Identifiers are in backticks, recorded errors
// and YAML in fenced blocks, and there is no decorative text -- what a coding
// agent needs is the facts, not the prose around them.
//
// copyText.test.ts holds golden strings for archiv3-small.

export { type PageContext, table } from "@operator-ui/console/model/copyText/shared";
export { homeText } from "@operator-ui/console/model/copyText/home";
export { attemptText, updatesText } from "@operator-ui/console/model/copyText/updates";
export { producerText, sourcesText } from "@operator-ui/console/model/copyText/sources";
export { exploreText, recordText } from "@operator-ui/console/model/copyText/explore";
