#!/usr/bin/env python3
"""Guard active docs against dead repository and documentation references."""

from __future__ import annotations

import posixpath
import re
import string
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable, NamedTuple
from urllib.parse import unquote_to_bytes, urlsplit

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
HISTORY_PREFIXES: tuple[str, ...] = ()

# Repo top-level dirs whose paths we can resolve on disk.
KNOWN_TOP_DIRS = (
    "src/",
    "pact/",
    "scripts/",
    "tests/",
    "deployments/",
    "docs/",
    "docs/runbooks/",
    ".claude/",
    ".agents/",
    ".github/",
    "bench/",
    "formal/",
)
PLACEHOLDER = re.compile(r"[<>{}$*]|\.\.\.|…|\bN+\b|\$\{|Y{4}|\bMM\b|\bDD\b|MM-DD|YYYY")
INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
RFC_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
BACKTICK = re.compile(r"`([^`\n]+)`")
FENCE_OPEN = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
LIST_PREFIX = re.compile(r"^( {0,3})([-+*]|\d{1,9}[.)])([ \t]+)(.*)$")
ATX_HEADING = re.compile(r"^ {0,3}#{1,6}(?:[ \t]+|$)")
THEMATIC_BREAK = re.compile(
    r"^ {0,3}(?:\*[ \t]*){3,}$|^ {0,3}(?:-[ \t]*){3,}$|^ {0,3}(?:_[ \t]*){3,}$"
)
ESCAPABLE = frozenset(string.punctuation)


class MarkdownDestination(NamedTuple):
    """One extracted Markdown destination before URL normalization."""

    raw: str
    target: str
    image: bool


class MarkdownSyntaxIssue(NamedTuple):
    """A local-looking Markdown destination that the bounded parser rejected."""

    raw: str
    detail: str


class TargetResult(NamedTuple):
    """The final, non-fallback classification and outcome for one destination."""

    classification: str
    ok: bool
    detail: str = ""


class ReferenceIssue(NamedTuple):
    """A source-located reference diagnostic."""

    kind: str
    source: str
    line: int
    target: str
    detail: str


class ListPrefix(NamedTuple):
    body: str
    content_indent: int


class MarkdownBlockState:
    """Track bounded CommonMark paragraph, container, and code-block state."""

    def __init__(self) -> None:
        self.fence_char: str | None = None
        self.fence_size = 0
        self.fence_quote_depth = 0
        self.fence_list_indent: int | None = None
        self.indented = False
        self.paragraph_open = False
        self.quote_depth = 0
        self.list_indent: int | None = None
        self.line_issues: list[MarkdownSyntaxIssue] = []

    def _close_fence(self) -> None:
        self.fence_char = None
        self.fence_size = 0
        self.fence_list_indent = None
        self.paragraph_open = False

    def _fence_container_ended(self, content: str, quote_depth: int) -> bool:
        """Has the blockquote or list item that opened this fence closed?

        A fence lives inside its container, so the container ending ends the
        fence with it. Without this the fence stays open to end of file and
        every later line is silently treated as code -- a whole-file blind spot
        that would hide real dead links rather than report them.
        """
        if quote_depth < self.fence_quote_depth:
            return True
        if self.fence_list_indent is None or quote_depth != self.fence_quote_depth:
            return False
        # A blank line does not end a list item; a later line indented less
        # than the item body does.
        return (
            bool(content.strip()) and _leading_indent(content) < self.fence_list_indent
        )

    def visible_line(self, line: str) -> str | None:
        self.line_issues = []
        content, quote_depth = _strip_blockquote_prefixes(line)
        if self.fence_char is not None and self._fence_container_ended(
            content, quote_depth
        ):
            self._close_fence()
            self.indented = False
            self.list_indent = None
        if self.fence_char is not None:
            candidate = content
            if quote_depth == self.fence_quote_depth and self.list_indent is not None:
                indent = _leading_indent(candidate)
                if indent >= self.list_indent:
                    candidate = _strip_indent(candidate, self.list_indent)
            stripped = candidate.lstrip(" ")
            indent = len(candidate) - len(stripped)
            run = len(stripped) - len(stripped.lstrip(self.fence_char))
            if (
                quote_depth == self.fence_quote_depth
                and indent <= 3
                and run >= self.fence_size
                and not stripped[run:].strip()
            ):
                self._close_fence()
            return None

        if quote_depth != self.quote_depth:
            self.indented = False
            self.paragraph_open = False
            self.list_indent = None
            self.quote_depth = quote_depth

        if not content.strip():
            self.paragraph_open = False
            return None

        in_list_body = False
        prefix = _list_prefix(content)
        if prefix is not None:
            in_list_body = True
            self.list_indent = prefix.content_indent
            content = prefix.body
            self.indented = False
            self.paragraph_open = False
            while nested := _list_prefix(content):
                self.list_indent += nested.content_indent
                content = nested.body
        elif self.list_indent is not None:
            indent = _leading_indent(content)
            if indent >= self.list_indent:
                in_list_body = True
                content = _strip_indent(content, self.list_indent)
                nested = _list_prefix(content)
                if nested is not None:
                    self.list_indent += nested.content_indent
                    content = nested.body
                    self.paragraph_open = False
            elif not self.paragraph_open:
                self.list_indent = None

        ambiguous = _ambiguous_list_body(content)
        if ambiguous is not None and _local_looking(ambiguous):
            self.line_issues.append(
                MarkdownSyntaxIssue(
                    ambiguous.strip(), "unsupported ambiguous list prefix"
                )
            )
            return None

        opening = FENCE_OPEN.match(content)
        if opening:
            fence = opening.group(1)
            info = opening.group(2)
            if fence[0] != "`" or "`" not in info:
                self.fence_char = fence[0]
                self.fence_size = len(fence)
                self.fence_quote_depth = quote_depth
                # Only bind the fence to a list item when this line really sits
                # in that item body; a lazily continued line that is not
                # indented to it would otherwise close the fence at once.
                self.fence_list_indent = self.list_indent if in_list_body else None
                self.indented = False
                self.paragraph_open = False
                return None

        indented = _leading_indent(content) >= 4
        if self.indented:
            if indented:
                return None
            self.indented = False
        if indented and not self.paragraph_open:
            self.indented = True
            return None
        self.paragraph_open = _opens_paragraph(content)
        return _mask_inline_code(content)


def _strip_blockquote_prefixes(line: str) -> tuple[str, int]:
    cursor = 0
    depth = 0
    while cursor < len(line):
        probe = cursor
        spaces = 0
        while probe < len(line) and line[probe] == " " and spaces < 3:
            probe += 1
            spaces += 1
        if probe >= len(line) or line[probe] != ">":
            break
        depth += 1
        cursor = probe + 1
        if cursor < len(line) and line[cursor] in " \t":
            cursor += 1
    return line[cursor:], depth


def _leading_indent(text: str) -> int:
    width = 0
    for char in text:
        if char == " ":
            width += 1
        elif char == "\t":
            width += 4 - width % 4
        else:
            break
    return width


def _strip_indent(text: str, columns: int) -> str:
    width = 0
    index = 0
    while index < len(text) and width < columns and text[index] in " \t":
        if text[index] == " ":
            width += 1
        else:
            width += 4 - width % 4
        index += 1
    return (" " * max(0, width - columns)) + text[index:]


def _list_prefix(text: str) -> ListPrefix | None:
    match = LIST_PREFIX.match(text)
    if not match:
        return None
    spacing = _leading_indent(match.group(3))
    if not 1 <= spacing <= 4:
        return None
    content_indent = len(match.group(1)) + len(match.group(2)) + spacing
    return ListPrefix(match.group(4), content_indent)


def _ambiguous_list_body(text: str) -> str | None:
    match = LIST_PREFIX.match(text)
    if match and _leading_indent(match.group(3)) > 4:
        return match.group(4)
    return None


def _opens_paragraph(text: str) -> bool:
    if not text.strip():
        return False
    if ATX_HEADING.match(text) or THEMATIC_BREAK.match(text):
        return False
    return True


def _active_doc_paths() -> list[Path]:
    paths = [ROOT / "README.md"]
    paths += sorted(DOCS.rglob("*.md"))
    paths += [
        DOCS / "reference" / "error_codes.yaml",
        DOCS / "reference" / "alerts.yaml",
    ]
    out = []
    for path in paths:
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith(HISTORY_PREFIXES):
            continue
        if path.exists():
            out.append(path)
    return out


def _markdown_unescape(value: str) -> str:
    out: list[str] = []
    index = 0
    while index < len(value):
        if (
            value[index] == "\\"
            and index + 1 < len(value)
            and value[index + 1] in ESCAPABLE
        ):
            out.append(value[index + 1])
            index += 2
            continue
        out.append(value[index])
        index += 1
    return "".join(out)


def _read_title(text: str, index: int, *, outer_closer: bool) -> tuple[int, str | None]:
    while index < len(text) and text[index].isspace():
        index += 1
    if outer_closer and index < len(text) and text[index] == ")":
        return index + 1, None
    if not outer_closer and index == len(text):
        return index, None
    if index >= len(text) or text[index] not in "\"'(":
        return index, "expected a quoted title or the end of the link"

    opener = text[index]
    closer = {'"': '"', "'": "'", "(": ")"}[opener]
    depth = 1
    index += 1
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            index += 2
            continue
        if opener == "(" and char == "(":
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                index += 1
                break
        index += 1
    else:
        return index, "unterminated link title"

    while index < len(text) and text[index].isspace():
        index += 1
    if outer_closer:
        if index >= len(text) or text[index] != ")":
            return index, "expected the closing link parenthesis after the title"
        return index + 1, None
    if index != len(text):
        return index, "unexpected content after the reference-definition title"
    return index, None


def _parse_destination(
    text: str,
    index: int,
    *,
    outer_closer: bool,
) -> tuple[str, str, int, str | None]:
    """Parse one bounded CommonMark destination and optional title."""
    while index < len(text) and text[index].isspace():
        index += 1

    if index < len(text) and text[index] == "<":
        index += 1
        raw_start = index
        while index < len(text):
            if text[index] == "\\" and index + 1 < len(text):
                index += 2
                continue
            if text[index] == ">":
                break
            if text[index] == "<":
                return (
                    text[raw_start:index],
                    "",
                    index,
                    "unexpected '<' in angle destination",
                )
            index += 1
        if index >= len(text):
            return text[raw_start:], "", index, "unterminated angle destination"
        raw = text[raw_start:index]
        target = _markdown_unescape(raw)
        index += 1
    else:
        raw_start = index
        target_chars: list[str] = []
        depth = 0
        while index < len(text):
            char = text[index]
            if char == "\\" and index + 1 < len(text):
                following = text[index + 1]
                if following in ESCAPABLE:
                    target_chars.append(following)
                else:
                    target_chars.extend((char, following))
                index += 2
                continue
            if char == "(":
                depth += 1
                if depth > 32:
                    return (
                        text[raw_start:index],
                        "",
                        index,
                        "destination nesting exceeds 32 levels",
                    )
                target_chars.append(char)
                index += 1
                continue
            if char == ")":
                if depth:
                    depth -= 1
                    target_chars.append(char)
                    index += 1
                    continue
                if outer_closer:
                    break
                return (
                    text[raw_start:index],
                    "",
                    index,
                    "unmatched ')' in reference destination",
                )
            if char.isspace() and depth == 0:
                break
            target_chars.append(char)
            index += 1
        if depth:
            return (
                text[raw_start:index],
                "",
                index,
                "unbalanced parentheses in destination",
            )
        raw = text[raw_start:index]
        target = "".join(target_chars)

    end, error = _read_title(text, index, outer_closer=outer_closer)
    return raw, target, end, error


def _local_looking(fragment: str) -> bool:
    candidate = fragment.lstrip()
    if candidate.startswith("<"):
        candidate = candidate[1:]
    candidate = _markdown_unescape(candidate)
    if candidate.startswith(("/", "./", "../", "docs/", *KNOWN_TOP_DIRS)):
        return True
    first = candidate.split(maxsplit=1)[0].rstrip(")>") if candidate else ""
    return first.endswith(".md") or (
        "/" in first and "." not in PurePosixPath(first).name
    )


def _escaped(text: str, index: int) -> bool:
    backslashes = 0
    index -= 1
    while index >= 0 and text[index] == "\\":
        backslashes += 1
        index -= 1
    return backslashes % 2 == 1


def _mask_inline_code(line: str) -> str:
    """Mask complete backtick code spans without changing character offsets."""
    masked = list(line)
    cursor = 0
    while cursor < len(line):
        opening = line.find("`", cursor)
        if opening < 0:
            break
        width = 1
        while opening + width < len(line) and line[opening + width] == "`":
            width += 1

        search = opening + width
        closing = -1
        while search < len(line):
            candidate = line.find("`", search)
            if candidate < 0:
                break
            candidate_width = 1
            while (
                candidate + candidate_width < len(line)
                and line[candidate + candidate_width] == "`"
            ):
                candidate_width += 1
            if candidate_width == width:
                closing = candidate
                break
            search = candidate + candidate_width
        if closing < 0:
            cursor = opening + width
            continue
        for index in range(opening, closing + width):
            masked[index] = " "
        cursor = closing + width
    return "".join(masked)


def _find_label_close(line: str, opening: int) -> int | None:
    depth = 1
    index = opening + 1
    while index < len(line):
        if line[index] == "\\" and index + 1 < len(line):
            index += 2
            continue
        if line[index] == "[":
            depth += 1
        elif line[index] == "]":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def _unbalanced_label_issue(line: str, opening: int) -> MarkdownSyntaxIssue | None:
    destination = re.search(r"\]\s*\((.*)$", line[opening + 1 :])
    if destination and _local_looking(destination.group(1)):
        return MarkdownSyntaxIssue(
            destination.group(1).strip(), "unbalanced link label brackets"
        )
    return None


def _extract_markdown_destinations(
    line: str,
    *,
    inline_code_masked: bool = False,
) -> tuple[list[MarkdownDestination], list[MarkdownSyntaxIssue]]:
    """Extract bounded Markdown destinations from non-code text."""
    if not inline_code_masked:
        line = _mask_inline_code(line)
    destinations: list[MarkdownDestination] = []
    issues: list[MarkdownSyntaxIssue] = []
    cursor = 0

    while cursor < len(line):
        opening = line.find("[", cursor)
        if opening < 0:
            break
        if _escaped(line, opening):
            cursor = opening + 1
            continue
        closing = _find_label_close(line, opening)
        if closing is None:
            issue = _unbalanced_label_issue(line, opening)
            if issue:
                issues.append(issue)
            break

        after = closing + 1
        is_definition = (
            not line[:opening].strip() and after < len(line) and line[after] == ":"
        )
        if is_definition:
            fragment = line[after + 1 :]
            raw, target, _, error = _parse_destination(
                line, after + 1, outer_closer=False
            )
            if error:
                if _local_looking(fragment):
                    issues.append(MarkdownSyntaxIssue(fragment.strip(), error))
            else:
                destinations.append(MarkdownDestination(raw, target, False))
            break

        if after < len(line) and line[after] == "(":
            fragment = line[after + 1 :]
            raw, target, end, error = _parse_destination(
                line, after + 1, outer_closer=True
            )
            if error:
                if _local_looking(fragment):
                    issues.append(MarkdownSyntaxIssue(fragment.strip(), error))
                cursor = after + 1
            else:
                image = (
                    opening > 0
                    and line[opening - 1] == "!"
                    and not _escaped(line, opening - 1)
                )
                destinations.append(MarkdownDestination(raw, target, image))
                cursor = max(end, after + 1)
            continue

        spaced = after
        while spaced < len(line) and line[spaced].isspace():
            spaced += 1
        if spaced > after and spaced < len(line) and line[spaced] == "(":
            fragment = line[spaced + 1 :]
            if _local_looking(fragment):
                issues.append(
                    MarkdownSyntaxIssue(
                        fragment.strip(),
                        "whitespace between the link label and destination",
                    )
                )
        cursor = closing + 1
    return destinations, issues


def _candidate_tokens(line: str) -> list[str]:
    destinations, _ = _extract_markdown_destinations(line)
    return BACKTICK.findall(line) + [item.target for item in destinations]


def _strip_suffix(token: str) -> str:
    # Drop pytest ::selector, URL query/fragment, and :line suffixes.
    token = token.split("::", 1)[0]
    try:
        token = urlsplit(token).path
    except ValueError:
        # A token urlsplit cannot parse (an unmatched bracket, say) has no
        # query or fragment to drop, so keep it whole and let the remaining
        # suffix rules and the later existence check judge it.
        pass
    token = re.sub(r":\d+(?:-\d+)?$", "", token)
    return token.strip().rstrip("/")


def _looks_like_path(token: str) -> bool:
    if "://" in token or token.startswith(("http", "mailto:")):
        return False
    if PLACEHOLDER.search(token):
        return False
    if "/" not in token:
        return False
    return token.startswith(KNOWN_TOP_DIRS)


def _path_exists(token: str, source: Path = ROOT) -> bool:
    return (ROOT / token).exists() or (source.parent / token).exists()


def _decode_local_path(path: str) -> tuple[str | None, str | None]:
    if INVALID_PERCENT_ESCAPE.search(path):
        return None, "malformed percent escape"
    try:
        decoded = unquote_to_bytes(path).decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None, "percent escapes are not valid UTF-8"
    if "\x00" in decoded:
        return None, "decoded path contains NUL"
    if "\\" in decoded:
        return None, "decoded path contains a backslash"
    return decoded, None


def _known_root_path(path: str) -> bool:
    return path.startswith(KNOWN_TOP_DIRS)


def _page_like(path: str) -> bool:
    trimmed = path.rstrip("/")
    if not trimmed:
        return True
    suffix = PurePosixPath(trimmed).suffix
    return suffix in ("", ".md")


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _resolve_doc_page(
    decoded_path: str,
    source: Path,
    classification: str,
    *,
    docs_root: Path = DOCS,
) -> TargetResult:
    resolved_docs = docs_root.resolve(strict=True)
    if classification == "site-absolute":
        relative = decoded_path[1:]
    elif classification == "readme-docs":
        relative = decoded_path.removeprefix("docs/")
    else:
        try:
            source_parent = source.parent.relative_to(docs_root).as_posix()
        except ValueError:
            return TargetResult(classification, False, "source page is outside docs")
        relative = posixpath.join(source_parent, decoded_path)

    normalized = posixpath.normpath(relative)
    if normalized == ".":
        normalized = ""
    if (
        posixpath.isabs(normalized)
        or normalized == ".."
        or normalized.startswith("../")
    ):
        return TargetResult(classification, False, "path escapes docs")

    if normalized.endswith(".md"):
        candidates = [docs_root / PurePosixPath(normalized)]
    elif normalized:
        candidates = [
            docs_root / PurePosixPath(f"{normalized}.md"),
            docs_root / PurePosixPath(normalized) / "index.md",
        ]
    else:
        candidates = [docs_root / "index.md"]

    for candidate in candidates:
        resolved_candidate = candidate.resolve(strict=False)
        if not _inside(resolved_candidate, resolved_docs):
            return TargetResult(classification, False, "resolved path escapes docs")
        try:
            existing = candidate.resolve(strict=True)
        except (FileNotFoundError, NotADirectoryError, OSError):
            continue
        if not _inside(existing, resolved_docs):
            return TargetResult(classification, False, "resolved symlink escapes docs")
        if existing.is_file():
            return TargetResult(classification, True)
    return TargetResult(classification, False, "documentation page does not exist")


def _validate_markdown_destination(
    destination: MarkdownDestination,
    source: Path,
    *,
    docs_root: Path = DOCS,
    root: Path = ROOT,
) -> TargetResult:
    target = destination.target
    if not target:
        return TargetResult("ignored", True)
    if target.startswith("//") or RFC_SCHEME.match(target):
        return TargetResult("ignored", True)
    try:
        parts = urlsplit(target)
    except ValueError as exc:
        return TargetResult("local-invalid", False, f"invalid URL target: {exc}")
    if parts.scheme:
        return TargetResult("ignored", True)
    if not parts.path and (parts.fragment or parts.query):
        return TargetResult("ignored", True)
    # Judge the placeholder skip on the path alone. A query or fragment is not
    # part of the page identity, so a placeholder there must not decide whether
    # the page itself is checked.
    if PLACEHOLDER.search(parts.path):
        return TargetResult("ignored", True)

    decoded, error = _decode_local_path(parts.path)
    if error:
        return TargetResult("local-invalid", False, error)
    assert decoded is not None
    known_root = _known_root_path(decoded)
    page_like = _page_like(decoded)

    if destination.image or not page_like:
        if known_root:
            exists = (root / decoded).exists()
            return TargetResult(
                "repository", exists, "" if exists else "repository path does not exist"
            )
        return TargetResult("ignored", True)

    if decoded.startswith("/"):
        return _resolve_doc_page(decoded, source, "site-absolute", docs_root=docs_root)

    try:
        source.relative_to(docs_root)
        source_is_doc = source.suffix == ".md"
    except ValueError:
        source_is_doc = False
    if source_is_doc:
        return _resolve_doc_page(
            decoded, source, "source-relative", docs_root=docs_root
        )

    if source.resolve(strict=False) == (root / "README.md").resolve(
        strict=False
    ) and decoded.startswith("docs/"):
        return _resolve_doc_page(decoded, source, "readme-docs", docs_root=docs_root)

    if known_root:
        exists = (root / decoded).exists()
        return TargetResult(
            "repository", exists, "" if exists else "repository path does not exist"
        )
    return TargetResult("ignored", True)


def find_reference_issues(
    paths: Iterable[Path] | None = None,
    *,
    docs_root: Path = DOCS,
    root: Path = ROOT,
) -> list[ReferenceIssue]:
    """Return every source-located dead, invalid, or unsupported reference."""
    found: list[ReferenceIssue] = []
    active_paths = list(paths) if paths is not None else _active_doc_paths()
    for path in active_paths:
        markdown_state = MarkdownBlockState()
        try:
            rel = path.relative_to(root).as_posix()
        except ValueError:
            rel = path.as_posix()
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), 1
        ):
            for raw in BACKTICK.findall(line):
                token = _strip_suffix(raw)
                if token and _looks_like_path(token) and not _path_exists(token, path):
                    found.append(
                        ReferenceIssue(
                            "repository",
                            rel,
                            line_number,
                            token,
                            "repository path does not exist",
                        )
                    )

            if path.suffix != ".md":
                continue
            visible = markdown_state.visible_line(line)
            for issue in markdown_state.line_issues:
                found.append(
                    ReferenceIssue("syntax", rel, line_number, issue.raw, issue.detail)
                )
            if visible is None:
                continue
            destinations, syntax_issues = _extract_markdown_destinations(
                visible, inline_code_masked=True
            )
            for issue in syntax_issues:
                found.append(
                    ReferenceIssue("syntax", rel, line_number, issue.raw, issue.detail)
                )
            for destination in destinations:
                result = _validate_markdown_destination(
                    destination, path, docs_root=docs_root, root=root
                )
                if not result.ok:
                    found.append(
                        ReferenceIssue(
                            result.classification,
                            rel,
                            line_number,
                            destination.raw,
                            result.detail,
                        )
                    )
    return found


def find_dead_refs() -> list[tuple[str, int, str]]:
    """Compatibility view of all reference issues as source/line/target tuples."""
    return [
        (issue.source, issue.line, issue.target) for issue in find_reference_issues()
    ]


def run_checks() -> list[str]:
    errors = []
    for issue in find_reference_issues():
        if issue.kind == "repository":
            errors.append(
                f"dead doc reference in {issue.source}:{issue.line}: `{issue.target}`"
            )
        elif issue.kind == "syntax":
            errors.append(
                f"unsupported local Markdown link syntax in {issue.source}:{issue.line}: "
                f"`{issue.target}` ({issue.detail})"
            )
        else:
            errors.append(
                f"invalid documentation page in {issue.source}:{issue.line}: "
                f"`{issue.target}` ({issue.detail})"
            )
    return errors


def main() -> int:
    errors = run_checks()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        print(
            f"\n{len(errors)} dead or invalid documentation reference(s). Fix the "
            "target or rewrite the text so it does not present a missing artifact "
            "as live.",
            file=sys.stderr,
        )
        return 1
    print("doc references: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
