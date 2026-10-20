"""`okg source draft`: print a first source manifest drafted from samples.

PACT no-code-source-manifest-http. The command reads up to 200 sample
records from local files or one GET of a JSON API and prints a commented
`okg.source/v1` manifest to standard output. It writes nothing and never
adds the source: review the draft, then preview it with
`okg deployment add-source <deployment> <name> --manifest <file>`.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from okg.substrate.errors import SubstrateError


def cmd_source(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.cli.main import _emit_error
    from okg.substrate.sources.manifest_draft import DraftError, draft

    if getattr(args, "source_cmd", None) != "draft":
        print("usage: okg source draft <file-or-url> [...]", file=sys.stderr)
        return 2
    try:
        text = draft(
            list(args.targets), auth=getattr(args, "auth", None),
            type_name=getattr(args, "type_name", None),
        )
    except DraftError as exc:
        return _emit_error(SubstrateError(str(exc), fix_hint=exc.fix), as_json=as_json)
    if as_json:
        print(json.dumps({"ok": True, "manifest": text}))
    else:
        sys.stdout.write(text)
    return 0


def register_source_parser(sub: Any, *, add_json_flag: Any) -> None:
    p_source = sub.add_parser(
        "source",
        help="Draft a source manifest from sample files or a JSON API",
    )
    add_json_flag(p_source)
    p_source_sub = p_source.add_subparsers(dest="source_cmd")
    p_draft = p_source_sub.add_parser(
        "draft",
        help=(
            "Print a commented source manifest drafted from up to 200 sample "
            "records; writes nothing and never adds the source"
        ),
    )
    add_json_flag(p_draft)
    p_draft.add_argument(
        "targets", nargs="+", metavar="FILE_OR_URL",
        help="Sample JSON, JSONL, CSV or YAML files, or one http(s) URL that returns JSON",
    )
    p_draft.add_argument(
        "--auth", metavar="MODE:ENV",
        help=(
            "For a URL: bearer:ENV, header:Name:ENV, basic:USER_ENV:PASSWORD_ENV "
            "or client_certificate:CERT_ENV:KEY_ENV[:CA_DIR_ENV]; names "
            "environment variables, never values"
        ),
    )
    p_draft.add_argument(
        "--type", dest="type_name", metavar="NAME",
        help="The record type name to use (snake_case); default: from the URL or file name",
    )


__all__ = ["cmd_source", "register_source_parser"]
