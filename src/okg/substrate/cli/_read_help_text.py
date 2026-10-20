"""What `okg <read tool> --help` prints. GENERATED: do not edit.

Written by `scripts/gen_read_help_text.py` from the MCP server built with no
database (probe mode, the `full` capability fixture, deployment `default`).
`tests/substrate/cli/test_read_help_text.py` fails when this file and a fresh
generation differ; run the script to write it again.

Per tool: `description`, the tool's description exactly as the server lists
it, one string per line; `arguments`, one line per argument from the
published input schema; `example_argv`, a command line after `okg <tool>`;
and `example_arguments`, the argument object that command line sends.
"""
from __future__ import annotations

from typing import Any

READ_HELP: dict[str, dict[str, Any]] = {
    "search": {
        "description": (
            "Find records by words, best first.",
            "",
            "search(query=\"<words>\", where={\"type\":\"<type>\"}, limit=8). A page is",
            "the best matches, not all of them; completeness=\"exact\" counts every",
            "match or refuses. Read narrow first: ids and titles",
            "(select=[\"node_id\",\"handle\"] or a small limit), then read the",
            "records that matter. Without a type in `where`, the deployment's default",
            "search types apply. `where` with no query looks records up by field;",
            "`contains` finds text in attrs.<key>, path or title. A",
            "known id goes to `inspect`.",
            "",
            "method=\"grep\" is exact, case-insensitive: words, \"a phrase\",",
            "a NEAR/80 b, -word, re:/<pattern>/, path:<text>. All terms must match",
            "one text; re:/a|b|c/ for variants.",
            "`scope={\"record\": id}` searches one record and its passages (lexical).",
            "`retain=true` names the page's records as a result reference",
            "for map/filter/aggregate to read.",
        ),
        "arguments": {
            "query": "string",
            "method": "string; default lexical",
            "where": "object",
            "scope": "object {record: string; records: list of string, 1 to 100 items; children_of: string; edge_type: string}",
            "subtype_filter": "accepted, but not in the tool's published arguments; the tool reads it as a `where` condition",
            "attr_filters": "accepted, but not in the tool's published arguments; the tool reads it as a `where` condition",
            "filter": "accepted, but not in the tool's published arguments; the tool reads it as a `where` condition",
            "profile": "accepted, but not in the tool's published arguments: applies a declared subtype scope (`all` disables it); the deployment's default applies when neither profile nor a `where` type is given",
            "source_family": "accepted, but not in the tool's published arguments: picks declared producer families before ranking",
            "max_per_parent": "accepted, but not in the tool's published arguments: a per-parent diversity bound on ranked rows",
            "rank_by": "accepted, but not in the tool's published arguments: lexical defaults to `records`: one card per record, passages folded in, `limit` counting records; `self` ranks each row by its own text; `passages` needs a declared child relation",
            "limit": "integer from 1 to 1000",
            "offset": "integer from 0 to 10000",
            "generation_id": "accepted, but not in the tool's published arguments: reads this one call at another published generation",
            "select": "list of string",
            "authority": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "allow_unverified_embedding_provenance": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "max_bytes": "accepted, but not in the tool's published arguments: the reply's byte budget; a page over it keeps every row and shortens passages and selected values to fit",
            "max_chars": "integer from 64 to 4096",
            "expand_concepts": "true or false",
            "path_prefix": "string",
            "continuation_token": "string",
            "view": "one of cards, list, windows, metadata",
            "matching": "accepted, but not in the tool's published arguments: windows view only: `record` decides each term across the record's chained text, `passage` within each text alone",
            "context_chars": "accepted, but not in the tool's published arguments: windows view only: the characters around each match",
            "completeness": "one of bounded, exact",
            "retain": "true or false; default false",
            "summary": "accepted, but not in the tool's published arguments: 1-4 fields, with view=\"metadata\" only: adds the counts per value of the referenced records, top 50 groups",
        },
        "example_argv": (
            "zirconium",
            "--where",
            "{\"op\": \"eq\", \"field\": \"type\", \"value\": \"doc\"}",
            "--limit",
            "5",
        ),
        "example_arguments": {
            "query": "zirconium",
            "where": {
                "op": "eq",
                "field": "type",
                "value": "doc",
            },
            "limit": 5,
        },
    },
    "inspect": {
        "description": (
            "Read one named thing.",
            "",
            "`target.kind` picks the subject. Start with",
            "inspect(target={\"kind\":\"graph\"}): record and link type counts.",
            "`select` and `max_bytes` go beside `target`, not in it:",
            "inspect(target={\"kind\":\"node\",\"node_id\":\"<id>\"}, select=[\"attrs\"]).",
            "A `node` over max_bytes comes back with its largest parts cut, each",
            "named. `node`/`text` take `path` (+`source`) for `node_id`.",
            "`text` reads a record's whole text in reading order,",
            "in pages of max_bytes (default 65536, up to 262144); `lines` reads",
            "a list of [first,last] line ranges.",
            "`next` continues a page at its page size, or at a new max_bytes.",
        ),
        "arguments": {
            "target": "any JSON value or object {kind (required): any JSON value} or object {kind (required): node; node_id: string; path: string; source: string; hydrate: list of string or true or false} or object {kind (required): one of graph} or object {kind (required): subtype; subtype: string; continuation_token: string; detail: one of summary, sample_nodes; page_size: integer from 1 to 32} or object {kind (required): text; node_id: string; path: string; source: string; at: object with keys chunk, offset, hash; lines: list of list of any JSON value; continuation_token: string} or object {kind (required): texts; items (required): list of object with keys at or object with keys node_id, 1 to 8 items}",
            "generation_id": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "select": "list of string",
            "authority": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "max_bytes": "integer, at least 4096",
            "next": "string",
        },
        "example_argv": (
            "--target",
            "{\"kind\": \"node\", \"node_id\": \"doc:000\"}",
        ),
        "example_arguments": {
            "target": {
                "kind": "node",
                "node_id": "doc:000",
            },
        },
    },
    "expand": {
        "description": (
            "Follow links from one record to the records on the other end.",
            "",
            "expand(input=\"<id>\", edge_type=\"<link>\", direction=\"in\"). `select`:",
            "rows are the linked records (node_id, subtype, attrs, provenance;",
            "1-hop adds link, link.attrs). A 1-hop select of only edge_id, src,",
            "dst, edge_type, direction (attrs: the link's) returns one row per link.",
            "Only one hop takes `where` and `order_by` (on the far record).",
            "path=[\"<link>:out\", \"<link>:in\"] ('*' for any link) follows up to 3",
            "hops and returns the records at the end; it resumes by `next` alone.",
        ),
        "arguments": {
            "input": "object or string",
            "edge_type": "string",
            "direction": "one of out, in; default out",
            "path": "list of string, 1 to 3 items",
            "depth": "accepted, but not in the tool's published arguments: repeats one edge_type and direction up to 3 hops; resumes by `next` alone",
            "target_subtype": "accepted, but not in the tool's published arguments: on a path or depth, filters only the terminal record, so a terminal-empty path stays distinguishable from no path",
            "continuation_token": "accepted, but not in the tool's published arguments: the old paging token; `next` replaces it",
            "filter": "accepted, but not in the tool's published arguments: the old spelling of `where` (one hop only)",
            "generation_id": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "limit": "integer from 1 to 1000; default 50",
            "offset": "integer from 0 to 10000",
            "select": "list of string",
            "order_by": "string",
            "include_siblings": "accepted, but not in the tool's published arguments: adds policy-filtered same-type neighbours through catalog-declared self-relations, telling undeclared structure apart from a declared empty relation",
            "live": "accepted, but not in the tool's published arguments: {relation, args}: also asks one live relation for this record's type; its rows return in `external_live`, never merged with the graph rows",
            "authority": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "max_bytes": "integer, at least 4096",
            "next": "string",
        },
        "example_argv": (
            "doc:000",
            "--edge-type",
            "contains",
        ),
        "example_arguments": {
            "input": "doc:000",
            "edge_type": "contains",
        },
    },
    "filter": {
        "description": (
            "List every record meeting a condition, in pages.",
            "",
            "filter(where={\"type\":\"<subtype>\",\"attrs.path\":{\"op\":\"suffix\",",
            "\"value\":\"docker/Dockerfile\"}}).",
            "`contains` is JSON containment on attrs (field 'attrs', value",
            "{key: exact}), or text inside attrs.<key> (ASCII case ignored);",
            "`like` matches a substring of a field, ignoring accents and case.",
            "Use `search` for words, `matches` on",
            "field `text` for words or re:/regex/ (rows add match, match_count).",
            "`order_by` is",
            "'<field>' or '<field> desc', including a declared attrs.<key>.",
            "Past offset 10000, `next` alone continues.",
        ),
        "arguments": {
            "input": "accepted, but not in the tool's published arguments: the set: {\"kind\": \"nodes\"|\"edges\", \"subtype\", \"edge_type\", \"filter\"} or {\"ref\": id}; `where` with a type stands for it",
            "predicate": "accepted, but not in the tool's published arguments: the old spelling of `where`",
            "generation_id": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "limit": "integer from 1 to 1000; default 50",
            "offset": "integer from 0 to 10000",
            "select": "list of string",
            "order_by": "string",
            "authority": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "max_bytes": "integer, at least 4096",
            "retain": "true or false; default false",
            "next": "string",
        },
        "example_argv": (
            "--input",
            "{\"kind\": \"nodes\", \"subtype\": \"ticket\"}",
            "--predicate",
            "{}",
        ),
        "example_arguments": {
            "input": {
                "kind": "nodes",
                "subtype": "ticket",
            },
            "predicate": {},
        },
    },
    "aggregate": {
        "description": (
            "Count a set, per value.",
            "",
            "`by` names 1-4 fields: subtype, attrs.<key>, month(date.created)",
            "(or day, week, year) or match, e.g.",
            "aggregate(where={\"type\":\"<subtype>\"}, by=[\"attrs.<key>\"],",
            "metrics=[\"count\"]). by=[\"match\"] with where",
            "{\"text\":{\"op\":\"matches\",\"value\":\"re:/<regex>/\"}} counts each",
            "distinct match.",
        ),
        "arguments": {
            "input": "The set: {\"ref\": id}, {\"kind\": \"edges\", \"edge_type\": \"<type>\"}, or one record id.",
            "by": "required. list of string, 1 to 4 items",
            "metrics": "required. list of count, exactly 1 item",
            "generation_id": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "limit": "integer from 1 to 1000; default 50",
            "offset": "integer from 0 to 10000",
            "authority": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "max_bytes": "integer, at least 4096",
        },
        "example_argv": (
            "--input",
            "{\"kind\": \"nodes\"}",
            "--by",
            "subtype",
            "--metrics",
            "count",
        ),
        "example_arguments": {
            "input": {
                "kind": "nodes",
            },
            "by": [
                "subtype",
            ],
            "metrics": [
                "count",
            ],
        },
    },
    "map": {
        "description": (
            "Chosen fields of every record in a set.",
            "",
            "map(where={\"type\":\"<subtype>\"}, select=[\"node_id\",\"attrs.<key>\"]).",
            "Past offset 10000, `next` alone continues.",
        ),
        "arguments": {
            "input": "The set; {\"ref\": id} reads a retained result.",
            "projection": "accepted, but not in the tool's published arguments: the old spelling of `select`; also {alias: field} to rename",
            "generation_id": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "limit": "integer from 1 to 1000; default 50",
            "offset": "integer from 0 to 10000",
            "authority": "accepted, but not in the tool's published arguments; inspect(target={\"kind\":\"surface\"}) lists it",
            "max_bytes": "integer, at least 4096",
            "next": "string",
        },
        "example_argv": (
            "--input",
            "{\"kind\": \"nodes\", \"subtype\": \"ticket\"}",
            "--projection",
            "node_id",
        ),
        "example_arguments": {
            "input": {
                "kind": "nodes",
                "subtype": "ticket",
            },
            "projection": [
                "node_id",
            ],
        },
    },
    "query": {
        "description": (
            "Read-only SQL on one pinned generation: okg.v_nodes(node_id, subtype, attrs jsonb), okg.v_edges(edge_id, src, dst, edge_type, attrs). Text is attrs->>'text', a field attrs->>'name'. Regex in $$...$$ (word boundary \\y, not \\b). Fit max_bytes with LIMIT and left(). Examples:",
            "SELECT subtype, count(*) FROM okg.v_nodes GROUP BY 1",
            "SELECT edge_type, d.subtype, count(*) FROM okg.v_edges JOIN okg.v_nodes d ON node_id = dst GROUP BY 1, 2",
            "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~* $$fail(ed|ure)$$ LIMIT 20",
        ),
        "arguments": {
            "sql": "required. string",
            "generation_id": "integer or string",
            "row_cap": "integer from 1 to 10000; default 1000",
            "timeout_ms": "integer from 100 to 60000; default 5000",
            "authority": "object",
            "max_bytes": "integer, at least 4096",
        },
        "example_argv": (
            "SELECT node_id FROM okg.v_nodes ORDER BY node_id LIMIT 3",
            "--row-cap",
            "10",
        ),
        "example_arguments": {
            "sql": "SELECT node_id FROM okg.v_nodes ORDER BY node_id LIMIT 3",
            "row_cap": 10,
        },
    },
}
