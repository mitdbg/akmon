"""A fixture Open WebUI app database on the PINNED schema.

The DDL below is VERBATIM from a running Open WebUI v0.11.0 instance
(``sqlite_master.sql`` for the four tables this source reads, at alembic
revision ``f0bd01a18a3d``). Authoring it by hand would have let the tests
pass against a schema the adapter will never meet; copying the vendor's own
statements is what makes the pin gate meaningful. The pin moved to v0.11.4
(revision ``d4c1a8e37b62``); of its three migrations only ``d4c1a8e37b62``
changes a table here, adding ``chat.timer_at``, and the DDL applies that
column with the migration's own ``ADD COLUMN``.

Row CONTENT is authored rather than copied: the operator's real conversations
are private, and committing them to a public repository to make a test
realistic would be the wrong trade. The shapes are the ones the live instance
actually stores -- ``chat_message.id`` is ``<chat id>-<uuid>``, ``content`` is
a JSON-encoded string, ``feedback.data`` carries ``rating`` /
``model_id`` / ``sibling_model_ids`` and ``feedback.meta`` carries ``arena`` /
``chat_id`` / ``message_id``.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

from okg.substrate.chat.audit_filter import CITATION_HEADER, CITATION_SEPARATOR
from okg.substrate.chat.audit_record import (
    AuditArtifact,
    GenerationAgreement,
    GenerationObservation,
    TaskKind,
    canonical_payload,
)
from okg.substrate.library.sources.chat_sessions import PINNED_APP_DB_REVISION
from tests.substrate._fake_credentials import fake_credential


PINNED_DDL = """
CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL,
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
CREATE TABLE chat (
	id VARCHAR NOT NULL,
	user_id VARCHAR,
	title TEXT,
	created_at BIGINT,
	updated_at BIGINT,
	share_id TEXT,
	archived BOOLEAN, chat JSON, pinned BOOLEAN, meta JSON DEFAULT '{}' NOT NULL,
	folder_id TEXT, tasks JSON, summary TEXT, last_read_at BIGINT,
	current_message_id TEXT, variables JSON,
	PRIMARY KEY (id),
	UNIQUE (share_id)
);
-- v0.11.4: migration d4c1a8e37b62 adds this column the same way.
ALTER TABLE chat ADD COLUMN timer_at BIGINT;
CREATE TABLE chat_message (
	id TEXT NOT NULL,
	chat_id TEXT NOT NULL,
	user_id TEXT,
	role TEXT NOT NULL,
	parent_id TEXT,
	content JSON,
	output JSON,
	model_id TEXT,
	files JSON,
	sources JSON,
	embeds JSON,
	done BOOLEAN,
	status_history JSON,
	error JSON,
	usage JSON,
	created_at BIGINT,
	updated_at BIGINT, context_summary TEXT, meta JSON,
	PRIMARY KEY (id),
	FOREIGN KEY(chat_id) REFERENCES chat (id) ON DELETE CASCADE
);
CREATE TABLE feedback (
	id TEXT NOT NULL,
	user_id TEXT,
	version BIGINT,
	type TEXT,
	data JSON,
	meta JSON,
	snapshot JSON,
	created_at BIGINT NOT NULL,
	updated_at BIGINT NOT NULL,
	PRIMARY KEY (id)
);
"""

#: The other tables the live instance carries. Created EMPTY so a test can
#: prove the adapter never opens them; without them present, "did not read
#: `auth`" would be true for the uninteresting reason that `auth` was absent.
CREDENTIAL_TABLE_DDL = """
CREATE TABLE auth (id TEXT PRIMARY KEY, email TEXT, password TEXT, active BOOLEAN);
CREATE TABLE api_key (id TEXT PRIMARY KEY, user_id TEXT, api_key TEXT);
CREATE TABLE oauth_session (id TEXT PRIMARY KEY, user_id TEXT, token TEXT);
CREATE TABLE config (id INTEGER PRIMARY KEY, data JSON);
CREATE TABLE user (
    id VARCHAR PRIMARY KEY, name VARCHAR, email VARCHAR, role VARCHAR,
    oauth JSON, settings JSON, info JSON
);
"""

PRINCIPAL = "30aa5184-69a8-403c-a566-684016faeb56"
MODEL_WITH_GRAPH = "openai/gpt-5.6-luna+okg"
MODEL_WITHOUT_GRAPH = "openai/gpt-5.6-luna"

CHAT_GRAPH = "05451598-e1e3-403c-9d25-e7c490087cea"
CHAT_SECRET = "e421d5ef-bcae-4e44-8d4a-056dee2d7a05"
CHAT_ARENA = "fde4455e-1872-43a0-a9ca-bda7e5bd8a4b"

MSG_GRAPH_USER = "8d617bb2-9308-49a7-b186-b5e5aa3cd88c"
MSG_GRAPH_ANSWER = "fcdedbbf-3a6d-4a28-9036-a01e25196252"
MSG_SECRET_USER = "0f74032f-15a3-4097-baee-5dfb9ac4005a"
MSG_ARENA_USER = "c13685f4-b66a-422b-8f5f-518755110c4e"
MSG_ARENA_ANSWER = "4a08d773-77ff-4777-a208-6cdfcdf9f72f"

#: An in-allowlist citation, a second in-allowlist citation, and one whose id
#: prefix is NOT allowlisted. The third exists so the drop path is exercised
#: by a real citation rather than by a unit-level call.
CITED_PACT = "pact-change:chat-transcript-source"
CITED_FILE = "file:okg:src/okg/substrate/chat/audit_filter.py"
CITED_UNKNOWN = "runtime:okg:worker-7"

#: A credential-shaped string in USER FREE TEXT. Chat messages are exactly
#: where one shows up in practice: somebody pastes a command to ask about it.
#: One distinct value per text path the adapter writes, so a missing
#: redaction call on any single path is attributable rather than masked by
#: another path's coverage.
PASTED_SECRET = "OPENAI_API_KEY=sk-liveKEYmaterial0123456789"
TITLE_SECRET = "GITHUB_TOKEN=ghp_titleSECRET0123456789"
COMMENT_SECRET = "SLACK_TOKEN=" + fake_credential("xoxb-", "commentSECRET0123456789")
PAYLOAD_SECRET = "AWS_SECRET_ACCESS_KEY=AKIAPAYLOADSECRET99"

#: Long enough that the default 400-character bound must truncate it. Without
#: this, removing the bound would leave every test green.
LONG_MESSAGE = "the substrate publishes nodes and edges. " * 200

ANSWER_WITH_CITATIONS = (
    "Two PACTs touched the chat surface.\n"
    "\n"
    "generation_id: g-000042\n"
    f"{CITATION_HEADER}\n"
    f"- [{CITED_PACT}](https://console.example/#node/abc)"
    f"{CITATION_SEPARATOR}okg-workspace.openspec\n"
    f"- {CITED_FILE}{CITATION_SEPARATOR}okg-workspace.git-files "
    "(no console link: this deployment declares no operator console base "
    "(chat.deep_links.operator_ui_base), so no console link can be built)\n"
    f"- {CITED_UNKNOWN}{CITATION_SEPARATOR}okg-workspace.compute-env\n"
)


#: The builder's CONSTRUCTION MANIFEST for the default fixture: what it
#: writes, stated independently of anything that later reads it. The publish
#: proof uses this as its upstream count so the number does not come from the
#: adapter's own reader.
CONSTRUCTED_COUNTS = {"conversations": 3, "messages": 5, "feedback": 2}


def _insert(conn: sqlite3.Connection, table: str, row: dict[str, Any]) -> None:
    columns = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    conn.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({marks})", tuple(row.values())
    )


def _message(
    chat_id: str,
    message_uuid: str,
    *,
    role: str,
    content: str,
    created_at: int,
    parent: Optional[str] = None,
    model_id: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "id": f"{chat_id}-{message_uuid}",
        "chat_id": chat_id,
        "user_id": PRINCIPAL,
        "role": role,
        "parent_id": parent,
        "content": json.dumps(content),
        "model_id": model_id,
        "done": 1,
        "created_at": created_at,
        "updated_at": created_at,
    }


def build_app_db(
    path: Path,
    *,
    revision: str = PINNED_APP_DB_REVISION,
    include_arena_chat: bool = True,
    include_ratings: bool = True,
) -> Path:
    """Write a fixture app database at ``path`` and return it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(PINNED_DDL)
        conn.executescript(CREDENTIAL_TABLE_DDL)
        conn.execute(
            "INSERT INTO alembic_version (version_num) VALUES (?)", (revision,)
        )
        # Credential-bearing rows the adapter must never touch.
        _insert(conn, "auth", {
            "id": PRINCIPAL,
            "email": "operator@example.org",
            "password": "$2b$12$notarealhashbutshaped",
            "active": 1,
        })
        _insert(conn, "api_key", {
            "id": "key-1", "user_id": PRINCIPAL, "api_key": "sk-instanceADMINkey00001",
        })
        _insert(conn, "oauth_session", {
            "id": "sess-1", "user_id": PRINCIPAL,
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJlSEVSRQ",
        })
        _insert(conn, "user", {
            "id": PRINCIPAL, "name": "Operator",
            "email": "operator@example.org", "role": "admin",
            "oauth": json.dumps({"sub": "oidc|1234"}),
            "settings": json.dumps({"ui": {}}),
            "info": json.dumps({}),
        })

        _insert(conn, "chat", {
            "id": CHAT_GRAPH,
            "user_id": PRINCIPAL,
            "title": "Which PACTs touched the chat surface",
            "created_at": 1785876863,
            "updated_at": 1785877508,
            "archived": 0,
            "chat": json.dumps({"history": {"currentId": MSG_GRAPH_ANSWER}}),
            "meta": json.dumps({"tags": ["technology"]}),
        })
        _insert(conn, "chat_message", _message(
            CHAT_GRAPH, MSG_GRAPH_USER, role="user",
            content="which PACTs touched the chat surface?",
            created_at=1785876863,
        ))
        _insert(conn, "chat_message", _message(
            CHAT_GRAPH, MSG_GRAPH_ANSWER, role="assistant",
            content=ANSWER_WITH_CITATIONS,
            created_at=1785876890, parent=MSG_GRAPH_USER,
            model_id=MODEL_WITH_GRAPH,
        ))

        _insert(conn, "chat", {
            "id": CHAT_SECRET,
            "user_id": PRINCIPAL,
            # The TITLE is generated by Open WebUI from the user's own first
            # turn, so it is a second free-text path a credential can take.
            "title": f"Why does {TITLE_SECRET} fail",
            "created_at": 1785876151,
            "updated_at": 1785876187,
            "archived": 0,
            "chat": json.dumps({}),
            "meta": json.dumps({}),
        })
        _insert(conn, "chat_message", _message(
            CHAT_SECRET, MSG_SECRET_USER, role="user",
            content=(
                "this fails for me:\n\n"
                f"    {PASTED_SECRET} uv run okg mcp-serve\n\n"
                "any idea why?"
            ),
            created_at=1785876151,
        ))

        if include_arena_chat:
            _insert(conn, "chat", {
                "id": CHAT_ARENA,
                "user_id": PRINCIPAL,
                "title": "Compare the two presets",
                "created_at": 1785874276,
                "updated_at": 1785874400,
                "archived": 0,
                "chat": json.dumps({}),
                "meta": json.dumps({}),
            })
            _insert(conn, "chat_message", _message(
                CHAT_ARENA, MSG_ARENA_USER, role="user",
                content="what does this repo publish on every generation?",
                created_at=1785874276,
            ))
            _insert(conn, "chat_message", _message(
                CHAT_ARENA, MSG_ARENA_ANSWER, role="assistant",
                content=LONG_MESSAGE,
                created_at=1785874300, parent=MSG_ARENA_USER,
                model_id=MODEL_WITH_GRAPH,
            ))

        if include_ratings:
            _insert(conn, "feedback", {
                "id": "fb-rating-1",
                "user_id": PRINCIPAL,
                "version": 0,
                "type": "rating",
                "data": json.dumps({
                    "rating": 1,
                    "model_id": MODEL_WITH_GRAPH,
                    "reason": "accurate_information",
                    # A third free-text path: the operator's own comment.
                    "comment": f"cited the right PACT; ignore my {COMMENT_SECRET}",
                }),
                "meta": json.dumps({
                    "chat_id": CHAT_GRAPH,
                    "message_id": MSG_GRAPH_ANSWER,
                }),
                "created_at": 1785877000,
                "updated_at": 1785877000,
            })
            if include_arena_chat:
                _insert(conn, "feedback", {
                    "id": "fb-arena-1",
                    "user_id": PRINCIPAL,
                    "version": 0,
                    "type": "rating",
                    "data": json.dumps({
                        "rating": 1,
                        "model_id": MODEL_WITH_GRAPH,
                        "sibling_model_ids": [MODEL_WITHOUT_GRAPH],
                        "reason": "complete_response",
                    }),
                    "meta": json.dumps({
                        "arena": True,
                        "chat_id": CHAT_ARENA,
                        "message_id": MSG_ARENA_ANSWER,
                    }),
                    "created_at": 1785874500,
                    "updated_at": 1785874500,
                })
        conn.commit()
    finally:
        conn.close()
    return path


def delete_conversation(path: Path, chat_id: str) -> None:
    """Remove a conversation the way the instance does (cascade included)."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("DELETE FROM chat_message WHERE chat_id = ?", (chat_id,))
        conn.execute("DELETE FROM chat WHERE id = ?", (chat_id,))
        conn.execute(
            "DELETE FROM feedback WHERE json_extract(meta, '$.chat_id') = ?",
            (chat_id,),
        )
        conn.commit()
    finally:
        conn.close()


def write_audit_artifact(
    state_home: Path,
    *,
    deployment: str,
    preset: str,
    schema_version: Optional[str] = None,
    payload_text: str = (
        "which PACTs touched the chat surface? my env has "
        f"{PAYLOAD_SECRET} in it"
    ),
) -> dict[str, Any]:
    """Append one inlet + completion pair for the graph conversation's answer.

    ``message_id`` is the BARE uuid, which is what Open WebUI's request
    metadata carries -- the stored ``chat_message.id`` is the composite. The
    adapter's join has to bridge that, so the fixture uses the real spelling.
    """
    kwargs: dict[str, Any] = {"state_home": state_home}
    if schema_version is not None:
        kwargs["schema_version"] = schema_version
    artifact = AuditArtifact.open(deployment, preset, **kwargs)
    body = {
        "messages": [{"role": "user", "content": payload_text}],
        "model": MODEL_WITH_GRAPH,
    }
    record = artifact.append_inlet(
        deployment=deployment,
        preset=preset,
        conversation_id=CHAT_GRAPH,
        message_id=MSG_GRAPH_ANSWER,
        task_kind=TaskKind.USER_TURN,
        principal=PRINCIPAL,
        model=MODEL_WITH_GRAPH,
        provider="openai",
        compaction_enabled=False,
        payload=canonical_payload(body),
        openwebui_version="0.11.0",
    )
    artifact.append_completion(
        record_id=record["record_id"],
        observation=GenerationObservation(
            GenerationAgreement.SINGLE, ("g-000042",)
        ),
    )
    return record


def node_ids(instance: str) -> dict[str, str]:
    """The node ids the adapter is expected to mint for this fixture."""
    return {
        "graph_session": f"chat:{instance}:{CHAT_GRAPH}",
        "graph_answer": f"chat-msg:{instance}:{CHAT_GRAPH}-{MSG_GRAPH_ANSWER}",
        "graph_question": f"chat-msg:{instance}:{CHAT_GRAPH}-{MSG_GRAPH_USER}",
        "secret_session": f"chat:{instance}:{CHAT_SECRET}",
        "secret_message": f"chat-msg:{instance}:{CHAT_SECRET}-{MSG_SECRET_USER}",
        "arena_session": f"chat:{instance}:{CHAT_ARENA}",
        "rating": f"chat-rating:{instance}:fb-rating-1",
        "arena_preference": f"chat-arena:{instance}:fb-arena-1",
    }
