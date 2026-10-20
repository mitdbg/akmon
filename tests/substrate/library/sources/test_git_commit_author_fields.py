"""A commit's author as plain fields on the commit node (issue #3649).

`author` is the id of the `git_author` node the commit's `assigned_to`
edge starts from, and `author_name` is the name as that commit records it.
With them, commits can be counted and filtered by author with the read
tools that already count tickets by assignee.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


_BASE_ENV = {
    "GIT_COMMITTER_NAME": "Fixture Committer",
    "GIT_COMMITTER_EMAIL": "committer@example.test",
    "GIT_AUTHOR_DATE": "2026-01-02T03:04:05+00:00",
    "GIT_COMMITTER_DATE": "2026-01-02T03:04:05+00:00",
}


def _repo(root: Path) -> Path:
    root.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
    return root


def _commit(root: Path, filename: str, *, name: str, email: str) -> str:
    """Commit one new file as ``name <email>``; return the new SHA."""
    env = {
        **os.environ,
        **_BASE_ENV,
        "GIT_AUTHOR_NAME": name,
        "GIT_AUTHOR_EMAIL": email,
    }
    (root / filename).write_text(f"{filename}\n", encoding="utf-8")
    subprocess.run(["git", "add", filename], cwd=root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", f"Add {filename}"],
        cwd=root,
        env=env,
        check=True,
    )
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()


def _run(root: Path, **kwargs):
    """Run the history source; return ``(facts, next_cursor)``. The run's
    facts are a one-pass stream, so they are read into a list here."""
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )

    run = GitHistorySource(repo="r", repo_root=root).run(
        kwargs.pop("run_id", "run-1"), **kwargs
    )
    return list(run.facts), run.next_cursor


def _commit_attrs(facts) -> dict[str, dict]:
    from okg.substrate.library.sources.base import NodeFact

    return {
        f.node_id: f.attrs
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "git_commit"
    }


def _author_edges(facts) -> dict[str, str]:
    """``{commit id: author id}`` from the `assigned_to` edges."""
    from okg.substrate.library.sources.base import EdgeFact

    return {
        f.dst: f.src
        for f in facts
        if isinstance(f, EdgeFact)
        and f.edge_type == "assigned_to"
        and f.dst.startswith("commit:")
    }


def test_commit_carries_its_author_id_and_the_name_it_was_written_under(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.base import NodeFact

    root = _repo(tmp_path / "repo")
    # One person, one e-mail (in two letter cases), two spellings.
    first = _commit(root, "a.txt", name="A. Example", email="Dev@Example.test")
    second = _commit(root, "b.txt", name="Alex Example", email="dev@example.test")
    other = _commit(root, "c.txt", name="Sam Other", email="sam@example.test")

    facts, _ = _run(root)
    attrs = _commit_attrs(facts)

    # `author` is the author node's id, lower-cased like the node id, so
    # both spellings of one e-mail count as one author.
    assert attrs[f"commit:r:{first}"]["author"] == "author:dev@example.test"
    assert attrs[f"commit:r:{second}"]["author"] == "author:dev@example.test"
    assert attrs[f"commit:r:{other}"]["author"] == "author:sam@example.test"
    # ... and it is exactly where the commit's author edge starts.
    edges = _author_edges(facts)
    assert len(edges) == 3
    assert {
        commit_id: commit["author"] for commit_id, commit in attrs.items()
    } == edges

    # `author_name` is the name on THAT commit. The author node keeps one
    # name per e-mail, so the other spelling exists nowhere else.
    assert attrs[f"commit:r:{first}"]["author_name"] == "A. Example"
    assert attrs[f"commit:r:{second}"]["author_name"] == "Alex Example"
    author_node = next(
        f
        for f in facts
        if isinstance(f, NodeFact) and f.node_id == "author:dev@example.test"
    )
    assert author_node.attrs["display_name"] == "Alex Example"


def test_commit_with_no_author_email_gets_no_author_field(tmp_path: Path) -> None:
    """An empty author e-mail makes no author node and no author edge, so
    the commit gets no `author` either: not an empty string, not an id that
    names no node. A count by author then shows these commits as having no
    value instead of crediting an author that does not exist."""
    root = _repo(tmp_path / "repo")
    known = _commit(root, "a.txt", name="Alex Example", email="dev@example.test")
    nameless = _commit(root, "b.txt", name="Batch Job", email="")

    facts, _ = _run(root)
    attrs = _commit_attrs(facts)

    assert attrs[f"commit:r:{known}"]["author"] == "author:dev@example.test"
    orphan = attrs[f"commit:r:{nameless}"]
    assert "author" not in orphan
    edges = _author_edges(facts)
    assert edges == {f"commit:r:{known}": "author:dev@example.test"}
    # The name the log does hold is still kept.
    assert orphan["author_name"] == "Batch Job"


def test_incremental_run_writes_the_same_author_id_for_a_new_commit(
    tmp_path: Path,
) -> None:
    """A later cursor run that emits only the new commit writes the same
    lower-cased author id as the first run did, whatever case the new
    commit's e-mail is typed in."""
    root = _repo(tmp_path / "repo")
    _commit(root, "a.txt", name="Alex Example", email="dev@example.test")
    _, cursor = _run(root, run_id="run-0", mode="cursor")

    later = _commit(root, "b.txt", name="ALEX", email="DEV@EXAMPLE.TEST")
    facts, _ = _run(root, run_id="run-1", mode="cursor", cursor=cursor)

    attrs = _commit_attrs(facts)
    assert set(attrs) == {f"commit:r:{later}"}
    assert attrs[f"commit:r:{later}"]["author"] == "author:dev@example.test"
    assert attrs[f"commit:r:{later}"]["author_name"] == "ALEX"
    assert _author_edges(facts) == {
        f"commit:r:{later}": "author:dev@example.test"
    }


def _raw_commit(root: Path, author_line: str) -> str:
    """Write a commit object whose ``author`` header is ``author_line``
    verbatim and move ``main`` to it. ``git commit`` refuses an empty name
    and trims the e-mail, so odd identities that real histories do hold
    (imports from other systems) are written as raw objects."""
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    tree = subprocess.check_output(
        ["git", "rev-parse", "HEAD^{tree}"], cwd=root, text=True
    ).strip()
    body = (
        f"tree {tree}\n"
        f"parent {head}\n"
        f"author {author_line} 1767323045 +0000\n"
        "committer Fixture Committer <committer@example.test> 1767323045 +0000\n"
        "\n"
        "Imported change\n"
    )
    sha = subprocess.check_output(
        ["git", "hash-object", "-t", "commit", "-w", "--literally", "--stdin"],
        cwd=root,
        input=body,
        text=True,
    ).strip()
    subprocess.run(
        ["git", "update-ref", "refs/heads/main", sha], cwd=root, check=True
    )
    return sha


def test_commit_with_an_email_and_no_name_gets_author_only(tmp_path: Path) -> None:
    root = _repo(tmp_path / "repo")
    _commit(root, "a.txt", name="Alex Example", email="dev@example.test")
    nameless = _raw_commit(root, "<import@example.test>")

    facts, _ = _run(root)
    commit = _commit_attrs(facts)[f"commit:r:{nameless}"]

    assert commit["author"] == "author:import@example.test"
    assert "author_name" not in commit
    assert _author_edges(facts)[f"commit:r:{nameless}"] == (
        "author:import@example.test"
    )


def test_commit_with_a_blank_email_agrees_with_its_author_edge(
    tmp_path: Path,
) -> None:
    """Whatever the source makes of an e-mail that is only white space, the
    field and the edge agree: either both name the same author node or the
    commit has neither."""
    root = _repo(tmp_path / "repo")
    _commit(root, "a.txt", name="Alex Example", email="dev@example.test")
    blank = _raw_commit(root, "Batch Job < >")

    facts, _ = _run(root)
    commit = _commit_attrs(facts)[f"commit:r:{blank}"]
    edge = _author_edges(facts).get(f"commit:r:{blank}")

    assert commit.get("author") == edge
    assert commit["author_name"] == "Batch Job"


def test_cursor_from_before_the_author_fields_rewalks_once(tmp_path: Path) -> None:
    """A deployment's existing commits get the fields on the next sync: a
    cursor written at the previous field version re-emits every commit once,
    and an unchanged run after that emits nothing."""
    from okg.substrate.library.sources._git_graph_history_emit import (
        MESSAGE_SCHEMA_VERSION,
    )

    root = _repo(tmp_path / "repo")
    first = _commit(root, "a.txt", name="Alex Example", email="dev@example.test")
    second = _commit(root, "b.txt", name="Sam Other", email="sam@example.test")

    _, seeded_cursor = _run(root, run_id="run-0", mode="cursor")
    assert MESSAGE_SCHEMA_VERSION >= 5
    older = {**seeded_cursor, "message_schema_version": 4}

    rewalk_facts, rewalk_cursor = _run(
        root, run_id="run-1", mode="cursor", cursor=older
    )
    attrs = _commit_attrs(rewalk_facts)
    assert set(attrs) == {f"commit:r:{first}", f"commit:r:{second}"}
    assert attrs[f"commit:r:{first}"]["author"] == "author:dev@example.test"
    assert attrs[f"commit:r:{second}"]["author_name"] == "Sam Other"
    assert rewalk_cursor["message_schema_version"] == MESSAGE_SCHEMA_VERSION

    unchanged_facts, _ = _run(
        root, run_id="run-2", mode="cursor", cursor=rewalk_cursor
    )
    assert unchanged_facts == []


def _git_graph_catalog(tmp_path: Path):
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )

    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        json.dumps({"name": "author-fields", "modules": ["git_graph"]})
    )
    cross_links = tmp_path / "cross_links"
    cross_links.mkdir()
    return compose_catalog(
        deployment,
        modules_root=load_modules_root(),
        cross_links_root=cross_links,
    )


def test_git_module_declares_the_two_fields_and_no_index_for_them(
    tmp_path: Path,
) -> None:
    """The schema names both fields, so they are documented attributes of a
    commit. It declares no filterable type for any commit field: a shipped
    module that did would turn the declared-index doctor check red on every
    deployment that has not built declared indexes, and would type those
    field names for every record type. Counting and filtering by the fields
    needs no declaration; ordering by a commit date does, and is a separate
    decision."""
    import yaml

    from okg.substrate.catalog.modules_compose import load_modules_root

    schema = yaml.safe_load(
        (load_modules_root() / "git_graph" / "schema.yaml").read_text(
            encoding="utf-8"
        )
    )
    commit = schema["classes"]["GitCommit"]["attributes"]
    assert {"author", "author_name"} <= set(commit)
    for name, body in commit.items():
        assert "filterable" not in ((body or {}).get("annotations") or {}), name
    composed = _git_graph_catalog(tmp_path)
    assert "filterable_attrs" not in composed.subtype_metadata.get(
        "git_commit", {}
    )


def test_commit_author_fields_are_classified_like_the_author_node(
    tmp_path: Path,
) -> None:
    """The git module classifies none of its personal data today, so a
    reader under access rules sees an author node's name and e-mail unless
    the whole node is withheld. The commit's two fields are the same data
    and must not be more, or less, visible than the node's: same outcome
    unclassified, and same outcome once a catalog classifies both."""
    from okg.substrate.nomos.read_masking import (
        MASK_TOKEN,
        mask_attrs,
        subtype_attr_classifier,
    )

    composed = _git_graph_catalog(tmp_path)
    for subtype in ("git_commit", "git_author"):
        # Today: neither subtype declares classes (no opt-in).
        assert "pii_classes" not in composed.subtype_metadata.get(subtype, {})

    commit = {
        "author": "author:dev@example.test",
        "author_name": "Alex Example",
        "repo": "r",
    }
    author = {"email": "dev@example.test", "display_name": "Alex Example"}

    # Unclassified: both shown, exactly as the author node already is.
    legacy = subtype_attr_classifier(None)
    shown_commit = mask_attrs(commit, classifier=legacy)
    shown_author = mask_attrs(author, classifier=legacy)
    assert shown_author["display_name"] == "Alex Example"
    assert shown_author["email"] == "dev@example.test"
    assert shown_commit["author_name"] == "Alex Example"
    assert shown_commit["author"] == "author:dev@example.test"

    # Classified the way the person module classifies the same data: the
    # name and the e-mail are hidden on the author node, and the commit's
    # copies are hidden with them. A commit map that classifies its other
    # fields but forgets these two hides them too (unlisted fails safe).
    author_classes = {
        "email": "direct_identifier",
        "display_name": "quasi_identifier",
    }
    for commit_classes in (
        {
            "author": "direct_identifier",
            "author_name": "quasi_identifier",
            "repo": "non_pii",
        },
        {"repo": "non_pii"},
    ):
        hidden_author = mask_attrs(
            author, classifier=subtype_attr_classifier(author_classes)
        )
        hidden_commit = mask_attrs(
            commit, classifier=subtype_attr_classifier(commit_classes)
        )
        assert hidden_author["display_name"] == MASK_TOKEN
        assert hidden_author["email"] == MASK_TOKEN
        assert hidden_commit["author_name"] == MASK_TOKEN
        assert hidden_commit["author"] == MASK_TOKEN
        assert hidden_commit["repo"] == "r"
