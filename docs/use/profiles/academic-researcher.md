---
audience: researcher, data owner, operator, developer
type: reference
canonical_id: profile-academic-researcher
section: reference
visibility: secondary
status: active
owner: documentation
---
# Academic researcher profile

Install this profile with `okg install --profile academic-researcher`.
Without `--profile`, `okg install` builds the empty `minimal` starter instead.
It is a workstation evaluation configuration for an academic researcher,
professor, or small research group. It brings together graph types, source defaults,
publication checks, and dashboards for publications, code, projects, grants,
teaching, advising, and research-group administration. It is not a managed
service or a production deployment for regulated data.

It replaces two earlier profiles. `academic-pi` was renamed to it, and
`cs-researcher`, whose graph types were a subset of it, was merged into it.
Both old names are refused with `profile_retired`; see
[Deployments made with an old profile name](#deployments-made-with-an-old-profile-name).

Use this profile when one operator can own the workstation, database, source
approvals, backups, and access policy. Use a separately planned deployment when
several organizations share responsibility, the graph needs network exposure,
or legal and retention requirements need formal controls.

## Graph added by the profile

The profile includes general paper, project, person, document, calendar, and
communication types. It adds the following research-group records:

| Record | Represents |
|---|---|
| `Advisee` | A person in an advising relationship and its current status |
| `Lab` | A research group and its organizational relationships |
| `Grant` | A funding application or award |
| `Deadline` | A dated obligation and whether it was met |
| `Committee` | A service or governance group |
| `Review` | A paper, grant, tenure, or other review assignment |
| `Course` | A taught course |
| `TalkEngagement` | A talk, panel, tutorial, or similar event |
| `RecommendationLetter` | A letter request and its status |
| `ActionItem` | An assigned piece of work and whether it is open |

These types let a published graph answer questions such as “which grant
deadline affects this project?” or “which paper supports this lab decision?”
The profile does not establish whether an authorship, deadline, scientific
claim, or administrative record is correct.

## Sources and approval

The profile contains defaults for scholarly metadata, publication files,
notes, repositories, grants, calendars, mail, chat, courses, and research-group
pages. They are candidates, not active connections. The available route depends
on the workstation, assistant host, provider, credentials, and selected scope.

Installation does not read these sources. During setup, review each proposed
account, folder, label, calendar, channel, repository, and time range. Reject
anything you do not need. For private information, identify every system
that may handle a copy—the provider, connection host, assistant host, local
database, logs, and backups—and confirm its retention, deletion, and credential
revocation behavior.

## Install on a workstation

You need about 10 GB of free disk, Docker with Compose v2 running without
`sudo`, Python 3.12 or 3.13, and a supported assistant application opened at
least once. Confirm Docker first:

```bash
docker info >/dev/null
docker compose version
```

Stop if either command fails.

`okg install` is the one documented way in; name this profile with
`--profile academic-researcher`. From an installed wheel -- see
[Install from a wheel](../../operate/install-packaged.md#install-from-a-wheel) for how
to get one -- that is:

```bash
okg install --profile academic-researcher --name <name> --json
```

From a clone of the repository instead, `install.sh` wraps the same installer
with the checkout's own environment:

```bash
git clone https://github.com/mitdbg/akmon.git ~/okg
~/okg/install.sh --profile academic-researcher
```

Either route creates a local PostgreSQL database and deployment files, then
registers supported assistants it finds. Open the assistant named by the
installer and enter the phrase it prints, with your deployment's name:

```text
set up my OKG for <name>
```

Approve only the proposed accounts, folders, labels, calendars, channels,
repositories, and time ranges AKMON may read. Connection methods are listed in
[Built-in capabilities](../../reference/capabilities.md#connection-routes).

The installer accepts these optional inputs:

```bash
okg install --profile academic-researcher --name <name>
okg install --profile academic-researcher --orcid <orcid-id>
okg install --profile academic-researcher --pi-name "<full name>"
okg install --profile academic-researcher --dblp-pid <dblp-author-id>
okg install --profile academic-researcher --git-repos <repository-path>
```

`install.sh` takes the same flags. `--orcid`, `--pi-name` and `--dblp-pid`
help the publication sources find your papers; without them the installer
falls back to your Git user name.

`--git-repos` proposes a repository; it does not read it before approval. When
several academic-researcher deployments exist, always pass `--name` when
rerunning the installer.

To use a PostgreSQL database you already run, pass its DSN:

```bash
okg install --profile academic-researcher --postgres-dsn postgresql://user:pass@host:5432/okg
```

The installer connects to that database once before it writes anything and
asks five questions of it. It refuses if the database does not answer; if the
role may not create in it; if the server cannot install the extensions the
schema needs; if an extension still has to be installed and the role may not
create in schema `public`, or that schema is gone; or if the role may neither
create nor administer the roles the migration creates and grants. Those are
the questions one connection can answer, not a guarantee that the whole
migration succeeds. It then starts
no container and creates no Docker volume or network, stores the DSN in the
deployment, and hands the same DSN to the assistants it registers.
`--postgres-port` is ignored on this route. Without the flag, the installer
provisions Docker PostgreSQL as described above.

The DSN is stored in `deployment.yaml` in the clear, so anyone who can read
the deployment directory can read the password. A role that is not a superuser can run the
schema migration if it has CREATE and CONNECT on the database, CREATE on
schema `public`, the CREATEROLE attribute, membership in `okg_substrate`, and
ADMIN OPTION on the roles the migration grants. A superuser must first install
the required extensions and create those roles. CREATEROLE applies to the
whole server, not just this database. Only the migration step has been
verified this way; a full install and first publish with such a role have
not.

## Deployments made with an old profile name

A deployment scaffolded before the rename records `academic-pi` or
`cs-researcher` as its profile in `deployment.yaml`, and an `academic-pi` one
also names the old ontology module `academic_pi` and the old source package.
`okg install --profile academic-researcher` refuses such a deployment with
`profile_retired` rather than
skipping it and creating a second one. The error prints the one command that
rewrites the record in place, for example:

```bash
perl -pi -e 's/academic-pi/academic-researcher/g; s/academic_pi/academic_researcher/g; s/AcademicPi/AcademicResearcher/g' <deployment>/deployment.yaml <deployment>/source_registry.yaml
```

Run it, then rerun `okg install --profile academic-researcher` and
`okg migrate --deployment <name> --apply`
so the database catalog records the renamed module.

## Verify and remove the deployment

Replace `<name>` with the deployment name printed by the installer:

```bash
okg doctor --deployment <name>
okg generation list --deployment <name>
```

`doctor` should complete without an error. A new installation can be healthy
without a published generation because no source has been imported. After
approving a source, [import it](../sources/index.md),
[publish the graph](../sources/index.md#keep-it-up-to-date), and
[run a generation-pinned search](../search/index.md) that returns the expected
evidence.

To remove the local deployment, first preserve anything you need and confirm
the deployment name. This command removes its Docker container and data
volume, its deployment files, and its assistant registrations. It does not
touch a database you supplied with `--postgres-dsn`; drop that yourself.

```bash
okg reset --deployment <name> --yes
```

If repository access fails, authenticate with GitHub and run the
`git clone` command above again. If Docker is unavailable,
restart it and repeat the two Docker checks. If no assistant is detected, open
the assistant once, close it, and rerun the installer. If a source is missing,
repeat `set up my OKG for <name>` and review the new proposal.

Publication checks cover graph structure and configured operating conditions.
A failed check leaves readers on the previous generation; it does not establish
scientific validity, authorship, legal status, or completeness.

## Keep evidence with a result

When a result informs a paper, report, or lab decision, retain the source
identity, published `generation_id`, query or filters, and relevant deployment
configuration. This provenance supports reproducibility: another reader can
identify the graph version and evidence behind the result. AKMON may preserve a
DOI or another source identifier, but it does not create a formatted citation
or bibliography. See
[Read the evidence](../search/index.md#read-the-evidence).

The exact profile files live under
`src/okg/substrate/library/profiles/academic-researcher/`; its ontology module
lives under `src/okg/substrate/library/ontologies/academic_researcher/`. General source and
publication behavior is documented in
[Connect your data](../sources/index.md).
