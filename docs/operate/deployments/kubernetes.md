---
audience: operator
type: how-to
canonical_id: kubernetes-deploy
---
# Deploy an OKG on Kubernetes


The supported Kubernetes path is the Helm chart at
`ops/kubernetes/okg`. It packages the existing OKG lifecycle; it does not
introduce a second graph deployer:

1. a Postgres 17 StatefulSet with durable storage, or an operator-supplied
   external Postgres DSN;
2. an idempotent `okg provision` init container that must publish before the
   runtime starts;
3. one singleton `okg runtime worker`;
4. one read-only streamable-HTTP MCP sidecar, reachable only through a
   ClusterIP service and same-namespace NetworkPolicy by default; and
5. optional deployment-owned sidecars, such as an artifact mirror.

Deployments whose sources need Git history can enable a repository-sync init
stage. It checks out an explicit revision onto the runtime PVC before
provisioning, so the runtime image never contains `.git` metadata or checkout
credentials. The chart also points `OKG_DEPLOYMENTS_DIR` at that checkout;
deployment manifests and `${repo_root}` source paths therefore resolve from the
selected revision rather than the baked image snapshot.

Without repository sync, the deployment is read from the image at
`/opt/okg/deployments/<deployment>`, which is not a Git checkout. Such a
deployment must declare `release.kind: standalone` (see
[Deployments outside a Git checkout](external.md#deployments-outside-a-git-checkout));
otherwise every publish is refused with `deployment release authority is
unavailable`. The bundled `kubernetes-smoke` deployment declares it.

The runtime image runs as UID/GID 10001 with a read-only root filesystem.
Writable deployment data belongs on the runtime PVC. Credentials are accepted
only through an existing Kubernetes Secret. Bootstrap, worker, and mirror
processes use the `okg_service` Nomos subject; the network-facing MCP process
uses the separate `mcp_client` read subject. Extra Secret-backed environment
variables are scoped to named workloads, so a token that only repository
sync and the worker need never reaches MCP, the Helm test, or the
database-role initializer.

## Prerequisites

- Kubernetes 1.25 or newer.
- Helm 3 or 4.
- A CNI that enforces Kubernetes `NetworkPolicy`; same-namespace-only MCP
  access is not a security boundary on clusters that ignore those policies.
- A `ReadWriteOnce` StorageClass, named explicitly: the chart refuses an
  empty class name rather than fall back to the cluster default.
- The AKMON runtime and Postgres images in a registry reachable by the cluster.
- A Postgres 17 target with TimescaleDB, `pg_textsearch` and `pgvector`
  (`pg_cron` is optional; the chart does not preload it). The included image is suitable for a first installation; use an
  operated Postgres service for a larger production deployment.

Build locally:

```bash
docker build \
  --build-arg VCS_REF="$(git rev-parse HEAD)" \
  -t registry.example/okg:"$(git rev-parse --short=12 HEAD)" .
docker build -t registry.example/okg-postgres:pg17 ops/pg
```

Push both images, then pass their immutable tags with `--set`.

Each AKMON release `vX.Y.Z` publishes the runtime image
`ghcr.io/mitdbg/akmon:vX.Y.Z`, the Postgres image
`ghcr.io/mitdbg/akmon-postgres:vX.Y.Z`, and the chart
`oci://ghcr.io/mitdbg/charts/akmon` at version `X.Y.Z`. The released
chart already names both images and pulls the tag of its own release, so
an install from it needs no image values:

```bash
helm upgrade --install <release> oci://ghcr.io/mitdbg/charts/akmon \
  --version X.Y.Z --namespace <namespace> --values <your-values.yaml>
```

The chart in this source tree, `ops/kubernetes/okg`, names no default
registry and refuses to render until `images.okg.repository` and
`postgresql.image.repository` are set. Use it with images you built from
this source as shown above and pushed to your registry, as
`<your-registry>/okg` and `<your-registry>/okg-postgres`.

## Credentials and the database roles

Do not put secret values in a Helm values file or command history. Create one
Kubernetes Secret from operator-controlled files, mode `0600`. The chart reads
the Secret named by `secrets.existingSecret`; it has no default and never
creates one. These are its keys; a deployment's values may declare more for
its own workloads.

| Secret key | Contents |
| --- | --- |
| `postgres-password` | Password used by the included Postgres container |
| `okg-dsn` | Writer DSN used by provision and the runtime worker |
| `mcp-dsn` | Read-only DSN used by MCP (use a restricted database role) |
| `mcp-password` | Separate password for the least-privilege `okg_mcp` login |

```bash
kubectl create namespace <namespace>
kubectl -n <namespace> create secret generic <secret-name> \
  --from-file=postgres-password=/secure/path/postgres-password \
  --from-file=okg-dsn=/secure/path/okg-dsn \
  --from-file=mcp-dsn=/secure/path/mcp-dsn \
  --from-file=mcp-password=/secure/path/mcp-password
```

The chart's objects are named `<prefix>-postgres`, `<prefix>-runtime` and
`<prefix>-mcp`. `<prefix>` is `<release>-akmon` for the released chart
(`oci://ghcr.io/mitdbg/charts/akmon`) and `<release>-okg` for the chart in
this source tree (`ops/kubernetes/okg`), unless you set `nameOverride` or
`fullnameOverride`.

With the included database, the host is `<prefix>-postgres`, the port 5432,
and the database `okg` (`postgresql.database`). For release `myrel` of the
released chart, the `okg-dsn` file holds
`postgresql://postgres:<password>@myrel-akmon-postgres:5432/okg`, and
`mcp-dsn` uses the same host and database with `okg_mcp:<mcp-password>`.
URL-encode the password in the DSN files. When the deployment declares its
own database name (`postgres.database` in its `deployment.yaml`), set
`postgresql.database` and both DSNs to that name. `okg mcp-serve` accepts
only the declared database or an isolated copy of it (the declared name plus an
isolation suffix), so a DSN for a different database keeps the MCP sidecar from
starting. The bundled `kubernetes-smoke` deployment declares `okg`, the chart
default, so it installs with no database override.

The `mcp-dsn` username must be `okg_mcp` and its password must match the
`mcp-password` file. After provision applies the least-privilege grants, a
second init container enables that role for login and rotates its password.
The chart never renders those keys' content.

For an operated external Postgres service, provision the restricted
`okg_mcp` login through that service's role-management path and install with
`--set postgresql.internal.enabled=false --set mcp.manageRole=false`. The
supplied `mcp-dsn` must already authenticate as that role; the chart attempts
no cluster-level role mutation.

## Install

Write a values file for your deployment. It names the deployment baked into
the runtime image, the Secret above, the StorageClass for both volumes, and
the repository the worker checks out (`kubectl get storageclass` lists the
classes):

```yaml
okg:
  deployment: <deployment>
secrets:
  existingSecret: <secret-name>
postgresql:
  storage:
    storageClassName: <storage-class>
runtime:
  storage:
    storageClassName: <storage-class>
repositorySync:
  enabled: true
  url: https://<git-host>/<owner>/<repository>.git
bootstrap:
  requireApprovalRevision: true
```

Pin both images to the tag published for one full commit, and check out that
same commit. With `bootstrap.requireApprovalRevision: true`, the chart refuses
to render unless `bootstrap.approvedRevision` equals `repositorySync.revision`:

```bash
COMMIT="$(git rev-parse HEAD)"
helm upgrade --install <release> ops/kubernetes/okg \
  --namespace <namespace> \
  --values <your-values.yaml> \
  --set-string images.okg.repository=<your-registry>/okg \
  --set-string postgresql.image.repository=<your-registry>/okg-postgres \
  --set-string images.okg.tag="sha-${COMMIT}" \
  --set-string postgresql.image.tag="sha-${COMMIT}" \
  --set-string repositorySync.revision="${COMMIT}" \
  --set-string bootstrap.approvedRevision="${COMMIT}" \
  --timeout 30m \
  --wait
```

For registry-enforced immutability, set each image's `tag` to an empty string
and `images.okg.digest` / `postgresql.image.digest` to the `sha256:...`
manifest digest. The chart refuses a tag and a digest together.

A released chart can leave the tag and digest of both images empty: it then
pulls the runtime and Postgres images tagged with the chart's `appVersion`,
the images of the same release. The release packaging marks the chart it
publishes with the `Chart.yaml` annotation `okg.mitdbg.io/released: "true"`.
A source checkout, such as `ops/kubernetes/okg` here, carries `"false"`,
because no image is published for its `appVersion`; it refuses to render
without a tag or digest for each image instead of leaving the pods unable to
pull.

The runtime is a single replica, so the chart renders no PodDisruptionBudget
by default: a `minAvailable: 1` budget on one replica blocks every node drain
until someone deletes it. Set `podDisruptionBudget.enabled: true` only where
blocking voluntary evictions is the intent.

The worker Pod first checks out `repositorySync.revision`, then stays in
`Init` until `okg provision` has migrated the database, applied the catalog
and Nomos baseline, ingested the bootstrap sources, and published a
generation.

## Private registries, private Git hosts and Argo CD

These values cover what an external deployment otherwise has to patch into a
copy of the chart. All of them are off by default.

| Value | What it does |
| --- | --- |
| `images.pullSecrets` | Names of existing `kubernetes.io/dockerconfigjson` Secrets. Both StatefulSets and the proof pod pull with them. |
| `extraEnv` | Plain, non-secret environment variables, each with a `name`, a `value` and the `targets` that receive it (`bootstrap`, `worker`, `mcp`, `mirror`). Values render into the pod spec in clear text; put credentials in `secrets.extraEnv` instead. |
| `gitCredentials.hosts` | Private Git hosts that sources clone from. Each entry names a `host`, a `username` and the `secretKey` in `secrets.existingSecret` that holds the token. |
| `tests.annotations` | Annotations on the proof pod. The default makes it a `helm test` hook. |

The chart refuses an `extraEnv` or `secrets.extraEnv` name that it sets
itself, and the same name set twice for one container. Reserved names include
`OKG_DSN`, `OKG_RUNTIME_SUPERVISOR`, `GIT_TERMINAL_PROMPT`, `GIT_CONFIG_COUNT`,
`GIT_CONFIG_PARAMETERS`, and any name starting with `GIT_CONFIG_KEY_`,
`GIT_CONFIG_VALUE_` or `GIT_CRED_TOKEN_`. Other Git settings such as
`GIT_CONFIG_GLOBAL` and `GIT_CONFIG_NOSYSTEM` stay available.

Kubernetes expands `$(VAR)` inside an `extraEnv` value when `VAR` is set
earlier in the same container's environment, and leaves it as written
otherwise. Write `$$(VAR)` for a literal `$(VAR)`.

For each Git host, the bootstrap and worker containers get a Git credential
helper through `GIT_CONFIG_COUNT`, `GIT_CONFIG_KEY_<n>` and
`GIT_CONFIG_VALUE_<n>` (Git 2.31 or newer). The helper prints the token from
an environment variable that a `secretKeyRef` fills, so the token never
appears in a source URL, the source registry, a values file, the command line
or a log. Git prompts are off, so a host with no entry fails at once instead of
waiting for a terminal. MCP, the mirror and the proof pod never receive the
tokens. The repository-sync checkout keeps its own `repositorySync.authSecretKey`.

```yaml
images:
  pullSecrets: [registry-pull]
extraEnv:
  - name: EXAMPLE_DATA_ROOT
    value: /var/lib/okg
    targets: [bootstrap, worker, mcp]
gitCredentials:
  hosts:
    - host: gitlab.example.org
      username: oauth2
      secretKey: gitlab-token
```

Argo CD never runs `helm test`. To run the proof pod after every Argo CD
sync, replace the Helm hook with an Argo CD one. A `null` value removes a
default key:

```yaml
tests:
  annotations:
    helm.sh/hook: null
    helm.sh/hook-delete-policy: null
    argocd.argoproj.io/hook: PostSync
    argocd.argoproj.io/hook-delete-policy: BeforeHookCreation
```

The chart refuses annotations with neither `helm.sh/hook` nor
`argocd.argoproj.io/hook`, because the proof pod would then be created as an
ordinary pod on every install. It also refuses a hook value that is empty or
not a known hook type. `helm.sh/hook` accepts `test`, `test-success`,
`pre-install`, `post-install`, `pre-upgrade`, `post-upgrade`, `pre-delete`,
`post-delete`, `pre-rollback` and `post-rollback`; `argocd.argoproj.io/hook`
accepts `PreSync`, `Sync`, `PostSync`, `SyncFail`, `Skip`, `PreDelete` and
`PostDelete`. Both accept a comma-separated list.

Each `gitCredentials.hosts` entry must name a different `host`; the chart
refuses a host listed twice.

## Check it is ready

```bash
kubectl -n <namespace> rollout status \
  statefulset/<prefix>-postgres --timeout=10m
kubectl -n <namespace> rollout status \
  statefulset/<prefix>-runtime --timeout=30m
helm test <release> -n <namespace>
kubectl -n <namespace> exec statefulset/<prefix>-runtime -c worker -- \
  okg generation list --deployment <deployment> --limit 1 --json
kubectl -n <namespace> exec statefulset/<prefix>-runtime -c worker -- \
  okg doctor --deployment <deployment> --json
```

If the runtime rollout does not finish, read the init containers' logs:

```bash
kubectl -n <namespace> logs statefulset/<prefix>-runtime \
  -c repository-sync
kubectl -n <namespace> logs statefulset/<prefix>-runtime -c bootstrap
```

The latest generation must be `published` and doctor must be clean; a paused
publish schedule only warns. The MCP
service is reachable only inside the namespace; for an operator-only local
connection, run
`kubectl -n <namespace> port-forward service/<prefix>-mcp 8100:8100`.

## Upgrades and removal

`helm rollback` restores Kubernetes objects; it does not reverse a database
migration. Take a database backup before an upgrade that changes the image or
the checked-out `repositorySync.revision`, and roll back only after the
database compatibility boundary is understood.

`okg up` does not upgrade a chart release; `helm upgrade` does, and the chart
does not yet run a backup or compatibility hook for you. Before upgrading to a
new image:

1. Check the deployment against the new okg: run
   `okg deployment compat <deployment> --against <old-contract.json> --json`
   from the new image, for example in a one-off pod of the new tag. Exit 1
   means a blocker; exit 2 means compatibility is unknown. Exit 0 is not
   enough on its own: also read `breaking_since.affecting_this_deployment` in
   the JSON, which lists breaking changes that touch this deployment and does
   not change the exit code. Treat any row there as a blocker. The old
   contract is what the old image prints for
   `python -c 'from okg.substrate.release_contract import build_contract, render_contract; print(render_contract(build_contract()), end="")'`.
2. Back up the graph database with `pg_dump --format=custom`, and check that
   the archive restores into a scratch database (`okg db clone
   --restore-dump` does this). Pass the password through `PGPASSWORD` or a
   `PGPASSFILE`, never on the command line.
3. Run `helm upgrade`, then the checks in [Check it is ready](#check-it-is-ready).

The StatefulSets' storage templates carry no chart or app version, so a chart
version bump upgrades in place. A release installed before that change has
version labels in those templates, and Kubernetes refuses to change them: its
first upgrade fails with `updates to statefulset spec for fields other than
... are forbidden`. Run this once: delete the two StatefulSets without
deleting their Pods or claims, then upgrade. The new StatefulSets reuse the
existing claims, whose names do not change. Omit `<prefix>-postgres`
when the chart's internal database is disabled:

```bash
kubectl -n <namespace> delete statefulset --cascade=orphan \
  <prefix>-runtime <prefix>-postgres
helm upgrade --install <release> ops/kubernetes/okg ...
```

Deleting the Helm release does not delete StatefulSet PVCs automatically.
Delete those claims only when the graph is intentionally destroyed and a
recoverable backup exists.
