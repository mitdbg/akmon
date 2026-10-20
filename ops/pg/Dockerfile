# Local-dev Postgres for OKG.
#
# Base: timescaledb-ha pg17-all image which already bundles
# TimescaleDB (community + TSL), pgvector, pg_cron, pg_stat_statements,
# pg_textsearch (Timescale's BM25 extension), and several others we
# won't enable.
#
# pg_textsearch: the base image bundles 1.3.0, whose index scan repeats and
# drops rows when tied scores span segments (issue #3335, fixed upstream in
# 1.3.1) and stops silently at 100,000 results (issue #3162, removed in
# 1.5.0). No published timescaledb-ha image ships 1.5.1 yet, so the RUN
# below installs upstream's own release package over the bundled copy, the
# way timescaledb-docker-ha's build_scripts/shared_install.sh
# (install_pg_textsearch) installs it: download the release zip, unzip it,
# dpkg-install the .deb inside. The zip is pinned by sha256, so a changed
# or wrong file fails the build. An earlier version of this file copied
# .so/.sql/.control files out of the zip, which holds only the .deb, so it
# copied nothing and shipped the base image's 1.3.0; the checks after the
# install (package version, control file, upgrade script, library) fail the
# build if that ever happens again.
#
# The substrate uses only Apache-2 / community features; no TSL code
# is on the hot path.
FROM docker.io/timescale/timescaledb-ha:pg17.10-ts2.29.0-all@sha256:d3300a7bdbc77a4ec35289eda5083ba4580aa23ba13dfc6cf77826771e5338e6

ARG VCS_REF=unknown
ARG POSTGRES_VERSION=17.10
ARG TIMESCALEDB_VERSION=2.29.0
ARG PG_TEXTSEARCH_VERSION=1.5.1
# sha256 of the release assets pg-textsearch-v${PG_TEXTSEARCH_VERSION}-pg17-<arch>.zip
# from https://github.com/timescale/pg_textsearch/releases/tag/v1.5.1. To
# refresh after a version bump, download each asset and run `sha256sum` on it;
# the value must equal the asset's `digest` in
# `gh api repos/timescale/pg_textsearch/releases/tags/v<version>`. Both
# architectures are pinned because .github/workflows/container-images.yml
# publishes this image for linux/amd64 and linux/arm64, and a local build on
# Apple Silicon builds arm64 (docs/develop/index.md).
ARG PG_TEXTSEARCH_SHA256_AMD64=ed4410e8b6879a8971cd87fd03853f182e47b85b199c9bfcd9b34f159caba9fb
ARG PG_TEXTSEARCH_SHA256_ARM64=e65f953e99f47c05dde88a031884cf36ea269bcb62377f554b76ddb4d13fdc3e
# Every version from the base image's bundled one to PG_TEXTSEARCH_VERSION:
# a database created on the base image upgrades along this chain with
# `ALTER EXTENSION pg_textsearch UPDATE`, so each step's script must exist.
ARG PG_TEXTSEARCH_UPGRADE_PATH="1.3.0 1.3.1 1.4.0 1.5.0 1.5.1"

USER root
RUN set -eux; \
    ARCH="$(dpkg --print-architecture)"; \
    case "$ARCH" in \
      amd64) PGTS_SHA256="$PG_TEXTSEARCH_SHA256_AMD64" ;; \
      arm64) PGTS_SHA256="$PG_TEXTSEARCH_SHA256_ARM64" ;; \
      *) echo "pg_textsearch: no pinned release asset for $ARCH" >&2; exit 1 ;; \
    esac; \
    WORK="$(mktemp -d)"; \
    ASSET="pg-textsearch-v${PG_TEXTSEARCH_VERSION}-pg17-${ARCH}.zip"; \
    # The build downloads from github.com: retried, https only; the sha256
    # below is what makes the file trustworthy.
    curl --silent --show-error --fail --location --proto '=https' --tlsv1.2 \
      --retry 5 --retry-all-errors --output "$WORK/$ASSET" \
      "https://github.com/timescale/pg_textsearch/releases/download/v${PG_TEXTSEARCH_VERSION}/${ASSET}"; \
    echo "$PGTS_SHA256  $WORK/$ASSET" | sha256sum --check --strict -; \
    unzip -q "$WORK/$ASSET" -d "$WORK"; \
    mkdir -p /var/lib/okg/pg_textsearch-dpkg/updates /var/lib/okg/pg_textsearch-dpkg/info; \
    touch /var/lib/okg/pg_textsearch-dpkg/status; \
    dpkg --install --admindir=/var/lib/okg/pg_textsearch-dpkg --log="$WORK/dpkg.log" \
      --force-depends --force-overwrite "$WORK"/pg-textsearch-postgresql-17_*.deb; \
    # Remove every pg_textsearch file the package does not own: the base
    # image's own install script for its bundled version (1.3.0) would let
    # `CREATE EXTENSION pg_textsearch VERSION '1.3.0'` build 1.3.0 objects on
    # the new library. Upgrades use only the package's --X--Y scripts.
    dpkg-query --admindir=/var/lib/okg/pg_textsearch-dpkg -L pg-textsearch-postgresql-17 > "$WORK/owned"; \
    for f in "$(pg_config --sharedir)"/extension/pg_textsearch* "$(pg_config --pkglibdir)"/pg_textsearch*; do \
      grep -qxF "$f" "$WORK/owned" || rm -v "$f"; \
    done; \
    rm -rf "$WORK"

RUN set -eux; \
    CONTROL_FILE="$(pg_config --sharedir)/extension/timescaledb.control"; \
    DEFAULT_VERSION="$(sed -n "s/^default_version = '\([^']*\)'/\1/p" "$CONTROL_FILE")"; \
    test "$DEFAULT_VERSION" = "$TIMESCALEDB_VERSION"; \
    test "$(dpkg-query --admindir=/var/lib/okg/pg_textsearch-dpkg -W -f '${Version}' pg-textsearch-postgresql-17)" = "${PG_TEXTSEARCH_VERSION}-1"; \
    PGTS_CONTROL="$(pg_config --sharedir)/extension/pg_textsearch.control"; \
    PGTS_VERSION="$(sed -n "s/^default_version = '\([^']*\)'/\1/p" "$PGTS_CONTROL")"; \
    test "$PGTS_VERSION" = "$PG_TEXTSEARCH_VERSION"; \
    test -f "$(pg_config --sharedir)/extension/pg_textsearch--${PG_TEXTSEARCH_VERSION}.sql"; \
    test ! -e "$(pg_config --sharedir)/extension/pg_textsearch--1.3.0.sql"; \
    PREV=""; \
    for NEXT in $PG_TEXTSEARCH_UPGRADE_PATH; do \
      if [ -n "$PREV" ]; then test -f "$(pg_config --sharedir)/extension/pg_textsearch--${PREV}--${NEXT}.sql"; fi; \
      PREV="$NEXT"; \
    done; \
    test "$PREV" = "$PG_TEXTSEARCH_VERSION"; \
    test -f "$(pg_config --pkglibdir)/pg_textsearch.so"

# The library's own version, exactly: load it in a throwaway cluster in
# single-user mode (no listener; `reject` auth) and read the
# `pg_textsearch.library_version` setting it defines.
USER postgres
RUN set -eux; \
    TMPPG="$(mktemp -d)"; \
    "$(pg_config --bindir)/initdb" --no-sync --auth=reject -U postgres -D "$TMPPG/data" > /dev/null; \
    LIB_VERSION="$(echo 'SHOW pg_textsearch.library_version;' \
      | "$(pg_config --bindir)/postgres" --single -D "$TMPPG/data" \
          -c shared_preload_libraries=pg_textsearch postgres \
      | sed -n 's/^[[:space:]]*1: pg_textsearch.library_version = "\([^"]*\)".*/\1/p')"; \
    rm -rf "$TMPPG"; \
    test "$LIB_VERSION" = "$PG_TEXTSEARCH_VERSION"
USER root

# Worker slots (issue #3143). PostgreSQL's default max_worker_processes = 8 is
# too small for TimescaleDB: every database with the extension takes a
# scheduler slot, and an okg database schedules 24 jobs, all retried without
# limit. Five okg databases filled the 8 slots and looped job-launch failures
# into the job-history table until the disk filled. 32 matches
# ops/environments/*/postgres-capacity.yaml. initdb copies this sample into
# every new cluster's postgresql.conf, so a fresh container with ops/pg/initdb
# mounted gets 32 whatever command starts it. (Without that mount the base
# image's own timescaledb-tune script runs and sizes it from the host's CPUs
# instead.) docker-compose.yaml and its bench and golden overlays also pass it
# with -c, so a data volume created before this line gets it too; the
# required-CI compose file relies on this default. See ops/pg/README.md.
RUN set -eux; \
    SAMPLE="$(pg_config --sharedir)/postgresql.conf.sample"; \
    printf '\n# OKG (ops/pg/Dockerfile, issue #3143)\nmax_worker_processes = 32\n' >> "$SAMPLE"; \
    test "$(sed -n 's/^max_worker_processes = \([0-9]*\)$/\1/p' "$SAMPLE")" = 32

LABEL org.opencontainers.image.title="OKG PostgreSQL" \
      org.opencontainers.image.description="PostgreSQL runtime for OKG deployments" \
      org.opencontainers.image.source="https://github.com/mitdbg/akmon" \
      io.okg.postgresql.version="${POSTGRES_VERSION}" \
      io.okg.timescaledb.version="${TIMESCALEDB_VERSION}" \
      io.okg.pg_textsearch.version="${PG_TEXTSEARCH_VERSION}"

USER postgres

# Baseline healthcheck config honored by compose.
# pg_isready gives up (-t 2) well before Docker's timeout kills the shell,
# which makes an orphaned pg_isready under a Postgres running as PID 1 (a
# plain `docker run` without --init) unlikely. It cannot rule it out: a
# severe IO stall can delay the start past the margin. Only an init closes
# it; see the note in ops/pg/docker-compose.yaml.
HEALTHCHECK --interval=5s --timeout=5s --retries=20 \
  CMD pg_isready -t 2 -U postgres -d okg || exit 1

# Keep the per-build revision at the end so changing the SHA does not invalidate
# the extension-check layer above.
LABEL org.opencontainers.image.revision="${VCS_REF}"
