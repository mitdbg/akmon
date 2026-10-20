#!/usr/bin/env bash
#
# install.sh — bootstrap script for a fresh OKG operator.
# Per `dead-simple-install` Item 1B + Option B (2026-05-21).
#
# What it does, in order:
#
#   1. Verify Docker is installed + the daemon is reachable, only when
#      the install starts its own PostgreSQL: `--profile
#      academic-researcher` without --postgres-dsn. The default
#      `minimal` profile never starts a database, so it never needs
#      Docker; it needs --postgres-dsn, or a terminal to ask on.
#   2. Install `uv` (Astral's Python package manager) if absent.
#      We use uv because it's faster than pip and handles
#      Python-version pinning + venv creation atomically.
#   3. Clone the OKG repository to ~/okg/ (or update it if already
#      there) over HTTPS. A publicly readable repository needs no
#      credential; a private one needs a GitHub credential first
#      (`gh auth login` or a credential helper).
#   4. Set up the venv + install the okg package in editable mode,
#      and link its `okg` command into ~/.local/bin (OKG_BIN_DIR), the
#      folder uv's own installer and Debian/Ubuntu's ~/.profile put on
#      PATH, so the next shell finds it.
#   5. Run `okg install` with any args the operator passed
#      through. The log line masks a DSN password as ***.
#
# Usage, from a clone:
#
#   git clone https://github.com/mitdbg/akmon.git ~/okg
#   export OKG_DSN='postgresql://<user>:<password>@<host>:5432/<dbname>'
#   ~/okg/install.sh --deployment-name <name> --postgres-dsn '${OKG_DSN}'
#
# The single-quoted ${OKG_DSN} keeps the password off the command line:
# `okg install` expands it from the environment itself.
#
# With no `--profile`, `okg install` builds the empty `minimal`
# starter graph in the PostgreSQL database you name, and you grow it
# with `okg deployment add-source`. It asks for the two answers on a
# terminal; without one, pass them as flags. For the one-command
# research-group install, which starts its own PostgreSQL in Docker
# and needs no other argument:
#
#   ~/okg/install.sh --profile academic-researcher [--orcid 0000-0001-...]
#
# `okg install --help` lists every profile. Without a clone, from a
# publicly readable repository:
#
#   curl -fsSL <raw URL of this install.sh on the default branch> | bash -s -- <args>
#
# Any args this script receives forward to `okg install`.

set -euo pipefail

# HTTPS is the default path. A private repository needs GitHub
# authentication over HTTPS first (`gh auth login`, a credential
# helper, or a PAT-backed URL via OKG_REPO_URL). Operators with a
# preference can override via `OKG_REPO_URL=...`.
REPO_URL="${OKG_REPO_URL:-https://github.com/mitdbg/akmon.git}"
REPO_BRANCH="${OKG_REPO_BRANCH:-main}"
INSTALL_ROOT="${OKG_INSTALL_ROOT:-${HOME}/okg}"
BIN_DIR="${OKG_BIN_DIR:-${HOME}/.local/bin}"
# PATH as the operator's shell had it, before ensure_uv adds ~/.local/bin
# for this script only: the final message says whether THEIR shell finds
# the linked command.
ORIGINAL_PATH="${PATH}"

# ---------- helpers ----------

log() { printf "==> %s\n" "$*" >&2; }
die() { printf "ERROR: %s\n" "$*" >&2; exit 1; }


require_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        if [[ "$(uname -s)" == "Darwin" ]]; then
            die "Docker not found on PATH. --profile academic-researcher
     starts its own PostgreSQL in Docker. Install Docker Desktop from
       https://www.docker.com/products/docker-desktop/
     then re-run this script, or pass --postgres-dsn to use a database
     you already have."
        fi
        die "Docker not found on PATH. --profile academic-researcher
     starts its own PostgreSQL in Docker. Install Docker Engine with the
     Compose plugin:
       https://docs.docker.com/engine/install/
     then re-run this script, or pass --postgres-dsn to use a database
     you already have."
    fi
    # Triage `docker info` failures instead of blaming the daemon
    # for everything. On Linux the daemon is typically RUNNING
    # while the operator isn't in the `docker` group — the real
    # error is "permission denied" on the socket, and "start
    # Docker Desktop" is wrong twice over (it's running; there is
    # no Docker Desktop on a Linux server).
    local docker_err
    if ! docker_err="$(docker info 2>&1 >/dev/null)"; then
        if echo "${docker_err}" | grep -qi "permission denied"; then
            die "Docker is running, but this user can't reach it
     (permission denied on the Docker socket).
     Fix:   sudo usermod -aG docker \$USER
     then LOG OUT and back in (or run: newgrp docker),
     and re-run this script."
        fi
        die "Docker is installed but the daemon isn't reachable.
     Linux:  sudo systemctl enable --now docker
     macOS:  start Docker Desktop and wait for it to finish booting
     then re-run this script.
     (docker info said: ${docker_err})"
    fi
    log "Docker daemon reachable."
    # The Compose v2 plugin ships separately from the docker CLI on
    # most Linux distros (docker.io without docker-compose-v2). When
    # it's absent, `docker compose up -d --build` falls through to
    # the bare docker CLI and dies mid-install with the cryptic
    # \"unknown shorthand flag: 'd' in -d\" (observed live on a PI's
    # first install, 2026-06-09). Fail here, with the fix.
    if ! docker compose version >/dev/null 2>&1; then
        die "Docker is running, but the Compose v2 plugin is missing
     ('docker compose' is not available).
     Ubuntu/Debian:  sudo apt-get install docker-compose-v2
     Fedora/RHEL:    sudo dnf install docker-compose-plugin
     macOS:          update Docker Desktop (Compose is bundled)
     then re-run this script."
    fi
    log "Docker Compose plugin present."
}


# The --profile the forwarded args name ("" when none), and whether they
# name a database with --postgres-dsn ("1" or "").
ARG_PROFILE=""
ARG_DSN=""
scan_args() {
    ARG_PROFILE=""
    ARG_DSN=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --postgres-dsn|--postgres-dsn=*) ARG_DSN=1 ;;
            --profile) ARG_PROFILE="${2:-}"; [[ $# -gt 1 ]] && shift ;;
            --profile=*) ARG_PROFILE="${1#--profile=}" ;;
        esac
        shift
    done
}


# Whether the `okg install` args this script forwards need Docker.
# Only the academic-researcher installer starts a PostgreSQL container,
# and only when no --postgres-dsn names an existing database. The default
# `minimal` profile, and every other profile, never starts one.
needs_docker() {
    scan_args "$@"
    [[ "${ARG_PROFILE}" == "academic-researcher" && -z "${ARG_DSN}" ]]
}


# Whether the args leave the default profile without a database: no
# --profile (or `--profile minimal`) and no --postgres-dsn.
needs_dsn() {
    local arg
    for arg in "$@"; do
        case "${arg}" in
            --help|-h|--dry-run) return 1 ;;
        esac
    done
    scan_args "$@"
    [[ -z "${ARG_DSN}" && ( -z "${ARG_PROFILE}" || "${ARG_PROFILE}" == "minimal" ) ]]
}


# The default profile with no --postgres-dsn: 'okg install' asks for the
# database on a terminal. Without one (curl | bash, CI, a redirect) it
# cannot ask, so say what to pass instead of failing later.
require_dsn_or_terminal() {
    if [[ -t 0 ]]; then
        log "No --postgres-dsn given; the installer will ask for the database."
        return
    fi
    # The ${OKG_DSN} below is literal on purpose: it is what to type.
    # shellcheck disable=SC2016
    die 'The default profile builds its graph in a PostgreSQL database you
     already have. No --postgres-dsn was given, and there is no terminal
     to ask on. Name the database, keeping the password off the command
     line:

       export OKG_DSN="postgresql://<user>:<password>@<host>:5432/<dbname>"
       install.sh --deployment-name NAME --postgres-dsn '"'"'${OKG_DSN}'"'"'

     Keep the single quotes: the installer reads OKG_DSN itself, so the
     password stays out of the process list and your shell history.

     No database yet? The OKG source includes one (it needs Docker with
     Compose v2). Clone OKG where this installer will reuse it, then
     start the database:

       git clone --branch '"${REPO_BRANCH}"' '"${REPO_URL}"' '"${INSTALL_ROOT}"'
       cd '"${INSTALL_ROOT}"'/ops/pg && docker compose up -d --build --wait

     It listens on 127.0.0.1:5433; the database is okg, the user
     postgres and the password okg.
     Or use --profile academic-researcher, which starts its own
     PostgreSQL in Docker.'
}


ensure_uv() {
    if command -v uv >/dev/null 2>&1; then
        log "uv already installed ($(uv --version 2>/dev/null | head -1))."
        return
    fi
    log "Installing uv (Astral's Python package manager)..."
    # Astral's official installer — drops `uv` into ~/.local/bin
    # by default. Idempotent.
    curl -fsSL https://astral.sh/uv/install.sh | sh
    # Ensure ~/.local/bin is on PATH for the rest of this script.
    export PATH="${HOME}/.local/bin:${PATH}"
    if ! command -v uv >/dev/null 2>&1; then
        die "uv install completed but 'uv' isn't on PATH.
     Add ~/.local/bin to your PATH and re-run."
    fi
}


clone_or_update_repo() {
    if git -C "${INSTALL_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        log "OKG repo already at ${INSTALL_ROOT}; updating..."
        git -C "${INSTALL_ROOT}" fetch --quiet origin "${REPO_BRANCH}"
        git -C "${INSTALL_ROOT}" checkout --quiet "${REPO_BRANCH}"
        git -C "${INSTALL_ROOT}" pull --quiet --ff-only
        return
    fi
    if [[ -e "${INSTALL_ROOT}" ]] && [[ -n "$(ls -A "${INSTALL_ROOT}" 2>/dev/null || true)" ]]; then
        die "${INSTALL_ROOT} exists but is not a git checkout.
     Move it aside or set OKG_INSTALL_ROOT to an empty directory."
    fi
    log "Cloning OKG into ${INSTALL_ROOT}..."
    mkdir -p "$(dirname "${INSTALL_ROOT}")"
    # Capture stderr so we can recognize auth failures and
    # print an actionable message instead of bash's default
    # "fatal: …" stack.
    if ! git clone --quiet --branch "${REPO_BRANCH}" \
            "${REPO_URL}" "${INSTALL_ROOT}" 2> /tmp/okg-clone.err
    then
        local err
        err="$(cat /tmp/okg-clone.err 2>/dev/null || true)"
        rm -f /tmp/okg-clone.err || true
        if echo "$err" | grep -qE \
            "Authentication failed|could not read Username|Permission denied|publickey|could not read from remote|Repository not found"
        then
            # owner/repo of an https GitHub URL, for the auth hints below.
            local repo_path="<owner>/<repo>"
            if [[ "${REPO_URL}" == https://github.com/* ]]; then
                repo_path="${REPO_URL#https://github.com/}"
                repo_path="${repo_path%/}"
                repo_path="${repo_path%.git}"
            fi
            die "Couldn't clone ${REPO_URL}.

The repository refused access. If it is private, to install:

  1. Make sure your GitHub account can read ${repo_path}.
  2. Authenticate. Pick whichever you already have:

     a) GitHub SSH keys already set up? Zero new setup:
          export OKG_REPO_URL=git@github.com:${repo_path}.git
          ${0}
     b) GitHub CLI (works headless via device code):
          gh auth login        # install first: sudo apt install gh
     c) A personal access token (repo scope):
          export OKG_REPO_URL=https://<USER>:<PAT>@github.com/${repo_path}.git
          ${0}

  3. Test access:
       git ls-remote ${REPO_URL}   # (or the ssh URL)

A publicly readable repository needs none of this: a plain
HTTPS clone or curl-pipe-bash works."
        fi
        die "git clone failed: ${err}"
    fi
}


install_venv() {
    cd "${INSTALL_ROOT}"
    log "Setting up Python venv via uv..."
    # `uv sync` creates .venv and installs all declared deps.
    # Subsequent runs are incremental.
    uv sync --quiet
}


# The args with any DSN password replaced by ***, the way okg's own
# redactor prints a DSN: postgresql://user:***@host/db and password=***.
# libpq ends a URL password at the first '@', but the mask runs to the
# last '@' before the path, so a mistyped unencoded '@' cannot leak part
# of it (it over-masks instead). It stops only at '/' or whitespace: a
# password may hold '#' or '?'. A quoted keyword value (password='a b')
# is masked whole, quotes included.
masked_args() {
    local arg out=()
    for arg in "$@"; do
        out+=("$(printf '%s' "${arg}" | sed -E \
            -e 's|(://[^/:@[:space:]]*:)[^/[:space:]]*@|\1***@|g' \
            -e "s#([Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd][[:space:]]*=[[:space:]]*)'([^'\\\\]|\\\\.)*'#\\1***#g" \
            -e 's#([Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd][[:space:]]*=[[:space:]]*)[^[:space:]]+#\1***#g')")
    done
    printf '%s' "${out[*]-}"
}


# Link the checkout's command into BIN_DIR so the next shell finds it.
# Replace only a link this installer owns: one that is dangling or points
# into INSTALL_ROOT. uv tool and pipx put their own links here.
# LINKED_OKG records the outcome for path_hint.
LINKED_OKG=""
link_okg_on_path() {
    local target="${INSTALL_ROOT}/.venv/bin/okg"
    local link="${BIN_DIR}/okg"
    local current dir dir_phys root root_phys
    [[ -x "${target}" ]] || die "uv sync did not create ${target}."
    mkdir -p "${BIN_DIR}"
    root="${INSTALL_ROOT%/}"
    if ! root_phys="$(cd "${root}" 2>/dev/null && pwd -P)"; then
        root_phys="${root}"
    fi
    if [[ -L "${link}" ]]; then
        current="$(readlink "${link}")"
        # A relative target is relative to the link's own folder.
        case "${current}" in
            /*) ;;
            *) current="${BIN_DIR%/}/${current}" ;;
        esac
        if [[ -e "${link}" ]]; then
            # Compare physical paths: no '..', no symlinked folders.
            dir="$(dirname "${current}")"
            if dir_phys="$(cd "${dir}" 2>/dev/null && pwd -P)"; then
                dir="${dir_phys}"
            fi
            current="${dir}/$(basename "${current}")"
            case "${current}" in
                "${root}"/*|"${root_phys}"/*) ;;
                *)
                    log "WARNING: ${link} links to ${current}, another okg install;"
                    log "leaving it alone. This install's command is ${target}."
                    return
                    ;;
            esac
        fi
    elif [[ -e "${link}" ]]; then
        log "WARNING: ${link} exists and is not a link; leaving it alone."
        log "This install's command is ${target}."
        return
    fi
    ln -sfn "${target}" "${link}"
    LINKED_OKG="${link}"
    log "Linked ${link} -> ${target}"
}


# Say which okg the operator's own shell runs: resolve 'okg' under the
# PATH they started with and compare it to the link.
path_hint() {
    local target="${INSTALL_ROOT}/.venv/bin/okg"
    local found
    found="$(PATH="${ORIGINAL_PATH}"; hash -r; command -v okg 2>/dev/null || true)"
    if [[ -z "${LINKED_OKG}" ]]; then
        log "okg was not linked into ${BIN_DIR}. Run this install's command as"
        log "  ${target}"
        if [[ -n "${found}" ]]; then
            log "In your shell, 'okg' runs ${found}, not this install."
        fi
        return
    fi
    if [[ "${found}" == "${LINKED_OKG}" || "${found}" -ef "${LINKED_OKG}" ]]; then
        log "'okg' is on your PATH (${LINKED_OKG}); run the next steps as printed."
        return
    fi
    if [[ -n "${found}" ]]; then
        log "WARNING: in your shell, 'okg' runs ${found}, not this install's"
        log "${LINKED_OKG}. Put ${BIN_DIR} first on your PATH:"
        log "  export PATH=\"${BIN_DIR}:\$PATH\""
        log "or remove the other okg, or run ${target} directly."
        return
    fi
    log "'okg' is in ${BIN_DIR}, which this shell's PATH does not include."
    log "A new terminal finds it if your shell profile adds ${BIN_DIR}"
    log "(uv's installer and Debian/Ubuntu's ~/.profile do). Otherwise run"
    log "  export PATH=\"${BIN_DIR}:\$PATH\""
    log "and add that line to ~/.bashrc or ~/.zshrc."
}


run_install() {
    cd "${INSTALL_ROOT}"
    log "Running 'okg install' with: $(masked_args "$@")"
    # All args this script received pass through. With no --profile
    # this builds the minimal starter (issue #3414); the
    # academic-researcher profile's smart defaults (deployment name,
    # pi-name from git config, Docker-assigned port) apply only with
    # --profile academic-researcher.
    uv run okg install "$@"
}


# ---------- main ----------

main() {
    log "OKG bootstrap starting."
    if needs_docker "$@"; then
        require_docker
    else
        log "This install starts no database container; skipping the Docker check."
    fi
    if needs_dsn "$@"; then
        require_dsn_or_terminal
    fi
    ensure_uv
    clone_or_update_repo
    install_venv
    link_okg_on_path
    run_install "$@"
    log "Done. Follow the next steps printed above."
    path_hint
}

# OKG_INSTALL_SH_SOURCE_ONLY=1 lets a test source the helpers
# without running the bootstrap.
if [[ -z "${OKG_INSTALL_SH_SOURCE_ONLY:-}" ]]; then
    main "$@"
fi
