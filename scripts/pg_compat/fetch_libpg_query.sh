#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PINS="${PG_COMPAT_PINS:-${ROOT}/tests/pg_compat/upstream_pins.json}"
CACHE="${PG_COMPAT_CACHE:-/tmp/parsersql-pg-compat}"
WITH_POSTGRES_SOURCE=0
LOCK_DIR="${CACHE}/.pg_compat.lock"
LOCK_OWNER_PATTERN=$'^([1-9][0-9]*)\t([A-Za-z0-9][A-Za-z0-9._-]*)\n$'
LOCK_OWNER_RECORD=
LOCK_OWNER_TMP=
LOCK_RECLAIM_DIR=
LOCK_RECLAIM_FILE=
LOCK_HELD=0
FILE_CONTENT=
ACTIVE_CHILD_PID=

usage() {
    echo "Usage: $0 [--with-postgres-source]" >&2
}

if [[ $# -gt 1 ]]; then
    usage
    exit 2
fi
if [[ $# -eq 1 ]]; then
    if [[ "$1" != "--with-postgres-source" ]]; then
        usage
        exit 2
    fi
    WITH_POSTGRES_SOURCE=1
fi

cd "$ROOT"

read_file_exact() {
    local path="$1"

    if [[ ! -f "$path" ]]; then
        return 1
    fi
    FILE_CONTENT="$(cat "$path"; printf '\034')" || return 1
    FILE_CONTENT="${FILE_CONTENT%$'\034'}"
}

restore_reclaim_owner() {
    if [[ -z "$LOCK_RECLAIM_FILE" || ! -f "$LOCK_RECLAIM_FILE" ]]; then
        if [[ -n "$LOCK_RECLAIM_DIR" ]]; then
            rmdir "$LOCK_RECLAIM_DIR" 2>/dev/null || true
            LOCK_RECLAIM_DIR=
            LOCK_RECLAIM_FILE=
        fi
        return
    fi
    if [[ -d "$LOCK_DIR" && ! -e "$LOCK_DIR/owner" ]]; then
        if ln "$LOCK_RECLAIM_FILE" "$LOCK_DIR/owner" 2>/dev/null; then
            rm -f "$LOCK_RECLAIM_FILE"
            rmdir "$LOCK_RECLAIM_DIR" 2>/dev/null || true
            LOCK_RECLAIM_DIR=
            LOCK_RECLAIM_FILE=
        fi
        return
    fi
    rm -f "$LOCK_RECLAIM_FILE"
    rmdir "$LOCK_RECLAIM_DIR" 2>/dev/null || true
    LOCK_RECLAIM_DIR=
    LOCK_RECLAIM_FILE=
}

release_lock() {
    restore_reclaim_owner
    if [[ "$LOCK_HELD" -eq 1 && -d "$LOCK_DIR" ]]; then
        if [[ ! -e "$LOCK_DIR/owner" ]]; then
            rmdir "$LOCK_DIR" 2>/dev/null || true
        elif read_file_exact "$LOCK_DIR/owner" &&
            [[ "$FILE_CONTENT" == "$LOCK_OWNER_RECORD" ]]; then
            rm -f "$LOCK_DIR/owner"
            rmdir "$LOCK_DIR" 2>/dev/null || true
        fi
    fi
    if [[ -n "$LOCK_OWNER_TMP" ]]; then
        rm -f "$LOCK_OWNER_TMP"
        LOCK_OWNER_TMP=
    fi
    LOCK_HELD=0
}

reclaim_stale_lock() {
    local initial_owner
    local owner_pid
    local reclaim_dir

    if ! read_file_exact "$LOCK_DIR/owner"; then
        return 1
    fi
    initial_owner="$FILE_CONTENT"
    if [[ ! "$initial_owner" =~ $LOCK_OWNER_PATTERN ]]; then
        return 1
    fi
    owner_pid="${BASH_REMATCH[1]}"
    if kill -0 "$owner_pid" 2>/dev/null; then
        return 1
    fi
    if ! read_file_exact "$LOCK_DIR/owner" ||
        [[ "$FILE_CONTENT" != "$initial_owner" ]]; then
        return 1
    fi

    if ! reclaim_dir="$(mktemp -d "${CACHE}/.pg_compat.reclaim.XXXXXX")"; then
        return 1
    fi
    LOCK_RECLAIM_DIR="$reclaim_dir"
    LOCK_RECLAIM_FILE="${LOCK_RECLAIM_DIR}/owner"
    if ! mv "$LOCK_DIR/owner" "$LOCK_RECLAIM_FILE" 2>/dev/null; then
        rmdir "$LOCK_RECLAIM_DIR" 2>/dev/null || true
        LOCK_RECLAIM_DIR=
        LOCK_RECLAIM_FILE=
        return 1
    fi
    if ! read_file_exact "$LOCK_RECLAIM_FILE" ||
        [[ "$FILE_CONTENT" != "$initial_owner" ]]; then
        restore_reclaim_owner
        return 1
    fi
    if ! rmdir "$LOCK_DIR" 2>/dev/null; then
        restore_reclaim_owner
        return 1
    fi

    rm -f "$LOCK_RECLAIM_FILE"
    rmdir "$LOCK_RECLAIM_DIR" 2>/dev/null || true
    LOCK_RECLAIM_DIR=
    LOCK_RECLAIM_FILE=
}

publish_lock() {
    if ! mv "$LOCK_OWNER_TMP" "$LOCK_DIR/owner"; then
        echo "Unable to publish PostgreSQL compatibility cache lock owner" >&2
        exit 1
    fi
    LOCK_OWNER_TMP=
}

acquire_lock() {
    local lock_token

    mkdir -p "$CACHE"
    LOCK_OWNER_TMP="$(mktemp "${CACHE}/.pg_compat.owner.XXXXXX")"
    lock_token="${LOCK_OWNER_TMP##*.pg_compat.owner.}"
    LOCK_OWNER_RECORD="$$"$'\t'"${lock_token}"$'\n'
    printf '%s' "$LOCK_OWNER_RECORD" > "$LOCK_OWNER_TMP"

    if mkdir "$LOCK_DIR" 2>/dev/null; then
        LOCK_HELD=1
        publish_lock
        return
    fi
    if reclaim_stale_lock && mkdir "$LOCK_DIR" 2>/dev/null; then
        LOCK_HELD=1
        publish_lock
        return
    fi

    rm -f "$LOCK_OWNER_TMP"
    LOCK_OWNER_TMP=
    echo "PostgreSQL compatibility cache is locked: ${CACHE}" >&2
    exit 1
}

handle_signal() {
    local signal_name="$1"
    local exit_status="$2"
    local child_pid="$ACTIVE_CHILD_PID"

    trap - HUP INT TERM
    ACTIVE_CHILD_PID=
    if [[ -n "$child_pid" ]]; then
        if ! kill "-${signal_name}" -- "-${child_pid}" 2>/dev/null; then
            kill "-${signal_name}" "$child_pid" 2>/dev/null || true
        fi
        wait "$child_pid" 2>/dev/null || true
    fi
    release_lock
    exit "$exit_status"
}

run_interruptible() {
    local child_pid
    local child_status

    python3 -c '
import os
import signal
import sys

os.setsid()
for signal_number in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
    signal.signal(signal_number, signal.SIG_DFL)
os.execvp(sys.argv[1], sys.argv[1:])
' "$@" &
    child_pid=$!
    ACTIVE_CHILD_PID="$child_pid"
    if wait "$child_pid"; then
        child_status=0
    else
        child_status=$?
    fi
    ACTIVE_CHILD_PID=
    return "$child_status"
}

trap release_lock EXIT
trap 'handle_signal HUP 129' HUP
trap 'handle_signal INT 130' INT
trap 'handle_signal TERM 143' TERM

LIBPG_QUERY_URL="$(
    python3 - "$PINS" <<'PY'
import sys

from scripts.pg_compat.common import load_pins

print(load_pins(sys.argv[1])["libpg_query_url"])
PY
)"

acquire_lock

pin_values() {
    local role="$1"
    python3 - "$PINS" "$role" <<'PY'
import sys

from scripts.pg_compat.common import load_pins

version = load_pins(sys.argv[1])["versions"][sys.argv[2]]
print(
    version["branch"],
    version["commit"],
    version["pg_version"],
    version["pg_version_num"],
    version["postgres_sha256"],
    sep="\t",
)
PY
}

verify_makefile_version() {
    local checkout="$1"
    local expected_version="$2"
    local expected_version_num="$3"
    local actual_version
    local actual_version_num

    IFS=$'\t' read -r actual_version actual_version_num <<< "$(
        python3 - "$checkout/Makefile" <<'PY'
import sys
from pathlib import Path

from scripts.pg_compat.common import parse_makefile_pg_version

version, version_num = parse_makefile_pg_version(
    Path(sys.argv[1]).read_text(encoding="utf-8")
)
print(version, version_num, sep="\t")
PY
    )"

    if [[ "$actual_version" != "$expected_version" ]]; then
        echo "PG_VERSION mismatch in ${checkout}: expected ${expected_version}, got ${actual_version}" >&2
        exit 1
    fi
    if [[ "$actual_version_num" != "$expected_version_num" ]]; then
        echo "PG_VERSION_NUM mismatch in ${checkout}: expected ${expected_version_num}, got ${actual_version_num}" >&2
        exit 1
    fi
}

fetch_libpg_query() {
    local role="$1"
    local branch="$2"
    local commit="$3"
    local pg_version="$4"
    local pg_version_num="$5"
    local checkout="${CACHE}/libpg_query/${role}"
    local actual_head

    mkdir -p "$(dirname "$checkout")"
    if [[ ! -d "$checkout/.git" ]]; then
        if [[ -e "$checkout" ]]; then
            echo "Cache path exists but is not a Git checkout: ${checkout}" >&2
            exit 1
        fi
        run_interruptible git clone --no-checkout "$LIBPG_QUERY_URL" "$checkout"
    fi

    run_interruptible git -C "$checkout" remote set-url origin "$LIBPG_QUERY_URL"
    echo "Fetching libpg_query ${role}: ${branch} at ${commit}"
    run_interruptible git -C "$checkout" fetch --force --no-tags origin "$commit"
    run_interruptible git -C "$checkout" checkout --detach --force "$commit"

    actual_head="$(git -C "$checkout" rev-parse HEAD)"
    if [[ "$actual_head" != "$commit" ]]; then
        echo "HEAD mismatch in ${checkout}: expected ${commit}, got ${actual_head}" >&2
        exit 1
    fi

    verify_makefile_version "$checkout" "$pg_version" "$pg_version_num"
}

sha256_file() {
    python3 - "$1" <<'PY'
import hashlib
import sys
from pathlib import Path

digest = hashlib.sha256()
with Path(sys.argv[1]).open("rb") as source:
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
}

fetch_postgres_source() {
    local pg_version="$1"
    local expected_sha256="$2"
    local postgres_root="${CACHE}/postgresql"
    local archive="${postgres_root}/postgresql-${pg_version}.tar.bz2"
    local source_dir="${postgres_root}/postgresql-${pg_version}"
    local archive_url="https://ftp.postgresql.org/pub/source/v${pg_version}/postgresql-${pg_version}.tar.bz2"
    local download_tmp
    local extract_tmp
    local actual_sha256

    mkdir -p "$postgres_root"
    if [[ -f "$archive" ]]; then
        actual_sha256="$(sha256_file "$archive")"
        if [[ "$actual_sha256" != "$expected_sha256" ]]; then
            echo "Removing invalid cached archive ${archive}: expected ${expected_sha256}, got ${actual_sha256}" >&2
            rm -f "$archive"
        fi
    fi

    if [[ ! -f "$archive" ]]; then
        download_tmp="$(mktemp "${postgres_root}/.postgresql-${pg_version}.download.XXXXXX")"
        if ! run_interruptible curl --fail --location --retry 3 --output "$download_tmp" "$archive_url"; then
            rm -f "$download_tmp"
            return 1
        fi
        actual_sha256="$(sha256_file "$download_tmp")"
        if [[ "$actual_sha256" != "$expected_sha256" ]]; then
            echo "SHA-256 mismatch for downloaded ${archive}: expected ${expected_sha256}, got ${actual_sha256}" >&2
            rm -f "$download_tmp"
            return 1
        fi
        mv "$download_tmp" "$archive"
    fi

    if [[ ! -d "$source_dir" ]]; then
        extract_tmp="$(mktemp -d "${postgres_root}/.postgresql-${pg_version}.extract.XXXXXX")"
        if ! run_interruptible tar -xjf "$archive" -C "$extract_tmp"; then
            rm -rf "$extract_tmp"
            return 1
        fi
        mv "${extract_tmp}/postgresql-${pg_version}" "$source_dir"
        rmdir "$extract_tmp"
    fi
}

for role in previous target; do
    IFS=$'\t' read -r branch commit pg_version pg_version_num postgres_sha256 <<< "$(pin_values "$role")"
    fetch_libpg_query "$role" "$branch" "$commit" "$pg_version" "$pg_version_num"
    if [[ "$WITH_POSTGRES_SOURCE" -eq 1 ]]; then
        fetch_postgres_source "$pg_version" "$postgres_sha256"
    fi
done
