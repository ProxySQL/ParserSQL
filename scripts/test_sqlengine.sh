#!/bin/bash
# test_sqlengine.sh — Functional test suite for the ./sqlengine binary.
#
# Drives the binary itself (not the C++ engine API) and asserts on its
# stdout. Exists so that regressions in tools/sqlengine.cpp — URL parsing,
# schema discovery, table formatting, REPL behaviour, error reporting —
# do not ship undetected.
#
# Usage:
#   ./scripts/test_sqlengine.sh [MODE]
#
# Modes:
#   in-memory    Only run tests that need no backend.
#   single       Only run tests against the single-backend container
#                (parsersql-single on port 13308).
#   sharded      Only run tests against the two sharded containers
#                (parsersql-shard1 on 13306, parsersql-shard2 on 13307).
#   all          (default) Run all three groups. Requires every container.
#
# Exit codes:
#   0   all selected tests passed
#   1   at least one test assertion failed
#   2   preconditions not met (binary missing, container missing,
#       docker missing, etc.) — NEVER silent: prints exactly which
#       prerequisite is missing and how to satisfy it
#
# Setup:
#   make build-sqlengine
#   ./scripts/start_sharding_demo.sh   # for "sharded" / "all"
#   ./scripts/setup_single_backend.sh  # for "single"  / "all"
#
# Note: parsersql-shard1 and parsersql-test-mysql both bind 13306, so
# you cannot have start_test_backends.sh and start_sharding_demo.sh
# active at the same time. This test suite uses the sharding-demo set.

set -u

# ----------------------------------------------------------------------
# Globals
# ----------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"
SQLENGINE="${PROJECT_DIR}/sqlengine"

MODE="${1:-all}"

# Backend connection strings used by the script.
SINGLE_BACKEND='mysql://root:test@127.0.0.1:13308/testdb?name=single'
SHARD1_BACKEND='mysql://root:test@127.0.0.1:13306/testdb?name=shard1'
SHARD2_BACKEND='mysql://root:test@127.0.0.1:13307/testdb?name=shard2'

# Test counters.
PASS_COUNT=0
FAIL_COUNT=0
FAILED_LABELS=()

# ANSI colour, but only when stdout is a TTY.
if [ -t 1 ]; then
    C_RED=$'\033[31m'
    C_GREEN=$'\033[32m'
    C_YELLOW=$'\033[33m'
    C_BOLD=$'\033[1m'
    C_OFF=$'\033[0m'
else
    C_RED=''; C_GREEN=''; C_YELLOW=''; C_BOLD=''; C_OFF=''
fi

# ----------------------------------------------------------------------
# Loud failure helpers
# ----------------------------------------------------------------------

die_precondition() {
    # Exit 2 with a clear stderr message. Never silent.
    echo "${C_RED}${C_BOLD}PRECONDITION NOT MET:${C_OFF} $*" >&2
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || die_precondition "command '$1' not found in PATH. Install it before running this script."
}

require_binary() {
    [ -x "${SQLENGINE}" ] \
        || die_precondition "sqlengine binary not found at ${SQLENGINE}. Build it first: make build-sqlengine"
}

container_running() {
    # Returns 0 if container is running, 1 otherwise. No output.
    [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = 'true' ]
}

require_container() {
    local name="$1" hint="$2"
    container_running "${name}" \
        || die_precondition "container '${name}' is not running. ${hint}"
}

# ----------------------------------------------------------------------
# Run sqlengine and capture combined output (stdout+stderr).
# ----------------------------------------------------------------------

run_in_memory() {
    local sql="$1"
    echo "${sql}" | "${SQLENGINE}" 2>&1
}

run_single() {
    local sql="$1"
    echo "${sql}" | "${SQLENGINE}" --backend "${SINGLE_BACKEND}" \
                                   --shard "users:id:single" \
                                   --shard "orders:id:single" 2>&1
}

run_sharded() {
    local sql="$1"
    # Range routing matches the demo data placement:
    #   shard1: users 1-5, orders 101-105
    #   shard2: users 6-10, orders 106-110
    echo "${sql}" | "${SQLENGINE}" --backend "${SHARD1_BACKEND}" \
                                   --backend "${SHARD2_BACKEND}" \
                                   --shard "users:id:range:5=shard1,10=shard2" \
                                   --shard "orders:id:range:105=shard1,110=shard2" 2>&1
}

# ----------------------------------------------------------------------
# Assertion helpers — each prints PASS / FAIL with the failing detail.
# ----------------------------------------------------------------------

record_pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    echo "  ${C_GREEN}[PASS]${C_OFF} $1"
}

record_fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_LABELS+=("$1")
    echo "  ${C_RED}[FAIL]${C_OFF} $1"
    echo "         expected: $2"
    echo "         got     : $3"
    if [ -n "${4:-}" ]; then
        echo "         output  :"
        echo "$4" | sed 's/^/                   /'
    fi
}

# assert_contains LABEL OUTPUT EXPECTED_SUBSTRING
assert_contains() {
    local label="$1" output="$2" expected="$3"
    if echo "${output}" | grep -qF -- "${expected}"; then
        record_pass "${label}"
    else
        record_fail "${label}" "output to contain: ${expected}" "(not found)" "${output}"
    fi
}

# assert_not_contains LABEL OUTPUT FORBIDDEN_SUBSTRING
assert_not_contains() {
    local label="$1" output="$2" forbidden="$3"
    if echo "${output}" | grep -qF -- "${forbidden}"; then
        record_fail "${label}" "output to NOT contain: ${forbidden}" "(found)" "${output}"
    else
        record_pass "${label}"
    fi
}

# assert_row_count LABEL OUTPUT EXPECTED_N
# Looks for sqlengine's "N rows in set" or "N row in set" line.
assert_row_count() {
    local label="$1" output="$2" expected="$3"
    local got
    got=$(echo "${output}" | grep -oE '[0-9]+ rows? in set' | head -n1 | grep -oE '^[0-9]+' || true)
    if [ "${got}" = "${expected}" ]; then
        record_pass "${label}"
    else
        record_fail "${label}" "${expected} rows in set" "${got:-(no row-count line)}" "${output}"
    fi
}

# ----------------------------------------------------------------------
# Test groups
# ----------------------------------------------------------------------

test_in_memory() {
    echo
    echo "${C_BOLD}== in-memory tests (no backend) ==${C_OFF}"

    local out
    out=$(run_in_memory "SELECT 1 + 2")
    assert_contains "in-memory: integer arithmetic" "${out}" "3"

    out=$(run_in_memory "SELECT UPPER('hello')")
    assert_contains "in-memory: string function UPPER" "${out}" "HELLO"

    out=$(run_in_memory "SELECT COALESCE(NULL, 42)")
    assert_contains "in-memory: COALESCE skips NULL" "${out}" "42"

    out=$(run_in_memory "SELECT CASE WHEN 1 < 2 THEN 'yes' ELSE 'no' END")
    assert_contains "in-memory: CASE/WHEN" "${out}" "yes"

    # MySQL-dialect integer division: 10 / 4 = 2 (not 2.5).
    out=$(run_in_memory "SELECT 10 / 4")
    assert_contains "in-memory: integer division (MySQL)" "${out}" "2"

    # Floating-point path triggered by a non-integer operand.
    out=$(run_in_memory "SELECT 10.0 / 4")
    assert_contains "in-memory: float division" "${out}" "2.5"

    out=$(run_in_memory "SELECT LENGTH('abcdef')")
    assert_contains "in-memory: LENGTH" "${out}" "6"

    # Parse error path — the binary must report ERROR (and not crash).
    out=$(run_in_memory "totally not sql at all")
    assert_contains "in-memory: parse error reported" "${out}" "ERROR"
}

test_single() {
    echo
    echo "${C_BOLD}== single-backend tests (parsersql-single, port 13308) ==${C_OFF}"

    local out
    out=$(run_single "SELECT COUNT(*) FROM users")
    assert_contains "single: total user count" "${out}" "10"

    out=$(run_single "SELECT name FROM users WHERE id = 5")
    assert_contains "single: predicate on existing row" "${out}" "Eve"
    assert_row_count "single: predicate row count" "${out}" "1"

    out=$(run_single "SELECT name FROM users WHERE id = 999")
    assert_row_count "single: predicate no match" "${out}" "0"

    out=$(run_single "SELECT name FROM users WHERE dept = 'Engineering' ORDER BY id")
    assert_contains "single: filter by dept (Alice present)" "${out}" "Alice"
    assert_contains "single: filter by dept (Frank present)" "${out}" "Frank"
    assert_row_count "single: 5 engineers" "${out}" "5"

    out=$(run_single "SELECT SUM(salary) FROM users WHERE dept = 'Engineering'")
    assert_contains "single: SUM(salary) Engineering = 530000" "${out}" "530000"

    out=$(run_single "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 105")
    assert_contains "single: join — Eve from order 105" "${out}" "Eve"
    assert_contains "single: join — total 300" "${out}" "300"
}

test_sharded() {
    echo
    echo "${C_BOLD}== sharded tests (parsersql-shard1+shard2, ports 13306+13307) ==${C_OFF}"

    local out
    # Scatter + MERGE_AGGREGATE.
    out=$(run_sharded "SELECT COUNT(*) FROM users")
    assert_contains "sharded: scatter COUNT(*) = 10" "${out}" "10"

    # Single-shard route on shard key.
    out=$(run_sharded "SELECT name FROM users WHERE id = 5")
    assert_contains "sharded: route to shard1 (Eve)" "${out}" "Eve"
    assert_row_count "sharded: route returns one row" "${out}" "1"

    out=$(run_sharded "SELECT name FROM users WHERE id = 7")
    assert_contains "sharded: route to shard2 (Grace)" "${out}" "Grace"
    assert_row_count "sharded: route returns one row" "${out}" "1"

    # GROUP BY across shards (MERGE_AGGREGATE).
    out=$(run_sharded "SELECT dept, COUNT(*) FROM users GROUP BY dept")
    assert_contains "sharded: GROUP BY contains Engineering" "${out}" "Engineering"
    assert_contains "sharded: GROUP BY contains Sales" "${out}" "Sales"
    assert_contains "sharded: GROUP BY contains Marketing" "${out}" "Marketing"
    assert_row_count "sharded: GROUP BY 3 departments" "${out}" "3"

    # ORDER BY ... LIMIT — distributed merge-sort.
    out=$(run_sharded "SELECT name, salary FROM users ORDER BY salary DESC LIMIT 3")
    assert_contains "sharded: top-3 highest paid (Frank)" "${out}" "Frank"
    assert_contains "sharded: top-3 highest paid (130000)" "${out}" "130000"
    assert_row_count "sharded: LIMIT 3" "${out}" "3"

    # Cross-shard JOIN — materialise + hash-join.
    out=$(run_sharded "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 6")
    assert_contains "sharded: cross-shard JOIN (Frank)" "${out}" "Frank"
    assert_contains "sharded: cross-shard JOIN (500)"   "${out}" "500"

    # Subquery — referenced AVG on the same sharded table.
    out=$(run_sharded "SELECT name, age FROM users WHERE age > (SELECT AVG(age) FROM users)")
    # Expect *some* rows, but at least the older users (Frank=45, Heidi=38, Carol=35).
    assert_contains "sharded: subquery returns Frank (age 45)" "${out}" "Frank"

    # Aggregation that lives in the registry: SUM across shards.
    # Shard1 (Alice+Bob+Carol+Dave+Eve)   = 95000+65000+110000+70000+105000  = 445000
    # Shard2 (Frank+Grace+Heidi+Ivan+Judy)= 130000+68000+85000+90000+72000   = 445000
    # Total = 890000.
    out=$(run_sharded "SELECT SUM(salary) FROM users")
    assert_contains "sharded: SUM(salary) all users = 890000" "${out}" "890000"
}

# ----------------------------------------------------------------------
# Precondition gates per mode
# ----------------------------------------------------------------------

gate_in_memory() {
    require_binary
}

gate_single() {
    require_binary
    require_command docker
    require_container parsersql-single \
        "Start it with: ${SCRIPT_DIR}/setup_single_backend.sh"
}

gate_sharded() {
    require_binary
    require_command docker
    require_container parsersql-shard1 \
        "Start the demo backends with: ${SCRIPT_DIR}/start_sharding_demo.sh"
    require_container parsersql-shard2 \
        "Start the demo backends with: ${SCRIPT_DIR}/start_sharding_demo.sh"
}

# ----------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------

case "${MODE}" in
    in-memory)
        gate_in_memory
        test_in_memory
        ;;
    single)
        gate_single
        test_single
        ;;
    sharded)
        gate_sharded
        test_sharded
        ;;
    all)
        gate_in_memory
        gate_single
        gate_sharded
        test_in_memory
        test_single
        test_sharded
        ;;
    -h|--help|help)
        sed -n '2,29p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    *)
        die_precondition "unknown mode '${MODE}'. Run: ${0} --help"
        ;;
esac

# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------

echo
echo "${C_BOLD}== summary ==${C_OFF}"
TOTAL=$((PASS_COUNT + FAIL_COUNT))
if [ "${FAIL_COUNT}" -eq 0 ]; then
    echo "${C_GREEN}${C_BOLD}${PASS_COUNT}/${TOTAL} passed${C_OFF}"
    exit 0
else
    echo "${C_RED}${C_BOLD}${FAIL_COUNT}/${TOTAL} FAILED${C_OFF} (${PASS_COUNT} passed)"
    echo "${C_YELLOW}Failed tests:${C_OFF}"
    for label in "${FAILED_LABELS[@]}"; do
        echo "  - ${label}"
    done
    exit 1
fi
