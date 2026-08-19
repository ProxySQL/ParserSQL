# PostgreSQL libpg_query Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible differential compatibility harness that reports which PostgreSQL 18 statements ParserSQL deeply parses, only classifies, or does not support, with a separate PostgreSQL 17-to-18 release delta.

**Architecture:** Build the same C++ oracle/runner against pinned `libpg_query` 17 and 18 sources. The runner splits and validates upstream regression SQL, inspects protobuf root nodes, invokes ParserSQL, and emits JSON Lines. Python standard-library tooling deduplicates inventories, computes release and structural diffs, evaluates committed baselines, generates CI cases, and writes the Markdown report.

**Tech Stack:** C++17, ParserSQL, `libpg_query` C API, protobuf-c generated API, Python 3 standard library, POSIX shell, GNU Make, GitHub Actions.

---

## File Structure

Create or modify these files:

```text
Makefile
.gitignore
.github/workflows/ci.yml
README.md
tools/pg_compat/
    pg_compat.cpp                 # libpg_query splitting/oracle + ParserSQL result records
    result.h                      # compatibility result enum and string conversion
    statement_type_map.cpp        # explicit protobuf node -> ParserSQL statement mapping
    statement_type_map.h
scripts/pg_compat/
    common.py                     # JSONL, hashing, atomic writes, pin loading
    fetch_libpg_query.sh          # pinned clone/build and PostgreSQL archive fetch
    extract_statements.py         # inventory normalization, IDs, deduplication
    compare_versions.py           # PG17/PG18 statement and structural deltas
    baseline.py                   # transition policy and CI case generation
    generate_report.py            # Markdown report
    run_compat.py                 # command orchestration
tests/pg_compat/
    upstream_pins.json
    witnesses.sql
    witnesses.json
    structural_dispositions.json
    statement_type_cases.cpp
    runner_cases.sql
    expected_results.jsonl
    ci_cases.jsonl
    fixtures/
        pg17-inventory.jsonl
        pg18-inventory.jsonl
        pg17-gram.y
        pg18-gram.y
        pg17-kwlist.h
        pg18-kwlist.h
        pg17-parsenodes.h
        pg18-parsenodes.h
        pg17.proto
        pg18.proto
    test_common.py
    test_extract_statements.py
    test_compare_versions.py
    test_baseline.py
    test_generate_report.py
docs/compatibility/
    postgresql-18.md
```

Generated upstream source and build artifacts live under `PG_COMPAT_CACHE`, defaulting to `/tmp/parsersql-pg-compat`. Do not commit cloned `libpg_query`, PostgreSQL archives, extracted source trees, runner binaries, or complete temporary inventories.

### Task 1: Add Immutable Upstream Pins and Fetching

**Files:**
- Create: `tests/pg_compat/upstream_pins.json`
- Create: `scripts/pg_compat/common.py`
- Create: `scripts/pg_compat/fetch_libpg_query.sh`
- Create: `tests/pg_compat/test_common.py`

- [ ] **Step 1: Write failing pin-validation tests**

Create `tests/pg_compat/test_common.py`:

```python
import json
import tempfile
import unittest
from pathlib import Path

from scripts.pg_compat.common import load_pins, parse_makefile_pg_version


class PinTests(unittest.TestCase):
    def test_load_pins_requires_previous_and_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pins.json"
            path.write_text('{"libpg_query_url":"https://example.invalid/repo"}')
            with self.assertRaisesRegex(ValueError, "previous"):
                load_pins(path)

    def test_makefile_version_matches_pin(self):
        text = "PG_VERSION = 18.4\nPG_VERSION_NUM = 180004\n"
        self.assertEqual(parse_makefile_pg_version(text), ("18.4", 180004))

    def test_repository_pins_are_immutable(self):
        pins = load_pins(Path("tests/pg_compat/upstream_pins.json"))
        for role in ("previous", "target"):
            commit = pins["versions"][role]["commit"]
            self.assertRegex(commit, r"^[0-9a-f]{40}$")
            self.assertRegex(
                pins["versions"][role]["postgres_sha256"],
                r"^[0-9a-f]{64}$",
            )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests and verify the import fails**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_common.py' -v
```

Expected: `ModuleNotFoundError: No module named 'scripts.pg_compat.common'`.

- [ ] **Step 3: Add pinned PG17 and PG18 metadata**

Create `tests/pg_compat/upstream_pins.json`:

```json
{
  "libpg_query_url": "https://github.com/pganalyze/libpg_query.git",
  "versions": {
    "previous": {
      "branch": "17-latest",
      "commit": "815abf77660ca3c38511e3f5af7777b31764c1d5",
      "pg_version": "17.7",
      "pg_version_num": 170007,
      "postgres_sha256": "ef9e343302eccd33112f1b2f0247be493cb5768313adeb558b02de8797a2e9b5"
    },
    "target": {
      "branch": "18-latest",
      "commit": "9ab9951b7021adcc6d25d261d5016ca0b83726b8",
      "pg_version": "18.4",
      "pg_version_num": 180004,
      "postgres_sha256": "81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094"
    }
  }
}
```

- [ ] **Step 4: Implement shared pin and atomic-file helpers**

Create `scripts/pg_compat/common.py`:

```python
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path


def load_pins(path):
    data = json.loads(Path(path).read_text())
    if "libpg_query_url" not in data:
        raise ValueError("missing libpg_query_url")
    versions = data.get("versions", {})
    for role in ("previous", "target"):
        if role not in versions:
            raise ValueError(f"missing {role} version")
        for key in (
            "branch",
            "commit",
            "pg_version",
            "pg_version_num",
            "postgres_sha256",
        ):
            if key not in versions[role]:
                raise ValueError(f"missing {role}.{key}")
    return data


def parse_makefile_pg_version(text):
    version = re.search(r"^PG_VERSION\s*=\s*(\S+)\s*$", text, re.MULTILINE)
    number = re.search(r"^PG_VERSION_NUM\s*=\s*(\d+)\s*$", text, re.MULTILINE)
    if not version or not number:
        raise ValueError("Makefile does not declare PG_VERSION and PG_VERSION_NUM")
    return version.group(1), int(number.group(1))


def statement_id(normalized_sql, oracle_node):
    payload = f"{oracle_node}\0{normalized_sql}".encode()
    return hashlib.sha256(payload).hexdigest()[:24]


def read_jsonl(path):
    with Path(path).open() as stream:
        for line_number, line in enumerate(stream, 1):
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_number}: {exc}") from exc


def atomic_write_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(text)
        os.replace(temporary, path)
    except BaseException:
        os.unlink(temporary)
        raise
```

- [ ] **Step 5: Implement pinned fetch and verification**

Create executable `scripts/pg_compat/fetch_libpg_query.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PINS="${PG_COMPAT_PINS:-$ROOT/tests/pg_compat/upstream_pins.json}"
CACHE="${PG_COMPAT_CACHE:-/tmp/parsersql-pg-compat}"
WITH_POSTGRES_SOURCE=0
[[ "${1:-}" == "--with-postgres-source" ]] && WITH_POSTGRES_SOURCE=1
cd "$ROOT"

read_pin() {
    python3 - "$PINS" "$1" "$2" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1]))
print(data["versions"][sys.argv[2]][sys.argv[3]])
PY
}

URL="$(python3 - "$PINS" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1]))["libpg_query_url"])
PY
)"

mkdir -p "$CACHE/libpg_query" "$CACHE/postgresql"

for role in previous target; do
    branch="$(read_pin "$role" branch)"
    commit="$(read_pin "$role" commit)"
    pg_version="$(read_pin "$role" pg_version)"
    pg_version_num="$(read_pin "$role" pg_version_num)"
    expected_sha="$(read_pin "$role" postgres_sha256)"
    repo="$CACHE/libpg_query/$role"

    if [[ ! -d "$repo/.git" ]]; then
        git clone --filter=blob:none --no-checkout "$URL" "$repo"
    fi
    git -C "$repo" fetch --depth 1 origin "$commit"
    git -C "$repo" checkout --detach "$commit"
    [[ "$(git -C "$repo" rev-parse HEAD)" == "$commit" ]]

    actual="$(python3 - "$repo/Makefile" <<'PY'
import sys
from scripts.pg_compat.common import parse_makefile_pg_version
print(*parse_makefile_pg_version(open(sys.argv[1]).read()))
PY
)"
    [[ "$actual" == "$pg_version $pg_version_num" ]]

    if [[ "$WITH_POSTGRES_SOURCE" == 1 ]]; then
        archive="$CACHE/postgresql/postgresql-$pg_version.tar.bz2"
        if [[ ! -f "$archive" ]]; then
            curl -fL "https://ftp.postgresql.org/pub/source/v$pg_version/postgresql-$pg_version.tar.bz2" -o "$archive"
        fi
        python3 - "$archive" "$expected_sha" <<'PY'
import hashlib
import sys
actual = hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest()
if actual != sys.argv[2]:
    raise SystemExit(f"checksum mismatch: {actual}")
PY

        source="$CACHE/postgresql/postgresql-$pg_version"
        if [[ ! -d "$source" ]]; then
            tar -xjf "$archive" -C "$CACHE/postgresql"
        fi
    fi
done
```

- [ ] **Step 6: Run pin tests and shell syntax checks**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_common.py' -v
bash -n scripts/pg_compat/fetch_libpg_query.sh
```

Expected: all tests pass and `bash -n` exits `0`.

- [ ] **Step 7: Verify pinned fetch end to end**

Run:

```bash
PG_COMPAT_CACHE=/tmp/parsersql-pg-compat-test \
  ./scripts/pg_compat/fetch_libpg_query.sh --with-postgres-source
```

Expected:

- both repositories are detached at the pinned SHAs
- PostgreSQL 17.7 and 18.4 archives pass SHA-256 verification
- extracted source directories exist

- [ ] **Step 8: Commit**

```bash
git add tests/pg_compat/upstream_pins.json \
  tests/pg_compat/test_common.py \
  scripts/pg_compat/common.py \
  scripts/pg_compat/fetch_libpg_query.sh
git commit -m "chore: pin PostgreSQL parser oracle sources"
```

### Task 2: Implement Compatibility Results and Statement-Type Mapping

**Files:**
- Create: `tools/pg_compat/result.h`
- Create: `tools/pg_compat/statement_type_map.h`
- Create: `tools/pg_compat/statement_type_map.cpp`
- Create: `tests/pg_compat/statement_type_cases.cpp`

- [ ] **Step 1: Write the failing mapping test**

Create `tests/pg_compat/statement_type_cases.cpp`:

```cpp
#include <cassert>
#include "tools/pg_compat/statement_type_map.h"

using namespace pg_compat;
using sql_parser::StmtType;

int main() {
    PgQuery__Node select = PG_QUERY__NODE__INIT;
    select.node_case = PG_QUERY__NODE__NODE_SELECT_STMT;
    auto select_mapping = expected_stmt_type(select);
    assert(select_mapping.kind == MappingKind::Equivalent);
    assert(select_mapping.type == StmtType::SELECT);

    PgQuery__TransactionStmt txn = PG_QUERY__TRANSACTION_STMT__INIT;
    txn.kind = PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT;
    PgQuery__Node transaction = PG_QUERY__NODE__INIT;
    transaction.node_case = PG_QUERY__NODE__NODE_TRANSACTION_STMT;
    transaction.transaction_stmt = &txn;
    auto transaction_mapping = expected_stmt_type(transaction);
    assert(transaction_mapping.kind == MappingKind::Equivalent);
    assert(transaction_mapping.type == StmtType::COMMIT);

    PgQuery__Node vacuum = PG_QUERY__NODE__INIT;
    vacuum.node_case = PG_QUERY__NODE__NODE_VACUUM_STMT;
    assert(expected_stmt_type(vacuum).kind == MappingKind::NoEquivalent);
}
```

- [ ] **Step 2: Compile and verify failure**

After fetching the target source, run:

```bash
CACHE=/tmp/parsersql-pg-compat-test
g++ -std=c++17 -I. -Iinclude \
  -I"$CACHE/libpg_query/target" \
  -I"$CACHE/libpg_query/target/protobuf" \
  tests/pg_compat/statement_type_cases.cpp \
  tools/pg_compat/statement_type_map.cpp \
  -o /tmp/statement_type_cases
```

Expected: compilation fails because the mapping files do not exist.

- [ ] **Step 3: Add result contract**

Create `tools/pg_compat/result.h`:

```cpp
#ifndef PG_COMPAT_RESULT_H
#define PG_COMPAT_RESULT_H

namespace pg_compat {

enum class CompatibilityResult {
    DeepSupported,
    ClassifiedOnly,
    Partial,
    Error,
    TrailingInput,
    TypeMismatch,
    OracleRejected,
};

inline const char* result_name(CompatibilityResult result) {
    switch (result) {
    case CompatibilityResult::DeepSupported: return "DEEP_SUPPORTED";
    case CompatibilityResult::ClassifiedOnly: return "CLASSIFIED_ONLY";
    case CompatibilityResult::Partial: return "PARTIAL";
    case CompatibilityResult::Error: return "ERROR";
    case CompatibilityResult::TrailingInput: return "TRAILING_INPUT";
    case CompatibilityResult::TypeMismatch: return "TYPE_MISMATCH";
    case CompatibilityResult::OracleRejected: return "ORACLE_REJECTED";
    }
    return "ERROR";
}

} // namespace pg_compat

#endif
```

- [ ] **Step 4: Add explicit mapping interface**

Create `tools/pg_compat/statement_type_map.h`:

```cpp
#ifndef PG_COMPAT_STATEMENT_TYPE_MAP_H
#define PG_COMPAT_STATEMENT_TYPE_MAP_H

#include "sql_parser/common.h"
#include "protobuf/pg_query.pb-c.h"

namespace pg_compat {

enum class MappingKind {
    Equivalent,
    NoEquivalent,
    Unmapped,
};

struct StatementTypeMapping {
    MappingKind kind = MappingKind::Unmapped;
    sql_parser::StmtType type = sql_parser::StmtType::UNKNOWN;
};

StatementTypeMapping expected_stmt_type(const PgQuery__Node& node);
const char* oracle_node_name(PgQuery__Node__NodeCase node_case);
const char* stmt_type_name(sql_parser::StmtType type);

} // namespace pg_compat

#endif
```

- [ ] **Step 5: Implement mappings for ParserSQL categories**

Create `tools/pg_compat/statement_type_map.cpp`. Use explicit switch cases. The minimum equivalent groups are:

```cpp
#include "tools/pg_compat/statement_type_map.h"

namespace pg_compat {
using sql_parser::StmtType;

static StatementTypeMapping equivalent(StmtType type) {
    return {MappingKind::Equivalent, type};
}

StatementTypeMapping expected_stmt_type(const PgQuery__Node& node) {
    switch (node.node_case) {
    case PG_QUERY__NODE__NODE_SELECT_STMT: return equivalent(StmtType::SELECT);
    case PG_QUERY__NODE__NODE_INSERT_STMT: return equivalent(StmtType::INSERT);
    case PG_QUERY__NODE__NODE_UPDATE_STMT: return equivalent(StmtType::UPDATE);
    case PG_QUERY__NODE__NODE_DELETE_STMT: return equivalent(StmtType::DELETE_STMT);
    case PG_QUERY__NODE__NODE_VARIABLE_SET_STMT: return equivalent(StmtType::SET);
    case PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT: return equivalent(StmtType::SHOW);
    case PG_QUERY__NODE__NODE_PREPARE_STMT: return equivalent(StmtType::PREPARE);
    case PG_QUERY__NODE__NODE_EXECUTE_STMT: return equivalent(StmtType::EXECUTE);
    case PG_QUERY__NODE__NODE_DEALLOCATE_STMT: return equivalent(StmtType::DEALLOCATE);
    case PG_QUERY__NODE__NODE_EXPLAIN_STMT: return equivalent(StmtType::EXPLAIN);
    case PG_QUERY__NODE__NODE_CALL_STMT: return equivalent(StmtType::CALL);
    case PG_QUERY__NODE__NODE_DO_STMT: return equivalent(StmtType::DO_STMT);
    case PG_QUERY__NODE__NODE_TRUNCATE_STMT: return equivalent(StmtType::TRUNCATE);
    case PG_QUERY__NODE__NODE_LOCK_STMT: return equivalent(StmtType::LOCK);
    case PG_QUERY__NODE__NODE_GRANT_STMT:
    case PG_QUERY__NODE__NODE_GRANT_ROLE_STMT:
        return equivalent(StmtType::GRANT);
    case PG_QUERY__NODE__NODE_TRANSACTION_STMT:
        if (!node.transaction_stmt) return {};
        switch (node.transaction_stmt->kind) {
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_BEGIN:
            return equivalent(StmtType::BEGIN);
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_START:
            return equivalent(StmtType::START_TRANSACTION);
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT_PREPARED:
            return equivalent(StmtType::COMMIT);
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_TO:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_PREPARED:
            return equivalent(StmtType::ROLLBACK);
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_SAVEPOINT:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_RELEASE:
            return equivalent(StmtType::SAVEPOINT);
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_PREPARE:
            return equivalent(StmtType::PREPARE);
        default:
            return {};
        }
    case PG_QUERY__NODE__NODE_CREATE_STMT:
    case PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT:
    case PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT:
    case PG_QUERY__NODE__NODE_INDEX_STMT:
    case PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT:
    case PG_QUERY__NODE__NODE_CREATE_ROLE_STMT:
    case PG_QUERY__NODE__NODE_CREATE_SEQ_STMT:
    case PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT:
    case PG_QUERY__NODE__NODE_CREATE_ENUM_STMT:
    case PG_QUERY__NODE__NODE_CREATE_RANGE_STMT:
    case PG_QUERY__NODE__NODE_CREATEDB_STMT:
        return equivalent(StmtType::CREATE);
    case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT:
    case PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT:
    case PG_QUERY__NODE__NODE_ALTER_ROLE_STMT:
    case PG_QUERY__NODE__NODE_ALTER_SEQ_STMT:
    case PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT:
    case PG_QUERY__NODE__NODE_ALTER_ENUM_STMT:
    case PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT:
    case PG_QUERY__NODE__NODE_RENAME_STMT:
        return equivalent(StmtType::ALTER);
    case PG_QUERY__NODE__NODE_DROP_STMT:
    case PG_QUERY__NODE__NODE_DROPDB_STMT:
    case PG_QUERY__NODE__NODE_DROP_ROLE_STMT:
    case PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT:
        return equivalent(StmtType::DROP);
    case PG_QUERY__NODE__NODE_COPY_STMT:
    case PG_QUERY__NODE__NODE_MERGE_STMT:
    case PG_QUERY__NODE__NODE_VACUUM_STMT:
    case PG_QUERY__NODE__NODE_NOTIFY_STMT:
    case PG_QUERY__NODE__NODE_LISTEN_STMT:
    case PG_QUERY__NODE__NODE_UNLISTEN_STMT:
        return {MappingKind::NoEquivalent, StmtType::UNKNOWN};
    default:
        return {};
    }
}

const char* oracle_node_name(PgQuery__Node__NodeCase node_case) {
    switch (node_case) {
    case PG_QUERY__NODE__NODE_SELECT_STMT:
        return "PG_QUERY__NODE__NODE_SELECT_STMT";
    case PG_QUERY__NODE__NODE_INSERT_STMT:
        return "PG_QUERY__NODE__NODE_INSERT_STMT";
    case PG_QUERY__NODE__NODE_UPDATE_STMT:
        return "PG_QUERY__NODE__NODE_UPDATE_STMT";
    case PG_QUERY__NODE__NODE_DELETE_STMT:
        return "PG_QUERY__NODE__NODE_DELETE_STMT";
    case PG_QUERY__NODE__NODE_TRANSACTION_STMT:
        return "PG_QUERY__NODE__NODE_TRANSACTION_STMT";
    case PG_QUERY__NODE__NODE_CREATE_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_STMT";
    case PG_QUERY__NODE__NODE_VACUUM_STMT:
        return "PG_QUERY__NODE__NODE_VACUUM_STMT";
    default:
        return "UNMAPPED_NODE_CASE";
    }
}

const char* stmt_type_name(StmtType type) {
    switch (type) {
    case StmtType::UNKNOWN: return "UNKNOWN";
    case StmtType::SELECT: return "SELECT";
    case StmtType::INSERT: return "INSERT";
    case StmtType::UPDATE: return "UPDATE";
    case StmtType::DELETE_STMT: return "DELETE";
    case StmtType::SET: return "SET";
    case StmtType::SHOW: return "SHOW";
    case StmtType::BEGIN: return "BEGIN";
    case StmtType::START_TRANSACTION: return "START_TRANSACTION";
    case StmtType::COMMIT: return "COMMIT";
    case StmtType::ROLLBACK: return "ROLLBACK";
    case StmtType::SAVEPOINT: return "SAVEPOINT";
    case StmtType::PREPARE: return "PREPARE";
    case StmtType::EXECUTE: return "EXECUTE";
    case StmtType::DEALLOCATE: return "DEALLOCATE";
    case StmtType::CREATE: return "CREATE";
    case StmtType::ALTER: return "ALTER";
    case StmtType::DROP: return "DROP";
    case StmtType::TRUNCATE: return "TRUNCATE";
    case StmtType::GRANT: return "GRANT";
    case StmtType::LOCK: return "LOCK";
    case StmtType::EXPLAIN: return "EXPLAIN";
    case StmtType::CALL: return "CALL";
    case StmtType::DO_STMT: return "DO";
    default: return "OTHER";
    }
}

} // namespace pg_compat
```

During the first full PG18 run, every top-level node reported as `Unmapped` must be added explicitly either as `Equivalent` or `NoEquivalent`. Do not replace the default with prefix matching; a new PostgreSQL protobuf node must remain an infrastructure failure until reviewed.

Every case added to `expected_stmt_type()` must also receive its exact generated constant name in `oracle_node_name()`. Keep both switches adjacent in the same source file and cover each new case in `statement_type_cases.cpp`.

- [ ] **Step 6: Compile and run the mapping test**

Run:

```bash
CACHE=/tmp/parsersql-pg-compat-test
g++ -std=c++17 -I. -Iinclude \
  -I"$CACHE/libpg_query/target" \
  -I"$CACHE/libpg_query/target/protobuf" \
  tests/pg_compat/statement_type_cases.cpp \
  tools/pg_compat/statement_type_map.cpp \
  "$CACHE/libpg_query/target/libpg_query.a" \
  -lm -lpthread \
  -o /tmp/statement_type_cases
/tmp/statement_type_cases
```

Expected: exit `0`.

- [ ] **Step 7: Commit**

```bash
git add tools/pg_compat tests/pg_compat/statement_type_cases.cpp
git commit -m "test: define PostgreSQL compatibility result mapping"
```

### Task 3: Build the Version-Specific C++ Oracle and ParserSQL Runner

**Files:**
- Create: `tools/pg_compat/pg_compat.cpp`
- Create: `tests/pg_compat/runner_cases.sql`

- [ ] **Step 1: Add a deterministic runner fixture**

Create `tests/pg_compat/runner_cases.sql`:

```sql
SELECT id FROM users WHERE id = 1;
CREATE TABLE users (id integer);
VACUUM users;
SELECT 1; SELECT 2;
```

- [ ] **Step 2: Define the runner CLI and verify it is absent**

The executable interface is:

```text
pg_compat_runner --input FILE --branch NAME --commit SHA
```

Run:

```bash
/tmp/parsersql-pg-compat-test/bin/pg_compat-18 \
  --input tests/pg_compat/runner_cases.sql \
  --branch 18-latest \
  --commit 9ab9951b7021adcc6d25d261d5016ca0b83726b8
```

Expected: command fails because the runner does not exist.

- [ ] **Step 3: Implement JSON escaping and meaningful-trailing-input checks**

In `tools/pg_compat/pg_compat.cpp`, add:

```cpp
static std::string json_escape(std::string_view input) {
    std::string output;
    for (unsigned char c : input) {
        switch (c) {
        case '"': output += "\\\""; break;
        case '\\': output += "\\\\"; break;
        case '\b': output += "\\b"; break;
        case '\f': output += "\\f"; break;
        case '\n': output += "\\n"; break;
        case '\r': output += "\\r"; break;
        case '\t': output += "\\t"; break;
        default:
            if (c < 0x20) {
                char buffer[7];
                std::snprintf(buffer, sizeof(buffer), "\\u%04x", c);
                output += buffer;
            } else {
                output += static_cast<char>(c);
            }
        }
    }
    return output;
}

static bool has_meaningful_remaining(sql_parser::StringRef remaining) {
    for (uint32_t i = 0; i < remaining.len; ++i) {
        unsigned char c = static_cast<unsigned char>(remaining.ptr[i]);
        if (!std::isspace(c) && c != ';') return true;
    }
    return false;
}
```

- [ ] **Step 4: Implement scanner-token normalization**

Use `pg_query_scan()` and `pg_query__scan_result__unpack()`:

```cpp
static std::string normalize_sql(std::string_view sql) {
    std::string owned_sql(sql);
    PgQueryScanResult raw = pg_query_scan(owned_sql.c_str());
    if (raw.error) {
        std::string message = raw.error->message;
        pg_query_free_scan_result(raw);
        throw std::runtime_error(message);
    }
    PgQuery__ScanResult* scan = pg_query__scan_result__unpack(
        nullptr, raw.pbuf.len,
        reinterpret_cast<const uint8_t*>(raw.pbuf.data));
    if (!scan) {
        pg_query_free_scan_result(raw);
        throw std::runtime_error("unable to unpack pg_query scan protobuf");
    }
    std::string normalized;
    for (size_t i = 0; i < scan->n_tokens; ++i) {
        const PgQuery__ScanToken* token = scan->tokens[i];
        std::string text(sql.substr(token->start, token->end - token->start));
        if (token->keyword_kind != PG_QUERY__KEYWORD_KIND__NO_KEYWORD) {
            std::transform(text.begin(), text.end(), text.begin(),
                           [](unsigned char c) { return std::toupper(c); });
        }
        if (!normalized.empty()) normalized.push_back(' ');
        normalized += text;
    }
    pg_query__scan_result__free_unpacked(scan, nullptr);
    pg_query_free_scan_result(raw);
    return normalized;
}
```

- [ ] **Step 5: Implement candidate splitting with parser-to-scanner fallback**

Read the whole file, try `pg_query_split_with_parser()`, and when it returns an error retry with `pg_query_split_with_scanner()`. Preserve:

- input path
- `stmt_location`
- `stmt_len`
- computed 1-based line number
- splitter mode (`parser` or `scanner`)

Treat a zero `stmt_len` as extending to the end of the input, matching PostgreSQL `RawStmt` behavior.

- [ ] **Step 6: Implement oracle parsing and root-node extraction**

For each candidate:

```cpp
PgQueryProtobufParseResult raw = pg_query_parse_protobuf(sql.c_str());
if (raw.error) {
    emit_oracle_rejected(...);
    pg_query_free_protobuf_parse_result(raw);
    continue;
}
PgQuery__ParseResult* parsed = pg_query__parse_result__unpack(
    nullptr, raw.parse_tree.len,
    reinterpret_cast<const uint8_t*>(raw.parse_tree.data));
if (!parsed || parsed->n_stmts != 1 || !parsed->stmts[0]->stmt) {
    fail_infrastructure("oracle did not produce exactly one RawStmt");
}
const PgQuery__Node& root = *parsed->stmts[0]->stmt;
```

Free both protobuf objects on every path.

- [ ] **Step 7: Implement ParserSQL result classification**

Use this exact priority:

```cpp
static CompatibilityResult classify(
    const sql_parser::ParseResult& parsed,
    const StatementTypeMapping& mapping) {
    if (parsed.status == sql_parser::ParseResult::ERROR)
        return CompatibilityResult::Error;
    if (parsed.status == sql_parser::ParseResult::PARTIAL)
        return CompatibilityResult::Partial;
    if (mapping.kind == MappingKind::Unmapped)
        throw std::runtime_error("unmapped PostgreSQL top-level node");
    if (mapping.kind == MappingKind::NoEquivalent ||
        parsed.stmt_type != mapping.type)
        return CompatibilityResult::TypeMismatch;
    if (has_meaningful_remaining(parsed.remaining))
        return CompatibilityResult::TrailingInput;
    return parsed.ast
        ? CompatibilityResult::DeepSupported
        : CompatibilityResult::ClassifiedOnly;
}
```

- [ ] **Step 8: Emit one JSON record per candidate**

Each accepted record must contain:

```json
{
  "source_file": "select.sql",
  "offset": 123,
  "line": 9,
  "splitter": "parser",
  "sql": "SELECT 1",
  "normalized_sql": "SELECT 1",
  "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
  "expected_stmt_type": "SELECT",
  "parser_status": "OK",
  "parser_stmt_type": "SELECT",
  "has_ast": true,
  "remaining": "",
  "result": "DEEP_SUPPORTED",
  "branch": "18-latest",
  "commit": "9ab9951b7021adcc6d25d261d5016ca0b83726b8"
}
```

Oracle-rejected diagnostics use `result: "ORACLE_REJECTED"` and include the oracle error message.

- [ ] **Step 9: Build the target runner manually**

Run:

```bash
CACHE=/tmp/parsersql-pg-compat-test
mkdir -p "$CACHE/bin"
make lib
make -C "$CACHE/libpg_query/target" -j2
g++ -std=c++17 -Wall -Wextra -I. -Iinclude \
  -I"$CACHE/libpg_query/target" \
  -I"$CACHE/libpg_query/target/src" \
  -I"$CACHE/libpg_query/target/src/postgres/include" \
  tools/pg_compat/pg_compat.cpp \
  tools/pg_compat/statement_type_map.cpp \
  -L. -lsqlparser \
  "$CACHE/libpg_query/target/libpg_query.a" \
  -lm -lpthread -o "$CACHE/bin/pg_compat-18"
```

Expected: compilation succeeds.

- [ ] **Step 10: Run and validate fixture outcomes**

Run:

```bash
"$CACHE/bin/pg_compat-18" \
  --input tests/pg_compat/runner_cases.sql \
  --branch 18-latest \
  --commit 9ab9951b7021adcc6d25d261d5016ca0b83726b8 \
  > /tmp/runner-results.jsonl
python3 - <<'PY'
import json
rows = [json.loads(line) for line in open("/tmp/runner-results.jsonl")]
assert rows[0]["result"] == "DEEP_SUPPORTED"
assert rows[1]["result"] == "CLASSIFIED_ONLY"
assert rows[2]["result"] == "TYPE_MISMATCH"
assert len(rows) == 5
PY
```

Expected: assertions pass. The multi-statement input produces separate `SELECT` records.

- [ ] **Step 11: Commit**

```bash
git add tools/pg_compat/pg_compat.cpp tests/pg_compat/runner_cases.sql
git commit -m "feat: add libpg_query differential runner"
```

### Task 4: Normalize, Deduplicate, and Identify Inventory Statements

**Files:**
- Create: `scripts/pg_compat/extract_statements.py`
- Create: `tests/pg_compat/test_extract_statements.py`

- [ ] **Step 1: Write failing inventory tests**

Create tests covering:

```python
def test_deduplicates_same_normalized_statement(self):
    rows = [
        {"normalized_sql": "SELECT 1", "oracle_node": "SELECT", "source_file": "a.sql", "line": 1},
        {"normalized_sql": "SELECT 1", "oracle_node": "SELECT", "source_file": "b.sql", "line": 3},
    ]
    inventory = build_inventory(rows)
    self.assertEqual(len(inventory), 1)
    self.assertEqual(len(inventory[0]["occurrences"]), 2)

def test_oracle_rejections_are_diagnostics_not_inventory(self):
    rows = [{"result": "ORACLE_REJECTED", "sql": "SELECT FROM"}]
    inventory, diagnostics = partition_rows(rows)
    self.assertEqual(inventory, [])
    self.assertEqual(len(diagnostics), 1)
```

- [ ] **Step 2: Run and verify failure**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_extract_statements.py' -v
```

Expected: import failure for `scripts.pg_compat.extract_statements`.

- [ ] **Step 3: Implement deterministic inventory construction**

Implement:

```python
def build_inventory(rows):
    grouped = {}
    for row in rows:
        if row["result"] == "ORACLE_REJECTED":
            continue
        key = (row["oracle_node"], row["normalized_sql"])
        occurrence = {
            "source_file": row["source_file"],
            "offset": row["offset"],
            "line": row["line"],
        }
        if key not in grouped:
            record = dict(row)
            record["id"] = statement_id(row["normalized_sql"], row["oracle_node"])
            record["occurrences"] = [occurrence]
            grouped[key] = record
        else:
            grouped[key]["occurrences"].append(occurrence)
    return sorted(grouped.values(), key=lambda row: row["id"])
```

The CLI accepts raw runner JSONL and writes:

- accepted deduplicated inventory JSONL
- oracle diagnostics JSONL

Use `atomic_write_text()` for both outputs.

- [ ] **Step 4: Run unit tests**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_extract_statements.py' -v
```

Expected: all tests pass.

- [ ] **Step 5: Run against the runner fixture**

Run:

```bash
python3 scripts/pg_compat/extract_statements.py \
  --input /tmp/runner-results.jsonl \
  --inventory /tmp/runner-inventory.jsonl \
  --diagnostics /tmp/runner-diagnostics.jsonl
```

Expected: inventory contains five unique accepted statements and diagnostics is empty.

- [ ] **Step 6: Commit**

```bash
git add scripts/pg_compat/extract_statements.py \
  tests/pg_compat/test_extract_statements.py
git commit -m "feat: build deterministic PostgreSQL statement inventories"
```

### Task 5: Compute Release and Structural Deltas

**Files:**
- Create: `scripts/pg_compat/compare_versions.py`
- Create: `tests/pg_compat/test_compare_versions.py`
- Create: `tests/pg_compat/fixtures/*`

- [ ] **Step 1: Add small PG17 and PG18 inventory fixtures**

Use three records:

- shared `SELECT 1`
- PG17-only `CREATE TABLE old_table`
- PG18-only `CREATE TABLE new_table`

Give each record its real `statement_id()` result.

- [ ] **Step 2: Write failing release-delta tests**

Test that:

- the shared ID is unchanged
- one record is `added`
- one record is `removed`
- matching source file plus changed normalized SQL appears in `changed`

- [ ] **Step 3: Write failing structural-diff tests**

Fixture changes must include:

- one new `gram.y` production alternative
- one keyword category change
- one new parse-node field
- one new protobuf field

Assert stable feature IDs and classifications:

```python
self.assertEqual(
    {feature["kind"] for feature in features},
    {"grammar", "keyword", "parse_node", "protobuf"},
)
```

- [ ] **Step 4: Run and verify failure**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_compare_versions.py' -v
```

Expected: import failure for `scripts.pg_compat.compare_versions`.

- [ ] **Step 5: Implement inventory diffing**

Implement maps by `id`, plus a secondary map by source file and nearest line for `changed` diagnostics. Output:

```json
{
  "added": [],
  "removed": [],
  "changed": [],
  "new_oracle_nodes": []
}
```

- [ ] **Step 6: Implement structural extraction**

Implement focused text extractors:

- `gram.y`: production name and normalized alternative text, excluding C semantic-action bodies
- `kwlist.h`: `PG_KEYWORD("word", TOKEN, CATEGORY, BARE_LABEL_STATUS)`
- `parsenodes.h`: struct names and field declarations
- protobuf: message, enum, and field declarations

Hash `kind + symbol + normalized_change` into stable 24-character feature IDs. Emit unmatched additions as `UNWITNESSED_FEATURE` until witness metadata references that feature ID.

- [ ] **Step 7: Run unit tests**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_compare_versions.py' -v
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add scripts/pg_compat/compare_versions.py \
  tests/pg_compat/test_compare_versions.py \
  tests/pg_compat/fixtures
git commit -m "feat: detect PostgreSQL release and grammar deltas"
```

### Task 6: Implement Baseline Transition Policy and CI Case Generation

**Files:**
- Create: `scripts/pg_compat/baseline.py`
- Create: `tests/pg_compat/test_baseline.py`

- [ ] **Step 1: Write the complete transition matrix test**

Create a table-driven test for every old/new pair among:

```python
RESULTS = (
    "DEEP_SUPPORTED",
    "CLASSIFIED_ONLY",
    "PARTIAL",
    "ERROR",
    "TRAILING_INPUT",
    "TYPE_MISMATCH",
)
```

Allowed transitions:

- unchanged result
- any result to `DEEP_SUPPORTED`
- `PARTIAL`, `ERROR`, `TRAILING_INPUT`, or `TYPE_MISMATCH` to `CLASSIFIED_ONLY`

Everything else must be reported as a regression or review-required change.

- [ ] **Step 2: Write CI selection tests**

Assert that `build_ci_cases()` includes:

- every known non-`DEEP_SUPPORTED` result
- every release-delta statement
- every witness
- one `DEEP_SUPPORTED` and one `CLASSIFIED_ONLY` example per oracle node where available

- [ ] **Step 3: Run and verify failure**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_baseline.py' -v
```

Expected: import failure.

- [ ] **Step 4: Implement transition evaluation**

Use:

```python
def transition_allowed(previous, current):
    if previous == current:
        return True
    if current == "DEEP_SUPPORTED":
        return True
    return (
        previous in {"PARTIAL", "ERROR", "TRAILING_INPUT", "TYPE_MISMATCH"}
        and current == "CLASSIFIED_ONLY"
    )
```

Unknown statement IDs in the current results are new cases and require baseline refresh. Missing IDs are reported separately because upstream pins or extraction behavior changed.

- [ ] **Step 5: Implement deterministic CI selection**

Sort selected cases by ID and write full SQL text plus expected result to `ci_cases.jsonl`. Never sample randomly.

- [ ] **Step 6: Run unit tests**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_baseline.py' -v
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add scripts/pg_compat/baseline.py tests/pg_compat/test_baseline.py
git commit -m "feat: enforce PostgreSQL compatibility baselines"
```

### Task 7: Add Reviewed Witnesses and Metadata Validation

**Files:**
- Create: `tests/pg_compat/witnesses.sql`
- Create: `tests/pg_compat/witnesses.json`
- Create: `tests/pg_compat/structural_dispositions.json`
- Modify: `scripts/pg_compat/baseline.py`
- Modify: `tests/pg_compat/test_baseline.py`

- [ ] **Step 1: Write failing witness-validation tests**

Test these errors:

- duplicate witness ID
- metadata count differs from split SQL count
- unsupported `first_postgresql_major`
- missing `structural_feature_ids`
- oracle node differs from `expected_oracle_node`

- [ ] **Step 2: Add initial witnesses**

Start with small, stable PostgreSQL statements that exercise the metadata path:

`tests/pg_compat/witnesses.sql`:

```sql
SELECT json_object('a' VALUE 1);
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET value = s.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value);
```

`tests/pg_compat/witnesses.json`:

```json
[
  {
    "id": "pg-json-object-constructor",
    "first_postgresql_major": 16,
    "expected_oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
    "structural_feature_ids": [],
    "note": "Exercises SQL/JSON constructor syntax through a SELECT."
  },
  {
    "id": "pg-merge-statement",
    "first_postgresql_major": 15,
    "expected_oracle_node": "PG_QUERY__NODE__NODE_MERGE_STMT",
    "structural_feature_ids": [],
    "note": "Known PostgreSQL statement with no ParserSQL StmtType equivalent."
  }
]
```

Empty structural feature lists are valid only for seed witnesses and must be reported as `unlinked_witnesses` until a refresh links them to extracted features.

Create `tests/pg_compat/structural_dispositions.json` initially as:

```json
[]
```

Each later entry has:

```json
{
  "feature_id": "stable-24-character-id",
  "disposition": "internal_non_syntax",
  "reason": "The changed field affects parse-tree serialization only and introduces no accepted SQL form."
}
```

- [ ] **Step 3: Implement validation**

Parse witnesses through the target runner. Join split statements and metadata by order, then require unique IDs and matching oracle node names.

- [ ] **Step 4: Run witness tests**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_baseline.py' -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/pg_compat/witnesses.sql \
  tests/pg_compat/witnesses.json \
  tests/pg_compat/structural_dispositions.json \
  scripts/pg_compat/baseline.py \
  tests/pg_compat/test_baseline.py
git commit -m "test: add reviewed PostgreSQL syntax witnesses"
```

### Task 8: Generate Markdown Compatibility Reports

**Files:**
- Create: `scripts/pg_compat/generate_report.py`
- Create: `tests/pg_compat/test_generate_report.py`

- [ ] **Step 1: Write failing snapshot assertions**

Given fixed fixture inputs, assert the report contains:

```text
# PostgreSQL 18 Compatibility
DEEP_SUPPORTED
CLASSIFIED_ONLY
PG17 to PG18 Release Delta
Unwitnessed Structural Features
Reproduction
```

Also assert deterministic ordering by result, oracle node, and statement ID.

- [ ] **Step 2: Run and verify failure**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_generate_report.py' -v
```

Expected: import failure.

- [ ] **Step 3: Implement report generation**

The report header must include:

- generation timestamp
- ParserSQL commit
- both `libpg_query` branches and SHAs
- PostgreSQL 17 and 18 patch versions
- exact commands

Tables:

- totals for all six compatibility results
- PG18 backlog excluding `DEEP_SUPPORTED`
- `CLASSIFIED_ONLY` routing coverage
- release-delta outcomes
- newly supported and regressed baseline transitions
- structural features and witness links

Limit SQL shown in Markdown to 200 characters, but retain complete SQL in JSONL results.

- [ ] **Step 4: Run report tests**

Run:

```bash
python3 -m unittest discover -s tests/pg_compat -p 'test_generate_report.py' -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/pg_compat/generate_report.py \
  tests/pg_compat/test_generate_report.py
git commit -m "feat: generate PostgreSQL compatibility reports"
```

### Task 9: Add Orchestration and Makefile Targets

**Files:**
- Create: `scripts/pg_compat/run_compat.py`
- Modify: `Makefile`
- Modify: `.gitignore`

- [ ] **Step 1: Implement orchestration modes**

`run_compat.py` supports:

```text
test       run Python tests and committed CI cases
full       use committed pins, run both complete upstream suites, check baseline
refresh    resolve branch heads, print pin changes, regenerate baseline/report
```

The `full` pipeline is:

1. fetch pinned sources
2. build both `libpg_query` libraries and runners
3. run PG17 and PG18 regression directories
4. build deduplicated inventories
5. run and validate witnesses
6. invoke `fetch_libpg_query.sh --with-postgres-source` and extract matching PostgreSQL source
7. compute statement and structural deltas
8. compare PG18 results with `expected_results.jsonl`
9. generate `/tmp` candidate baseline, CI cases, and report
10. replace committed generated files only in `refresh` mode

- [ ] **Step 2: Add build variables and targets**

Add:

```make
PG_COMPAT_CACHE ?= /tmp/parsersql-pg-compat

.PHONY: build-pg-compat pg-compat pg-compat-refresh test-pg-compat

build-pg-compat: lib
	PG_COMPAT_CACHE=$(PG_COMPAT_CACHE) ./scripts/pg_compat/fetch_libpg_query.sh
	python3 ./scripts/pg_compat/run_compat.py build --cache $(PG_COMPAT_CACHE)

test-pg-compat: build-pg-compat
	python3 -m unittest discover -s tests/pg_compat -p 'test_*.py' -v
	python3 ./scripts/pg_compat/run_compat.py test --cache $(PG_COMPAT_CACHE)

pg-compat: build-pg-compat
	python3 ./scripts/pg_compat/run_compat.py full --cache $(PG_COMPAT_CACHE)

pg-compat-refresh: build-pg-compat
	python3 ./scripts/pg_compat/run_compat.py refresh --cache $(PG_COMPAT_CACHE)
```

The `build` mode compiles one runner per role with that role's include paths and static library.

- [ ] **Step 3: Ignore local runner names if emitted in the repository**

Add:

```gitignore
pg_compat_17
pg_compat_18
```

Prefer cache-local binaries, so these entries are defensive.

- [ ] **Step 4: Run deterministic tests**

Run:

```bash
make test-pg-compat PG_COMPAT_CACHE=/tmp/parsersql-pg-compat-test
```

Expected: Python tests pass, mapping test passes, and committed CI cases match their expected results.

- [ ] **Step 5: Commit**

```bash
git add Makefile .gitignore scripts/pg_compat/run_compat.py
git commit -m "build: add PostgreSQL compatibility commands"
```

### Task 10: Run the First Full PG17/PG18 Refresh

**Files:**
- Modify: `tools/pg_compat/statement_type_map.cpp`
- Create: `tests/pg_compat/expected_results.jsonl`
- Create: `tests/pg_compat/ci_cases.jsonl`
- Create: `docs/compatibility/postgresql-18.md`
- Modify: `tests/pg_compat/witnesses.json`
- Modify: `tests/pg_compat/structural_dispositions.json`
- Modify: `tests/pg_compat/upstream_pins.json` only if branch heads changed intentionally

- [ ] **Step 1: Run refresh into temporary outputs**

Run:

```bash
make pg-compat-refresh PG_COMPAT_CACHE=/tmp/parsersql-pg-compat
```

Expected: the first run may stop with a complete list of unmapped top-level protobuf nodes.

- [ ] **Step 2: Classify every encountered top-level node explicitly**

For each reported node:

- map to an existing ParserSQL `StmtType` only when ParserSQL has that semantic category
- map to `MappingKind::NoEquivalent` when ParserSQL has no category
- add a focused assertion to `statement_type_cases.cpp`

Rerun:

```bash
make pg-compat-refresh PG_COMPAT_CACHE=/tmp/parsersql-pg-compat
```

Expected: no `Unmapped` nodes remain.

- [ ] **Step 3: Review structural features**

For each `UNWITNESSED_FEATURE`:

- link an existing accepted regression statement by ID, or
- add one minimal oracle-accepted witness, or
- mark it `internal_non_syntax` with a concrete explanation in `tests/pg_compat/structural_dispositions.json`

Do not suppress a grammar production merely because ParserSQL lacks support.

- [ ] **Step 4: Inspect generated baseline and report**

Run:

```bash
python3 - <<'PY'
import json
from collections import Counter
rows = [json.loads(line) for line in open("tests/pg_compat/expected_results.jsonl")]
print(Counter(row["result"] for row in rows))
assert rows
assert any(row["result"] == "DEEP_SUPPORTED" for row in rows)
assert any(row["result"] == "CLASSIFIED_ONLY" for row in rows)
PY
git diff -- tests/pg_compat docs/compatibility/postgresql-18.md
```

Expected: nonempty full baseline, separate deep/classified counts, release delta, and structural feature section.

- [ ] **Step 5: Re-run without refresh**

Run:

```bash
make pg-compat PG_COMPAT_CACHE=/tmp/parsersql-pg-compat
```

Expected: exits `0` against the newly committed baseline candidate.

- [ ] **Step 6: Commit**

```bash
git add tools/pg_compat/statement_type_map.cpp \
  tests/pg_compat/statement_type_cases.cpp \
  tests/pg_compat/expected_results.jsonl \
  tests/pg_compat/ci_cases.jsonl \
  tests/pg_compat/witnesses.json \
  tests/pg_compat/structural_dispositions.json \
  tests/pg_compat/upstream_pins.json \
  docs/compatibility/postgresql-18.md
git commit -m "test: baseline ParserSQL against PostgreSQL 18"
```

### Task 11: Add CI Coverage

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add deterministic pull-request compatibility job**

Add an Ubuntu 24.04 job:

```yaml
  pg-compat:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4

      - name: Cache libpg_query
        uses: actions/cache@v4
        with:
          path: /tmp/parsersql-pg-compat
          key: pg-compat-${{ hashFiles('tests/pg_compat/upstream_pins.json') }}

      - name: Run PostgreSQL compatibility CI cases
        run: make test-pg-compat
```

- [ ] **Step 2: Add scheduled full-suite workflow trigger**

Extend workflow triggers:

```yaml
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: "17 3 * * 1"
```

Add a full job guarded by:

```yaml
if: github.event_name == 'schedule'
```

and run:

```yaml
- name: Run full pinned PostgreSQL compatibility suite
  run: make pg-compat
```

Scheduled CI uses committed pins. Advancing pins remains a reviewed local `pg-compat-refresh` operation.

- [ ] **Step 3: Validate workflow syntax and local target**

Run:

```bash
make test-pg-compat
git diff --check
```

Expected: all compatibility tests pass and no whitespace errors are reported.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: check PostgreSQL parser compatibility"
```

### Task 12: Document Use and Run Final Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/benchmarks/REPRODUCING.md`

- [ ] **Step 1: Document compatibility commands**

Add a concise README section describing:

- `libpg_query` as the PostgreSQL syntax oracle
- `DEEP_SUPPORTED` versus `CLASSIFIED_ONLY`
- full PG18 backlog and PG17-to-PG18 delta
- `make test-pg-compat`, `make pg-compat`, and `make pg-compat-refresh`

- [ ] **Step 2: Add reproducibility prerequisites**

Document:

```text
git
curl
tar with bzip2 support
python3
C/C++ compiler
make
```

State that `PG_COMPAT_CACHE` controls external sources and defaults to `/tmp/parsersql-pg-compat`.

- [ ] **Step 3: Run unit and compatibility verification**

Run:

```bash
make clean
make test
make test-pg-compat
make pg-compat
git diff --check
git status --short
```

Expected:

- ParserSQL GoogleTest suite passes
- compatibility unit and CI cases pass
- full pinned PG17/PG18 comparison matches the committed baseline
- no whitespace errors
- only intended documentation changes remain before commit

- [ ] **Step 4: Commit**

```bash
git add README.md docs/benchmarks/REPRODUCING.md
git commit -m "docs: document PostgreSQL compatibility workflow"
```

- [ ] **Step 5: Review the complete branch**

Run:

```bash
git log --oneline --decorate -15
git diff main~12..HEAD --stat
```

Confirm:

- no production target links `libpg_query`
- no upstream checkout or binary is committed
- every accepted PostgreSQL statement is one of the six compatibility results
- only `DEEP_SUPPORTED` contributes to full parser parity
- `CLASSIFIED_ONLY` is reported separately
- new unmapped PostgreSQL top-level nodes fail the harness
- ordinary CI is deterministic and full CI uses committed pins
