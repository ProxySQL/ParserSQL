# INSERT/REPLACE Deep Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract shared table reference parsing into `TableRefParser<D>`, then build a full INSERT/REPLACE deep parser for MySQL and PostgreSQL with emitter support and comprehensive tests.

**Architecture:** First, the table reference methods (`parse_from_clause`, `parse_table_reference`, `parse_join`, `parse_optional_alias`, `is_join_start`, `is_alias_start`) are extracted from `SelectParser<D>` into a standalone `TableRefParser<D>` utility class. Then, `InsertParser<D>` is built as a header-only template following the same pattern as `SelectParser<D>` and `SetParser<D>`. It handles VALUES, SELECT, SET (MySQL), ON DUPLICATE KEY UPDATE (MySQL), ON CONFLICT (PostgreSQL), and RETURNING (PostgreSQL). The parser is integrated via `parser.cpp` by replacing the existing `extract_insert()` and `extract_replace()` Tier 2 extractors.

**Tech Stack:** C++17, existing parser infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md`

---

## Scope

This plan builds:
1. `TableRefParser<D>` — shared FROM/JOIN/table ref parsing utility, extracted from SelectParser
2. `InsertParser<D>` — full INSERT/REPLACE deep parser for both dialects
3. Emitter extensions for all INSERT-related node types
4. Classifier updates to route TK_INSERT and TK_REPLACE to the deep parser
5. Comprehensive tests including round-trip tests

**Closes:** #5

**Dependencies:** None (this is the first plan in the Tier 1 promotions series).

---

## File Structure

```
include/sql_parser/
    table_ref_parser.h    — (create) shared FROM/JOIN/table reference parsing
    insert_parser.h       — (create) INSERT/REPLACE statement parser
    select_parser.h       — (modify) replace private methods with TableRefParser calls
    emitter.h             — (modify) add INSERT emit methods
    common.h              — (modify) add new NodeType values
    token.h               — (modify) add new TokenType values

src/sql_parser/
    parser.cpp            — (modify) replace extract_insert/extract_replace with parse_insert()

include/sql_parser/
    parser.h              — (modify) add parse_insert() declaration, remove extract_insert/extract_replace

include/sql_parser/
    keywords_mysql.h      — (modify) add new keywords
    keywords_pgsql.h      — (modify) add new keywords

tests/
    test_insert.cpp       — (create) INSERT parser tests

Makefile.new              — (modify) add test_insert.cpp to TEST_SRCS
```

---

### Task 1: Extract TableRefParser from SelectParser

**Files:**
- Create: `include/sql_parser/table_ref_parser.h`
- Modify: `include/sql_parser/select_parser.h`

This task extracts all table reference parsing logic into a shared utility class. SelectParser's private methods are replaced with calls to TableRefParser. All existing tests must continue to pass.

- [ ] **Step 1: Create `table_ref_parser.h` with extracted methods**

Create `include/sql_parser/table_ref_parser.h`:
```cpp
#ifndef SQL_PARSER_TABLE_REF_PARSER_H
#define SQL_PARSER_TABLE_REF_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"

namespace sql_parser {

template <Dialect D>
class TableRefParser {
public:
    TableRefParser(Tokenizer<D>& tokenizer, Arena& arena,
                   ExpressionParser<D>& expr_parser)
        : tok_(tokenizer), arena_(arena), expr_parser_(expr_parser) {}

    // Parse a FROM clause: table_ref [, table_ref | JOIN ...]*
    AstNode* parse_from_clause();

    // Parse a single table reference (simple name, qualified name, subquery)
    AstNode* parse_table_reference();

    // Parse a JOIN clause (modifiers + JOIN keyword already detected by caller)
    AstNode* parse_join(AstNode* left_ref);

    // Parse optional alias (AS name or implicit alias)
    void parse_optional_alias(AstNode* parent);

    // Check if a token can start a JOIN
    static bool is_join_start(TokenType type);

    // Check if a token can start an implicit alias
    static bool is_alias_start(TokenType type);

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D>& expr_parser_;
};

} // namespace sql_parser

#endif // SQL_PARSER_TABLE_REF_PARSER_H
```

The implementation follows the exact same logic currently in `SelectParser<D>`. Move the method bodies from `select_parser.h` into this class.

- [ ] **Step 2: Update SelectParser to use TableRefParser**

Modify `include/sql_parser/select_parser.h`:
- Add `#include "sql_parser/table_ref_parser.h"`
- Add a `TableRefParser<D> table_ref_parser_` member, initialized in the constructor
- Replace all calls to the private `parse_from_clause()`, `parse_table_reference()`, `parse_join()`, `parse_optional_alias()` with calls to `table_ref_parser_.parse_from_clause()`, etc.
- Remove the private method implementations that were moved
- Keep `is_alias_start()` calls in `parse_select_item()` pointing to `TableRefParser<D>::is_alias_start()`

- [ ] **Step 3: Run all existing tests — they must pass unchanged**

```bash
make -f Makefile.new test
```

No test changes should be needed. This is a pure refactoring step.

---

### Task 2: Add New Tokens and Node Types

**Files:**
- Modify: `include/sql_parser/token.h`
- Modify: `include/sql_parser/common.h`
- Modify: `include/sql_parser/keywords_mysql.h`
- Modify: `include/sql_parser/keywords_pgsql.h`

- [ ] **Step 1: Add new token types to `token.h`**

Add after the existing `TK_SQL_CALC_FOUND_ROWS` line:
```cpp
TK_DELAYED,
TK_HIGH_PRIORITY,
TK_DUPLICATE,
TK_KEY,
TK_CONFLICT,
TK_DO,
TK_NOTHING,
TK_RETURNING,
TK_CONSTRAINT,
```

- [ ] **Step 2: Add new node types to `common.h`**

Add after the existing `NODE_CASE_WHEN` entry:
```cpp
// INSERT nodes
NODE_INSERT_STMT,
NODE_INSERT_COLUMNS,       // (col1, col2, ...)
NODE_VALUES_CLAUSE,        // VALUES keyword wrapper
NODE_VALUES_ROW,           // single (val1, val2, ...) row
NODE_INSERT_SET_CLAUSE,    // MySQL INSERT ... SET col=val form
NODE_ON_DUPLICATE_KEY,     // MySQL ON DUPLICATE KEY UPDATE
NODE_ON_CONFLICT,          // PostgreSQL ON CONFLICT
NODE_CONFLICT_TARGET,      // PostgreSQL conflict target (cols or ON CONSTRAINT)
NODE_CONFLICT_ACTION,      // DO UPDATE SET ... or DO NOTHING
NODE_RETURNING_CLAUSE,     // PostgreSQL RETURNING expr_list

// Shared
NODE_STMT_OPTIONS,         // LOW_PRIORITY, IGNORE, QUICK, DELAYED, etc.
NODE_UPDATE_SET_ITEM,      // single col=expr pair (shared by INSERT SET and UPDATE SET)
```

- [ ] **Step 3: Register keywords in keyword tables**

Add to `keywords_mysql.h`: DELAYED, HIGH_PRIORITY, DUPLICATE, KEY, CONFLICT, DO, NOTHING, RETURNING, CONSTRAINT.

Add to `keywords_pgsql.h`: CONFLICT, DO, NOTHING, RETURNING, CONSTRAINT.

- [ ] **Step 4: Update `is_alias_start()` blocklist in `TableRefParser`**

Add to the blocklist: `TK_RETURNING`, `TK_CONFLICT`, `TK_DO`, `TK_NOTHING`, `TK_DUPLICATE`.

---

### Task 3: INSERT Parser — Core Structure, VALUES, Column List

**Files:**
- Create: `include/sql_parser/insert_parser.h`
- Create: `tests/test_insert.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Write tests for basic INSERT parsing**

Create `tests/test_insert.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLInsertTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    int child_count(const AstNode* node) {
        int n = 0;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
        return n;
    }

    const AstNode* find_child(const AstNode* node, NodeType type) {
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == type) return c;
        }
        return nullptr;
    }

    std::string round_trip(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        return std::string(result.ptr, result.len);
    }
};

// ========== Basic INSERT ==========

TEST_F(MySQLInsertTest, SimpleInsert) {
    auto r = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')", 49);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_INSERT_STMT);
}

TEST_F(MySQLInsertTest, InsertWithoutInto) {
    auto r = parser.parse("INSERT users (id) VALUES (1)", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLInsertTest, InsertWithoutColumnList) {
    auto r = parser.parse("INSERT INTO users VALUES (1, 'Alice')", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* cols = find_child(r.ast, NodeType::NODE_INSERT_COLUMNS);
    EXPECT_EQ(cols, nullptr);  // no column list
}

TEST_F(MySQLInsertTest, InsertColumnList) {
    auto r = parser.parse("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')", 67);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* cols = find_child(r.ast, NodeType::NODE_INSERT_COLUMNS);
    ASSERT_NE(cols, nullptr);
    EXPECT_EQ(child_count(cols), 3);
}

TEST_F(MySQLInsertTest, InsertMultiRow) {
    auto r = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')", 60);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* values = find_child(r.ast, NodeType::NODE_VALUES_CLAUSE);
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(child_count(values), 2);  // two rows
}

TEST_F(MySQLInsertTest, InsertTableRef) {
    auto r = parser.parse("INSERT INTO mydb.users (id) VALUES (1)", 39);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* tref = find_child(r.ast, NodeType::NODE_TABLE_REF);
    ASSERT_NE(tref, nullptr);
}

// ========== MySQL Options ==========

TEST_F(MySQLInsertTest, InsertLowPriority) {
    auto r = parser.parse("INSERT LOW_PRIORITY INTO users (id) VALUES (1)", 47);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* opts = find_child(r.ast, NodeType::NODE_STMT_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(MySQLInsertTest, InsertDelayed) {
    auto r = parser.parse("INSERT DELAYED INTO users (id) VALUES (1)", 42);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLInsertTest, InsertHighPriority) {
    auto r = parser.parse("INSERT HIGH_PRIORITY INTO users (id) VALUES (1)", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLInsertTest, InsertIgnore) {
    auto r = parser.parse("INSERT IGNORE INTO users (id) VALUES (1)", 41);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLInsertTest, InsertLowPriorityIgnore) {
    auto r = parser.parse("INSERT LOW_PRIORITY IGNORE INTO users (id) VALUES (1)", 54);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== INSERT ... SELECT ==========

TEST_F(MySQLInsertTest, InsertSelect) {
    auto r = parser.parse("INSERT INTO users (id, name) SELECT id, name FROM temp_users", 60);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* select = find_child(r.ast, NodeType::NODE_SELECT_STMT);
    ASSERT_NE(select, nullptr);
}

TEST_F(MySQLInsertTest, InsertSelectWithWhere) {
    const char* sql = "INSERT INTO users (id, name) SELECT id, name FROM temp WHERE active = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL INSERT ... SET ==========

TEST_F(MySQLInsertTest, InsertSet) {
    auto r = parser.parse("INSERT INTO users SET id = 1, name = 'Alice'", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* set_clause = find_child(r.ast, NodeType::NODE_INSERT_SET_CLAUSE);
    ASSERT_NE(set_clause, nullptr);
    EXPECT_EQ(child_count(set_clause), 2);  // two col=val pairs
}

// ========== ON DUPLICATE KEY UPDATE ==========

TEST_F(MySQLInsertTest, OnDuplicateKey) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = 'Alice2'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* odku = find_child(r.ast, NodeType::NODE_ON_DUPLICATE_KEY);
    ASSERT_NE(odku, nullptr);
}

TEST_F(MySQLInsertTest, OnDuplicateKeyMultiple) {
    const char* sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com') "
                      "ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email)";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* odku = find_child(r.ast, NodeType::NODE_ON_DUPLICATE_KEY);
    ASSERT_NE(odku, nullptr);
    EXPECT_EQ(child_count(odku), 2);
}

// ========== REPLACE ==========

TEST_F(MySQLInsertTest, ReplaceSimple) {
    auto r = parser.parse("REPLACE INTO users (id, name) VALUES (1, 'Alice')", 50);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::REPLACE);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_INSERT_STMT);
    // REPLACE flag should be set in flags
    EXPECT_NE(r.ast->flags & 0x01, 0);  // FLAG_REPLACE = 0x01
}

TEST_F(MySQLInsertTest, ReplaceLowPriority) {
    auto r = parser.parse("REPLACE LOW_PRIORITY INTO users (id) VALUES (1)", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::REPLACE);
}

TEST_F(MySQLInsertTest, ReplaceDelayed) {
    auto r = parser.parse("REPLACE DELAYED INTO users (id) VALUES (1)", 43);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== PostgreSQL INSERT ==========

class PgSQLInsertTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

    int child_count(const AstNode* node) {
        int n = 0;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
        return n;
    }

    const AstNode* find_child(const AstNode* node, NodeType type) {
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == type) return c;
        }
        return nullptr;
    }

    std::string round_trip(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        return std::string(result.ptr, result.len);
    }
};

TEST_F(PgSQLInsertTest, SimpleInsert) {
    auto r = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')", 49);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLInsertTest, DefaultValues) {
    auto r = parser.parse("INSERT INTO users DEFAULT VALUES", 32);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLInsertTest, OnConflictDoNothing) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT DO NOTHING";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* oc = find_child(r.ast, NodeType::NODE_ON_CONFLICT);
    ASSERT_NE(oc, nullptr);
}

TEST_F(PgSQLInsertTest, OnConflictDoUpdate) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') "
                      "ON CONFLICT (id) DO UPDATE SET name = 'Alice2'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* oc = find_child(r.ast, NodeType::NODE_ON_CONFLICT);
    ASSERT_NE(oc, nullptr);
}

TEST_F(PgSQLInsertTest, OnConflictOnConstraint) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') "
                      "ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLInsertTest, OnConflictDoUpdateWhere) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') "
                      "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.active = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLInsertTest, Returning) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') RETURNING id, name";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ret = find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE);
    ASSERT_NE(ret, nullptr);
    EXPECT_EQ(child_count(ret), 2);
}

TEST_F(PgSQLInsertTest, ReturningStar) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') RETURNING *";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLInsertTest, OnConflictWithReturning) {
    const char* sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') "
                      "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING *";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ON_CONFLICT), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE), nullptr);
}

// ========== Bulk data-driven tests ==========

struct InsertTestCase {
    const char* sql;
    const char* description;
};

static const InsertTestCase mysql_insert_bulk_cases[] = {
    {"INSERT INTO t (a) VALUES (1)", "simple single column"},
    {"INSERT INTO t (a, b) VALUES (1, 2)", "two columns"},
    {"INSERT INTO t (a, b, c) VALUES (1, 2, 3)", "three columns"},
    {"INSERT INTO t VALUES (1, 2)", "no column list"},
    {"INSERT t (a) VALUES (1)", "without INTO"},
    {"INSERT INTO db.t (a) VALUES (1)", "qualified table"},
    {"INSERT INTO t (a) VALUES (1), (2), (3)", "multi-row"},
    {"INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')", "multi-row with strings"},
    {"INSERT LOW_PRIORITY INTO t (a) VALUES (1)", "low priority"},
    {"INSERT DELAYED INTO t (a) VALUES (1)", "delayed"},
    {"INSERT HIGH_PRIORITY INTO t (a) VALUES (1)", "high priority"},
    {"INSERT IGNORE INTO t (a) VALUES (1)", "ignore"},
    {"INSERT LOW_PRIORITY IGNORE INTO t (a) VALUES (1)", "low priority ignore"},
    {"INSERT INTO t SET a = 1", "set form single"},
    {"INSERT INTO t SET a = 1, b = 'x'", "set form multiple"},
    {"INSERT INTO t (a) SELECT a FROM t2", "insert select"},
    {"INSERT INTO t (a, b) SELECT a, b FROM t2 WHERE c > 0", "insert select with where"},
    {"INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a = 2", "on duplicate key"},
    {"INSERT INTO t (a, b) VALUES (1, 'x') ON DUPLICATE KEY UPDATE b = VALUES(b)", "odku values()"},
    {"INSERT INTO t (a, b) VALUES (1, 'x') ON DUPLICATE KEY UPDATE a = a + 1, b = 'y'", "odku multi"},
    {"REPLACE INTO t (a) VALUES (1)", "replace simple"},
    {"REPLACE INTO t (a, b) VALUES (1, 2)", "replace two cols"},
    {"REPLACE LOW_PRIORITY INTO t (a) VALUES (1)", "replace low priority"},
    {"REPLACE INTO t SET a = 1", "replace set form"},
};

TEST(MySQLInsertBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_insert_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

static const InsertTestCase pgsql_insert_bulk_cases[] = {
    {"INSERT INTO t (a) VALUES (1)", "simple"},
    {"INSERT INTO t (a, b) VALUES (1, 2)", "two columns"},
    {"INSERT INTO t VALUES (1, 2)", "no column list"},
    {"INSERT INTO t DEFAULT VALUES", "default values"},
    {"INSERT INTO t (a) VALUES (1), (2)", "multi-row"},
    {"INSERT INTO t (a) SELECT a FROM t2", "insert select"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING", "on conflict do nothing"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING", "on conflict col do nothing"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2", "on conflict do update"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING", "on constraint"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a", "excluded ref"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2 WHERE t.b > 0", "do update where"},
    {"INSERT INTO t (a) VALUES (1) RETURNING a", "returning single"},
    {"INSERT INTO t (a) VALUES (1) RETURNING *", "returning star"},
    {"INSERT INTO t (a) VALUES (1) RETURNING a, b", "returning multi"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING RETURNING *", "conflict + returning"},
};

TEST(PgSQLInsertBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_insert_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== Round-trip tests ==========

static const InsertTestCase mysql_insert_roundtrip_cases[] = {
    {"INSERT INTO t (a) VALUES (1)", "simple"},
    {"INSERT INTO t (a, b) VALUES (1, 'x')", "two cols with string"},
    {"INSERT INTO t (a) VALUES (1), (2), (3)", "multi-row"},
    {"INSERT INTO t SET a = 1, b = 'x'", "set form"},
    {"INSERT LOW_PRIORITY IGNORE INTO t (a) VALUES (1)", "options"},
    {"INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a = 2", "odku"},
    {"REPLACE INTO t (a, b) VALUES (1, 2)", "replace"},
};

TEST(MySQLInsertRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_insert_roundtrip_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        ASSERT_NE(r.ast, nullptr)
            << "Parse failed: " << tc.description << "\n  SQL: " << tc.sql;
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        std::string out(result.ptr, result.len);
        EXPECT_EQ(out, std::string(tc.sql))
            << "Round-trip mismatch: " << tc.description;
    }
}

static const InsertTestCase pgsql_insert_roundtrip_cases[] = {
    {"INSERT INTO t (a) VALUES (1)", "simple"},
    {"INSERT INTO t DEFAULT VALUES", "default values"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING", "on conflict do nothing"},
    {"INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2", "on conflict do update"},
    {"INSERT INTO t (a) VALUES (1) RETURNING *", "returning star"},
};

TEST(PgSQLInsertRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_insert_roundtrip_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        ASSERT_NE(r.ast, nullptr)
            << "Parse failed: " << tc.description << "\n  SQL: " << tc.sql;
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        std::string out(result.ptr, result.len);
        EXPECT_EQ(out, std::string(tc.sql))
            << "Round-trip mismatch: " << tc.description;
    }
}
```

- [ ] **Step 2: Add test_insert.cpp to Makefile.new**

Add `$(TEST_DIR)/test_insert.cpp \` to the `TEST_SRCS` list.

- [ ] **Step 3: Implement InsertParser class declaration**

Create `include/sql_parser/insert_parser.h`:
```cpp
#ifndef SQL_PARSER_INSERT_PARSER_H
#define SQL_PARSER_INSERT_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/table_ref_parser.h"
#include "sql_parser/select_parser.h"

namespace sql_parser {

// Flag on NODE_INSERT_STMT to indicate REPLACE
static constexpr uint16_t FLAG_REPLACE = 0x01;

template <Dialect D>
class InsertParser {
public:
    InsertParser(Tokenizer<D>& tokenizer, Arena& arena, bool is_replace = false)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena),
          table_ref_parser_(tokenizer, arena, expr_parser_),
          is_replace_(is_replace) {}

    // Parse INSERT/REPLACE statement (INSERT/REPLACE keyword already consumed).
    AstNode* parse();

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    TableRefParser<D> table_ref_parser_;
    bool is_replace_;

    // Parse MySQL options: LOW_PRIORITY, DELAYED, HIGH_PRIORITY, IGNORE
    AstNode* parse_stmt_options();

    // Parse column list: (col1, col2, ...)
    AstNode* parse_column_list();

    // Parse VALUES clause: VALUES (row1), (row2), ...
    AstNode* parse_values_clause();

    // Parse a single values row: (expr, expr, ...)
    AstNode* parse_values_row();

    // Parse MySQL SET form: SET col=val, col=val, ...
    AstNode* parse_insert_set_clause();

    // Parse a single col=expr pair
    AstNode* parse_set_item();

    // Parse MySQL ON DUPLICATE KEY UPDATE
    AstNode* parse_on_duplicate_key();

    // Parse PostgreSQL ON CONFLICT
    AstNode* parse_on_conflict();

    // Parse PostgreSQL RETURNING
    AstNode* parse_returning();
};

} // namespace sql_parser

#endif // SQL_PARSER_INSERT_PARSER_H
```

- [ ] **Step 4: Implement InsertParser parse methods**

Implement all methods in the header following the spec's syntax. Key logic:
- `parse()`: options -> table ref -> column list -> (VALUES | SELECT | SET | DEFAULT VALUES) -> (ON DUPLICATE KEY UPDATE | ON CONFLICT) -> RETURNING
- Use `if constexpr (D == Dialect::MySQL)` for MySQL-only features (SET form, ON DUPLICATE KEY, DELAYED/HIGH_PRIORITY)
- Use `if constexpr (D == Dialect::PostgreSQL)` for PostgreSQL-only features (ON CONFLICT, RETURNING, DEFAULT VALUES)
- For INSERT ... SELECT, instantiate `SelectParser<D>` and call its `parse()` method
- Refer to `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md` for full syntax details

---

### Task 4: Emitter Support for INSERT Nodes

**Files:**
- Modify: `include/sql_parser/emitter.h`

- [ ] **Step 1: Add emit methods for all INSERT node types**

Add cases to the `emit_node()` switch:
```cpp
case NodeType::NODE_INSERT_STMT:     emit_insert_stmt(node); break;
case NodeType::NODE_INSERT_COLUMNS:  emit_insert_columns(node); break;
case NodeType::NODE_VALUES_CLAUSE:   emit_values_clause(node); break;
case NodeType::NODE_VALUES_ROW:      emit_values_row(node); break;
case NodeType::NODE_INSERT_SET_CLAUSE: emit_insert_set_clause(node); break;
case NodeType::NODE_ON_DUPLICATE_KEY: emit_on_duplicate_key(node); break;
case NodeType::NODE_ON_CONFLICT:     emit_on_conflict(node); break;
case NodeType::NODE_CONFLICT_TARGET: emit_conflict_target(node); break;
case NodeType::NODE_CONFLICT_ACTION: emit_conflict_action(node); break;
case NodeType::NODE_RETURNING_CLAUSE: emit_returning(node); break;
case NodeType::NODE_STMT_OPTIONS:    emit_stmt_options(node); break;
case NodeType::NODE_UPDATE_SET_ITEM: emit_update_set_item(node); break;
```

- [ ] **Step 2: Implement each emit method**

Key emit methods (signatures only — implementer fills in bodies following existing emitter patterns):

```cpp
void emit_insert_stmt(const AstNode* node);       // INSERT/REPLACE INTO table ...
void emit_insert_columns(const AstNode* node);     // (col1, col2, ...)
void emit_values_clause(const AstNode* node);       // VALUES (row), (row)
void emit_values_row(const AstNode* node);          // (val, val, ...)
void emit_insert_set_clause(const AstNode* node);   // SET col=val, col=val
void emit_on_duplicate_key(const AstNode* node);    // ON DUPLICATE KEY UPDATE col=val
void emit_on_conflict(const AstNode* node);         // ON CONFLICT ... DO ...
void emit_conflict_target(const AstNode* node);     // (cols) or ON CONSTRAINT name
void emit_conflict_action(const AstNode* node);     // DO UPDATE SET ... or DO NOTHING
void emit_returning(const AstNode* node);           // RETURNING expr, expr
void emit_stmt_options(const AstNode* node);        // LOW_PRIORITY IGNORE etc.
void emit_update_set_item(const AstNode* node);     // col = expr
```

`emit_insert_stmt()` must check the FLAG_REPLACE flag to emit "REPLACE" vs "INSERT".

---

### Task 5: Classifier Integration

**Files:**
- Modify: `include/sql_parser/parser.h`
- Modify: `src/sql_parser/parser.cpp`

- [ ] **Step 1: Add `parse_insert()` method declaration to `parser.h`**

Add to the private section:
```cpp
ParseResult parse_insert(bool is_replace = false);
```

Remove (or keep as fallback, but they will no longer be called):
```cpp
// These are superseded by parse_insert():
// ParseResult extract_insert(const Token& first);
// ParseResult extract_replace(const Token& first);
```

- [ ] **Step 2: Implement `parse_insert()` in `parser.cpp`**

```cpp
template <Dialect D>
ParseResult Parser<D>::parse_insert(bool is_replace) {
    ParseResult r;
    r.stmt_type = is_replace ? StmtType::REPLACE : StmtType::INSERT;

    InsertParser<D> insert_parser(tokenizer_, arena_, is_replace);
    AstNode* ast = insert_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}
```

- [ ] **Step 3: Update `classify_and_dispatch()` switch**

Replace:
```cpp
case TokenType::TK_INSERT:   return extract_insert(first);
// ...
case TokenType::TK_REPLACE:  return extract_replace(first);
```

With:
```cpp
case TokenType::TK_INSERT:   return parse_insert(false);
case TokenType::TK_REPLACE:  return parse_insert(true);
```

- [ ] **Step 4: Add `#include "sql_parser/insert_parser.h"` to `parser.cpp`**

- [ ] **Step 5: Run all tests**

```bash
make -f Makefile.new test
```

All existing tests plus new INSERT tests should pass.
