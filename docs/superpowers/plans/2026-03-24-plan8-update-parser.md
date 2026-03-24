# UPDATE Deep Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a full UPDATE deep parser for MySQL and PostgreSQL that handles multi-table JOINs (MySQL), FROM clause (PostgreSQL), SET assignments, ORDER BY/LIMIT (MySQL), and RETURNING (PostgreSQL).

**Architecture:** `UpdateParser<D>` is a header-only template class following the established pattern. It uses `TableRefParser<D>` (from Plan 7) for table reference parsing, `ExpressionParser<D>` for all expression positions, and reuses `NODE_UPDATE_SET_ITEM` (also from Plan 7) for SET assignments. The parser is integrated via `parser.cpp` by replacing the existing `extract_update()` Tier 2 extractor.

**Tech Stack:** C++17, existing parser infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md`

---

## Scope

This plan builds:
1. `UpdateParser<D>` — full UPDATE deep parser for both dialects
2. Emitter extensions for UPDATE-specific node types
3. Classifier update to route TK_UPDATE to the deep parser
4. Comprehensive tests including round-trip tests

**Closes:** #6

**Dependencies:** Plan 7 (provides `TableRefParser<D>`, `NODE_STMT_OPTIONS`, `NODE_UPDATE_SET_ITEM`, `NODE_RETURNING_CLAUSE`, and related tokens/emitter infrastructure).

---

## File Structure

```
include/sql_parser/
    update_parser.h       — (create) UPDATE statement parser (header-only template)
    emitter.h             — (modify) add UPDATE emit methods
    common.h              — (modify) add NODE_UPDATE_STMT, NODE_UPDATE_SET_CLAUSE
    parser.h              — (modify) add parse_update() declaration, remove extract_update

src/sql_parser/
    parser.cpp            — (modify) replace extract_update with parse_update()

tests/
    test_update.cpp       — (create) UPDATE parser tests

Makefile.new              — (modify) add test_update.cpp to TEST_SRCS
```

---

### Task 1: Add UPDATE Node Types

**Files:**
- Modify: `include/sql_parser/common.h`

- [ ] **Step 1: Add new node types**

Add to `NodeType` enum (if not already added by Plan 7):
```cpp
// UPDATE nodes
NODE_UPDATE_STMT,
NODE_UPDATE_SET_CLAUSE,    // SET col=expr, col=expr in UPDATE context
```

Note: `NODE_UPDATE_SET_ITEM`, `NODE_STMT_OPTIONS`, and `NODE_RETURNING_CLAUSE` are already added by Plan 7.

---

### Task 2: UPDATE Parser Implementation

**Files:**
- Create: `include/sql_parser/update_parser.h`
- Create: `tests/test_update.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Write tests for UPDATE parsing**

Create `tests/test_update.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLUpdateTest : public ::testing::Test {
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

// ========== Basic UPDATE ==========

TEST_F(MySQLUpdateTest, SimpleUpdate) {
    auto r = parser.parse("UPDATE users SET name = 'Alice' WHERE id = 1", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::UPDATE);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_UPDATE_STMT);
}

TEST_F(MySQLUpdateTest, UpdateMultipleColumns) {
    const char* sql = "UPDATE users SET name = 'Alice', email = 'a@b.com' WHERE id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* set_clause = find_child(r.ast, NodeType::NODE_UPDATE_SET_CLAUSE);
    ASSERT_NE(set_clause, nullptr);
    EXPECT_EQ(child_count(set_clause), 2);
}

TEST_F(MySQLUpdateTest, UpdateNoWhere) {
    auto r = parser.parse("UPDATE users SET active = 0", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* where = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    EXPECT_EQ(where, nullptr);
}

TEST_F(MySQLUpdateTest, UpdateQualifiedTable) {
    auto r = parser.parse("UPDATE mydb.users SET name = 'x' WHERE id = 1", 46);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL Options ==========

TEST_F(MySQLUpdateTest, UpdateLowPriority) {
    auto r = parser.parse("UPDATE LOW_PRIORITY users SET name = 'x' WHERE id = 1", 54);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* opts = find_child(r.ast, NodeType::NODE_STMT_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(MySQLUpdateTest, UpdateIgnore) {
    auto r = parser.parse("UPDATE IGNORE users SET name = 'x' WHERE id = 1", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLUpdateTest, UpdateLowPriorityIgnore) {
    auto r = parser.parse("UPDATE LOW_PRIORITY IGNORE users SET name = 'x' WHERE id = 1", 61);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL ORDER BY + LIMIT ==========

TEST_F(MySQLUpdateTest, UpdateOrderByLimit) {
    const char* sql = "UPDATE users SET rank = rank + 1 WHERE active = 1 ORDER BY score DESC LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLUpdateTest, UpdateLimit) {
    auto r = parser.parse("UPDATE users SET active = 0 LIMIT 100", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

// ========== MySQL Multi-Table UPDATE ==========

TEST_F(MySQLUpdateTest, MultiTableJoin) {
    const char* sql = "UPDATE users u JOIN orders o ON u.id = o.user_id "
                      "SET u.total = u.total + o.amount WHERE o.status = 'shipped'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLUpdateTest, MultiTableCommaJoin) {
    const char* sql = "UPDATE users, orders SET users.total = orders.amount "
                      "WHERE users.id = orders.user_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLUpdateTest, MultiTableLeftJoin) {
    const char* sql = "UPDATE users u LEFT JOIN orders o ON u.id = o.user_id "
                      "SET u.has_orders = 0 WHERE o.id IS NULL";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== PostgreSQL UPDATE ==========

class PgSQLUpdateTest : public ::testing::Test {
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

TEST_F(PgSQLUpdateTest, SimpleUpdate) {
    auto r = parser.parse("UPDATE users SET name = 'Alice' WHERE id = 1", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::UPDATE);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateFrom) {
    const char* sql = "UPDATE users SET total = orders.amount FROM orders WHERE users.id = orders.user_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateFromMultipleTables) {
    const char* sql = "UPDATE users SET total = o.amount "
                      "FROM orders o, payments p "
                      "WHERE users.id = o.user_id AND o.id = p.order_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateReturning) {
    const char* sql = "UPDATE users SET name = 'Alice' WHERE id = 1 RETURNING id, name";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ret = find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE);
    ASSERT_NE(ret, nullptr);
    EXPECT_EQ(child_count(ret), 2);
}

TEST_F(PgSQLUpdateTest, UpdateReturningStar) {
    const char* sql = "UPDATE users SET name = 'Alice' WHERE id = 1 RETURNING *";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateFromReturning) {
    const char* sql = "UPDATE users SET total = o.amount FROM orders o "
                      "WHERE users.id = o.user_id RETURNING users.id, users.total";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_FROM_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE), nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateWithAlias) {
    const char* sql = "UPDATE users AS u SET name = 'Alice' WHERE u.id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Bulk data-driven tests ==========

struct UpdateTestCase {
    const char* sql;
    const char* description;
};

static const UpdateTestCase mysql_update_bulk_cases[] = {
    {"UPDATE t SET a = 1", "simple no where"},
    {"UPDATE t SET a = 1 WHERE b = 2", "simple with where"},
    {"UPDATE t SET a = 1, b = 2 WHERE c = 3", "multi column"},
    {"UPDATE t SET a = a + 1 WHERE b > 0", "expression value"},
    {"UPDATE t SET a = 'hello' WHERE b = 1", "string value"},
    {"UPDATE db.t SET a = 1", "qualified table"},
    {"UPDATE LOW_PRIORITY t SET a = 1", "low priority"},
    {"UPDATE IGNORE t SET a = 1", "ignore"},
    {"UPDATE LOW_PRIORITY IGNORE t SET a = 1", "low priority ignore"},
    {"UPDATE t SET a = 1 ORDER BY b LIMIT 10", "order by limit"},
    {"UPDATE t SET a = 1 LIMIT 100", "limit only"},
    {"UPDATE t1 JOIN t2 ON t1.id = t2.fk SET t1.a = t2.b", "join update"},
    {"UPDATE t1, t2 SET t1.a = t2.b WHERE t1.id = t2.fk", "comma join update"},
    {"UPDATE t1 LEFT JOIN t2 ON t1.id = t2.fk SET t1.a = 0 WHERE t2.id IS NULL", "left join"},
    {"UPDATE t SET a = NOW()", "function in value"},
    {"UPDATE t SET a = NULL WHERE b = 1", "set null"},
    {"UPDATE t SET a = CASE WHEN b > 0 THEN 1 ELSE 0 END", "case expression"},
};

TEST(MySQLUpdateBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_update_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::UPDATE)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

static const UpdateTestCase pgsql_update_bulk_cases[] = {
    {"UPDATE t SET a = 1", "simple no where"},
    {"UPDATE t SET a = 1 WHERE b = 2", "simple with where"},
    {"UPDATE t SET a = 1, b = 2 WHERE c = 3", "multi column"},
    {"UPDATE t AS x SET a = 1 WHERE x.b = 2", "alias"},
    {"UPDATE t SET a = 1 FROM t2 WHERE t.id = t2.fk", "from clause"},
    {"UPDATE t SET a = t2.b FROM t2, t3 WHERE t.id = t2.fk AND t2.id = t3.fk", "from multi"},
    {"UPDATE t SET a = 1 WHERE b = 2 RETURNING *", "returning star"},
    {"UPDATE t SET a = 1 WHERE b = 2 RETURNING a, b", "returning cols"},
    {"UPDATE t SET a = 1 FROM t2 WHERE t.id = t2.fk RETURNING t.a", "from + returning"},
};

TEST(PgSQLUpdateBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_update_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::UPDATE)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== Round-trip tests ==========

static const UpdateTestCase mysql_update_roundtrip_cases[] = {
    {"UPDATE t SET a = 1 WHERE b = 2", "simple"},
    {"UPDATE t SET a = 1, b = 'x' WHERE c = 3", "multi col"},
    {"UPDATE LOW_PRIORITY IGNORE t SET a = 1", "options"},
    {"UPDATE t SET a = 1 ORDER BY b DESC LIMIT 10", "order by limit"},
};

TEST(MySQLUpdateRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_update_roundtrip_cases) {
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

static const UpdateTestCase pgsql_update_roundtrip_cases[] = {
    {"UPDATE t SET a = 1 WHERE b = 2", "simple"},
    {"UPDATE t SET a = 1 FROM t2 WHERE t.id = t2.fk", "from clause"},
    {"UPDATE t SET a = 1 WHERE b = 2 RETURNING *", "returning"},
};

TEST(PgSQLUpdateRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_update_roundtrip_cases) {
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

- [ ] **Step 2: Add test_update.cpp to Makefile.new**

Add `$(TEST_DIR)/test_update.cpp \` to the `TEST_SRCS` list.

- [ ] **Step 3: Implement UpdateParser class**

Create `include/sql_parser/update_parser.h`:
```cpp
#ifndef SQL_PARSER_UPDATE_PARSER_H
#define SQL_PARSER_UPDATE_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/table_ref_parser.h"

namespace sql_parser {

template <Dialect D>
class UpdateParser {
public:
    UpdateParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena),
          table_ref_parser_(tokenizer, arena, expr_parser_) {}

    // Parse UPDATE statement (UPDATE keyword already consumed).
    AstNode* parse();

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    TableRefParser<D> table_ref_parser_;

    // Parse MySQL options: LOW_PRIORITY, IGNORE
    AstNode* parse_stmt_options();

    // Parse SET clause: SET col=expr [, col=expr ...]
    AstNode* parse_update_set_clause();

    // Parse a single col=expr pair
    AstNode* parse_set_item();

    // Parse WHERE clause
    AstNode* parse_where_clause();

    // Parse ORDER BY clause (MySQL single-table only)
    AstNode* parse_order_by();

    // Parse LIMIT clause (MySQL single-table only)
    AstNode* parse_limit();

    // Parse PostgreSQL FROM clause (after SET, before WHERE)
    AstNode* parse_from_clause();

    // Parse PostgreSQL RETURNING clause
    AstNode* parse_returning();
};

} // namespace sql_parser

#endif // SQL_PARSER_UPDATE_PARSER_H
```

- [ ] **Step 4: Implement UpdateParser parse methods**

Implement all methods in the header. Key logic for `parse()`:

**MySQL flow:**
1. Parse options (LOW_PRIORITY, IGNORE)
2. Parse table references using `table_ref_parser_` (supports JOINs for multi-table)
3. Expect and consume `SET` keyword
4. Parse SET assignments
5. Parse optional WHERE
6. Parse optional ORDER BY (single-table only)
7. Parse optional LIMIT (single-table only)

**PostgreSQL flow:**
1. Parse optional ONLY keyword
2. Parse single table reference with optional alias
3. Expect and consume `SET` keyword
4. Parse SET assignments
5. Parse optional FROM clause (additional table sources)
6. Parse optional WHERE
7. Parse optional RETURNING

Use `if constexpr (D == Dialect::MySQL)` and `if constexpr (D == Dialect::PostgreSQL)` for dialect-specific branches.

Refer to `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md` for full AST structure details.

---

### Task 3: Emitter Support for UPDATE Nodes

**Files:**
- Modify: `include/sql_parser/emitter.h`

- [ ] **Step 1: Add emit methods for UPDATE node types**

Add cases to the `emit_node()` switch:
```cpp
case NodeType::NODE_UPDATE_STMT:       emit_update_stmt(node); break;
case NodeType::NODE_UPDATE_SET_CLAUSE: emit_update_set_clause(node); break;
```

Note: `NODE_UPDATE_SET_ITEM`, `NODE_STMT_OPTIONS`, `NODE_RETURNING_CLAUSE` emitters were added in Plan 7.

- [ ] **Step 2: Implement emit_update_stmt and emit_update_set_clause**

```cpp
void emit_update_stmt(const AstNode* node);
void emit_update_set_clause(const AstNode* node);
```

`emit_update_stmt()` must handle the different child ordering between MySQL (table refs before SET) and PostgreSQL (single table, SET, then optional FROM). The emitter walks the children and emits them in the correct order based on their node types.

---

### Task 4: Classifier Integration

**Files:**
- Modify: `include/sql_parser/parser.h`
- Modify: `src/sql_parser/parser.cpp`

- [ ] **Step 1: Add `parse_update()` method declaration**

Add to the private section of `Parser<D>`:
```cpp
ParseResult parse_update();
```

- [ ] **Step 2: Implement `parse_update()` in `parser.cpp`**

Follow the same pattern as `parse_insert()`:
```cpp
template <Dialect D>
ParseResult Parser<D>::parse_update() {
    ParseResult r;
    r.stmt_type = StmtType::UPDATE;

    UpdateParser<D> update_parser(tokenizer_, arena_);
    AstNode* ast = update_parser.parse();

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
case TokenType::TK_UPDATE:   return extract_update(first);
```
With:
```cpp
case TokenType::TK_UPDATE:   return parse_update();
```

- [ ] **Step 4: Add `#include "sql_parser/update_parser.h"` to `parser.cpp`**

- [ ] **Step 5: Run all tests**

```bash
make -f Makefile.new test
```

All existing tests plus new UPDATE tests should pass.
