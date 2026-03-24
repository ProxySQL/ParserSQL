# DELETE Deep Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a full DELETE deep parser for MySQL and PostgreSQL that handles single-table delete, MySQL multi-table (both forms), PostgreSQL USING, ORDER BY/LIMIT (MySQL), and RETURNING (PostgreSQL).

**Architecture:** `DeleteParser<D>` is a header-only template class following the established pattern. It uses `TableRefParser<D>` (from Plan 7) for table reference parsing and `ExpressionParser<D>` for expressions. MySQL multi-table DELETE has two forms that require disambiguation: form 1 (`DELETE t1, t2 FROM ...`) and form 2 (`DELETE FROM t1, t2 USING ...`). The parser disambiguates by checking whether `FROM` appears immediately after options/targets or whether a table list precedes it.

**Tech Stack:** C++17, existing parser infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md`

---

## Scope

This plan builds:
1. `DeleteParser<D>` — full DELETE deep parser for both dialects
2. Emitter extensions for DELETE-specific node types
3. Classifier update to route TK_DELETE to the deep parser
4. Comprehensive tests including round-trip tests

**Closes:** #7

**Dependencies:** Plan 7 (provides `TableRefParser<D>`, `NODE_STMT_OPTIONS`, `NODE_RETURNING_CLAUSE`, and related infrastructure). Plan 8 is independent of this plan.

---

## File Structure

```
include/sql_parser/
    delete_parser.h       — (create) DELETE statement parser (header-only template)
    emitter.h             — (modify) add DELETE emit methods
    common.h              — (modify) add NODE_DELETE_STMT, NODE_DELETE_USING_CLAUSE

src/sql_parser/
    parser.cpp            — (modify) replace extract_delete with parse_delete()

include/sql_parser/
    parser.h              — (modify) add parse_delete() declaration

tests/
    test_delete.cpp       — (create) DELETE parser tests

Makefile.new              — (modify) add test_delete.cpp to TEST_SRCS
```

---

### Task 1: Add DELETE Node Types

**Files:**
- Modify: `include/sql_parser/common.h`

- [ ] **Step 1: Add new node types**

Add to `NodeType` enum (if not already present from Plan 7/8):
```cpp
// DELETE nodes
NODE_DELETE_STMT,
NODE_DELETE_USING_CLAUSE,  // PostgreSQL USING or MySQL USING form
```

Note: `NODE_STMT_OPTIONS`, `NODE_RETURNING_CLAUSE`, `NODE_FROM_CLAUSE`, `NODE_WHERE_CLAUSE`, `NODE_ORDER_BY_CLAUSE`, `NODE_LIMIT_CLAUSE` already exist.

---

### Task 2: DELETE Parser Implementation

**Files:**
- Create: `include/sql_parser/delete_parser.h`
- Create: `tests/test_delete.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Write tests for DELETE parsing**

Create `tests/test_delete.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLDeleteTest : public ::testing::Test {
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

// ========== Basic DELETE ==========

TEST_F(MySQLDeleteTest, SimpleDelete) {
    auto r = parser.parse("DELETE FROM users WHERE id = 1", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_DELETE_STMT);
}

TEST_F(MySQLDeleteTest, DeleteNoWhere) {
    auto r = parser.parse("DELETE FROM users", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* where = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    EXPECT_EQ(where, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteQualifiedTable) {
    auto r = parser.parse("DELETE FROM mydb.users WHERE id = 1", 35);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteComplexWhere) {
    const char* sql = "DELETE FROM users WHERE status = 'inactive' AND last_login < '2020-01-01'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL Options ==========

TEST_F(MySQLDeleteTest, DeleteLowPriority) {
    auto r = parser.parse("DELETE LOW_PRIORITY FROM users WHERE id = 1", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* opts = find_child(r.ast, NodeType::NODE_STMT_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteQuick) {
    auto r = parser.parse("DELETE QUICK FROM users WHERE id = 1", 36);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteIgnore) {
    auto r = parser.parse("DELETE IGNORE FROM users WHERE id = 1", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteAllOptions) {
    auto r = parser.parse("DELETE LOW_PRIORITY QUICK IGNORE FROM users WHERE id = 1", 56);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL ORDER BY + LIMIT ==========

TEST_F(MySQLDeleteTest, DeleteOrderByLimit) {
    const char* sql = "DELETE FROM users WHERE active = 0 ORDER BY created_at ASC LIMIT 100";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLDeleteTest, DeleteLimitOnly) {
    auto r = parser.parse("DELETE FROM users WHERE active = 0 LIMIT 100", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

// ========== MySQL Multi-Table Form 1: DELETE t1, t2 FROM ... ==========

TEST_F(MySQLDeleteTest, MultiTableForm1Single) {
    const char* sql = "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.status = 0";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, MultiTableForm1Multiple) {
    const char* sql = "DELETE t1, t2 FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t1.status = 0";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL Multi-Table Form 2: DELETE FROM t1, t2 USING ... ==========

TEST_F(MySQLDeleteTest, MultiTableForm2) {
    const char* sql = "DELETE FROM t1, t2 USING t1 JOIN t2 ON t1.id = t2.fk WHERE t1.status = 0";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, MultiTableForm2Single) {
    const char* sql = "DELETE FROM t1 USING t1 JOIN t2 ON t1.id = t2.fk WHERE t2.bad = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== PostgreSQL DELETE ==========

class PgSQLDeleteTest : public ::testing::Test {
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

TEST_F(PgSQLDeleteTest, SimpleDelete) {
    auto r = parser.parse("DELETE FROM users WHERE id = 1", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteUsing) {
    const char* sql = "DELETE FROM users USING orders WHERE users.id = orders.user_id AND orders.bad = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* using_clause = find_child(r.ast, NodeType::NODE_DELETE_USING_CLAUSE);
    ASSERT_NE(using_clause, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteUsingMultiple) {
    const char* sql = "DELETE FROM t1 USING t2, t3 WHERE t1.id = t2.fk AND t2.id = t3.fk";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteReturning) {
    const char* sql = "DELETE FROM users WHERE id = 1 RETURNING *";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ret = find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE);
    ASSERT_NE(ret, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteReturningColumns) {
    const char* sql = "DELETE FROM users WHERE id = 1 RETURNING id, name";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ret = find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE);
    ASSERT_NE(ret, nullptr);
    EXPECT_EQ(child_count(ret), 2);
}

TEST_F(PgSQLDeleteTest, DeleteUsingReturning) {
    const char* sql = "DELETE FROM users USING orders "
                      "WHERE users.id = orders.user_id RETURNING users.id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_DELETE_USING_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE), nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteWithAlias) {
    const char* sql = "DELETE FROM users AS u WHERE u.id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Bulk data-driven tests ==========

struct DeleteTestCase {
    const char* sql;
    const char* description;
};

static const DeleteTestCase mysql_delete_bulk_cases[] = {
    {"DELETE FROM t", "simple no where"},
    {"DELETE FROM t WHERE a = 1", "simple with where"},
    {"DELETE FROM t WHERE a > 1 AND b < 10", "complex where"},
    {"DELETE FROM db.t WHERE a = 1", "qualified table"},
    {"DELETE LOW_PRIORITY FROM t WHERE a = 1", "low priority"},
    {"DELETE QUICK FROM t WHERE a = 1", "quick"},
    {"DELETE IGNORE FROM t WHERE a = 1", "ignore"},
    {"DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE a = 1", "all options"},
    {"DELETE FROM t WHERE a = 1 ORDER BY b LIMIT 10", "order by limit"},
    {"DELETE FROM t WHERE a = 1 LIMIT 100", "limit only"},
    {"DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.x = 0", "multi-table form 1"},
    {"DELETE t1, t2 FROM t1 JOIN t2 ON t1.id = t2.fk", "multi-table form 1 multi target"},
    {"DELETE FROM t1 USING t1 JOIN t2 ON t1.id = t2.fk WHERE t2.x = 0", "multi-table form 2"},
    {"DELETE FROM t1, t2 USING t1 JOIN t2 ON t1.id = t2.fk", "multi-table form 2 multi target"},
};

TEST(MySQLDeleteBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_delete_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

static const DeleteTestCase pgsql_delete_bulk_cases[] = {
    {"DELETE FROM t", "simple no where"},
    {"DELETE FROM t WHERE a = 1", "simple with where"},
    {"DELETE FROM t WHERE a > 1 AND b < 10", "complex where"},
    {"DELETE FROM t AS x WHERE x.a = 1", "alias"},
    {"DELETE FROM t USING t2 WHERE t.id = t2.fk", "using single"},
    {"DELETE FROM t USING t2, t3 WHERE t.id = t2.fk AND t2.id = t3.fk", "using multi"},
    {"DELETE FROM t WHERE a = 1 RETURNING *", "returning star"},
    {"DELETE FROM t WHERE a = 1 RETURNING a, b", "returning cols"},
    {"DELETE FROM t USING t2 WHERE t.id = t2.fk RETURNING t.a", "using + returning"},
};

TEST(PgSQLDeleteBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_delete_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== Round-trip tests ==========

static const DeleteTestCase mysql_delete_roundtrip_cases[] = {
    {"DELETE FROM t WHERE a = 1", "simple"},
    {"DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE a = 1", "all options"},
    {"DELETE FROM t WHERE a = 1 ORDER BY b LIMIT 10", "order by limit"},
};

TEST(MySQLDeleteRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_delete_roundtrip_cases) {
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

static const DeleteTestCase pgsql_delete_roundtrip_cases[] = {
    {"DELETE FROM t WHERE a = 1", "simple"},
    {"DELETE FROM t USING t2 WHERE t.id = t2.fk", "using"},
    {"DELETE FROM t WHERE a = 1 RETURNING *", "returning"},
};

TEST(PgSQLDeleteRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_delete_roundtrip_cases) {
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

- [ ] **Step 2: Add test_delete.cpp to Makefile.new**

Add `$(TEST_DIR)/test_delete.cpp \` to the `TEST_SRCS` list.

- [ ] **Step 3: Implement DeleteParser class**

Create `include/sql_parser/delete_parser.h`:
```cpp
#ifndef SQL_PARSER_DELETE_PARSER_H
#define SQL_PARSER_DELETE_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/table_ref_parser.h"

namespace sql_parser {

// Flags on NODE_DELETE_STMT
static constexpr uint16_t FLAG_DELETE_MULTI_TABLE = 0x01;   // multi-table form
static constexpr uint16_t FLAG_DELETE_FORM2 = 0x02;         // MySQL form 2 (DELETE FROM ... USING)

template <Dialect D>
class DeleteParser {
public:
    DeleteParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena),
          table_ref_parser_(tokenizer, arena, expr_parser_) {}

    // Parse DELETE statement (DELETE keyword already consumed).
    AstNode* parse();

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    TableRefParser<D> table_ref_parser_;

    // Parse MySQL options: LOW_PRIORITY, QUICK, IGNORE
    AstNode* parse_stmt_options();

    // Parse target table list for multi-table: t1 [, t2, ...]
    AstNode* parse_target_tables();

    // Parse WHERE clause
    AstNode* parse_where_clause();

    // Parse ORDER BY (MySQL single-table)
    AstNode* parse_order_by();

    // Parse LIMIT (MySQL single-table)
    AstNode* parse_limit();

    // Parse PostgreSQL USING clause
    AstNode* parse_using_clause();

    // Parse PostgreSQL RETURNING clause
    AstNode* parse_returning();
};

} // namespace sql_parser

#endif // SQL_PARSER_DELETE_PARSER_H
```

- [ ] **Step 4: Implement DeleteParser parse methods**

The key complexity is MySQL multi-table disambiguation in `parse()`:

**MySQL flow:**
1. Parse options (LOW_PRIORITY, QUICK, IGNORE)
2. Check if next token is `FROM`:
   - **Yes**: could be single-table OR form 2. Consume FROM, parse table list.
     - If next token is `USING`: this is **form 2**. Consume USING, parse table references (source tables with JOINs).
     - Otherwise: single-table delete. Parse optional WHERE, ORDER BY, LIMIT.
   - **No**: this is **form 1**. Parse target table list (t1, t2, ...), then expect FROM, parse table references (source tables with JOINs), then WHERE.

**PostgreSQL flow:**
1. Consume FROM
2. Parse optional ONLY keyword
3. Parse single table reference with optional alias
4. Parse optional USING clause
5. Parse optional WHERE
6. Parse optional RETURNING

Refer to `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md` for full syntax and AST structure.

---

### Task 3: Emitter Support for DELETE Nodes

**Files:**
- Modify: `include/sql_parser/emitter.h`

- [ ] **Step 1: Add emit methods for DELETE node types**

Add cases to the `emit_node()` switch:
```cpp
case NodeType::NODE_DELETE_STMT:          emit_delete_stmt(node); break;
case NodeType::NODE_DELETE_USING_CLAUSE:  emit_delete_using(node); break;
```

- [ ] **Step 2: Implement emit_delete_stmt and emit_delete_using**

```cpp
void emit_delete_stmt(const AstNode* node);
void emit_delete_using(const AstNode* node);
```

`emit_delete_stmt()` must check `flags` to determine the delete form:
- Single-table: `DELETE [options] FROM table [WHERE] [ORDER BY] [LIMIT]`
- Multi-table form 1: `DELETE [options] targets FROM table_refs [WHERE]`
- Multi-table form 2: `DELETE [options] FROM targets USING table_refs [WHERE]`
- PostgreSQL: `DELETE FROM table [USING] [WHERE] [RETURNING]`

---

### Task 4: Classifier Integration

**Files:**
- Modify: `include/sql_parser/parser.h`
- Modify: `src/sql_parser/parser.cpp`

- [ ] **Step 1: Add `parse_delete()` declaration**

Add to the private section of `Parser<D>`:
```cpp
ParseResult parse_delete();
```

- [ ] **Step 2: Implement `parse_delete()` in `parser.cpp`**

```cpp
template <Dialect D>
ParseResult Parser<D>::parse_delete() {
    ParseResult r;
    r.stmt_type = StmtType::DELETE_STMT;

    DeleteParser<D> delete_parser(tokenizer_, arena_);
    AstNode* ast = delete_parser.parse();

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
case TokenType::TK_DELETE:   return extract_delete(first);
```
With:
```cpp
case TokenType::TK_DELETE:   return parse_delete();
```

- [ ] **Step 4: Add `#include "sql_parser/delete_parser.h"` to `parser.cpp`**

- [ ] **Step 5: Run all tests**

```bash
make -f Makefile.new test
```

All existing tests plus new DELETE tests should pass.
