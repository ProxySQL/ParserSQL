# Compound Query (UNION/INTERSECT/EXCEPT) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `CompoundQueryParser<D>` that handles UNION [ALL], INTERSECT [ALL], and EXCEPT [ALL] with correct precedence (INTERSECT binds tighter than UNION/EXCEPT), parenthesized nesting, and trailing ORDER BY/LIMIT on compound results.

**Architecture:** `CompoundQueryParser<D>` is a separate layer above `SelectParser<D>`. It uses Pratt-style precedence parsing: INTERSECT has higher precedence than UNION/EXCEPT (which are equal). Each operand is either a parenthesized compound (recursive) or a single SELECT via `SelectParser<D>`. If no set operator follows the first SELECT, the compound parser returns the bare `NODE_SELECT_STMT` as-is with zero overhead. The parser is integrated by updating `Parser<D>::parse_select()` to call `CompoundQueryParser` instead of `SelectParser` directly.

**Tech Stack:** C++17, existing parser infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md`

---

## Scope

This plan builds:
1. New tokens: `TK_INTERSECT`, `TK_EXCEPT`
2. New node types: `NODE_COMPOUND_QUERY`, `NODE_SET_OPERATION`
3. `CompoundQueryParser<D>` — header-only template for compound query parsing
4. Emitter extensions for compound query nodes
5. Integration into `parse_select()` flow
6. Comprehensive tests with precedence verification

**Closes:** #8

**Dependencies:** None strictly required, but Plan 7's `is_alias_start()` update (adding TK_INTERSECT, TK_EXCEPT to blocklist) is helpful. If Plan 7 is not done first, this plan must do that update itself.

---

## File Structure

```
include/sql_parser/
    compound_query_parser.h — (create) UNION/INTERSECT/EXCEPT parser
    emitter.h               — (modify) add compound query emit methods
    common.h                — (modify) add NODE_COMPOUND_QUERY, NODE_SET_OPERATION
    token.h                 — (modify) add TK_INTERSECT, TK_EXCEPT
    select_parser.h         — (modify) update is_alias_start blocklist (if not done by Plan 7)

src/sql_parser/
    parser.cpp              — (modify) update parse_select() to use CompoundQueryParser

include/sql_parser/
    keywords_mysql.h        — (modify) add INTERSECT, EXCEPT keywords
    keywords_pgsql.h        — (modify) add INTERSECT, EXCEPT keywords

tests/
    test_compound.cpp       — (create) compound query tests

Makefile.new                — (modify) add test_compound.cpp to TEST_SRCS
```

---

### Task 1: Add New Tokens and Node Types

**Files:**
- Modify: `include/sql_parser/token.h`
- Modify: `include/sql_parser/common.h`
- Modify: `include/sql_parser/keywords_mysql.h`
- Modify: `include/sql_parser/keywords_pgsql.h`

- [ ] **Step 1: Add new token types**

Add to `TokenType` enum:
```cpp
TK_INTERSECT,
TK_EXCEPT,
```

Note: `TK_UNION` already exists.

- [ ] **Step 2: Add new node types**

Add to `NodeType` enum:
```cpp
// Compound query nodes
NODE_COMPOUND_QUERY,       // root for UNION/INTERSECT/EXCEPT
NODE_SET_OPERATION,        // operator (UNION, INTERSECT, EXCEPT) with ALL flag
```

- [ ] **Step 3: Register keywords**

Add `INTERSECT` and `EXCEPT` to both `keywords_mysql.h` and `keywords_pgsql.h`.

- [ ] **Step 4: Update `is_alias_start()` blocklist**

In `TableRefParser<D>::is_alias_start()` (or `SelectParser<D>` if Plan 7 is not yet applied), add:
```cpp
case TokenType::TK_INTERSECT:
case TokenType::TK_EXCEPT:
```

These keywords start compound operators and must not be misinterpreted as implicit aliases.

---

### Task 2: CompoundQueryParser Implementation

**Files:**
- Create: `include/sql_parser/compound_query_parser.h`
- Create: `tests/test_compound.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Write tests for compound queries**

Create `tests/test_compound.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLCompoundTest : public ::testing::Test {
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

// ========== Simple SELECT (no compound) ==========
// CompoundQueryParser must return bare NODE_SELECT_STMT when no set operator follows

TEST_F(MySQLCompoundTest, PlainSelectUnchanged) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    // Should be NODE_SELECT_STMT, NOT NODE_COMPOUND_QUERY
    EXPECT_EQ(r.ast->type, NodeType::NODE_SELECT_STMT);
}

// ========== UNION ==========

TEST_F(MySQLCompoundTest, SimpleUnion) {
    const char* sql = "SELECT 1 UNION SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
    auto* setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(setop, nullptr);
}

TEST_F(MySQLCompoundTest, UnionAll) {
    const char* sql = "SELECT 1 UNION ALL SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, UnionThreeSelects) {
    const char* sql = "SELECT 1 UNION SELECT 2 UNION SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, UnionWithOrderBy) {
    const char* sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
}

TEST_F(MySQLCompoundTest, UnionWithLimit) {
    const char* sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2 LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLCompoundTest, UnionWithOrderByAndLimit) {
    const char* sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

// ========== INTERSECT ==========

TEST_F(MySQLCompoundTest, SimpleIntersect) {
    const char* sql = "SELECT 1 INTERSECT SELECT 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, IntersectAll) {
    const char* sql = "SELECT 1 INTERSECT ALL SELECT 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== EXCEPT ==========

TEST_F(MySQLCompoundTest, SimpleExcept) {
    const char* sql = "SELECT 1 EXCEPT SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, ExceptAll) {
    const char* sql = "SELECT 1 EXCEPT ALL SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Precedence: INTERSECT > UNION/EXCEPT ==========

TEST_F(MySQLCompoundTest, IntersectBindsTighterThanUnion) {
    // SELECT 1 UNION SELECT 2 INTERSECT SELECT 3
    // Should parse as: SELECT 1 UNION (SELECT 2 INTERSECT SELECT 3)
    const char* sql = "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);

    // The top-level set operation should be UNION
    auto* top_setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(top_setop, nullptr);
    // The value should contain "UNION"
    StringRef op_text = top_setop->value();
    EXPECT_TRUE(op_text.equals_ci("UNION", 5));

    // The right child of UNION should be a SET_OPERATION (INTERSECT)
    const AstNode* left = top_setop->first_child;
    ASSERT_NE(left, nullptr);
    const AstNode* right = left->next_sibling;
    ASSERT_NE(right, nullptr);
    EXPECT_EQ(right->type, NodeType::NODE_SET_OPERATION);
    StringRef right_op = right->value();
    EXPECT_TRUE(right_op.equals_ci("INTERSECT", 9));
}

TEST_F(MySQLCompoundTest, IntersectBindsTighterThanExcept) {
    // SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3
    // Should parse as: SELECT 1 EXCEPT (SELECT 2 INTERSECT SELECT 3)
    const char* sql = "SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* top_setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(top_setop, nullptr);
    StringRef op_text = top_setop->value();
    EXPECT_TRUE(op_text.equals_ci("EXCEPT", 6));
}

// ========== Parenthesized nesting ==========

TEST_F(MySQLCompoundTest, ParenthesizedUnion) {
    const char* sql = "(SELECT 1) UNION (SELECT 2)";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, ParenthesizedOverridesPrecedence) {
    // (SELECT 1 UNION SELECT 2) INTERSECT SELECT 3
    // Parentheses force UNION to be evaluated first
    const char* sql = "(SELECT 1 UNION SELECT 2) INTERSECT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);

    auto* top_setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(top_setop, nullptr);
    StringRef op_text = top_setop->value();
    EXPECT_TRUE(op_text.equals_ci("INTERSECT", 9));
}

// ========== Complex compound queries ==========

TEST_F(MySQLCompoundTest, UnionWithFullSelects) {
    const char* sql = "SELECT a, b FROM t1 WHERE x = 1 UNION ALL SELECT a, b FROM t2 WHERE y = 2 ORDER BY a LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

// ========== PostgreSQL compound queries ==========

class PgSQLCompoundTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

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

TEST_F(PgSQLCompoundTest, SimpleUnion) {
    const char* sql = "SELECT 1 UNION SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(PgSQLCompoundTest, IntersectExcept) {
    const char* sql = "SELECT 1 INTERSECT SELECT 2 EXCEPT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLCompoundTest, UnionReturnsCorrectDialect) {
    const char* sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Bulk data-driven tests ==========

struct CompoundTestCase {
    const char* sql;
    const char* description;
};

static const CompoundTestCase compound_bulk_cases[] = {
    {"SELECT 1 UNION SELECT 2", "simple union"},
    {"SELECT 1 UNION ALL SELECT 2", "union all"},
    {"SELECT 1 UNION SELECT 2 UNION SELECT 3", "triple union"},
    {"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3", "triple union all"},
    {"SELECT 1 INTERSECT SELECT 2", "simple intersect"},
    {"SELECT 1 INTERSECT ALL SELECT 2", "intersect all"},
    {"SELECT 1 EXCEPT SELECT 2", "simple except"},
    {"SELECT 1 EXCEPT ALL SELECT 2", "except all"},
    {"SELECT 1 UNION SELECT 2 INTERSECT SELECT 3", "union + intersect precedence"},
    {"SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3", "except + intersect precedence"},
    {"(SELECT 1) UNION (SELECT 2)", "parenthesized"},
    {"(SELECT 1 UNION SELECT 2) INTERSECT SELECT 3", "paren override"},
    {"SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a", "trailing order by"},
    {"SELECT a FROM t1 UNION ALL SELECT a FROM t2 LIMIT 10", "trailing limit"},
    {"SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5", "trailing order by + limit"},
    {"SELECT * FROM t1 WHERE x = 1 UNION SELECT * FROM t2 WHERE y = 2", "union with where"},
};

TEST(MySQLCompoundBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : compound_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

TEST(PgSQLCompoundBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : compound_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== Round-trip tests ==========

static const CompoundTestCase compound_roundtrip_cases[] = {
    {"SELECT 1 UNION SELECT 2", "simple union"},
    {"SELECT 1 UNION ALL SELECT 2", "union all"},
    {"SELECT 1 INTERSECT SELECT 2", "intersect"},
    {"SELECT 1 EXCEPT SELECT 2", "except"},
    {"SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a", "with order by"},
    {"SELECT a FROM t1 UNION ALL SELECT a FROM t2 LIMIT 10", "with limit"},
};

TEST(MySQLCompoundRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : compound_roundtrip_cases) {
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
```

- [ ] **Step 2: Add test_compound.cpp to Makefile.new**

Add `$(TEST_DIR)/test_compound.cpp \` to the `TEST_SRCS` list.

- [ ] **Step 3: Implement CompoundQueryParser class**

Create `include/sql_parser/compound_query_parser.h`:
```cpp
#ifndef SQL_PARSER_COMPOUND_QUERY_PARSER_H
#define SQL_PARSER_COMPOUND_QUERY_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/select_parser.h"
#include "sql_parser/expression_parser.h"

namespace sql_parser {

// Flag on NODE_SET_OPERATION to indicate ALL
static constexpr uint16_t FLAG_SET_OP_ALL = 0x01;

template <Dialect D>
class CompoundQueryParser {
public:
    CompoundQueryParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena) {}

    // Parse a compound query (or a plain SELECT if no set operator follows).
    // Returns NODE_SELECT_STMT for plain selects, NODE_COMPOUND_QUERY for compounds.
    AstNode* parse();

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;

    // Precedence levels
    static constexpr int PREC_UNION_EXCEPT = 1;
    static constexpr int PREC_INTERSECT = 2;

    // Parse a compound expression with minimum precedence (Pratt-style)
    AstNode* parse_compound_expr(int min_prec);

    // Parse a single operand: parenthesized compound or plain SELECT
    AstNode* parse_operand();

    // Get the precedence of a set operator token, or 0 if not a set operator
    static int get_set_op_precedence(TokenType type);

    // Check if a token is a set operator
    static bool is_set_operator(TokenType type);

    // Parse trailing ORDER BY for compound result
    AstNode* parse_order_by();

    // Parse trailing LIMIT for compound result
    AstNode* parse_limit();
};

} // namespace sql_parser

#endif // SQL_PARSER_COMPOUND_QUERY_PARSER_H
```

- [ ] **Step 4: Implement CompoundQueryParser methods**

Key implementation logic for `parse()`:

```
parse():
  1. result = parse_compound_expr(0)
  2. if result is NODE_SET_OPERATION:
       wrap in NODE_COMPOUND_QUERY
       parse trailing ORDER BY / LIMIT as children of COMPOUND_QUERY
  3. if result is NODE_SELECT_STMT:
       return as-is (no compound wrapper for plain selects)

parse_compound_expr(min_prec):
  1. left = parse_operand()
  2. while (peek is set operator AND get_set_op_precedence(peek) > min_prec):
       op_token = consume set operator
       consume optional ALL
       right = parse_compound_expr(get_set_op_precedence(op_token))
       left = make NODE_SET_OPERATION with left, right as children
  3. return left

parse_operand():
  1. if peek is '(':
       consume '('
       if next is SELECT: inner = parse_compound_expr(0)
       consume ')'
       return inner
  2. else:
       return SelectParser<D>(tok_, arena_).parse()

get_set_op_precedence(type):
  TK_UNION, TK_EXCEPT => PREC_UNION_EXCEPT
  TK_INTERSECT => PREC_INTERSECT
  otherwise => 0
```

The NODE_SET_OPERATION node's `value` field stores the operator text ("UNION", "UNION ALL", "INTERSECT", etc.). The FLAG_SET_OP_ALL flag distinguishes ALL variants.

---

### Task 3: Emitter Support for Compound Query Nodes

**Files:**
- Modify: `include/sql_parser/emitter.h`

- [ ] **Step 1: Add emit methods for compound nodes**

Add cases to the `emit_node()` switch:
```cpp
case NodeType::NODE_COMPOUND_QUERY:  emit_compound_query(node); break;
case NodeType::NODE_SET_OPERATION:   emit_set_operation(node); break;
```

- [ ] **Step 2: Implement emit methods**

```cpp
void emit_compound_query(const AstNode* node);
void emit_set_operation(const AstNode* node);
```

`emit_compound_query()`: emit the top-level set operation child, then trailing ORDER BY and LIMIT children.

`emit_set_operation()`: emit left child, then operator text (from node value), then right child. The operator text includes the ALL modifier if FLAG_SET_OP_ALL is set.

---

### Task 4: Integration into parse_select()

**Files:**
- Modify: `src/sql_parser/parser.cpp`

- [ ] **Step 1: Update `parse_select()` to use CompoundQueryParser**

Replace the current `parse_select()` implementation:

```cpp
template <Dialect D>
ParseResult Parser<D>::parse_select() {
    ParseResult r;
    r.stmt_type = StmtType::SELECT;

    CompoundQueryParser<D> compound_parser(tokenizer_, arena_);
    AstNode* ast = compound_parser.parse();

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

- [ ] **Step 2: Add `#include "sql_parser/compound_query_parser.h"` to `parser.cpp`**

- [ ] **Step 3: Handle parenthesized SELECT at classifier level**

The classifier currently only dispatches on `TK_SELECT`. A query starting with `(SELECT ...` would not be recognized. Add handling for `TK_LPAREN` in the classifier: peek ahead; if next token is `SELECT` (or another `TK_LPAREN`), dispatch to `parse_select()`. The `CompoundQueryParser` will handle the parenthesized form.

```cpp
case TokenType::TK_LPAREN: {
    // Peek to see if this is a parenthesized SELECT / compound query
    Token next = tokenizer_.peek();
    if (next.type == TokenType::TK_SELECT || next.type == TokenType::TK_LPAREN) {
        // Put the LPAREN back by adjusting state, or handle in CompoundQueryParser
        return parse_select_from_lparen();
    }
    return extract_unknown(first);
}
```

Note: The exact mechanism for "putting back" the `(` depends on how the tokenizer works. The simplest approach is for `parse_select()` to handle the case where the first token was `(` instead of `SELECT` -- pass a flag or have `CompoundQueryParser` check for leading `(`.

- [ ] **Step 4: Run all tests**

```bash
make -f Makefile.new test
```

All existing SELECT tests must still pass (CompoundQueryParser returns bare NODE_SELECT_STMT for non-compound queries). New compound tests should pass.
