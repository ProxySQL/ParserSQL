# Query Emitter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the query emitter that reconstructs valid SQL from an AST, enabling ProxySQL to parse a query, modify the AST, and emit the modified query.

**Architecture:** The emitter is a dialect-templated class `Emitter<D>` that walks an `AstNode` tree and writes SQL into an arena-backed `StringBuilder`. For unmodified nodes, it emits the original input text via `StringRef` (zero-copy). For modified nodes, it emits the new values. The emitter handles all node types produced by the SET, SELECT, and expression parsers. Round-trip tests (parse → emit → parse → compare) validate correctness.

**Tech Stack:** C++17, existing arena/AST infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-sql-parser-design.md` (Query Reconstruction section)

---

## Scope

This plan builds:
1. `StringBuilder` — arena-backed string builder for output
2. `Emitter<D>` — walks AST, emits SQL for all node types
3. `emit()` convenience function on `Parser<D>` — public API
4. Round-trip tests for SET and SELECT statements
5. Modification tests — parse, modify AST, emit, verify output

**Not in scope:** Cross-dialect emission, prepared statement cache.

---

## File Structure

```
include/sql_parser/
    string_builder.h      — Arena-backed string builder
    emitter.h             — Emitter<D> template (header-only)
    parser.h              — (modify) Add emit() method

tests/
    test_emitter.cpp      — Round-trip and modification tests

Makefile.new              — (modify) Add test_emitter.cpp
```

---

### Task 1: StringBuilder and Emitter — Expression Nodes

**Files:**
- Create: `include/sql_parser/string_builder.h`
- Create: `include/sql_parser/emitter.h`
- Create: `tests/test_emitter.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Write tests for expression round-trips**

Create `tests/test_emitter.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLEmitterTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    // Parse, emit, return the emitted string
    std::string round_trip(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        return std::string(result.ptr, result.len);
    }
};

// ========== SET round-trips ==========

TEST_F(MySQLEmitterTest, SetSimpleVariable) {
    std::string out = round_trip("SET autocommit = 1");
    EXPECT_EQ(out, "SET autocommit = 1");
}

TEST_F(MySQLEmitterTest, SetMultipleVariables) {
    std::string out = round_trip("SET autocommit = 1, wait_timeout = 28800");
    EXPECT_EQ(out, "SET autocommit = 1, wait_timeout = 28800");
}

TEST_F(MySQLEmitterTest, SetNames) {
    std::string out = round_trip("SET NAMES utf8mb4");
    EXPECT_EQ(out, "SET NAMES utf8mb4");
}

TEST_F(MySQLEmitterTest, SetNamesCollate) {
    std::string out = round_trip("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci");
    EXPECT_EQ(out, "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci");
}

TEST_F(MySQLEmitterTest, SetCharacterSet) {
    std::string out = round_trip("SET CHARACTER SET utf8");
    EXPECT_EQ(out, "SET CHARACTER SET utf8");
}

TEST_F(MySQLEmitterTest, SetCharset) {
    // CHARSET is normalized to CHARACTER SET in emitted output
    std::string out = round_trip("SET CHARSET utf8");
    EXPECT_EQ(out, "SET CHARACTER SET utf8");
}

TEST_F(MySQLEmitterTest, SetGlobalVariable) {
    std::string out = round_trip("SET GLOBAL max_connections = 100");
    EXPECT_EQ(out, "SET GLOBAL max_connections = 100");
}

TEST_F(MySQLEmitterTest, SetSessionVariable) {
    std::string out = round_trip("SET SESSION wait_timeout = 600");
    EXPECT_EQ(out, "SET SESSION wait_timeout = 600");
}

TEST_F(MySQLEmitterTest, SetDoubleAtVariable) {
    std::string out = round_trip("SET @@session.wait_timeout = 600");
    EXPECT_EQ(out, "SET @@session.wait_timeout = 600");
}

TEST_F(MySQLEmitterTest, SetUserVariable) {
    std::string out = round_trip("SET @my_var = 42");
    EXPECT_EQ(out, "SET @my_var = 42");
}

TEST_F(MySQLEmitterTest, SetTransaction) {
    std::string out = round_trip("SET TRANSACTION READ ONLY");
    EXPECT_EQ(out, "SET TRANSACTION READ ONLY");
}

TEST_F(MySQLEmitterTest, SetTransactionIsolation) {
    // ISOLATION LEVEL keywords are consumed by parser; emitter outputs the level value directly
    std::string out = round_trip("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    EXPECT_EQ(out, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    // Note: To support this, the SET parser must preserve "ISOLATION LEVEL" in the AST.
    // The emitter's emit_set_transaction() must check children and re-insert the keywords.
}

TEST_F(MySQLEmitterTest, SetFunctionRHS) {
    std::string out = round_trip("SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')");
    EXPECT_EQ(out, "SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')");
}

// ========== SELECT round-trips ==========

TEST_F(MySQLEmitterTest, SelectLiteral) {
    std::string out = round_trip("SELECT 1");
    EXPECT_EQ(out, "SELECT 1");
}

TEST_F(MySQLEmitterTest, SelectStar) {
    std::string out = round_trip("SELECT * FROM users");
    EXPECT_EQ(out, "SELECT * FROM users");
}

TEST_F(MySQLEmitterTest, SelectColumns) {
    std::string out = round_trip("SELECT id, name FROM users");
    EXPECT_EQ(out, "SELECT id, name FROM users");
}

TEST_F(MySQLEmitterTest, SelectWithAlias) {
    std::string out = round_trip("SELECT id AS user_id FROM users");
    EXPECT_EQ(out, "SELECT id AS user_id FROM users");
}

TEST_F(MySQLEmitterTest, SelectDistinct) {
    std::string out = round_trip("SELECT DISTINCT name FROM users");
    EXPECT_EQ(out, "SELECT DISTINCT name FROM users");
}

TEST_F(MySQLEmitterTest, SelectWhere) {
    std::string out = round_trip("SELECT * FROM users WHERE id = 1");
    EXPECT_EQ(out, "SELECT * FROM users WHERE id = 1");
}

TEST_F(MySQLEmitterTest, SelectWhereAnd) {
    std::string out = round_trip("SELECT * FROM users WHERE age > 18 AND status = 'active'");
    EXPECT_EQ(out, "SELECT * FROM users WHERE age > 18 AND status = 'active'");
}

TEST_F(MySQLEmitterTest, SelectJoin) {
    std::string out = round_trip("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    EXPECT_EQ(out, "SELECT * FROM users JOIN orders ON users.id = orders.user_id");
}

TEST_F(MySQLEmitterTest, SelectLeftJoin) {
    std::string out = round_trip("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    EXPECT_EQ(out, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
}

TEST_F(MySQLEmitterTest, SelectGroupBy) {
    std::string out = round_trip("SELECT status, COUNT(*) FROM users GROUP BY status");
    EXPECT_EQ(out, "SELECT status, COUNT(*) FROM users GROUP BY status");
}

TEST_F(MySQLEmitterTest, SelectGroupByHaving) {
    std::string out = round_trip("SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5");
    EXPECT_EQ(out, "SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5");
}

TEST_F(MySQLEmitterTest, SelectOrderBy) {
    std::string out = round_trip("SELECT * FROM users ORDER BY name ASC");
    EXPECT_EQ(out, "SELECT * FROM users ORDER BY name ASC");
}

TEST_F(MySQLEmitterTest, SelectLimit) {
    std::string out = round_trip("SELECT * FROM users LIMIT 10");
    EXPECT_EQ(out, "SELECT * FROM users LIMIT 10");
}

TEST_F(MySQLEmitterTest, SelectLimitOffset) {
    std::string out = round_trip("SELECT * FROM users LIMIT 10 OFFSET 20");
    EXPECT_EQ(out, "SELECT * FROM users LIMIT 10 OFFSET 20");
}

TEST_F(MySQLEmitterTest, SelectForUpdate) {
    std::string out = round_trip("SELECT * FROM users FOR UPDATE");
    EXPECT_EQ(out, "SELECT * FROM users FOR UPDATE");
}

// ========== Expression round-trips ==========

TEST_F(MySQLEmitterTest, ExprIsNull) {
    std::string out = round_trip("SELECT * FROM t WHERE x IS NULL");
    EXPECT_EQ(out, "SELECT * FROM t WHERE x IS NULL");
}

TEST_F(MySQLEmitterTest, ExprIsNotNull) {
    std::string out = round_trip("SELECT * FROM t WHERE x IS NOT NULL");
    EXPECT_EQ(out, "SELECT * FROM t WHERE x IS NOT NULL");
}

TEST_F(MySQLEmitterTest, ExprBetween) {
    std::string out = round_trip("SELECT * FROM t WHERE x BETWEEN 1 AND 10");
    EXPECT_EQ(out, "SELECT * FROM t WHERE x BETWEEN 1 AND 10");
}

TEST_F(MySQLEmitterTest, ExprIn) {
    std::string out = round_trip("SELECT * FROM t WHERE x IN (1, 2, 3)");
    EXPECT_EQ(out, "SELECT * FROM t WHERE x IN (1, 2, 3)");
}

TEST_F(MySQLEmitterTest, ExprFunctionCall) {
    std::string out = round_trip("SELECT COUNT(*) FROM users");
    EXPECT_EQ(out, "SELECT COUNT(*) FROM users");
}

TEST_F(MySQLEmitterTest, ExprUnaryMinus) {
    std::string out = round_trip("SELECT -1");
    EXPECT_EQ(out, "SELECT -1");
}

// ========== Bulk round-trip tests ==========

struct RoundTripCase {
    const char* sql;
    const char* description;
};

static const RoundTripCase roundtrip_cases[] = {
    {"SET autocommit = 0", "set simple"},
    {"SET NAMES utf8", "set names"},
    {"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci", "set names collate"},
    {"SET CHARACTER SET utf8", "set character set"},
    {"SET GLOBAL max_connections = 100", "set global"},
    {"SET @x = 42", "set user var"},
    {"SET @@session.wait_timeout = 600", "set sys var"},
    {"SELECT 1", "select literal"},
    {"SELECT * FROM t", "select star"},
    {"SELECT a, b FROM t", "select columns"},
    {"SELECT a AS x FROM t", "select alias"},
    {"SELECT DISTINCT a FROM t", "select distinct"},
    {"SELECT * FROM t WHERE a = 1", "select where"},
    {"SELECT * FROM t WHERE a > 1 AND b < 10", "select where and"},
    {"SELECT * FROM t ORDER BY a", "select order by"},
    {"SELECT * FROM t ORDER BY a DESC", "select order by desc"},
    {"SELECT * FROM t LIMIT 10", "select limit"},
    {"SELECT * FROM t LIMIT 10 OFFSET 5", "select limit offset"},
    {"SELECT * FROM t FOR UPDATE", "select for update"},
    {"SELECT COUNT(*) FROM t", "select count"},
    {"SELECT * FROM t WHERE x IS NULL", "is null"},
    {"SELECT * FROM t WHERE x IS NOT NULL", "is not null"},
    {"SELECT * FROM t WHERE x IN (1, 2, 3)", "in list"},
    {"SELECT * FROM t WHERE x BETWEEN 1 AND 10", "between"},
};

TEST(MySQLEmitterBulk, RoundTripsMatch) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : roundtrip_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        ASSERT_NE(r.ast, nullptr) << "Parse failed: " << tc.description << "\n  SQL: " << tc.sql;
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        std::string out(result.ptr, result.len);
        EXPECT_EQ(out, std::string(tc.sql))
            << "Round-trip mismatch: " << tc.description;
    }
}

// ========== AST modification tests ==========

TEST_F(MySQLEmitterTest, ModifySetValue) {
    // Parse SET autocommit = 1, modify value to 0, emit
    auto r = parser.parse("SET autocommit = 1", 18);
    ASSERT_NE(r.ast, nullptr);

    // Navigate to the value node: SET_STMT -> VAR_ASSIGNMENT -> (target, value)
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    ASSERT_EQ(assignment->type, NodeType::NODE_VAR_ASSIGNMENT);

    // Second child of assignment is the RHS value
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    AstNode* value = target->next_sibling;
    ASSERT_NE(value, nullptr);

    // Modify the value
    const char* new_val = "0";
    value->value_ptr = new_val;
    value->value_len = 1;

    Emitter<Dialect::MySQL> emitter(parser.arena());
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SET autocommit = 0");
}

// ========== PostgreSQL round-trips ==========

TEST(PgSQLEmitterTest, SetVarTo) {
    // PostgreSQL TO is normalized to = in emitted output
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse("SET client_encoding TO 'UTF8'", 29);
    ASSERT_NE(r.ast, nullptr);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SET client_encoding = 'UTF8'");
}

TEST(PgSQLEmitterTest, SelectBasic) {
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse("SELECT * FROM users WHERE id = 1", 32);
    ASSERT_NE(r.ast, nullptr);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SELECT * FROM users WHERE id = 1");
}
```

- [ ] **Step 2: Create string_builder.h**

Create `include/sql_parser/string_builder.h`:
```cpp
#ifndef SQL_PARSER_STRING_BUILDER_H
#define SQL_PARSER_STRING_BUILDER_H

#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include <cstring>

namespace sql_parser {

// Arena-backed string builder for emitting SQL.
// Builds a string by appending chunks. The final result is a contiguous
// StringRef obtained via finish(). All memory is arena-allocated.
class StringBuilder {
public:
    explicit StringBuilder(Arena& arena, size_t initial_capacity = 1024)
        : arena_(arena), capacity_(initial_capacity), len_(0) {
        buf_ = static_cast<char*>(arena_.allocate(capacity_));
    }

    void append(const char* s, size_t n) {
        ensure_capacity(n);
        if (buf_) {
            std::memcpy(buf_ + len_, s, n);
            len_ += n;
        }
    }

    void append(StringRef ref) {
        if (ref.ptr && ref.len > 0) {
            append(ref.ptr, ref.len);
        }
    }

    void append(const char* s) {
        append(s, std::strlen(s));
    }

    void append_char(char c) {
        ensure_capacity(1);
        if (buf_) {
            buf_[len_++] = c;
        }
    }

    // Append a space if the last character isn't already a space
    void space() {
        if (len_ > 0 && buf_[len_ - 1] != ' ') {
            append_char(' ');
        }
    }

    StringRef finish() {
        return StringRef{buf_, static_cast<uint32_t>(len_)};
    }

    size_t length() const { return len_; }

private:
    Arena& arena_;
    char* buf_;
    size_t capacity_;
    size_t len_;

    void ensure_capacity(size_t additional) {
        if (!buf_) return;
        if (len_ + additional <= capacity_) return;

        size_t new_cap = capacity_ * 2;
        while (new_cap < len_ + additional) new_cap *= 2;

        char* new_buf = static_cast<char*>(arena_.allocate(new_cap));
        if (new_buf) {
            std::memcpy(new_buf, buf_, len_);
        }
        buf_ = new_buf;
        capacity_ = new_cap;
        // Old buffer is abandoned in the arena — freed on arena reset
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_STRING_BUILDER_H
```

- [ ] **Step 3: Create emitter.h**

Create `include/sql_parser/emitter.h`:
```cpp
#ifndef SQL_PARSER_EMITTER_H
#define SQL_PARSER_EMITTER_H

#include "sql_parser/common.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/string_builder.h"

namespace sql_parser {

template <Dialect D>
class Emitter {
public:
    explicit Emitter(Arena& arena) : sb_(arena) {}

    void emit(const AstNode* node) {
        if (!node) return;
        emit_node(node);
    }

    StringRef result() { return sb_.finish(); }

private:
    StringBuilder sb_;

    void emit_node(const AstNode* node) {
        switch (node->type) {
            // ---- SET statement ----
            case NodeType::NODE_SET_STMT:     emit_set_stmt(node); break;
            case NodeType::NODE_SET_NAMES:    emit_set_names(node); break;
            case NodeType::NODE_SET_CHARSET:  emit_set_charset(node); break;
            case NodeType::NODE_SET_TRANSACTION: emit_set_transaction(node); break;
            case NodeType::NODE_VAR_ASSIGNMENT: emit_var_assignment(node); break;
            case NodeType::NODE_VAR_TARGET:   emit_var_target(node); break;

            // ---- SELECT statement ----
            case NodeType::NODE_SELECT_STMT:     emit_select_stmt(node); break;
            case NodeType::NODE_SELECT_OPTIONS:  emit_select_options(node); break;
            case NodeType::NODE_SELECT_ITEM_LIST:emit_select_item_list(node); break;
            case NodeType::NODE_SELECT_ITEM:     emit_select_item(node); break;
            case NodeType::NODE_FROM_CLAUSE:     emit_from_clause(node); break;
            case NodeType::NODE_JOIN_CLAUSE:     emit_join_clause(node); break;
            case NodeType::NODE_WHERE_CLAUSE:    emit_where_clause(node); break;
            case NodeType::NODE_GROUP_BY_CLAUSE: emit_group_by(node); break;
            case NodeType::NODE_HAVING_CLAUSE:   emit_having(node); break;
            case NodeType::NODE_ORDER_BY_CLAUSE: emit_order_by(node); break;
            case NodeType::NODE_ORDER_BY_ITEM:   emit_order_by_item(node); break;
            case NodeType::NODE_LIMIT_CLAUSE:    emit_limit(node); break;
            case NodeType::NODE_LOCKING_CLAUSE:  emit_locking(node); break;
            case NodeType::NODE_INTO_CLAUSE:     emit_into(node); break;

            // ---- Table references ----
            case NodeType::NODE_TABLE_REF:       emit_table_ref(node); break;
            case NodeType::NODE_ALIAS:           emit_alias(node); break;
            case NodeType::NODE_QUALIFIED_NAME:  emit_qualified_name(node); break;

            // ---- Expressions ----
            case NodeType::NODE_BINARY_OP:       emit_binary_op(node); break;
            case NodeType::NODE_UNARY_OP:        emit_unary_op(node); break;
            case NodeType::NODE_FUNCTION_CALL:   emit_function_call(node); break;
            case NodeType::NODE_IS_NULL:         emit_is_null(node); break;
            case NodeType::NODE_IS_NOT_NULL:     emit_is_not_null(node); break;
            case NodeType::NODE_BETWEEN:         emit_between(node); break;
            case NodeType::NODE_IN_LIST:         emit_in_list(node); break;
            case NodeType::NODE_CASE_WHEN:       emit_case_when(node); break;
            case NodeType::NODE_SUBQUERY:        emit_value(node); break;

            // ---- Leaf nodes (emit value directly) ----
            case NodeType::NODE_LITERAL_INT:
            case NodeType::NODE_LITERAL_FLOAT:
            case NodeType::NODE_LITERAL_NULL:
            case NodeType::NODE_COLUMN_REF:
            case NodeType::NODE_ASTERISK:
            case NodeType::NODE_PLACEHOLDER:
            case NodeType::NODE_IDENTIFIER:
                emit_value(node); break;

            case NodeType::NODE_LITERAL_STRING:
                emit_string_literal(node); break;

            default:
                emit_value(node); break;
        }
    }

    void emit_value(const AstNode* node) {
        sb_.append(node->value_ptr, node->value_len);
    }

    void emit_string_literal(const AstNode* node) {
        sb_.append_char('\'');
        sb_.append(node->value_ptr, node->value_len);
        sb_.append_char('\'');
    }

    // ---- SET ----

    void emit_set_stmt(const AstNode* node) {
        sb_.append("SET ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_set_names(const AstNode* node) {
        sb_.append("NAMES ");
        const AstNode* charset = node->first_child;
        if (charset) emit_node(charset);
        const AstNode* collation = charset ? charset->next_sibling : nullptr;
        if (collation) {
            sb_.append(" COLLATE ");
            emit_node(collation);
        }
    }

    void emit_set_charset(const AstNode* node) {
        // Detect if original was CHARACTER SET or CHARSET from value
        sb_.append("CHARACTER SET ");
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_set_transaction(const AstNode* node) {
        sb_.append("TRANSACTION ");
        const AstNode* child = node->first_child;
        // First child may be scope (GLOBAL/SESSION)
        if (child && child->value_len > 0) {
            StringRef val = child->value();
            // Check if this is a scope keyword
            if (val.equals_ci("GLOBAL", 6) || val.equals_ci("SESSION", 7) ||
                val.equals_ci("LOCAL", 5)) {
                // This was already emitted before TRANSACTION by the SET stmt emitter
                child = child->next_sibling;
            }
        }
        if (child) {
            StringRef val = child->value();
            // Check if this is an isolation level or access mode
            if (val.equals_ci("READ ONLY", 9) || val.equals_ci("READ WRITE", 10)) {
                emit_node(child);
            } else {
                // It's an isolation level value
                sb_.append("ISOLATION LEVEL ");
                emit_node(child);
            }
        }
    }

    void emit_var_assignment(const AstNode* node) {
        const AstNode* target = node->first_child;
        const AstNode* rhs = target ? target->next_sibling : nullptr;

        if (target) emit_node(target);
        sb_.append(" = ");
        if (rhs) emit_node(rhs);
    }

    void emit_var_target(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    // ---- SELECT ----

    void emit_select_stmt(const AstNode* node) {
        sb_.append("SELECT ");
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
        }
    }

    void emit_select_options(const AstNode* node) {
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
            sb_.append_char(' ');
        }
    }

    void emit_select_item_list(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_select_item(const AstNode* node) {
        const AstNode* expr = node->first_child;
        if (expr) emit_node(expr);
        const AstNode* alias = expr ? expr->next_sibling : nullptr;
        if (alias && alias->type == NodeType::NODE_ALIAS) {
            emit_node(alias);
        }
    }

    void emit_from_clause(const AstNode* node) {
        sb_.append(" FROM ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_JOIN_CLAUSE) {
                sb_.append_char(' ');
                emit_node(child);
            } else {
                if (!first) sb_.append(", ");
                first = false;
                emit_node(child);
            }
        }
    }

    void emit_table_ref(const AstNode* node) {
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
        }
    }

    void emit_alias(const AstNode* node) {
        sb_.append(" AS ");
        emit_value(node);
    }

    void emit_qualified_name(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char('.');
            first = false;
            emit_node(child);
        }
    }

    void emit_join_clause(const AstNode* node) {
        // Join type stored in node value
        emit_value(node);
        sb_.append_char(' ');
        // Children: table_ref, [ON expr | USING (...)]
        const AstNode* table = node->first_child;
        if (table) {
            emit_node(table);
        }
        const AstNode* condition = table ? table->next_sibling : nullptr;
        if (condition) {
            if (condition->type == NodeType::NODE_IDENTIFIER &&
                condition->value_len == 5 &&
                std::memcmp(condition->value_ptr, "USING", 5) == 0) {
                sb_.append(" USING (");
                bool first = true;
                for (const AstNode* col = condition->first_child; col; col = col->next_sibling) {
                    if (!first) sb_.append(", ");
                    first = false;
                    emit_node(col);
                }
                sb_.append_char(')');
            } else {
                sb_.append(" ON ");
                emit_node(condition);
            }
        }
    }

    void emit_where_clause(const AstNode* node) {
        sb_.append(" WHERE ");
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_group_by(const AstNode* node) {
        sb_.append(" GROUP BY ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_having(const AstNode* node) {
        sb_.append(" HAVING ");
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_order_by(const AstNode* node) {
        sb_.append(" ORDER BY ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_order_by_item(const AstNode* node) {
        const AstNode* expr = node->first_child;
        if (expr) emit_node(expr);
        const AstNode* dir = expr ? expr->next_sibling : nullptr;
        if (dir) {
            sb_.append_char(' ');
            emit_node(dir);
        }
    }

    void emit_limit(const AstNode* node) {
        sb_.append(" LIMIT ");
        const AstNode* first_val = node->first_child;
        if (first_val) emit_node(first_val);
        const AstNode* second_val = first_val ? first_val->next_sibling : nullptr;
        if (second_val) {
            sb_.append(" OFFSET ");
            emit_node(second_val);
        }
    }

    void emit_locking(const AstNode* node) {
        sb_.append(" FOR ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    void emit_into(const AstNode* node) {
        sb_.append(" INTO ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    // ---- Expressions ----

    void emit_binary_op(const AstNode* node) {
        const AstNode* left = node->first_child;
        const AstNode* right = left ? left->next_sibling : nullptr;
        if (left) emit_node(left);
        sb_.append_char(' ');
        emit_value(node);
        sb_.append_char(' ');
        if (right) emit_node(right);
    }

    void emit_unary_op(const AstNode* node) {
        emit_value(node);
        // Add space for keyword operators like NOT, no space for - or +
        if (node->value_len > 1) sb_.append_char(' ');
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_function_call(const AstNode* node) {
        emit_value(node);
        sb_.append_char('(');
        bool first = true;
        for (const AstNode* arg = node->first_child; arg; arg = arg->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(arg);
        }
        sb_.append_char(')');
    }

    void emit_is_null(const AstNode* node) {
        if (node->first_child) emit_node(node->first_child);
        sb_.append(" IS NULL");
    }

    void emit_is_not_null(const AstNode* node) {
        if (node->first_child) emit_node(node->first_child);
        sb_.append(" IS NOT NULL");
    }

    void emit_between(const AstNode* node) {
        const AstNode* expr = node->first_child;
        const AstNode* low = expr ? expr->next_sibling : nullptr;
        const AstNode* high = low ? low->next_sibling : nullptr;
        if (expr) emit_node(expr);
        sb_.append(" BETWEEN ");
        if (low) emit_node(low);
        sb_.append(" AND ");
        if (high) emit_node(high);
    }

    void emit_in_list(const AstNode* node) {
        const AstNode* expr = node->first_child;
        if (expr) emit_node(expr);
        sb_.append(" IN (");
        bool first = true;
        for (const AstNode* val = expr ? expr->next_sibling : nullptr; val; val = val->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(val);
        }
        sb_.append_char(')');
    }

    void emit_case_when(const AstNode* node) {
        sb_.append("CASE ");
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
            sb_.append_char(' ');
        }
        sb_.append("END");
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_EMITTER_H
```

- [ ] **Step 4: Expose arena from Parser and add emit convenience**

Modify `include/sql_parser/parser.h` — add a public `arena()` accessor:

Add after `void reset();`:
```cpp
    // Access the arena (for emitter use)
    Arena& arena() { return arena_; }
```

- [ ] **Step 5: Update Makefile.new**

Add `$(TEST_DIR)/test_emitter.cpp` to `TEST_SRCS`.

- [ ] **Step 6: Build and run tests**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```

The round-trip tests verify that `parse(sql) → emit(ast)` produces output identical to the input. If some tests fail due to spacing/formatting differences between original SQL and emitted SQL, adjust the emitter to match. The emitter's job is to produce **semantically equivalent** SQL — exact whitespace preservation is not required, but we aim for faithful reproduction.

- [ ] **Step 7: Commit**

```bash
git add include/sql_parser/string_builder.h include/sql_parser/emitter.h \
    include/sql_parser/parser.h tests/test_emitter.cpp Makefile.new
git commit -m "feat: add query emitter with round-trip support for SET and SELECT"
```

---

### Task 2: Fix Round-Trip Failures and Edge Cases

This task is for fixing any round-trip test failures from Task 1. The emitter needs to produce output that matches the original input for unmodified ASTs. Common issues:

- Extra or missing spaces
- Keyword casing (emitter should use same case as original when possible)
- String quoting (emitter must re-add quotes around string literals)
- SET CHARSET vs CHARACTER SET (need to detect which was used)

- [ ] **Step 1: Run tests and identify failures**

```bash
make -f Makefile.new clean && make -f Makefile.new all 2>&1 | grep FAILED
```

- [ ] **Step 2: Fix each failure**

For each failing test, trace through the emitter logic and fix the output. Common fixes:
- Adjust spacing in `emit_select_stmt` (no trailing space before FROM)
- Handle string literal quoting in `emit_string_literal`
- Preserve original keyword text from AST node values

- [ ] **Step 3: Re-run all tests**

```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: All tests pass.

- [ ] **Step 4: Commit fixes**

```bash
git add include/sql_parser/emitter.h tests/test_emitter.cpp
git commit -m "fix: correct emitter output for round-trip test compliance"
```

---

## What's Next

After this plan is complete:

1. **Plan 5: Prepared Statement Cache** — Binary protocol support
2. **Plan 6: Performance Benchmarks** — Validate latency targets
