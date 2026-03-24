# SELECT Deep Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the full SELECT statement deep parser, upgrading SELECT from a Tier 2 stub to a Tier 1 parser that produces a complete AST with all clauses (FROM, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, locking).

**Architecture:** The SELECT parser is a header-only template class `SelectParser<D>` that follows the same pattern as `SetParser<D>`. It uses the existing `ExpressionParser<D>` for all expression positions (select items, WHERE conditions, HAVING, JOIN ON, etc.). Each clause is a separate parse method. The parser is lenient — it produces as much AST as it can, even for partial/malformed queries.

**Tech Stack:** C++17, existing arena/tokenizer/expression_parser infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-sql-parser-design.md` (Tier 1 SELECT Parser section)

---

## Scope

This plan builds:
1. SELECT parser (`select_parser.h`) — full AST for all SELECT clauses
2. Integration into `Parser<D>` — `parse_select()` upgraded from stub to real parser
3. Comprehensive tests from simple to complex SELECT statements

**Not in scope:** Query emitter/reconstruction, prepared statement cache, UNION/INTERSECT/EXCEPT.

---

## File Structure

```
include/sql_parser/
    select_parser.h       — SELECT statement parser (header-only template)

src/sql_parser/
    parser.cpp            — (modify) Replace parse_select() stub, add #include

tests/
    test_select.cpp       — SELECT parser tests

Makefile.new              — (modify) Add test_select.cpp to TEST_SRCS
```

---

### Task 1: SELECT Parser — Basic SELECT and FROM

**Files:**
- Create: `include/sql_parser/select_parser.h`
- Create: `tests/test_select.cpp`
- Modify: `Makefile.new` — add test_select.cpp
- Modify: `src/sql_parser/parser.cpp` — replace parse_select() stub

This task implements the core SELECT structure: select options, select item list (with aliases), and FROM clause with simple table references.

- [ ] **Step 1: Write tests for basic SELECT**

Create `tests/test_select.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

class MySQLSelectTest : public ::testing::Test {
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
};

// ========== Basic SELECT ==========

TEST_F(MySQLSelectTest, SelectLiteral) {
    auto r = parser.parse("SELECT 1", 8);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_SELECT_STMT);
}

TEST_F(MySQLSelectTest, SelectStar) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
}

TEST_F(MySQLSelectTest, SelectColumns) {
    auto r = parser.parse("SELECT id, name, email FROM users", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    EXPECT_EQ(child_count(items), 3);
}

TEST_F(MySQLSelectTest, SelectWithAlias) {
    auto r = parser.parse("SELECT id AS user_id, name AS user_name FROM users", 50);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    // Each item should have an alias child
    auto* first_item = items->first_child;
    ASSERT_NE(first_item, nullptr);
    EXPECT_EQ(first_item->type, NodeType::NODE_SELECT_ITEM);
    auto* alias = find_child(first_item, NodeType::NODE_ALIAS);
    ASSERT_NE(alias, nullptr);
}

TEST_F(MySQLSelectTest, SelectImplicitAlias) {
    // Alias without AS keyword
    auto r = parser.parse("SELECT id user_id FROM users", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectDistinct) {
    auto r = parser.parse("SELECT DISTINCT name FROM users", 31);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* opts = find_child(r.ast, NodeType::NODE_SELECT_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(MySQLSelectTest, SelectSqlCalcFoundRows) {
    auto r = parser.parse("SELECT SQL_CALC_FOUND_ROWS * FROM users", 40);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromQualifiedTable) {
    auto r = parser.parse("SELECT * FROM mydb.users", 24);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromTableAlias) {
    auto r = parser.parse("SELECT u.id FROM users u", 24);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromTableAsAlias) {
    auto r = parser.parse("SELECT u.id FROM users AS u", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromMultipleTables) {
    auto r = parser.parse("SELECT * FROM users, orders", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
    EXPECT_GE(child_count(from), 2);
}

TEST_F(MySQLSelectTest, SelectExpression) {
    auto r = parser.parse("SELECT 1 + 2, 'hello', NOW()", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectNoFrom) {
    auto r = parser.parse("SELECT 1, 'a', NOW()", 20);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    // No FROM clause
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    EXPECT_EQ(from, nullptr);
}
```

- [ ] **Step 2: Add TK_UNION, TK_OF, and TK_EXISTS to token.h and keyword tables**

These tokens are referenced by `select_parser.h` and `expression_parser.h`, so they must exist before those files compile.

Add `TK_UNION`, `TK_OF`, and `TK_EXISTS` to `TokenType` enum in `include/sql_parser/token.h` (after `TK_RESET`):
```cpp
TK_UNION,
TK_OF,
TK_EXISTS,
```

Note: `TK_EXISTS` is already in the enum from Plan 1. Verify it exists; if not, add it.

Add to `include/sql_parser/keywords_mysql.h` sorted array:
- `{"OF", 2, TokenType::TK_OF},` between `NULL` and `OFFSET`
- `{"UNION", 5, TokenType::TK_UNION},` between `TRUNCATE` and `UNCOMMITTED`

Add same entries to `include/sql_parser/keywords_pgsql.h` at the same sorted positions.

Also add `EXISTS` handling to the expression parser. In `include/sql_parser/expression_parser.h`, add a case in `parse_atom()` before the `TK_LPAREN` case:
```cpp
            case TokenType::TK_EXISTS: {
                tok_.skip();
                // EXISTS (subquery)
                AstNode* node = make_node(arena_, NodeType::NODE_SUBQUERY);
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    skip_to_matching_paren();
                }
                return node;
            }
```

- [ ] **Step 3: Create select_parser.h**

Create `include/sql_parser/select_parser.h`:
```cpp
#ifndef SQL_PARSER_SELECT_PARSER_H
#define SQL_PARSER_SELECT_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"

namespace sql_parser {

template <Dialect D>
class SelectParser {
public:
    SelectParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena) {}

    // Parse a SELECT statement (SELECT keyword already consumed by classifier).
    AstNode* parse() {
        AstNode* root = make_node(arena_, NodeType::NODE_SELECT_STMT);
        if (!root) return nullptr;

        // SELECT options: DISTINCT, ALL, SQL_CALC_FOUND_ROWS
        AstNode* opts = parse_select_options();
        if (opts) root->add_child(opts);

        // Select item list
        AstNode* items = parse_select_item_list();
        if (items) root->add_child(items);

        // INTO (before FROM in some MySQL variants — skip for now, handle after FROM)

        // FROM clause
        if (tok_.peek().type == TokenType::TK_FROM) {
            tok_.skip();
            AstNode* from = parse_from_clause();
            if (from) root->add_child(from);
        }

        // WHERE clause
        if (tok_.peek().type == TokenType::TK_WHERE) {
            tok_.skip();
            AstNode* where = parse_where_clause();
            if (where) root->add_child(where);
        }

        // GROUP BY clause
        if (tok_.peek().type == TokenType::TK_GROUP) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_BY) tok_.skip();
            AstNode* group_by = parse_group_by();
            if (group_by) root->add_child(group_by);
        }

        // HAVING clause
        if (tok_.peek().type == TokenType::TK_HAVING) {
            tok_.skip();
            AstNode* having = parse_having();
            if (having) root->add_child(having);
        }

        // ORDER BY clause
        if (tok_.peek().type == TokenType::TK_ORDER) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_BY) tok_.skip();
            AstNode* order_by = parse_order_by();
            if (order_by) root->add_child(order_by);
        }

        // LIMIT clause
        if (tok_.peek().type == TokenType::TK_LIMIT) {
            tok_.skip();
            AstNode* limit = parse_limit();
            if (limit) root->add_child(limit);
        }

        // FOR UPDATE / FOR SHARE (locking)
        if (tok_.peek().type == TokenType::TK_FOR) {
            AstNode* lock = parse_locking();
            if (lock) root->add_child(lock);
        }

        // INTO (MySQL: can appear here too — INTO OUTFILE/DUMPFILE/var)
        if constexpr (D == Dialect::MySQL) {
            if (tok_.peek().type == TokenType::TK_INTO) {
                AstNode* into = parse_into();
                if (into) root->add_child(into);
            }
        }

        return root;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;

    // ---- SELECT options ----

    AstNode* parse_select_options() {
        AstNode* opts = nullptr;
        while (true) {
            Token t = tok_.peek();
            if (t.type == TokenType::TK_DISTINCT || t.type == TokenType::TK_ALL) {
                if (!opts) opts = make_node(arena_, NodeType::NODE_SELECT_OPTIONS);
                tok_.skip();
                opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, t.text));
            } else if (t.type == TokenType::TK_SQL_CALC_FOUND_ROWS) {
                if (!opts) opts = make_node(arena_, NodeType::NODE_SELECT_OPTIONS);
                tok_.skip();
                opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, t.text));
            } else {
                break;
            }
        }
        return opts;
    }

    // ---- Select item list ----

    AstNode* parse_select_item_list() {
        AstNode* list = make_node(arena_, NodeType::NODE_SELECT_ITEM_LIST);
        if (!list) return nullptr;

        while (true) {
            AstNode* item = parse_select_item();
            if (!item) break;
            list->add_child(item);
            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }
        return list;
    }

    AstNode* parse_select_item() {
        AstNode* item = make_node(arena_, NodeType::NODE_SELECT_ITEM);
        if (!item) return nullptr;

        AstNode* expr = expr_parser_.parse();
        if (!expr) return nullptr;
        item->add_child(expr);

        // Optional alias: AS name, or just name (implicit alias)
        Token next = tok_.peek();
        if (next.type == TokenType::TK_AS) {
            tok_.skip();
            Token alias_name = tok_.next_token();
            AstNode* alias = make_node(arena_, NodeType::NODE_ALIAS, alias_name.text);
            item->add_child(alias);
        } else if (is_alias_start(next.type)) {
            // Implicit alias (no AS keyword): SELECT expr alias_name
            tok_.skip();
            AstNode* alias = make_node(arena_, NodeType::NODE_ALIAS, next.text);
            item->add_child(alias);
        }
        return item;
    }

    // ---- FROM clause ----

    AstNode* parse_from_clause() {
        AstNode* from = make_node(arena_, NodeType::NODE_FROM_CLAUSE);
        if (!from) return nullptr;

        // First table reference
        AstNode* table_ref = parse_table_reference();
        if (table_ref) from->add_child(table_ref);

        // Additional table refs (comma join) or explicit JOINs
        while (true) {
            Token t = tok_.peek();
            if (t.type == TokenType::TK_COMMA) {
                // Comma join: FROM t1, t2
                tok_.skip();
                AstNode* next_ref = parse_table_reference();
                if (next_ref) from->add_child(next_ref);
            } else if (is_join_start(t.type)) {
                // Explicit JOIN
                AstNode* join = parse_join(from->first_child);
                if (join) {
                    // Replace the last table ref with the join node
                    // Actually, append the join as a child of FROM
                    from->add_child(join);
                }
            } else {
                break;
            }
        }

        return from;
    }

    AstNode* parse_table_reference() {
        Token t = tok_.peek();

        // Subquery: (SELECT ...)
        if (t.type == TokenType::TK_LPAREN) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SELECT) {
                AstNode* subq = make_node(arena_, NodeType::NODE_SUBQUERY);
                // Skip to matching paren
                int depth = 1;
                while (depth > 0) {
                    Token st = tok_.next_token();
                    if (st.type == TokenType::TK_LPAREN) ++depth;
                    else if (st.type == TokenType::TK_RPAREN) --depth;
                    else if (st.type == TokenType::TK_EOF) break;
                }
                // Optional alias
                AstNode* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
                ref->add_child(subq);
                parse_optional_alias(ref);
                return ref;
            }
            // Parenthesized table reference — parse inner
            AstNode* inner = parse_table_reference();
            if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
            return inner;
        }

        // Simple table name or schema.table
        AstNode* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
        Token name = tok_.next_token();

        if (tok_.peek().type == TokenType::TK_DOT) {
            // Qualified: schema.table
            tok_.skip();
            Token table_name = tok_.next_token();
            AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, table_name.text));
            ref->add_child(qname);
        } else {
            ref->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
        }

        // Optional alias
        parse_optional_alias(ref);
        return ref;
    }

    void parse_optional_alias(AstNode* parent) {
        Token t = tok_.peek();
        if (t.type == TokenType::TK_AS) {
            tok_.skip();
            Token alias_name = tok_.next_token();
            parent->add_child(make_node(arena_, NodeType::NODE_ALIAS, alias_name.text));
        } else if (is_alias_start(t.type)) {
            tok_.skip();
            parent->add_child(make_node(arena_, NodeType::NODE_ALIAS, t.text));
        }
    }

    // ---- JOIN ----

    AstNode* parse_join(AstNode* /* left_ref */) {
        AstNode* join = make_node(arena_, NodeType::NODE_JOIN_CLAUSE);
        if (!join) return nullptr;

        // Consume join type tokens
        Token t = tok_.peek();
        StringRef join_type_start = t.text;
        StringRef join_type_end = t.text;

        // Optional: NATURAL, LEFT, RIGHT, FULL, INNER, OUTER, CROSS
        while (t.type == TokenType::TK_NATURAL || t.type == TokenType::TK_LEFT ||
               t.type == TokenType::TK_RIGHT || t.type == TokenType::TK_FULL ||
               t.type == TokenType::TK_INNER || t.type == TokenType::TK_OUTER ||
               t.type == TokenType::TK_CROSS) {
            tok_.skip();
            join_type_end = t.text;
            t = tok_.peek();
        }

        // Expect JOIN keyword
        if (t.type == TokenType::TK_JOIN) {
            join_type_end = t.text;
            tok_.skip();
        }

        // Set join type as value (covers the span from first modifier to JOIN)
        StringRef join_type{join_type_start.ptr,
            static_cast<uint32_t>((join_type_end.ptr + join_type_end.len) - join_type_start.ptr)};
        join->value_ptr = join_type.ptr;
        join->value_len = join_type.len;

        // Right table reference
        AstNode* right_ref = parse_table_reference();
        if (right_ref) join->add_child(right_ref);

        // Join condition: ON expr or USING (col_list)
        if (tok_.peek().type == TokenType::TK_ON) {
            tok_.skip();
            AstNode* on_expr = expr_parser_.parse();
            if (on_expr) join->add_child(on_expr);
        } else if (tok_.peek().type == TokenType::TK_USING) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_LPAREN) {
                tok_.skip();
                AstNode* using_list = make_node(arena_, NodeType::NODE_IDENTIFIER, StringRef{"USING", 5});
                while (true) {
                    Token col = tok_.next_token();
                    using_list->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, col.text));
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
                if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                join->add_child(using_list);
            }
        }

        return join;
    }

    // ---- WHERE ----

    AstNode* parse_where_clause() {
        AstNode* where = make_node(arena_, NodeType::NODE_WHERE_CLAUSE);
        if (!where) return nullptr;
        AstNode* expr = expr_parser_.parse();
        if (expr) where->add_child(expr);
        return where;
    }

    // ---- GROUP BY ----

    AstNode* parse_group_by() {
        AstNode* group_by = make_node(arena_, NodeType::NODE_GROUP_BY_CLAUSE);
        if (!group_by) return nullptr;

        while (true) {
            AstNode* expr = expr_parser_.parse();
            if (!expr) break;
            group_by->add_child(expr);
            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }
        return group_by;
    }

    // ---- HAVING ----

    AstNode* parse_having() {
        AstNode* having = make_node(arena_, NodeType::NODE_HAVING_CLAUSE);
        if (!having) return nullptr;
        AstNode* expr = expr_parser_.parse();
        if (expr) having->add_child(expr);
        return having;
    }

    // ---- ORDER BY ----

    AstNode* parse_order_by() {
        AstNode* order_by = make_node(arena_, NodeType::NODE_ORDER_BY_CLAUSE);
        if (!order_by) return nullptr;

        while (true) {
            AstNode* expr = expr_parser_.parse();
            if (!expr) break;

            AstNode* item = make_node(arena_, NodeType::NODE_ORDER_BY_ITEM);
            item->add_child(expr);

            // Optional ASC/DESC
            Token dir = tok_.peek();
            if (dir.type == TokenType::TK_ASC || dir.type == TokenType::TK_DESC) {
                tok_.skip();
                item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, dir.text));
            }

            order_by->add_child(item);

            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }
        return order_by;
    }

    // ---- LIMIT ----

    AstNode* parse_limit() {
        AstNode* limit = make_node(arena_, NodeType::NODE_LIMIT_CLAUSE);
        if (!limit) return nullptr;

        // LIMIT count [OFFSET offset]  or  LIMIT offset, count (MySQL)
        AstNode* first = expr_parser_.parse();
        if (first) limit->add_child(first);

        if (tok_.peek().type == TokenType::TK_OFFSET) {
            tok_.skip();
            AstNode* offset = expr_parser_.parse();
            if (offset) limit->add_child(offset);
        } else if (tok_.peek().type == TokenType::TK_COMMA) {
            // MySQL: LIMIT offset, count
            tok_.skip();
            AstNode* count = expr_parser_.parse();
            if (count) limit->add_child(count);
        }

        if constexpr (D == Dialect::PostgreSQL) {
            // PostgreSQL also supports FETCH FIRST N ROWS ONLY after LIMIT/OFFSET
            // We handle OFFSET here too since PgSQL uses LIMIT x OFFSET y
        }

        return limit;
    }

    // ---- FOR UPDATE / FOR SHARE ----

    AstNode* parse_locking() {
        AstNode* lock = make_node(arena_, NodeType::NODE_LOCKING_CLAUSE);
        if (!lock) return nullptr;

        tok_.skip(); // consume FOR
        Token strength = tok_.next_token(); // UPDATE or SHARE
        lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, strength.text));

        // Optional: OF table_list
        if (tok_.peek().type == TokenType::TK_OF) {
            tok_.skip();
            while (true) {
                Token table = tok_.next_token();
                lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, table.text));
                if (tok_.peek().type == TokenType::TK_COMMA) {
                    tok_.skip();
                } else {
                    break;
                }
            }
        }

        // Optional: NOWAIT or SKIP LOCKED
        if (tok_.peek().type == TokenType::TK_NOWAIT) {
            tok_.skip();
            lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, StringRef{"NOWAIT", 6}));
        } else if (tok_.peek().type == TokenType::TK_SKIP) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_LOCKED) tok_.skip();
            lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, StringRef{"SKIP LOCKED", 11}));
        }

        return lock;
    }

    // ---- INTO (MySQL: INTO OUTFILE/DUMPFILE/@var) ----

    AstNode* parse_into() {
        AstNode* into = make_node(arena_, NodeType::NODE_INTO_CLAUSE);
        if (!into) return nullptr;

        tok_.skip(); // consume INTO
        Token t = tok_.peek();

        if (t.type == TokenType::TK_OUTFILE) {
            tok_.skip();
            Token filename = tok_.next_token();
            into->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                StringRef{"OUTFILE", 7}));
            into->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, filename.text));
        } else if (t.type == TokenType::TK_DUMPFILE) {
            tok_.skip();
            Token filename = tok_.next_token();
            into->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                StringRef{"DUMPFILE", 8}));
            into->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, filename.text));
        } else {
            // INTO @var1, @var2, ...
            while (true) {
                AstNode* var = expr_parser_.parse();
                if (var) into->add_child(var);
                if (tok_.peek().type == TokenType::TK_COMMA) {
                    tok_.skip();
                } else {
                    break;
                }
            }
        }

        return into;
    }

    // ---- Helpers ----

    static bool is_join_start(TokenType type) {
        return type == TokenType::TK_JOIN || type == TokenType::TK_INNER ||
               type == TokenType::TK_LEFT || type == TokenType::TK_RIGHT ||
               type == TokenType::TK_FULL || type == TokenType::TK_OUTER ||
               type == TokenType::TK_CROSS || type == TokenType::TK_NATURAL;
    }

    // Check if a token can start an implicit alias (identifier-like, not a clause keyword)
    static bool is_alias_start(TokenType type) {
        if (type == TokenType::TK_IDENTIFIER) return true;
        // Some keywords are NOT valid alias starts because they start clauses
        switch (type) {
            case TokenType::TK_FROM:
            case TokenType::TK_WHERE:
            case TokenType::TK_GROUP:
            case TokenType::TK_HAVING:
            case TokenType::TK_ORDER:
            case TokenType::TK_LIMIT:
            case TokenType::TK_FOR:
            case TokenType::TK_INTO:
            case TokenType::TK_JOIN:
            case TokenType::TK_INNER:
            case TokenType::TK_LEFT:
            case TokenType::TK_RIGHT:
            case TokenType::TK_FULL:
            case TokenType::TK_OUTER:
            case TokenType::TK_CROSS:
            case TokenType::TK_NATURAL:
            case TokenType::TK_ON:
            case TokenType::TK_USING:
            case TokenType::TK_UNION:
            case TokenType::TK_SEMICOLON:
            case TokenType::TK_RPAREN:
            case TokenType::TK_EOF:
            case TokenType::TK_COMMA:
            case TokenType::TK_SET:
            case TokenType::TK_LOCK:
            case TokenType::TK_UNLOCK:
                return false;
            default:
                return true;  // Keywords not in the blocklist can be implicit aliases
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_SELECT_PARSER_H
```

- [ ] **Step 4: Integrate into Parser class**

Modify `src/sql_parser/parser.cpp` — add include and replace stub:

Add after existing includes:
```cpp
#include "sql_parser/select_parser.h"
```

Replace `parse_select()`:
```cpp
template <Dialect D>
ParseResult Parser<D>::parse_select() {
    ParseResult r;
    r.stmt_type = StmtType::SELECT;

    SelectParser<D> select_parser(tokenizer_, arena_);
    AstNode* ast = select_parser.parse();

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

- [ ] **Step 5: Update Makefile.new**

Add `$(TEST_DIR)/test_select.cpp` to `TEST_SRCS` in `Makefile.new`.

- [ ] **Step 6: Build and run tests**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add include/sql_parser/select_parser.h include/sql_parser/token.h \
    include/sql_parser/keywords_mysql.h include/sql_parser/keywords_pgsql.h \
    src/sql_parser/parser.cpp tests/test_select.cpp Makefile.new
git commit -m "feat: add SELECT deep parser with FROM, WHERE, GROUP BY, ORDER BY, LIMIT, JOIN"
```

---

### Task 2: Comprehensive SELECT Tests — JOINs, Subqueries, Complex Queries

**Files:**
- Modify: `tests/test_select.cpp` — add extensive tests

- [ ] **Step 1: Add JOIN tests**

Append to `tests/test_select.cpp`:
```cpp
// ========== JOINs ==========

TEST_F(MySQLSelectTest, InnerJoin) {
    auto r = parser.parse("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", 66);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
}

TEST_F(MySQLSelectTest, LeftJoin) {
    auto r = parser.parse("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id", 65);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, RightJoin) {
    auto r = parser.parse("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id", 66);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, LeftOuterJoin) {
    auto r = parser.parse("SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id", 71);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, CrossJoin) {
    auto r = parser.parse("SELECT * FROM users CROSS JOIN orders", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, NaturalJoin) {
    auto r = parser.parse("SELECT * FROM users NATURAL JOIN orders", 39);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, JoinUsing) {
    auto r = parser.parse("SELECT * FROM users JOIN orders USING (user_id)", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, MultipleJoins) {
    const char* sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN items ON orders.id = items.order_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, JoinWithAlias) {
    auto r = parser.parse("SELECT * FROM users u JOIN orders o ON u.id = o.user_id", 55);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== WHERE ==========

TEST_F(MySQLSelectTest, WhereSimple) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1", 32);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* where = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    ASSERT_NE(where, nullptr);
}

TEST_F(MySQLSelectTest, WhereComplex) {
    auto r = parser.parse("SELECT * FROM users WHERE age > 18 AND status = 'active'", 56);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereIn) {
    auto r = parser.parse("SELECT * FROM users WHERE id IN (1, 2, 3)", 42);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereBetween) {
    auto r = parser.parse("SELECT * FROM users WHERE age BETWEEN 18 AND 65", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereLike) {
    auto r = parser.parse("SELECT * FROM users WHERE name LIKE '%john%'", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereIsNull) {
    auto r = parser.parse("SELECT * FROM users WHERE email IS NULL", 39);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereSubquery) {
    auto r = parser.parse("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", 60);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== GROUP BY / HAVING ==========

TEST_F(MySQLSelectTest, GroupBy) {
    auto r = parser.parse("SELECT status, COUNT(*) FROM users GROUP BY status", 51);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* gb = find_child(r.ast, NodeType::NODE_GROUP_BY_CLAUSE);
    ASSERT_NE(gb, nullptr);
}

TEST_F(MySQLSelectTest, GroupByMultiple) {
    auto r = parser.parse("SELECT dept, status, COUNT(*) FROM users GROUP BY dept, status", 62);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, GroupByHaving) {
    auto r = parser.parse("SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5", 71);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* having = find_child(r.ast, NodeType::NODE_HAVING_CLAUSE);
    ASSERT_NE(having, nullptr);
}

// ========== ORDER BY ==========

TEST_F(MySQLSelectTest, OrderBy) {
    auto r = parser.parse("SELECT * FROM users ORDER BY name", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ob = find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE);
    ASSERT_NE(ob, nullptr);
}

TEST_F(MySQLSelectTest, OrderByDesc) {
    auto r = parser.parse("SELECT * FROM users ORDER BY created_at DESC", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, OrderByMultiple) {
    auto r = parser.parse("SELECT * FROM users ORDER BY last_name ASC, first_name ASC", 58);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* ob = find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE);
    ASSERT_NE(ob, nullptr);
    EXPECT_EQ(child_count(ob), 2);
}

// ========== LIMIT ==========

TEST_F(MySQLSelectTest, Limit) {
    auto r = parser.parse("SELECT * FROM users LIMIT 10", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* limit = find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE);
    ASSERT_NE(limit, nullptr);
}

TEST_F(MySQLSelectTest, LimitOffset) {
    auto r = parser.parse("SELECT * FROM users LIMIT 10 OFFSET 20", 38);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* limit = find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE);
    ASSERT_NE(limit, nullptr);
    EXPECT_EQ(child_count(limit), 2);
}

TEST_F(MySQLSelectTest, LimitCommaOffset) {
    // MySQL syntax: LIMIT offset, count
    auto r = parser.parse("SELECT * FROM users LIMIT 20, 10", 32);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* limit = find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE);
    ASSERT_NE(limit, nullptr);
    EXPECT_EQ(child_count(limit), 2);
}

// ========== FOR UPDATE / FOR SHARE ==========

TEST_F(MySQLSelectTest, ForUpdate) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR UPDATE", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* lock = find_child(r.ast, NodeType::NODE_LOCKING_CLAUSE);
    ASSERT_NE(lock, nullptr);
}

TEST_F(MySQLSelectTest, ForShare) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR SHARE", 43);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, ForUpdateNowait) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT", 51);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, ForUpdateSkipLocked) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR UPDATE SKIP LOCKED", 56);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== Complex queries ==========

TEST_F(MySQLSelectTest, FullQuery) {
    const char* sql = "SELECT u.id, u.name, COUNT(o.id) AS order_count "
                      "FROM users u "
                      "LEFT JOIN orders o ON u.id = o.user_id "
                      "WHERE u.status = 'active' "
                      "GROUP BY u.id, u.name "
                      "HAVING COUNT(o.id) > 5 "
                      "ORDER BY order_count DESC "
                      "LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_FROM_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_WHERE_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_GROUP_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_HAVING_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLSelectTest, SubqueryInFrom) {
    const char* sql = "SELECT t.id FROM (SELECT id FROM users WHERE active = 1) AS t";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, MultiStatement) {
    const char* sql = "SELECT 1; SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    EXPECT_TRUE(r.has_remaining());
}

TEST_F(MySQLSelectTest, SelectWithSemicolon) {
    auto r = parser.parse("SELECT * FROM users;", 20);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== Bulk data-driven tests ==========

struct SelectTestCase {
    const char* sql;
    const char* description;
};

static const SelectTestCase select_bulk_cases[] = {
    {"SELECT 1", "literal"},
    {"SELECT 1, 2, 3", "multiple literals"},
    {"SELECT 'hello'", "string literal"},
    {"SELECT NULL", "null"},
    {"SELECT TRUE", "true"},
    {"SELECT FALSE", "false"},
    {"SELECT NOW()", "function call"},
    {"SELECT 1 + 2", "arithmetic"},
    {"SELECT *", "star"},
    {"SELECT * FROM t", "star from table"},
    {"SELECT a FROM t", "single column"},
    {"SELECT a, b, c FROM t", "multiple columns"},
    {"SELECT a AS x FROM t", "alias with AS"},
    {"SELECT t.a FROM t", "qualified column"},
    {"SELECT t.* FROM t", "qualified star"},
    {"SELECT DISTINCT a FROM t", "distinct"},
    {"SELECT ALL a FROM t", "all"},
    {"SELECT SQL_CALC_FOUND_ROWS * FROM t", "sql_calc_found_rows"},
    {"SELECT * FROM db.t", "qualified table"},
    {"SELECT * FROM t AS alias", "table alias with AS"},
    {"SELECT * FROM t alias", "table alias implicit"},
    {"SELECT * FROM t1, t2", "comma join"},
    {"SELECT * FROM t1 JOIN t2 ON t1.id = t2.id", "inner join"},
    {"SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id", "left join"},
    {"SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id", "right join"},
    {"SELECT * FROM t1 CROSS JOIN t2", "cross join"},
    {"SELECT * FROM t1 NATURAL JOIN t2", "natural join"},
    {"SELECT * FROM t1 JOIN t2 USING (id)", "join using"},
    {"SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id", "left outer join"},
    {"SELECT * FROM t WHERE a = 1", "where equal"},
    {"SELECT * FROM t WHERE a > 1 AND b < 10", "where and"},
    {"SELECT * FROM t WHERE a IN (1,2,3)", "where in"},
    {"SELECT * FROM t WHERE a IS NULL", "where is null"},
    {"SELECT * FROM t WHERE a IS NOT NULL", "where is not null"},
    {"SELECT * FROM t WHERE a BETWEEN 1 AND 10", "where between"},
    {"SELECT * FROM t WHERE a LIKE '%x%'", "where like"},
    {"SELECT * FROM t WHERE a NOT IN (1,2)", "where not in"},
    {"SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)", "where exists"},
    {"SELECT a, COUNT(*) FROM t GROUP BY a", "group by"},
    {"SELECT a, b, COUNT(*) FROM t GROUP BY a, b", "group by multiple"},
    {"SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1", "having"},
    {"SELECT * FROM t ORDER BY a", "order by"},
    {"SELECT * FROM t ORDER BY a DESC", "order by desc"},
    {"SELECT * FROM t ORDER BY a ASC, b DESC", "order by multiple"},
    {"SELECT * FROM t LIMIT 10", "limit"},
    {"SELECT * FROM t LIMIT 10 OFFSET 5", "limit offset"},
    {"SELECT * FROM t LIMIT 5, 10", "limit comma"},
    {"SELECT * FROM t WHERE a = 1 FOR UPDATE", "for update"},
    {"SELECT * FROM t WHERE a = 1 FOR SHARE", "for share"},
    {"SELECT * FROM t FOR UPDATE NOWAIT", "for update nowait"},
    {"SELECT * FROM t FOR UPDATE SKIP LOCKED", "for update skip locked"},
    {"SELECT COUNT(*), SUM(a), AVG(b), MIN(c), MAX(d) FROM t", "aggregate functions"},
    {"SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM t", "case when"},
    {"SELECT * FROM (SELECT 1) AS t", "subquery in from"},
    {"SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.b = t3.b", "multiple joins"},
    {"SELECT a FROM t WHERE b = (SELECT MAX(b) FROM t2)", "scalar subquery in where"},
};

TEST(MySQLSelectBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : select_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::SELECT)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== PostgreSQL SELECT ==========

class PgSQLSelectTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;
};

TEST_F(PgSQLSelectTest, BasicSelect) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSelectTest, LimitOffset) {
    auto r = parser.parse("SELECT * FROM users LIMIT 10 OFFSET 5", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(PgSQLSelectTest, ForUpdate) {
    auto r = parser.parse("SELECT * FROM users FOR UPDATE", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
}
```

- [ ] **Step 2: Build and run all tests**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_select.cpp
git commit -m "test: add comprehensive SELECT parser tests with JOINs, subqueries, and bulk cases"
```

---

### Task 3: Verify and Clean Up

**Files:**
- No new files — verification only

- [ ] **Step 1: Run full test suite**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: ALL tests pass, zero warnings.

- [ ] **Step 2: Check for compiler warnings**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all 2>&1 | grep -i warning
```
Expected: Zero warnings (or only from Google Test internals).

- [ ] **Step 3: Commit if any fixes were needed**

```bash
# Only if changes were made
git add -A && git commit -m "fix: clean up warnings after SELECT parser integration"
```

---

## What's Next

After this plan is complete:

1. **Plan 4: Query Emitter** — AST → SQL reconstruction (parse → modify → emit)
2. **Plan 5: Prepared Statement Cache** — Binary protocol support
3. **Plan 6: Performance Benchmarks** — Validate latency targets
