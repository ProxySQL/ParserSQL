# Expression Parser + SET Deep Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Pratt expression parser and full SET statement deep parser, upgrading SET from a Tier 2 stub to a Tier 1 parser that produces a complete AST for query reconstruction.

**Architecture:** The expression parser is a standalone module using precedence climbing (Pratt parsing). It handles literals, identifiers, unary/binary operators, function calls, IS [NOT] NULL, BETWEEN, IN, CASE/WHEN, and subqueries. The SET parser uses the expression parser for right-hand-side values. Both are dialect-templated. After this plan, `parse_set()` returns `ParseResult::OK` with a full AST instead of `PARTIAL`.

**Tech Stack:** C++17, existing arena/tokenizer/ast infrastructure from Plan 1

**Spec:** `docs/superpowers/specs/2026-03-24-sql-parser-design.md`

---

## Scope

This plan builds:
1. Pratt expression parser (`expression_parser.h`) — shared by SET and future SELECT parser
2. SET deep parser (`set_parser.h`) — full AST for all SET variants (MySQL + PostgreSQL)
3. Integration into `Parser<D>` — `parse_set()` upgraded from stub to real parser
4. Tests for both expression parsing and SET parsing

**Not in scope:** SELECT deep parser, emitter/reconstruction, prepared statement cache.

---

## File Structure

```
include/sql_parser/
    expression_parser.h   — Pratt expression parser (header-only template)
    set_parser.h          — SET statement parser (header-only template)
    common.h              — (modify) Add NODE_SCOPE_SPECIFIER if needed

src/sql_parser/
    parser.cpp            — (modify) Replace parse_set() stub with real implementation

tests/
    test_expression.cpp   — Expression parser unit tests
    test_set.cpp          — SET parser unit tests

Makefile.new              — (modify) Add new test files
```

---

### Task 1: Expression Parser — Literals and Identifiers

**Files:**
- Create: `include/sql_parser/expression_parser.h`
- Create: `tests/test_expression.cpp`
- Modify: `Makefile.new` — add `test_expression.cpp` to TEST_SRCS

- [ ] **Step 1: Write failing tests for literal and identifier parsing**

Create `tests/test_expression.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/expression_parser.h"

using namespace sql_parser;

// Helper: parse an expression from a SQL string using a fresh parser context.
// We use the tokenizer directly since expression parsing is an internal function.
class ExpressionTest : public ::testing::Test {
protected:
    Arena arena{4096};
    Tokenizer<Dialect::MySQL> tok;

    AstNode* parse_expr(const char* sql) {
        tok.reset(sql, strlen(sql));
        ExpressionParser<Dialect::MySQL> ep(tok, arena);
        return ep.parse();
    }
};

TEST_F(ExpressionTest, IntegerLiteral) {
    AstNode* node = parse_expr("42");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "42");
}

TEST_F(ExpressionTest, FloatLiteral) {
    AstNode* node = parse_expr("3.14");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_FLOAT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "3.14");
}

TEST_F(ExpressionTest, StringLiteral) {
    AstNode* node = parse_expr("'hello'");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "hello");
}

TEST_F(ExpressionTest, NullLiteral) {
    AstNode* node = parse_expr("NULL");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_NULL);
}

TEST_F(ExpressionTest, TrueLiteral) {
    AstNode* node = parse_expr("TRUE");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "TRUE");
}

TEST_F(ExpressionTest, FalseLiteral) {
    AstNode* node = parse_expr("FALSE");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "FALSE");
}

TEST_F(ExpressionTest, SimpleIdentifier) {
    AstNode* node = parse_expr("my_column");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_COLUMN_REF);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "my_column");
}

TEST_F(ExpressionTest, QualifiedIdentifier) {
    AstNode* node = parse_expr("t.col");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_QUALIFIED_NAME);
    // first child = table, second child = column
    ASSERT_NE(node->first_child, nullptr);
    ASSERT_NE(node->first_child->next_sibling, nullptr);
}

TEST_F(ExpressionTest, Asterisk) {
    AstNode* node = parse_expr("*");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_ASTERISK);
}

TEST_F(ExpressionTest, Placeholder) {
    AstNode* node = parse_expr("?");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_PLACEHOLDER);
}

TEST_F(ExpressionTest, DefaultKeyword) {
    AstNode* node = parse_expr("DEFAULT");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IDENTIFIER);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "DEFAULT");
}

TEST_F(ExpressionTest, UserVariable) {
    AstNode* node = parse_expr("@my_var");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_COLUMN_REF);
}

TEST_F(ExpressionTest, ParenthesizedExpression) {
    AstNode* node = parse_expr("(42)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "42");
}
```

- [ ] **Step 2: Create expression_parser.h with atom parsing**

Create `include/sql_parser/expression_parser.h`:
```cpp
#ifndef SQL_PARSER_EXPRESSION_PARSER_H
#define SQL_PARSER_EXPRESSION_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"

namespace sql_parser {

// Operator precedence levels for Pratt parsing
enum class Precedence : uint8_t {
    NONE = 0,
    OR,            // OR
    AND,           // AND
    NOT,           // NOT (prefix)
    COMPARISON,    // =, <, >, <=, >=, !=, <>, IS, LIKE, IN, BETWEEN
    ADDITION,      // +, -
    MULTIPLICATION,// *, /, %
    UNARY,         // - (prefix), NOT
    POSTFIX,       // IS NULL, IS NOT NULL
    CALL,          // function()
    PRIMARY,       // literals, identifiers
};

template <Dialect D>
class ExpressionParser {
public:
    ExpressionParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena) {}

    // Parse an expression with minimum precedence 0
    AstNode* parse(Precedence min_prec = Precedence::NONE) {
        AstNode* left = parse_atom();
        if (!left) return nullptr;

        while (true) {
            Precedence prec = infix_precedence(tok_.peek().type);
            if (prec <= min_prec) break;

            left = parse_infix(left, prec);
            if (!left) return nullptr;
        }

        return left;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;

    // Parse a primary expression (atom)
    AstNode* parse_atom() {
        Token t = tok_.peek();

        switch (t.type) {
            case TokenType::TK_INTEGER: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_INT, t.text);
            }
            case TokenType::TK_FLOAT: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_FLOAT, t.text);
            }
            case TokenType::TK_STRING: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_STRING, t.text);
            }
            case TokenType::TK_NULL: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_NULL, t.text);
            }
            case TokenType::TK_TRUE:
            case TokenType::TK_FALSE: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_INT, t.text);
            }
            case TokenType::TK_DEFAULT: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_IDENTIFIER, t.text);
            }
            case TokenType::TK_ASTERISK: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_ASTERISK, t.text);
            }
            case TokenType::TK_QUESTION: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_PLACEHOLDER, t.text);
            }
            case TokenType::TK_DOLLAR_NUM: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_PLACEHOLDER, t.text);
            }
            case TokenType::TK_AT: {
                // User variable: @name
                tok_.skip();
                Token name = tok_.next_token();
                // Build @name as a single COLUMN_REF with combined text
                // value_ptr points to @ in original input, len covers @name
                StringRef full{t.text.ptr,
                    static_cast<uint32_t>((name.text.ptr + name.text.len) - t.text.ptr)};
                return make_node(arena_, NodeType::NODE_COLUMN_REF, full);
            }
            case TokenType::TK_DOUBLE_AT: {
                // System variable: @@name or @@scope.name
                tok_.skip();
                Token name = tok_.next_token();
                StringRef full{t.text.ptr,
                    static_cast<uint32_t>((name.text.ptr + name.text.len) - t.text.ptr)};
                AstNode* node = make_node(arena_, NodeType::NODE_COLUMN_REF, full);
                // Check for @@scope.name
                if (tok_.peek().type == TokenType::TK_DOT) {
                    tok_.skip();
                    Token var_name = tok_.next_token();
                    full = StringRef{t.text.ptr,
                        static_cast<uint32_t>((var_name.text.ptr + var_name.text.len) - t.text.ptr)};
                    node->value_ptr = full.ptr;
                    node->value_len = full.len;
                }
                return node;
            }
            case TokenType::TK_MINUS: {
                // Unary minus
                tok_.skip();
                AstNode* operand = parse(Precedence::UNARY);
                if (!operand) return nullptr;
                AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text);
                node->add_child(operand);
                return node;
            }
            case TokenType::TK_PLUS: {
                // Unary plus
                tok_.skip();
                return parse(Precedence::UNARY);
            }
            case TokenType::TK_NOT: {
                tok_.skip();
                // NOT IN, NOT BETWEEN, NOT LIKE are not unary prefix on the
                // next atom — they modify the following infix operator.
                // But NOT here is in prefix position (before the operand),
                // so we parse the operand, then check if the next infix is
                // IN/BETWEEN/LIKE and negate it.
                AstNode* operand = parse(Precedence::NOT);
                if (!operand) return nullptr;
                AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text);
                node->add_child(operand);
                return node;
            }
            case TokenType::TK_CASE: {
                tok_.skip();
                return parse_case();
            }
            case TokenType::TK_LPAREN: {
                tok_.skip();
                // Could be subquery: (SELECT ...)
                if (tok_.peek().type == TokenType::TK_SELECT) {
                    // Subquery — for now, skip to matching paren
                    AstNode* node = make_node(arena_, NodeType::NODE_SUBQUERY);
                    skip_to_matching_paren();
                    return node;
                }
                AstNode* expr = parse();
                if (tok_.peek().type == TokenType::TK_RPAREN) {
                    tok_.skip();
                }
                return expr;
            }
            case TokenType::TK_IDENTIFIER: {
                tok_.skip();
                return parse_identifier_or_function(t);
            }
            // Keywords that can appear as identifiers in expression context
            // (e.g., column names that happen to be keywords)
            default: {
                if (is_keyword_as_identifier(t.type)) {
                    tok_.skip();
                    return parse_identifier_or_function(t);
                }
                return nullptr;  // not an expression
            }
        }
    }

    AstNode* parse_identifier_or_function(const Token& name_token) {
        // Check for function call: name(
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();  // consume (
            AstNode* func = make_node(arena_, NodeType::NODE_FUNCTION_CALL, name_token.text);
            // Parse argument list
            if (tok_.peek().type != TokenType::TK_RPAREN) {
                while (true) {
                    AstNode* arg = parse();
                    if (arg) func->add_child(arg);
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
            }
            if (tok_.peek().type == TokenType::TK_RPAREN) {
                tok_.skip();
            }
            return func;
        }

        // Check for qualified name: table.column
        if (tok_.peek().type == TokenType::TK_DOT) {
            tok_.skip();  // consume dot
            Token col = tok_.next_token();
            AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name_token.text));
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, col.text));
            return qname;
        }

        return make_node(arena_, NodeType::NODE_COLUMN_REF, name_token.text);
    }

    // Infix precedence for a token type.
    // Returns NONE if not an infix operator (stops the Pratt loop).
    // NOT is handled here as a pseudo-infix: NOT IN, NOT BETWEEN, NOT LIKE
    // are compound operators at COMPARISON precedence.
    static Precedence infix_precedence(TokenType type) {
        switch (type) {
            case TokenType::TK_OR:             return Precedence::OR;
            case TokenType::TK_AND:            return Precedence::AND;
            case TokenType::TK_NOT:            return Precedence::COMPARISON; // NOT IN/BETWEEN/LIKE
            case TokenType::TK_EQUAL:
            case TokenType::TK_NOT_EQUAL:
            case TokenType::TK_LESS:
            case TokenType::TK_GREATER:
            case TokenType::TK_LESS_EQUAL:
            case TokenType::TK_GREATER_EQUAL:
            case TokenType::TK_LIKE:           return Precedence::COMPARISON;
            case TokenType::TK_IS:             return Precedence::COMPARISON;
            case TokenType::TK_IN:             return Precedence::COMPARISON;
            case TokenType::TK_BETWEEN:        return Precedence::COMPARISON;
            case TokenType::TK_PLUS:
            case TokenType::TK_MINUS:          return Precedence::ADDITION;
            case TokenType::TK_ASTERISK:
            case TokenType::TK_SLASH:
            case TokenType::TK_PERCENT:        return Precedence::MULTIPLICATION;
            case TokenType::TK_DOUBLE_PIPE:    return Precedence::ADDITION; // string concat
            default:                           return Precedence::NONE;
        }
    }

    AstNode* parse_infix(AstNode* left, Precedence prec) {
        Token op = tok_.next_token();

        switch (op.type) {
            case TokenType::TK_NOT: {
                // NOT IN / NOT BETWEEN / NOT LIKE — compound negated infix
                Token actual_op = tok_.peek();
                if (actual_op.type == TokenType::TK_IN) {
                    tok_.skip();
                    AstNode* in_node = parse_in(left);
                    // Wrap in NOT
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(in_node);
                    return not_node;
                }
                if (actual_op.type == TokenType::TK_BETWEEN) {
                    tok_.skip();
                    AstNode* between_node = parse_between(left);
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(between_node);
                    return not_node;
                }
                if (actual_op.type == TokenType::TK_LIKE) {
                    tok_.skip();
                    AstNode* right = parse(prec);
                    AstNode* like_node = make_node(arena_, NodeType::NODE_BINARY_OP, actual_op.text);
                    like_node->add_child(left);
                    if (right) like_node->add_child(right);
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(like_node);
                    return not_node;
                }
                // Standalone NOT in infix position — shouldn't happen, return left
                return left;
            }
            case TokenType::TK_IS: {
                // IS [NOT] NULL
                bool is_not = false;
                if (tok_.peek().type == TokenType::TK_NOT) {
                    is_not = true;
                    tok_.skip();
                }
                if (tok_.peek().type == TokenType::TK_NULL) {
                    tok_.skip();
                    NodeType nt = is_not ? NodeType::NODE_IS_NOT_NULL : NodeType::NODE_IS_NULL;
                    AstNode* node = make_node(arena_, nt);
                    node->add_child(left);
                    return node;
                }
                // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE
                if (tok_.peek().type == TokenType::TK_TRUE || tok_.peek().type == TokenType::TK_FALSE) {
                    Token val = tok_.next_token();
                    AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP,
                        is_not ? StringRef{"IS NOT", 6} : StringRef{"IS", 2});
                    node->add_child(left);
                    node->add_child(make_node(arena_, NodeType::NODE_LITERAL_INT, val.text));
                    return node;
                }
                return left;
            }
            case TokenType::TK_IN:
                return parse_in(left);
            case TokenType::TK_BETWEEN:
                return parse_between(left);
            default: {
                // Standard binary operator
                AstNode* right = parse(prec);
                if (!right) return left;
                AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP, op.text);
                node->add_child(left);
                node->add_child(right);
                return node;
            }
        }
    }

    // IN (value_list) or IN (subquery)
    AstNode* parse_in(AstNode* left) {
        AstNode* node = make_node(arena_, NodeType::NODE_IN_LIST);
        node->add_child(left);
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SELECT) {
                AstNode* sq = make_node(arena_, NodeType::NODE_SUBQUERY);
                skip_to_matching_paren();
                node->add_child(sq);
            } else {
                while (true) {
                    AstNode* val = parse();
                    if (val) node->add_child(val);
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
                if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
            }
        }
        return node;
    }

    // BETWEEN low AND high
    AstNode* parse_between(AstNode* left) {
        AstNode* node = make_node(arena_, NodeType::NODE_BETWEEN);
        node->add_child(left);
        AstNode* low = parse(Precedence::COMPARISON);
        node->add_child(low);
        if (tok_.peek().type == TokenType::TK_AND) {
            tok_.skip();
        }
        AstNode* high = parse(Precedence::COMPARISON);
        node->add_child(high);
        return node;
    }

    // CASE [expr] WHEN ... THEN ... [ELSE ...] END
    AstNode* parse_case() {
        AstNode* node = make_node(arena_, NodeType::NODE_CASE_WHEN);
        // Optional simple CASE expression: CASE expr WHEN ...
        if (tok_.peek().type != TokenType::TK_WHEN) {
            AstNode* case_expr = parse();
            if (case_expr) node->add_child(case_expr);
        }
        // WHEN ... THEN ... pairs
        while (tok_.peek().type == TokenType::TK_WHEN) {
            tok_.skip();
            AstNode* when_expr = parse();
            if (when_expr) node->add_child(when_expr);
            if (tok_.peek().type == TokenType::TK_THEN) tok_.skip();
            AstNode* then_expr = parse();
            if (then_expr) node->add_child(then_expr);
        }
        // Optional ELSE
        if (tok_.peek().type == TokenType::TK_ELSE) {
            tok_.skip();
            AstNode* else_expr = parse();
            if (else_expr) node->add_child(else_expr);
        }
        // END
        if (tok_.peek().type == TokenType::TK_END) tok_.skip();
        return node;
    }

    // Skip tokens until matching closing paren (handles nesting)
    void skip_to_matching_paren() {
        int depth = 1;
        while (depth > 0) {
            Token t = tok_.next_token();
            if (t.type == TokenType::TK_LPAREN) ++depth;
            else if (t.type == TokenType::TK_RPAREN) --depth;
            else if (t.type == TokenType::TK_EOF) break;
        }
    }

    // Some keywords can appear as identifiers in expression context
    static bool is_keyword_as_identifier(TokenType type) {
        switch (type) {
            // Keywords commonly used as column/table names
            case TokenType::TK_COUNT:
            case TokenType::TK_SUM:
            case TokenType::TK_AVG:
            case TokenType::TK_MIN:
            case TokenType::TK_MAX:
            case TokenType::TK_IF:
            case TokenType::TK_VALUES:
            case TokenType::TK_DATABASE:
            case TokenType::TK_SCHEMA:
            case TokenType::TK_TABLE:
            case TokenType::TK_INDEX:
            case TokenType::TK_VIEW:
            case TokenType::TK_NAMES:
            case TokenType::TK_CHARACTER:
            case TokenType::TK_CHARSET:
            case TokenType::TK_GLOBAL:
            case TokenType::TK_SESSION:
            case TokenType::TK_LOCAL:
            case TokenType::TK_LEVEL:
            case TokenType::TK_READ:
            case TokenType::TK_WRITE:
            case TokenType::TK_ONLY:
            case TokenType::TK_TRANSACTION:
            case TokenType::TK_ISOLATION:
            case TokenType::TK_COMMITTED:
            case TokenType::TK_UNCOMMITTED:
            case TokenType::TK_REPEATABLE:
            case TokenType::TK_SERIALIZABLE:
            case TokenType::TK_SHARE:
            case TokenType::TK_DATA:
            case TokenType::TK_RESET:
                return true;
            default:
                return false;
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_EXPRESSION_PARSER_H
```

- [ ] **Step 3: Add test_expression.cpp to Makefile.new**

In `Makefile.new`, add `$(TEST_DIR)/test_expression.cpp` and `$(TEST_DIR)/test_set.cpp` to `TEST_SRCS`:

Change the `TEST_SRCS` line to:
```makefile
TEST_SRCS = $(TEST_DIR)/test_main.cpp \
            $(TEST_DIR)/test_arena.cpp \
            $(TEST_DIR)/test_tokenizer.cpp \
            $(TEST_DIR)/test_classifier.cpp \
            $(TEST_DIR)/test_expression.cpp \
            $(TEST_DIR)/test_set.cpp
```

Also create an empty `tests/test_set.cpp` placeholder so the build doesn't break:
```cpp
#include <gtest/gtest.h>
// SET parser tests will be added in Task 3
```

- [ ] **Step 4: Build and run tests**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: All existing tests pass + new expression tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/sql_parser/expression_parser.h tests/test_expression.cpp tests/test_set.cpp Makefile.new
git commit -m "feat: add Pratt expression parser with literals, identifiers, and operators"
```

---

### Task 2: Expression Parser — Binary Operators, IS NULL, BETWEEN, IN, Functions

**Files:**
- Modify: `tests/test_expression.cpp` — add operator and complex expression tests

- [ ] **Step 1: Add binary operator and complex expression tests**

Append to `tests/test_expression.cpp`:
```cpp
TEST_F(ExpressionTest, BinaryAdd) {
    AstNode* node = parse_expr("1 + 2");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "+");
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_LITERAL_INT);
    ASSERT_NE(node->first_child->next_sibling, nullptr);
    EXPECT_EQ(node->first_child->next_sibling->type, NodeType::NODE_LITERAL_INT);
}

TEST_F(ExpressionTest, Precedence_MulOverAdd) {
    // 1 + 2 * 3 should parse as 1 + (2 * 3)
    AstNode* node = parse_expr("1 + 2 * 3");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "+");
    // Right child should be 2*3
    AstNode* right = node->first_child->next_sibling;
    ASSERT_NE(right, nullptr);
    EXPECT_EQ(right->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(right->value_ptr, right->value_len), "*");
}

TEST_F(ExpressionTest, ComparisonEqual) {
    AstNode* node = parse_expr("x = 1");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "=");
}

TEST_F(ExpressionTest, LogicalAnd) {
    AstNode* node = parse_expr("a = 1 AND b = 2");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "AND");
}

TEST_F(ExpressionTest, LogicalOr) {
    AstNode* node = parse_expr("a = 1 OR b = 2");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "OR");
}

TEST_F(ExpressionTest, UnaryMinus) {
    AstNode* node = parse_expr("-42");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP);
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_LITERAL_INT);
}

TEST_F(ExpressionTest, UnaryNot) {
    AstNode* node = parse_expr("NOT x");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP);
}

TEST_F(ExpressionTest, IsNull) {
    AstNode* node = parse_expr("x IS NULL");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IS_NULL);
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_COLUMN_REF);
}

TEST_F(ExpressionTest, IsNotNull) {
    AstNode* node = parse_expr("x IS NOT NULL");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IS_NOT_NULL);
}

TEST_F(ExpressionTest, Between) {
    AstNode* node = parse_expr("x BETWEEN 1 AND 10");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BETWEEN);
    // 3 children: expr, low, high
    ASSERT_NE(node->first_child, nullptr);
    ASSERT_NE(node->first_child->next_sibling, nullptr);
    ASSERT_NE(node->first_child->next_sibling->next_sibling, nullptr);
}

TEST_F(ExpressionTest, InList) {
    AstNode* node = parse_expr("x IN (1, 2, 3)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IN_LIST);
    // Children: expr, val1, val2, val3 = 4 children
    int count = 0;
    for (AstNode* c = node->first_child; c; c = c->next_sibling) ++count;
    EXPECT_EQ(count, 4);
}

TEST_F(ExpressionTest, FunctionCall) {
    AstNode* node = parse_expr("COUNT(*)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_FUNCTION_CALL);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "COUNT");
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_ASTERISK);
}

TEST_F(ExpressionTest, FunctionCallMultiArg) {
    AstNode* node = parse_expr("COALESCE(a, b, 0)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_FUNCTION_CALL);
    int count = 0;
    for (AstNode* c = node->first_child; c; c = c->next_sibling) ++count;
    EXPECT_EQ(count, 3);
}

TEST_F(ExpressionTest, NestedParens) {
    AstNode* node = parse_expr("(1 + 2) * 3");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "*");
    // Left child should be 1+2
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->first_child->value_ptr, node->first_child->value_len), "+");
}

TEST_F(ExpressionTest, LikeOperator) {
    AstNode* node = parse_expr("name LIKE '%test%'");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "LIKE");
}

TEST_F(ExpressionTest, StringConcat) {
    AstNode* node = parse_expr("a || b");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
}

TEST_F(ExpressionTest, NotIn) {
    AstNode* node = parse_expr("x NOT IN (1, 2)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP); // NOT wraps IN_LIST
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_IN_LIST);
}

TEST_F(ExpressionTest, NotBetween) {
    AstNode* node = parse_expr("x NOT BETWEEN 1 AND 10");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP); // NOT wraps BETWEEN
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_BETWEEN);
}

TEST_F(ExpressionTest, NotLike) {
    AstNode* node = parse_expr("name NOT LIKE '%test'");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP); // NOT wraps LIKE
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_BINARY_OP);
}

TEST_F(ExpressionTest, CaseWhenSimple) {
    AstNode* node = parse_expr("CASE WHEN x = 1 THEN 'a' ELSE 'b' END");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_CASE_WHEN);
}

TEST_F(ExpressionTest, CaseWhenSearched) {
    AstNode* node = parse_expr("CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_CASE_WHEN);
}

TEST_F(ExpressionTest, ZeroArgFunction) {
    AstNode* node = parse_expr("NOW()");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_FUNCTION_CALL);
    EXPECT_EQ(node->first_child, nullptr); // no args
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
git add tests/test_expression.cpp
git commit -m "test: add operator, IS NULL, BETWEEN, IN, and function call expression tests"
```

---

### Task 3: SET Deep Parser

**Files:**
- Create: `include/sql_parser/set_parser.h`
- Modify: `tests/test_set.cpp` — add SET tests
- Modify: `src/sql_parser/parser.cpp` — replace `parse_set()` stub
- Modify: `include/sql_parser/parser.h` — add SET parser include and method declarations

- [ ] **Step 1: Write SET parser tests**

Replace `tests/test_set.cpp` with:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

class MySQLSetTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    // Helper to count children of a node
    int child_count(const AstNode* node) {
        int n = 0;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
        return n;
    }
};

TEST_F(MySQLSetTest, SetSimpleVariable) {
    auto r = parser.parse("SET autocommit = 1", 18);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_SET_STMT);
    // One assignment child
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_VAR_ASSIGNMENT);
}

TEST_F(MySQLSetTest, SetMultipleVariables) {
    auto r = parser.parse("SET autocommit = 1, wait_timeout = 28800", 41);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(child_count(r.ast), 2);
}

TEST_F(MySQLSetTest, SetGlobalVariable) {
    auto r = parser.parse("SET GLOBAL max_connections = 100", 31);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    // First child of assignment is the var target
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    EXPECT_EQ(target->type, NodeType::NODE_VAR_TARGET);
}

TEST_F(MySQLSetTest, SetSessionVariable) {
    auto r = parser.parse("SET SESSION wait_timeout = 600", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetDoubleAtVariable) {
    auto r = parser.parse("SET @@session.wait_timeout = 600", 32);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetUserVariable) {
    auto r = parser.parse("SET @my_var = 42", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetNames) {
    auto r = parser.parse("SET NAMES utf8mb4", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_SET_STMT);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_NAMES);
}

TEST_F(MySQLSetTest, SetNamesCollate) {
    auto r = parser.parse("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_NAMES);
    // Should have 2 children: charset and collation
    EXPECT_EQ(child_count(r.ast->first_child), 2);
}

TEST_F(MySQLSetTest, SetCharacterSet) {
    auto r = parser.parse("SET CHARACTER SET utf8", 21);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_CHARSET);
}

TEST_F(MySQLSetTest, SetCharset) {
    auto r = parser.parse("SET CHARSET utf8", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_CHARSET);
}

TEST_F(MySQLSetTest, SetTransaction) {
    auto r = parser.parse("SET TRANSACTION READ ONLY", 25);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_TRANSACTION);
}

TEST_F(MySQLSetTest, SetTransactionIsolation) {
    auto r = parser.parse("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetGlobalTransaction) {
    auto r = parser.parse("SET GLOBAL TRANSACTION READ WRITE", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetExpressionRHS) {
    auto r = parser.parse("SET @x = 1 + 2", 14);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetColonEqual) {
    auto r = parser.parse("SET @x := 42", 12);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetNamesDefault) {
    auto r = parser.parse("SET NAMES DEFAULT", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetWithSemicolon) {
    const char* sql = "SET autocommit = 0; BEGIN";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    EXPECT_TRUE(r.has_remaining());
}

// ========== PostgreSQL SET Tests ==========

class PgSQLSetTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;
};

TEST_F(PgSQLSetTest, SetVarToValue) {
    auto r = parser.parse("SET client_encoding TO 'UTF8'", 29);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSetTest, SetVarEqualValue) {
    auto r = parser.parse("SET work_mem = '256MB'", 22);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSetTest, SetLocalVar) {
    auto r = parser.parse("SET LOCAL timezone = 'UTC'", 25);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSetTest, SetNamesPostgres) {
    auto r = parser.parse("SET NAMES 'UTF8'", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}
```

- [ ] **Step 2: Write set_parser.h**

Create `include/sql_parser/set_parser.h`:
```cpp
#ifndef SQL_PARSER_SET_PARSER_H
#define SQL_PARSER_SET_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"

namespace sql_parser {

template <Dialect D>
class SetParser {
public:
    SetParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena) {}

    // Parse a SET statement (SET keyword already consumed by classifier).
    // Returns the root NODE_SET_STMT node, or nullptr on failure.
    AstNode* parse() {
        AstNode* root = make_node(arena_, NodeType::NODE_SET_STMT);
        if (!root) return nullptr;

        Token next = tok_.peek();

        // SET NAMES ...
        if (next.type == TokenType::TK_NAMES) {
            tok_.skip();
            AstNode* names_node = parse_set_names();
            if (names_node) root->add_child(names_node);
            return root;
        }

        // SET CHARACTER SET ... or SET CHARSET ...
        if (next.type == TokenType::TK_CHARACTER) {
            tok_.skip();
            // Expect SET keyword
            if (tok_.peek().type == TokenType::TK_SET) {
                tok_.skip();
            }
            AstNode* charset_node = parse_set_charset();
            if (charset_node) root->add_child(charset_node);
            return root;
        }
        if (next.type == TokenType::TK_CHARSET) {
            tok_.skip();
            AstNode* charset_node = parse_set_charset();
            if (charset_node) root->add_child(charset_node);
            return root;
        }

        // SET [GLOBAL|SESSION] TRANSACTION ...
        // Need to check for scope + TRANSACTION or just TRANSACTION
        if (next.type == TokenType::TK_TRANSACTION) {
            tok_.skip();
            AstNode* txn_node = parse_set_transaction(StringRef{});
            if (txn_node) root->add_child(txn_node);
            return root;
        }

        if (next.type == TokenType::TK_GLOBAL || next.type == TokenType::TK_SESSION) {
            Token scope_tok = tok_.next_token();
            if (tok_.peek().type == TokenType::TK_TRANSACTION) {
                tok_.skip();
                AstNode* txn_node = parse_set_transaction(scope_tok.text);
                if (txn_node) root->add_child(txn_node);
                return root;
            }
            // Not TRANSACTION — it's SET GLOBAL var = expr
            // Fall through to variable assignment with scope
            AstNode* assignment = parse_variable_assignment(&scope_tok);
            if (assignment) root->add_child(assignment);
            // Parse remaining comma-separated assignments
            while (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                AstNode* next_assign = parse_variable_assignment(nullptr);
                if (next_assign) root->add_child(next_assign);
            }
            return root;
        }

        // PostgreSQL: SET LOCAL var = expr
        if constexpr (D == Dialect::PostgreSQL) {
            if (next.type == TokenType::TK_LOCAL) {
                Token scope_tok = tok_.next_token();
                AstNode* assignment = parse_variable_assignment(&scope_tok);
                if (assignment) root->add_child(assignment);
                return root;
            }
        }

        // SET var = expr [, var = expr, ...]
        AstNode* assignment = parse_variable_assignment(nullptr);
        if (assignment) root->add_child(assignment);
        while (tok_.peek().type == TokenType::TK_COMMA) {
            tok_.skip();
            AstNode* next_assign = parse_variable_assignment(nullptr);
            if (next_assign) root->add_child(next_assign);
        }

        return root;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;

    // SET NAMES charset [COLLATE collation]
    AstNode* parse_set_names() {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_NAMES);
        if (!node) return nullptr;

        // charset name or DEFAULT
        Token charset = tok_.next_token();
        node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, charset.text));

        // Optional COLLATE
        if (tok_.peek().type == TokenType::TK_COLLATE) {
            tok_.skip();
            Token collation = tok_.next_token();
            node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, collation.text));
        }
        return node;
    }

    // SET CHARACTER SET charset / SET CHARSET charset
    AstNode* parse_set_charset() {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_CHARSET);
        if (!node) return nullptr;

        Token charset = tok_.next_token();
        node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, charset.text));
        return node;
    }

    // SET [GLOBAL|SESSION] TRANSACTION ...
    AstNode* parse_set_transaction(StringRef scope) {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_TRANSACTION);
        if (!node) return nullptr;

        if (!scope.empty()) {
            node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, scope));
        }

        // ISOLATION LEVEL ... or READ ONLY/WRITE
        Token next = tok_.peek();
        if (next.type == TokenType::TK_ISOLATION) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_LEVEL) tok_.skip();

            // READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE
            Token level = tok_.next_token();
            if (level.type == TokenType::TK_READ) {
                Token sublevel = tok_.next_token();
                // Combine "READ COMMITTED" or "READ UNCOMMITTED"
                StringRef combined{level.text.ptr,
                    static_cast<uint32_t>((sublevel.text.ptr + sublevel.text.len) - level.text.ptr)};
                node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, combined));
            } else if (level.type == TokenType::TK_REPEATABLE) {
                Token read_tok = tok_.next_token(); // READ
                StringRef combined{level.text.ptr,
                    static_cast<uint32_t>((read_tok.text.ptr + read_tok.text.len) - level.text.ptr)};
                node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, combined));
            } else {
                // SERIALIZABLE
                node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, level.text));
            }
        } else if (next.type == TokenType::TK_READ) {
            tok_.skip();
            Token rw = tok_.next_token(); // ONLY or WRITE
            StringRef combined{next.text.ptr,
                static_cast<uint32_t>((rw.text.ptr + rw.text.len) - next.text.ptr)};
            node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, combined));
        }

        return node;
    }

    // Parse a single variable assignment: [scope] target = expr
    // scope_token is non-null if GLOBAL/SESSION/LOCAL was already consumed
    AstNode* parse_variable_assignment(const Token* scope_token) {
        AstNode* assignment = make_node(arena_, NodeType::NODE_VAR_ASSIGNMENT);
        if (!assignment) return nullptr;

        // Build the variable target
        AstNode* target = make_node(arena_, NodeType::NODE_VAR_TARGET);
        if (!target) return nullptr;

        if (scope_token) {
            target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, scope_token->text));
        }

        Token var = tok_.peek();
        if (var.type == TokenType::TK_AT) {
            // User variable @name
            tok_.skip();
            Token name = tok_.next_token();
            StringRef full{var.text.ptr,
                static_cast<uint32_t>((name.text.ptr + name.text.len) - var.text.ptr)};
            target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, full));
        } else if (var.type == TokenType::TK_DOUBLE_AT) {
            // System variable @@[scope.]name
            tok_.skip();
            Token name = tok_.next_token();
            StringRef full{var.text.ptr,
                static_cast<uint32_t>((name.text.ptr + name.text.len) - var.text.ptr)};
            // Check for @@scope.name
            if (tok_.peek().type == TokenType::TK_DOT) {
                tok_.skip();
                Token actual_name = tok_.next_token();
                full = StringRef{var.text.ptr,
                    static_cast<uint32_t>((actual_name.text.ptr + actual_name.text.len) - var.text.ptr)};
            }
            target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, full));
        } else {
            // Plain variable name
            Token name = tok_.next_token();
            target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
        }

        assignment->add_child(target);

        // Expect = or := (MySQL) or TO (PostgreSQL)
        Token eq = tok_.peek();
        if (eq.type == TokenType::TK_EQUAL || eq.type == TokenType::TK_COLON_EQUAL) {
            tok_.skip();
        } else if constexpr (D == Dialect::PostgreSQL) {
            if (eq.type == TokenType::TK_TO) {
                tok_.skip();
            }
        }

        // Parse RHS expression
        AstNode* rhs = expr_parser_.parse();
        if (rhs) assignment->add_child(rhs);

        return assignment;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_SET_PARSER_H
```

- [ ] **Step 3: Integrate SET parser into Parser class**

Modify `src/sql_parser/parser.cpp` — add includes for set_parser.h and expression_parser.h at the top of the file (these are implementation details, not public API, so they belong in the .cpp not the .h):

Add after `#include "sql_parser/parser.h"`:
```cpp
#include "sql_parser/expression_parser.h"
#include "sql_parser/set_parser.h"
```

Then replace the `parse_set()` stub:

Replace:
```cpp
template <Dialect D>
ParseResult Parser<D>::parse_set() {
    ParseResult r;
    r.status = ParseResult::PARTIAL;
    r.stmt_type = StmtType::SET;
    scan_to_end(r);
    return r;
}
```

With:
```cpp
template <Dialect D>
ParseResult Parser<D>::parse_set() {
    ParseResult r;
    r.stmt_type = StmtType::SET;

    SetParser<D> set_parser(tokenizer_, arena_);
    AstNode* ast = set_parser.parse();

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

- [ ] **Step 4: Build and run all tests**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: All tests pass including new SET tests.

- [ ] **Step 5: Commit**

```bash
git add include/sql_parser/set_parser.h include/sql_parser/parser.h src/sql_parser/parser.cpp tests/test_set.cpp
git commit -m "feat: add SET deep parser with full AST for all SET variants"
```

---

### Task 4: Verify existing classifier tests still pass and clean up

**Files:**
- No new files — verification and cleanup only

- [ ] **Step 1: Run full test suite**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: ALL tests pass, zero warnings.

- [ ] **Step 2: Verify SET classifier tests now return OK instead of PARTIAL**

The existing `ClassifySet` test in `test_classifier.cpp` checked for `stmt_type == StmtType::SET` but did not check `status`. The SET parser now returns `OK` instead of `PARTIAL`. Verify this doesn't break anything:

Run:
```bash
./run_tests --gtest_filter="*Set*"
```

- [ ] **Step 3: Check for compiler warnings**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all 2>&1 | grep -i warning
```
Expected: Zero warnings (or only from Google Test internals).

- [ ] **Step 4: Commit if any fixes were needed**

```bash
# Only if changes were made
git add -A && git commit -m "fix: clean up warnings and test compatibility after SET parser integration"
```

---

## What's Next

After this plan is complete, the following plans remain:

1. **Plan 3: SELECT Deep Parser** — Full SELECT parsing with FROM, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, locking. Uses the expression parser from this plan.
2. **Plan 4: Query Emitter** — AST → SQL reconstruction.
3. **Plan 5: Prepared Statement Cache** — Binary protocol support.
4. **Plan 6: Performance Benchmarks** — Validate latency targets.
