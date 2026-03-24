# Query Digest / Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a query digest module that normalizes SQL queries (literals to `?`, IN list collapsing, keyword uppercasing) and produces a 64-bit hash for query rules matching. Works for all statement types: AST-based for Tier 1 statements, token-level fallback for Tier 2/unknown statements.

**Architecture:** The digest system has two paths. For Tier 1 statements with a full AST, the existing `Emitter<D>` is extended with an `EmitMode::DIGEST` flag that changes how literals, IN lists, and keywords are emitted. For Tier 2 statements (or parse failures), a token-level walker normalizes directly from the token stream. Both paths produce a normalized string and a 64-bit FNV-1a hash. The `Digest<D>` class provides the public API, wrapping both paths behind `compute(ast)` and `compute(sql, len)` methods.

**Tech Stack:** C++17, existing parser infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-tier1-promotions-and-digest-design.md`

---

## Scope

This plan builds:
1. `DigestResult` struct with normalized string and 64-bit hash
2. `EmitMode::DIGEST` flag on `Emitter<D>` with modified emit behavior
3. Token-level digest fallback for Tier 2 statements
4. FNV-1a hash computation (incremental)
5. `Digest<D>` public API class
6. Comprehensive tests: same-query-different-literals, IN collapsing, cross-tier consistency

**Closes:** #9

**Dependencies:** Benefits from Plans 7-10 being complete (more Tier 1 statements to test AST-based digest), but works independently via token-level fallback for any statement type.

---

## File Structure

```
include/sql_parser/
    digest.h              — (create) Digest<D> class, DigestResult, FNV-1a hash
    emitter.h             — (modify) add EmitMode enum, modify literal/in-list emission

tests/
    test_digest.cpp       — (create) digest tests

Makefile.new              — (modify) add test_digest.cpp to TEST_SRCS
```

---

### Task 1: DigestResult and FNV-1a Hash

**Files:**
- Create: `include/sql_parser/digest.h`

- [ ] **Step 1: Define DigestResult and hash function**

Create `include/sql_parser/digest.h`:
```cpp
#ifndef SQL_PARSER_DIGEST_H
#define SQL_PARSER_DIGEST_H

#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/emitter.h"
#include "sql_parser/string_builder.h"
#include <cstdint>

namespace sql_parser {

struct DigestResult {
    StringRef normalized;  // "SELECT * FROM t WHERE id = ?"
    uint64_t hash;         // 64-bit FNV-1a hash
};

// FNV-1a 64-bit hash — simple, fast, no external dependency
struct FnvHash {
    static constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
    static constexpr uint64_t FNV_PRIME = 1099511628211ULL;

    uint64_t state = FNV_OFFSET_BASIS;

    void update(const char* data, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            state ^= static_cast<uint64_t>(static_cast<uint8_t>(data[i]));
            state *= FNV_PRIME;
        }
    }

    void update_char(char c) {
        state ^= static_cast<uint64_t>(static_cast<uint8_t>(c));
        state *= FNV_PRIME;
    }

    uint64_t finish() const { return state; }
};

template <Dialect D>
class Digest {
public:
    explicit Digest(Arena& arena) : arena_(arena) {}

    // From a parsed AST (Tier 1) — uses Emitter in DIGEST mode
    DigestResult compute(const AstNode* ast);

    // From raw SQL (works for any statement) — uses token-level fallback
    DigestResult compute(const char* sql, size_t len);

private:
    Arena& arena_;

    // Token-level digest: walk tokens, normalize, hash
    DigestResult compute_token_level(const char* sql, size_t len);
};

} // namespace sql_parser

#endif // SQL_PARSER_DIGEST_H
```

---

### Task 2: Emitter Digest Mode

**Files:**
- Modify: `include/sql_parser/emitter.h`

- [ ] **Step 1: Add EmitMode enum and constructor parameter**

Add to `emitter.h`:
```cpp
enum class EmitMode : uint8_t { NORMAL, DIGEST };
```

Update the `Emitter` constructor:
```cpp
explicit Emitter(Arena& arena, EmitMode mode = EmitMode::NORMAL,
                 const ParamBindings* bindings = nullptr)
    : sb_(arena), bindings_(bindings), placeholder_index_(0), mode_(mode) {}
```

Add `EmitMode mode_` as a private member.

- [ ] **Step 2: Modify emit_value for literals in DIGEST mode**

In `emit_node()`, change literal handling:
```cpp
case NodeType::NODE_LITERAL_INT:
case NodeType::NODE_LITERAL_FLOAT:
    if (mode_ == EmitMode::DIGEST) { sb_.append_char('?'); break; }
    emit_value(node); break;

case NodeType::NODE_LITERAL_STRING:
    if (mode_ == EmitMode::DIGEST) { sb_.append_char('?'); break; }
    emit_string_literal(node); break;
```

- [ ] **Step 3: Modify emit_in_list for DIGEST mode**

In `emit_in_list()`, when `mode_ == EmitMode::DIGEST`:
```cpp
void emit_in_list(const AstNode* node) {
    const AstNode* expr = node->first_child;
    if (expr) emit_node(expr);
    sb_.append(" IN (");
    if (mode_ == EmitMode::DIGEST) {
        sb_.append_char('?');
    } else {
        bool first = true;
        for (const AstNode* val = expr ? expr->next_sibling : nullptr; val; val = val->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(val);
        }
    }
    sb_.append_char(')');
}
```

- [ ] **Step 4: Keyword uppercasing in DIGEST mode**

All keywords are already emitted as literals (e.g., `sb_.append("SELECT ")`) which are uppercase. Identifiers from the input that happen to be keywords are emitted via `emit_value()` which preserves original case. For digest mode, the token-level path handles uppercasing. For AST-based digest, since the emitter already uses uppercase keyword strings in `emit_*` methods, this is mostly free. The only concern is node values that contain lowercase keywords (e.g., a `join` type stored as-is). For full correctness, `emit_value()` in DIGEST mode should uppercase keyword-type nodes, but this can be deferred as the emitter's structural methods already use uppercase constants.

---

### Task 3: Token-Level Digest Implementation

**Files:**
- Modify: `include/sql_parser/digest.h`

- [ ] **Step 1: Implement token-level digest**

Add the `compute_token_level()` implementation to `digest.h`:

```cpp
template <Dialect D>
DigestResult Digest<D>::compute_token_level(const char* sql, size_t len) {
    Tokenizer<D> tok;
    tok.reset(sql, len);
    StringBuilder sb(arena_);
    FnvHash hasher;
    bool first = true;
    bool in_list_context = false;
    bool last_was_placeholder = false;

    while (true) {
        Token t = tok.next_token();
        if (t.type == TokenType::TK_EOF) break;
        if (t.type == TokenType::TK_SEMICOLON) break;

        // Determine what to emit for this token
        // ... (see implementation notes below)
    }

    StringRef normalized = sb.finish();
    hasher.update(normalized.ptr, normalized.len);
    return DigestResult{normalized, hasher.finish()};
}
```

Key logic for each token type:
- **TK_INTEGER, TK_FLOAT, TK_STRING**: emit `?`. If inside an IN list and the previous emitted token was also `?`, skip (IN list collapsing).
- **Keywords**: emit uppercase form. Use the token's text but uppercased.
- **TK_IDENTIFIER**: emit as-is.
- **TK_COMMA in IN context**: if collapsing, skip the comma too.
- **All other tokens**: emit as-is with single space separation.

Track `in_list_context` by watching for `IN` followed by `(`. Reset when `)` is seen.

- [ ] **Step 2: Implement AST-based compute**

```cpp
template <Dialect D>
DigestResult Digest<D>::compute(const AstNode* ast) {
    Emitter<D> emitter(arena_, EmitMode::DIGEST);
    emitter.emit(ast);
    StringRef normalized = emitter.result();
    FnvHash hasher;
    hasher.update(normalized.ptr, normalized.len);
    return DigestResult{normalized, hasher.finish()};
}
```

- [ ] **Step 3: Implement raw SQL compute (with auto-detection)**

```cpp
template <Dialect D>
DigestResult Digest<D>::compute(const char* sql, size_t len) {
    // Always use token-level for the raw SQL path.
    // Callers with a parsed AST should use compute(ast) directly.
    return compute_token_level(sql, len);
}
```

Note: The `compute(sql, len)` overload is for convenience when no AST is available. Callers that have already parsed can use `compute(ast)` for potentially better normalization (AST-aware IN collapsing, correct VALUES row handling).

---

### Task 4: Comprehensive Tests

**Files:**
- Create: `tests/test_digest.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Write digest tests**

Create `tests/test_digest.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/digest.h"

using namespace sql_parser;

class MySQLDigestTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    DigestResult digest_ast(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        Digest<Dialect::MySQL> digest(parser.arena());
        if (r.ast) {
            return digest.compute(r.ast);
        }
        // Fallback to token-level
        return digest.compute(sql, strlen(sql));
    }

    DigestResult digest_token(const char* sql) {
        Digest<Dialect::MySQL> digest(parser.arena());
        return digest.compute(sql, strlen(sql));
    }

    std::string normalized(const char* sql) {
        auto d = digest_ast(sql);
        return std::string(d.normalized.ptr, d.normalized.len);
    }

    std::string normalized_token(const char* sql) {
        auto d = digest_token(sql);
        return std::string(d.normalized.ptr, d.normalized.len);
    }
};

// ========== Literal normalization ==========

TEST_F(MySQLDigestTest, IntegerLiteralNormalized) {
    std::string out = normalized("SELECT * FROM t WHERE id = 42");
    EXPECT_EQ(out, "SELECT * FROM t WHERE id = ?");
}

TEST_F(MySQLDigestTest, FloatLiteralNormalized) {
    std::string out = normalized("SELECT * FROM t WHERE price > 3.14");
    EXPECT_EQ(out, "SELECT * FROM t WHERE price > ?");
}

TEST_F(MySQLDigestTest, StringLiteralNormalized) {
    std::string out = normalized("SELECT * FROM t WHERE name = 'Alice'");
    EXPECT_EQ(out, "SELECT * FROM t WHERE name = ?");
}

TEST_F(MySQLDigestTest, MultipleLiteralsNormalized) {
    std::string out = normalized("SELECT * FROM t WHERE a = 1 AND b = 'x' AND c = 3.14");
    EXPECT_EQ(out, "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?");
}

// ========== Same query, different literals => same hash ==========

TEST_F(MySQLDigestTest, SameQueryDifferentInts) {
    auto d1 = digest_ast("SELECT * FROM t WHERE id = 1");
    auto d2 = digest_ast("SELECT * FROM t WHERE id = 999");
    EXPECT_EQ(d1.hash, d2.hash);
}

TEST_F(MySQLDigestTest, SameQueryDifferentStrings) {
    auto d1 = digest_ast("SELECT * FROM t WHERE name = 'Alice'");
    auto d2 = digest_ast("SELECT * FROM t WHERE name = 'Bob'");
    EXPECT_EQ(d1.hash, d2.hash);
}

TEST_F(MySQLDigestTest, DifferentQueriesDifferentHash) {
    auto d1 = digest_ast("SELECT * FROM t WHERE id = 1");
    auto d2 = digest_ast("SELECT * FROM t WHERE name = 1");
    EXPECT_NE(d1.hash, d2.hash);
}

TEST_F(MySQLDigestTest, DifferentTablesDifferentHash) {
    auto d1 = digest_ast("SELECT * FROM users WHERE id = 1");
    auto d2 = digest_ast("SELECT * FROM orders WHERE id = 1");
    EXPECT_NE(d1.hash, d2.hash);
}

// ========== IN list collapsing ==========

TEST_F(MySQLDigestTest, InListCollapsed) {
    std::string out = normalized("SELECT * FROM t WHERE id IN (1, 2, 3)");
    EXPECT_EQ(out, "SELECT * FROM t WHERE id IN (?)");
}

TEST_F(MySQLDigestTest, InListDifferentSizesSameHash) {
    auto d1 = digest_ast("SELECT * FROM t WHERE id IN (1, 2, 3)");
    auto d2 = digest_ast("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5)");
    EXPECT_EQ(d1.hash, d2.hash);
}

TEST_F(MySQLDigestTest, InListSingleValueSameHash) {
    auto d1 = digest_ast("SELECT * FROM t WHERE id IN (1)");
    auto d2 = digest_ast("SELECT * FROM t WHERE id IN (1, 2, 3)");
    EXPECT_EQ(d1.hash, d2.hash);
}

// ========== Keyword uppercasing ==========

TEST_F(MySQLDigestTest, KeywordsUppercased) {
    // Token-level digest should uppercase keywords
    std::string out = normalized_token("select * from t where id = 1");
    EXPECT_EQ(out, "SELECT * FROM t WHERE id = ?");
}

// ========== Token-level fallback for Tier 2 ==========

TEST_F(MySQLDigestTest, TokenLevelInsert) {
    std::string out = normalized_token("INSERT INTO users (name) VALUES ('Alice')");
    EXPECT_EQ(out, "INSERT INTO users (name) VALUES (?)");
}

TEST_F(MySQLDigestTest, TokenLevelUpdate) {
    std::string out = normalized_token("UPDATE users SET name = 'Bob' WHERE id = 42");
    EXPECT_EQ(out, "UPDATE users SET name = ? WHERE id = ?");
}

TEST_F(MySQLDigestTest, TokenLevelDelete) {
    std::string out = normalized_token("DELETE FROM users WHERE id = 1");
    EXPECT_EQ(out, "DELETE FROM users WHERE id = ?");
}

TEST_F(MySQLDigestTest, TokenLevelCreateTable) {
    // Tier 2 statement -- should still normalize literals
    std::string out = normalized_token("CREATE TABLE t (id INT DEFAULT 0)");
    EXPECT_EQ(out, "CREATE TABLE t (id INT DEFAULT ?)");
}

TEST_F(MySQLDigestTest, TokenLevelInCollapsing) {
    std::string out = normalized_token("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5)");
    EXPECT_EQ(out, "SELECT * FROM t WHERE id IN (?)");
}

// ========== Consistency: AST-based and token-level produce same hash ==========

TEST_F(MySQLDigestTest, AstAndTokenLevelConsistentForSelect) {
    const char* sql = "SELECT * FROM users WHERE id = 42";
    auto d_ast = digest_ast(sql);
    auto d_tok = digest_token(sql);
    // The normalized strings should be the same
    EXPECT_EQ(
        std::string(d_ast.normalized.ptr, d_ast.normalized.len),
        std::string(d_tok.normalized.ptr, d_tok.normalized.len)
    );
    // Therefore hashes should match
    EXPECT_EQ(d_ast.hash, d_tok.hash);
}

// ========== SET statement digest ==========

TEST_F(MySQLDigestTest, SetVariableDigest) {
    auto d1 = digest_ast("SET autocommit = 1");
    auto d2 = digest_ast("SET autocommit = 0");
    // SET with different values should produce same digest
    EXPECT_EQ(d1.hash, d2.hash);
}

// ========== NULL and boolean literals ==========

TEST_F(MySQLDigestTest, NullPreserved) {
    // NULL is not a literal value — it's a keyword, should not be replaced with ?
    std::string out = normalized("SELECT * FROM t WHERE a IS NULL");
    EXPECT_EQ(out, "SELECT * FROM t WHERE a IS NULL");
}

TEST_F(MySQLDigestTest, LimitDigest) {
    auto d1 = digest_ast("SELECT * FROM t LIMIT 10");
    auto d2 = digest_ast("SELECT * FROM t LIMIT 20");
    EXPECT_EQ(d1.hash, d2.hash);
}

// ========== Placeholder passthrough ==========

TEST_F(MySQLDigestTest, PlaceholderPassthrough) {
    std::string out = normalized_token("SELECT * FROM t WHERE id = ?");
    EXPECT_EQ(out, "SELECT * FROM t WHERE id = ?");
}

// ========== Bulk digest tests ==========

struct DigestTestCase {
    const char* sql1;
    const char* sql2;
    bool same_hash;
    const char* description;
};

static const DigestTestCase digest_bulk_cases[] = {
    {"SELECT * FROM t WHERE id = 1", "SELECT * FROM t WHERE id = 2", true, "different int literals"},
    {"SELECT * FROM t WHERE s = 'a'", "SELECT * FROM t WHERE s = 'b'", true, "different string literals"},
    {"SELECT * FROM t WHERE x = 1.5", "SELECT * FROM t WHERE x = 2.7", true, "different float literals"},
    {"SELECT * FROM t WHERE id IN (1,2)", "SELECT * FROM t WHERE id IN (1,2,3,4)", true, "in list sizes"},
    {"SELECT * FROM t LIMIT 10", "SELECT * FROM t LIMIT 100", true, "different limits"},
    {"SELECT * FROM t1 WHERE id = 1", "SELECT * FROM t2 WHERE id = 1", false, "different tables"},
    {"SELECT a FROM t WHERE id = 1", "SELECT b FROM t WHERE id = 1", false, "different columns"},
    {"SELECT * FROM t WHERE a = 1", "SELECT * FROM t WHERE b = 1", false, "different where cols"},
    {"SELECT * FROM t ORDER BY a", "SELECT * FROM t ORDER BY b", false, "different order"},
};

TEST(MySQLDigestBulk, HashConsistency) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : digest_bulk_cases) {
        auto r1 = parser.parse(tc.sql1, strlen(tc.sql1));
        Digest<Dialect::MySQL> d1(parser.arena());
        auto dr1 = r1.ast ? d1.compute(r1.ast) : d1.compute(tc.sql1, strlen(tc.sql1));

        auto r2 = parser.parse(tc.sql2, strlen(tc.sql2));
        Digest<Dialect::MySQL> d2(parser.arena());
        auto dr2 = r2.ast ? d2.compute(r2.ast) : d2.compute(tc.sql2, strlen(tc.sql2));

        if (tc.same_hash) {
            EXPECT_EQ(dr1.hash, dr2.hash)
                << "Expected same hash: " << tc.description
                << "\n  SQL1: " << tc.sql1 << "\n  SQL2: " << tc.sql2
                << "\n  Norm1: " << std::string(dr1.normalized.ptr, dr1.normalized.len)
                << "\n  Norm2: " << std::string(dr2.normalized.ptr, dr2.normalized.len);
        } else {
            EXPECT_NE(dr1.hash, dr2.hash)
                << "Expected different hash: " << tc.description
                << "\n  SQL1: " << tc.sql1 << "\n  SQL2: " << tc.sql2;
        }
    }
}

// ========== PostgreSQL digest ==========

class PgSQLDigestTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

    DigestResult digest_token(const char* sql) {
        Digest<Dialect::PostgreSQL> digest(parser.arena());
        return digest.compute(sql, strlen(sql));
    }

    std::string normalized_token(const char* sql) {
        auto d = digest_token(sql);
        return std::string(d.normalized.ptr, d.normalized.len);
    }
};

TEST_F(PgSQLDigestTest, BasicDigest) {
    std::string out = normalized_token("SELECT * FROM users WHERE id = 42");
    EXPECT_EQ(out, "SELECT * FROM users WHERE id = ?");
}

TEST_F(PgSQLDigestTest, DollarPlaceholderPreserved) {
    std::string out = normalized_token("SELECT * FROM users WHERE id = $1");
    EXPECT_EQ(out, "SELECT * FROM users WHERE id = $1");
}

TEST_F(PgSQLDigestTest, InListCollapsed) {
    std::string out = normalized_token("SELECT * FROM t WHERE id IN (1, 2, 3)");
    EXPECT_EQ(out, "SELECT * FROM t WHERE id IN (?)");
}

TEST_F(PgSQLDigestTest, ReturningDigest) {
    std::string out = normalized_token("INSERT INTO t (a) VALUES (1) RETURNING *");
    EXPECT_EQ(out, "INSERT INTO t (a) VALUES (?) RETURNING *");
}
```

- [ ] **Step 2: Add test_digest.cpp to Makefile.new**

Add `$(TEST_DIR)/test_digest.cpp \` to the `TEST_SRCS` list.

- [ ] **Step 3: Run all tests**

```bash
make -f Makefile.new test
```

---

### Task 5: Edge Cases and Robustness

- [ ] **Step 1: Handle VALUES rows in digest mode**

For INSERT ... VALUES, the AST-based digest should normalize all value expressions to `?` but preserve the row structure. Multi-row INSERTs should collapse to a single row in the digest: `INSERT INTO t (a,b) VALUES (?,?)` regardless of how many rows the original had. This is the ProxySQL convention.

Implementation: In `emit_values_clause()`, when `mode_ == EmitMode::DIGEST`, emit only the first `NODE_VALUES_ROW` child and skip the rest.

- [ ] **Step 2: Ensure whitespace normalization**

Both AST-based and token-level digest must produce single-space-separated output with no leading/trailing whitespace. The existing `StringBuilder` and emitter already produce well-formed output, but verify in tests.

- [ ] **Step 3: Verify hash stability**

FNV-1a is deterministic. Add a test that computes a digest for a known query and asserts the exact hash value to catch accidental changes:

```cpp
TEST_F(MySQLDigestTest, HashStability) {
    auto d = digest_ast("SELECT * FROM users WHERE id = 1");
    // The normalized form is "SELECT * FROM users WHERE id = ?"
    // FNV-1a of that string should always be the same
    EXPECT_NE(d.hash, 0ULL);
    // Store the computed hash and assert it in future test runs
    // (exact value depends on implementation, fill in after first run)
}
```

- [ ] **Step 4: Run all tests**

```bash
make -f Makefile.new test
```
