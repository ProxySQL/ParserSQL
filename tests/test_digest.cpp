#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/digest.h"

using namespace sql_parser;

// Helper struct to hold digest results as stable std::string + hash
struct StableDigest {
    std::string normalized;
    uint64_t hash;
};

class MySQLDigestTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    // AST-based digest (parses SQL, invalidates previous arena allocations)
    StableDigest digest_ast(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        Digest<Dialect::MySQL> digest(parser.arena());
        DigestResult dr;
        if (r.ast) {
            dr = digest.compute(r.ast);
        } else {
            dr = digest.compute(sql, strlen(sql));
        }
        return StableDigest{std::string(dr.normalized.ptr, dr.normalized.len), dr.hash};
    }

    // Token-level digest (uses arena but does NOT call parse, so arena is stable
    // within a single call but may be invalidated by subsequent parse calls)
    StableDigest digest_token(const char* sql) {
        parser.reset();
        Digest<Dialect::MySQL> digest(parser.arena());
        auto dr = digest.compute(sql, strlen(sql));
        return StableDigest{std::string(dr.normalized.ptr, dr.normalized.len), dr.hash};
    }

    std::string normalized(const char* sql) {
        return digest_ast(sql).normalized;
    }

    std::string normalized_token(const char* sql) {
        return digest_token(sql).normalized;
    }
};

// ========== Literal normalization ==========

TEST_F(MySQLDigestTest, IntegerLiteralNormalized) {
    EXPECT_EQ(normalized("SELECT * FROM t WHERE id = 42"),
              "SELECT * FROM t WHERE id = ?");
}

TEST_F(MySQLDigestTest, FloatLiteralNormalized) {
    EXPECT_EQ(normalized("SELECT * FROM t WHERE price > 3.14"),
              "SELECT * FROM t WHERE price > ?");
}

TEST_F(MySQLDigestTest, StringLiteralNormalized) {
    EXPECT_EQ(normalized("SELECT * FROM t WHERE name = 'Alice'"),
              "SELECT * FROM t WHERE name = ?");
}

TEST_F(MySQLDigestTest, MultipleLiteralsNormalized) {
    EXPECT_EQ(normalized("SELECT * FROM t WHERE a = 1 AND b = 'x' AND c = 3.14"),
              "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?");
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
    EXPECT_EQ(normalized("SELECT * FROM t WHERE id IN (1, 2, 3)"),
              "SELECT * FROM t WHERE id IN (?)");
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
    EXPECT_EQ(normalized_token("select * from t where id = 1"),
              "SELECT * FROM t WHERE id = ?");
}

// ========== Token-level fallback for Tier 2 ==========

TEST_F(MySQLDigestTest, TokenLevelInsert) {
    EXPECT_EQ(normalized_token("INSERT INTO users (name) VALUES ('Alice')"),
              "INSERT INTO users (name) VALUES (?)");
}

TEST_F(MySQLDigestTest, TokenLevelUpdate) {
    EXPECT_EQ(normalized_token("UPDATE users SET name = 'Bob' WHERE id = 42"),
              "UPDATE users SET name = ? WHERE id = ?");
}

TEST_F(MySQLDigestTest, TokenLevelDelete) {
    EXPECT_EQ(normalized_token("DELETE FROM users WHERE id = 1"),
              "DELETE FROM users WHERE id = ?");
}

TEST_F(MySQLDigestTest, TokenLevelCreateTable) {
    EXPECT_EQ(normalized_token("CREATE TABLE t (id INT DEFAULT 0)"),
              "CREATE TABLE t (id INT DEFAULT ?)");
}

TEST_F(MySQLDigestTest, TokenLevelInCollapsing) {
    EXPECT_EQ(normalized_token("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5)"),
              "SELECT * FROM t WHERE id IN (?)");
}

// ========== SET statement digest ==========

TEST_F(MySQLDigestTest, SetVariableDigest) {
    auto d1 = digest_ast("SET autocommit = 1");
    auto d2 = digest_ast("SET autocommit = 0");
    EXPECT_EQ(d1.hash, d2.hash);
}

// ========== NULL and boolean literals ==========

TEST_F(MySQLDigestTest, NullPreserved) {
    EXPECT_EQ(normalized("SELECT * FROM t WHERE a IS NULL"),
              "SELECT * FROM t WHERE a IS NULL");
}

TEST_F(MySQLDigestTest, LimitDigest) {
    auto d1 = digest_ast("SELECT * FROM t LIMIT 10");
    auto d2 = digest_ast("SELECT * FROM t LIMIT 20");
    EXPECT_EQ(d1.hash, d2.hash);
}

// ========== Placeholder passthrough ==========

TEST_F(MySQLDigestTest, PlaceholderPassthrough) {
    EXPECT_EQ(normalized_token("SELECT * FROM t WHERE id = ?"),
              "SELECT * FROM t WHERE id = ?");
}

// ========== Hash stability ==========

TEST_F(MySQLDigestTest, HashStability) {
    auto d1 = digest_ast("SELECT * FROM users WHERE id = 1");
    EXPECT_NE(d1.hash, 0ULL);
    auto d2 = digest_ast("SELECT * FROM users WHERE id = 42");
    EXPECT_EQ(d1.hash, d2.hash);
}

// ========== Consistency: AST-based and token-level produce same hash ==========

TEST_F(MySQLDigestTest, AstAndTokenLevelConsistentForSimpleSelect) {
    auto d_ast = digest_ast("SELECT * FROM users WHERE id = 42");
    auto d_tok = digest_token("SELECT * FROM users WHERE id = 42");
    EXPECT_EQ(d_ast.normalized, d_tok.normalized);
    EXPECT_EQ(d_ast.hash, d_tok.hash);
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
        // Parse and digest first query, copy results
        auto r1 = parser.parse(tc.sql1, strlen(tc.sql1));
        Digest<Dialect::MySQL> d1(parser.arena());
        auto dr1 = r1.ast ? d1.compute(r1.ast) : d1.compute(tc.sql1, strlen(tc.sql1));
        std::string norm1(dr1.normalized.ptr, dr1.normalized.len);
        uint64_t hash1 = dr1.hash;

        // Parse and digest second query
        auto r2 = parser.parse(tc.sql2, strlen(tc.sql2));
        Digest<Dialect::MySQL> d2(parser.arena());
        auto dr2 = r2.ast ? d2.compute(r2.ast) : d2.compute(tc.sql2, strlen(tc.sql2));
        std::string norm2(dr2.normalized.ptr, dr2.normalized.len);
        uint64_t hash2 = dr2.hash;

        if (tc.same_hash) {
            EXPECT_EQ(hash1, hash2)
                << "Expected same hash: " << tc.description
                << "\n  SQL1: " << tc.sql1 << "\n  SQL2: " << tc.sql2
                << "\n  Norm1: " << norm1
                << "\n  Norm2: " << norm2;
        } else {
            EXPECT_NE(hash1, hash2)
                << "Expected different hash: " << tc.description
                << "\n  SQL1: " << tc.sql1 << "\n  SQL2: " << tc.sql2;
        }
    }
}

// ========== INSERT digest (AST-based) ==========

TEST_F(MySQLDigestTest, InsertDigestNormalized) {
    EXPECT_EQ(normalized("INSERT INTO t (a, b) VALUES (1, 'hello')"),
              "INSERT INTO t (a, b) VALUES (?, ?)");
}

TEST_F(MySQLDigestTest, InsertMultiRowCollapsed) {
    auto d1 = digest_ast("INSERT INTO t (a) VALUES (1)");
    auto d2 = digest_ast("INSERT INTO t (a) VALUES (1), (2), (3)");
    EXPECT_EQ(d1.normalized, d2.normalized);
    EXPECT_EQ(d1.hash, d2.hash);
}

// ========== PostgreSQL digest ==========

class PgSQLDigestTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

    StableDigest digest_token(const char* sql) {
        parser.reset();
        Digest<Dialect::PostgreSQL> digest(parser.arena());
        auto dr = digest.compute(sql, strlen(sql));
        return StableDigest{std::string(dr.normalized.ptr, dr.normalized.len), dr.hash};
    }

    std::string normalized_token(const char* sql) {
        return digest_token(sql).normalized;
    }
};

TEST_F(PgSQLDigestTest, BasicDigest) {
    EXPECT_EQ(normalized_token("SELECT * FROM users WHERE id = 42"),
              "SELECT * FROM users WHERE id = ?");
}

TEST_F(PgSQLDigestTest, DollarPlaceholderPreserved) {
    EXPECT_EQ(normalized_token("SELECT * FROM users WHERE id = $1"),
              "SELECT * FROM users WHERE id = $1");
}

TEST_F(PgSQLDigestTest, InListCollapsed) {
    EXPECT_EQ(normalized_token("SELECT * FROM t WHERE id IN (1, 2, 3)"),
              "SELECT * FROM t WHERE id IN (?)");
}

TEST_F(PgSQLDigestTest, ReturningDigest) {
    EXPECT_EQ(normalized_token("INSERT INTO t (a) VALUES (1) RETURNING *"),
              "INSERT INTO t (a) VALUES (?) RETURNING *");
}

// ========== Token-level digest for various Tier 2 statements ==========

TEST_F(MySQLDigestTest, TokenLevelGrant) {
    std::string out = normalized_token("GRANT SELECT ON db.* TO 'user'@'host'");
    EXPECT_EQ(out, "GRANT SELECT ON db.* TO ?@?");
}

TEST_F(MySQLDigestTest, TokenLevelDropTable) {
    EXPECT_EQ(normalized_token("DROP TABLE IF EXISTS t"),
              "DROP TABLE IF EXISTS t");
}

// ========== Token-level VALUES collapsing ==========

TEST_F(MySQLDigestTest, TokenLevelValuesMultiRowCollapsed) {
    EXPECT_EQ(normalized_token("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')"),
              "INSERT INTO t (a, b) VALUES (?, ?)");
}

TEST_F(MySQLDigestTest, TokenLevelValuesMultiRowSameHash) {
    auto d1 = digest_token("INSERT INTO t (a) VALUES (1)");
    auto d2 = digest_token("INSERT INTO t (a) VALUES (1), (2), (3)");
    EXPECT_EQ(d1.hash, d2.hash);
}
