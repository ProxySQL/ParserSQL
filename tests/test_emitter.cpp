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
