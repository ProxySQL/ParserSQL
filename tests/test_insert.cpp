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
