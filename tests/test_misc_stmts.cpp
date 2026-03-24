#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

// =====================================================================
// EXPLAIN tests (MySQL)
// =====================================================================

class MySQLExplainTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

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

TEST_F(MySQLExplainTest, ExplainSelect) {
    const char* sql = "EXPLAIN SELECT * FROM t";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_EXPLAIN_STMT);

    // Should have an inner SELECT stmt as child
    auto* inner = find_child(r.ast, NodeType::NODE_SELECT_STMT);
    EXPECT_NE(inner, nullptr);
}

TEST_F(MySQLExplainTest, ExplainAnalyzeSelect) {
    const char* sql = "EXPLAIN ANALYZE SELECT * FROM t WHERE id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);

    auto* opts = find_child(r.ast, NodeType::NODE_EXPLAIN_OPTIONS);
    EXPECT_NE(opts, nullptr);
}

TEST_F(MySQLExplainTest, ExplainFormatJson) {
    const char* sql = "EXPLAIN FORMAT = JSON SELECT * FROM t";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);

    auto* opts = find_child(r.ast, NodeType::NODE_EXPLAIN_OPTIONS);
    ASSERT_NE(opts, nullptr);
    // Should have FORMAT child
    auto* fmt = find_child(opts, NodeType::NODE_EXPLAIN_FORMAT);
    EXPECT_NE(fmt, nullptr);
}

TEST_F(MySQLExplainTest, ExplainInsert) {
    const char* sql = "EXPLAIN INSERT INTO t VALUES (1)";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);
    auto* inner = find_child(r.ast, NodeType::NODE_INSERT_STMT);
    EXPECT_NE(inner, nullptr);
}

TEST_F(MySQLExplainTest, ExplainUpdate) {
    const char* sql = "EXPLAIN UPDATE t SET x = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);
    auto* inner = find_child(r.ast, NodeType::NODE_UPDATE_STMT);
    EXPECT_NE(inner, nullptr);
}

TEST_F(MySQLExplainTest, ExplainDelete) {
    const char* sql = "EXPLAIN DELETE FROM t WHERE id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);
    auto* inner = find_child(r.ast, NodeType::NODE_DELETE_STMT);
    EXPECT_NE(inner, nullptr);
}

TEST_F(MySQLExplainTest, DescribeTable) {
    const char* sql = "DESCRIBE users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DESCRIBE);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_EXPLAIN_STMT);

    auto* tref = find_child(r.ast, NodeType::NODE_TABLE_REF);
    EXPECT_NE(tref, nullptr);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLExplainTest, DescTable) {
    const char* sql = "DESC users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DESCRIBE);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLExplainTest, ExplainTableShorthand) {
    const char* sql = "EXPLAIN users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

// ========== EXPLAIN round-trip ==========

TEST_F(MySQLExplainTest, RoundTripExplainSelect) {
    EXPECT_EQ(round_trip("EXPLAIN SELECT * FROM t"),
                         "EXPLAIN SELECT * FROM t");
}

TEST_F(MySQLExplainTest, RoundTripExplainAnalyze) {
    EXPECT_EQ(round_trip("EXPLAIN ANALYZE SELECT * FROM t"),
                         "EXPLAIN ANALYZE SELECT * FROM t");
}

TEST_F(MySQLExplainTest, RoundTripExplainFormat) {
    EXPECT_EQ(round_trip("EXPLAIN FORMAT = JSON SELECT * FROM t"),
                         "EXPLAIN FORMAT = JSON SELECT * FROM t");
}

TEST_F(MySQLExplainTest, RoundTripDescribe) {
    EXPECT_EQ(round_trip("DESCRIBE users"), "DESCRIBE users");
}

// =====================================================================
// EXPLAIN tests (PostgreSQL)
// =====================================================================

class PgSQLExplainTest : public ::testing::Test {
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

TEST_F(PgSQLExplainTest, ExplainParenthesizedOptions) {
    const char* sql = "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) SELECT * FROM t";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
    ASSERT_NE(r.ast, nullptr);

    auto* opts = find_child(r.ast, NodeType::NODE_EXPLAIN_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(PgSQLExplainTest, ExplainAnalyzeSelect) {
    const char* sql = "EXPLAIN ANALYZE SELECT * FROM t";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
}

// =====================================================================
// CALL tests (MySQL)
// =====================================================================

class MySQLCallTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    const AstNode* find_child(const AstNode* node, NodeType type) {
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == type) return c;
        }
        return nullptr;
    }

    int child_count(const AstNode* node) {
        int n = 0;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
        return n;
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

TEST_F(MySQLCallTest, CallNoArgs) {
    const char* sql = "CALL my_proc()";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::CALL);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_CALL_STMT);
    // First child is name, no more children = no args
    EXPECT_EQ(child_count(r.ast), 1);
}

TEST_F(MySQLCallTest, CallWithArgs) {
    const char* sql = "CALL my_proc(1, 'a', NOW())";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::CALL);
    ASSERT_NE(r.ast, nullptr);
    // name + 3 args = 4 children
    EXPECT_EQ(child_count(r.ast), 4);
}

TEST_F(MySQLCallTest, CallQualified) {
    const char* sql = "CALL schema1.my_proc(42)";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::CALL);
    ASSERT_NE(r.ast, nullptr);

    auto* qname = find_child(r.ast, NodeType::NODE_QUALIFIED_NAME);
    EXPECT_NE(qname, nullptr);
}

TEST_F(MySQLCallTest, RoundTripCallNoArgs) {
    EXPECT_EQ(round_trip("CALL my_proc()"), "CALL my_proc()");
}

TEST_F(MySQLCallTest, RoundTripCallWithArgs) {
    EXPECT_EQ(round_trip("CALL my_proc(1, 'a', NOW())"),
                         "CALL my_proc(1, 'a', NOW())");
}

TEST_F(MySQLCallTest, RoundTripCallQualified) {
    EXPECT_EQ(round_trip("CALL schema1.my_proc(42)"),
                         "CALL schema1.my_proc(42)");
}

// =====================================================================
// CALL tests (PostgreSQL)
// =====================================================================

class PgSQLCallTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

    std::string round_trip(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        return std::string(result.ptr, result.len);
    }
};

TEST_F(PgSQLCallTest, CallBasic) {
    const char* sql = "CALL my_proc()";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::CALL);
}

TEST_F(PgSQLCallTest, RoundTripCall) {
    EXPECT_EQ(round_trip("CALL my_proc(1, 2)"), "CALL my_proc(1, 2)");
}

// =====================================================================
// DO tests (MySQL only)
// =====================================================================

class MySQLDoTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    int child_count(const AstNode* node) {
        int n = 0;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
        return n;
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

TEST_F(MySQLDoTest, DoLiteral) {
    const char* sql = "DO 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DO_STMT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_DO_STMT);
    EXPECT_EQ(child_count(r.ast), 1);
}

TEST_F(MySQLDoTest, DoFunction) {
    const char* sql = "DO SLEEP(1)";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DO_STMT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(child_count(r.ast), 1);
}

TEST_F(MySQLDoTest, DoMultipleExprs) {
    const char* sql = "DO 1 + 2, RELEASE_LOCK('x')";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DO_STMT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(child_count(r.ast), 2);
}

TEST_F(MySQLDoTest, RoundTripDoLiteral) {
    EXPECT_EQ(round_trip("DO 1"), "DO 1");
}

TEST_F(MySQLDoTest, RoundTripDoFunction) {
    EXPECT_EQ(round_trip("DO SLEEP(1)"), "DO SLEEP(1)");
}

TEST_F(MySQLDoTest, RoundTripDoMultiple) {
    EXPECT_EQ(round_trip("DO 1 + 2, RELEASE_LOCK('x')"),
                         "DO 1 + 2, RELEASE_LOCK('x')");
}

// =====================================================================
// LOAD DATA tests (MySQL only)
// =====================================================================

class MySQLLoadDataTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

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

TEST_F(MySQLLoadDataTest, BasicLoadData) {
    const char* sql = "LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::LOAD_DATA);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_LOAD_DATA_STMT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLLoadDataTest, LoadDataLocal) {
    const char* sql = "LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::LOAD_DATA);
    ASSERT_NE(r.ast, nullptr);

    auto* opts = find_child(r.ast, NodeType::NODE_LOAD_DATA_OPTIONS);
    EXPECT_NE(opts, nullptr);
}

TEST_F(MySQLLoadDataTest, LoadDataFieldsTerminated) {
    const char* sql = "LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users FIELDS TERMINATED BY ',' ENCLOSED BY '\"'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::LOAD_DATA);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLLoadDataTest, LoadDataReplace) {
    const char* sql = "LOAD DATA INFILE '/tmp/data.csv' REPLACE INTO TABLE users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::LOAD_DATA);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLLoadDataTest, RoundTripBasic) {
    EXPECT_EQ(round_trip("LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users"),
                         "LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users");
}

TEST_F(MySQLLoadDataTest, RoundTripFieldsTerminated) {
    EXPECT_EQ(round_trip("LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users FIELDS TERMINATED BY ','"),
                         "LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users FIELDS TERMINATED BY ','");
}

TEST_F(MySQLLoadDataTest, RoundTripReplace) {
    EXPECT_EQ(round_trip("LOAD DATA INFILE '/tmp/data.csv' REPLACE INTO TABLE users"),
                         "LOAD DATA INFILE '/tmp/data.csv' REPLACE INTO TABLE users");
}

TEST_F(MySQLLoadDataTest, RoundTripLocal) {
    EXPECT_EQ(round_trip("LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE users"),
                         "LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE users");
}

// =====================================================================
// Bulk data-driven tests
// =====================================================================

struct MiscStmtTestCase {
    const char* sql;
    StmtType expected_type;
    ParseResult::Status expected_status;
};

class MySQLMiscStmtBulk : public ::testing::TestWithParam<MiscStmtTestCase> {
protected:
    Parser<Dialect::MySQL> parser;
};

static MiscStmtTestCase mysql_misc_cases[] = {
    // EXPLAIN
    {"EXPLAIN SELECT * FROM t", StmtType::EXPLAIN, ParseResult::OK},
    {"EXPLAIN ANALYZE SELECT * FROM t", StmtType::EXPLAIN, ParseResult::OK},
    {"EXPLAIN FORMAT = JSON SELECT * FROM t", StmtType::EXPLAIN, ParseResult::OK},
    {"EXPLAIN FORMAT = TREE SELECT * FROM t", StmtType::EXPLAIN, ParseResult::OK},
    {"EXPLAIN INSERT INTO t VALUES (1)", StmtType::EXPLAIN, ParseResult::OK},
    {"EXPLAIN UPDATE t SET x = 1", StmtType::EXPLAIN, ParseResult::OK},
    {"EXPLAIN DELETE FROM t WHERE id = 1", StmtType::EXPLAIN, ParseResult::OK},
    {"EXPLAIN users", StmtType::EXPLAIN, ParseResult::OK},
    // DESCRIBE / DESC
    {"DESCRIBE users", StmtType::DESCRIBE, ParseResult::OK},
    {"DESC users", StmtType::DESCRIBE, ParseResult::OK},
    {"DESCRIBE mydb.users", StmtType::DESCRIBE, ParseResult::OK},
    // CALL
    {"CALL my_proc()", StmtType::CALL, ParseResult::OK},
    {"CALL my_proc(1, 'a', NOW())", StmtType::CALL, ParseResult::OK},
    {"CALL schema1.my_proc(42)", StmtType::CALL, ParseResult::OK},
    // DO
    {"DO 1", StmtType::DO_STMT, ParseResult::OK},
    {"DO SLEEP(1)", StmtType::DO_STMT, ParseResult::OK},
    {"DO 1 + 2, RELEASE_LOCK('x')", StmtType::DO_STMT, ParseResult::OK},
    // LOAD DATA
    {"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users", StmtType::LOAD_DATA, ParseResult::OK},
    {"LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE users", StmtType::LOAD_DATA, ParseResult::OK},
    {"LOAD DATA INFILE '/tmp/data.csv' REPLACE INTO TABLE users", StmtType::LOAD_DATA, ParseResult::OK},
};

TEST_P(MySQLMiscStmtBulk, ClassifyAndParse) {
    auto& tc = GetParam();
    auto r = parser.parse(tc.sql, strlen(tc.sql));
    EXPECT_EQ(r.stmt_type, tc.expected_type) << "SQL: " << tc.sql;
    EXPECT_EQ(r.status, tc.expected_status) << "SQL: " << tc.sql;
    if (tc.expected_status == ParseResult::OK) {
        EXPECT_NE(r.ast, nullptr) << "SQL: " << tc.sql;
    }
}

INSTANTIATE_TEST_SUITE_P(
    MiscStatements, MySQLMiscStmtBulk,
    ::testing::ValuesIn(mysql_misc_cases));
