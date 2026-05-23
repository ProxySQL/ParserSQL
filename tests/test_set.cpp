#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include <cstring>

using namespace sql_parser;

// ============================================================================
// Data-driven test infrastructure
// ============================================================================

struct SetTestCase {
    const char* sql;
    const char* description;
};

// ============================================================================
// MySQL SET bulk test cases — all should parse successfully
// ============================================================================

static const SetTestCase mysql_set_cases[] = {
    // --- sql_mode variants ---
    {"SET @@sql_mode = 'TRADITIONAL'", "sql_mode with @@ prefix"},
    {"SET SESSION sql_mode = 'TRADITIONAL'", "sql_mode with SESSION scope"},
    {"SET @@session.sql_mode = 'TRADITIONAL'", "sql_mode with @@session. prefix"},
    {"SET @@local.sql_mode = 'TRADITIONAL'", "sql_mode with @@local. prefix"},
    {"SET sql_mode = 'TRADITIONAL'", "sql_mode unqualified"},
    {"SET SQL_MODE   ='TRADITIONAL'", "sql_mode with extra spaces before ="},
    {"SET SQL_MODE  = \"TRADITIONAL\"", "sql_mode with double-quoted value"},
    {"SET SQL_MODE  = TRADITIONAL", "sql_mode with unquoted value"},
    {"set sql_mode = IFNULL(NULL,\"STRICT_TRANS_TABLES\")", "sql_mode with IFNULL function call"},
    {"set sql_mode = IFNULL(NULL,'STRICT_TRANS_TABLES')", "sql_mode with IFNULL single-quoted"},
    {"SET @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ', STRICT_ALL_TABLES'), ', NO_AUTO_VALUE_ON_ZERO')", "sql_mode with nested CONCAT"},
    {"SET @@LOCAL.sql_mode = CONCAT(CONCAT(@@sql_mode, ', STRICT_ALL_TABLES'), ', NO_AUTO_VALUE_ON_ZERO')", "sql_mode with nested CONCAT via LOCAL"},
    {"set session sql_mode = 'ONLY_FULL_GROUP_BY'", "sql_mode lowercase session"},
    {"SET sql_mode = 'NO_ZERO_DATE,STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY'", "sql_mode comma-separated modes in string"},
    {"SET @@sql_mode = CONCAT(@@sql_mode, ',', 'ONLY_FULL_GROUP_BY')", "sql_mode CONCAT with 3 args"},
    {"SET @@sql_mode = REPLACE(REPLACE(REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY,', ''),',ONLY_FULL_GROUP_BY', ''),'ONLY_FULL_GROUP_BY', '')", "sql_mode deeply nested REPLACE"},
    {"SET @@sql_mode = REPLACE( REPLACE( REPLACE( @@sql_mode, 'ONLY_FULL_GROUP_BY,', ''),',ONLY_FULL_GROUP_BY', ''),'ONLY_FULL_GROUP_BY', '')", "sql_mode deeply nested REPLACE with spaces"},
    {"SET SQL_MODE=IFNULL(@@sql_mode,'')", "sql_mode IFNULL with @@sysvar no spaces"},
    {"SET SQL_MODE=IFNULL(@old_sql_mode,'')", "sql_mode IFNULL with user variable"},
    {"SET SQL_MODE=IFNULL(@OLD_SQL_MODE,'')", "sql_mode IFNULL with uppercase user variable"},
    {"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION'))", "sql_mode with subquery"},
    {"SET sql_mode=''", "sql_mode empty string"},

    // --- time_zone variants ---
    {"SET @@time_zone = 'Europe/Paris'", "time_zone 2-component"},
    {"SET @@time_zone = '+00:00'", "time_zone numeric offset"},
    {"SET @@time_zone = \"Europe/Paris\"", "time_zone double-quoted"},
    {"SET @@time_zone = \"+00:00\"", "time_zone numeric offset double-quoted"},
    {"SET @@TIME_ZONE := 'SYSTEM'", "time_zone colon-equal"},
    {"SET time_zone := 'SYSTEM'", "time_zone unqualified colon-equal"},
    {"SET time_zone = 'UTC'", "time_zone UTC"},
    {"SET time_zone = SYSTEM", "time_zone unquoted SYSTEM"},
    {"SET time_zone = UTC", "time_zone unquoted UTC"},
    {"SET time_zone = 'America/Argentina/Buenos_Aires'", "time_zone 3-component"},
    {"SET time_zone = 'America/Indiana/Indianapolis'", "time_zone 3-component Indianapolis"},
    {"SET time_zone = \"America/Kentucky/Louisville\"", "time_zone 3-component double-quoted"},
    {"SET time_zone = 'America/Port-au-Prince'", "time_zone with hyphens"},
    {"SET time_zone = 'America/Blanc-Sablon'", "time_zone with hyphen"},
    {"SET time_zone = \"US/East-Indiana\"", "time_zone with hyphen double-quoted"},
    {"SET time_zone = '+08:00'", "time_zone +08:00"},
    {"SET time_zone = '-05:30'", "time_zone -05:30"},
    {"SET time_zone = '-10:00'", "time_zone -10:00"},
    {"SET @@time_zone = @OLD_TIME_ZONE", "time_zone user variable RHS"},
    {"SET @@TIME_ZONE = @OLD_TIME_ZONE", "time_zone uppercase user variable RHS"},

    // --- NAMES / CHARSET ---
    {"SET NAMES utf8", "NAMES unquoted"},
    {"SET NAMES 'utf8'", "NAMES single-quoted"},
    {"SET NAMES \"utf8\"", "NAMES double-quoted"},
    {"SET NAMES utf8 COLLATE unicode_ci", "NAMES with COLLATE"},
    {"SET NAMES DEFAULT", "NAMES DEFAULT"},

    // --- CHARACTER SET / CHARSET ---
    {"SET CHARACTER SET utf8", "CHARACTER SET unquoted"},
    {"SET CHARACTER SET 'utf8'", "CHARACTER SET single-quoted"},
    {"SET CHARSET utf8", "CHARSET unquoted"},
    {"SET CHARSET 'latin1'", "CHARSET single-quoted"},

    // --- Session/Global scope ---
    {"SET @@SESSION.SQL_SELECT_LIMIT= DEFAULT", "SESSION SQL_SELECT_LIMIT DEFAULT"},
    {"SET @@LOCAL.SQL_SELECT_LIMIT= DEFAULT", "LOCAL SQL_SELECT_LIMIT DEFAULT"},
    {"SET @@SQL_SELECT_LIMIT= DEFAULT", "@@ SQL_SELECT_LIMIT DEFAULT"},
    {"SET SESSION SQL_SELECT_LIMIT   = DEFAULT", "SESSION keyword SQL_SELECT_LIMIT DEFAULT"},
    {"SET @@SESSION.SQL_SELECT_LIMIT= 1234", "SESSION SQL_SELECT_LIMIT number"},
    {"SET @@LOCAL.SQL_SELECT_LIMIT= 1234", "LOCAL SQL_SELECT_LIMIT number"},
    {"SET @@SQL_SELECT_LIMIT= 1234", "@@ SQL_SELECT_LIMIT number"},
    {"SET SESSION SQL_SELECT_LIMIT   = 1234", "SESSION keyword SQL_SELECT_LIMIT number"},
    {"SET @@SESSION.SQL_SELECT_LIMIT= @old_sql_select_limit", "SESSION SQL_SELECT_LIMIT user var"},
    {"SET @@LOCAL.SQL_SELECT_LIMIT= @old_sql_select_limit", "LOCAL SQL_SELECT_LIMIT user var"},
    {"SET SQL_SELECT_LIMIT= @old_sql_select_limit", "SQL_SELECT_LIMIT user var"},
    {"SET GLOBAL max_connections = 100", "GLOBAL max_connections"},
    {"SET @@SESSION.sql_auto_is_null = 0", "SESSION sql_auto_is_null"},
    {"SET @@LOCAL.sql_auto_is_null = 0", "LOCAL sql_auto_is_null"},
    {"SET SESSION sql_auto_is_null = 1", "SESSION keyword sql_auto_is_null"},
    {"SET sql_auto_is_null = OFF", "sql_auto_is_null OFF"},
    {"SET @@sql_auto_is_null = ON", "sql_auto_is_null ON"},
    {"SET @@SESSION.sql_safe_updates = 0", "SESSION sql_safe_updates"},
    {"SET @@LOCAL.sql_safe_updates = 0", "LOCAL sql_safe_updates"},
    {"SET SESSION sql_safe_updates = 1", "SESSION keyword sql_safe_updates"},
    {"SET SQL_SAFE_UPDATES = OFF", "SQL_SAFE_UPDATES OFF"},
    {"SET @@sql_safe_updates = ON", "sql_safe_updates ON"},

    // --- session_track_gtids ---
    {"SET @@session_track_gtids = OFF", "session_track_gtids OFF"},
    {"SET @@session_track_gtids = OWN_GTID", "session_track_gtids OWN_GTID"},
    {"SET @@SESSION.session_track_gtids = OWN_GTID", "SESSION session_track_gtids OWN_GTID"},
    {"SET @@LOCAL.session_track_gtids = OWN_GTID", "LOCAL session_track_gtids OWN_GTID"},
    {"SET SESSION session_track_gtids = OWN_GTID", "SESSION keyword session_track_gtids OWN_GTID"},
    {"SET @@session_track_gtids = ALL_GTIDS", "session_track_gtids ALL_GTIDS"},

    // --- character_set_results ---
    {"SET @@character_set_results = utf8", "character_set_results utf8"},
    {"SET @@character_set_results = NULL", "character_set_results NULL"},
    {"SET character_set_results = NULL", "character_set_results NULL unqualified"},
    {"SET @@session.character_set_results = NULL", "session.character_set_results NULL"},
    {"SET @@local.character_set_results = NULL", "local.character_set_results NULL"},
    {"SET session character_set_results = NULL", "session keyword character_set_results NULL"},

    // --- Transaction ---
    {"SET session transaction read only", "SESSION TRANSACTION READ ONLY"},
    {"SET session transaction read write", "SESSION TRANSACTION READ WRITE"},
    {"SET session transaction isolation level READ COMMITTED", "SESSION TRANSACTION READ COMMITTED"},
    {"SET session transaction isolation level READ UNCOMMITTED", "SESSION TRANSACTION READ UNCOMMITTED"},
    {"SET session transaction isolation level REPEATABLE READ", "SESSION TRANSACTION REPEATABLE READ"},
    {"SET session transaction isolation level SERIALIZABLE", "SESSION TRANSACTION SERIALIZABLE"},
    {"SET TRANSACTION READ ONLY", "TRANSACTION READ ONLY no scope"},
    {"SET TRANSACTION READ WRITE", "TRANSACTION READ WRITE no scope"},
    {"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", "TRANSACTION ISOLATION LEVEL REPEATABLE READ"},
    {"SET GLOBAL TRANSACTION READ WRITE", "GLOBAL TRANSACTION READ WRITE"},

    // --- Multiple variables (comma-separated) ---
    {"SET time_zone = 'Europe/Paris', sql_mode = 'TRADITIONAL'", "multi: timezone + sql_mode"},
    {"SET time_zone = 'Europe/Paris', sql_mode = IFNULL(NULL,\"STRICT_TRANS_TABLES\")", "multi: timezone + sql_mode IFNULL"},
    {"SET time_zone = 'America/Argentina/Buenos_Aires', sql_mode = 'TRADITIONAL'", "multi: 3-component timezone + sql_mode"},
    {"SET  @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'),  @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483", "multi: 3 session vars with nested CONCAT"},
    {"set autocommit=1, sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')", "multi: autocommit + sql_mode concat"},
    {"SET autocommit = 1, wait_timeout = 28800", "multi: autocommit + wait_timeout"},
    {"SET character_set_connection=utf8,character_set_results=utf8,character_set_client=binary", "multi: 3 charset vars unquoted no spaces"},

    // --- User variables ---
    {"SET @my_var = 42", "user variable numeric"},
    {"SET @old_sql_mode = 'TRADITIONAL'", "user variable string"},
    {"SET @x = 1 + 2", "user variable expression"},
    {"SET @x := 42", "user variable colon-equal"},

    // --- Optimizer switch ---
    {"SET optimizer_switch='index_merge=on,index_merge_union=off'", "optimizer_switch single-quoted"},

    // --- Multi-statement (semicolon) ---
    {"SET autocommit = 0; BEGIN", "multi-statement SET + BEGIN"},

    // --- Special RHS values ---
    {"SET character_set_results = NULL", "NULL RHS"},
    {"SET sql_log_bin=1", "sql_log_bin 1"},
    {"SET sql_log_bin=0", "sql_log_bin 0"},
    {"SET wait_timeout = 28800", "large number RHS"},
    {"SET max_join_size=18446744073709551615", "uint64 max RHS"},
};

// Cases involving NAMES in the middle of multi-SET (comma-separated).
// The current set_parser.h dispatches NAMES only at the top level,
// so these are expected to fail parsing correctly.
static const SetTestCase mysql_names_in_multi_set_cases[] = {
    {"SET sql_mode = 'TRADITIONAL', NAMES 'utf8' COLLATE 'unicode_ci'", "multi: sql_mode + NAMES COLLATE"},
    {"SET NAMES utf8, @@SESSION.sql_mode = CONCAT(REPLACE(REPLACE(REPLACE(@@sql_mode, 'STRICT_TRANS_TABLES', ''), 'STRICT_ALL_TABLES', ''), 'TRADITIONAL', ''), ',NO_AUTO_VALUE_ON_ZERO'), @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 3600", "multi: NAMES + 3 assignments"},
    {"set autocommit=1, session_track_schema=1, sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES'), @@SESSION.net_write_timeout=7200", "multi: 4 vars including session"},
    {"SET character_set_results=NULL, NAMES latin7, character_set_client='utf8mb4'", "multi: NULL + NAMES in middle + assignment"},
    {"SET character_set_results=NULL,NAMES latin7,character_set_client='utf8mb4'", "multi: NULL + NAMES in middle no spaces"},
    {"set character_set_results=null, names latin7, character_set_client='utf8mb4'", "multi: lowercase null + names in middle"},
    {"set character_set_results=null,names latin7,character_set_client='utf8mb4'", "multi: lowercase null + names no spaces"},
    {"SET @@autocommit := 0 , NAMES \"utf8mb3\"", "multi: colon-equal + NAMES"},
    {"SET character_set_results=NULL,NAMES latin7,character_set_client='utf8mb4', autocommit := 1 , time_zone = 'Europe/Paris'", "multi: 5 vars with NAMES in middle"},
};

// Cases involving backtick-quoted variable names.
static const SetTestCase mysql_backtick_cases[] = {
    {"SET `group_concat_max_len`=4096", "backtick-quoted variable name"},
    {"SET `sql_select_limit`=3030", "backtick-quoted sql_select_limit"},
    {"SET `tx_isolation`='READ-COMMITTED', `group_concat_max_len`=4096", "backtick-quoted multi-var"},
};

// Cases involving backtick-quoted values.
static const SetTestCase mysql_backtick_value_cases[] = {
    {"SET optimizer_switch=`index_merge=OFF`", "backtick-quoted value for optimizer_switch"},
};

// Multi-statement with multiple SETs separated by semicolons.
static const SetTestCase mysql_multi_statement_cases[] = {
    {"SET sql_select_limit=3030, session_track_gtids=OWN_GTID; SET max_join_size=10000;", "multi-statement double SET"},
};

// ============================================================================
// PostgreSQL SET test cases
// ============================================================================

static const SetTestCase pgsql_set_cases[] = {
    {"SET client_encoding TO 'UTF8'", "PG client_encoding TO"},
    {"SET work_mem = '256MB'", "PG work_mem"},
    {"SET LOCAL timezone = 'UTC'", "PG LOCAL timezone"},
    {"SET NAMES 'UTF8'", "PG NAMES"},
    {"SET search_path TO public, extensions", "PG search_path TO list"},
    // PG SET TIME ZONE — two-keyword alias for SET TimeZone = <value>.
    {"SET TIME ZONE 'UTC'",                  "PG SET TIME ZONE string literal"},
    {"SET TIME ZONE DEFAULT",                "PG SET TIME ZONE DEFAULT"},
    {"SET TIME ZONE '+05:30'",               "PG SET TIME ZONE numeric offset literal"},
    {"SET TIME ZONE LOCAL",                  "PG SET TIME ZONE LOCAL"},
    {"set time zone 'UTC'",                  "PG SET TIME ZONE lowercase"},
    // PG SET multi-value list — single variable, commas are value continuation.
    {"SET search_path TO 'a', 'b'",          "PG search_path two-value list via TO"},
    {"SET search_path = 'a', 'b', 'c'",      "PG search_path three-value list via ="},
    {"SET search_path TO \"$user\", public", "PG search_path with quoted identifier + plain identifier"},
    {"SET datestyle TO 'ISO', 'mdy'",        "PG datestyle two-value list"},
};

// ============================================================================
// MySQL bulk test: all standard cases parse successfully
// ============================================================================

TEST(MySQLSetBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_set_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::SET)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ============================================================================
// MySQL: NAMES in multi-SET (may fail if parser doesn't support it)
// ============================================================================

TEST(MySQLSetBulk, NamesInMultiSetCases) {
    Parser<Dialect::MySQL> parser;
    int pass_count = 0;
    int fail_count = 0;
    for (const auto& tc : mysql_names_in_multi_set_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        if (r.status == ParseResult::OK && r.ast != nullptr && r.stmt_type == StmtType::SET) {
            pass_count++;
        } else {
            fail_count++;
            // Use non-fatal to report but not block
            ADD_FAILURE() << "NAMES-in-multi-SET not supported: " << tc.description
                          << "\n  SQL: " << tc.sql;
        }
    }
    std::cout << "[  INFO   ] NAMES-in-multi-SET: " << pass_count << " passed, "
              << fail_count << " failed (parser gap)" << std::endl;
}

// ============================================================================
// MySQL: backtick-quoted variable names
// ============================================================================

TEST(MySQLSetBulk, BacktickQuotedVariableNames) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_backtick_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::SET)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ============================================================================
// MySQL: backtick-quoted values
// ============================================================================

TEST(MySQLSetBulk, BacktickQuotedValues) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_backtick_value_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::SET)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ============================================================================
// MySQL: multi-statement SETs
// ============================================================================

TEST(MySQLSetBulk, MultiStatementSets) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_multi_statement_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::SET)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_TRUE(r.has_remaining())
            << "Expected remaining SQL: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ============================================================================
// PostgreSQL bulk test
// ============================================================================

TEST(PgSQLSetBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_set_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::SET)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ============================================================================
// Individual structural tests — MySQL
// ============================================================================

class MySQLSetTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

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
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_VAR_ASSIGNMENT);
}

TEST_F(MySQLSetTest, SetMultipleVariables) {
    auto r = parser.parse("SET autocommit = 1, wait_timeout = 28800", 41);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(child_count(r.ast), 2);
}

TEST_F(MySQLSetTest, SetThreeVariables) {
    const char* sql = "SET character_set_connection=utf8,character_set_results=utf8,character_set_client=binary";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(child_count(r.ast), 3);
}

TEST_F(MySQLSetTest, SetGlobalVariable) {
    auto r = parser.parse("SET GLOBAL max_connections = 100", 31);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
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

TEST_F(MySQLSetTest, SetSessionTransactionReadOnly) {
    const char* sql = "SET session transaction read only";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_TRANSACTION);
}

TEST_F(MySQLSetTest, SetSessionTransactionReadWrite) {
    const char* sql = "SET session transaction read write";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_TRANSACTION);
}

TEST_F(MySQLSetTest, SetTransactionIsolationReadCommitted) {
    const char* sql = "SET session transaction isolation level READ COMMITTED";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetTransactionIsolationReadUncommitted) {
    const char* sql = "SET session transaction isolation level READ UNCOMMITTED";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetTransactionIsolationSerializable) {
    const char* sql = "SET session transaction isolation level SERIALIZABLE";
    auto r = parser.parse(sql, strlen(sql));
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

TEST_F(MySQLSetTest, SetSubqueryRHS) {
    const char* sql = "SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION'))";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    // The assignment should have a subquery node in its RHS
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    EXPECT_EQ(assignment->type, NodeType::NODE_VAR_ASSIGNMENT);
}

TEST_F(MySQLSetTest, SetFunctionCallRHS) {
    const char* sql = "set sql_mode = IFNULL(NULL,'STRICT_TRANS_TABLES')";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    // RHS should be a function call node (second child of assignment)
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    AstNode* rhs = target->next_sibling;
    ASSERT_NE(rhs, nullptr);
    EXPECT_EQ(rhs->type, NodeType::NODE_FUNCTION_CALL);
}

TEST_F(MySQLSetTest, SetNestedFunctionCallRHS) {
    const char* sql = "SET @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ', STRICT_ALL_TABLES'), ', NO_AUTO_VALUE_ON_ZERO')";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    AstNode* rhs = target->next_sibling;
    ASSERT_NE(rhs, nullptr);
    EXPECT_EQ(rhs->type, NodeType::NODE_FUNCTION_CALL);
}

TEST_F(MySQLSetTest, SetDeeplyNestedReplace) {
    const char* sql = "SET @@sql_mode = REPLACE(REPLACE(REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY,', ''),',ONLY_FULL_GROUP_BY', ''),'ONLY_FULL_GROUP_BY', '')";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetNullRHS) {
    const char* sql = "SET character_set_results = NULL";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    AstNode* rhs = target->next_sibling;
    ASSERT_NE(rhs, nullptr);
    EXPECT_EQ(rhs->type, NodeType::NODE_LITERAL_NULL);
}

TEST_F(MySQLSetTest, SetEmptyStringRHS) {
    const char* sql = "SET sql_mode=''";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    AstNode* rhs = target->next_sibling;
    ASSERT_NE(rhs, nullptr);
    EXPECT_EQ(rhs->type, NodeType::NODE_LITERAL_STRING);
}

TEST_F(MySQLSetTest, SetDefaultRHS) {
    const char* sql = "SET @@SESSION.SQL_SELECT_LIMIT= DEFAULT";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    AstNode* rhs = target->next_sibling;
    ASSERT_NE(rhs, nullptr);
    EXPECT_EQ(rhs->type, NodeType::NODE_IDENTIFIER);
}

TEST_F(MySQLSetTest, SetUserVariableRHS) {
    const char* sql = "SET @@time_zone = @OLD_TIME_ZONE";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    AstNode* rhs = target->next_sibling;
    ASSERT_NE(rhs, nullptr);
    EXPECT_EQ(rhs->type, NodeType::NODE_COLUMN_REF);
}

// ============================================================================
// Individual structural tests — PostgreSQL
// ============================================================================

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

TEST_F(PgSQLSetTest, SetSearchPathToList) {
    const char* sql = "SET search_path TO public, extensions";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ----------------------------------------------------------------------------
// PG SET TIME ZONE — two-keyword alias for SET TimeZone = <value>.
// Regression coverage: before the fix, the parser treated TIME as a normal
// identifier (variable name "time") and ZONE as the value, so the assignment
// emitted was `time = ZONE` and the rest of the statement was discarded.
// ----------------------------------------------------------------------------

namespace {

// Helper: walk to the variable-target identifier node, return its StringRef.
inline StringRef set_var_name(const AstNode* root) {
    if (!root || !root->first_child) return {};
    const AstNode* assignment = root->first_child;
    if (!assignment || !assignment->first_child) return {};
    const AstNode* target = assignment->first_child;
    if (!target || !target->first_child) return {};
    return target->first_child->value();
}

// Helper: count children of a node.
inline int child_count(const AstNode* node) {
    int n = 0;
    for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
    return n;
}

} // namespace

TEST_F(PgSQLSetTest, SetTimeZoneStringLiteral) {
    const char* sql = "SET TIME ZONE 'UTC'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_SET_STMT);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    EXPECT_EQ(assignment->type, NodeType::NODE_VAR_ASSIGNMENT);
    EXPECT_TRUE(set_var_name(r.ast).equals_ci("timezone", 8))
        << "Expected variable name 'timezone' (PG alias for SET TIME ZONE)";
    AstNode* target = assignment->first_child;
    ASSERT_NE(target->next_sibling, nullptr) << "Missing RHS value";
}

TEST_F(PgSQLSetTest, SetTimeZoneDefault) {
    const char* sql = "SET TIME ZONE DEFAULT";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_TRUE(set_var_name(r.ast).equals_ci("timezone", 8));
}

TEST_F(PgSQLSetTest, SetTimeZoneNumericOffset) {
    const char* sql = "SET TIME ZONE '+05:30'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_TRUE(set_var_name(r.ast).equals_ci("timezone", 8));
}

TEST_F(PgSQLSetTest, SetTimeZoneLocal) {
    const char* sql = "SET TIME ZONE LOCAL";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_TRUE(set_var_name(r.ast).equals_ci("timezone", 8));
}

TEST_F(PgSQLSetTest, SetTimeZoneLowercase) {
    const char* sql = "set time zone 'UTC'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_TRUE(set_var_name(r.ast).equals_ci("timezone", 8));
}

// ----------------------------------------------------------------------------
// PG SET multi-value list — PG SET is single-variable; commas after the
// first value are continuation of the value list, NOT new variable
// assignments. Per PG docs:
//   SET configuration_parameter { TO | = } { value | 'value' | DEFAULT }
//   "Some configuration parameters take a list of values, such as
//    search_path and datestyle."
// Regression coverage: before the fix, the parser treated each comma as a
// new assignment, so the second value was tested as a new variable target
// without `=` and silently dropped.
// ----------------------------------------------------------------------------

TEST_F(PgSQLSetTest, SetSearchPathMultiValueTo) {
    const char* sql = "SET search_path TO 'a', 'b'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    EXPECT_EQ(assignment->type, NodeType::NODE_VAR_ASSIGNMENT);
    EXPECT_EQ(child_count(assignment), 3)
        << "Expected target + 2 value children under the same assignment";
    EXPECT_EQ(assignment->next_sibling, nullptr)
        << "PG comma after first value is list continuation, not a new assignment";
}

TEST_F(PgSQLSetTest, SetSearchPathMultiValueEq) {
    const char* sql = "SET search_path = 'a', 'b', 'c'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    EXPECT_EQ(child_count(assignment), 4) << "target + 3 values";
    EXPECT_EQ(assignment->next_sibling, nullptr);
}

TEST_F(PgSQLSetTest, SetSearchPathQuotedIdentifierAndPlain) {
    // The customer-facing failure shape from pgsql-set_parameter_validation_test-t.
    const char* sql = "SET search_path TO \"$user\", public";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    EXPECT_EQ(child_count(assignment), 3) << "target + 2 values";
}

TEST_F(PgSQLSetTest, SetDatestyleMultiValue) {
    const char* sql = "SET datestyle TO 'ISO', 'mdy'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    EXPECT_EQ(child_count(assignment), 3) << "target + 2 values";
}

// ============================================================================
// Invalid syntax should return PARTIAL, not OK (issue #36)
// ============================================================================

TEST(MySQLSetBulk, InvalidSyntaxReturnsError) {
    // Updated from the prior PARTIAL expectation: parse_set() now
    // distinguishes truncated / malformed input from genuine parse
    // failures and surfaces the former as ParseResult::ERROR. This
    // matches the semantics consumers actually want (lock_hostgroup
    // or hard-fail on clearly-bad SQL).
    Parser<Dialect::MySQL> parser;
    struct { const char* sql; int expected_status; } cases[] = {
        {"SET",                        (int)ParseResult::ERROR},
        {"SET ;",                      (int)ParseResult::ERROR},
        {"SET GLOBAL",                 (int)ParseResult::ERROR},
    };
    for (const auto& tc : cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ((int)r.status, tc.expected_status)
            << "SQL: " << tc.sql;
    }
}

TEST(MySQLSetBulk, LenientAcceptsUnusualSyntax) {
    Parser<Dialect::MySQL> parser;
    struct { const char* sql; int expected_status; } cases[] = {
        {"SET @@@ BROKEN",             (int)ParseResult::OK},
        {"SET NAMES DEFAULT",          (int)ParseResult::OK},
        {"SET NAMES utf8, autocommit=1", (int)ParseResult::OK},
        {"SET NAMES utf8 COLLATE utf8_bin, autocommit=1", (int)ParseResult::OK},
        {"SET a=1, NAMES utf8, b=2",  (int)ParseResult::OK},
        {"SET CHARACTER SET utf8, autocommit=1", (int)ParseResult::OK},
        {"SET CHARSET utf8, NAMES latin1", (int)ParseResult::OK},
        {"SET @@global.autocommit=1, @@session.sql_mode='TRADITIONAL'", (int)ParseResult::OK},
        {"SET a=1,,b=2",              (int)ParseResult::OK},
        {"SET NAMES utf8, broken",    (int)ParseResult::OK},
    };
    for (const auto& tc : cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ((int)r.status, tc.expected_status)
            << "SQL: " << tc.sql;
    }
}

// ============================================================================
// Coverage for the ProxySQL set_parser_algorithm=3 audit (issues P1..P6).
// ============================================================================

// Helper: walk to first VAR_ASSIGNMENT's VAR_TARGET, then to its single
// IDENTIFIER child (single-target case). Returns nullptr if shape differs.
static const AstNode* first_target_identifier(const AstNode* set_stmt) {
    if (!set_stmt || set_stmt->type != NodeType::NODE_SET_STMT) return nullptr;
    const AstNode* va = set_stmt->first_child;
    if (!va || va->type != NodeType::NODE_VAR_ASSIGNMENT) return nullptr;
    const AstNode* vt = va->first_child;
    if (!vt || vt->type != NodeType::NODE_VAR_TARGET) return nullptr;
    const AstNode* id = vt->first_child;
    if (!id || id->type != NodeType::NODE_IDENTIFIER) return nullptr;
    return id;
}

// Helper: get the RHS value node of the first VAR_ASSIGNMENT.
static const AstNode* first_value(const AstNode* set_stmt) {
    if (!set_stmt || set_stmt->type != NodeType::NODE_SET_STMT) return nullptr;
    const AstNode* va = set_stmt->first_child;
    if (!va || va->type != NodeType::NODE_VAR_ASSIGNMENT) return nullptr;
    const AstNode* vt = va->first_child;
    if (!vt) return nullptr;
    return vt->next_sibling;
}

// ----------------------------------------------------------------------------
// P6: MySQL SET @@`var`, SET @`var`, SET @@`scope`.`var` -- keep full name,
// canonical form drops the backticks (matching the plain `SET `var`` path
// which already produced IDENTIFIER [var] with no delimiters).
// ----------------------------------------------------------------------------
TEST(MySQLSetP6, DoubleAtBacktickedNameKeepsFullText) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET @@`wait_timeout`=28801";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "@@wait_timeout");
}

TEST(MySQLSetP6, AtBacktickedNameKeepsFullText) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET @`my_var`=1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "@my_var");
}

TEST(MySQLSetP6, DoubleAtScopedBacktickedNameKeepsFullText) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET @@`session`.`sql_mode`='TRADITIONAL'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "@@session.sql_mode");
}

TEST(MySQLSetP6, DoubleAtScopedMixedDelimitersKeepsFullText) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET @@session.`sql_mode`='TRADITIONAL'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "@@session.sql_mode");
}

// ----------------------------------------------------------------------------
// P3: PG SET SCHEMA name -- shorthand for SET search_path TO name.
// ----------------------------------------------------------------------------
TEST(PgSQLSetP3, SetSchemaQuotedString) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET SCHEMA 'public'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "search_path");
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(std::string(v->value_ptr, v->value_len), "public");
}

TEST(PgSQLSetP3, SetSchemaUnquoted) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET SCHEMA public";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "search_path");
}

// ----------------------------------------------------------------------------
// P4: PG SET SEED N -- emit synthetic VAR_TARGET[seed] (lowercased), not
// VAR_TARGET[SEED] which downstream consumers misclassify as an unknown GUC.
// ----------------------------------------------------------------------------
TEST(PgSQLSetP4, SetSeedFloat) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET SEED 0.5";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "seed");
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_LITERAL_FLOAT);
}

// ----------------------------------------------------------------------------
// P1: FLAG_IDENT_DELIMITED set iff RHS identifier was source-delimited.
// ----------------------------------------------------------------------------
TEST(PgSQLSetP1, DoubleQuotedRhsIdentifierIsFlagged) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = \"public\"";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_COLUMN_REF);
    EXPECT_TRUE(v->flags & FLAG_IDENT_DELIMITED) << "DELIMITED flag should be set";
    EXPECT_EQ(std::string(v->value_ptr, v->value_len), "public");
}

TEST(PgSQLSetP1, UnquotedRhsIdentifierIsNotFlagged) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = public";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_COLUMN_REF);
    EXPECT_FALSE(v->flags & FLAG_IDENT_DELIMITED);
}

TEST(MySQLSetP1, BackticksOnRhsIdentifierAreFlagged) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET sql_mode = `STRICT_ALL_TABLES`";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_COLUMN_REF);
    EXPECT_TRUE(v->flags & FLAG_IDENT_DELIMITED);
}

// ----------------------------------------------------------------------------
// P2: bare PG $word in SET value position must be ERROR, not PARTIAL.
// Quoted forms `'$user'` and `"$user"` remain valid.
// ----------------------------------------------------------------------------
TEST(PgSQLSetP2, BareDollarWordIsError) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = $user";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

TEST(PgSQLSetP2, SingleQuotedDollarUserStillOk) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = '$user'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(std::string(v->value_ptr, v->value_len), "$user");
}

TEST(PgSQLSetP2, DoubleQuotedDollarUserStillOkAndFlagged) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = \"$user\"";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_COLUMN_REF);
    EXPECT_TRUE(v->flags & FLAG_IDENT_DELIMITED);
    EXPECT_EQ(std::string(v->value_ptr, v->value_len), "$user");
}

TEST(PgSQLSetP2, NumericPlaceholderStillOk) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT * FROM t WHERE c = $1";
    auto r = parser.parse(sql, strlen(sql));
    // Not a SET, but make sure the tokenizer hasn't regressed $N placeholders.
    EXPECT_NE(r.status, ParseResult::ERROR);
}

// PostgreSQL identifiers can contain $ after the first character (per
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS).
// Earlier versions of the tokenizer stopped at $, so `schema$1` parsed as
// the identifier `schema` followed by the placeholder `$1`, which was then
// truncated/rejected downstream.
TEST(PgSQLSetP2, DollarInUnquotedIdentIsContinuation) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = schema$1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(std::string(v->value_ptr, v->value_len), "schema$1");
}

TEST(PgSQLSetP2, DollarInMiddleOfIdent) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = my$schema$2_name";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(std::string(v->value_ptr, v->value_len), "my$schema$2_name");
}

TEST(PgSQLSetP2, DollarAtStartStillEmitsError) {
    // `$word` (dollar followed by non-digit) is reserved and must still
    // emit TK_ERROR. Only mid-identifier `$` becomes a continuation char.
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = $bareword";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

// MySQL still disallows $ in unquoted identifiers — the PG-only continuation
// rule must not leak into MySQL parsing.
TEST(MySQLSet, DollarStillBreaksUnquotedIdent) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET schema$1 = 1";
    auto r = parser.parse(sql, strlen(sql));
    // MySQL: $ stops the identifier so the parse fails (or produces a partial
    // result without `schema$1` as a single token).
    const AstNode* v = first_value(r.ast);
    if (v != nullptr) {
        EXPECT_NE(std::string(v->value_ptr, v->value_len), "schema$1");
    }
}

// An unclosed double-quote delimited identifier must fail the parse, not
// silently swallow the rest of the input as one identifier. Without this,
// `SET search_path = "unclosed_quote, public` parses as identifier
// `unclosed_quote, public` (commas, spaces and all), which then passes
// downstream validation and corrupts the stored value.
TEST(PgSQLSetUnclosedQuote, DoubleQuoteUnterminatedIsError) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = \"unclosed_quote, public";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

// Same protection for MySQL backtick-delimited identifiers: an unclosed
// backtick must error, not consume to EOF.
TEST(MySQLSetUnclosedQuote, BacktickUnterminatedIsError) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET `unclosed_ident = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

// Properly-closed delimited identifiers must still parse OK -- regression
// guard against making the new error path too aggressive.
TEST(PgSQLSetUnclosedQuote, ClosedDoubleQuoteStillOk) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = \"public\"";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST(MySQLSetUnclosedQuote, ClosedBacktickStillOk) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET `wait_timeout` = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ============================================================================
// Post-1.0.4 audit follow-ups: PG non-GUC SET forms and value-preservation.
// ============================================================================

// Helper for the new NODE_SET_ROLE / NODE_SET_SESSION_AUTHORIZATION /
// NODE_SET_CONSTRAINTS top-level children of NODE_SET_STMT.
static const AstNode* first_set_child(const AstNode* set_stmt) {
    return (set_stmt && set_stmt->type == NodeType::NODE_SET_STMT)
        ? set_stmt->first_child : nullptr;
}

// ---- SET ROLE / SET LOCAL ROLE ---------------------------------------------
TEST(PgSQLSetRole, QuotedRole) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET ROLE 'admin'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_ROLE);
    ASSERT_NE(c->first_child, nullptr);
    EXPECT_EQ(c->first_child->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(std::string(c->first_child->value_ptr, c->first_child->value_len), "admin");
}

TEST(PgSQLSetRole, UnquotedRole) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET ROLE admin";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_ROLE);
}

TEST(PgSQLSetRole, RoleNoneIsAccepted) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET ROLE NONE";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_ROLE);
    ASSERT_NE(c->first_child, nullptr);
    EXPECT_EQ(std::string(c->first_child->value_ptr, c->first_child->value_len), "NONE");
}

TEST(PgSQLSetRole, LocalRoleCarriesScope) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET LOCAL ROLE 'admin'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_ROLE);
    // First child is the LOCAL scope identifier, second is the value.
    ASSERT_NE(c->first_child, nullptr);
    EXPECT_EQ(c->first_child->type, NodeType::NODE_IDENTIFIER);
    EXPECT_EQ(std::string(c->first_child->value_ptr, c->first_child->value_len), "LOCAL");
    ASSERT_NE(c->first_child->next_sibling, nullptr);
    EXPECT_EQ(c->first_child->next_sibling->type, NodeType::NODE_LITERAL_STRING);
}

// ---- SET SESSION AUTHORIZATION ---------------------------------------------
TEST(PgSQLSetSessionAuth, QuotedName) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET SESSION AUTHORIZATION 'alice'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_SESSION_AUTHORIZATION);
    ASSERT_NE(c->first_child, nullptr);
    EXPECT_EQ(c->first_child->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(std::string(c->first_child->value_ptr, c->first_child->value_len), "alice");
}

TEST(PgSQLSetSessionAuth, DefaultIsAccepted) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET SESSION AUTHORIZATION DEFAULT";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_SESSION_AUTHORIZATION);
}

// ---- SET CONSTRAINTS -------------------------------------------------------
TEST(PgSQLSetConstraints, AllDeferred) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET CONSTRAINTS ALL DEFERRED";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_CONSTRAINTS);
    ASSERT_NE(c->first_child, nullptr);
    EXPECT_EQ(std::string(c->first_child->value_ptr, c->first_child->value_len), "ALL");
    ASSERT_NE(c->first_child->next_sibling, nullptr);
    EXPECT_EQ(std::string(c->first_child->next_sibling->value_ptr,
                          c->first_child->next_sibling->value_len), "DEFERRED");
}

TEST(PgSQLSetConstraints, NamedImmediate) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET CONSTRAINTS my_fk IMMEDIATE";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_CONSTRAINTS);
}

// ---- SET SESSION CHARACTERISTICS AS TRANSACTION ----------------------------
TEST(PgSQLSetSessionCharacteristics, EmitsSetTransaction) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* c = first_set_child(r.ast);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->type, NodeType::NODE_SET_TRANSACTION);
}

// ---- Schema-qualified GUC name (PG) ----------------------------------------
TEST(PgSQLSetSchemaQualified, PgCatalogSearchPath) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET pg_catalog.search_path TO public";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "pg_catalog.search_path");
}

TEST(PgSQLSetSchemaQualified, CustomNamespace) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET myapp.setting = 'value'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* id = first_target_identifier(r.ast);
    ASSERT_NE(id, nullptr);
    EXPECT_EQ(std::string(id->value_ptr, id->value_len), "myapp.setting");
}

// ---- TIME ZONE INTERVAL literal preserved ----------------------------------
TEST(PgSQLSetTimeZoneInterval, FullLiteralPreserved) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET TIME ZONE INTERVAL '1' HOUR";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    const AstNode* v = first_value(r.ast);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(std::string(v->value_ptr, v->value_len), "INTERVAL '1' HOUR");
}

// ---- Clearly-malformed SET surfaces ERROR ----------------------------------
TEST(PgSQLSetErrors, EqualsSignAsTargetIsError) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

TEST(PgSQLSetErrors, EmptyRhsIsError) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET datestyle = ;";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

TEST(PgSQLSetErrors, TruncatedRhsIsError) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path TO";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

TEST(PgSQLSetErrors, LeadingCommaIsError) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET search_path = ,public";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

TEST(MySQLSetErrors, TruncatedRhsIsError) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET autocommit =";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}
