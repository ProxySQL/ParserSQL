#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLStarModifiersTest : public ::testing::Test {
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

class PgSQLStarModifiersTest : public ::testing::Test {
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

// ========== EXCEPT ==========

TEST_F(MySQLStarModifiersTest, SelectStarExcept) {
    const char* sql = "SELECT * EXCEPT(id, created_at) FROM users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);

    // Find SELECT_ITEM_LIST -> SELECT_ITEM -> STAR_EXCEPT
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    auto* item = items->first_child;
    ASSERT_NE(item, nullptr);
    auto* except_node = find_child(item, NodeType::NODE_STAR_EXCEPT);
    ASSERT_NE(except_node, nullptr);

    // First child is the asterisk, then the excluded columns
    auto* star = except_node->first_child;
    ASSERT_NE(star, nullptr);
    EXPECT_EQ(star->type, NodeType::NODE_ASTERISK);

    auto* col1 = star->next_sibling;
    ASSERT_NE(col1, nullptr);
    EXPECT_EQ(col1->type, NodeType::NODE_IDENTIFIER);

    auto* col2 = col1->next_sibling;
    ASSERT_NE(col2, nullptr);
    EXPECT_EQ(col2->type, NodeType::NODE_IDENTIFIER);
}

TEST_F(MySQLStarModifiersTest, SelectQualifiedStarExcept) {
    const char* sql = "SELECT users.* EXCEPT(password) FROM users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);

    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    auto* item = items->first_child;
    ASSERT_NE(item, nullptr);
    auto* except_node = find_child(item, NodeType::NODE_STAR_EXCEPT);
    ASSERT_NE(except_node, nullptr);

    // First child is qualified name (users.*), then excluded column
    auto* qname = except_node->first_child;
    ASSERT_NE(qname, nullptr);
    EXPECT_EQ(qname->type, NodeType::NODE_QUALIFIED_NAME);
}

// ========== REPLACE ==========

TEST_F(MySQLStarModifiersTest, SelectStarReplace) {
    const char* sql = "SELECT * REPLACE(UPPER(name) AS name) FROM users";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);

    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    auto* item = items->first_child;
    ASSERT_NE(item, nullptr);
    auto* replace_node = find_child(item, NodeType::NODE_STAR_REPLACE);
    ASSERT_NE(replace_node, nullptr);

    // First child is asterisk, then REPLACE_ITEM(s)
    auto* star = replace_node->first_child;
    ASSERT_NE(star, nullptr);
    EXPECT_EQ(star->type, NodeType::NODE_ASTERISK);

    auto* ritem = star->next_sibling;
    ASSERT_NE(ritem, nullptr);
    EXPECT_EQ(ritem->type, NodeType::NODE_REPLACE_ITEM);
}

// ========== Round-trip ==========

TEST_F(MySQLStarModifiersTest, RoundTripExcept) {
    std::string out = round_trip("SELECT * EXCEPT(id, created_at) FROM users");
    EXPECT_NE(out.find("EXCEPT"), std::string::npos);
    EXPECT_NE(out.find("id"), std::string::npos);
    EXPECT_NE(out.find("created_at"), std::string::npos);
}

TEST_F(MySQLStarModifiersTest, RoundTripReplace) {
    std::string out = round_trip("SELECT * REPLACE(UPPER(name) AS name) FROM users");
    EXPECT_NE(out.find("REPLACE"), std::string::npos);
    EXPECT_NE(out.find("UPPER"), std::string::npos);
    EXPECT_NE(out.find("name"), std::string::npos);
}

// ========== PostgreSQL dialect ==========

TEST_F(PgSQLStarModifiersTest, SelectStarExceptPgSQL) {
    const char* sql = "SELECT * EXCEPT(id) FROM t1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
}

// ========== Multiple replacements ==========

TEST_F(MySQLStarModifiersTest, MultipleReplacements) {
    const char* sql = "SELECT * REPLACE(1 + 2 AS x, 'hello' AS y) FROM t1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);

    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    auto* item = items->first_child;
    ASSERT_NE(item, nullptr);
    auto* replace_node = find_child(item, NodeType::NODE_STAR_REPLACE);
    ASSERT_NE(replace_node, nullptr);

    // Count REPLACE_ITEM children (skip the first child which is the asterisk)
    int replace_items = 0;
    for (const AstNode* c = replace_node->first_child; c; c = c->next_sibling) {
        if (c->type == NodeType::NODE_REPLACE_ITEM) ++replace_items;
    }
    EXPECT_EQ(replace_items, 2);
}
