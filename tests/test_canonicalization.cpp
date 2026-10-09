#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/expression_parser.h"

using namespace sql_parser;

class MySQLCanonicalizationTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    const AstNode* find_descendant(const AstNode* node, NodeType type) {
        if (!node) return nullptr;
        if (node->type == type) return node;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (const AstNode* hit = find_descendant(c, type)) return hit;
        }
        return nullptr;
    }

    std::string value_of(const char* sql, NodeType type) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        const AstNode* node = find_descendant(r.ast, type);
        if (!node) return "[NOT_FOUND]";
        return std::string(node->value_ptr, node->value_len);
    }

    std::string child_value_of(const char* sql, NodeType type, int index) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        const AstNode* node = find_descendant(r.ast, type);
        if (!node) return "[NOT_FOUND]";
        const AstNode* child = node->first_child;
        for (int i = 0; i < index && child; ++i) child = child->next_sibling;
        if (!child) return "[NO_CHILD]";
        return std::string(child->value_ptr, child->value_len);
    }
};

class PgSQLCanonicalizationTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

    const AstNode* find_descendant(const AstNode* node, NodeType type) {
        if (!node) return nullptr;
        if (node->type == type) return node;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (const AstNode* hit = find_descendant(c, type)) return hit;
        }
        return nullptr;
    }

    std::string value_of(const char* sql, NodeType type) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        const AstNode* node = find_descendant(r.ast, type);
        if (!node) return "[NOT_FOUND]";
        return std::string(node->value_ptr, node->value_len);
    }
};

// ========== Set operators ==========

TEST_F(MySQLCanonicalizationTest, SetOperators) {
    EXPECT_EQ(value_of("SELECT 1 union SELECT 2", NodeType::NODE_SET_OPERATION), "UNION");
    EXPECT_EQ(value_of("SELECT 1 UnIoN SELECT 2", NodeType::NODE_SET_OPERATION), "UNION");
    EXPECT_EQ(value_of("SELECT 1 intersect SELECT 2", NodeType::NODE_SET_OPERATION), "INTERSECT");
    EXPECT_EQ(value_of("SELECT 1 except SELECT 2", NodeType::NODE_SET_OPERATION), "EXCEPT");
}

// ========== Expression operators ==========

TEST_F(MySQLCanonicalizationTest, BinaryOperators) {
    EXPECT_EQ(value_of("SELECT a FROM t WHERE a and b", NodeType::NODE_BINARY_OP), "AND");
    EXPECT_EQ(value_of("SELECT a FROM t WHERE a or b", NodeType::NODE_BINARY_OP), "OR");
    EXPECT_EQ(value_of("SELECT a FROM t WHERE b like 'x'", NodeType::NODE_BINARY_OP), "LIKE");
}

TEST_F(MySQLCanonicalizationTest, UnaryOperators) {
    EXPECT_EQ(value_of("SELECT not a FROM t", NodeType::NODE_UNARY_OP), "NOT");
}

// ========== Literals ==========

TEST_F(MySQLCanonicalizationTest, NullLiteral) {
    EXPECT_EQ(value_of("SELECT null", NodeType::NODE_LITERAL_NULL), "NULL");
}

TEST_F(MySQLCanonicalizationTest, NullLiteralKeepsLosslessSource) {
    const char* sql = "SELECT null";
    auto r = parser.parse(sql, strlen(sql));
    ASSERT_NE(r.ast, nullptr);
    const AstNode* node = find_descendant(r.ast, NodeType::NODE_LITERAL_NULL);
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(std::string(node->source().ptr, node->source().len), "null");
}

// ========== Clause keywords ==========

TEST_F(MySQLCanonicalizationTest, JoinTypes) {
    EXPECT_EQ(value_of("SELECT a FROM t1 inner join t2 ON t1.a = t2.b",
                       NodeType::NODE_JOIN_CLAUSE), "INNER JOIN");
    EXPECT_EQ(value_of("SELECT a FROM t1 left outer join t2 ON t1.a = t2.b",
                       NodeType::NODE_JOIN_CLAUSE), "LEFT OUTER JOIN");
}

TEST_F(MySQLCanonicalizationTest, JoinTypeSpacingIsNormalized) {
    EXPECT_EQ(value_of("SELECT a FROM t1 LEFT   OUTER   JOIN t2 ON t1.a = t2.b",
                       NodeType::NODE_JOIN_CLAUSE), "LEFT OUTER JOIN");
    EXPECT_EQ(value_of("SELECT a FROM t1 left\touter\tjoin t2 ON t1.a = t2.b",
                       NodeType::NODE_JOIN_CLAUSE), "LEFT OUTER JOIN");
}

TEST_F(MySQLCanonicalizationTest, OrderByDirection) {
    EXPECT_EQ(child_value_of("SELECT a FROM t ORDER BY a desc",
                             NodeType::NODE_ORDER_BY_ITEM, 1), "DESC");
    EXPECT_EQ(child_value_of("SELECT a FROM t ORDER BY a asc",
                             NodeType::NODE_ORDER_BY_ITEM, 1), "ASC");
}

TEST_F(MySQLCanonicalizationTest, SelectOptions) {
    EXPECT_EQ(child_value_of("SELECT distinct a FROM t",
                             NodeType::NODE_SELECT_OPTIONS, 0), "DISTINCT");
}

TEST_F(MySQLCanonicalizationTest, LockStrength) {
    EXPECT_EQ(child_value_of("SELECT a FROM t for update",
                             NodeType::NODE_LOCKING_CLAUSE, 0), "UPDATE");
}

// ========== Function names ==========

TEST_F(MySQLCanonicalizationTest, FunctionNames) {
    EXPECT_EQ(value_of("SELECT count(*) FROM t", NodeType::NODE_FUNCTION_CALL), "COUNT");
    EXPECT_EQ(value_of("SELECT MaX(a) FROM t", NodeType::NODE_FUNCTION_CALL), "MAX");
}

TEST_F(PgSQLCanonicalizationTest, UndelimitedFunctionNamesFoldDown) {
    EXPECT_EQ(value_of("SELECT MYFUNC(a) FROM t", NodeType::NODE_FUNCTION_CALL), "myfunc");
    EXPECT_EQ(value_of("SELECT MyFunc(a) FROM t", NodeType::NODE_FUNCTION_CALL), "myfunc");
}

TEST_F(PgSQLCanonicalizationTest, DelimitedFunctionNameKeepsItsOwnSpelling) {
    // PostgreSQL folds undelimited names down, so "MYFUNC" is a different function.
    EXPECT_EQ(value_of("SELECT \"MYFUNC\"(a) FROM t", NodeType::NODE_FUNCTION_CALL), "\"MYFUNC\"");
}

// ========== System variables ==========

TEST_F(MySQLCanonicalizationTest, SystemVariableSeparatorIsNormalized) {
    // MySQL accepts whitespace, newlines and comments around the dot.
    EXPECT_EQ(value_of("SELECT @@session.sql_mode", NodeType::NODE_COLUMN_REF),
              "@@session.sql_mode");
    EXPECT_EQ(value_of("SELECT @@session . sql_mode", NodeType::NODE_COLUMN_REF),
              "@@session.sql_mode");
    EXPECT_EQ(value_of("SELECT @@session  .  sql_mode", NodeType::NODE_COLUMN_REF),
              "@@session.sql_mode");
    EXPECT_EQ(value_of("SELECT @@session\n.\nsql_mode", NodeType::NODE_COLUMN_REF),
              "@@session.sql_mode");
    EXPECT_EQ(value_of("SELECT @@session/*c*/.sql_mode", NodeType::NODE_COLUMN_REF),
              "@@session.sql_mode");
}

TEST_F(MySQLCanonicalizationTest, UnscopedSystemVariable) {
    EXPECT_EQ(value_of("SELECT @@sql_mode", NodeType::NODE_COLUMN_REF), "@@sql_mode");
}

// ========== Identifiers are not keywords ==========

TEST_F(MySQLCanonicalizationTest, IdentifiersKeepTheirCase) {
    // Table and column names are case-sensitive wherever the filesystem is.
    EXPECT_EQ(value_of("SELECT a FROM MyTable", NodeType::NODE_IDENTIFIER), "MyTable");
    EXPECT_EQ(value_of("SELECT MyCol FROM t", NodeType::NODE_COLUMN_REF), "MyCol");
}

namespace {
void expect_source_spans_in_input(const AstNode* node, const char* sql, size_t length) {
    if (!node) return;
    if (node->source_len) {
        const auto begin = reinterpret_cast<uintptr_t>(sql);
        const auto source = reinterpret_cast<uintptr_t>(node->source_ptr);
        ASSERT_GE(source, begin);
        ASSERT_LE(source - begin, length);
        EXPECT_LE(node->source_len, length - (source - begin));
    }
    for (const auto* child = node->first_child; child; child = child->next_sibling)
        expect_source_spans_in_input(child, sql, length);
}
}

TEST_F(MySQLCanonicalizationTest, CanonicalValuesDoNotBecomeSourceLocations) {
    for (const char* sql : {"SELECT -abs(1)", "SELECT -@@session.sql_mode", "SELECT NOT abs(1)"}) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        expect_source_spans_in_input(result.ast, sql, strlen(sql));
    }
}

TEST_F(PgSQLCanonicalizationTest, CanonicalValuesDoNotBecomeSourceLocations) {
    const char* sql = "SELECT NOT MYFUNC(1)";
    auto result = parser.parse(sql, strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    expect_source_spans_in_input(result.ast, sql, strlen(sql));
}

template<Dialect D>
static std::string canonical_sql(const char* sql, EmitMode mode = EmitMode::NORMAL) {
    Parser<D> parser;
    auto result = parser.parse(sql, strlen(sql));
    EXPECT_TRUE(result.ok()) << sql;
    EXPECT_TRUE(result.full_input) << sql;
    Emitter<D> emitter(parser.arena(), mode);
    emitter.emit(result.ast);
    auto value = emitter.result();
    return std::string(value.ptr ? value.ptr : "", value.len);
}

TEST(CanonicalizationReview, FunctionDelimitersSurviveRoundTrip) {
    EXPECT_EQ(canonical_sql<Dialect::PostgreSQL>("SELECT \"MiXeD\"(1)"), "SELECT \"MiXeD\"(1)");
    EXPECT_EQ(canonical_sql<Dialect::PostgreSQL>("SELECT \"my func\"(1)"), "SELECT \"my func\"(1)");
    EXPECT_EQ(canonical_sql<Dialect::PostgreSQL>("SELECT \"a\"\"B\"(1)"), "SELECT \"a\"\"B\"(1)");
    EXPECT_EQ(canonical_sql<Dialect::MySQL>("SELECT `myfunc`(1)"), "SELECT `MYFUNC`(1)");
}

TEST(CanonicalizationReview, QualifiedFunctionsFoldEachUndelimitedComponent) {
    EXPECT_EQ(canonical_sql<Dialect::PostgreSQL>("SELECT Public /*comment*/ . MYFUNC(1)"),
              "SELECT public.myfunc(1)");
    EXPECT_EQ(canonical_sql<Dialect::PostgreSQL>("SELECT \"Public\" . MYFUNC(1)"),
              "SELECT \"Public\".myfunc(1)");
    EXPECT_EQ(canonical_sql<Dialect::PostgreSQL>("SELECT Public . \"MyFunc\"(1)"),
              "SELECT public.\"MyFunc\"(1)");
}

TEST(CanonicalizationReview, QuotedSystemVariablesRemainQuoted) {
    EXPECT_EQ(canonical_sql<Dialect::MySQL>("SELECT @@session . `sql_mode`"),
              "SELECT @@session.`sql_mode`");
}

TEST(CanonicalizationReview, SourceSpansRetainTheOriginalExpression) {
    for (const char* sql : {"SELECT -abs(1)", "SELECT -@@session . sql_mode"}) {
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, strlen(sql));
        ASSERT_TRUE(result.ok());
        const auto* unary = result.ast->first_child->first_child->first_child;
        ASSERT_EQ(unary->type, NodeType::NODE_UNARY_OP);
        ASSERT_NE(unary->source_ptr, nullptr);
        ASSERT_LE(unary->source_len, strlen(sql));
        EXPECT_EQ(std::string(unary->source_ptr, unary->source_len), std::string(sql + 7));
    }
}

TEST(CanonicalizationReview, CaseFoldingReportsAllocationFailure) {
    Arena arena(64, 64);
    ASSERT_NE(arena.allocate(64), nullptr);
    EXPECT_TRUE(arena.allocate_upper(StringRef{"lower", 5}).empty());
    EXPECT_TRUE(arena.allocate_lower(StringRef{"UPPER", 5}).empty());
}

TEST(CanonicalizationReview, AlreadyCanonicalNamesNeedNoAllocation) {
    Arena arena;
    const size_t used = arena.bytes_used();
    EXPECT_EQ(arena.allocate_upper(StringRef{"UPPER", 5}), (StringRef{"UPPER", 5}));
    EXPECT_EQ(arena.allocate_lower(StringRef{"lower", 5}), (StringRef{"lower", 5}));
    EXPECT_EQ(arena.bytes_used(), used);
}

TEST(CanonicalizationReview, FunctionArgumentsCannotDisappearWhenArenaFills) {
    Arena arena(96, 96);
    Tokenizer<Dialect::MySQL> tokens;
    const char* sql = "abs(1)";
    tokens.reset(sql, strlen(sql));
    ExpressionParser<Dialect::MySQL> parser(tokens, arena);
    auto* expression = parser.parse();
    EXPECT_EQ(expression, nullptr);
    EXPECT_TRUE(tokens.has_error());
}
