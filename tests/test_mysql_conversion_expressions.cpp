#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string emit_conversion(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    auto sql = emitter.result();
    return std::string(sql.ptr, sql.len);
}

void check_conversion(const char* sql, const char* expected = nullptr, bool bindable = false) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    const auto emitted = emit_conversion(result.ast, parser.arena());
    EXPECT_EQ(emitted, expected ? expected : sql);
    {
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
        Arena output;
        EXPECT_EQ(parameterize_ast<Dialect::MySQL>(result, output).ok(), bindable);
    }
    Parser<Dialect::MySQL> again;
    const auto reparsed = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(reparsed.ok());
    ASSERT_TRUE(reparsed.full_input);
    EXPECT_EQ(emit_conversion(reparsed.ast, again.arena()), emitted);
}

void reject_conversion(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}

TEST(MySQLConversions, CastRetainsValidatedTypeAndOperands) {
    for (const char* sql : {"SELECT CAST(n AS DECIMAL(10,2)) FROM t",
         "SELECT CAST(n AS SIGNED INTEGER) FROM t", "SELECT CAST(n AS CHAR(8) CHARACTER SET utf8mb4) FROM t",
         "SELECT CAST(COALESCE(n, 0) AS UNSIGNED) FROM t",
         "SELECT CAST((SELECT n FROM t LIMIT 1) AS DECIMAL(12,4))",
         "UPDATE t SET n = CAST(n + 1 AS SIGNED)"}) check_conversion(sql, nullptr, true);
}

TEST(MySQLConversions, ConvertRetainsTypedAndCharsetForms) {
    for (const char* sql : {"SELECT CONVERT(n, DECIMAL(10,2)) FROM t",
         "SELECT CONVERT(n, SIGNED INTEGER) FROM t", "SELECT CONVERT(n, CHAR(12)) FROM t",
         "SELECT CONVERT(body USING utf8mb4) FROM t", "SELECT CONVERT(body USING 'utf8mb4') FROM t",
         "SELECT CONVERT(body USING `utf8mb4`) FROM t", "SELECT CONVERT(body USING BINARY) FROM t"})
        check_conversion(sql);
}

TEST(MySQLConversions, RejectsMalformedConversionTypesAndClauses) {
    for (const char* sql : {"SELECT CAST()", "SELECT CAST(n)", "SELECT CAST(n AS)",
         "SELECT CAST(n AS DECIMAL())", "SELECT CAST(n AS DECIMAL(10,))",
         "SELECT CAST(n AS DECIMAL(10,2,1))", "SELECT CAST(n AS arbitrary_type)",
         "SELECT CAST(n AS VARCHAR(8))", "SELECT CAST(n AS `SIGNED`)",
         "SELECT CAST(n AS SIGNED ARRAY)", "SELECT CAST(* AS SIGNED)",
         "SELECT CAST(DEFAULT AS SIGNED)", "SELECT CONVERT(n)", "SELECT CONVERT(n,)",
         "SELECT CONVERT(n USING)", "SELECT CONVERT(n USING DEFAULT)",
         "SELECT CONVERT(n USING db.charset)", "SELECT CONVERT(n USING 1)",
         "SELECT CONVERT(n AS SIGNED)", "SELECT CONVERT(n, SIGNED, 1)",
         "SELECT CAST(n + AS SIGNED)"}) reject_conversion(sql);
}

TEST(MySQLConversions, CharsetLiteralsPreserveIntroducersAndQuotedText) {
    for (const char* sql : {"SELECT _utf8mb4'hello'", "SELECT _utf8'hello'", "SELECT _UTF8MB4'hello'",
         "SELECT _binary'abc'", "SELECT _latin1'caf\\xe9'",
         "SELECT _utf8mb4'can''t'", "SELECT _utf8mb4'hello' ' world'"}) check_conversion(sql);
    check_conversion("SELECT _utf8mb4 'hello'", "SELECT _utf8mb4'hello'");
    check_conversion("SELECT _utf8mb4\"can't\"");
}

TEST(MySQLConversions, CharsetLiteralsPreserveHexAndBitSpelling) {
    for (const char* sql : {"SELECT _binary X'4142'", "SELECT _binary B'01000001'",
         "SELECT _utf8mb4 0x4142", "SELECT _binary 0b01000001"}) check_conversion(sql);
}

TEST(MySQLConversions, CollationRetainsNameSyntaxAndPrecedence) {
    for (const char* sql : {"SELECT _utf8mb4'hello' COLLATE utf8mb4_bin",
         "SELECT body COLLATE `utf8mb4_bin` FROM t", "SELECT body COLLATE 'utf8mb4_bin' FROM t",
         "SELECT body COLLATE utf8mb4_bin = 'x' FROM t",
         "SELECT (body COLLATE utf8mb4_bin) = 'x' FROM t",
         "SELECT MATCH(body) AGAINST ('x' COLLATE utf8mb4_bin IN BOOLEAN MODE) FROM t",
         "SELECT body FROM t ORDER BY body COLLATE utf8mb4_bin",
         "UPDATE t SET body = _utf8mb4'x' COLLATE utf8mb4_bin"}) check_conversion(sql);
}

TEST(MySQLConversions, RejectsMalformedIntroducersAndCollations) {
    for (const char* sql : {"SELECT _utf8mb4", "SELECT _utf8mb4 123", "SELECT _binary NULL",
         "SELECT _utf8mb4()", "SELECT _binary X'1'", "SELECT _binary B'2'",
         "SELECT _binary 0X41", "SELECT _binary 0B01000001",
         "SELECT 'x' COLLATE", "SELECT 'x' COLLATE DEFAULT", "SELECT 'x' COLLATE BINARY",
         "SELECT 'x' COLLATE 1", "SELECT 'x' COLLATE db.utf8mb4_bin", "SELECT * COLLATE utf8mb4_bin"})
        reject_conversion(sql);
}

TEST(MySQLConversions, CastParameterizationPreservesPrecisionAndScale) {
    const char* sql = "SELECT CAST(12.34 AS DECIMAL(10,2)), CAST('abc' AS CHAR(8))";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    Arena output;
    auto bound = parameterize_ast<Dialect::MySQL>(result, output);
    ASSERT_TRUE(bound.ok());
    EXPECT_EQ(bound.parameters.size(), 2u);
    EXPECT_EQ(emit_conversion(bound.ast, output), "SELECT CAST(? AS DECIMAL(10,2)), CAST(? AS CHAR(8))");
}

TEST(MySQLConversions, CharsetAndCollationCloneOwnSyntax) {
    Arena output;
    AstNode* ast = nullptr;
    {
        std::string sql = "SELECT CONVERT(_utf8mb4'hello' ' world' USING latin1) COLLATE 'latin1_bin'";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        auto copy = clone_ast(result.ast, output);
        ASSERT_TRUE(copy.ok());
        ast = copy.ast;
        parser.reset(); sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit_conversion(ast, output), "SELECT CONVERT(_utf8mb4'hello' ' world' USING latin1) COLLATE 'latin1_bin'");
}

TEST(MySQLConversions, NativeTypeAliasesRoundTrip) {
    for (const char* sql : {"SELECT CAST(1 AS FLOAT4)", "SELECT CAST(1 AS FLOAT8)",
         "SELECT CAST(1 AS FLOAT8 PRECISION)", "SELECT CAST(1 AS SIGNED INT4)"})
        check_conversion(sql, nullptr, true);
    check_conversion("SELECT CONVERT(1, UNSIGNED INT4)");
}

TEST(MySQLConversions, PrecisionUsesNativeNumericTokenBounds) {
    for (const char* sql : {"SELECT CAST(1 AS DECIMAL(2147483648,2))",
         "SELECT CAST(1 AS DECIMAL(10,2147483648))", "SELECT CAST(1 AS TIME(2147483648))",
         "SELECT CAST(1 AS DATETIME(2147483648))"}) reject_conversion(sql);
    for (const char* sql : {"SELECT CAST(1 AS DECIMAL(2147483647,2))",
         "SELECT CAST(1 AS TIME(2147483647))", "SELECT CAST(1 AS CHAR(2147483648))",
         "SELECT CAST(1 AS CHAR(1.2))"}) check_conversion(sql, nullptr, true);
}

TEST(MySQLConversions, QuotedAndUnknownUnderscoreNamesRemainIdentifiers) {
    for (const char* sql : {"SELECT `_utf8mb4` FROM t", "SELECT _notacharset FROM t"}) {
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_EQ(emit_conversion(result.ast, parser.arena()), sql);
        EXPECT_TRUE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
    }
}

TEST(MySQLConversions, MetadataNamesRespectReservedAndIntroducerTokens) {
    for (const char* sql : {"SELECT CAST(1 AS CHAR CHARACTER SET VARCHAR)",
         "SELECT CAST(1 AS CHAR CHARACTER SET FLOAT)",
         "SELECT CAST(1 AS CHAR CHARACTER SET _utf8mb4)",
         "SELECT CONVERT('x' USING _utf8mb4)", "SELECT 'x' COLLATE _utf8mb4"}) reject_conversion(sql);
    check_conversion("SELECT CONVERT('x' USING `_utf8mb4`)"); // server resolves the quoted name
    check_conversion("SELECT 'x' COLLATE '_notacharset'");
}

TEST(MySQLConversions, DoubleQuotedOperandsDoNotProduceInvalidSingleQuotedSql) {
    for (const char* sql : {"SELECT CAST(\"can't\" AS CHAR)",
         "SELECT CONVERT(CONCAT(\"can't\", 'x') USING utf8mb4)",
         "SELECT \"can't\" COLLATE utf8mb4_bin",
         "SELECT CAST(\"a\"\"b\" AS CHAR)"}) check_conversion(sql, nullptr, std::strstr(sql, "CAST(") != nullptr);
}
