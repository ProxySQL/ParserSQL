#include <gtest/gtest.h>
#include "sql_parser/mysql_cast_type_parser.h"

using namespace sql_parser;

TEST(MySQLCastTypeParserTest, AcceptsNativeTypesAndExactSourceSpelling) {
    const char* types[] = {
        "BINARY", "binary(12)", "CHAR", "CHARACTER(20)", "NCHAR(5)",
        "NATIONAL CHAR", "national character ( 10 )", "SIGNED", "SIGNED INT",
        "signed integer", "UNSIGNED", "UNSIGNED INT", "UNSIGNED INTEGER",
        "DATE", "YEAR", "TIME", "TIME(6)", "DATETIME", "DATETIME(3)",
        "DECIMAL", "DECIMAL(12)", "DECIMAL(12, 4)", "DEC(5,2)",
        "FLOAT", "FLOAT(24)", "DOUBLE", "double precision", "REAL", "JSON",
        "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING",
        "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMCOLLECTION",
        "CHAR(0)", "CHAR(1.5)", "BINARY(.5)", "FLOAT(1.)", "DECIMAL(1.2)", "FLOAT(100)", "DECIMAL(1,9)", "TIME(100)",
        "CHAR(999999999999999999999999999999)", "CHAR /* length */ ( 12 )"
    };
    for (const char* type : types) {
        SCOPED_TRACE(type);
        const std::string sql = std::string("(") + type + "   )";
        Tokenizer<Dialect::MySQL> tok;
        tok.reset(sql.data(), sql.size());
        ASSERT_EQ(tok.next_token().type, TokenType::TK_LPAREN);
        const StringRef parsed = MySQLCastTypeParser(tok).parse();
        EXPECT_EQ(std::string(parsed.ptr ? parsed.ptr : "", parsed.len), type);
        EXPECT_EQ(parsed.ptr, sql.data() + 1);
        EXPECT_EQ(tok.peek().type, TokenType::TK_RPAREN);
        EXPECT_FALSE(tok.has_error());
    }
}

TEST(MySQLCastTypeParserTest, AcceptsNativeCharacterAttributes) {
    const char* types[] = {
        "CHAR ASCII", "CHAR ASCII BINARY", "CHAR BINARY ASCII",
        "CHAR UNICODE", "CHAR UNICODE BINARY", "CHAR BINARY UNICODE", "CHAR BYTE",
        "CHAR BINARY", "CHAR(10) CHARACTER SET utf8mb4",
        "CHAR CHAR SET utf8mb4", "CHAR CHARSET utf8mb4",
        "CHAR CHARACTER SET utf8mb4 BINARY", "CHAR BINARY CHARACTER SET utf8mb4",
        "CHAR CHARSET 'utf8mb4'", "CHAR CHARSET `utf8mb4`", "CHAR CHARSET \"utf8mb4\"",
        "CHAR CHARSET binary", "CHAR BINARY CHARSET binary",
        "CHAR CHARACTER SET /* preserve */ utf8mb4 BINARY"
    };
    for (const char* type : types) {
        SCOPED_TRACE(type);
        const std::string sql = std::string("(") + type + ")";
        Tokenizer<Dialect::MySQL> tok;
        tok.reset(sql.data(), sql.size());
        tok.next_token();
        const StringRef parsed = MySQLCastTypeParser(tok).parse();
        EXPECT_EQ(std::string(parsed.ptr ? parsed.ptr : "", parsed.len), type);
        EXPECT_EQ(tok.peek().type, TokenType::TK_RPAREN);
        EXPECT_FALSE(tok.has_error());
    }
}

TEST(MySQLCastTypeParserTest, RejectsUnknownQuotedAndMalformedTypes) {
    const char* types[] = {
        "", "made_up", "INT", "INTEGER", "VARCHAR(10)", "NUMERIC(8,2)",
        "FIXED", "GEOMETRY", "TIMESTAMP", "`CHAR`", "'CHAR'", "\"CHAR\"",
        "NATIONAL", "NATIONAL VARCHAR", "CHAR()", "CHAR(-1)", "CHAR(+1)",
        "CHAR(1e2)", "CHAR(0x10)", "CHAR(0b10)", "CHAR(?)",
        "CHAR(foo)", "CHAR(1,2)", "CHAR(1+2)", "CHAR(1",
        "DECIMAL()", "DECIMAL(1,)", "DECIMAL(,2)", "DECIMAL(1,2,3)",
        "DECIMAL(1.2,2)", "DECIMAL(1,2.2)", "TIME(1.2)", "DATETIME(1.2)", "DECIMAL(1,-2)", "DECIMAL(1,+2)", "FLOAT(3,2)", "TIME(1,2)",
        "CHAR CHARACTER", "CHAR CHARACTER SET", "CHAR CHARSET", "CHAR CHARSET 12",
        "CHAR BINARY CHARACTER SET", "CHAR CHARSET = utf8mb4"
    };
    for (const char* type : types) {
        SCOPED_TRACE(type);
        const std::string sql = std::string("(") + type + ")";
        Tokenizer<Dialect::MySQL> tok;
        tok.reset(sql.data(), sql.size());
        tok.next_token();
        EXPECT_TRUE(MySQLCastTypeParser(tok).parse().empty());
        EXPECT_TRUE(tok.has_fatal_error());
    }
}

TEST(MySQLCastTypeParserTest, LeavesTokensOutsideTheTypeProductionForCaller) {
    const struct { const char* input; const char* type; const char* next; } cases[] = {
        {"SIGNED INTEGER ARRAY", "SIGNED INTEGER", "ARRAY"},
        {"NCHAR BINARY", "NCHAR", "BINARY"},
        {"DATE(1)", "DATE", "("},
        {"DOUBLE(5)", "DOUBLE", "("},
        {"CHAR CHARSET utf8mb4 BINARY BINARY", "CHAR CHARSET utf8mb4 BINARY", "BINARY"},
        {"CHAR BINARY CHARSET utf8mb4 BINARY", "CHAR BINARY CHARSET utf8mb4", "BINARY"},
        {"CHAR BYTE BINARY", "CHAR BYTE", "BINARY"},
        {"CHAR ASCII CHARSET utf8mb4", "CHAR ASCII", "CHARSET"},
        {"DATETIME AT TIME ZONE 'UTC'", "DATETIME", "AT"}
    };
    for (const auto& test : cases) {
        SCOPED_TRACE(test.input);
        const std::string sql = std::string("(") + test.input + ")";
        Tokenizer<Dialect::MySQL> tok;
        tok.reset(sql.data(), sql.size());
        tok.next_token();
        const StringRef parsed = MySQLCastTypeParser(tok).parse();
        EXPECT_EQ(std::string(parsed.ptr ? parsed.ptr : "", parsed.len), test.type);
        const Token next = tok.peek();
        EXPECT_EQ(std::string(next.source.ptr, next.source.len), test.next);
        EXPECT_FALSE(tok.has_error());
    }
}
