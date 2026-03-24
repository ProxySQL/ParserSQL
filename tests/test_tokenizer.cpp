#include <gtest/gtest.h>
#include "sql_parser/tokenizer.h"

using namespace sql_parser;

// ========== MySQL Tokenizer Tests ==========

class MySQLTokenizerTest : public ::testing::Test {
protected:
    Tokenizer<Dialect::MySQL> tok;
};

TEST_F(MySQLTokenizerTest, SimpleSelect) {
    const char* sql = "SELECT * FROM users;";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_SELECT);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_ASTERISK);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_FROM);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
    EXPECT_EQ(t.text.len, 5u);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "users");

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_SEMICOLON);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_EOF);
}

TEST_F(MySQLTokenizerTest, CaseInsensitiveKeywords) {
    const char* sql = "select FROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, BacktickIdentifier) {
    const char* sql = "`my table`";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "my table");
}

TEST_F(MySQLTokenizerTest, SingleQuotedString) {
    const char* sql = "'hello world'";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_STRING);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "hello world");
}

TEST_F(MySQLTokenizerTest, IntegerLiteral) {
    const char* sql = "42";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_INTEGER);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "42");
}

TEST_F(MySQLTokenizerTest, FloatLiteral) {
    const char* sql = "3.14";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_FLOAT);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "3.14");
}

TEST_F(MySQLTokenizerTest, ComparisonOperators) {
    const char* sql = "= != < > <= >=";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_EQUAL);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_NOT_EQUAL);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_LESS);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_GREATER);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_LESS_EQUAL);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_GREATER_EQUAL);
}

TEST_F(MySQLTokenizerTest, DiamondNotEqual) {
    const char* sql = "<>";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_NOT_EQUAL);
}

TEST_F(MySQLTokenizerTest, AtVariables) {
    const char* sql = "@myvar @@global_var";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_AT);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_DOUBLE_AT);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
}

TEST_F(MySQLTokenizerTest, Placeholder) {
    const char* sql = "?";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_QUESTION);
}

TEST_F(MySQLTokenizerTest, ColonEqual) {
    const char* sql = ":=";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_COLON_EQUAL);
}

TEST_F(MySQLTokenizerTest, LineComment) {
    const char* sql = "SELECT -- this is a comment\nFROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, HashComment) {
    const char* sql = "SELECT # comment\nFROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, BlockComment) {
    const char* sql = "SELECT /* comment */ FROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, PeekDoesNotConsume) {
    const char* sql = "SELECT FROM";
    tok.reset(sql, strlen(sql));

    Token peeked = tok.peek();
    EXPECT_EQ(peeked.type, TokenType::TK_SELECT);

    Token consumed = tok.next_token();
    EXPECT_EQ(consumed.type, TokenType::TK_SELECT);

    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, EmptyInput) {
    tok.reset("", 0);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_EOF);
}

TEST_F(MySQLTokenizerTest, WhitespaceOnly) {
    const char* sql = "   \t\n\r  ";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_EOF);
}

TEST_F(MySQLTokenizerTest, QualifiedIdentifier) {
    const char* sql = "myschema.orders";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_IDENTIFIER); // myschema
    EXPECT_EQ(tok.next_token().type, TokenType::TK_DOT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_IDENTIFIER); // orders
}

// ========== PostgreSQL Tokenizer Tests ==========

class PgSQLTokenizerTest : public ::testing::Test {
protected:
    Tokenizer<Dialect::PostgreSQL> tok;
};

TEST_F(PgSQLTokenizerTest, DoubleQuotedIdentifier) {
    const char* sql = "\"my table\"";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "my table");
}

TEST_F(PgSQLTokenizerTest, DollarQuotedString) {
    const char* sql = "$$hello world$$";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_STRING);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "hello world");
}

TEST_F(PgSQLTokenizerTest, DoubleColonCast) {
    const char* sql = "::";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_DOUBLE_COLON);
}

TEST_F(PgSQLTokenizerTest, PositionalParam) {
    const char* sql = "$1 $23";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_DOLLAR_NUM);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "$1");

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_DOLLAR_NUM);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "$23");
}

TEST_F(PgSQLTokenizerTest, NestedBlockComment) {
    const char* sql = "SELECT /* outer /* inner */ still comment */ FROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(PgSQLTokenizerTest, NoHashComment) {
    // PostgreSQL does NOT support # comments — # should be TK_HASH token
    const char* sql = "#";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_HASH);
}
