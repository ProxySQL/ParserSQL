#include <gtest/gtest.h>
#include "sql_engine/like.h"

using namespace sql_engine;
using sql_parser::Dialect;
using sql_parser::StringRef;

// Helper to make StringRef from string literal
static StringRef S(const char* s) {
    return StringRef{s, static_cast<uint32_t>(std::strlen(s))};
}

// ===== MySQL (case-insensitive) =====

class LikeMySQLTest : public ::testing::Test {};

TEST_F(LikeMySQLTest, ExactMatch) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hello")));
}

TEST_F(LikeMySQLTest, ExactMatchCaseInsensitive) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("Hello"), S("hello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("HELLO"), S("hello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("HELLO")));
}

TEST_F(LikeMySQLTest, NoMatch) {
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("world")));
}

TEST_F(LikeMySQLTest, PercentPrefix) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%llo")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%hello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%o")));
}

TEST_F(LikeMySQLTest, PercentSuffix) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hel%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hello%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("h%")));
}

TEST_F(LikeMySQLTest, PercentBoth) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%ell%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%hello%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%%")));
}

TEST_F(LikeMySQLTest, PercentOnly) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("anything"), S("%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("%")));
}

TEST_F(LikeMySQLTest, Underscore) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hell_")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("_ello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("_____")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("____")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("______")));
}

TEST_F(LikeMySQLTest, UnderscoreAndPercent) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("_ell%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%ll_")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello world"), S("hello_world")));
}

TEST_F(LikeMySQLTest, EscapeCharacter) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("100%"), S("100\\%")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("100x"), S("100\\%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("a_b"), S("a\\_b")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("axb"), S("a\\_b")));
}

TEST_F(LikeMySQLTest, CustomEscapeCharacter) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("100%"), S("100#%"), '#'));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("100x"), S("100#%"), '#'));
}

TEST_F(LikeMySQLTest, EmptyString) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("%")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S(""), S("_")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S(""), S("a")));
}

TEST_F(LikeMySQLTest, EmptyPattern) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("")));
}

TEST_F(LikeMySQLTest, MultiplePercents) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("abcdef"), S("%b%d%f")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("abcdef"), S("%b%e%")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("abcdef"), S("%z%")));
}

// ===== PostgreSQL (case-sensitive) =====

class LikePgSQLTest : public ::testing::Test {};

TEST_F(LikePgSQLTest, ExactMatch) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hello")));
}

TEST_F(LikePgSQLTest, CaseSensitive) {
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("Hello"), S("hello")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("HELLO"), S("hello")));
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hello")));
}

TEST_F(LikePgSQLTest, PercentPrefix) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("%llo")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("hello"), S("%LLO")));
}

TEST_F(LikePgSQLTest, PercentSuffix) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hel%")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("hello"), S("HEL%")));
}

TEST_F(LikePgSQLTest, Underscore) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hell_")));
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("_ello")));
}

TEST_F(LikePgSQLTest, EscapeCharacter) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("100%"), S("100\\%")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("100x"), S("100\\%")));
}

TEST_F(LikePgSQLTest, EmptyStringEdgeCases) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S(""), S("")));
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S(""), S("%")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S(""), S("_")));
}
