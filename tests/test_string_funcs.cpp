#include <gtest/gtest.h>
#include "sql_engine/functions/string.h"
#include "sql_parser/arena.h"
#include <string>

using namespace sql_engine;
using namespace sql_engine::functions;
using sql_parser::Arena;
using sql_parser::StringRef;

class StringFuncTest : public ::testing::Test {
protected:
    Arena arena{4096};

    Value S(const char* s) { return value_string(StringRef{s, static_cast<uint32_t>(std::strlen(s))}); }
    std::string str(const Value& v) { return std::string(v.str_val.ptr, v.str_val.len); }
};

// --- CONCAT ---

TEST_F(StringFuncTest, ConcatTwo) {
    Value args[] = {S("hello"), S(" world")};
    EXPECT_EQ(str(fn_concat(args, 2, arena)), "hello world");
}

TEST_F(StringFuncTest, ConcatThree) {
    Value args[] = {S("a"), S("b"), S("c")};
    EXPECT_EQ(str(fn_concat(args, 3, arena)), "abc");
}

TEST_F(StringFuncTest, ConcatNull) {
    Value args[] = {S("hello"), value_null()};
    EXPECT_TRUE(fn_concat(args, 2, arena).is_null());
}

TEST_F(StringFuncTest, ConcatEmpty) {
    Value args[] = {S(""), S("")};
    auto r = fn_concat(args, 2, arena);
    EXPECT_EQ(r.str_val.len, 0u);
}

// --- CONCAT_WS ---

TEST_F(StringFuncTest, ConcatWsBasic) {
    Value args[] = {S(","), S("a"), S("b"), S("c")};
    EXPECT_EQ(str(fn_concat_ws(args, 4, arena)), "a,b,c");
}

TEST_F(StringFuncTest, ConcatWsSkipsNull) {
    Value args[] = {S(","), S("a"), value_null(), S("c")};
    EXPECT_EQ(str(fn_concat_ws(args, 4, arena)), "a,c");
}

TEST_F(StringFuncTest, ConcatWsNullSeparator) {
    Value args[] = {value_null(), S("a"), S("b")};
    EXPECT_TRUE(fn_concat_ws(args, 3, arena).is_null());
}

// --- LENGTH ---

TEST_F(StringFuncTest, LengthBasic) {
    Value args[] = {S("hello")};
    EXPECT_EQ(fn_length(args, 1, arena).int_val, 5);
}

TEST_F(StringFuncTest, LengthEmpty) {
    Value args[] = {S("")};
    EXPECT_EQ(fn_length(args, 1, arena).int_val, 0);
}

TEST_F(StringFuncTest, LengthNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_length(args, 1, arena).is_null());
}

TEST_F(StringFuncTest, LengthCountsUtf8Bytes) {
    Value args[] = {S("caf" "\xC3" "\xA9")};
    EXPECT_EQ(fn_length(args, 1, arena).int_val, 5);
}

TEST_F(StringFuncTest, CharLengthCountsAsciiCharacters) {
    Value args[] = {S("hello")};
    EXPECT_EQ(fn_char_length(args, 1, arena).int_val, 5);
}

TEST_F(StringFuncTest, CharLengthCountsUtf8CodePoints) {
    Value args[] = {S("caf" "\xC3" "\xA9")};
    EXPECT_EQ(fn_char_length(args, 1, arena).int_val, 4);
}

TEST_F(StringFuncTest, CharLengthCountsEmojiAsSingleCharacter) {
    Value args[] = {S("A" "\xF0" "\x9F" "\x98" "\x80" "B")};
    EXPECT_EQ(fn_char_length(args, 1, arena).int_val, 3);
}

TEST_F(StringFuncTest, CharLengthNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_char_length(args, 1, arena).is_null());
}

// --- UPPER / LOWER ---

TEST_F(StringFuncTest, UpperBasic) {
    Value args[] = {S("hello")};
    EXPECT_EQ(str(fn_upper(args, 1, arena)), "HELLO");
}

TEST_F(StringFuncTest, LowerBasic) {
    Value args[] = {S("HELLO")};
    EXPECT_EQ(str(fn_lower(args, 1, arena)), "hello");
}

TEST_F(StringFuncTest, UpperNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_upper(args, 1, arena).is_null());
}

TEST_F(StringFuncTest, LowerMixed) {
    Value args[] = {S("HeLLo WoRLd")};
    EXPECT_EQ(str(fn_lower(args, 1, arena)), "hello world");
}

// --- SUBSTRING ---

TEST_F(StringFuncTest, SubstringFrom) {
    Value args[] = {S("hello world"), value_int(7)};
    EXPECT_EQ(str(fn_substring(args, 2, arena)), "world");
}

TEST_F(StringFuncTest, SubstringFromLen) {
    Value args[] = {S("hello world"), value_int(1), value_int(5)};
    EXPECT_EQ(str(fn_substring(args, 3, arena)), "hello");
}

TEST_F(StringFuncTest, SubstringNegative) {
    Value args[] = {S("hello"), value_int(-3)};
    EXPECT_EQ(str(fn_substring(args, 2, arena)), "llo");
}

TEST_F(StringFuncTest, SubstringNull) {
    Value args[] = {value_null(), value_int(1)};
    EXPECT_TRUE(fn_substring(args, 2, arena).is_null());
}

TEST_F(StringFuncTest, SubstringBeyondEnd) {
    Value args[] = {S("hi"), value_int(10)};
    EXPECT_EQ(fn_substring(args, 2, arena).str_val.len, 0u);
}

// --- TRIM / LTRIM / RTRIM ---

TEST_F(StringFuncTest, TrimBoth) {
    Value args[] = {S("  hello  ")};
    EXPECT_EQ(str(fn_trim(args, 1, arena)), "hello");
}

TEST_F(StringFuncTest, LtrimBasic) {
    Value args[] = {S("  hello  ")};
    EXPECT_EQ(str(fn_ltrim(args, 1, arena)), "hello  ");
}

TEST_F(StringFuncTest, RtrimBasic) {
    Value args[] = {S("  hello  ")};
    EXPECT_EQ(str(fn_rtrim(args, 1, arena)), "  hello");
}

TEST_F(StringFuncTest, TrimNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_trim(args, 1, arena).is_null());
}

TEST_F(StringFuncTest, TrimAllSpaces) {
    Value args[] = {S("   ")};
    EXPECT_EQ(fn_trim(args, 1, arena).str_val.len, 0u);
}

// --- REPLACE ---

TEST_F(StringFuncTest, ReplaceBasic) {
    Value args[] = {S("hello world"), S("world"), S("earth")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "hello earth");
}

TEST_F(StringFuncTest, ReplaceMultiple) {
    Value args[] = {S("aaa"), S("a"), S("bb")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "bbbbbb");
}

TEST_F(StringFuncTest, ReplaceNoMatch) {
    Value args[] = {S("hello"), S("xyz"), S("abc")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "hello");
}

TEST_F(StringFuncTest, ReplaceNull) {
    Value args[] = {value_null(), S("a"), S("b")};
    EXPECT_TRUE(fn_replace(args, 3, arena).is_null());
}

TEST_F(StringFuncTest, ReplaceEmptySearch) {
    Value args[] = {S("hello"), S(""), S("x")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "hello");
}

// --- REVERSE ---

TEST_F(StringFuncTest, ReverseBasic) {
    Value args[] = {S("hello")};
    EXPECT_EQ(str(fn_reverse(args, 1, arena)), "olleh");
}

TEST_F(StringFuncTest, ReverseEmpty) {
    Value args[] = {S("")};
    EXPECT_EQ(fn_reverse(args, 1, arena).str_val.len, 0u);
}

TEST_F(StringFuncTest, ReverseNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_reverse(args, 1, arena).is_null());
}

// --- LEFT / RIGHT ---

TEST_F(StringFuncTest, LeftBasic) {
    Value args[] = {S("hello"), value_int(3)};
    EXPECT_EQ(str(fn_left(args, 2, arena)), "hel");
}

TEST_F(StringFuncTest, LeftBeyondLen) {
    Value args[] = {S("hi"), value_int(10)};
    EXPECT_EQ(str(fn_left(args, 2, arena)), "hi");
}

TEST_F(StringFuncTest, RightBasic) {
    Value args[] = {S("hello"), value_int(3)};
    EXPECT_EQ(str(fn_right(args, 2, arena)), "llo");
}

TEST_F(StringFuncTest, LeftZero) {
    Value args[] = {S("hello"), value_int(0)};
    EXPECT_EQ(fn_left(args, 2, arena).str_val.len, 0u);
}

// --- LPAD / RPAD ---

TEST_F(StringFuncTest, LpadBasic) {
    Value args[] = {S("hi"), value_int(5), S("*")};
    EXPECT_EQ(str(fn_lpad(args, 3, arena)), "***hi");
}

TEST_F(StringFuncTest, LpadCyclicPad) {
    Value args[] = {S("hi"), value_int(7), S("abc")};
    EXPECT_EQ(str(fn_lpad(args, 3, arena)), "abcabhi");
}

TEST_F(StringFuncTest, LpadTruncate) {
    Value args[] = {S("hello"), value_int(3), S("*")};
    EXPECT_EQ(str(fn_lpad(args, 3, arena)), "hel");
}

TEST_F(StringFuncTest, RpadBasic) {
    Value args[] = {S("hi"), value_int(5), S("*")};
    EXPECT_EQ(str(fn_rpad(args, 3, arena)), "hi***");
}

TEST_F(StringFuncTest, RpadNull) {
    Value args[] = {value_null(), value_int(5), S("*")};
    EXPECT_TRUE(fn_rpad(args, 3, arena).is_null());
}

// --- REPEAT ---

TEST_F(StringFuncTest, RepeatBasic) {
    Value args[] = {S("ab"), value_int(3)};
    EXPECT_EQ(str(fn_repeat(args, 2, arena)), "ababab");
}

TEST_F(StringFuncTest, RepeatZero) {
    Value args[] = {S("ab"), value_int(0)};
    EXPECT_EQ(fn_repeat(args, 2, arena).str_val.len, 0u);
}

TEST_F(StringFuncTest, RepeatNull) {
    Value args[] = {value_null(), value_int(3)};
    EXPECT_TRUE(fn_repeat(args, 2, arena).is_null());
}

// --- SPACE ---

TEST_F(StringFuncTest, SpaceBasic) {
    Value args[] = {value_int(5)};
    EXPECT_EQ(str(fn_space(args, 1, arena)), "     ");
}

TEST_F(StringFuncTest, SpaceZero) {
    Value args[] = {value_int(0)};
    EXPECT_EQ(fn_space(args, 1, arena).str_val.len, 0u);
}

TEST_F(StringFuncTest, SpaceNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_space(args, 1, arena).is_null());
}
