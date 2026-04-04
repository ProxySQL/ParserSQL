#include <gtest/gtest.h>
#include "sql_engine/row.h"
#include "sql_parser/arena.h"

using namespace sql_engine;
using namespace sql_parser;

TEST(RowTest, CreateRowAllNull) {
    Arena arena;
    Row row = make_row(arena, 3);
    EXPECT_EQ(row.column_count, 3);
    EXPECT_TRUE(row.is_null(0));
    EXPECT_TRUE(row.is_null(1));
    EXPECT_TRUE(row.is_null(2));
}

TEST(RowTest, SetAndGetInt) {
    Arena arena;
    Row row = make_row(arena, 2);
    row.set(0, value_int(42));
    row.set(1, value_int(99));

    Value v0 = row.get(0);
    ASSERT_EQ(v0.tag, Value::TAG_INT64);
    EXPECT_EQ(v0.int_val, 42);

    Value v1 = row.get(1);
    ASSERT_EQ(v1.tag, Value::TAG_INT64);
    EXPECT_EQ(v1.int_val, 99);
}

TEST(RowTest, SetAndGetString) {
    Arena arena;
    Row row = make_row(arena, 1);
    const char* hello = "hello";
    row.set(0, value_string(StringRef{hello, 5}));

    Value v = row.get(0);
    ASSERT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(v.str_val.len, 5u);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello");
}

TEST(RowTest, SetAndGetBool) {
    Arena arena;
    Row row = make_row(arena, 2);
    row.set(0, value_bool(true));
    row.set(1, value_bool(false));

    EXPECT_EQ(row.get(0).tag, Value::TAG_BOOL);
    EXPECT_TRUE(row.get(0).bool_val);
    EXPECT_FALSE(row.get(1).bool_val);
}

TEST(RowTest, SetAndGetDouble) {
    Arena arena;
    Row row = make_row(arena, 1);
    row.set(0, value_double(3.14));
    Value v = row.get(0);
    ASSERT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.14);
}

TEST(RowTest, NullCheckAfterSet) {
    Arena arena;
    Row row = make_row(arena, 3);
    EXPECT_TRUE(row.is_null(0));
    EXPECT_TRUE(row.is_null(1));

    row.set(0, value_int(1));
    EXPECT_FALSE(row.is_null(0));
    EXPECT_TRUE(row.is_null(1));

    // Set back to null
    row.set(0, value_null());
    EXPECT_TRUE(row.is_null(0));
}

TEST(RowTest, MixedTypes) {
    Arena arena;
    Row row = make_row(arena, 4);
    row.set(0, value_int(42));
    row.set(1, value_double(2.5));
    row.set(2, value_bool(true));
    // index 3 stays null

    EXPECT_EQ(row.get(0).tag, Value::TAG_INT64);
    EXPECT_EQ(row.get(1).tag, Value::TAG_DOUBLE);
    EXPECT_EQ(row.get(2).tag, Value::TAG_BOOL);
    EXPECT_TRUE(row.is_null(3));
}

TEST(RowTest, ArenaAllocation) {
    Arena arena;
    // Create multiple rows from the same arena
    Row r1 = make_row(arena, 2);
    Row r2 = make_row(arena, 3);
    r1.set(0, value_int(1));
    r2.set(0, value_int(2));

    // They should be independent
    EXPECT_EQ(r1.get(0).int_val, 1);
    EXPECT_EQ(r2.get(0).int_val, 2);
    EXPECT_EQ(r1.column_count, 2);
    EXPECT_EQ(r2.column_count, 3);
}
