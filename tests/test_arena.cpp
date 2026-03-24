#include <gtest/gtest.h>
#include "sql_parser/arena.h"

using namespace sql_parser;

TEST(ArenaTest, AllocateAndReset) {
    Arena arena(4096);
    void* p1 = arena.allocate(64);
    ASSERT_NE(p1, nullptr);
    void* p2 = arena.allocate(64);
    ASSERT_NE(p2, nullptr);
    EXPECT_NE(p1, p2);

    arena.reset();
    void* p3 = arena.allocate(64);
    ASSERT_NE(p3, nullptr);
    EXPECT_EQ(p1, p3);
}

TEST(ArenaTest, AllocateAligned) {
    Arena arena(4096);
    (void)arena.allocate(1);  // advance cursor by 1 byte
    void* p2 = arena.allocate(8);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p2) % 8, 0u);
}

TEST(ArenaTest, OverflowToNewBlock) {
    Arena arena(128);
    void* p1 = arena.allocate(100);
    ASSERT_NE(p1, nullptr);
    void* p2 = arena.allocate(100);
    ASSERT_NE(p2, nullptr);
    EXPECT_NE(p1, p2);
}

TEST(ArenaTest, ResetFreesOverflowBlocks) {
    Arena arena(128);
    arena.allocate(100);
    arena.allocate(100);
    arena.reset();
    void* p = arena.allocate(64);
    ASSERT_NE(p, nullptr);
}

TEST(ArenaTest, MaxSizeEnforced) {
    Arena arena(128, 256);
    void* p1 = arena.allocate(100);
    ASSERT_NE(p1, nullptr);
    void* p2 = arena.allocate(100);
    ASSERT_NE(p2, nullptr);
    void* p3 = arena.allocate(100);
    EXPECT_EQ(p3, nullptr);
}

TEST(ArenaTest, AllocateTyped) {
    Arena arena(4096);

    struct TestStruct {
        int a;
        double b;
    };

    TestStruct* ts = arena.allocate_typed<TestStruct>();
    ASSERT_NE(ts, nullptr);
    ts->a = 42;
    ts->b = 3.14;
    EXPECT_EQ(ts->a, 42);
    EXPECT_DOUBLE_EQ(ts->b, 3.14);
}

TEST(ArenaTest, AllocateString) {
    Arena arena(4096);
    const char* src = "hello world";
    StringRef ref = arena.allocate_string(src, 11);
    EXPECT_EQ(ref.len, 11u);
    EXPECT_EQ(std::memcmp(ref.ptr, "hello world", 11), 0);
    EXPECT_NE(ref.ptr, src);
}
