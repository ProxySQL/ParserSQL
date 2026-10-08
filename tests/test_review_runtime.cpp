#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <limits>
#include <cstdlib>

using namespace sql_parser;
using namespace sql_engine;

TEST(ReviewRuntime, PostgreSQLNullLimitsStayUnbounded) {
    struct Case { const char* sql; int64_t count; int64_t offset; };
    for (const auto& c : {Case{"SELECT 1 LIMIT ALL", -1, 0},
                          Case{"SELECT 1 LIMIT ALL OFFSET 2", -1, 2},
                          Case{"SELECT 1 LIMIT NULL", -1, 0},
                          Case{"SELECT 1 LIMIT 0", 0, 0},
                          Case{"SELECT 1 LIMIT 3 OFFSET NULL", 3, 0},
                          Case{"SELECT 1 LIMIT 3 OFFSET 2", 3, 2}}) {
        SCOPED_TRACE(c.sql);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parser.parse(c.sql, std::strlen(c.sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input);
        InMemoryCatalog catalog;
        PlanBuilder<Dialect::PostgreSQL> builder(catalog, parser.arena());
        auto* plan = builder.build(parsed.ast);
        ASSERT_NE(plan, nullptr);
        ASSERT_EQ(plan->type, PlanNodeType::LIMIT);
        EXPECT_EQ(plan->limit.count, c.count);
        EXPECT_EQ(plan->limit.offset, c.offset);
    }
}

TEST(ReviewRuntime, UnsupportedLimitValuesCannotBecomeDigitCounts) {
    for (const char* sql : {"SELECT 1 LIMIT 1+2", "SELECT 1 LIMIT -1",
                            "SELECT 1 LIMIT $1", "SELECT 1 LIMIT 1.5",
                            "SELECT 1 LIMIT 9223372036854775808",
                            "SELECT 1 LIMIT 3 OFFSET 1+2"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input);
        InMemoryCatalog catalog;
        PlanBuilder<Dialect::PostgreSQL> builder(catalog, parser.arena());
        EXPECT_EQ(builder.build(parsed.ast), nullptr);
    }
}

TEST(ReviewRuntime, OversizedInputIsRejectedBeforeReadingTheBuffer) {
    if (std::numeric_limits<size_t>::max() <= std::numeric_limits<uint32_t>::max()) GTEST_SKIP();
    const size_t oversized = static_cast<size_t>(std::numeric_limits<uint32_t>::max()) + 1;
    // Oversized input is rejected before dereferencing the supplied pointer.
    EXPECT_EXIT({
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(nullptr, oversized);
        std::exit(result.status == ParseResult::ERROR && !result.full_input &&
                  !result.ast && !result.error.message.empty() ? 0 : 1);
    }, ::testing::ExitedWithCode(0), "");
    EXPECT_EXIT({
        Parser<Dialect::MySQL> parser;
        auto batch = parser.parse_all(nullptr, oversized);
        std::exit(!batch.ok() && batch.statements.size() == 1 &&
                  batch.statements[0].result.status == ParseResult::ERROR &&
                  !batch.statements[0].result.error.message.empty() ? 0 : 1);
    }, ::testing::ExitedWithCode(0), "");
}
