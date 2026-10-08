#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/expression_parser.h"
#include <cstdlib>
#include <cstring>
#include <string>

using namespace sql_parser;

TEST(Pr67Expressions, QuantifiedComparisonDoesNotMoveNotAcrossLike) {
    // PostgreSQL: NOT (('a' LIKE 'a') = ANY(...)), which is false.
    // Moving NOT into LIKE instead makes this expression true.
    const char* sql = "SELECT NOT 'a' LIKE 'a' = ANY(ARRAY[true,false])";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(result.ast);
    auto emitted = emitter.result();
    EXPECT_EQ(std::string(emitted.ptr, emitted.len),
              "SELECT NOT 'a' LIKE 'a' = ANY(ARRAY[true, false])");
}

TEST(Pr67Expressions, QuantifiedOperatorsKeepTheirLeftBindingPrecedence) {
    struct Case { const char* sql; const char* outer; const char* inner; bool inner_on_right; };
    // Expected grouping comes from PostgreSQL 18 gram.y and libpg_query.
    const Case cases[] = {
        {"a LIKE b = ANY(c)", "=", "LIKE", false},
        {"a ILIKE b <> ALL(c)", "<>", "ILIKE", false},
        {"a + b * ANY(c)", "+", "*", true},
        {"a = b LIKE ANY(c)", "=", "LIKE", true},
        {"a = ANY(b) = c", "=", "=", false},
        {"a || b = ANY(c)", "=", "||", false},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.sql);
        Arena arena;
        Tokenizer<Dialect::PostgreSQL> tokenizer;
        tokenizer.reset(c.sql, std::strlen(c.sql));
        ExpressionParser<Dialect::PostgreSQL> parser(tokenizer, arena, true);
        auto* expression = parser.parse_complete();
        ASSERT_NE(expression, nullptr);
        ASSERT_EQ(tokenizer.peek().type, TokenType::TK_EOF);
        EXPECT_EQ(std::string(expression->value_ptr, expression->value_len), c.outer);
        auto* inner = expression->first_child;
        ASSERT_NE(inner, nullptr);
        if (c.inner_on_right) inner = inner->next_sibling;
        ASSERT_NE(inner, nullptr);
        EXPECT_EQ(std::string(inner->value_ptr, inner->value_len), c.inner);
    }
}

template<Dialect D>
static void check_window_allocation_limits(const char* sql) {
    // Exercise each allocation boundary in the spec, ordering, bounds and exclusion.
    // Successful parses must retain every clause; exhausted parses must flag failure.
    for (size_t capacity = sizeof(AstNode); capacity <= 1024; capacity += sizeof(AstNode)) {
        Arena arena(capacity, capacity);
        Tokenizer<D> tokenizer;
        tokenizer.reset(sql, std::strlen(sql));
        ExpressionParser<D> parser(tokenizer, arena, true);
        auto* spec = parser.parse_window_spec();
        if (!spec) {
            if (!tokenizer.has_error()) std::exit(2);
        } else {
            if (tokenizer.peek().type != TokenType::TK_EOF) std::exit(3);
            Arena output;
            Emitter<D> emitter(output);
            emitter.emit(spec);
            auto emitted = emitter.result();
            if (std::string(emitted.ptr, emitted.len) != sql) std::exit(4);
        }
    }
}

TEST(Pr67Expressions, WindowAllocationFailuresReturnErrorsInBothDialects) {
    EXPECT_EXIT(([] {
        const char* sql = "(ORDER BY x DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
        check_window_allocation_limits<Dialect::PostgreSQL>(sql);
        check_window_allocation_limits<Dialect::MySQL>(sql);
        const char* offset = "(PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)";
        check_window_allocation_limits<Dialect::PostgreSQL>(offset);
        check_window_allocation_limits<Dialect::MySQL>(offset);
        std::exit(0);
    }()), ::testing::ExitedWithCode(0), "");
}

TEST(Pr67Expressions, WindowExclusionCannotDisappearWhenArenaIsFull) {
    EXPECT_EXIT(([] {
        check_window_allocation_limits<Dialect::PostgreSQL>(
            "(ORDER BY x ROWS CURRENT ROW EXCLUDE TIES)");
        std::exit(0);
    }()), ::testing::ExitedWithCode(0), "");
}

TEST(Pr67Expressions, PublicParserReportsDefaultArenaExhaustionAtWindowFrame) {
    EXPECT_EXIT(([] {
        std::string sql = "SELECT ";
        for (unsigned i = 0; i < 10915; ++i) sql += "1, ";
        sql += "sum(x) OVER (ORDER BY y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t";
        Parser<Dialect::PostgreSQL> pg;
        auto pg_result = pg.parse(sql.data(), sql.size());
        if (pg_result.ok() && pg_result.full_input) std::exit(2);
        Parser<Dialect::MySQL> mysql;
        auto mysql_result = mysql.parse(sql.data(), sql.size());
        if (mysql_result.ok() && mysql_result.full_input) std::exit(3);
        std::exit(0);
    }()), ::testing::ExitedWithCode(0), "");
}

TEST(Pr67Expressions, NotStaysOutsideQuantifiedPatternComparisons) {
    for (const char* quantifier : {"ANY", "ALL", "SOME"}) {
        for (const char* pattern : {"LIKE", "ILIKE"}) {
            for (bool prefix_not : {false, true}) {
                const std::string sql = std::string(prefix_not ? "SELECT NOT 'a' " : "SELECT 'a' NOT ") +
                    pattern + " " + quantifier + "(ARRAY['a', 'b'])";
                SCOPED_TRACE(sql);
                Parser<Dialect::PostgreSQL> parser;
                auto parsed = parser.parse(sql.data(), sql.size());
                ASSERT_TRUE(parsed.ok() && parsed.full_input);
                Emitter<Dialect::PostgreSQL> emitter(parser.arena());
                emitter.emit(parsed.ast);
                auto output = emitter.result();
                EXPECT_EQ(std::string(output.ptr, output.len), sql);
            }
        }
    }
}
