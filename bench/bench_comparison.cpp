#include <benchmark/benchmark.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

// libpg_query
extern "C" {
#include "pg_query.h"
#include "pg_query_internal.h"  // for pg_query_raw_parse (parse-only, no JSON)
}

using namespace sql_parser;

// ========== Query set — same queries for both parsers ==========

struct QueryCase {
    const char* name;
    const char* sql;
};

static const QueryCase comparison_queries[] = {
    {"simple_select",   "SELECT col FROM t WHERE id = 1"},
    {"select_join",     "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"},
    {"select_complex",  "SELECT u.id, u.name, COUNT(o.id) AS order_count "
                        "FROM users u LEFT JOIN orders o ON u.id = o.user_id "
                        "WHERE u.status = 'active' "
                        "GROUP BY u.id, u.name "
                        "HAVING COUNT(o.id) > 5 "
                        "ORDER BY order_count DESC LIMIT 50"},
    {"insert_values",   "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"},
    {"update_simple",   "UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'"},
    {"delete_simple",   "DELETE FROM users WHERE id = 42"},
    {"set_simple",      "SET @@session.wait_timeout = 600"},
    {"set_names",       "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"},
    {"begin",           "BEGIN"},
    {"create_table",    "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))"},
};

// ========== Our parser (PostgreSQL dialect for fair comparison) ==========

static void BM_Ours_Select_Simple(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = comparison_queries[0].sql;
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Ours_Select_Simple);

static void BM_PgQuery_Select_Simple(benchmark::State& state) {
    const char* sql = comparison_queries[0].sql;
    for (auto _ : state) {
        PgQueryParseResult result = pg_query_parse(sql);
        benchmark::DoNotOptimize(result.parse_tree);
        pg_query_free_parse_result(result);
    }
}
BENCHMARK(BM_PgQuery_Select_Simple);

static void BM_Ours_Select_Join(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = comparison_queries[1].sql;
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Ours_Select_Join);

static void BM_PgQuery_Select_Join(benchmark::State& state) {
    const char* sql = comparison_queries[1].sql;
    for (auto _ : state) {
        PgQueryParseResult result = pg_query_parse(sql);
        benchmark::DoNotOptimize(result.parse_tree);
        pg_query_free_parse_result(result);
    }
}
BENCHMARK(BM_PgQuery_Select_Join);

static void BM_Ours_Select_Complex(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = comparison_queries[2].sql;
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Ours_Select_Complex);

static void BM_PgQuery_Select_Complex(benchmark::State& state) {
    const char* sql = comparison_queries[2].sql;
    for (auto _ : state) {
        PgQueryParseResult result = pg_query_parse(sql);
        benchmark::DoNotOptimize(result.parse_tree);
        pg_query_free_parse_result(result);
    }
}
BENCHMARK(BM_PgQuery_Select_Complex);

static void BM_Ours_Insert(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = comparison_queries[3].sql;
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Ours_Insert);

static void BM_PgQuery_Insert(benchmark::State& state) {
    const char* sql = comparison_queries[3].sql;
    for (auto _ : state) {
        PgQueryParseResult result = pg_query_parse(sql);
        benchmark::DoNotOptimize(result.parse_tree);
        pg_query_free_parse_result(result);
    }
}
BENCHMARK(BM_PgQuery_Insert);

static void BM_Ours_Update(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = comparison_queries[4].sql;
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Ours_Update);

static void BM_PgQuery_Update(benchmark::State& state) {
    const char* sql = comparison_queries[4].sql;
    for (auto _ : state) {
        PgQueryParseResult result = pg_query_parse(sql);
        benchmark::DoNotOptimize(result.parse_tree);
        pg_query_free_parse_result(result);
    }
}
BENCHMARK(BM_PgQuery_Update);

static void BM_Ours_Delete(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = comparison_queries[5].sql;
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Ours_Delete);

static void BM_PgQuery_Delete(benchmark::State& state) {
    const char* sql = comparison_queries[5].sql;
    for (auto _ : state) {
        PgQueryParseResult result = pg_query_parse(sql);
        benchmark::DoNotOptimize(result.parse_tree);
        pg_query_free_parse_result(result);
    }
}
BENCHMARK(BM_PgQuery_Delete);

static void BM_Ours_Begin(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = comparison_queries[8].sql;
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.stmt_type);
    }
}
BENCHMARK(BM_Ours_Begin);

static void BM_PgQuery_Begin(benchmark::State& state) {
    const char* sql = comparison_queries[8].sql;
    for (auto _ : state) {
        PgQueryParseResult result = pg_query_parse(sql);
        benchmark::DoNotOptimize(result.parse_tree);
        pg_query_free_parse_result(result);
    }
}
BENCHMARK(BM_PgQuery_Begin);

// ========== libpg_query RAW PARSE (no JSON serialization) ==========
// This is the fairest comparison — parse only, no serialization.
// Still includes PostgreSQL memory context setup/teardown per call.

static void BM_PgQueryRaw_Select_Simple(benchmark::State& state) {
    const char* sql = comparison_queries[0].sql;
    for (auto _ : state) {
        MemoryContext ctx = pg_query_enter_memory_context();
        PgQueryInternalParsetreeAndError result = pg_query_raw_parse(sql, 0);
        benchmark::DoNotOptimize(result.tree);
        pg_query_exit_memory_context(ctx);
    }
}
BENCHMARK(BM_PgQueryRaw_Select_Simple);

static void BM_PgQueryRaw_Select_Join(benchmark::State& state) {
    const char* sql = comparison_queries[1].sql;
    for (auto _ : state) {
        MemoryContext ctx = pg_query_enter_memory_context();
        PgQueryInternalParsetreeAndError result = pg_query_raw_parse(sql, 0);
        benchmark::DoNotOptimize(result.tree);
        pg_query_exit_memory_context(ctx);
    }
}
BENCHMARK(BM_PgQueryRaw_Select_Join);

static void BM_PgQueryRaw_Select_Complex(benchmark::State& state) {
    const char* sql = comparison_queries[2].sql;
    for (auto _ : state) {
        MemoryContext ctx = pg_query_enter_memory_context();
        PgQueryInternalParsetreeAndError result = pg_query_raw_parse(sql, 0);
        benchmark::DoNotOptimize(result.tree);
        pg_query_exit_memory_context(ctx);
    }
}
BENCHMARK(BM_PgQueryRaw_Select_Complex);

static void BM_PgQueryRaw_Insert(benchmark::State& state) {
    const char* sql = comparison_queries[3].sql;
    for (auto _ : state) {
        MemoryContext ctx = pg_query_enter_memory_context();
        PgQueryInternalParsetreeAndError result = pg_query_raw_parse(sql, 0);
        benchmark::DoNotOptimize(result.tree);
        pg_query_exit_memory_context(ctx);
    }
}
BENCHMARK(BM_PgQueryRaw_Insert);

static void BM_PgQueryRaw_Begin(benchmark::State& state) {
    const char* sql = comparison_queries[8].sql;
    for (auto _ : state) {
        MemoryContext ctx = pg_query_enter_memory_context();
        PgQueryInternalParsetreeAndError result = pg_query_raw_parse(sql, 0);
        benchmark::DoNotOptimize(result.tree);
        pg_query_exit_memory_context(ctx);
    }
}
BENCHMARK(BM_PgQueryRaw_Begin);
