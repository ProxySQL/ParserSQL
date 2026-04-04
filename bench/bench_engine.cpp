// bench_engine.cpp — Engine execution benchmarks
//
// Benchmarks the query engine pipeline: expression evaluation, plan building,
// full pipeline execution, filter/join/sort/aggregate operators.

#include <benchmark/benchmark.h>

#include "sql_parser/parser.h"
#include "sql_parser/common.h"
#include "sql_engine/session.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_engine/local_txn.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/optimizer.h"
#include "sql_engine/catalog_resolver.h"
#include "sql_engine/value.h"
#include "sql_engine/row.h"

#include <vector>
#include <string>
#include <cstring>

using namespace sql_parser;
using namespace sql_engine;

// ========== Helpers ==========

// Build a catalog with a "users" table: id INT, name VARCHAR, age INT, score DOUBLE
static InMemoryCatalog make_users_catalog() {
    InMemoryCatalog catalog;
    catalog.add_table("", "users", {
        {"id",    SqlType::make_int(),        false},
        {"name",  SqlType::make_varchar(255), true},
        {"age",   SqlType::make_int(),        true},
        {"score", SqlType{SqlType::DOUBLE, 0, 0, false, false}, true},
    });
    return catalog;
}

// Build a catalog with "orders" table: id INT, user_id INT, total DOUBLE
static void add_orders_table(InMemoryCatalog& catalog) {
    catalog.add_table("", "orders", {
        {"id",      SqlType::make_int(),        false},
        {"user_id", SqlType::make_int(),        true},
        {"total",   SqlType{SqlType::DOUBLE, 0, 0, false, false}, true},
    });
}

// Build N rows for users table
static std::vector<Row> make_user_rows(Arena& arena, int n) {
    std::vector<Row> rows;
    rows.reserve(n);
    for (int i = 0; i < n; ++i) {
        Row r = make_row(arena, 4);
        r.set(0, value_int(static_cast<int64_t>(i + 1)));

        char buf[32];
        int len = snprintf(buf, sizeof(buf), "user_%d", i);
        char* name = static_cast<char*>(arena.allocate(static_cast<uint32_t>(len)));
        std::memcpy(name, buf, static_cast<size_t>(len));
        r.set(1, value_string(StringRef{name, static_cast<uint32_t>(len)}));

        r.set(2, value_int(static_cast<int64_t>(18 + (i % 50))));
        r.set(3, value_double(static_cast<double>(50 + (i % 100))));
        rows.push_back(r);
    }
    return rows;
}

// Build N rows for orders table
static std::vector<Row> make_order_rows(Arena& arena, int n, int max_user_id) {
    std::vector<Row> rows;
    rows.reserve(n);
    for (int i = 0; i < n; ++i) {
        Row r = make_row(arena, 3);
        r.set(0, value_int(static_cast<int64_t>(i + 1)));
        r.set(1, value_int(static_cast<int64_t>((i % max_user_id) + 1)));
        r.set(2, value_double(static_cast<double>(10 + (i % 200))));
        rows.push_back(r);
    }
    return rows;
}

// ========== Expression Evaluation Benchmarks ==========

// Simple: 1 + 2
static void BM_Expr_SimpleArith(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    FunctionRegistry<Dialect::MySQL> funcs;
    funcs.register_builtins();
    Arena arena{65536, 1048576};

    const char* sql = "SELECT 1 + 2";
    size_t len = std::strlen(sql);

    auto pr = parser.parse(sql, len);
    // The expression is in the SELECT list: pr.ast->first_child
    AstNode* expr = pr.ast ? pr.ast->first_child : nullptr;

    auto resolve = [](const StringRef&) -> Value {
        return value_null();
    };

    for (auto _ : state) {
        Value v = evaluate_expression<Dialect::MySQL>(expr, resolve, funcs, arena);
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(BM_Expr_SimpleArith);

// Complex: price * qty > 100
static void BM_Expr_ComplexCompare(benchmark::State& state) {
    InMemoryCatalog catalog;
    catalog.add_table("", "items", {
        {"price", SqlType{SqlType::DOUBLE, 0, 0, false, false}, false},
        {"qty",   SqlType::make_int(), false},
    });

    Parser<Dialect::MySQL> parser;
    FunctionRegistry<Dialect::MySQL> funcs;
    funcs.register_builtins();
    Arena arena{65536, 1048576};

    // Parse: SELECT price * qty > 100 FROM items
    const char* sql = "SELECT price * qty > 100 FROM items";
    size_t len = std::strlen(sql);
    auto pr = parser.parse(sql, len);
    AstNode* expr = pr.ast ? pr.ast->first_child : nullptr;

    // Build a row with price=42.5, qty=3
    Row row = make_row(arena, 2);
    row.set(0, value_double(42.5));
    row.set(1, value_int(3));

    const TableInfo* ti = catalog.get_table(StringRef{"items", 5});
    auto resolve = make_resolver(catalog, ti, row.values);

    for (auto _ : state) {
        Value v = evaluate_expression<Dialect::MySQL>(expr, resolve, funcs, arena);
        benchmark::DoNotOptimize(v);
    }
}
BENCHMARK(BM_Expr_ComplexCompare);

// ========== Plan Building Benchmarks ==========

// Simple SELECT plan build
static void BM_PlanBuild_Simple(benchmark::State& state) {
    InMemoryCatalog catalog = make_users_catalog();
    Parser<Dialect::MySQL> parser;

    const char* sql = "SELECT id, name FROM users WHERE age > 21";
    size_t len = std::strlen(sql);

    for (auto _ : state) {
        parser.reset();
        auto pr = parser.parse(sql, len);
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        benchmark::DoNotOptimize(plan);
    }
}
BENCHMARK(BM_PlanBuild_Simple);

// Complex SELECT plan build (JOIN + GROUP BY + HAVING + ORDER BY + LIMIT)
static void BM_PlanBuild_Complex(benchmark::State& state) {
    InMemoryCatalog catalog = make_users_catalog();
    add_orders_table(catalog);
    Parser<Dialect::MySQL> parser;

    const char* sql =
        "SELECT u.id, u.name, COUNT(o.id) AS cnt FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "WHERE u.age > 18 GROUP BY u.id, u.name "
        "HAVING COUNT(o.id) > 2 ORDER BY cnt DESC LIMIT 10";
    size_t len = std::strlen(sql);

    for (auto _ : state) {
        parser.reset();
        auto pr = parser.parse(sql, len);
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        benchmark::DoNotOptimize(plan);
    }
}
BENCHMARK(BM_PlanBuild_Complex);

// ========== Full Pipeline Benchmarks ==========

// Parse -> plan -> optimize -> execute (simple SELECT, 100 rows)
static void BM_Pipeline_Simple(benchmark::State& state) {
    InMemoryCatalog catalog = make_users_catalog();
    Arena data_arena{65536, 1048576};
    auto rows = make_user_rows(data_arena, 100);
    const TableInfo* ti = catalog.get_table(StringRef{"users", 5});
    InMemoryDataSource source(ti, std::move(rows));

    FunctionRegistry<Dialect::MySQL> funcs;
    funcs.register_builtins();

    const char* sql = "SELECT id, name FROM users WHERE age > 30";
    size_t len = std::strlen(sql);

    for (auto _ : state) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, len);
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        Optimizer<Dialect::MySQL> optimizer(catalog, funcs);
        plan = optimizer.optimize(plan, parser.arena());
        PlanExecutor<Dialect::MySQL> executor(funcs, catalog, parser.arena());
        executor.add_data_source("users", &source);
        ResultSet rs = executor.execute(plan);
        benchmark::DoNotOptimize(rs.rows.size());
    }
}
BENCHMARK(BM_Pipeline_Simple);

// ========== Operator Benchmarks ==========

// Filter: scan 1000 rows, filter to ~100
static void BM_Op_Filter1000(benchmark::State& state) {
    InMemoryCatalog catalog = make_users_catalog();
    Arena data_arena{262144, 1048576};
    auto rows = make_user_rows(data_arena, 1000);
    const TableInfo* ti = catalog.get_table(StringRef{"users", 5});
    InMemoryDataSource source(ti, std::move(rows));

    FunctionRegistry<Dialect::MySQL> funcs;
    funcs.register_builtins();

    // age > 60 filters to ~7/50 * 1000 = ~140 rows
    const char* sql = "SELECT id, name FROM users WHERE age > 60";
    size_t len = std::strlen(sql);

    for (auto _ : state) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, len);
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        PlanExecutor<Dialect::MySQL> executor(funcs, catalog, parser.arena());
        executor.add_data_source("users", &source);
        ResultSet rs = executor.execute(plan);
        benchmark::DoNotOptimize(rs.rows.size());
    }
}
BENCHMARK(BM_Op_Filter1000);

// Join: two 1000-row tables via nested-loop
static void BM_Op_Join1000(benchmark::State& state) {
    InMemoryCatalog catalog = make_users_catalog();
    add_orders_table(catalog);
    Arena data_arena{262144, 4194304};
    auto user_rows = make_user_rows(data_arena, 100);
    auto order_rows = make_order_rows(data_arena, 1000, 100);

    const TableInfo* uti = catalog.get_table(StringRef{"users", 5});
    const TableInfo* oti = catalog.get_table(StringRef{"orders", 6});
    InMemoryDataSource user_src(uti, std::move(user_rows));
    InMemoryDataSource order_src(oti, std::move(order_rows));

    FunctionRegistry<Dialect::MySQL> funcs;
    funcs.register_builtins();

    const char* sql =
        "SELECT u.id, o.total FROM users u "
        "JOIN orders o ON u.id = o.user_id";
    size_t len = std::strlen(sql);

    for (auto _ : state) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, len);
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        PlanExecutor<Dialect::MySQL> executor(funcs, catalog, parser.arena());
        executor.add_data_source("users", &user_src);
        executor.add_data_source("orders", &order_src);
        ResultSet rs = executor.execute(plan);
        benchmark::DoNotOptimize(rs.rows.size());
    }
}
BENCHMARK(BM_Op_Join1000);

// Sort: 1000 rows, 1 key
static void BM_Op_Sort1000(benchmark::State& state) {
    InMemoryCatalog catalog = make_users_catalog();
    Arena data_arena{262144, 1048576};
    auto rows = make_user_rows(data_arena, 1000);
    const TableInfo* ti = catalog.get_table(StringRef{"users", 5});
    InMemoryDataSource source(ti, std::move(rows));

    FunctionRegistry<Dialect::MySQL> funcs;
    funcs.register_builtins();

    const char* sql = "SELECT id, name, age FROM users ORDER BY age DESC";
    size_t len = std::strlen(sql);

    for (auto _ : state) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, len);
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        PlanExecutor<Dialect::MySQL> executor(funcs, catalog, parser.arena());
        executor.add_data_source("users", &source);
        ResultSet rs = executor.execute(plan);
        benchmark::DoNotOptimize(rs.rows.size());
    }
}
BENCHMARK(BM_Op_Sort1000);

// Aggregate: 1000 rows, ~10 groups (GROUP BY age % 10 effectively)
static void BM_Op_Aggregate1000(benchmark::State& state) {
    InMemoryCatalog catalog = make_users_catalog();
    Arena data_arena{262144, 1048576};
    auto rows = make_user_rows(data_arena, 1000);
    const TableInfo* ti = catalog.get_table(StringRef{"users", 5});
    InMemoryDataSource source(ti, std::move(rows));

    FunctionRegistry<Dialect::MySQL> funcs;
    funcs.register_builtins();

    // age has 50 distinct values (18..67), so GROUP BY age gives ~50 groups
    const char* sql =
        "SELECT age, COUNT(id), AVG(score) FROM users GROUP BY age";
    size_t len = std::strlen(sql);

    for (auto _ : state) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, len);
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        PlanExecutor<Dialect::MySQL> executor(funcs, catalog, parser.arena());
        executor.add_data_source("users", &source);
        ResultSet rs = executor.execute(plan);
        benchmark::DoNotOptimize(rs.rows.size());
    }
}
BENCHMARK(BM_Op_Aggregate1000);
