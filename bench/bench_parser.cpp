#include <benchmark/benchmark.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

#include <chrono>
#include <vector>
#include <algorithm>
#include <cmath>

using namespace sql_parser;

// ========== Tier 2: Classification ==========
// Target: <100ns

static void BM_Classify_Insert(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "INSERT INTO users VALUES (1, 'name', 'email')";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.stmt_type);
    }
}
BENCHMARK(BM_Classify_Insert);

static void BM_Classify_Update(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "UPDATE users SET name = 'x' WHERE id = 1";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.stmt_type);
    }
}
BENCHMARK(BM_Classify_Update);

static void BM_Classify_Delete(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "DELETE FROM users WHERE id = 1";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.stmt_type);
    }
}
BENCHMARK(BM_Classify_Delete);

static void BM_Classify_Begin(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "BEGIN";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.stmt_type);
    }
}
BENCHMARK(BM_Classify_Begin);

// ========== Tier 1: SET parse ==========
// Target: <300ns

static void BM_Set_Simple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET @@session.wait_timeout = 600";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Set_Simple);

static void BM_Set_Names(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Set_Names);

static void BM_Set_MultiVar(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET autocommit = 1, wait_timeout = 28800, sql_mode = 'STRICT_TRANS_TABLES'";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Set_MultiVar);

static void BM_Set_FunctionRHS(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Set_FunctionRHS);

// ========== Tier 1: SELECT parse ==========
// Target: <500ns simple, <2us complex

static void BM_Select_Simple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT col FROM t WHERE id = 1";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Select_Simple);

static void BM_Select_MultiColumn(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT id, name, email, status FROM users WHERE active = 1 ORDER BY name LIMIT 100";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Select_MultiColumn);

static void BM_Select_Join(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Select_Join);

static void BM_Select_Complex(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql =
        "SELECT u.id, u.name, COUNT(o.id) AS order_count "
        "FROM users u "
        "LEFT JOIN orders o ON u.id = o.user_id "
        "WHERE u.status = 'active' AND u.created_at > '2024-01-01' "
        "GROUP BY u.id, u.name "
        "HAVING COUNT(o.id) > 5 "
        "ORDER BY order_count DESC "
        "LIMIT 50 OFFSET 10";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Select_Complex);

static void BM_Select_MultiJoin(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql =
        "SELECT a.id, b.name, c.value, d.total "
        "FROM t1 a "
        "JOIN t2 b ON a.id = b.a_id "
        "LEFT JOIN t3 c ON b.id = c.b_id "
        "JOIN t4 d ON c.id = d.c_id "
        "WHERE a.status = 1 AND d.total > 100 "
        "ORDER BY d.total DESC "
        "LIMIT 20";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_Select_MultiJoin);

// ========== Query Reconstruction (round-trip) ==========
// Target: <500ns

static void BM_Emit_SetSimple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET autocommit = 1";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        benchmark::DoNotOptimize(emitter.result());
    }
}
BENCHMARK(BM_Emit_SetSimple);

static void BM_Emit_SelectSimple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT * FROM users WHERE id = 1";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        benchmark::DoNotOptimize(emitter.result());
    }
}
BENCHMARK(BM_Emit_SelectSimple);

// ========== Arena reset ==========
// Target: <10ns

static void BM_ArenaReset(benchmark::State& state) {
    Arena arena(65536);
    for (auto _ : state) {
        arena.allocate(256);  // allocate something
        arena.reset();
        benchmark::DoNotOptimize(arena.bytes_used());
    }
}
BENCHMARK(BM_ArenaReset);

// ========== PostgreSQL ==========

static void BM_PgSQL_Select_Simple(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT col FROM t WHERE id = 1";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_PgSQL_Select_Simple);

static void BM_PgSQL_Set_Simple(benchmark::State& state) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET work_mem = '256MB'";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_PgSQL_Set_Simple);

// ========== Multi-threaded benchmarks ==========
// Parser is per-thread — each thread creates its own instance

static void BM_MT_Set_Simple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;  // one per thread
    const char* sql = "SET @@session.wait_timeout = 600";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_MT_Set_Simple)->Threads(1)->Threads(2)->Threads(4)->Threads(8);

static void BM_MT_Select_Simple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT col FROM t WHERE id = 1";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_MT_Select_Simple)->Threads(1)->Threads(2)->Threads(4)->Threads(8);

static void BM_MT_Select_Complex(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql =
        "SELECT u.id, u.name, COUNT(o.id) AS order_count "
        "FROM users u "
        "LEFT JOIN orders o ON u.id = o.user_id "
        "WHERE u.status = 'active' AND u.created_at > '2024-01-01' "
        "GROUP BY u.id, u.name "
        "HAVING COUNT(o.id) > 5 "
        "ORDER BY order_count DESC "
        "LIMIT 50 OFFSET 10";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);
    }
}
BENCHMARK(BM_MT_Select_Complex)->Threads(1)->Threads(2)->Threads(4)->Threads(8);

static void BM_MT_Classify_Begin(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "BEGIN";
    size_t len = strlen(sql);
    for (auto _ : state) {
        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.stmt_type);
    }
}
BENCHMARK(BM_MT_Classify_Begin)->Threads(1)->Threads(2)->Threads(4)->Threads(8);

// ========== Percentile latency benchmarks ==========
// Custom benchmarks that collect per-iteration timing for percentile analysis.
// Collects individual latencies inside the benchmark loop, then computes
// percentiles after the loop completes. Only the parse call is timed;
// timestamp collection overhead is excluded via PauseTiming/ResumeTiming.

static void BM_Percentile_Set_Simple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET @@session.wait_timeout = 600";
    size_t len = strlen(sql);

    std::vector<double> latencies;
    latencies.reserve(1 << 20);
    for (auto _ : state) {
        state.PauseTiming();
        auto start = std::chrono::high_resolution_clock::now();
        state.ResumeTiming();

        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);

        state.PauseTiming();
        auto end = std::chrono::high_resolution_clock::now();
        latencies.push_back(std::chrono::duration<double, std::nano>(end - start).count());
        state.ResumeTiming();
    }

    if (!latencies.empty()) {
        std::sort(latencies.begin(), latencies.end());
        size_t N = latencies.size();
        double sum = 0;
        for (double l : latencies) sum += l;
        state.counters["avg_ns"] = sum / N;
        state.counters["p50_ns"] = latencies[N * 50 / 100];
        state.counters["p95_ns"] = latencies[N * 95 / 100];
        state.counters["p99_ns"] = latencies[N * 99 / 100];
        state.counters["min_ns"] = latencies[0];
        state.counters["max_ns"] = latencies[N - 1];
    }
}
BENCHMARK(BM_Percentile_Set_Simple);

static void BM_Percentile_Select_Simple(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT col FROM t WHERE id = 1";
    size_t len = strlen(sql);

    std::vector<double> latencies;
    latencies.reserve(1 << 20);
    for (auto _ : state) {
        state.PauseTiming();
        auto start = std::chrono::high_resolution_clock::now();
        state.ResumeTiming();

        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);

        state.PauseTiming();
        auto end = std::chrono::high_resolution_clock::now();
        latencies.push_back(std::chrono::duration<double, std::nano>(end - start).count());
        state.ResumeTiming();
    }

    if (!latencies.empty()) {
        std::sort(latencies.begin(), latencies.end());
        size_t N = latencies.size();
        double sum = 0;
        for (double l : latencies) sum += l;
        state.counters["avg_ns"] = sum / N;
        state.counters["p50_ns"] = latencies[N * 50 / 100];
        state.counters["p95_ns"] = latencies[N * 95 / 100];
        state.counters["p99_ns"] = latencies[N * 99 / 100];
        state.counters["min_ns"] = latencies[0];
        state.counters["max_ns"] = latencies[N - 1];
    }
}
BENCHMARK(BM_Percentile_Select_Simple);

static void BM_Percentile_Select_Complex(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql =
        "SELECT u.id, u.name, COUNT(o.id) AS order_count "
        "FROM users u "
        "LEFT JOIN orders o ON u.id = o.user_id "
        "WHERE u.status = 'active' "
        "GROUP BY u.id, u.name "
        "HAVING COUNT(o.id) > 5 "
        "ORDER BY order_count DESC "
        "LIMIT 50";
    size_t len = strlen(sql);

    std::vector<double> latencies;
    latencies.reserve(1 << 20);
    for (auto _ : state) {
        state.PauseTiming();
        auto start = std::chrono::high_resolution_clock::now();
        state.ResumeTiming();

        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.ast);

        state.PauseTiming();
        auto end = std::chrono::high_resolution_clock::now();
        latencies.push_back(std::chrono::duration<double, std::nano>(end - start).count());
        state.ResumeTiming();
    }

    if (!latencies.empty()) {
        std::sort(latencies.begin(), latencies.end());
        size_t N = latencies.size();
        double sum = 0;
        for (double l : latencies) sum += l;
        state.counters["avg_ns"] = sum / N;
        state.counters["p50_ns"] = latencies[N * 50 / 100];
        state.counters["p95_ns"] = latencies[N * 95 / 100];
        state.counters["p99_ns"] = latencies[N * 99 / 100];
        state.counters["min_ns"] = latencies[0];
        state.counters["max_ns"] = latencies[N - 1];
    }
}
BENCHMARK(BM_Percentile_Select_Complex);

static void BM_Percentile_Classify_Begin(benchmark::State& state) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "BEGIN";
    size_t len = strlen(sql);

    std::vector<double> latencies;
    latencies.reserve(1 << 20);
    for (auto _ : state) {
        state.PauseTiming();
        auto start = std::chrono::high_resolution_clock::now();
        state.ResumeTiming();

        auto r = parser.parse(sql, len);
        benchmark::DoNotOptimize(r.stmt_type);

        state.PauseTiming();
        auto end = std::chrono::high_resolution_clock::now();
        latencies.push_back(std::chrono::duration<double, std::nano>(end - start).count());
        state.ResumeTiming();
    }

    if (!latencies.empty()) {
        std::sort(latencies.begin(), latencies.end());
        size_t N = latencies.size();
        double sum = 0;
        for (double l : latencies) sum += l;
        state.counters["avg_ns"] = sum / N;
        state.counters["p50_ns"] = latencies[N * 50 / 100];
        state.counters["p95_ns"] = latencies[N * 95 / 100];
        state.counters["p99_ns"] = latencies[N * 99 / 100];
        state.counters["min_ns"] = latencies[0];
        state.counters["max_ns"] = latencies[N - 1];
    }
}
BENCHMARK(BM_Percentile_Classify_Begin);
