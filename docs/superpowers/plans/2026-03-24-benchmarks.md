# Performance Benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Google Benchmark-based performance tests to validate the parser meets its latency targets and catch performance regressions.

**Architecture:** Benchmarks use Google Benchmark (vendored alongside Google Test). Each benchmark measures a specific operation in isolation: Tier 2 classification, Tier 1 SET parse, Tier 1 SELECT parse (simple and complex), query reconstruction (round-trip), and arena reset. A single parser instance is reused across iterations (matching ProxySQL's per-thread usage pattern).

**Tech Stack:** C++17, Google Benchmark

**Spec:** `docs/superpowers/specs/2026-03-24-sql-parser-design.md` (Performance Targets section)

---

## Scope

1. Vendor Google Benchmark
2. Benchmark targets matching the spec:
   - Tier 2 classification: <100ns
   - Tier 1 SET parse: <300ns
   - Tier 1 SELECT parse (simple): <500ns
   - Tier 1 SELECT parse (complex): <2us
   - Query reconstruction: <500ns
   - Arena reset: <10ns
3. Makefile.new `bench` target

**Not in scope:** CI integration for benchmarks (too noisy in CI), optimization work.

---

## File Structure

```
bench/
    bench_main.cpp        — Google Benchmark main
    bench_parser.cpp      — All parser benchmarks

Makefile.new              — (modify) Add bench target
```

---

### Task 1: Benchmark Setup and All Benchmarks

**Files:**
- Create: `bench/bench_main.cpp`
- Create: `bench/bench_parser.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Vendor Google Benchmark**

```bash
git clone --depth 1 --branch v1.9.1 https://github.com/google/benchmark.git third_party/benchmark
```

- [ ] **Step 2: Create bench_main.cpp**

Create `bench/bench_main.cpp`:
```cpp
#include <benchmark/benchmark.h>

BENCHMARK_MAIN();
```

- [ ] **Step 3: Create bench_parser.cpp**

Create `bench/bench_parser.cpp`:
```cpp
#include <benchmark/benchmark.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

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
```

- [ ] **Step 4: Update Makefile.new**

Add benchmark build rules to `Makefile.new`:

```makefile
# Google Benchmark
GBENCH_DIR = $(PROJECT_ROOT)/third_party/benchmark
### NOTE: After cloning Google Benchmark v1.9.1, verify the actual source files:
###   ls third_party/benchmark/src/*.cc
### Then set GBENCH_SRCS to match. The following is a common set:
GBENCH_SRCS = $(wildcard $(GBENCH_DIR)/src/*.cc)
GBENCH_OBJS = $(GBENCH_SRCS:.cc=.o)
GBENCH_CPPFLAGS = -I$(GBENCH_DIR)/include -I$(GBENCH_DIR)/src -DHAVE_STD_REGEX -DHAVE_STEADY_CLOCK

BENCH_DIR = $(PROJECT_ROOT)/bench
BENCH_SRCS = $(BENCH_DIR)/bench_main.cpp $(BENCH_DIR)/bench_parser.cpp
BENCH_OBJS = $(BENCH_SRCS:.cpp=.o)
BENCH_TARGET = $(PROJECT_ROOT)/run_bench

# Benchmark objects
$(GBENCH_DIR)/src/%.o: $(GBENCH_DIR)/src/%.cc
	$(CXX) $(CXXFLAGS) $(GBENCH_CPPFLAGS) -c $< -o $@

$(BENCH_DIR)/%.o: $(BENCH_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(GBENCH_CPPFLAGS) -c $< -o $@

bench: $(BENCH_TARGET)
	./$(BENCH_TARGET) --benchmark_format=console

$(BENCH_TARGET): $(BENCH_OBJS) $(GBENCH_OBJS) $(LIB_TARGET)
	$(CXX) $(CXXFLAGS) -o $@ $(BENCH_OBJS) $(GBENCH_OBJS) -L$(PROJECT_ROOT) -lsqlparser -lpthread
```

Add `bench` to `.PHONY` and update `clean` to include bench artifacts:
```makefile
.PHONY: all lib test bench clean
```

Add to clean:
```makefile
	rm -f $(BENCH_OBJS) $(GBENCH_OBJS) $(BENCH_TARGET)
```

- [ ] **Step 5: Create bench directory and build**

```bash
mkdir -p bench
make -f Makefile.new clean && make -f Makefile.new lib && make -f Makefile.new test && make -f Makefile.new bench
```

- [ ] **Step 6: Update .gitignore**

Add to `.gitignore`:
```
run_bench
```

- [ ] **Step 7: Commit**

```bash
git add bench/ third_party/benchmark Makefile.new .gitignore
git commit -m "feat: add Google Benchmark performance tests for parser operations"
```
