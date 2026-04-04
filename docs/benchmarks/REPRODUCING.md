# Reproducing Parser Comparison Benchmarks

Step-by-step instructions to reproduce all comparison benchmarks from scratch on any Linux x86_64 machine.

---

## Prerequisites

```bash
# Required tools
sudo apt-get update
sudo apt-get install -y build-essential git curl

# Rust (for sqlparser-rs benchmark)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

# Verify
g++ --version    # need GCC 8+ (C++17 support)
cargo --version  # need Rust 1.70+
git --version
```

---

## Step 1: Clone ParserSQL

```bash
git clone https://github.com/ProxySQL/ParserSQL.git
cd ParserSQL
```

---

## Step 2: Build ParserSQL (release mode)

```bash
# Create release Makefile (-O3, no debug symbols)
sed 's/-g -O2/-O3/' Makefile > Makefile.release

# Build the parser library
make -f Makefile.release lib

# Verify: run unit tests
make -f Makefile.release test
# Expected output: [  PASSED  ] 430 tests.
```

---

## Step 3: Build libpg_query

libpg_query is PostgreSQL's parser extracted as a standalone C library.

```bash
# Clone libpg_query (vendored in third_party/)
# If not already present:
if [ ! -d third_party/libpg_query ]; then
    git clone --depth 1 https://github.com/pganalyze/libpg_query.git third_party/libpg_query
fi

# Build libpg_query
cd third_party/libpg_query
make clean
make -j$(nproc)
cd ../..

# Verify the static library was built
ls -la third_party/libpg_query/libpg_query.a
# Expected: ~30MB static library
```

**What libpg_query is:** This is PostgreSQL's actual parser (Bison-generated), extracted from the PostgreSQL source code by pganalyze. It compiles PostgreSQL's parser, lexer, memory management, and node types into a standalone library. The `pg_query_parse()` function takes a SQL string and returns a JSON-serialized parse tree. The `pg_query_raw_parse()` function (internal API) returns the raw AST without JSON serialization.

**Optimization flags:** libpg_query builds with `-O3 -g` by default (see its Makefile: `CFLAGS_OPT_LEVEL = -O3`). The `-g` flag adds debug symbols but does not affect runtime performance. Both ParserSQL and libpg_query are compiled at `-O3` — this is a fair comparison.

---

## Step 4: Build the comparison benchmark

```bash
# Build the comparison benchmark binary
make -f Makefile.release bench-compare

# Verify the binary exists
ls -la run_bench_compare
```

**What gets built:** A single binary (`run_bench_compare`) that contains Google Benchmark harness + our parser + libpg_query. It benchmarks the same SQL queries through both parsers for a direct comparison.

---

## Step 5: Run ParserSQL vs libpg_query benchmark

```bash
./run_bench_compare --benchmark_format=console
```

**Expected output** (numbers will vary by machine):

```
---------------------------------------------------------------------
Benchmark                           Time             CPU   Iterations
---------------------------------------------------------------------
BM_Ours_Select_Simple             223 ns          223 ns      3043637
BM_PgQuery_Select_Simple         1872 ns         1871 ns       374142
BM_PgQueryRaw_Select_Simple       684 ns          684 ns      1025094
BM_Ours_Select_Join               579 ns          579 ns      1210315
BM_PgQuery_Select_Join           4509 ns         4506 ns       154785
BM_PgQueryRaw_Select_Join        1646 ns         1646 ns       425588
... etc
```

**Reading the results:**
- `BM_Ours_*` — ParserSQL (this project)
- `BM_PgQuery_*` — libpg_query with JSON serialization (`pg_query_parse()`)
- `BM_PgQueryRaw_*` — libpg_query parse-only, no JSON (`pg_query_raw_parse()`)
- The `BM_PgQueryRaw_*` numbers are the **fair comparison** (parse-only vs parse-only)

**To save results as JSON** (for automated comparison):
```bash
./run_bench_compare --benchmark_format=json > comparison_results.json
```

---

## Step 6: Build and run sqlparser-rs benchmark

```bash
cd bench/sqlparser_rs_bench

# Build and run (Rust criterion benchmark)
cargo bench

cd ../..
```

**Expected output:**

```
sqlparser_rs_mysql_simple_select
                        time:   [4.6 µs 4.7 µs 4.7 µs]
sqlparser_rs_mysql_select_join
                        time:   [10.8 µs 10.9 µs 11.0 µs]
... etc
```

**Reading the results:** The `time:` line shows `[lower_bound median upper_bound]`. Compare the median value against ParserSQL's numbers from Step 5.

**Note:** criterion outputs results to `bench/sqlparser_rs_bench/target/criterion/` with HTML reports you can open in a browser:
```bash
open bench/sqlparser_rs_bench/target/criterion/report/index.html  # macOS
xdg-open bench/sqlparser_rs_bench/target/criterion/report/index.html  # Linux
```

---

## Step 7: Run the automated comparison script

The `scripts/run_comparison.sh` script runs all comparisons in sequence:

```bash
./scripts/run_comparison.sh
```

This produces console output with all three parser comparisons side by side.

---

## Step 8: Generate the full benchmark report

```bash
./scripts/run_benchmarks.sh docs/benchmarks/latest.md
```

This generates the complete performance report including:
- Single-threaded benchmarks (18 operations)
- Multi-threaded scaling (1/2/4/8 threads)
- Percentile latency (avg/p50/p95/p99)
- Corpus test results (9 corpora, 86K+ queries)

---

## Troubleshooting

### libpg_query build fails

```bash
# libpg_query needs standard C build tools
sudo apt-get install -y make gcc flex bison

# If you get "redefinition" errors, make sure you're using a clean clone:
cd third_party/libpg_query
make clean
make -j$(nproc)
```

### Rust build fails

```bash
# Ensure Rust is up to date
rustup update stable

# If criterion fails to download, check network/proxy
cd bench/sqlparser_rs_bench
cargo update
cargo bench
```

### Benchmark numbers are unstable

For the most reliable results:

```bash
# 1. Disable CPU frequency scaling
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# 2. Disable turbo boost (Intel)
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
# Or (AMD)
echo 0 | sudo tee /sys/devices/system/cpu/cpufreq/boost

# 3. Pin to a specific CPU core
taskset -c 0 ./run_bench_compare

# 4. Increase benchmark iterations for more stable results
./run_bench_compare --benchmark_min_time=5s
```

### Comparing across different machines

Absolute numbers vary by CPU. Compare **ratios** instead:
- ParserSQL / libpg_query ratio should be ~3x regardless of machine
- ParserSQL / sqlparser-rs ratio should be ~15-20x regardless of machine

---

## Queries Used in Benchmarks

All parsers are benchmarked on the same set of queries:

```sql
-- simple_select
SELECT col FROM t WHERE id = 1

-- select_join
SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'

-- select_complex
SELECT u.id, u.name, COUNT(o.id) AS order_count
FROM users u LEFT JOIN orders o ON u.id = o.user_id
WHERE u.status = 'active'
GROUP BY u.id, u.name
HAVING COUNT(o.id) > 5
ORDER BY order_count DESC LIMIT 50

-- insert_values
INSERT INTO users (name, email) VALUES ('John', 'john@example.com')

-- update_simple
UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'

-- delete_simple
DELETE FROM users WHERE id = 42

-- set_simple
SET @@session.wait_timeout = 600

-- set_names
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci

-- begin
BEGIN

-- create_table
CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))
```

---

## Reference Results

These were measured on AMD Ryzen 9 5950X, Linux 6.17, GCC 13.3 -O3:

| Query | ParserSQL | pg_query (raw) | pg_query (+JSON) | sqlparser-rs |
|---|---|---|---|---|
| SELECT simple | 223 ns | 684 ns (3.1x) | 1,872 ns (8.4x) | 4,687 ns (21x) |
| SELECT JOIN | 579 ns | 1,646 ns (2.8x) | 4,509 ns (7.8x) | 10,684 ns (18x) |
| SELECT complex | 1,189 ns | 3,304 ns (2.8x) | 8,675 ns (7.3x) | 23,411 ns (19x) |
| INSERT | 244 ns | 781 ns (3.2x) | 1,831 ns (7.5x) | 3,784 ns (16x) |
| BEGIN | 36 ns | 230 ns (6.4x) | 421 ns (11.7x) | 412 ns (11x) |
