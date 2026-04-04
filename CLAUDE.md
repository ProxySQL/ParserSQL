# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance hand-written recursive descent SQL parser and query engine for ProxySQL. Supports MySQL and PostgreSQL dialects via compile-time templating (`Parser<Dialect::MySQL>` / `Parser<Dialect::PostgreSQL>`). Designed for sub-microsecond latency on the proxy hot path. The query engine takes parsed ASTs and executes them through a Volcano-model operator pipeline.

## Build Commands

```bash
make all           # Build library + run all 871 tests
make lib           # Build only libsqlparser.a
make test          # Build + run tests
make bench         # Build + run benchmarks
make bench-compare # Run comparison vs libpg_query (requires libpg_query built)
make build-corpus-test  # Build corpus test harness
make clean         # Remove all build artifacts
```

For release benchmarks: `sed 's/-g -O2/-O3/' Makefile > /tmp/Makefile.release && make -f /tmp/Makefile.release bench`

## Parser Architecture

### Three-layer pipeline

1. **Tokenizer** (`tokenizer.h`) — Zero-copy pull-based iterator, dialect-templated. Keyword lookup via sorted-array binary search. Produces `Token{type, StringRef, offset}`.
2. **Classifier** (`parser.cpp:classify_and_dispatch()`) — Switch on first token. Routes to Tier 1 deep parser or Tier 2 extractor.
3. **Statement parsers** — Each Tier 1 statement has its own header-only template class (e.g., `SelectParser<D>`, `SetParser<D>`).

### Key types

- `Arena` — Block-chained bump allocator. 64KB default, 1MB max. O(1) reset.
- `StringRef` — `{const char* ptr, uint32_t len}`. Zero-copy view into input buffer. Trivially copyable.
- `AstNode` — 32 bytes. Intrusive linked list (first_child + next_sibling). Arena-allocated.
- `ParseResult` — Status (OK/PARTIAL/ERROR) + stmt_type + ast + table_name/schema_name + remaining (for multi-statement).

### Namespace

Everything is in `namespace sql_parser`. All templates are parameterized on `Dialect D` (MySQL or PostgreSQL).

### Adding a new deep parser

1. Create `include/sql_parser/xxx_parser.h` — header-only template following `SetParser<D>` pattern
2. Add node types to `NodeType` enum in `common.h`
3. Add tokens to `token.h` and both keyword tables (sorted!)
4. Add `parse_xxx()` method to `parser.h` and implement in `parser.cpp`
5. Update `classify_and_dispatch()` switch to route to new parser
6. Add emit methods to `emitter.h`
7. Add `is_keyword_as_identifier()` entries in `expression_parser.h` for new keywords
8. Update `is_alias_start()` blocklist in `table_ref_parser.h` for clause-starting keywords
9. Write tests in `tests/test_xxx.cpp`, add to `Makefile` TEST_SRCS

### Expression parsing

`ExpressionParser<D>` uses Pratt parsing (precedence climbing). Used by all Tier 1 parsers for WHERE conditions, SET values, function args, etc. Handles: literals, identifiers, binary/unary ops, IS NULL, BETWEEN, IN, NOT IN/BETWEEN/LIKE, CASE/WHEN, function calls, subqueries, ARRAY constructors, tuple constructors, field access.

### Table reference parsing

`TableRefParser<D>` is a shared utility extracted from SelectParser. Used by SELECT (FROM), UPDATE (MySQL multi-table), DELETE (MySQL multi-table), INSERT (for INSERT...SELECT). Handles: simple tables, qualified names, aliases, JOINs (all types), subqueries in FROM.

### Emitter

`Emitter<D>` walks AST and produces SQL text into arena-backed `StringBuilder`. Supports:
- Normal mode: faithful round-trip reconstruction
- Digest mode (`EmitMode::DIGEST`): literals→`?`, IN collapsing, keyword uppercasing
- Bindings mode: materializes `?` placeholders with bound parameter values

## Query Engine

### Architecture

The engine follows a five-component pipeline:

1. **Type System** (`types.h`, `value.h`) — `SqlType` describes column types (30+ kinds). `Value` is a 14-tag discriminated union for runtime values (null, bool, int64, uint64, double, decimal, string, bytes, date, time, datetime, timestamp, interval, json).
2. **Expression Evaluator** (`expression_eval.h`) — Recursively evaluates AST expression nodes against a row. Handles arithmetic, comparisons, boolean logic (three-valued), BETWEEN, IN, LIKE, CASE/WHEN, function calls. Uses `CoercionRules<D>` for type promotion and `null_semantics` for NULL propagation.
3. **Catalog** (`catalog.h`, `in_memory_catalog.h`) — Abstract interface for table/column metadata. `InMemoryCatalog` is the hash-map implementation. `CatalogResolver` creates column-resolve callbacks from catalog + table + row.
4. **Plan Builder** (`plan_builder.h`) — Translates a SELECT AST into a `PlanNode` tree. Translation order: FROM (Scan/Join) → WHERE (Filter) → GROUP BY (Aggregate) → HAVING (Filter) → SELECT list (Project) → DISTINCT → ORDER BY (Sort) → LIMIT.
5. **Executor** (`plan_executor.h`) — Converts a `PlanNode` tree into an `Operator` tree (Volcano model: open/next/close). Pulls rows through the tree and collects them into a `ResultSet`.

### Key types

- `Value` — 14-tag discriminated union. Constructors: `value_null()`, `value_int(i)`, `value_string(s)`, etc.
- `SqlType` — Column type descriptor with `Kind` enum, precision, scale, unsigned flag, timezone flag.
- `Row` — `{Value* values, uint16_t column_count}`. Arena-allocated via `make_row()`.
- `PlanNode` — Arena-allocated union node. Types: SCAN, FILTER, PROJECT, JOIN, AGGREGATE, SORT, LIMIT, DISTINCT, SET_OP.
- `Operator` — Abstract base class with `open()`, `next(Row&)`, `close()`. Nine implementations in `operators/`.
- `ResultSet` — `{vector<Row> rows, vector<string> column_names, uint16_t column_count}`.

### How to add a new operator

1. Create `include/sql_engine/operators/xxx_op.h` — implement `Operator` (open/next/close)
2. Add a new `PlanNodeType` enum value in `plan_node.h` and a corresponding union member in `PlanNode`
3. Add `build_xxx()` in `PlanExecutor` (in `plan_executor.h`) and wire it into `build_operator()` switch
4. Add translation logic in `PlanBuilder::build_select()` (in `plan_builder.h`)
5. Include the new header in `plan_executor.h`
6. Write tests in `tests/test_operators.cpp` or a new test file

### How to add a new SQL function

1. Write the function in the appropriate file under `include/sql_engine/functions/` (arithmetic.h, string.h, comparison.h, or cast.h). Signature: `Value fn(const Value* args, uint16_t arg_count, Arena& arena)`.
2. Register it in `src/sql_engine/function_registry.cpp` inside `register_builtins()`:
   ```cpp
   register_function({"MYFUNC", 6, my_func_impl, 1, 2});
   //                  name, name_len, impl, min_args, max_args (255=variadic)
   ```
3. Write tests in `tests/test_string_funcs.cpp`, `tests/test_arithmetic.cpp`, or `tests/test_registry.cpp`

### How to implement a custom DataSource

Implement the `DataSource` interface in `data_source.h`:

```cpp
class MySource : public DataSource {
    const TableInfo* table_info() const override;  // return table metadata
    void open() override;                           // initialize cursor
    bool next(Row& out) override;                   // fill row, return false when done
    void close() override;                          // release resources
};
```

Then register it with the executor: `executor.add_data_source("table_name", &my_source);`

### How to implement a custom Catalog

Implement the `Catalog` interface in `catalog.h`:

```cpp
class MyCatalog : public Catalog {
    const TableInfo* get_table(StringRef name) const override;
    const TableInfo* get_table(StringRef schema, StringRef table) const override;
    const ColumnInfo* get_column(const TableInfo* table, StringRef column_name) const override;
};
```

`TableInfo` must outlive any queries that reference it. `ColumnInfo::ordinal` must match the column position in rows returned by the corresponding `DataSource`.

### Engine namespace

Everything is in `namespace sql_engine`. Templates are parameterized on `Dialect D` where dialect-specific behavior applies (coercion rules, `||` semantics, LIKE matching).

## Tests

Google Test. 1,008 tests across 34 test files. Validated against 86K+ external queries (PostgreSQL regression, MySQL MTR, CockroachDB, Vitess, TiDB, sqlparser-rs, SQLGlot).

Run a single test: `./run_tests --gtest_filter="*SetTest*"`

### Test files by component

**Parser:**
`test_tokenizer.cpp`, `test_classifier.cpp`, `test_expression.cpp`, `test_select.cpp`, `test_insert.cpp`, `test_update.cpp`, `test_delete.cpp`, `test_set.cpp`, `test_compound.cpp`, `test_emitter.cpp`, `test_digest.cpp`, `test_stmt_cache.cpp`, `test_arena.cpp`, `test_misc_stmts.cpp`

**Engine:**
`test_value.cpp`, `test_row.cpp`, `test_coercion.cpp`, `test_null_semantics.cpp`, `test_like.cpp`, `test_expression_eval.cpp`, `test_eval_integration.cpp`, `test_catalog.cpp`, `test_registry.cpp`, `test_arithmetic.cpp`, `test_comparison.cpp`, `test_cast.cpp`, `test_string_funcs.cpp`, `test_operators.cpp`, `test_plan_builder.cpp`, `test_plan_executor.cpp`

## Benchmarks

Google Benchmark. 18 single-thread parser + 7 engine + 16 multi-thread + 4 percentile benchmarks.
Engine benchmarks in `bench/bench_engine.cpp` cover expression evaluation, plan building, pipeline execution, and operators (filter, join, sort, aggregate).
Comparison benchmarks against libpg_query and sqlparser-rs in `bench/bench_comparison.cpp`.

## CLI Tool (sqlengine)

Interactive SQL engine for testing the full pipeline. Build with `make build-sqlengine`.

```bash
# In-memory mode (expression evaluation, no backends)
echo "SELECT 1 + 2, UPPER('hello'), COALESCE(NULL, 42)" | ./sqlengine

# Interactive mode
./sqlengine

# With MySQL backend
./sqlengine --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1"

# Multiple backends with sharding
./sqlengine \
  --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1" \
  --backend "mysql://root:test@127.0.0.1:13307/testdb?name=shard2" \
  --shard "users:id:shard1,shard2"
```

Source: `tools/sqlengine.cpp`

## Corpus testing

`corpus_test` binary reads SQL from stdin (one per line), parses each, reports OK/PARTIAL/ERROR counts.
Usage: `./corpus_test mysql < queries.sql` or `./corpus_test pgsql < queries.sql`
