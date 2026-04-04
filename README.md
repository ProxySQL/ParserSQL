# ParserSQL — High-Performance SQL Parser & Query Engine

A high-performance, hand-written recursive descent SQL parser and composable query engine for [ProxySQL](https://github.com/sysown/proxysql). Supports both MySQL and PostgreSQL dialects with compile-time dispatch — zero runtime overhead for dialect selection. The parser produces an AST that feeds directly into the query engine's plan builder and executor pipeline.

## Performance

All parser operations run in sub-microsecond latency on modern hardware:

| Operation | Latency | Notes |
|---|---|---|
| Classify statement (BEGIN) | **29 ns** | Tier 2: type + metadata only |
| Parse SET statement | **111 ns** | Full AST |
| Parse simple SELECT | **186 ns** | Full AST |
| Parse complex SELECT (JOINs, GROUP BY, HAVING) | **1.1 µs** | Full AST |
| Parse INSERT | **212 ns** | Full AST |
| Query reconstruction (round-trip) | **116-226 ns** | Parse → emit |
| Arena reset | **3.5 ns** | O(1) pointer rewind |

Compared to other parsers on the same queries:

| Parser | Simple SELECT | Complex SELECT | Notes |
|---|---|---|---|
| **ParserSQL** | **175 ns** | **975 ns** | This project |
| libpg_query (raw parse) | 718 ns (4.1x slower) | 3,479 ns (3.6x) | PostgreSQL's own parser |
| sqlparser-rs (Rust) | 4,687 ns (27x slower) | 23,411 ns (24x) | Apache DataFusion |

See [docs/benchmarks/](docs/benchmarks/) for full results and [REPRODUCING.md](docs/benchmarks/REPRODUCING.md) for reproduction instructions.

## Quick Start

### Parse and emit SQL

```cpp
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

// Create a parser (one per thread)
Parser<Dialect::MySQL> parser;

// Parse a query
auto result = parser.parse("SELECT * FROM users WHERE id = 1", 32);
if (result.ok()) {
    // result.ast contains the full AST
    // result.stmt_type == StmtType::SELECT
}

// Reconstruct SQL from AST
Emitter<Dialect::MySQL> emitter(parser.arena());
emitter.emit(result.ast);
StringRef sql = emitter.result();  // "SELECT * FROM users WHERE id = 1"

// Query digest (normalize for fingerprinting)
Digest<Dialect::MySQL> digest(parser.arena());
DigestResult dr = digest.compute(result.ast);
// dr.normalized = "SELECT * FROM users WHERE id = ?"
// dr.hash = 0x... (64-bit FNV-1a)

// Reset arena after each query (O(1), reuses memory)
parser.reset();
```

### Full pipeline: parse, plan, execute

```cpp
#include "sql_parser/parser.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"

using namespace sql_parser;
using namespace sql_engine;

// 1. Set up catalog (table metadata)
InMemoryCatalog catalog;
catalog.add_table("", "users", {
    {"id",   SqlType::make_int(),        false},
    {"name", SqlType::make_varchar(255), true},
    {"age",  SqlType::make_int(),        true},
});

// 2. Populate data source
Arena data_arena{65536, 1048576};
std::vector<Row> rows = {
    // ... build rows with make_row() + value_int(), value_string(), etc.
};
const TableInfo* table = catalog.get_table(StringRef{"users", 5});
InMemoryDataSource source(table, std::move(rows));

// 3. Register built-in functions (UPPER, LOWER, COALESCE, etc.)
FunctionRegistry<Dialect::MySQL> functions;
functions.register_builtins();

// 4. Parse SQL
Parser<Dialect::MySQL> parser;
auto result = parser.parse("SELECT name, age FROM users WHERE age > 21", 43);

// 5. Build logical plan (AST -> plan tree)
PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
PlanNode* plan = builder.build(result.ast);

// 6. Execute plan
PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
executor.add_data_source("users", &source);
ResultSet rs = executor.execute(plan);

// 7. Read results
for (size_t i = 0; i < rs.row_count(); ++i) {
    Row& row = rs.rows[i];
    // row.get(0) = name (Value), row.get(1) = age (Value)
}

parser.reset();
```

### Quick Start: CLI tool

```bash
# Build the CLI tool
make build-sqlengine

# In-memory mode — evaluate expressions without any backend
echo "SELECT 1 + 2, UPPER('hello'), COALESCE(NULL, 42)" | ./sqlengine
# +---+-------+----+
# | 3 | HELLO | 42 |
# +---+-------+----+
# 1 row in set (0.000 sec)

# Interactive mode
./sqlengine

# With a MySQL backend
./sqlengine --backend "mysql://root:pass@127.0.0.1:3306/mydb?name=primary"

# Multiple backends with sharding
./sqlengine \
  --backend "mysql://root:pass@host1:3306/db?name=shard1" \
  --backend "mysql://root:pass@host2:3306/db?name=shard2" \
  --shard "users:id:shard1,shard2"
```

## Features

### Parser

- **Deep parsing (Tier 1):** SELECT, INSERT, UPDATE, DELETE, SET, REPLACE, EXPLAIN, CALL, DO, LOAD DATA
- **Compound queries:** UNION / INTERSECT / EXCEPT with SQL-standard precedence and parenthesized nesting
- **Tier 2 classification:** All other statement types (DDL, transactions, SHOW, GRANT, etc.)
- **Query reconstruction:** Parse → modify AST → emit valid SQL
- **Query digest:** Normalize queries for fingerprinting (literals → `?`, IN list collapsing, keyword uppercasing) with 64-bit FNV-1a hash
- **Prepared statement cache:** LRU cache with `parse_and_cache()` / `execute()` for binary protocol support
- **Both dialects:** MySQL and PostgreSQL via `Parser<Dialect::MySQL>` / `Parser<Dialect::PostgreSQL>`
- **Thread-safe:** One parser instance per thread, zero shared state, no locks

### Query Engine

- **Type system:** 30+ SQL types (`SqlType::Kind`) with 14-tag runtime `Value` (null, bool, int64, uint64, double, decimal, string, bytes, date, time, datetime, timestamp, interval, json)
- **Expression evaluator:** Recursive AST evaluator with three-valued logic (NULL propagation), type coercion, short-circuit AND/OR, BETWEEN, IN, LIKE, CASE/WHEN, IS [NOT] NULL
- **Catalog:** Abstract `Catalog` interface + `InMemoryCatalog` implementation for table/column metadata resolution
- **Plan builder:** Translates parsed SELECT AST into a logical plan tree (FROM → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT)
- **Executor with 9 operators:** Scan, Filter, Project, NestedLoopJoin (INNER/LEFT/RIGHT/FULL/CROSS), Aggregate (COUNT/SUM/AVG/MIN/MAX), Sort, Limit, Distinct, SetOp (UNION/INTERSECT/EXCEPT)
- **38 built-in functions:** Arithmetic (ABS, CEIL, FLOOR, ROUND, MOD, POWER, SQRT, LOG, LN, EXP, SIGN, TRUNCATE, RAND, GREATEST, LEAST), string (UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, LTRIM, RTRIM, REPLACE, REVERSE, LEFT, RIGHT, LPAD, RPAD, REPEAT), comparison (COALESCE, NULLIF, IF, IFNULL), type (CAST)
- **Composable data sources:** Implement `DataSource` interface for custom storage backends

## Architecture

```
Input SQL bytes
       │
       ▼
┌──────────────┐
│  Tokenizer   │  Zero-copy, dialect-templated, pull-based
│  <Dialect D> │  Binary search keyword lookup (~110 keywords)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Classifier  │  Switch on first token → route to parser
└──────┬───────┘
       │
       ├──── Tier 1 ──► Deep parser (SELECT, INSERT, UPDATE, DELETE, SET, ...)
       │                  │
       │                  ▼
       │                Full AST in arena ──┐
       │                                    │
       └──── Tier 2 ──► Lightweight         │
                         extractor           │
                                             ▼
                                      ┌──────────────┐
                                      │ Plan Builder  │  AST → logical plan tree
                                      └──────┬───────┘
                                             │
                                             ▼
                                      ┌──────────────┐
                                      │ Plan Executor │  Volcano-model iterator
                                      │  (Operators)  │  open() / next() / close()
                                      └──────┬───────┘
                                             │
                                             ▼
                                         ResultSet
```

**Key design decisions:**
- **Arena allocator** — 64KB bump allocator, O(1) reset. All AST nodes and plan nodes allocated from arena. No per-node new/delete.
- **Zero-copy StringRef** — Token values point into original input buffer. No string copies during parsing.
- **32-byte AstNode** — Compact intrusive linked-list (first_child + next_sibling). Half a cache line.
- **Compile-time dialect dispatch** — `if constexpr` for MySQL vs PostgreSQL differences. Zero runtime overhead.
- **Header-only parsers** — Maximum inlining opportunity. Only `arena.cpp` and `parser.cpp` are compiled separately.
- **Volcano execution model** — Operators implement open/next/close; rows are pulled through the tree one at a time.

## Testing

1,008 unit tests + validated against 86K+ queries from 9 external corpora:

| Corpus | Queries | OK Rate |
|---|---|---|
| PostgreSQL regression suite | 55,553 | 99.6% |
| MySQL MTR test suite | 2,270 | 99.9% |
| CockroachDB parser testdata | 17,429 | 95.1% |
| sqlparser-rs test cases | 2,431 | 99.5% |
| Vitess test cases | 2,291 | 99.8% |
| TiDB test cases | 5,043 | 99.8% |
| SQLGlot fixtures | 1,450 | 98.2% |

## File Layout

```
include/sql_parser/
    parser.h              Public API: Parser<D>
    common.h              StringRef, Dialect, StmtType, NodeType enums
    arena.h               Arena allocator
    ast.h                 AstNode (32 bytes)
    token.h               TokenType enum
    tokenizer.h           Tokenizer<D> (header-only)
    expression_parser.h   Pratt expression parser
    select_parser.h       SELECT deep parser
    set_parser.h          SET deep parser
    insert_parser.h       INSERT/REPLACE deep parser
    update_parser.h       UPDATE deep parser
    delete_parser.h       DELETE deep parser
    compound_query_parser.h  UNION/INTERSECT/EXCEPT
    table_ref_parser.h    Shared FROM/JOIN parsing
    emitter.h             AST → SQL reconstruction
    digest.h              Query normalization + hash
    parse_result.h        ParseResult, BoundValue
    stmt_cache.h          Prepared statement LRU cache
    string_builder.h      Arena-backed string builder
    keywords_mysql.h      MySQL keyword table
    keywords_pgsql.h      PostgreSQL keyword table

include/sql_engine/
    types.h               SqlType with 30+ SQL type kinds
    value.h               Tagged-union Value (14 tags) + constructors
    row.h                 Row: array of Value indexed by ordinal
    catalog.h             Abstract Catalog interface (TableInfo, ColumnInfo)
    in_memory_catalog.h   Hash-map Catalog implementation
    catalog_resolver.h    Column resolver callback factory
    data_source.h         DataSource interface + InMemoryDataSource
    expression_eval.h     Recursive AST expression evaluator
    function_registry.h   FunctionRegistry<D> with register_builtins()
    plan_node.h           PlanNode union (9 node types)
    plan_builder.h        AST → logical plan translation
    plan_executor.h       Plan → operator tree → ResultSet
    operator.h            Abstract Operator base (open/next/close)
    result_set.h          ResultSet: rows + column names
    coercion.h            Type coercion rules (dialect-specific)
    null_semantics.h      Three-valued logic (AND/OR/NOT with NULL)
    like.h                LIKE pattern matching
    tag_kind_map.h        Value::Tag ↔ SqlType::Kind mapping

    operators/
        scan_op.h         Table scan from DataSource
        filter_op.h       WHERE/HAVING predicate evaluation
        project_op.h      SELECT expression list evaluation
        join_op.h         Nested-loop join (5 join types)
        aggregate_op.h    GROUP BY + COUNT/SUM/AVG/MIN/MAX
        sort_op.h         ORDER BY (in-memory sort)
        limit_op.h        LIMIT + OFFSET
        distinct_op.h     Duplicate elimination
        set_op_op.h       UNION/INTERSECT/EXCEPT

    functions/
        arithmetic.h      ABS, CEIL, FLOOR, ROUND, MOD, POWER, SQRT, ...
        comparison.h      COALESCE, NULLIF, IF, IFNULL
        string.h          UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, ...
        cast.h            CAST type conversion

src/sql_parser/
    arena.cpp             Arena implementation
    parser.cpp            Classifier + integration

src/sql_engine/
    function_registry.cpp Built-in function registration
    in_memory_catalog.cpp InMemoryCatalog implementation

tools/
    sqlengine.cpp         Interactive SQL engine CLI tool

tests/                    1,008 unit tests (Google Test)
bench/                    25 benchmarks (parser + engine + comparison)
```

## Building

```bash
# Build library + run tests
make all

# Build and run benchmarks
make bench

# Build comparison benchmarks (requires libpg_query)
cd third_party/libpg_query && make && cd ../..
make bench-compare
```

Requires: `g++` or `clang++` with C++17 support. No external dependencies for the parser itself. Google Test and Google Benchmark are vendored in `third_party/`.

## License

See [LICENSE](LICENSE) file.
