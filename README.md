# ParserSQL

A high-performance, hand-written recursive descent SQL parser for [ProxySQL](https://github.com/sysown/proxysql). Supports both MySQL and PostgreSQL dialects with compile-time dispatch — zero runtime overhead for dialect selection.

## Performance

All operations run in sub-microsecond latency on modern hardware:

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

## Features

- **Deep parsing (Tier 1):** SELECT, INSERT, UPDATE, DELETE, SET, REPLACE, EXPLAIN, CALL, DO, LOAD DATA
- **Compound queries:** UNION / INTERSECT / EXCEPT with SQL-standard precedence and parenthesized nesting
- **Tier 2 classification:** All other statement types (DDL, transactions, SHOW, GRANT, etc.)
- **Query reconstruction:** Parse → modify AST → emit valid SQL
- **Query digest:** Normalize queries for fingerprinting (literals → `?`, IN list collapsing, keyword uppercasing) with 64-bit FNV-1a hash
- **Prepared statement cache:** LRU cache with `parse_and_cache()` / `execute()` for binary protocol support
- **Both dialects:** MySQL and PostgreSQL via `Parser<Dialect::MySQL>` / `Parser<Dialect::PostgreSQL>`
- **Thread-safe:** One parser instance per thread, zero shared state, no locks

## Quick Start

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

// Modify AST and re-emit
// ... modify nodes ...
// Emitter emitter2(parser.arena());
// emitter2.emit(result.ast);  // emits modified SQL

// Reset arena after each query (O(1), reuses memory)
parser.reset();
```

## Building

```bash
# Build library + run tests
make -f Makefile.new all

# Build and run benchmarks
make -f Makefile.new bench

# Build comparison benchmarks (requires libpg_query)
cd third_party/libpg_query && make && cd ../..
make -f Makefile.new bench-compare
```

Requires: `g++` or `clang++` with C++17 support. No external dependencies for the parser itself. Google Test and Google Benchmark are vendored in `third_party/`.

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
       │                Full AST in arena
       │
       └──── Tier 2 ──► Lightweight extractor
                          │
                          ▼
                        StmtType + table name + metadata
```

**Key design decisions:**
- **Arena allocator** — 64KB bump allocator, O(1) reset. All AST nodes allocated from arena. No per-node new/delete.
- **Zero-copy StringRef** — Token values point into original input buffer. No string copies during parsing.
- **32-byte AstNode** — Compact intrusive linked-list (first_child + next_sibling). Half a cache line.
- **Compile-time dialect dispatch** — `if constexpr` for MySQL vs PostgreSQL differences. Zero runtime overhead.
- **Header-only parsers** — Maximum inlining opportunity. Only `arena.cpp` and `parser.cpp` are compiled separately.

## Testing

430 unit tests + validated against 86K+ queries from 9 external corpora:

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

src/sql_parser/
    arena.cpp             Arena implementation
    parser.cpp            Classifier + integration

tests/                    430 unit tests (Google Test)
bench/                    18 benchmarks + comparison suite
```

## License

See [LICENSE](LICENSE) file.
