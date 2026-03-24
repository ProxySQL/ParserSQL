# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance hand-written recursive descent SQL parser for ProxySQL. Supports MySQL and PostgreSQL dialects via compile-time templating (`Parser<Dialect::MySQL>` / `Parser<Dialect::PostgreSQL>`). Designed for sub-microsecond latency on the proxy hot path.

## Build Commands

```bash
make -f Makefile.new all           # Build library + run all 430 tests
make -f Makefile.new lib           # Build only libsqlparser.a
make -f Makefile.new test          # Build + run tests
make -f Makefile.new bench         # Build + run benchmarks
make -f Makefile.new bench-compare # Run comparison vs libpg_query (requires libpg_query built)
make -f Makefile.new build-corpus-test  # Build corpus test harness
make -f Makefile.new clean         # Remove all build artifacts
```

For release benchmarks: `sed 's/-g -O2/-O3/' Makefile.new > /tmp/Makefile.release && make -f /tmp/Makefile.release bench`

**Note:** The old `Makefile` (no `.new`) is for the legacy Flex/Bison parser — do not use it for new code.

## Architecture

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
9. Write tests in `tests/test_xxx.cpp`, add to `Makefile.new` TEST_SRCS

### Expression parsing

`ExpressionParser<D>` uses Pratt parsing (precedence climbing). Used by all Tier 1 parsers for WHERE conditions, SET values, function args, etc. Handles: literals, identifiers, binary/unary ops, IS NULL, BETWEEN, IN, NOT IN/BETWEEN/LIKE, CASE/WHEN, function calls, subqueries, ARRAY constructors, tuple constructors, field access.

### Table reference parsing

`TableRefParser<D>` is a shared utility extracted from SelectParser. Used by SELECT (FROM), UPDATE (MySQL multi-table), DELETE (MySQL multi-table), INSERT (for INSERT...SELECT). Handles: simple tables, qualified names, aliases, JOINs (all types), subqueries in FROM.

### Emitter

`Emitter<D>` walks AST and produces SQL text into arena-backed `StringBuilder`. Supports:
- Normal mode: faithful round-trip reconstruction
- Digest mode (`EmitMode::DIGEST`): literals→`?`, IN collapsing, keyword uppercasing
- Bindings mode: materializes `?` placeholders with bound parameter values

### Tests

Google Test. 430 tests across 16 test files. Validated against 86K+ external queries (PostgreSQL regression, MySQL MTR, CockroachDB, Vitess, TiDB, sqlparser-rs, SQLGlot).

Run a single test: `./run_tests --gtest_filter="*SetTest*"`

### Benchmarks

Google Benchmark. 18 single-thread + 16 multi-thread + 4 percentile benchmarks.
Comparison benchmarks against libpg_query and sqlparser-rs in `bench/bench_comparison.cpp`.

### Corpus testing

`corpus_test` binary reads SQL from stdin (one per line), parses each, reports OK/PARTIAL/ERROR counts.
Usage: `./corpus_test mysql < queries.sql` or `./corpus_test pgsql < queries.sql`
