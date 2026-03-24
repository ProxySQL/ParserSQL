# SQL Parser for ProxySQL — Design Specification

## Overview

A high-performance, hand-written recursive descent SQL parser for ProxySQL. Supports both MySQL and PostgreSQL dialects. Designed for sub-microsecond latency on the proxy hot path.

### Goals

- **Two-tier parsing:** Deep parse (full AST + reconstruction) for SELECT and SET. Classify-and-extract for all other statement types.
- **Both dialects from the start:** MySQL and PostgreSQL via compile-time dialect templating. No runtime dispatch overhead.
- **Both protocols:** Text protocol (COM_QUERY) and binary protocol (COM_STMT_PREPARE / COM_STMT_EXECUTE).
- **Query reconstruction:** Parse → modify AST → emit valid SQL. With an option to use read-only inspection mode when reconstruction isn't needed.
- **Sub-microsecond latency:** Arena allocation, zero-copy string references, no exceptions, no virtual dispatch.
- **Thread-safe by isolation:** One parser instance per thread. No shared mutable state, no locks.

### Constraints

- C++17 floor. Optional C++20 features behind `#if __cplusplus >= 202002L` where they provide measurable performance gains.
- Must compile on AlmaLinux 8 (GCC 8.5) through Fedora 43 and macOS (Apple Silicon).
- Static library or header-only (whichever yields better performance after benchmarking). Linked into ProxySQL at build time.

### Migration from Existing POC

This project replaces the existing Flex/Bison-based POC parser wholesale. The old parser (Flex lexer, Bison LALR grammar, `std::string`-based AST with `std::vector<AstNode*>` children, per-node heap allocation) is not carried forward. The existing `src/mysql_parser/`, `src/pgsql_parser/`, and `include/` directories will be removed once the new parser is functional. Existing examples in `examples/` serve as a test corpus for validating that the new parser handles the same queries correctly, then they too will be replaced.

---

## Architecture

Three-layer architecture: Tokenizer → Classifier → Statement Parsers.

```
Input SQL bytes
       │
       ▼
┌──────────────┐
│  Tokenizer   │  Zero-copy, dialect-templated, pull-based
│  <Dialect D> │  Produces Token {type, StringRef, offset}
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Classifier  │  1-3 token lookahead, switch on first keyword
└──────┬───────┘
       │
       ├──── Tier 1 ──► Statement-specific deep parser (SELECT, SET)
       │                  │
       │                  ▼
       │                Full AST in arena → ParseResult
       │
       └──── Tier 2 ──► Lightweight extractor
                          │
                          ▼
                        StmtType + metadata → ParseResult
```

---

## Layer 1: Memory Model & Core Data Structures

### Arena Allocator

Each parser instance owns a thread-local arena — a pre-allocated memory block (64KB default). All AST nodes, materialized strings, and temporary data are allocated from the arena. After a query is fully processed, the arena resets (pointer rewind, O(1)). No per-node new/delete.

**Growth strategy:** The arena uses block chaining — never `realloc` (which would invalidate all pointers). When the current block is exhausted, a new block is allocated and linked. `reset()` retains the first (primary) block and frees any overflow blocks. This means `reset()` is O(1) in the common case (single block) and O(n_overflow_blocks) in the rare case. A configurable maximum arena size (default: 1MB) prevents unbounded growth; exceeding it returns `ParseResult::ERROR`.

```
┌──────────────────────┐    ┌──────────────────────┐
│   Block 1 (64KB)     │───►│  Block 2 (overflow)  │───► ...
│  [AstNode][string].. │    │  [AstNode][string].. │
│                  ^   │    │                  ^   │
│            cursor    │    │            cursor    │
│                      │    │                      │
│  reset() → cursor=0, │    │  (freed on reset)    │
│  free overflow blocks│    │                      │
└──────────────────────┘    └──────────────────────┘
```

### StringRef (Zero-Copy)

```cpp
struct StringRef {
    const char* ptr;
    uint32_t len;

    // Comparison, hashing helpers
};
static_assert(std::is_trivially_copyable_v<StringRef>);
```

`StringRef` must remain a trivial type (no constructors, destructors, or virtual functions) to be safely used in unions and to enable memcpy-based operations.

Points into the original input buffer. No copies or allocations for identifiers, keywords, or literals. The input SQL string must outlive the parse result (natural in ProxySQL — the query buffer is session-owned).

When a string must be materialized (e.g., unescaping a quoted literal), it is allocated from the arena.

### AstNode

Flat, compact struct. No virtual functions, no `std::vector`. Children use an intrusive linked list (first_child + next_sibling):

```cpp
struct AstNode {
    AstNode* first_child;   // 8 bytes — first child (intrusive list)
    AstNode* next_sibling;  // 8 bytes — next sibling
    const char* value_ptr;  // 8 bytes — pointer into input (inlined StringRef)
    uint32_t value_len;     // 4 bytes — length
    NodeType type;          // 2 bytes — enum
    uint16_t flags;         // 2 bytes — dialect bits, tier, modifiers
};
// 32 bytes per node on 64-bit — exactly half a cache line.
// Fields ordered to avoid padding: pointers first, then 4-byte, then 2-byte.
static_assert(sizeof(AstNode) == 32);
```

### ParseResult

```cpp
struct ParseResult {
    enum Status { OK, PARTIAL, ERROR };
    Status status;
    StmtType stmt_type;        // always set, even on error (best-effort)
    AstNode* ast;              // non-null for Tier 1 OK
    ErrorInfo error;           // populated on ERROR/PARTIAL
    StringRef remaining;       // unparsed input after semicolon (for multi-statement)

    // Tier 2 extracted metadata
    StringRef table_name;
    StringRef schema_name;
    // etc.
};

struct ErrorInfo {
    uint32_t offset;           // byte position in input
    StringRef message;         // arena-allocated
};
```

`PARTIAL` semantics by tier:
- **Tier 1:** classifier succeeded (statement type known) but the deep parser hit a syntax error. The AST may be partially populated. ProxySQL can still route on statement type — let the backend report the error.
- **Tier 2:** classifier succeeded but the extractor could not find expected metadata (e.g., `INSERT INTO` with no table name following). `stmt_type` is set but metadata fields may be empty.
- `ERROR` means the first token could not be classified at all (e.g., binary garbage or empty input).

**Lifetime note:** `ErrorInfo::message` points to arena-allocated memory. It becomes invalid after `parser.reset()`. Consumers must copy the message if they need it beyond the parse lifecycle.

---

## Layer 2: Tokenizer

Pull-based iterator. The parser calls `next_token()` which advances the cursor and returns a `Token`. No allocation, no token stream buffering.

### Token

```cpp
struct Token {
    TokenType type;       // enum: keyword, identifier, number, string, operator, etc.
    StringRef text;       // points into original input buffer
    uint32_t offset;      // byte position (for error reporting)
};
```

### Dialect Templating

```cpp
template <Dialect D>
class Tokenizer {
    const char* cursor_;
    const char* end_;
    Token peeked_;         // one-token lookahead cache
    bool has_peeked_;
public:
    void reset(const char* input, size_t len);
    Token next_token();
    Token peek();
    void skip();
};
```

Dialect-specific behavior resolved at compile time via `if constexpr`:

| Feature | MySQL | PostgreSQL |
|---|---|---|
| Quoted identifiers | `` `backtick` `` | `"double quote"` |
| String literals | `'single'` or `"double"` | `'single'`, `$$dollar$$` |
| Comments | `-- `, `#`, `/* */` | `-- `, `/* */` (nested) |
| Operators | `:=` assignment | `::` cast, `~` regex |
| Placeholders | `?` | `$1`, `$2`, ... |

### Keyword Recognition

Perfect hash function (generated at build time or constexpr-computed). SQL has a bounded keyword set per dialect (~200-300 keywords). Single array lookup, O(1).

```cpp
struct KeywordEntry {
    StringRef text;
    TokenType token;
    uint16_t flags;    // reserved vs non-reserved, dialect mask
};
```

### Lookahead

The tokenizer provides single-token lookahead via `peek()`, cached internally. Statement parsers that need multi-token disambiguation (e.g., `SET TRANSACTION` vs `SET var = ...`, or `INSERT INTO ... SELECT` vs `INSERT INTO ... VALUES`) handle this by consuming tokens and using the parser's own state to disambiguate — no backtracking needed. For example, the SET parser consumes the second token; if it's `TRANSACTION`, it enters `parse_set_transaction()`, otherwise it treats the consumed token as the start of a variable target. This is standard recursive descent practice and does not require a multi-token lookahead buffer in the tokenizer itself.

---

## Layer 3: Statement Classifier & Router

Consumes 1-3 tokens to identify statement type. Switch on first token's enum value (jump table).

```cpp
template <Dialect D>
ParseResult Parser<D>::parse(const char* sql, size_t len) {
    tokenizer_.reset(sql, len);
    Token first = tokenizer_.next_token();

    switch (first.type) {
        case TK_SELECT:  return parse_select();
        case TK_SET:     return parse_set();
        case TK_INSERT:  return extract_insert(first);
        case TK_UPDATE:  return extract_update(first);
        case TK_DELETE:  return extract_delete(first);
        case TK_BEGIN:
        case TK_START:   return extract_transaction(first);
        case TK_COMMIT:
        case TK_ROLLBACK:return extract_transaction(first);
        case TK_SHOW:    return extract_show(first);
        case TK_USE:     return extract_use(first);
        case TK_PREPARE: return extract_prepare(first);
        case TK_EXECUTE: return extract_execute(first);
        case TK_CREATE:
        case TK_ALTER:
        case TK_DROP:
        case TK_TRUNCATE:return extract_ddl(first);
        case TK_GRANT:
        case TK_REVOKE:  return extract_acl(first);
        default:         return extract_unknown(first);
    }
}
```

### Tier 2 Extractors

Lightweight — scan forward to extract key pieces without building a full AST. Example: INSERT extractor reads `INTO`, then table name (1-2 tokens for qualified name), returns `ParseResult{stmt_type=INSERT, table=StringRef}`.

### Promotion Path

Promoting a Tier 2 statement to Tier 1: replace the extractor function with a recursive descent parser module. Classifier and everything else unchanged.

---

## Tier 1 Deep Parsers

### SELECT Parser

```
parse_select()
  ├── parse_select_options()      // DISTINCT, ALL, SQL_CALC_FOUND_ROWS (MySQL)
  ├── parse_select_item_list()    // expressions, aliases, *
  │     └── parse_expression()    // Pratt parser: operators, subqueries, functions
  ├── parse_from_clause()
  │     ├── parse_table_ref()
  │     └── parse_join()          // JOIN type, ON/USING
  ├── parse_where_clause()
  │     └── parse_expression()
  ├── parse_group_by()
  ├── parse_having()
  ├── parse_order_by()
  ├── parse_limit()               // LIMIT/OFFSET (MySQL) vs LIMIT/FETCH (PgSQL)
  ├── parse_locking()             // FOR UPDATE/SHARE (dialect-specific)
  └── parse_into()                // INTO OUTFILE/DUMPFILE (MySQL only)
```

**Expression parsing** uses Pratt parsing (precedence climbing). A single `parse_expression(min_precedence)` handles unary, binary, ternary (BETWEEN), IS [NOT] NULL, IN (...), CASE/WHEN, subqueries, and function calls.

**Dialect branching** via `if constexpr (D == Dialect::PostgreSQL)` for:
- `::` type cast
- `LIMIT ALL` vs `LIMIT` with expression
- Dollar-quoted strings

Note: PostgreSQL's `RETURNING` clause applies to INSERT/UPDATE/DELETE, not SELECT. It will be handled when those statements are promoted to Tier 1. Until then, Tier 2 extractors for those statements will detect `RETURNING` and include it in metadata but not build an AST for the returned expressions.

### SET Parser

```
parse_set()
  ├── SET NAMES 'charset' [COLLATE 'collation']
  ├── SET CHARACTER SET 'charset'
  ├── SET TRANSACTION [READ ONLY | READ WRITE | ISOLATION LEVEL ...]
  └── SET [GLOBAL|SESSION|@@...] var = expr [, var = expr, ...]
        ├── parse_variable_target()   // @user_var, @@global.sys_var, plain name
        └── parse_expression()

PostgreSQL variants:
  ├── SET name TO value
  ├── SET name = value
  ├── SET LOCAL name = value
  └── RESET name / RESET ALL
```

The SET AST preserves full detail for query reconstruction — ProxySQL actively rewrites SET statements.

---

## Query Reconstruction (Emitter)

Each `NodeType` has a corresponding emit function. The emitter is dialect-templated, walking the AST and writing into an arena-backed output buffer.

- `StringRef` values are emitted directly from the original input (no copy unless the node was modified).
- Modified nodes emit their new values.

Cross-dialect emission (parse MySQL → emit PostgreSQL) is **out of scope** for the initial design. Many constructs have no direct equivalent across dialects (`SQL_CALC_FOUND_ROWS`, backtick quoting, `LIMIT` syntax differences). The emitter always emits in the same dialect it parsed.

---

## Binary Protocol & Prepared Statements

### Lifecycle

```
COM_STMT_PREPARE          COM_STMT_EXECUTE (repeated)       COM_STMT_CLOSE
      │                          │                                │
      ▼                          ▼                                ▼
  Parse SQL template      Bind params to cached AST         Evict from cache
  Cache AST + metadata    Return enriched ParseResult
```

### Prepare Phase

SQL template parsed normally. Placeholder tokens (`?` in MySQL, `$1`/`$2` in PostgreSQL) become `NODE_PLACEHOLDER` AST nodes with a parameter index in `flags`.

The AST is copied from the arena to a longer-lived **statement cache** (per-parser-instance, keyed by statement ID) via `parse_and_cache()`, which atomically parses and stores the result before the arena can be reset. This is the one place where memory leaves the arena.

**Threading note:** The statement cache is per-parser-instance (i.e., per-thread). In ProxySQL, prepared statement state is per-session. If sessions can migrate between threads, the session must carry its own prepared statement metadata (statement IDs, SQL templates). The parser on the destination thread can re-parse and cache the template on first execute if the cached AST is not found. This avoids any cross-thread sharing of parser state.

### Execute Phase

```cpp
struct BoundValue {
    enum Type { INT, FLOAT, DOUBLE, STRING, BLOB, NULL_VAL, DATETIME, DECIMAL };
    Type type;
    union {
        int64_t int_val;
        float float32_val;    // MySQL FLOAT (4 bytes) — distinct from DOUBLE
        double float64_val;   // MySQL DOUBLE (8 bytes)
        StringRef str_val;    // points into COM_STMT_EXECUTE packet buffer
                              // also used for DATETIME/DECIMAL (wire-format string)
    };
};
static_assert(std::is_trivially_copyable_v<BoundValue>);

struct ParamBindings {
    BoundValue* values;
    uint16_t count;
};
```

Parser looks up cached AST by statement ID, returns `ParseResult` carrying both the AST pointer and `ParamBindings`. Consumers walk the AST and resolve placeholders through the bindings.

### Reconstruction with Parameters

The emitter checks placeholder nodes against `ParamBindings` and writes materialized values, producing valid text-protocol SQL from a binary-protocol execution.

### Statement Cache

Per-thread, fixed-capacity LRU. Size maps to ProxySQL's `max_prepared_statements` config. Eviction on COM_STMT_CLOSE or LRU overflow.

---

## Error Handling

The parser never throws exceptions. Errors are reported through `ParseResult::status` and `ParseResult::error`.

- `OK` — parse succeeded fully.
- `PARTIAL` — classifier succeeded (statement type known), deep parse failed. ProxySQL can route on type; backend reports the syntax error.
- `ERROR` — could not classify at all.

**Lenient by design:** ProxySQL doesn't need to reject queries — the backend does. The parser extracts as much useful information as possible and degrades gracefully.

### Multi-Statement Queries

ProxySQL regularly receives semicolon-separated multi-statement queries (e.g., `SET autocommit=0; BEGIN`). The parser handles this by parsing the **first statement** and returning its `ParseResult` along with a `remaining` field (`StringRef` pointing to the unparsed tail after the semicolon). The caller is responsible for calling `parse()` again on the remainder if needed. This avoids allocating a list of results and lets the caller decide whether to parse subsequent statements.

### Maximum Query Length

The parser respects the caller's buffer size (the `len` parameter). It does not impose its own maximum query length — that is ProxySQL's responsibility (via `mysql-max_allowed_packet` or equivalent). The arena's maximum size (default 1MB) provides an implicit bound on the complexity of parseable queries; exceeding it returns `ERROR`.

---

## Public API

```cpp
template <Dialect D>
class Parser {
public:
    Parser(const ParserConfig& config = {});  // arena size, cache capacity

    ParseResult parse(const char* sql, size_t len);
    ParseResult parse_and_cache(const char* sql, size_t len, uint32_t stmt_id);
    ParseResult execute(uint32_t stmt_id, const ParamBindings& params);

    void prepare_cache_evict(uint32_t stmt_id);

    void reset();  // resets arena; call after each query is fully processed
};
```

One `Parser<Dialect::MySQL>` or `Parser<Dialect::PostgreSQL>` per thread, created at thread startup.

---

## Testing Strategy

1. **Unit tests per module** — each statement parser and extractor gets its own test file. Feed SQL strings, assert on AST structure or extracted metadata.
2. **Round-trip tests** — parse → reconstruct → parse → compare ASTs. Run across a corpus of real-world queries.
3. **Performance benchmarks** (Google Benchmark or similar):
   - Tier 2 classification latency (target: <100ns)
   - Tier 1 full parse latency (target: <1us for typical SELECT/SET)
   - Arena memory high-water mark per query

---

## File Organization

```
include/sql_parser/
    common.h              // StringRef, Arena, Token, enums
    tokenizer.h           // Tokenizer<D> template
    ast.h                 // AstNode, NodeType
    parser.h              // Parser<D> public API
    parse_result.h        // ParseResult, ErrorInfo, BoundValue
    keywords_mysql.h      // MySQL keyword table
    keywords_pgsql.h      // PostgreSQL keyword table

src/sql_parser/
    tokenizer.cpp         // explicit template instantiations (or header-only with LTO for max inlining)
    classifier.cpp        // switch dispatch
    select_parser.cpp     // Tier 1: SELECT
    set_parser.cpp        // Tier 1: SET
    expression_parser.cpp // shared Pratt expression parser
    extractors.cpp        // Tier 2: all lightweight extractors
    emitter.cpp           // query reconstruction
    stmt_cache.cpp        // prepared statement cache
    arena.cpp             // arena allocator

tests/
    test_tokenizer.cpp
    test_select.cpp
    test_set.cpp
    test_extractors.cpp
    test_roundtrip.cpp

bench/
    bench_classify.cpp
    bench_select.cpp
    bench_set.cpp
```

---

## Performance Targets

| Operation | Target Latency | Notes |
|---|---|---|
| Tier 2 classification | <100ns | 1-3 token read + switch |
| Tier 1 SELECT parse (simple) | <500ns | `SELECT col FROM t WHERE id = 1` |
| Tier 1 SELECT parse (complex) | <2us | Multi-join, subqueries, GROUP BY, ORDER BY |
| Tier 1 SET parse | <300ns | `SET @@session.var = value` |
| Query reconstruction | <500ns | Simple SELECT round-trip |
| Arena reset | <10ns | Pointer rewind (single-block case; overflow blocks add O(n) free calls) |

---

## Statement Tier Classification

### Tier 1 — Deep Parse (full AST, reconstruction)

- SELECT
- SET

### Tier 2 — Classify + Extract Key Metadata

- INSERT, UPDATE, DELETE, REPLACE
- USE, SHOW
- BEGIN, START TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT
- PREPARE, EXECUTE, DEALLOCATE
- CREATE, ALTER, DROP, TRUNCATE
- GRANT, REVOKE
- LOCK, UNLOCK
- LOAD DATA
- All other statements (classified as UNKNOWN with raw text)
