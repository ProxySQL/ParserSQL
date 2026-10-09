# Transaction statement nodes

## Purpose

Transaction-start characteristics must be available in the AST so consumers can
inspect access modes without reparsing an unconsumed SQL tail. This change adds
MySQL deep parsing for `BEGIN [WORK]` and `START TRANSACTION`, and retains the
PostgreSQL transaction grammar already present on `main`.

## Dialect grammar

```sql
-- MySQL
BEGIN [ WORK ]
START TRANSACTION [ transaction_characteristic [, ...] ]
-- transaction_characteristic:
--   WITH CONSISTENT SNAPSHOT | READ WRITE | READ ONLY

-- PostgreSQL
BEGIN [ WORK | TRANSACTION ] [ transaction_mode [, ...] ]
START TRANSACTION [ transaction_mode [, ...] ]
-- transaction_mode:
--   ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
--   READ WRITE | READ ONLY | [ NOT ] DEFERRABLE
```

PostgreSQL permits omitted commas between modes. MySQL requires commas and permits
characteristics only on `START TRANSACTION`, not `BEGIN`. MySQL isolation levels
are set with `SET TRANSACTION`. `WORK`, `CONSISTENT`, and `SNAPSHOT` must be unquoted
words; quoted identifiers with those spellings are not transaction keywords.

The PostgreSQL `PgUtilityParser` continues to handle commit, rollback, savepoints,
release, prepared transactions, and the `END` / `ABORT` synonyms. These existing
structured ASTs and their error handling are preserved. MySQL `COMMIT`, `ROLLBACK`,
and `SAVEPOINT` retain their existing classification/extraction behavior.

## AST contract

Both dialects use `NODE_TRANSACTION_STMT` with `NODE_TRANSACTION_OPTION` children.
The root value is the canonical introducer, `BEGIN` or `START TRANSACTION`.
`WORK` and PostgreSQL's optional `TRANSACTION` after `BEGIN` are noise words and
normalize to `BEGIN`. A plain transaction start has a root with no children.

Option values contain the entire canonical phrase, including `ISOLATION LEVEL`
for PostgreSQL isolation options. No isolation flag or new token types are needed.
Values use uppercase spelling with a single space between words, independent of
input case, whitespace, or comments. Options remain in input order, including
repeated characteristics; consumers must consider the complete list.

```text
BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY              (PostgreSQL)
└── NODE_TRANSACTION_STMT "BEGIN"
    ├── NODE_TRANSACTION_OPTION "ISOLATION LEVEL READ COMMITTED"
    └── NODE_TRANSACTION_OPTION "READ ONLY"

START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY        (MySQL)
└── NODE_TRANSACTION_STMT "START TRANSACTION"
    ├── NODE_TRANSACTION_OPTION "WITH CONSISTENT SNAPSHOT"
    └── NODE_TRANSACTION_OPTION "READ ONLY"

BEGIN WORK                                                 (both)
└── NODE_TRANSACTION_STMT "BEGIN"
```

## Emission and digests

The emitter writes the canonical introducer and each complete option value.
MySQL options are separated by commas; PostgreSQL uses spaces, which its grammar
accepts. The existing PostgreSQL emitter also preserves savepoint identifiers and
prepared-transaction values. Transaction-start digest casing follows the canonical
AST values.

| Input | Emitted | Dialect |
|---|---|---|
| `begin work` | `BEGIN` | both |
| `BEGIN TRANSACTION READ ONLY` | `BEGIN READ ONLY` | PostgreSQL |
| `BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY` | `BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY` | PostgreSQL |
| `start transaction read write` | `START TRANSACTION READ WRITE` | both |
| `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` | same | MySQL |

## Invalid input and completeness

Recognized but incomplete or malformed modes, a missing `TRANSACTION` after
`START`, trailing commas, and arena allocation failures return `ERROR`. Every
option allocation is checked, so a truncated option list cannot report success.
Malformed PostgreSQL isolation pairs are rejected by the existing strict grammar.

Unknown or dialect-inapplicable tails stay unconsumed and appear in `remaining`;
these may retain `OK`, but `full_input` is false. Consumers requiring a complete
statement must check both status and completeness. `full_input` alone describes
input consumption, not grammar validity.

Mode parsing never consumes a semicolon as part of a required phrase. For example,
`START TRANSACTION READ; SELECT 1` returns `ERROR` and leaves `SELECT 1` as the
remaining statement. A complete `START TRANSACTION READ ONLY; SELECT 1` returns
`OK` with the same remaining statement. The existing batch parser remains usable.

## Engine execution

The session executor rejects transaction-start options in both dialects because
its transaction manager cannot apply isolation, access, or snapshot options.
Parsing these statements into complete ASTs must not turn them into plain
`begin()` calls. Plain transaction starts continue to execute normally.

## Implementation and verification

- `Parser::parse_transaction()` handles MySQL transaction starts and delegates
  PostgreSQL to the established `PgUtilityParser` path.
- `Emitter::emit_transaction_stmt()` emits comma separators for MySQL options.
- `tests/test_misc_stmts.cpp` covers canonical AST values, dialect boundaries,
  round trips, malformed modes, quoted words, statement boundaries, and allocation
  exhaustion. PostgreSQL assertions use the existing option-node convention.
- `tests/test_review_transactions.cpp` verifies that unsupported MySQL options
  fail without calling the transaction manager, while plain starts still work.
- `tests/test_digest.cpp` covers canonical PostgreSQL transaction-start digests;
  the existing PostgreSQL utility tests retain coverage of the broader grammar.
