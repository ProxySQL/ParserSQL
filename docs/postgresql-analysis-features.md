# PostgreSQL query analysis additions

These additions extend parsing and SQL reconstruction. They do not add local
execution support for the new PostgreSQL query constructs. The plan builder
rejects unsupported constructs instead of silently executing a simpler query.

## Query syntax

- `SELECT DISTINCT ON (expressions)`.
- Aggregate `FILTER (WHERE expression)`, also followed by `OVER`.
- `LATERAL` subqueries and table functions, including qualified names and alias
  column lists.
- Named window declarations (`WINDOW w AS (...)`), references (`OVER w`), and
  inherited specifications (`OVER (w ...)`).
- `ROWS`, `RANGE`, and `GROUPS` frames; single bounds or `BETWEEN ... AND ...`;
  `UNBOUNDED`, offset `PRECEDING`/`FOLLOWING`, `CURRENT ROW`; and `EXCLUDE CURRENT
  ROW`, `GROUP`, `TIES`, or `NO OTHERS`.

These forms retain expressions and options as AST children for traversal and
transformation. They support normal SQL emission and digest generation.
PostgreSQL still performs name resolution, type checking, window inheritance
validation and other semantic checks.

The new clauses remain bounded by the existing expression/subquery grammar.
In particular, LATERAL currently uses the simple SELECT subquery callback;
WITH/VALUES/compound derived-query operands and interval-valued frame offsets
are not covered by this change.

Window expressions previously disappeared from emitted SQL and digests because
their AST nodes had no emitter handlers. They now appear correctly. Digest
strings/hashes for affected queries change as a consequence. Quoted identifiers
and PostgreSQL string-source boundaries are also preserved more accurately.

## Multiple statements

```cpp
sql_parser::Parser<sql_parser::Dialect::PostgreSQL> parser;
std::string sql = "BEGIN READ ONLY; SELECT ';'; COMMIT";
auto batch = parser.parse_all(sql.data(), sql.size());
for (const auto& statement : batch.statements) {
    // statement.offset: byte offset in the original input
    // statement.source: SQL span, including its terminating semicolon if any
    // statement.result: classification, AST, status and full_input
}
```

All returned ASTs remain alive until the next `parse`, `parse_all`, or `reset`
on that parser. Input SQL must remain alive too. Use `clone_ast` to retain a tree
independently. The result vector owns records, not the ASTs or source bytes.

The tokenizer identifies boundaries, so semicolons in strings, quoted
identifiers and comments do not split statements. Empty statements/comments are
skipped. Malformed statements remain in the result; scanning continues at the
next lexical statement boundary when available. Error offsets are relative to
the original batch input.

`batch.ok()` requires every nonempty statement to have `OK`, complete input and
a known statement type. It does not require every statement to have a deep AST
and is not a substitute for PostgreSQL syntax/semantic validation. Empty batches
succeed. The API handles SQL text, not inline COPY data, psql commands, or
unquoted procedural bodies with internal statement terminators.

## Transaction and COPY ASTs

PostgreSQL transaction commands now produce `NODE_TRANSACTION_STMT`, whose value
is the canonical command and whose children contain options, savepoint names or
prepared-transaction identifiers:

- `BEGIN` / `START TRANSACTION`, isolation levels, read-only/read-write mode,
  and `[NOT] DEFERRABLE`.
- `COMMIT` / `ROLLBACK`, `[AND [NO] CHAIN]`, and `END` / `ABORT` aliases.
- `SAVEPOINT`, `ROLLBACK TO [SAVEPOINT]`, `RELEASE [SAVEPOINT]`.
- `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`.

`COPY` produces `NODE_COPY_STMT` with direction in its value (`FROM` or `TO`).
Children are the table/query, optional column list, `NODE_COPY_ENDPOINT`, zero
or more `NODE_COPY_OPTION` nodes, and an optional WHERE clause. Endpoints
distinguish `STDIN`, `STDOUT`, `FILE`, and `PROGRAM`; filenames/commands are
string children. Parsing does not execute any command or open any file.

Supported COPY forms include table/column lists, query `TO`, `FROM ... WHERE`,
and modern parenthesized options: FORMAT, FREEZE, DELIMITER, NULL, DEFAULT,
HEADER, QUOTE, ESCAPE, FORCE_QUOTE, FORCE_NOT_NULL, FORCE_NULL, ENCODING,
ON_ERROR, REJECT_LIMIT and LOG_VERBOSITY. Query operands use the supported
SELECT/TABLE/VALUES grammar. Legacy COPY option syntax and DML query operands
are not implemented. Unsupported tails must not be treated as complete input.
Option compatibility, privileges and data conversion remain server concerns.

The new `StmtType::COPY` and `StmtType::RELEASE_SAVEPOINT` values are appended;
existing enum values and the 48-byte AST layout are unchanged. Consumers should
handle the new statement types explicitly.

See [PostgreSQL COPY syntax](https://www.postgresql.org/docs/18/sql-copy.html)
and [BEGIN syntax](https://www.postgresql.org/docs/18/sql-begin.html) for the server
grammar and semantics.

## AST APIs and parameterization

See [AST utility APIs](ast-utilities.md) for traversal with skip/stop controls,
owned cloning, validated subtree replacement, and parameterization with literal
and existing-bind mappings. Parameterization operates on a copied AST, preserves
query ordinals, and rejects unsupported contexts explicitly. It does not decode
literal values or infer PostgreSQL types.
