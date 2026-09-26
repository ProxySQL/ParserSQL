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

The new clauses remain bounded by the existing expression grammar. PostgreSQL
subqueries now share compound-query parsing, including SELECT, TABLE, VALUES and
WITH operands in derived tables and LATERAL. Interval-valued frame offsets are supported through structured interval literals.

Window expressions previously disappeared from emitted SQL and digests because
their AST nodes had no emitter handlers. They now appear correctly. Digest
strings/hashes for affected queries change as a consequence. Quoted identifiers
and PostgreSQL string-source boundaries are also preserved more accurately.

## PostgreSQL expression grammar

The expression parser now supports `expr::type`, `CAST(expr AS type)`, chained
casts and type-prefixed string literals such as `DATE '2026-09-22'`. Types can
have quoted/schema-qualified names, numeric modifiers, array dimensions and
multiword builtin syntax (`double precision`, `character varying`, timestamp
with/without time zone, and interval field ranges in cast types).

`NODE_TYPE_CAST` has an expression child followed by a `NODE_TYPE_NAME` leaf.
The leaf retains validated type spelling, including modifiers and bounds; those
numbers are syntax constants rather than values to parameterize. SQL emission
uses canonical `CAST` syntax. For unqualified CHAR/BIT prefix constants without
a length, emission uses `pg_catalog.bpchar`/`pg_catalog.bit` to preserve their
unconstrained length rather than introducing CAST's implicit length one.
Type leaves are not decomposed into independently editable modifiers.

PostgreSQL symbolic operators are lexed as complete names. This includes JSON
`->`, `->>`, `@>`, `?`, `@?`, regex `~`/`~*`/`!~`, array/range operators and
extension operators such as `<->`. Prefix operators, exponentiation and generic
binary operators follow PostgreSQL precedence, including its differences from
MySQL. Operator comments and adjacent unary signs keep their lexical boundaries.
`COLLATE` retains its collation name, including quoted/qualified names.

Function, table-function and CALL arguments support `name => expression` and
`name := expression`. `NODE_NAMED_ARGUMENT` retains the argument name and an
expression child; emission uses `=>`. PostgreSQL's keyword restrictions apply to
unquoted argument names. Quoted names remain valid. Parameterization binds the
argument values and preserves names.

The local planner rejects casts, named arguments, collation and operators whose
PostgreSQL behavior it cannot execute. Parser acceptance does not resolve type
names, operators or function overloads, and does not validate catalog semantics.
Consumers must also handle the appended node/token kinds. `?` is a PostgreSQL
operator; MySQL continues to tokenize it as an anonymous bind marker.

Qualified prefix and binary `OPERATOR(schema.op)` syntax, expression-valued
type modifiers and interval prefix-literal qualifiers are also supported.
These additions improve the native ParserSQL implementation; there is no
PostgreSQL runtime fallback or production library dependency.

## Qualified calls, aggregates and CTEs

Expression calls accept schema-qualified names, including quoted components.
Aggregate calls preserve `DISTINCT`/`ALL`, in-call `ORDER BY` (including direction
and NULLS placement), and `WITHIN GROUP (ORDER BY ...)`. FILTER and OVER can
follow the supported aggregate forms. This is syntactic parsing; PostgreSQL
still checks aggregate signatures and semantic restrictions.

`NODE_FUNCTION_CALL` retains ordinary argument children and node-specific flags
for qualification, DISTINCT, ALL and WITHIN GROUP. An appended
`NODE_AGGREGATE_ORDER_BY` child contains `NODE_ORDER_BY_ITEM` children. Its sort
expressions are values, not SELECT output ordinals: parameterization binds a
literal `1` inside `array_agg(x ORDER BY 1)` but preserves the query-level
`ORDER BY 1`. Malformed calls and missing closing delimiters report errors.

PostgreSQL CTE definitions accept output-column lists, `[NOT] MATERIALIZED`,
SELECT/TABLE/VALUES/compound bodies and nested WITH queries. RECURSIVE is retained
in the AST and emitted SQL; it does not enable recursive local execution.
The definition body remains its first child, followed by `NODE_CTE_COLUMNS`
when present. CTE names and column names preserve quoting. The CTE emitter
reconstructs the complete WITH clause and main query.

The local engine rejects qualified calls, aggregate modifiers/ordering,
recursive/nested CTEs, CTE output-column lists, materialization hints and
VALUES/TABLE query bodies before CTE materialization. These additions do not extend CTE parameterization, which
continues to return an unsupported-context error.

See PostgreSQL's
[aggregate syntax](https://www.postgresql.org/docs/18/sql-expressions.html#SYNTAX-AGGREGATES)
and [WITH queries](https://www.postgresql.org/docs/18/queries-with.html).

## Further native grammar coverage

Common PostgreSQL expressions now include `EXTRACT`, SQL `SUBSTRING`, `TRIM`,
`AT TIME ZONE`, `AT LOCAL`, interval literal precision/field qualifiers,
`IS [NOT] DISTINCT FROM`, `ARRAY(query)`, quantified comparison operands
(`ANY`/`ALL`/`SOME`) and Unicode `NORMALIZE`. Operands remain AST children.
Qualified operators and syntax modifiers retain their spelling during emission.

Queries support `GROUPING SETS`, `ROLLUP`, `CUBE`, grouping `ALL`/`DISTINCT`,
`LIMIT ALL`, either LIMIT/OFFSET order, offset-only pagination and
`FETCH FIRST/NEXT ... ROWS ONLY/WITH TIES`. Table functions support
`WITH ORDINALITY` and typed output-column definitions. `ONLY` relation sources
and parenthesized WITH operands are retained. New constructs that the local
engine cannot execute are rejected by its planner.

`MERGE` has structured target/source, join condition, match branches, optional
conditions, UPDATE/DELETE/INSERT/DO NOTHING actions and RETURNING. CTE bodies
and main statements may use supported INSERT/UPDATE/DELETE/MERGE forms.
Recursive `SEARCH` and `CYCLE` clauses, INSERT `OVERRIDING`, compound query
sources, assignment indirection and RETURNING options retain their structure.
The local DML planner rejects these new semantics before execution.

Common DDL now has validated productions for CREATE TABLE (including CTAS and
partition bounds), basic CREATE SCHEMA/DATABASE, ALTER TABLE actions, DROP
families, indexes,
views/materialized views, functions/procedures, triggers, privileges, role
membership, TRUNCATE, VACUUM and ANALYZE. Names, expressions, lists and clauses
are traversable; type names and fixed syntax are validated leaves. Quoted
function bodies remain string literals. This is syntax parsing, not catalog
validation or execution, and unsupported options must leave an error or
incomplete input rather than being accepted as an arbitrary tail.

SQL/JSON and XML productions retain constructors, JSON_ARRAYAGG/JSON_OBJECTAGG,
query/value/exists calls,
RETURNING and behavior options, table columns, serialization and default
expressions. Their new node kinds support traversal and SQL reconstruction;
parameterization remains conservative about unsupported contexts.

Additional expression forms include `VARIADIC` arguments, array slices,
`ILIKE`/`SIMILAR TO` and `ESCAPE`, SQL `POSITION`/`OVERLAY`, `IS JSON` predicates,
and `ORDER BY ... USING` in queries, aggregates and windows. PostgreSQL bit and
hexadecimal literals preserve their prefixes. Nested joins, join USING aliases,
`TABLESAMPLE ... REPEATABLE`, `ROWS FROM`, row-lock strengths/targets/wait policies,
and `SELECT INTO` have explicit AST nodes. EXPLAIN retains grouped options and
propagates its inner statement's completion and error status. Unsupported local
execution is guarded; parameterization conservatively rejects the new contexts.

Object DDL includes CREATE/ALTER DOMAIN, shell/composite/enum/range/base TYPE
forms, CREATE/ALTER SEQUENCE, routine properties and SET/RESET clauses,
object renaming/ownership/schema changes, and additional DROP families.
Defaults, constraints, names and options remain traversable AST components.

Coverage is still a subset of PostgreSQL. Remaining examples include embedded
CREATE SCHEMA elements, CREATE DATABASE options, other object-specific ALTER
commands, aggregate/operator definitions, roles, comments, cursors, unquoted SQL
function bodies, exclusion constraints, less common DDL options, assignment
slices and full ON CONFLICT index-inference options. PostgreSQL remains responsible
for name resolution, types, privileges and semantic checks. Corpus acceptance
alone does not establish grammar parity or round-trip equivalence for every
accepted statement.

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
