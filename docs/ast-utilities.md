# AST traversal, transformation, and parameterization

These header-only C++17 APIs use the existing 48-byte `AstNode` and `Arena`.
Include `sql_parser/ast_walk.h`, `sql_parser/ast_transform.h`, or
`sql_parser/parameterize.h` as needed.

## Traverse one subtree

```cpp
auto result = sql_parser::walk_ast(root,
    [](const sql_parser::AstNode& node,
       const sql_parser::AstVisitContext& context) {
        // context.parent, context.depth, context.child_index
        return sql_parser::AstVisitAction::Continue;
    });
```

Traversal is preorder, children in sibling order. `SkipChildren` skips only the
current node's descendants; `Stop` terminates the whole walk. The root's own
`next_sibling` is outside the traversal. A null root is a completed empty walk.
Do not change links during a walk.

The iterative implementation uses O(depth) auxiliary storage. `AstWalkLimits`
defaults to 1,000,000 visited nodes and a maximum depth of 4,096 (root depth zero).
`AstWalkResult` contains the visit count and `Completed`, `Stopped`, or
`LimitExceeded`. Bounds also prevent unbounded traversal of cyclic input.

## Copy and replace

```cpp
sql_parser::Arena destination;
auto copied = sql_parser::clone_ast(root, destination);
if (!copied.ok()) { /* inspect copied.error */ }
```

`clone_ast` copies one subtree and every value/source string into `destination`.
The input SQL and original arena may then be destroyed. The clone's root has no
sibling; internal child and sibling order and flags are retained. Cycles and
shared nodes are rejected with `InvalidTree`; exhaustion and traversal bounds
return explicit errors. Cloning null succeeds with a null result.

`make_owned_node(arena, type, value, flags, source)` is the owning counterpart of
`make_node`: it copies text and returns null on arena allocation failure.

`replace_ast_subtree(root, target, replacement)` changes the caller's root pointer
when necessary, preserving the target's sibling position. Pass null replacement
to remove the target. On success the old target is detached from its siblings.
The input root must be a standalone tree, with no root sibling. Replacement must
be a detached subtree with no root sibling and share no nodes with the input
tree. Validation happens before mutation; errors leave input links unchanged.
Nodes are never freed individually, and callers retain responsibility for the
lifetimes of every participating arena.

Failed operations can consume arena space; they do not reset or roll back an
arena that might contain other live nodes. Temporary STL allocation failures use
the usual C++ exception behavior.

## Parameterize complete statements

```cpp
sql_parser::Parser<sql_parser::Dialect::PostgreSQL> parser;
auto parsed = parser.parse(sql.data(), sql.size());
sql_parser::Arena output;
auto rewritten = sql_parser::parameterize_ast<sql_parser::Dialect::PostgreSQL>(
    parsed, output);
if (rewritten.ok()) {
    sql_parser::Emitter<sql_parser::Dialect::PostgreSQL> emitter(output);
    emitter.emit(rewritten.ast);
    auto sql_with_binds = emitter.result();
}
```

Prefer the `ParseResult` overload: it rejects non-OK results, unconsumed input,
and results without `full_input`. The raw `AstNode*` overload requires a complete,
valid AST in the specified dialect. Neither overload performs catalog lookup,
type inference, or a second SQL grammar validation.

The original tree is unchanged. The result contains a copied AST and an ordered
`parameters` vector of `ExtractedParameter { index, literal_type, value, source }`.
Indices are one-based. Values retain their lexical spelling, including any
escape sequences; they are not decoded strings or converted numeric values.
Source retains the original literal spelling when recorded by the parser, and
may be empty for manually built nodes or keywords without source metadata.
Mapping text and AST nodes live in the output arena; the result owns its vectors.
Repeated literals produce separate parameters.

PostgreSQL preserves existing `$n` spellings and indices and generates new
indices after the largest existing index. For `SELECT $3, 7, $1, 'a'`, the result
is `SELECT $3, $4, $1, $5`, with new mapping indices 4 and 5 and
`existing_parameters` equal to `[3, 1]`. Invalid indices and signed 32-bit index
overflow fail explicitly.

MySQL emits `?` and counts positions in the resulting SQL. For
`SELECT ?, 7, ?, 'a'`, new mapping indices are 2 and 4 and
`existing_parameters` is `[1, 3]`. The latter identifies the positions where
callers should interleave their original bindings. MySQL comma LIMIT syntax is
normalized from `LIMIT offset, count` to `LIMIT count OFFSET offset`, so extracted
literals follow the normalized emission order. If that comma LIMIT contains an
existing `?`, parameterization returns `UnsupportedContext` rather than silently
reordering the caller's original anonymous bindings. Existing binds elsewhere in
the statement and `LIMIT count OFFSET offset` remain supported.

Supported roots are SELECT, INSERT, UPDATE, DELETE, compound queries, and TABLE
queries. Bindable literals include integers, floats, strings, hex/bit literals,
and standalone TRUE/FALSE. NULL remains literal. Direct integer ordinals in
query GROUP BY, ORDER BY, and DISTINCT ON remain unchanged, including
parenthesized integers. Integers inside larger expressions and window ORDER BY
are values. `IS [NOT] TRUE/FALSE` keywords and PostgreSQL binary `::` type
subtrees remain unchanged. The precision argument of CURRENT_TIMESTAMP,
CURRENT_TIME, LOCALTIME, and LOCALTIMESTAMP remains a syntax constant. MySQL also
preserves precision arguments of NOW, CURTIME, SYSDATE, UTC_TIME, and
UTC_TIMESTAMP; these ordinary function names retain bindable arguments in
PostgreSQL. Quoted function names retain bindable arguments in both dialects.
Aggregate FILTER predicates and window frame offsets are bindable. PostgreSQL
casts and type-prefixed string literals now produce `NODE_TYPE_CAST` with an
expression child and a `NODE_TYPE_NAME` leaf. Cast values are bindable; type
modifiers and array bounds remain intact. Named function argument values are
bindable while their names remain intact. Manually constructed binary `::`
ASTs retain their existing support.

Unsupported roots and contexts return `UnsupportedRoot` or `UnsupportedContext`
with no AST or partial mapping. These include SET/transaction/COPY/DDL/other
utility statements, SELECT INTO, CTEs, generic function-shaped CAST nodes,
opaque subquery/interval fragments, and unknown future node types. PostgreSQL
string-shaped aliases are rejected. Supported type-prefixed literals such as
`SELECT TIMESTAMP '2020-01-01'` now parameterize as casts instead of being
misparsed as aliases. These
restrictions reflect contexts where replacing literals or the current emitter
would not produce reliable executable SQL. Parameterization does not change the
existing digest API.
