# ParserSQL PostgreSQL analysis features

The user authorized the recommended implementation sequence: common PostgreSQL
query grammar, AST traversal/transformation, multi-statement and utility parsing,
and context-aware parameterization. Existing uncommitted coverage and IN-list
performance improvements are the starting point and must be preserved.

Use the existing C++17 parser and arenas. Keep AstNode at 48 bytes and append
node tags without renumbering old ones. Existing parse(), digest and cache APIs
keep their behavior. No dependency on PostgreSQL in the production library.

Grammar additions: DISTINCT ON, aggregate FILTER, LATERAL references, named
windows and ROWS/RANGE/GROUPS frames with boundaries/exclusions. Each must have
structured AST representation, emission, malformed-input checks and tests. The
execution planner rejects newly parsed constructs it cannot execute.

AST utilities: preorder traversal with stop/skip, explicit arena-backed cloning
that copies text, and controlled subtree replacement. Parameterization returns
a copied AST plus an ordered parameter mapping; preserve original bind indices
and SQL syntax constants/ordinals. Fail explicitly for unsupported roots or
contexts instead of returning misleading executable SQL. Existing digest is
unchanged.

Multi-statement API: return ordered per-statement parse results and source
boundaries, retain all ASTs until parser reset, respect quoted strings/comments,
report errors without accepting an unparsed tail. Utility parsing: structured
PostgreSQL transaction options/savepoints and COPY relation/query, direction,
endpoint and common options. COPY data payloads and execution are outside scope.

Validation: meaningful failing tests before implementation; focused tests per
task; full forced C++ rebuild; corpus harness; PostgreSQL differential checks;
direct pg_raw_parse benchmark for existing and new supported AST cases.
Retain unsupported syntax explicitly. No claim of full PostgreSQL grammar or
semantic validation. Commit relevant implementation, tests and documentation
on a dedicated branch with detailed messages; keep benchmark changes uncommitted
as requested by the user.
