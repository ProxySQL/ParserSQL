# PostgreSQL expression coverage implementation plan

The user selected expansion of ParserSQL's own grammar and authorized proceeding
on PR #67. Execute inline using the executing-plans and test-driven-development
skills. Keep benchmarks uncommitted and use detailed commit descriptions.

## Design and scope

Extend the existing expression parser in two independently testable steps:
validated PostgreSQL type names/casts, then PostgreSQL symbolic operators. Keep
MySQL parsing unchanged, preserve the 48-byte AST layout, append enum entries,
and add no production dependencies. This is the first phase of grammar parity;
DDL, MERGE and complete PostgreSQL syntax remain future work.

Type names are validated syntax leaves (including modifiers and array bounds),
not executable expressions. Cast nodes have expression and type children, emit
canonical CAST syntax, and parameterize only their expression. Support qualified
and quoted type names, numeric modifiers, multiword builtin names, array types,
CAST(expr AS type), expr::type and ordinary type-prefixed string constants.
Malformed casts must not report complete input. Interval literal syntax remains
outside this phase; interval types in casts are supported.

Tokenize PostgreSQL symbolic operator names according to upstream scan.l,
including comment boundaries and trailing +/- rules. Keep built-in arithmetic
and comparison tokens; append a generic operator token. Parse generic prefix
and binary operators with PostgreSQL precedence, including JSON, regex, array,
range and extension operators. Preserve operator spelling in ASTs/emission and
reject unsupported engine evaluation. Qualified OPERATOR(schema.op) is deferred.

## Tasks and checks

- [x] Add focused tests to tests/test_pg_expressions.cpp and the Makefile. Check
  full-input parsing, exact emitted SQL, chained-cast ASTs, quoted/custom types,
  precision/array bounds, malformed operands, and parameter mappings. Compile
  directly with src/sql_parser/{arena,parser}.cpp and gtest; observe failures.
- [x] Add pg_type_parser.h, appended cast/type node tags and expression parser
  hooks. Add emitter, parameterizer and planner handling. Re-run focused tests.
  Example: SELECT $3::numeric(12,2), CAST(7 AS integer) must parameterize only 7.
- [x] Add tokenizer/precedence tests before changing operator handling. Cover
  JSON ->/->>, @>, ?, regex ~*, custom <->, prefix @/|/, adjacent signs, comment
  boundaries, exponent precedence, incomplete RHS and unchanged MySQL binds.
- [x] Implement PostgreSQL operator scanning, Pratt precedence, AST flags and
  planner rejection. Verify original precedence using AST-child assertions and
  PostgreSQL oracle deparse comparisons, not only acceptance counts.
- [x] Force the complete C++ rebuild and corpus harness build. Collect and
  review corpus transitions against the 19,139-case complete-input baseline.
  Refresh correctness snapshots only after examining losses. Replay all cases.
- [x] Request one fresh review of the expression changes; fix confirmed issues.
  Document supported grammar and limits, commit detailed changes, push to PR #67
  and update its description with final coverage and test evidence.

## Review focus

1. Cast postfix precedence versus unary signs, parentheses and array indexes.
2. Syntax constants must not become bind parameters; quoted type names survive.
3. Operator lexing must not consume comment starts or absorb unary signs.
4. PostgreSQL '?' is an operator; MySQL '?' remains a bind marker.
5. Complete-input counts must not conceal malformed ASTs or silent execution.

## Progress

- Baseline: f414da5, PR #67 open, 19,139 DEEP_SUPPORTED / 51,415 cases.
- Existing benchmark-only worktree files are preserved and excluded from staging.

- Ruling: add named argument ASTs in this expression phase. The correct `=>`
  token exposed 114 falsely-complete comparisons in the corpus; accepting
  function argument names as operators would preserve the wrong semantics.
- Ruling: add COLLATE parsing and postfix casts on completed predicates because
  they compose directly with the new cast precedence.
- Review fixes: token-based CHAR/BIT default-length normalization, builtin
  modifier restrictions, casts after COLLATE/IS, NCHAR VARYING, qualified
  keyword labels, malformed named argument rejection and PostgreSQL keyword
  categories for unquoted argument names. All were verified against failing
  focused tests and the pinned PostgreSQL oracle.
- Independent reviewer verified all 494 keyword classifications and 10 further
  argument checks, with no remaining findings in the final fixes.

## Final verification

- Commit b06aa50 implements the expression phase; 1,370 active C++ tests pass,
  37 backend-dependent tests skip, and the corpus harness builds after a forced
  rebuild. All 42 focused AST/expression tests pass under ASan/UBSan.
- All 46 PostgreSQL AST round-trip comparisons pass after source positions are
  removed. Independent review verifies 504 additional targeted checks.
- 26,243 / 51,415 corpus statements report DEEP_SUPPORTED, up 7,104 from the
  phase baseline. Transitions: 7,074 TRAILING_INPUT and 30 PARTIAL become deep;
  no previously deep case is lost. 6,454 newly deep cases are SELECT statements.
- All 87 compatibility harness tests and 26,835 refreshed CI fixtures pass.
- Full grammar parity remains future work: prioritize query composition and
  expressions, then DML/MERGE and structured DDL/utilities, with AST differential
  validation and negative cases throughout. Counts alone do not prove semantics.
