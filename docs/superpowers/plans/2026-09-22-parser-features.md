# ParserSQL analysis features implementation plan

> Execute the authorized plan using test-driven development and independent
> parallel tasks; integrate and review before reporting completion.

**Goal:** Improve PostgreSQL coverage and expose useful query-analysis APIs.
**Architecture:** Extend the existing AST without changing its layout. Add
header-only utilities and preserve the reusable parser's fast single-query path.
**Tech stack:** C++17, arenas, GoogleTest, pinned PostgreSQL compatibility harness.
**Spec:** ../specs/2026-09-22-parser-features-design.md

## Constraints and review focus

- Preserve existing uncommitted changes; append enum values; no new dependencies.
- Commit implementation, tests and documentation on a dedicated branch with
  detailed messages; leave benchmark changes uncommitted. Raw build and timing
  artifacts stay in /tmp.
- Check malformed new syntax, quoted identifiers, nested queries, parser reuse,
  allocation failure, and unsupported-engine behavior.
- Parameter indices must remain correct when existing binds and literals mix.
- Multi-statement lexical boundaries must ignore quoted/comment semicolons.

## Tasks

- [x] Query grammar: expression/select/table-reference parsers, common tags,
  emitter, planner guards, and focused grammar tests. Establish failing tests
  for DISTINCT ON, FILTER, LATERAL, named windows and frame boundaries; implement
  structured nodes; verify emission and invalid-input rejection.
- [x] AST utilities: new ast_walk.h / ast_transform.h / parameterize.h and
  focused tests. Test traversal order, skip/stop, copied text lifetime, subtree
  replacement, ordinals, and bind mapping before implementation.
- [x] Batch and utility parsing: parser.h / parser.cpp, utility parser header,
  tests. Test quoted semicolons, multiple preserved ASTs, malformed middle
  statements, transaction options and COPY direction/options; implement
  parse_all and utility dispatch, coordinate tags/emitter with grammar task.
- [x] Integrate: register tests in Makefile, run forced full tests and corpus
  build, differential oracle validation and review; correct identified issues.
- [x] Document public APIs, scope and limitations; rerun direct benchmark
  against the same pinned pg_raw_parse API and report actual coverage/timings.

## Completion evidence

1,354 active C++ tests passed (37 backend-dependent skips); corpus harness built;
87 compatibility-harness tests and 33,534 regenerated CI cases passed; 26 AST
utility tests passed ASan/UBSan. Independent review findings were fixed and
reviewer signed off. Public APIs and limitations are documented in
[the feature guide](../../postgresql-analysis-features.md) and
[the AST utility guide](../../ast-utilities.md). Benchmark reports and harness
changes are retained locally and excluded from the implementation commits.
