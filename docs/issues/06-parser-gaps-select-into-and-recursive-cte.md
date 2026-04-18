# Issue 06: Close Parser Gaps Around `SELECT ... INTO` And Recursive CTE Handling

## Priority

`P2`

## Problem

A few parser paths are still marked as skipped or future work. Notable examples are MySQL `SELECT ... INTO` placement variants and recursive CTE handling that is currently only marked in flags without complete downstream semantics.

## Evidence

- [include/sql_parser/select_parser.h](/data/rene/ParserSQL/include/sql_parser/select_parser.h:43)
- [src/sql_parser/parser.cpp](/data/rene/ParserSQL/src/sql_parser/parser.cpp:1223)
- [tests/test_set.cpp](/data/rene/ParserSQL/tests/test_set.cpp:237)

## Risk

- Partial dialect coverage in edge syntax
- Features appear parsed but are not fully executed or planned

## Desired Outcome

Document and close parser-specific surface gaps independently of executor-wide CTE integration work.

## Scope

- Add parser coverage for skipped `SELECT ... INTO` forms
- Decide whether recursive CTEs should be parsed-only, rejected, or supported end-to-end
- Remove stale comments once actual behavior is settled

## Acceptance Criteria

- Parser behavior matches the documented syntax contract
- Tests exist for each newly supported or explicitly rejected form

