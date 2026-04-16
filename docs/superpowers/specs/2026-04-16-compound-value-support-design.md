# Compound Value Support Design

**Goal:** Add first-class runtime `Value` support for arrays and tuples/composites so expression evaluation stops collapsing those AST nodes to `NULL`.

**Scope decision:** This design covers local runtime support only: `Value` representation, expression evaluation, and tests. It does not include remote protocol serialization, planner type inference, or a full SQL row/composite type system.

## Problem

The evaluator currently treats `NODE_TUPLE`, bare `NODE_ARRAY_CONSTRUCTOR`, and `NODE_FIELD_ACCESS` as unsupported, and `NODE_ARRAY_SUBSCRIPT` only works against literal array AST nodes. That means compound expressions do not have a real runtime representation and cannot flow through the engine as values.

## Chosen Approach

Use first-class `Value` tags for compound values instead of more AST special cases.

- Add `Value::TAG_ARRAY`
- Add `Value::TAG_TUPLE`
- Store both through a shared arena-owned descriptor rather than inventing a heap-managed object system

This keeps the runtime model aligned with the rest of the engine: `Value` stays copyable, payload lifetime is arena-bound, and compound values can be nested without a separate ownership framework.

## Runtime Model

Introduce a lightweight sequence descriptor in the value layer:

- `count`: number of elements
- `elements`: arena-allocated `Value[]`
- `field_names`: optional parallel `StringRef[]`, present only for named tuple/composite values

`TAG_ARRAY` and `TAG_TUPLE` both point at this descriptor. The tag defines semantics:

- `TAG_ARRAY`: ordered collection addressed by subscript
- `TAG_TUPLE`: ordered collection that may optionally expose named fields

Compound values remain opaque to arithmetic, comparison, and coercion unless a dedicated operator explicitly handles them.

## Evaluation Semantics

- `NODE_ARRAY_CONSTRUCTOR` evaluates each child and returns `TAG_ARRAY`
- `NODE_TUPLE` evaluates each child and returns `TAG_TUPLE`
- `NODE_ARRAY_SUBSCRIPT` evaluates the left-hand side first; if it yields `TAG_ARRAY`, apply existing dialect indexing rules (`0`-based for MySQL, `1`-based for PostgreSQL)
- `NODE_FIELD_ACCESS` evaluates the left-hand side first; if it yields `TAG_TUPLE` with `field_names`, resolve by case-insensitive field name match

Error semantics stay conservative:

- out-of-bounds subscript -> `NULL`
- `NULL` index -> `NULL`
- field name not found -> `NULL`
- field access on unnamed tuples -> `NULL`
- field/subscript access on non-compound values -> `NULL`

## Scope Boundaries

Included in this design:

- standalone array and tuple values
- nested compound values
- array subscript against evaluated runtime values, not just literal AST arrays
- named tuple field access once a tuple carries field metadata

Explicitly deferred:

- remote executor transport for arrays/tuples
- planner or catalog type inference for compound result columns
- decimal representation redesign
- full SQL composite producers beyond tuple helpers and evaluator paths

## Implementation Order

1. Extend `Value` with compound tags and descriptor helpers
2. Add evaluator support for array and tuple construction
3. Generalize array subscript to work against `TAG_ARRAY`
4. Add named tuple helpers and `NODE_FIELD_ACCESS` support
5. Expand tests for standalone values, nested compounds, and access semantics

## Testing

Add targeted unit coverage in:

- `tests/test_expression_eval.cpp` for tuple/array construction, nested arrays, runtime subscript, and field access
- `tests/test_value.cpp` if helper constructors or invariants need direct verification

Regression expectations:

- existing array subscript tests continue to pass
- `TupleReturnsNull` and `ArrayConstructorReturnsNull` become positive-value tests
- new field access tests use named tuple helpers or resolver-returned tuple values rather than requiring a new SQL producer first

## Non-Goals

- No attempt to make arrays or tuples comparable in this phase
- No attempt to print or serialize compound values for wire protocols yet
- No attempt to introduce schema-aware composite typing beyond optional field-name metadata
