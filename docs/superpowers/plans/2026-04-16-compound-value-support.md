# Compound Value Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-class runtime `Value` support for arrays and tuples/composites, including runtime subscripting and named tuple field access in expression evaluation.

**Architecture:** Extend `Value` with arena-owned compound payloads shared by new `TAG_ARRAY` and `TAG_TUPLE` tags. Then update `expression_eval.h` so array and tuple AST nodes evaluate into runtime compound values, array subscripts operate on evaluated arrays, and `FIELD_ACCESS` resolves named tuple fields. Keep scope local to the in-process runtime and tests; do not add remote serialization or planner typing.

**Tech Stack:** C++17, GoogleTest, arena-allocated AST/runtime data, existing `Value` and expression evaluator infrastructure

---

## File Structure

- `include/sql_engine/value.h`
  Purpose: add compound value tags, payload descriptor, constructors, and helpers used by the evaluator and tests.
- `include/sql_engine/expression_eval.h`
  Purpose: evaluate tuple and array AST nodes into compound runtime values and implement runtime array/field access.
- `tests/test_value.cpp`
  Purpose: lock down constructor-level invariants for compound `Value` helpers.
- `tests/test_expression_eval.cpp`
  Purpose: lock down tuple/array construction, runtime subscripting, nested compounds, and named field access.
- `docs/issues/05-expression-and-type-semantics.md`
  Purpose: update Issue 05 progress after compound value support lands.

### Task 1: Add compound runtime tags and constructors

**Files:**
- Modify: `include/sql_engine/value.h`
- Modify: `tests/test_value.cpp`

- [ ] **Step 1: Write the failing constructor tests**

Add these tests to `tests/test_value.cpp` and include `sql_parser/arena.h` at the top of the file:

```cpp
using sql_parser::Arena;

TEST(ValueTest, ArrayValue) {
    Arena arena{4096};
    Value elems[2] = {value_int(1), value_int(2)};
    Value v = value_array(arena, elems, 2);

    EXPECT_EQ(v.tag, Value::TAG_ARRAY);
    ASSERT_NE(v.compound_val, nullptr);
    EXPECT_EQ(v.compound_val->count, 2u);
    EXPECT_EQ(v.compound_val->elements[0].int_val, 1);
    EXPECT_EQ(v.compound_val->elements[1].int_val, 2);
}

TEST(ValueTest, NamedTupleValue) {
    Arena arena{4096};
    Value elems[2] = {value_int(7), value_string(StringRef{"bob", 3})};
    StringRef names[2] = {StringRef{"id", 2}, StringRef{"name", 4}};
    Value v = value_named_tuple(arena, elems, names, 2);

    EXPECT_EQ(v.tag, Value::TAG_TUPLE);
    ASSERT_NE(v.compound_val, nullptr);
    EXPECT_EQ(v.compound_val->count, 2u);
    ASSERT_NE(v.compound_val->field_names, nullptr);
    EXPECT_TRUE(v.compound_val->field_names[1].equals_ci("name", 4));
}
```

- [ ] **Step 2: Run the constructor tests to verify RED**

Run:

```bash
./run_tests --gtest_filter="ValueTest.ArrayValue:ValueTest.NamedTupleValue" --gtest_brief=1
```

Expected: compile or link failure because `TAG_ARRAY`, `TAG_TUPLE`, `compound_val`, `value_array`, and `value_named_tuple` do not exist yet.

- [ ] **Step 3: Add the compound payload and helpers**

Update `include/sql_engine/value.h` with the new payload type, union member, tag helpers, and constructors:

```cpp
#include "sql_parser/arena.h"

struct Value;

struct CompoundValueData {
    uint32_t count;
    Value* elements;
    StringRef* field_names;
};

struct Value {
    enum Tag : uint8_t {
        TAG_NULL = 0,
        TAG_BOOL,
        TAG_INT64,
        TAG_UINT64,
        TAG_DOUBLE,
        TAG_DECIMAL,
        TAG_STRING,
        TAG_BYTES,
        TAG_DATE,
        TAG_TIME,
        TAG_DATETIME,
        TAG_TIMESTAMP,
        TAG_JSON,
        TAG_ARRAY,
        TAG_TUPLE,
    };

    union {
        bool bool_val;
        int64_t int_val;
        uint64_t uint_val;
        double double_val;
        StringRef str_val;
        int32_t date_val;
        int64_t time_val;
        int64_t datetime_val;
        int64_t timestamp_val;
        CompoundValueData* compound_val;
    };

    bool is_compound() const { return tag == TAG_ARRAY || tag == TAG_TUPLE; }
};

inline Value make_compound_value(sql_parser::Arena& arena,
                                 Value::Tag tag,
                                 const Value* elements,
                                 const StringRef* field_names,
                                 uint32_t count) {
    Value* stored = static_cast<Value*>(arena.allocate(sizeof(Value) * count));
    for (uint32_t i = 0; i < count; ++i) stored[i] = elements[i];

    StringRef* stored_names = nullptr;
    if (field_names) {
        stored_names = static_cast<StringRef*>(arena.allocate(sizeof(StringRef) * count));
        for (uint32_t i = 0; i < count; ++i) stored_names[i] = field_names[i];
    }

    auto* payload = static_cast<CompoundValueData*>(arena.allocate(sizeof(CompoundValueData)));
    payload->count = count;
    payload->elements = stored;
    payload->field_names = stored_names;

    Value out{};
    out.tag = tag;
    out.compound_val = payload;
    return out;
}

inline Value value_array(sql_parser::Arena& arena, const Value* elements, uint32_t count) {
    return make_compound_value(arena, Value::TAG_ARRAY, elements, nullptr, count);
}

inline Value value_tuple(sql_parser::Arena& arena, const Value* elements, uint32_t count) {
    return make_compound_value(arena, Value::TAG_TUPLE, elements, nullptr, count);
}

inline Value value_named_tuple(sql_parser::Arena& arena,
                               const Value* elements,
                               const StringRef* field_names,
                               uint32_t count) {
    return make_compound_value(arena, Value::TAG_TUPLE, elements, field_names, count);
}
```

- [ ] **Step 4: Run the constructor tests to verify GREEN**

Run:

```bash
./run_tests --gtest_filter="ValueTest.ArrayValue:ValueTest.NamedTupleValue" --gtest_brief=1
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/value.h tests/test_value.cpp
git commit -m "feat: add compound value runtime tags"
```

### Task 2: Evaluate tuple and array AST nodes into runtime values

**Files:**
- Modify: `include/sql_engine/expression_eval.h`
- Modify: `tests/test_expression_eval.cpp`

- [ ] **Step 1: Replace the old NULL expectations with positive tests**

Replace the existing deferred-node tests in `tests/test_expression_eval.cpp` with these:

```cpp
TEST_F(ExprEvalTest, TupleReturnsTupleValue) {
    AstNode* n = make_node(arena, NodeType::NODE_TUPLE);
    n->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    n->add_child(leaf(NodeType::NODE_LITERAL_STRING, "hello"));

    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_TUPLE);
    ASSERT_NE(v.compound_val, nullptr);
    EXPECT_EQ(v.compound_val->count, 2u);
    EXPECT_EQ(v.compound_val->elements[0].int_val, 10);
    EXPECT_EQ(std::string(v.compound_val->elements[1].str_val.ptr,
                          v.compound_val->elements[1].str_val.len), "hello");
}

TEST_F(ExprEvalTest, ArrayConstructorReturnsArrayValue) {
    AstNode* n = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    n->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    n->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));

    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_ARRAY);
    ASSERT_NE(v.compound_val, nullptr);
    EXPECT_EQ(v.compound_val->count, 2u);
    EXPECT_EQ(v.compound_val->elements[1].int_val, 20);
}
```

- [ ] **Step 2: Run the focused evaluator tests to verify RED**

Run:

```bash
./run_tests --gtest_filter="ExprEvalTest.TupleReturnsTupleValue:ExprEvalTest.ArrayConstructorReturnsArrayValue" --gtest_brief=1
```

Expected: both tests fail because the evaluator still returns `NULL` for `NODE_TUPLE` and `NODE_ARRAY_CONSTRUCTOR`.

- [ ] **Step 3: Implement compound literal evaluation**

In `include/sql_engine/expression_eval.h`, add a forward declaration plus a small helper near the existing evaluator helpers, then route the node cases through it:

```cpp
template <Dialect D>
Value evaluate_expression(const AstNode* expr,
                          const std::function<Value(StringRef)>& resolve,
                          FunctionRegistry<D>& functions,
                          Arena& arena,
                          SubqueryExecutor<D>* subquery_exec);

template <Dialect D>
Value evaluate_compound_literal(const AstNode* expr,
                                Value::Tag tag,
                                const std::function<Value(StringRef)>& resolve,
                                FunctionRegistry<D>& functions,
                                Arena& arena,
                                SubqueryExecutor<D>* subquery_exec) {
    uint32_t count = detail::child_count(expr);
    Value* elems = static_cast<Value*>(arena.allocate(sizeof(Value) * count));
    uint32_t i = 0;
    for (const AstNode* child = expr->first_child; child; child = child->next_sibling, ++i) {
        elems[i] = evaluate_expression<D>(child, resolve, functions, arena, subquery_exec);
    }
    return (tag == Value::TAG_ARRAY)
        ? value_array(arena, elems, count)
        : value_tuple(arena, elems, count);
}

case NodeType::NODE_TUPLE:
    return evaluate_compound_literal<D>(
        expr, Value::TAG_TUPLE, resolve, functions, arena, subquery_exec);

case NodeType::NODE_ARRAY_CONSTRUCTOR:
    return evaluate_compound_literal<D>(
        expr, Value::TAG_ARRAY, resolve, functions, arena, subquery_exec);
```

- [ ] **Step 4: Run the focused evaluator tests to verify GREEN**

Run:

```bash
./run_tests --gtest_filter="ExprEvalTest.TupleReturnsTupleValue:ExprEvalTest.ArrayConstructorReturnsArrayValue" --gtest_brief=1
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/expression_eval.h tests/test_expression_eval.cpp
git commit -m "feat: evaluate tuple and array literals"
```

### Task 3: Make array subscript operate on evaluated arrays

**Files:**
- Modify: `include/sql_engine/expression_eval.h`
- Modify: `tests/test_expression_eval.cpp`

- [ ] **Step 1: Add failing tests for runtime array values**

Append these tests to `tests/test_expression_eval.cpp`:

```cpp
TEST_F(ExprEvalTest, ArraySubscriptFromResolvedRuntimeArray) {
    resolver = [this](StringRef name) -> Value {
        if (name.equals_ci("arr", 3)) {
            Value elems[3] = {value_int(10), value_int(20), value_int(30)};
            return value_array(arena, elems, 3);
        }
        return value_null();
    };

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(leaf(NodeType::NODE_COLUMN_REF, "arr"));
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "1"));

    auto v = eval_pg(subscript);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 10);
}

TEST_F(ExprEvalTest, NestedArraySubscriptUsesRuntimeValues) {
    AstNode* inner1 = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    inner1->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    inner1->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));

    AstNode* inner2 = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    inner2->add_child(leaf(NodeType::NODE_LITERAL_INT, "30"));
    inner2->add_child(leaf(NodeType::NODE_LITERAL_INT, "40"));

    AstNode* outer = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    outer->add_child(inner1);
    outer->add_child(inner2);

    AstNode* first = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    first->add_child(outer);
    first->add_child(leaf(NodeType::NODE_LITERAL_INT, "2"));

    AstNode* second = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    second->add_child(first);
    second->add_child(leaf(NodeType::NODE_LITERAL_INT, "1"));

    auto v = eval_pg(second);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 30);
}
```

- [ ] **Step 2: Run the array subscript tests to verify RED**

Run:

```bash
./run_tests --gtest_filter="ExprEvalTest.ArraySubscript*" --gtest_brief=1
```

Expected: the new runtime-array tests fail because `NODE_ARRAY_SUBSCRIPT` still special-cases only AST array constructors.

- [ ] **Step 3: Rework subscripting to read `TAG_ARRAY` payloads**

Replace the old `NODE_ARRAY_SUBSCRIPT` logic in `include/sql_engine/expression_eval.h` with runtime evaluation:

```cpp
case NodeType::NODE_ARRAY_SUBSCRIPT: {
    const AstNode* array_expr = expr->first_child;
    const AstNode* index_expr = array_expr ? array_expr->next_sibling : nullptr;
    if (!array_expr || !index_expr) return value_null();

    Value array_val = evaluate_expression<D>(array_expr, resolve, functions, arena, subquery_exec);
    if (array_val.tag != Value::TAG_ARRAY || !array_val.compound_val) return value_null();

    Value idx = evaluate_expression<D>(index_expr, resolve, functions, arena, subquery_exec);
    if (idx.is_null()) return value_null();

    int64_t pos = idx.to_int64();
    pos = (D == sql_parser::Dialect::PostgreSQL) ? pos - 1 : pos;
    if (pos < 0 || static_cast<uint32_t>(pos) >= array_val.compound_val->count) {
        return value_null();
    }
    return array_val.compound_val->elements[pos];
}
```

- [ ] **Step 4: Run the array subscript tests to verify GREEN**

Run:

```bash
./run_tests --gtest_filter="ExprEvalTest.ArraySubscript*" --gtest_brief=1
```

Expected: existing literal-array tests and the new runtime-array tests all pass.

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/expression_eval.h tests/test_expression_eval.cpp
git commit -m "feat: support runtime array subscripts"
```

### Task 4: Add named tuple field access

**Files:**
- Modify: `include/sql_engine/expression_eval.h`
- Modify: `tests/test_expression_eval.cpp`

- [ ] **Step 1: Add failing field access tests**

Append these tests to `tests/test_expression_eval.cpp`:

```cpp
TEST_F(ExprEvalTest, FieldAccessReturnsNamedTupleField) {
    resolver = [this](StringRef name) -> Value {
        if (name.equals_ci("rec", 3)) {
            Value elems[2] = {value_int(7), value_string(StringRef{"bob", 3})};
            StringRef fields[2] = {StringRef{"id", 2}, StringRef{"name", 4}};
            return value_named_tuple(arena, elems, fields, 2);
        }
        return value_null();
    };

    AstNode* access = make_node(arena, NodeType::NODE_FIELD_ACCESS);
    access->add_child(leaf(NodeType::NODE_COLUMN_REF, "rec"));
    access->add_child(leaf(NodeType::NODE_IDENTIFIER, "name"));

    auto v = eval_mysql(access);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "bob");
}

TEST_F(ExprEvalTest, FieldAccessUnnamedTupleReturnsNull) {
    resolver = [this](StringRef name) -> Value {
        if (name.equals_ci("rec", 3)) {
            Value elems[1] = {value_int(7)};
            return value_tuple(arena, elems, 1);
        }
        return value_null();
    };

    AstNode* access = make_node(arena, NodeType::NODE_FIELD_ACCESS);
    access->add_child(leaf(NodeType::NODE_COLUMN_REF, "rec"));
    access->add_child(leaf(NodeType::NODE_IDENTIFIER, "id"));

    EXPECT_TRUE(eval_mysql(access).is_null());
}
```

- [ ] **Step 2: Run the field access tests to verify RED**

Run:

```bash
./run_tests --gtest_filter="ExprEvalTest.FieldAccess*" --gtest_brief=1
```

Expected: the new tests fail because `NODE_FIELD_ACCESS` still returns `NULL` unconditionally.

- [ ] **Step 3: Implement named field lookup for `TAG_TUPLE`**

Replace the `NODE_FIELD_ACCESS` deferred case in `include/sql_engine/expression_eval.h` with:

```cpp
case NodeType::NODE_FIELD_ACCESS: {
    const AstNode* base_expr = expr->first_child;
    const AstNode* field_expr = base_expr ? base_expr->next_sibling : nullptr;
    if (!base_expr || !field_expr) return value_null();

    Value base = evaluate_expression<D>(base_expr, resolve, functions, arena, subquery_exec);
    if (base.tag != Value::TAG_TUPLE || !base.compound_val || !base.compound_val->field_names) {
        return value_null();
    }

    StringRef field = field_expr->value();
    for (uint32_t i = 0; i < base.compound_val->count; ++i) {
        if (base.compound_val->field_names[i].equals_ci(field.ptr, field.len)) {
            return base.compound_val->elements[i];
        }
    }
    return value_null();
}
```

- [ ] **Step 4: Run the field access tests to verify GREEN**

Run:

```bash
./run_tests --gtest_filter="ExprEvalTest.FieldAccess*" --gtest_brief=1
```

Expected: both field access tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/expression_eval.h tests/test_expression_eval.cpp
git commit -m "feat: add named tuple field access"
```

### Task 5: Update issue notes and run full verification

**Files:**
- Modify: `docs/issues/05-expression-and-type-semantics.md`
- Verify: `tests/test_value.cpp`
- Verify: `tests/test_expression_eval.cpp`

- [ ] **Step 1: Update the Issue 05 progress note**

Replace the current progress section in `docs/issues/05-expression-and-type-semantics.md` with explicit compound-value progress:

```md
## Progress Notes

- `CHAR_LENGTH` now counts UTF-8 code points, while `LENGTH` remains byte-based.
- PostgreSQL explicit casts now accept `on` / `off` for boolean strings and support string-to-`DATE` / `TIME` / `DATETIME` / `TIMESTAMP` helper paths.
- Arrays and tuples now have first-class runtime `Value` support, including runtime array subscript evaluation and named tuple field access.
- Decimal representation and richer planner/type-system support for structured values remain open.
```

- [ ] **Step 2: Run targeted verification**

Run:

```bash
./run_tests --gtest_filter="ValueTest.ArrayValue:ValueTest.NamedTupleValue:ExprEvalTest.TupleReturnsTupleValue:ExprEvalTest.ArrayConstructorReturnsArrayValue:ExprEvalTest.ArraySubscript*:ExprEvalTest.FieldAccess*" --gtest_brief=1
```

Expected: all targeted compound-value tests pass.

- [ ] **Step 3: Run the full suite**

Run:

```bash
./run_tests --gtest_brief=1
```

Expected: no regressions; local backend-dependent integration tests may still skip if MySQL/PostgreSQL services are unavailable.

- [ ] **Step 4: Commit**

```bash
git add docs/issues/05-expression-and-type-semantics.md
git commit -m "docs: update compound value support progress"
```
