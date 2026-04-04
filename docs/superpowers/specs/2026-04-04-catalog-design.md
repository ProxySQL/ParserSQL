# SQL Engine Catalog — Design Specification

## Overview

The catalog provides schema metadata (tables, columns, types) to the query engine. It is an abstract interface that embedders implement for their specific use case (in-memory, learned from traffic, loaded from config, etc.). A reference in-memory implementation is included.

This is sub-project 3 of the query engine. It depends on sub-project 1 (type system) and connects to sub-project 2 (expression evaluator) via a resolver utility.

### Goals

- **Abstract Catalog interface** — pure virtual, read-only, embedders implement it
- **InMemoryCatalog reference implementation** — programmatic schema definition for testing and simple use cases
- **Resolver utility** — bridges Catalog + row values to the expression evaluator's callback interface
- **Minimal metadata** — tables, columns, types, nullable flag. No indexes, constraints, or views yet.

### Constraints

- C++17
- Uses `SqlType` from the type system and `StringRef` from the parser
- Virtual interface (not template) — embedders provide implementations at runtime
- Thread-safe reads (const methods). Mutation is the implementation's concern.

### Non-Goals

- Index metadata (deferred to optimizer sub-project)
- Views, constraints, foreign keys
- Catalog persistence (loading/saving)
- Traffic-learning catalog (ProxySQL-specific implementation, not part of the library)

---

## Data Structures

```cpp
namespace sql_engine {

struct ColumnInfo {
    sql_parser::StringRef name;
    SqlType type;
    uint16_t ordinal;       // 0-based position in table
    bool nullable;
};

struct TableInfo {
    sql_parser::StringRef schema_name;  // empty if default/no schema
    sql_parser::StringRef table_name;
    const ColumnInfo* columns;
    uint16_t column_count;
};

// Convenience for building columns programmatically
struct ColumnDef {
    const char* name;
    SqlType type;
    bool nullable = true;
};

} // namespace sql_engine
```

---

## Catalog Interface

```cpp
class Catalog {
public:
    virtual ~Catalog() = default;

    // Find a table by unqualified name. Returns nullptr if not found.
    virtual const TableInfo* get_table(sql_parser::StringRef name) const = 0;

    // Find a table by qualified name (schema.table). Returns nullptr if not found.
    virtual const TableInfo* get_table(sql_parser::StringRef schema,
                                        sql_parser::StringRef table) const = 0;

    // Find a column in a table by name. Returns nullptr if not found.
    virtual const ColumnInfo* get_column(const TableInfo* table,
                                          sql_parser::StringRef column_name) const = 0;
};
```

**Design decisions:**
- **Pure virtual** — the library does not own the schema. Embedders provide it.
- **Raw const pointers** — catalog owns the data, callers get read-only views. No ownership transfer. Same pattern as `FunctionRegistry::lookup()`.
- **Case-insensitive name matching** — `get_table` and `get_column` should match case-insensitively (SQL identifiers are case-insensitive by default in both MySQL and PostgreSQL for unquoted names).

---

## InMemoryCatalog — Reference Implementation

```cpp
class InMemoryCatalog : public Catalog {
public:
    // Add a table with columns
    void add_table(const char* schema, const char* table,
                   std::initializer_list<ColumnDef> columns);

    // Remove a table
    void drop_table(const char* schema, const char* table);

    // Catalog interface
    const TableInfo* get_table(sql_parser::StringRef name) const override;
    const TableInfo* get_table(sql_parser::StringRef schema,
                                sql_parser::StringRef table) const override;
    const ColumnInfo* get_column(const TableInfo* table,
                                  sql_parser::StringRef column_name) const override;
};
```

**Internal storage:**
- `std::unordered_map<std::string, TableData>` keyed by lowercase table name (for case-insensitive lookup)
- For qualified names: key is `"schema.table"` (lowercase)
- `TableData` owns the `TableInfo` and a `std::vector<ColumnInfo>` for the columns
- Column strings are stored as `std::string` inside `TableData`, with `StringRef` pointing into them

**Column lookup:** Linear scan within a table (tables rarely have >100 columns). A hash map per table is unnecessary overhead.

---

## Catalog Resolver — Bridge to Expression Evaluator

```cpp
// Create a column resolver callback from catalog + table + row values
// Returns a std::function<Value(StringRef)> suitable for evaluate_expression()
inline auto make_resolver(const Catalog& catalog,
                          const TableInfo* table,
                          const Value* row_values) {
    return [&catalog, table, row_values](sql_parser::StringRef col_name) -> Value {
        const ColumnInfo* col = catalog.get_column(table, col_name);
        if (!col) return value_null();
        return row_values[col->ordinal];
    };
}
```

This bridges:
- **Catalog** (knows column names → ordinals)
- **Row values** (array indexed by ordinal)
- **Expression evaluator** (needs `StringRef → Value` callback)

No allocation, no virtual calls in the hot path — the resolver is a lambda that captures pointers.

---

## File Organization

```
include/sql_engine/
    catalog.h            — Catalog interface, TableInfo, ColumnInfo, ColumnDef
    in_memory_catalog.h  — InMemoryCatalog implementation (header-only or with .cpp)
    catalog_resolver.h   — make_resolver() utility

src/sql_engine/
    in_memory_catalog.cpp — InMemoryCatalog method implementations

tests/
    test_catalog.cpp     — All catalog tests
```

---

## Testing Strategy

### InMemoryCatalog tests
- Add table, get by name → found
- Add table with schema, get by qualified name → found
- Get table not found → nullptr
- Get column by name → correct ColumnInfo (type, ordinal, nullable)
- Get column not found → nullptr
- Drop table, get → nullptr
- Multiple tables, correct isolation
- Case-insensitive table/column lookup

### Resolver integration tests
- Create catalog with "users" table (id INT, name VARCHAR, age INT)
- Create row values: [42, "John", 30]
- Use make_resolver to create callback
- Call evaluate_expression on `age > 18` → true
- Call evaluate_expression on `name` → "John"
- Unknown column → NULL

### End-to-end test
- Parse `SELECT name FROM users WHERE age > 18`
- Navigate AST to WHERE clause expression
- Evaluate with catalog resolver + row values
- Verify result

---

## Performance Targets

| Operation | Target |
|---|---|
| get_table (hash lookup) | <50ns |
| get_column (linear scan, ~10 columns) | <30ns |
| make_resolver (lambda creation) | <5ns |
| Resolver callback (ordinal lookup) | <20ns |
