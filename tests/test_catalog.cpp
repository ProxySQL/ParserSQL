#include <gtest/gtest.h>
#include "sql_engine/catalog.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/catalog_resolver.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/parser.h"
#include <string>
#include <cstring>

using namespace sql_engine;
using namespace sql_parser;

// ===========================================================================
// InMemoryCatalog tests
// ===========================================================================

class InMemoryCatalogTest : public ::testing::Test {
protected:
    InMemoryCatalog catalog;

    void SetUp() override {
        catalog.add_table("", "users", {
            {"id",    SqlType::make_int(),        false},
            {"name",  SqlType::make_varchar(255), true},
            {"age",   SqlType::make_int(),        true},
        });
    }
};

TEST_F(InMemoryCatalogTest, GetTableByName) {
    StringRef name{"users", 5};
    const TableInfo* t = catalog.get_table(name);
    ASSERT_NE(t, nullptr);
    EXPECT_TRUE(t->table_name.equals_ci("users", 5));
    EXPECT_EQ(t->column_count, 3);
}

TEST_F(InMemoryCatalogTest, GetTableWithSchema) {
    catalog.add_table("mydb", "orders", {
        {"order_id", SqlType::make_int(), false},
        {"total",    SqlType::make_double(), true},
    });
    StringRef schema{"mydb", 4};
    StringRef table{"orders", 6};
    const TableInfo* t = catalog.get_table(schema, table);
    ASSERT_NE(t, nullptr);
    EXPECT_TRUE(t->table_name.equals_ci("orders", 6));
    EXPECT_TRUE(t->schema_name.equals_ci("mydb", 4));
    EXPECT_EQ(t->column_count, 2);
}

TEST_F(InMemoryCatalogTest, GetTableNotFound) {
    StringRef name{"nonexistent", 11};
    const TableInfo* t = catalog.get_table(name);
    EXPECT_EQ(t, nullptr);
}

TEST_F(InMemoryCatalogTest, GetColumnByName) {
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    StringRef cname{"name", 4};
    const ColumnInfo* c = catalog.get_column(t, cname);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->ordinal, 1);
    EXPECT_EQ(c->type.kind, SqlType::VARCHAR);
    EXPECT_TRUE(c->nullable);
}

TEST_F(InMemoryCatalogTest, GetColumnId) {
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    StringRef cname{"id", 2};
    const ColumnInfo* c = catalog.get_column(t, cname);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->ordinal, 0);
    EXPECT_EQ(c->type.kind, SqlType::INT);
    EXPECT_FALSE(c->nullable);
}

TEST_F(InMemoryCatalogTest, GetColumnNotFound) {
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    StringRef cname{"email", 5};
    const ColumnInfo* c = catalog.get_column(t, cname);
    EXPECT_EQ(c, nullptr);
}

TEST_F(InMemoryCatalogTest, DropTable) {
    catalog.drop_table("", "users");
    StringRef name{"users", 5};
    const TableInfo* t = catalog.get_table(name);
    EXPECT_EQ(t, nullptr);
}

TEST_F(InMemoryCatalogTest, MultipleTables) {
    catalog.add_table("", "products", {
        {"pid",   SqlType::make_int(), false},
        {"pname", SqlType::make_varchar(100), true},
    });

    StringRef u{"users", 5};
    StringRef p{"products", 8};
    const TableInfo* tu = catalog.get_table(u);
    const TableInfo* tp = catalog.get_table(p);
    ASSERT_NE(tu, nullptr);
    ASSERT_NE(tp, nullptr);
    EXPECT_EQ(tu->column_count, 3);
    EXPECT_EQ(tp->column_count, 2);
}

TEST_F(InMemoryCatalogTest, CaseInsensitiveTableLookup) {
    StringRef upper{"USERS", 5};
    const TableInfo* t = catalog.get_table(upper);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->column_count, 3);
}

TEST_F(InMemoryCatalogTest, CaseInsensitiveColumnLookup) {
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    StringRef cname{"NAME", 4};
    const ColumnInfo* c = catalog.get_column(t, cname);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->ordinal, 1);
}

TEST_F(InMemoryCatalogTest, CaseInsensitiveQualifiedLookup) {
    catalog.add_table("MyDB", "Orders", {
        {"order_id", SqlType::make_int(), false},
    });
    StringRef schema{"mydb", 4};
    StringRef table{"ORDERS", 6};
    const TableInfo* t = catalog.get_table(schema, table);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->column_count, 1);
}

TEST_F(InMemoryCatalogTest, GetColumnOnNullTable) {
    StringRef cname{"id", 2};
    const ColumnInfo* c = catalog.get_column(nullptr, cname);
    EXPECT_EQ(c, nullptr);
}

// ===========================================================================
// Resolver tests
// ===========================================================================

class CatalogResolverTest : public ::testing::Test {
protected:
    InMemoryCatalog catalog;

    void SetUp() override {
        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
        });
    }
};

TEST_F(CatalogResolverTest, ResolveColumnByName) {
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    // Row: id=42, name="John", age=30
    const char* john = "John";
    Value row[] = {
        value_int(42),
        value_string(StringRef{john, 4}),
        value_int(30),
    };

    auto resolver = make_resolver(catalog, t, row);

    // Resolve "age"
    Value age = resolver(StringRef{"age", 3});
    ASSERT_EQ(age.tag, Value::TAG_INT64);
    EXPECT_EQ(age.int_val, 30);

    // Resolve "name"
    Value name = resolver(StringRef{"name", 4});
    ASSERT_EQ(name.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(name.str_val.ptr, name.str_val.len), "John");

    // Resolve "id"
    Value id = resolver(StringRef{"id", 2});
    ASSERT_EQ(id.tag, Value::TAG_INT64);
    EXPECT_EQ(id.int_val, 42);
}

TEST_F(CatalogResolverTest, ResolveUnknownColumnReturnsNull) {
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    Value row[] = { value_int(42), value_int(0), value_int(30) };
    auto resolver = make_resolver(catalog, t, row);

    Value v = resolver(StringRef{"email", 5});
    EXPECT_TRUE(v.is_null());
}

TEST_F(CatalogResolverTest, ResolverCaseInsensitive) {
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    Value row[] = { value_int(42), value_int(0), value_int(30) };
    auto resolver = make_resolver(catalog, t, row);

    Value v = resolver(StringRef{"AGE", 3});
    ASSERT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 30);
}

// ===========================================================================
// End-to-end: parse SQL WHERE clause, evaluate with catalog + resolver
// ===========================================================================

class CatalogEndToEndTest : public ::testing::Test {
protected:
    InMemoryCatalog catalog;
    Parser<Dialect::MySQL> parser;
    FunctionRegistry<Dialect::MySQL> funcs;

    void SetUp() override {
        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
        });
        funcs.register_builtins();
    }

    // Helper: find child node by type
    static const AstNode* find_child(const AstNode* node, NodeType type) {
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == type) return c;
        }
        return nullptr;
    }
};

TEST_F(CatalogEndToEndTest, WhereAgeGreaterThan18) {
    const char* sql = "SELECT name FROM users WHERE age > 18";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);

    // Navigate to WHERE clause expression
    const AstNode* where_clause = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    ASSERT_NE(where_clause, nullptr);
    const AstNode* where_expr = where_clause->first_child;
    ASSERT_NE(where_expr, nullptr);

    // Set up row: id=1, name="John", age=30
    const char* john = "John";
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    Value row[] = {
        value_int(1),
        value_string(StringRef{john, 4}),
        value_int(30),
    };

    auto resolver = make_resolver(catalog, t, row);
    std::function<Value(StringRef)> resolve_fn = resolver;

    Value result = evaluate_expression<Dialect::MySQL>(
        where_expr, resolve_fn, funcs, parser.arena());

    ASSERT_EQ(result.tag, Value::TAG_BOOL);
    EXPECT_TRUE(result.bool_val);
}

TEST_F(CatalogEndToEndTest, WhereAgeGreaterThan18False) {
    const char* sql = "SELECT name FROM users WHERE age > 18";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);

    const AstNode* where_clause = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    ASSERT_NE(where_clause, nullptr);
    const AstNode* where_expr = where_clause->first_child;
    ASSERT_NE(where_expr, nullptr);

    const char* alice = "Alice";
    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    // age=10, should be false
    Value row[] = {
        value_int(2),
        value_string(StringRef{alice, 5}),
        value_int(10),
    };

    auto resolver = make_resolver(catalog, t, row);
    std::function<Value(StringRef)> resolve_fn = resolver;

    Value result = evaluate_expression<Dialect::MySQL>(
        where_expr, resolve_fn, funcs, parser.arena());

    ASSERT_EQ(result.tag, Value::TAG_BOOL);
    EXPECT_FALSE(result.bool_val);
}

TEST_F(CatalogEndToEndTest, SelectColumnExpression) {
    const char* sql = "SELECT age + 1 FROM users";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);

    // Navigate to first SELECT_ITEM expression
    const AstNode* item_list = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(item_list, nullptr);
    const AstNode* first_item = item_list->first_child;
    ASSERT_NE(first_item, nullptr);
    const AstNode* expr = first_item->first_child;
    ASSERT_NE(expr, nullptr);

    StringRef tname{"users", 5};
    const TableInfo* t = catalog.get_table(tname);
    ASSERT_NE(t, nullptr);

    const char* john = "John";
    Value row[] = {
        value_int(1),
        value_string(StringRef{john, 4}),
        value_int(30),
    };

    auto resolver = make_resolver(catalog, t, row);
    std::function<Value(StringRef)> resolve_fn = resolver;

    Value result = evaluate_expression<Dialect::MySQL>(
        expr, resolve_fn, funcs, parser.arena());

    ASSERT_EQ(result.tag, Value::TAG_INT64);
    EXPECT_EQ(result.int_val, 31);
}
