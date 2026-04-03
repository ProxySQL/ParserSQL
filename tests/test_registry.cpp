#include <gtest/gtest.h>
#include "sql_engine/function_registry.h"
#include "sql_engine/value.h"
#include "sql_parser/arena.h"

using namespace sql_engine;
using sql_parser::Dialect;
using sql_parser::Arena;

class RegistryTest : public ::testing::Test {
protected:
    Arena arena{4096};
};

// --- MySQL registry ---

TEST_F(RegistryTest, MySQLBuiltinsRegistered) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_GT(reg.size(), 0u);
}

TEST_F(RegistryTest, MySQLLookupAbs) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("ABS", 3);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->min_args, 1);
    EXPECT_EQ(entry->max_args, 1);
}

TEST_F(RegistryTest, MySQLLookupCaseInsensitive) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("abs", 3), nullptr);
    EXPECT_NE(reg.lookup("Abs", 3), nullptr);
    EXPECT_NE(reg.lookup("ABS", 3), nullptr);
}

TEST_F(RegistryTest, MySQLLookupUnknownReturnsNull) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_EQ(reg.lookup("DOES_NOT_EXIST", 14), nullptr);
}

TEST_F(RegistryTest, MySQLHasIfnull) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("IFNULL", 6), nullptr);
}

TEST_F(RegistryTest, MySQLHasIf) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("IF", 2), nullptr);
}

// --- PostgreSQL registry ---

TEST_F(RegistryTest, PgSQLBuiltinsRegistered) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_GT(reg.size(), 0u);
}

TEST_F(RegistryTest, PgSQLLookupAbs) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("ABS", 3), nullptr);
}

TEST_F(RegistryTest, PgSQLNoIfnull) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_EQ(reg.lookup("IFNULL", 6), nullptr);
}

TEST_F(RegistryTest, PgSQLNoIf) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_EQ(reg.lookup("IF", 2), nullptr);
}

// --- Dispatch through registry ---

TEST_F(RegistryTest, MySQLDispatchAbsViaRegistry) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("ABS", 3);
    ASSERT_NE(entry, nullptr);

    Value args[] = {value_int(-42)};
    Value result = entry->impl(args, 1, arena);
    EXPECT_EQ(result.int_val, 42);
}

TEST_F(RegistryTest, MySQLDispatchConcatViaRegistry) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("CONCAT", 6);
    ASSERT_NE(entry, nullptr);

    Value args[] = {
        value_string(sql_parser::StringRef{"hello", 5}),
        value_string(sql_parser::StringRef{" world", 6})
    };
    Value result = entry->impl(args, 2, arena);
    EXPECT_EQ(std::string(result.str_val.ptr, result.str_val.len), "hello world");
}

TEST_F(RegistryTest, PgSQLDispatchCoalesceViaRegistry) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("COALESCE", 8);
    ASSERT_NE(entry, nullptr);

    Value args[] = {value_null(), value_int(42), value_int(99)};
    Value result = entry->impl(args, 3, arena);
    EXPECT_EQ(result.int_val, 42);
}

// --- Lookup all P0 functions ---

TEST_F(RegistryTest, MySQLAllP0FunctionsPresent) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();

    const char* names[] = {
        "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNCATE", "MOD",
        "POWER", "POW", "SQRT", "SIGN",
        "COALESCE", "NULLIF", "LEAST", "GREATEST", "IFNULL", "IF",
        "CONCAT", "CONCAT_WS", "LENGTH", "CHAR_LENGTH", "UPPER", "UCASE",
        "LOWER", "LCASE", "SUBSTRING", "SUBSTR", "TRIM", "LTRIM", "RTRIM",
        "REPLACE", "REVERSE", "LEFT", "RIGHT", "LPAD", "RPAD", "REPEAT", "SPACE"
    };
    for (const char* name : names) {
        EXPECT_NE(reg.lookup(name, static_cast<uint32_t>(std::strlen(name))), nullptr)
            << "Missing function: " << name;
    }
}

TEST_F(RegistryTest, PgSQLAllP0FunctionsPresent) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();

    // Same as MySQL minus IFNULL and IF
    const char* names[] = {
        "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNCATE", "MOD",
        "POWER", "POW", "SQRT", "SIGN",
        "COALESCE", "NULLIF", "LEAST", "GREATEST",
        "CONCAT", "CONCAT_WS", "LENGTH", "CHAR_LENGTH", "UPPER", "UCASE",
        "LOWER", "LCASE", "SUBSTRING", "SUBSTR", "TRIM", "LTRIM", "RTRIM",
        "REPLACE", "REVERSE", "LEFT", "RIGHT", "LPAD", "RPAD", "REPEAT", "SPACE"
    };
    for (const char* name : names) {
        EXPECT_NE(reg.lookup(name, static_cast<uint32_t>(std::strlen(name))), nullptr)
            << "Missing function: " << name;
    }
}
