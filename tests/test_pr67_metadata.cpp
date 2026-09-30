#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include <cstring>
#include <string>

using namespace sql_parser;

TEST(Pr67Metadata, WithDmlReportsMainTargetRatherThanCteRelations) {
    struct Case { const char* sql; StmtType type; const char* schema; const char* table; };
    const Case cases[] = {
        {"WITH c AS (SELECT * FROM other.source) INSERT INTO db.t VALUES (1)", StmtType::INSERT, "db", "t"},
        {"WITH c AS (SELECT * FROM other.source) UPDATE db.t SET x=1", StmtType::UPDATE, "db", "t"},
        {"WITH c AS (SELECT * FROM other.source) DELETE FROM db.t", StmtType::DELETE_STMT, "db", "t"},
        {"WITH c AS (DELETE FROM other.source RETURNING *) UPDATE ONLY (db.t) SET x=1", StmtType::UPDATE, "db", "t"},
        {"WITH c AS (SELECT 1) DELETE FROM db.t * AS target", StmtType::DELETE_STMT, "db", "t"},
        {"WITH c AS (SELECT 1) UPDATE \"App\".\"Target\" SET x=1", StmtType::UPDATE, "App", "Target"},
        {"WITH c AS (SELECT * FROM other.source) UPDATE t SET x=1", StmtType::UPDATE, "", "t"},
        {"WITH c AS (DELETE FROM other.source RETURNING *) SELECT * FROM c", StmtType::SELECT, "", ""},
    };
    for (const auto& test : cases) {
        SCOPED_TRACE(test.sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(test.sql, std::strlen(test.sql));
        ASSERT_TRUE(result.ok() && result.full_input);
        EXPECT_EQ(result.stmt_type, test.type);
        EXPECT_EQ(std::string(result.schema_name.ptr ? result.schema_name.ptr : "", result.schema_name.len), test.schema);
        EXPECT_EQ(std::string(result.table_name.ptr ? result.table_name.ptr : "", result.table_name.len), test.table);
    }
}

TEST(Pr67Metadata, MergeReportsTargetIndependentlyOfSourceAndWithPrefix) {
    for (const char* prefix : {"", "WITH c AS (SELECT * FROM another.relation) "}) {
        for (const char* target : {"db.t", "ONLY (db.t)", "db.t *"}) {
            std::string sql = std::string(prefix) + "MERGE INTO " + target +
                " AS dst USING source.s AS src ON dst.id=src.id WHEN MATCHED THEN DELETE";
            SCOPED_TRACE(sql);
            Parser<Dialect::PostgreSQL> parser;
            auto result = parser.parse(sql.data(), sql.size());
            ASSERT_TRUE(result.ok() && result.full_input);
            EXPECT_EQ(result.stmt_type, StmtType::MERGE);
            EXPECT_EQ(std::string(result.schema_name.ptr ? result.schema_name.ptr : "", result.schema_name.len), "db");
            EXPECT_EQ(std::string(result.table_name.ptr ? result.table_name.ptr : "", result.table_name.len), "t");
        }
    }
}
