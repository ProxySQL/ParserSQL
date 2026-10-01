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

TEST(Pr67Metadata, CatalogQualifiedDmlReportsSchemaAndTable) {
    struct Case { const char* sql; StmtType type; };
    const Case cases[] = {
        {"INSERT INTO db.public.t VALUES (1)", StmtType::INSERT},
        {"UPDATE db.public.t SET x=1", StmtType::UPDATE},
        {"UPDATE ONLY (db.public.t) SET x=1", StmtType::UPDATE},
        {"DELETE FROM db.public.t", StmtType::DELETE_STMT},
        {"DELETE FROM db.public.t * AS dst", StmtType::DELETE_STMT},
        {"MERGE INTO db.public.t USING src ON true WHEN MATCHED THEN DELETE", StmtType::MERGE},
        {"MERGE INTO ONLY (db.public.t) AS dst USING src ON true WHEN MATCHED THEN DELETE", StmtType::MERGE},
    };
    for (const char* prefix : {"", "WITH c AS (SELECT * FROM other.source) "}) {
        for (const auto& test : cases) {
            std::string sql = std::string(prefix) + test.sql;
            SCOPED_TRACE(sql);
            Parser<Dialect::PostgreSQL> parser;
            auto result = parser.parse(sql.data(), sql.size());
            ASSERT_TRUE(result.ok() && result.full_input);
            EXPECT_EQ(result.stmt_type, test.type);
            EXPECT_EQ(std::string(result.schema_name.ptr, result.schema_name.len), "public");
            EXPECT_EQ(std::string(result.table_name.ptr, result.table_name.len), "t");
        }
    }
}

TEST(Pr67Metadata, QuotedCatalogQualifiedDmlPreservesIdentifierComponents) {
    const char* sql = "WITH c AS (SELECT 1) UPDATE \"my.db\".\"App.Schema\".\"Target.Table\" SET x=1";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    EXPECT_EQ(std::string(result.schema_name.ptr, result.schema_name.len), "App.Schema");
    EXPECT_EQ(std::string(result.table_name.ptr, result.table_name.len), "Target.Table");
}

TEST(Pr67Metadata, MysqlDmlKeepsSchemaTableAndDeleteWildcardMetadata) {
    struct Case { const char* sql; const char* schema; const char* table; };
    const Case cases[] = {
        {"INSERT INTO t VALUES (1)", "", "t"},
        {"INSERT INTO db.t VALUES (1)", "db", "t"},
        {"REPLACE INTO db.t VALUES (1)", "db", "t"},
        {"UPDATE db.t SET x=1", "db", "t"},
        {"DELETE FROM db.t", "db", "t"},
        {"DELETE t.* FROM t JOIN u ON t.id=u.id", "", "t"},
        {"DELETE db.t.* FROM db.t JOIN u ON t.id=u.id", "db", "t"},
    };
    for (const auto& test : cases) {
        SCOPED_TRACE(test.sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(test.sql, std::strlen(test.sql));
        ASSERT_TRUE(result.ok() && result.full_input);
        EXPECT_EQ(std::string(result.schema_name.ptr ? result.schema_name.ptr : "", result.schema_name.len), test.schema);
        EXPECT_EQ(std::string(result.table_name.ptr ? result.table_name.ptr : "", result.table_name.len), test.table);
    }
}
