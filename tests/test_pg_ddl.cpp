#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

TEST(PgDdl, TableDefinitionIsStructuredAndComplete) {
    const char* sql = "CREATE TABLE public.accounts (id bigint PRIMARY KEY, balance numeric(12,2) DEFAULT 0 CHECK (balance >= 0), owner text NOT NULL, UNIQUE (owner))";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_NE(result.ast->type, NodeType::NODE_STATEMENT);
}

TEST(PgDdl, CommonCommandsRoundTrip) {
    const char* queries[] = {
        "CREATE TEMP TABLE IF NOT EXISTS t (id integer GENERATED ALWAYS AS IDENTITY, note text DEFAULT 'n', CHECK (id > 0)) ON COMMIT DROP",
        "CREATE TABLE t (id integer, parent integer REFERENCES p(id) ON DELETE CASCADE, CONSTRAINT u UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE t (LIKE source INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
        "CREATE UNLOGGED TABLE t AS SELECT 1 AS id WITH NO DATA",
        "CREATE TABLE t (id integer, label text) PARTITION BY RANGE (id)",
        "CREATE TABLE t (id integer) WITH (fillfactor = 70) TABLESPACE fast",
        "ALTER TABLE IF EXISTS ONLY t ADD COLUMN IF NOT EXISTS n numeric(8,2) DEFAULT 2, DROP COLUMN IF EXISTS old CASCADE",
        "ALTER TABLE t ALTER COLUMN n TYPE bigint USING n::bigint, ALTER COLUMN n SET NOT NULL",
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES p(id) NOT VALID",
        "ALTER TABLE t RENAME COLUMN old TO new_name",
        "ALTER TABLE t OWNER TO alice",
        "ALTER TABLE t VALIDATE CONSTRAINT fk",
        "DROP TABLE IF EXISTS a, public.b CASCADE",
        "DROP INDEX CONCURRENTLY IF EXISTS ix",
        "DROP VIEW IF EXISTS v RESTRICT",
        "DROP FUNCTION IF EXISTS f(integer, text) CASCADE",
        "DROP TRIGGER IF EXISTS audit ON t",
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix ON ONLY public.t USING btree (id DESC NULLS LAST, (lower(name))) INCLUDE (payload) NULLS NOT DISTINCT WITH (fillfactor=90) WHERE id > 0",
        "CREATE OR REPLACE VIEW public.v (id) WITH (security_barrier=true) AS SELECT id FROM t WHERE id > 0 WITH LOCAL CHECK OPTION",
        "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS id WITH NO DATA",
        "CREATE FUNCTION public.f(x integer, y integer DEFAULT 2) RETURNS integer LANGUAGE sql IMMUTABLE STRICT AS $$SELECT x + y$$",
        "CREATE OR REPLACE FUNCTION f(integer) RETURNS SETOF integer AS 'obj', 'symbol' LANGUAGE C COST 2 ROWS 10 PARALLEL SAFE",
        "CREATE FUNCTION f() RETURNS TABLE (id integer, label text) LANGUAGE sql AS $$SELECT 1, 'a'::text$$",
        "CREATE TRIGGER tr BEFORE INSERT OR UPDATE OF x ON t FOR EACH ROW WHEN (new.x > 0) EXECUTE FUNCTION audit('x')",
        "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION audit()",
        "GRANT SELECT, UPDATE (balance) ON TABLE public.accounts TO alice, PUBLIC WITH GRANT OPTION",
        "REVOKE GRANT OPTION FOR ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM alice CASCADE",
        "GRANT USAGE ON SCHEMA public TO alice",
        "GRANT EXECUTE ON FUNCTION f(integer) TO alice",
        "VACUUM (FULL, ANALYZE true, PARALLEL 2) public.t (id), u",
        "VACUUM FULL FREEZE VERBOSE ANALYZE t",
        "ANALYZE (VERBOSE, SKIP_LOCKED true) t (id, name)",
        "ANALYZE",
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        ASSERT_NE(result.ast, nullptr);
        EXPECT_NE(result.ast->type, NodeType::NODE_STATEMENT);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto out = emitter.result();
        std::string emitted(out.ptr, out.len);
        SCOPED_TRACE(emitted);
        auto again = parser.parse(emitted.data(), emitted.size());
        EXPECT_TRUE(again.ok());
        EXPECT_TRUE(again.full_input);
    }
}

TEST(PgDdl, RejectsMalformedAndUnsupportedTails) {
    const char* queries[] = {
        "CREATE TABLE t (id)", "CREATE TABLE t (id integer,)",
        "CREATE TABLE t (id integer DEFAULT)", "CREATE TABLE t (CHECK ())",
        "CREATE TABLE t (FOREIGN KEY (id) REFERENCES)",
        "CREATE TABLE t (id integer) arbitrary tail",
        "CREATE TABLE t (id integer) PARTITION BY RANGE (id DESC)",
        "ALTER TABLE t ADD COLUMN", "ALTER TABLE t ALTER COLUMN x SET",
        "DROP TABLE", "DROP TABLE t,", "DROP TABLE t CASCADE garbage",
        "CREATE INDEX ix ON t ()", "CREATE INDEX ix ON t (id) WHERE",
        "CREATE INDEX ix ON t (t.id)",
        "CREATE VIEW v AS", "CREATE FUNCTION f() RETURNS integer LANGUAGE",
        "CREATE TRIGGER tr BEFORE ON t EXECUTE FUNCTION f()",
        "GRANT SELECT ON t TO", "GRANT MADEUP ON t TO alice",
        "VACUUM (PARALLEL)", "VACUUM (FULL true,)",
        "ANALYZE (FULL) t", "ANALYZE t (id,)"
    };
    Parser<Dialect::PostgreSQL> parser;
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

namespace {
const AstNode* find_ddl_node(const AstNode* n, NodeType type) {
    if (!n) return nullptr;
    if (n->type == type) return n;
    for (auto* c = n->first_child; c; c = c->next_sibling)
        if (auto* found = find_ddl_node(c, type)) return found;
    return nullptr;
}
}

TEST(PgDdl, ExpressionsAndNamesRemainTraversable) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "CREATE TABLE \"Books\" (\"Price\" numeric(12,2) DEFAULT 2 + 3 CHECK (\"Price\" > 0))";
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(result.ast->type, NodeType::NODE_PG_DDL_STMT);
    auto* name = find_ddl_node(result.ast, NodeType::NODE_IDENTIFIER);
    ASSERT_NE(name, nullptr);
    EXPECT_TRUE(name->flags & FLAG_IDENT_DELIMITED);
    EXPECT_EQ(std::string(name->value().ptr, name->value().len), "Books");
    EXPECT_NE(find_ddl_node(result.ast, NodeType::NODE_BINARY_OP), nullptr);
    auto* type = find_ddl_node(result.ast, NodeType::NODE_TYPE_NAME);
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(std::string(type->value().ptr, type->value().len), "numeric(12,2)");
    EXPECT_NE(find_ddl_node(result.ast, NodeType::NODE_LITERAL_INT), nullptr);
}

TEST(PgDdl, CreateTableAsUsesColumnNameList) {
    Parser<Dialect::PostgreSQL> parser;
    for (const char* sql : {"CREATE TABLE t (id, label) AS SELECT 1, 'x' WITH DATA",
                            "CREATE TABLE t AS VALUES (1), (2)"}) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_TRUE(result.ok()); EXPECT_TRUE(result.full_input);
    }
    const char* invalid = "CREATE TABLE t (id integer) AS SELECT 1";
    auto result = parser.parse(invalid, std::strlen(invalid));
    EXPECT_FALSE(result.ok() && result.full_input);
}

TEST(PgDdl, RejectsInvalidCommandCombinations) {
    Parser<Dialect::PostgreSQL> parser;
    for (const char* sql : {"CREATE TEMP MATERIALIZED VIEW v AS SELECT 1",
            "CREATE TABLE t (id integer GENERATED BY DEFAULT AS (1) STORED)",
            "CREATE TABLE t (id integer) AS SELECT 1",
            "VACUUM (FULL true 1)", "ALTER TABLE t RENAME TO u, ADD id integer",
            "DROP DATABASE a, b", "DROP ROLE alice CASCADE"}) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(PgDdl, PartitionBoundsAndRoleMembershipRoundTrip) {
    const char* queries[] = {
        "CREATE TABLE p_2026 PARTITION OF p FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
        "CREATE TABLE p_default PARTITION OF p DEFAULT",
        "CREATE TABLE p_hash PARTITION OF p FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
        "CREATE TABLE p_list PARTITION OF p FOR VALUES IN (1, 2, NULL)",
        "ALTER TABLE p ATTACH PARTITION p_2026 FOR VALUES FROM (1) TO (100)",
        "ALTER TABLE p DETACH PARTITION p_2026 CONCURRENTLY",
        "GRANT analyst, writer TO alice, bob WITH ADMIN OPTION",
        "GRANT analyst TO alice WITH ADMIN TRUE, INHERIT FALSE, SET TRUE GRANTED BY admin",
        "REVOKE ADMIN OPTION FOR analyst FROM alice CASCADE",
        "REVOKE analyst, writer FROM alice GRANTED BY admin RESTRICT"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
        auto out = emitter.result(); std::string emitted(out.ptr, out.len);
        SCOPED_TRACE(emitted);
        auto again = parser.parse(emitted.data(), emitted.size());
        EXPECT_TRUE(again.ok()); EXPECT_TRUE(again.full_input);
    }
}

TEST(PgDdl, RejectsMissingIndexNamesAndConstraintTriggerKeywords) {
    Parser<Dialect::PostgreSQL> parser;
    for (const char* sql : {
            "CREATE INDEX IF NOT EXISTS ON t (id)",
            "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t FOR ROW EXECUTE FUNCTION audit()",
            "CREATE CONSTRAINT TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION audit()",
            "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t FOR EACH STATEMENT EXECUTE FUNCTION audit()",
            "CREATE TABLE t (id int DEFAULT CHECK(id > 0))",
            "CREATE TABLE t (id int DEFAULT 2 + CHECK(id > 0))"}) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(PgDdl, RejectsCrossProductionOptions) {
    for (const char* sql : {
        "ALTER TABLE t ALTER COLUMN x ADD DEFAULT 1",
        "ALTER TABLE t ALTER COLUMN x ADD NOT NULL",
        "ALTER TABLE t ALTER COLUMN x ADD GENERATED ALWAYS AS (1) STORED",
        "ALTER TABLE t DISABLE ALWAYS TRIGGER tr",
        "ALTER TABLE t DISABLE REPLICA TRIGGER tr",
        "CREATE TABLE t (id int PRIMARY KEY NULLS NOT DISTINCT)",
        "CREATE TABLE t (id int, PRIMARY KEY NULLS DISTINCT (id))",
        "GRANT ALL, SELECT ON t TO a",
        "DROP SCHEMA a.b", "DROP TRIGGER schema.tr ON t", "DROP RULE schema.r ON t",
        "CREATE OR REPLACE CONSTRAINT TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t REFERENCING OLD TABLE old FOR EACH ROW EXECUTE FUNCTION f()"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
    for (const char* sql : {
        "ALTER TABLE t ALTER COLUMN x ADD GENERATED BY DEFAULT AS IDENTITY (START WITH 10)",
        "ALTER TABLE t ENABLE ALWAYS TRIGGER tr", "ALTER TABLE t DISABLE TRIGGER tr",
        "CREATE TABLE t (id int UNIQUE NULLS NOT DISTINCT)",
        "GRANT ALL PRIVILEGES ON t TO a", "DROP SCHEMA s", "DROP TRIGGER tr ON schema.t",
        "CREATE OR REPLACE TRIGGER tr AFTER INSERT ON t REFERENCING NEW TABLE n FOR EACH STATEMENT EXECUTE FUNCTION f()",
        "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql,std::strlen(sql));
        EXPECT_TRUE(result.ok() && result.full_input);
    }
}

TEST(PgDdl, BasicSchemaAndDatabaseCreation) {
    for (const char* sql : {
        "CREATE SCHEMA s", "CREATE SCHEMA IF NOT EXISTS s",
        "CREATE SCHEMA s AUTHORIZATION alice", "CREATE SCHEMA AUTHORIZATION CURRENT_USER",
        "CREATE SCHEMA IF NOT EXISTS AUTHORIZATION CURRENT_ROLE",
        "CREATE SCHEMA IF NOT EXISTS \"New Schema\" AUTHORIZATION \"Owner\"",
        "CREATE SCHEMA s AUTHORIZATION authorization", "CREATE SCHEMA AUTHORIZATION SESSION_USER",
        "CREATE DATABASE app", "CREATE DATABASE \"App DB\""
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql,std::strlen(sql));
        ASSERT_TRUE(result.ok() && result.full_input);
        EXPECT_EQ(result.ast->type,NodeType::NODE_PG_DDL_STMT);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
        auto emitted=emitter.result(); Parser<Dialect::PostgreSQL> again;
        auto roundtrip=again.parse(emitted.ptr,emitted.len);
        EXPECT_TRUE(roundtrip.ok() && roundtrip.full_input);
    }
    for (const char* sql : {
        "CREATE SCHEMA", "CREATE SCHEMA IF EXISTS s", "CREATE SCHEMA IF NOT EXISTS",
        "CREATE SCHEMA s AUTHORIZATION", "CREATE SCHEMA AUTHORIZATION SELECT",
        "CREATE SCHEMA a.b", "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS app",
        "CREATE OR REPLACE SCHEMA s", "CREATE TEMP SCHEMA s", "CREATE UNIQUE DATABASE app"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
    for (const char* sql : {"CREATE SCHEMA s CREATE TABLE t(id int)", "CREATE DATABASE app WITH OWNER alice"}) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result=parser.parse(sql,std::strlen(sql));
        EXPECT_TRUE(result.ok()); EXPECT_FALSE(result.full_input);
    }
}

TEST(PgDdl, SimpleRelationMetadataSurvivesAllocationGuards) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql="CREATE TABLE t(id int)";
    auto result=parser.parse(sql,std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    EXPECT_EQ(std::string(result.table_name.ptr,result.table_name.len),"t");
}
