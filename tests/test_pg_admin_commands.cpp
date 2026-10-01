#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_walk.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>
using namespace sql_parser;

static void admin_roundtrip(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::PostgreSQL> parser; auto result=parser.parse(sql,std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));
    Arena copied; EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(result,copied).error,AstError::UnsupportedRoot);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
    auto text=emitter.result(); Parser<Dialect::PostgreSQL> again;
    auto second=again.parse(text.ptr,text.len); EXPECT_TRUE(second.ok() && second.full_input) << std::string(text.ptr,text.len);
}
TEST(PgAdminCommands, RoleDefinitionsAndMembership) {
    for (const char* sql : {
        "CREATE ROLE r", "CREATE USER alice WITH LOGIN CREATEDB PASSWORD 'secret' VALID UNTIL 'infinity'",
        "CREATE GROUP g WITH USER alice, bob ADMIN carol IN ROLE staff",
        "CREATE ROLE r SYSID 123 INHERIT CONNECTION LIMIT -1", "CREATE ROLE \"Quoted\" ENCRYPTED PASSWORD 'x'",
        "ALTER ROLE CURRENT_USER WITH NOLOGIN NOCREATEDB NOINHERIT REPLICATION BYPASSRLS",
        "ALTER USER alice PASSWORD NULL", "ALTER GROUP g ADD USER alice, bob", "ALTER GROUP g DROP USER alice",
        "ALTER ROLE r RENAME TO other", "DROP ROLE IF EXISTS alice, bob", "DROP GROUP g",
        "ALTER ROLE ALL IN DATABASE db SET search_path TO public, pg_temp",
        "ALTER USER r RESET ALL", "ALTER ROLE r SET TIME ZONE 'UTC'", "ALTER ROLE r SET role TO 'x'"
    }) admin_roundtrip(sql);
}
TEST(PgAdminCommands, ObjectCommentsAndSecurityLabels) {
    for (const char* sql : {
        "COMMENT ON TABLE public.t IS 'table comment'", "COMMENT ON COLUMN s.t.c IS NULL",
        "COMMENT ON TYPE int[] IS 'type'", "COMMENT ON DOMAIN amount IS 'domain'",
        "COMMENT ON FUNCTION f(int, text) IS 'function'", "COMMENT ON PROCEDURE p(IN x int) IS 'proc'",
        "COMMENT ON ROUTINE f IS NULL", "COMMENT ON AGGREGATE agg(*) IS 'agg'",
        "COMMENT ON OPERATOR public.=== (int, int) IS 'op'",
        "COMMENT ON OPERATOR CLASS s.ops USING btree IS 'class'",
        "COMMENT ON OPERATOR FAMILY ops USING gist IS NULL",
        "COMMENT ON CONSTRAINT c ON DOMAIN d IS 'constraint'", "COMMENT ON TRIGGER tr ON s.t IS 'trigger'",
        "COMMENT ON CAST (int AS text) IS 'cast'", "COMMENT ON TRANSFORM FOR int LANGUAGE sql IS 'transform'",
        "COMMENT ON LARGE OBJECT 123 IS 'lob'", "COMMENT ON TEXT SEARCH DICTIONARY s.dict IS 'dictionary'",
        "COMMENT ON FOREIGN DATA WRAPPER fdw IS NULL", "COMMENT ON DATABASE db IS 'database'",
        "SECURITY LABEL FOR provider ON TABLE t IS 'label'", "SECURITY LABEL ON COLUMN t.c IS NULL",
        "SECURITY LABEL FOR 'provider' ON FUNCTION f(int) IS 'label'", "SECURITY LABEL ON LARGE OBJECT 123 IS 'label'"
    }) admin_roundtrip(sql);
}
TEST(PgAdminCommands, RejectsMalformedProductions) {
    for (const char* sql : {
        "CREATE ROLE", "CREATE ROLE r UNKNOWN_OPTION",
        "CREATE ROLE r 'login'", "CREATE ROLE r CONNECTION LIMIT 2147483648", "CREATE ROLE r SYSID 2147483648", "CREATE ROLE r PASSWORD 1",
        "CREATE ROLE r CONNECTION LIMIT x", "CREATE ROLE r SYSID -1", "CREATE ROLE r USER",
        "CREATE ROLE public", "CREATE ROLE none", "CREATE ROLE CURRENT_USER", "CREATE ROLE r UNENCRYPTED PASSWORD 'x'",
        "ALTER GROUP g LOGIN", "ALTER ROLE r IN ROLE g", "ALTER USER r ADMIN g", "DROP ROLE r CASCADE",
        "ALTER ROLE ALL LOGIN", "ALTER ROLE r IN DATABASE db LOGIN", "ALTER ROLE r SET LOCAL x TO 1",
        "COMMENT TABLE t IS 'x'", "COMMENT ON DATABASE s.db IS 'x'", "COMMENT ON TABLE t IS 1",
        "COMMENT ON FUNCTION f(int,) IS 'x'", "COMMENT ON TRIGGER tr IS 'x'",
        "COMMENT ON OPERATOR +(int) IS 'x'", "COMMENT ON CAST(int text) IS 'x'",
        "SECURITY LABEL ON OPERATOR +(int,int) IS 'x'", "SECURITY LABEL FOR ON TABLE t IS 'x'",
        "COMMENT ON TABLE t IS", "COMMENT ON SOMETHING t IS 'x'"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser; auto r=parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input);
    }
}
TEST(PgAdminCommands, RoleOptionsRespectKeywordAndIntegerGrammar) {
    for (const char* sql : {
        "CREATE ROLE r INHERIT", "CREATE ROLE r \"login\"", "CREATE ROLE r CONNECTION LIMIT 2147483647",
        "CREATE ROLE r CONNECTION LIMIT -2147483647", "CREATE ROLE r CONNECTION LIMIT 0x7fffffff",
        "CREATE ROLE r SYSID 0b101", "ALTER ROLE r CONNECTION LIMIT +0o177", "CREATE ROLE r SYSID 2_147_483_647"
    }) admin_roundtrip(sql);
    for (const char* sql : {
        "CREATE ROLE r \"inherit\"", "CREATE ROLE r 'login'", "CREATE ROLE r CONNECTION LIMIT 2147483648",
        "CREATE ROLE r CONNECTION LIMIT -2147483648", "CREATE ROLE r SYSID 2147483648",
        "CREATE ROLE r SYSID 0x80000000"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser; auto r=parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input);
    }
}
TEST(PgAdminCommands, RenameRequiresConcreteSourceRole) {
    for (const char* sql : {
        "ALTER ROLE r RENAME TO other", "ALTER GROUP \"CURRENT_USER\" RENAME TO other",
        "ALTER USER \"PUBLIC\" RENAME TO other"
    }) admin_roundtrip(sql);
    for (const char* sql : {
        "ALTER ROLE CURRENT_USER RENAME TO r", "ALTER USER SESSION_USER RENAME TO r",
        "ALTER GROUP CURRENT_ROLE RENAME TO r", "ALTER ROLE public RENAME TO r",
        "ALTER USER \"public\" RENAME TO r"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser; auto r=parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input);
    }
}
TEST(PgAdminCommands, LargeObjectIdentityRequiresNumericOnly) {
    for (const char* sql : {
        "COMMENT ON LARGE OBJECT +1 IS 'x'", "COMMENT ON LARGE OBJECT -1.5 IS 'x'",
        "SECURITY LABEL ON LARGE OBJECT 2147483648 IS 'x'"
    }) admin_roundtrip(sql);
    for (const char* sql : {
        "COMMENT ON LARGE OBJECT ~1 IS 'x'", "SECURITY LABEL ON LARGE OBJECT @1 IS 'x'",
        "COMMENT ON LARGE OBJECT (1) IS 'x'", "COMMENT ON LARGE OBJECT - -1 IS 'x'"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser; auto r=parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input);
    }
}

TEST(PgAdminCommands, StructuredOperandsAndArenaFailures) {
    for (const char* sql : {
        "CREATE ROLE \"Owner\" LOGIN CONNECTION LIMIT -1 PASSWORD 'x' IN ROLE a, b",
        "ALTER ROLE r IN DATABASE d SET search_path TO public, pg_temp",
        "COMMENT ON FUNCTION f(int, text) IS 'comment'",
        "COMMENT ON CAST(int AS text) IS 'cast'",
        "SECURITY LABEL FOR provider ON TABLE \"Schema\".\"Table\" IS 'label'"
    }) {
        SCOPED_TRACE(sql);
        auto count=[](const AstNode* ast) {
            size_t n=0; walk_ast(ast,[&](const AstNode&,const AstVisitContext&) {++n;return AstVisitAction::Continue;}); return n;
        };
        Parser<Dialect::PostgreSQL> baseline; auto valid=baseline.parse(sql,std::strlen(sql));
        ASSERT_TRUE(valid.ok() && valid.full_input); auto expected=count(valid.ast);
        bool identity=false, literal=false;
        walk_ast(valid.ast,[&](const AstNode& node,const AstVisitContext&) {
            identity |= node.type == NodeType::NODE_IDENTIFIER || node.type == NodeType::NODE_TYPE_NAME;
            literal |= node.type == NodeType::NODE_LITERAL_STRING;
            return AstVisitAction::Continue;
        });
        EXPECT_TRUE(identity);
        if (std::strstr(sql,"SET search_path") == nullptr) EXPECT_TRUE(literal);
        for (size_t capacity=48; capacity<=1920; capacity+=48) {
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size=capacity; config.arena_max_size=capacity;
            Parser<Dialect::PostgreSQL> parser(config); auto result=parser.parse(sql,std::strlen(sql));
            if (result.ok() && result.full_input) EXPECT_EQ(count(result.ast),expected);
        }
    }
}
