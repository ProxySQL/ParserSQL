#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>
using namespace sql_parser;

static void object_ddl_roundtrip(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::PostgreSQL> parser;
    auto r=parser.parse(sql,std::strlen(sql));
    ASSERT_TRUE(r.ok() && r.full_input);
    ASSERT_NE(r.ast,nullptr);
    EXPECT_EQ(r.ast->type,NodeType::NODE_PG_DDL_STMT);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(r.ast);
    auto output=emitter.result();
    Parser<Dialect::PostgreSQL> again;
    auto second=again.parse(output.ptr,output.len);
    EXPECT_TRUE(second.ok() && second.full_input) << std::string(output.ptr,output.len);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast));
}

TEST(PgObjectDdl, RoutinePropertiesAndIdentity) {
    for (const char* sql : {
        "ALTER FUNCTION f(int) PARALLEL SAFE",
        "ALTER FUNCTION public.f(IN x int, VARIADIC y text[]) IMMUTABLE STRICT COST 12.5 ROWS 100 RESTRICT",
        "ALTER FUNCTION f CALLED ON NULL INPUT EXTERNAL SECURITY DEFINER NOT LEAKPROOF",
        "ALTER FUNCTION f() RETURNS NULL ON NULL INPUT SUPPORT public.support_fn",
        "ALTER PROCEDURE p(int) SECURITY INVOKER SET search_path TO public, pg_temp RESET work_mem",
        "ALTER ROUTINE f(int) SET work_mem = '1MB' SET enable_seqscan TO off RESET ALL",
        "ALTER FUNCTION f() SET search_path FROM CURRENT SET work_mem TO DEFAULT",
        "ALTER FUNCTION f() SET TIME ZONE 'UTC' RESET TIME ZONE",
        "ALTER FUNCTION f() SET role TO 'x' SET names TO 'UTF8' SET time TO 'x' SET session TO 'x' SET xml TO 'x' RESET time",
        "ALTER FUNCTION f() SET TIME ZONE INTERVAL '1' MINUTE",
        "ALTER FUNCTION f() SET search_path TO e SET search_path TO E'public'",
        "ALTER FUNCTION f() SET schema TO 'public' SET transaction TO 'x' RESET session RESET transaction",
        "ALTER FUNCTION f() SET SCHEMA E'public' SET schema.path TO public",
        "ALTER FUNCTION f() SET schema.path TO public",
        "ALTER FUNCTION f() SET SCHEMA 'public' SET NAMES 'UTF8' RESET SESSION AUTHORIZATION",
        "ALTER FUNCTION f(int) RENAME TO new_f",
        "ALTER PROCEDURE p OWNER TO CURRENT_USER",
        "ALTER ROUTINE public.f(int) SET SCHEMA private",
        "ALTER FUNCTION f(int) DEPENDS ON EXTENSION ext",
        "ALTER PROCEDURE p() NO DEPENDS ON EXTENSION ext"
    }) object_ddl_roundtrip(sql);
}

TEST(PgObjectDdl, DomainProductions) {
    for (const char* sql : {
        "CREATE DOMAIN positive AS int DEFAULT 1 NOT NULL CHECK (VALUE > 0)",
        "CREATE DOMAIN public.label text COLLATE \"C\" CONSTRAINT nonempty CHECK (VALUE <> '')",
        "CREATE DOMAIN d AS numeric(10,2) DEFAULT 2 + 3",
        "CREATE DOMAIN d AS int CHECK (VALUE > 0) ENFORCED",
        "CREATE DOMAIN d AS int CONSTRAINT c CHECK (VALUE > 0) NOT ENFORCED",
        "CREATE DOMAIN d AS int NOT NULL NO INHERIT",
        "CREATE DOMAIN d AS boolean DEFAULT 1 < 2", "CREATE DOMAIN d AS boolean DEFAULT (NOT false)",
        "ALTER DOMAIN positive SET DEFAULT 2 * 3",
        "ALTER DOMAIN positive DROP DEFAULT",
        "ALTER DOMAIN positive SET NOT NULL", "ALTER DOMAIN positive DROP NOT NULL",
        "ALTER DOMAIN positive ADD CONSTRAINT valid CHECK (VALUE > 1) NOT VALID",
        "ALTER DOMAIN positive ADD NOT NULL",
        "ALTER DOMAIN positive DROP CONSTRAINT IF EXISTS valid CASCADE",
        "ALTER DOMAIN positive VALIDATE CONSTRAINT valid",
        "ALTER DOMAIN positive RENAME CONSTRAINT valid TO checked",
        "ALTER DOMAIN positive RENAME TO positive_int",
        "ALTER DOMAIN positive OWNER TO alice", "ALTER DOMAIN positive SET SCHEMA private"
    }) object_ddl_roundtrip(sql);
}

TEST(PgObjectDdl, TypeProductions) {
    for (const char* sql : {
        "CREATE TYPE shell", "CREATE TYPE t (INPUT = e, ALIGNMENT = double)",
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", "CREATE TYPE empty_enum AS ENUM ()",
        "CREATE TYPE public.pair AS (x int, label text COLLATE \"C\")", "CREATE TYPE empty_row AS ()",
        "CREATE TYPE intrange AS RANGE (subtype = int4, subtype_diff = int4range_subdiff)",
        "CREATE TYPE public.base (INPUT = public.base_in, OUTPUT = base_out, INTERNALLENGTH = VARIABLE, ALIGNMENT = int4, STORAGE = extended, PASSEDBYVALUE)",
        "ALTER TYPE mood ADD VALUE IF NOT EXISTS 'great' AFTER 'happy'",
        "ALTER TYPE mood ADD VALUE 'bad' BEFORE 'sad'",
        "ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'",
        "ALTER TYPE public.pair ADD ATTRIBUTE z numeric(8,2) CASCADE, DROP ATTRIBUTE IF EXISTS label RESTRICT",
        "ALTER TYPE public.pair ALTER ATTRIBUTE x SET DATA TYPE bigint COLLATE \"C\" CASCADE",
        "ALTER TYPE public.pair RENAME ATTRIBUTE x TO xx CASCADE",
        "ALTER TYPE public.pair SET (RECEIVE = public.receive_fn, SEND = NONE)",
        "ALTER TYPE public.pair RENAME TO pair2", "ALTER TYPE public.pair OWNER TO alice",
        "ALTER TYPE public.pair SET SCHEMA private"
    }) object_ddl_roundtrip(sql);
}

TEST(PgObjectDdl, SequenceProductions) {
    for (const char* sql : {
        "CREATE SEQUENCE s", "CREATE TEMP SEQUENCE IF NOT EXISTS s AS bigint START WITH 3 INCREMENT BY -2 MINVALUE -10 MAXVALUE 100 CACHE 5 NO CYCLE OWNED BY t.id",
        "CREATE UNLOGGED SEQUENCE s AS int START 1 NO MINVALUE NO MAXVALUE CYCLE",
        "CREATE SEQUENCE s OWNED BY NONE", "CREATE SEQUENCE s SEQUENCE NAME public.other",
        "ALTER SEQUENCE s RESTART WITH 4", "ALTER SEQUENCE IF EXISTS public.s RESTART",
        "ALTER SEQUENCE s AS smallint INCREMENT 2 START 10 CACHE 20 NO CYCLE",
        "ALTER SEQUENCE s LOGGED", "ALTER SEQUENCE s UNLOGGED",
        "ALTER SEQUENCE s SET LOGGED", "ALTER SEQUENCE IF EXISTS s SET UNLOGGED",
        "ALTER SEQUENCE IF EXISTS s RENAME TO other",
        "ALTER SEQUENCE s OWNER TO SESSION_USER", "ALTER SEQUENCE s SET SCHEMA private"
    }) object_ddl_roundtrip(sql);
}

TEST(PgObjectDdl, OtherObjectIdentityAndDrops) {
    for (const char* sql : {
        "ALTER SCHEMA s RENAME TO other", "ALTER SCHEMA s OWNER TO alice",
        "ALTER DATABASE db RENAME TO other", "ALTER DATABASE db OWNER TO alice",
        "ALTER VIEW IF EXISTS public.v RENAME TO v2", "ALTER VIEW v RENAME COLUMN a TO b",
        "ALTER VIEW v OWNER TO alice", "ALTER MATERIALIZED VIEW IF EXISTS v SET SCHEMA private",
        "ALTER INDEX IF EXISTS idx RENAME TO other", "ALTER INDEX idx OWNER TO alice",
        "ALTER COLLATION public.c OWNER TO alice", "ALTER CONVERSION public.c SET SCHEMA private",
        "ALTER STATISTICS public.st RENAME TO other",
        "DROP COLLATION IF EXISTS public.c, d CASCADE", "DROP CONVERSION public.c",
        "DROP STATISTICS IF EXISTS public.st", "DROP FOREIGN TABLE IF EXISTS public.t CASCADE",
        "DROP TEXT SEARCH CONFIGURATION public.c", "DROP TEXT SEARCH DICTIONARY public.d",
        "DROP TEXT SEARCH PARSER public.p", "DROP TEXT SEARCH TEMPLATE public.t",
        "DROP ACCESS METHOD am", "DROP EVENT TRIGGER IF EXISTS ev", "DROP EXTENSION ext CASCADE",
        "DROP FOREIGN DATA WRAPPER fdw", "DROP SERVER srv CASCADE", "DROP PROCEDURAL LANGUAGE pl",
        "DROP PUBLICATION IF EXISTS pub", "DROP POLICY IF EXISTS pol ON public.t CASCADE",
        "DROP TYPE IF EXISTS int[], public.pair", "DROP DOMAIN IF EXISTS public.d"
    }) object_ddl_roundtrip(sql);
}

TEST(PgObjectDdl, RejectsMalformedAndCrossProductionSyntax) {
    for (const char* sql : {
        "ALTER FUNCTION f()", "ALTER FUNCTION f() LANGUAGE sql", "ALTER FUNCTION f() AS 'body'",
        "ALTER FUNCTION f() WINDOW", "ALTER FUNCTION f() PARALLEL", "ALTER FUNCTION f() COST 1 + 2",
        "ALTER FUNCTION f(int DEFAULT 1) STRICT", "ALTER FUNCTION IF EXISTS f() STRICT",
        "ALTER FUNCTION f() SET search_path public", "ALTER FUNCTION f() SET search_path TO public,",
        "ALTER FUNCTION f() SET TIME ZONE INTERVAL '1' SECOND",
        "ALTER FUNCTION f() SET search_path TO E 'public'",
        "CREATE TYPE t (INPUT = E 'input')", "CREATE TYPE t (ALIGNMENT = double nonsense)",
        "ALTER SEQUENCE s SET LOGGED CACHE 2", "ALTER SEQUENCE s SET TEMPORARY",
        "CREATE DOMAIN d AS int CHECK (VALUE > 0) ENFORCED nonsense",
        "CREATE DOMAIN d AS int NOT NULL NO",
        "ALTER FUNCTION f() RESET", "ALTER FUNCTION f() IMMUTABLE CASCADE",
        "ALTER FUNCTION f() RENAME TO g STRICT", "ALTER FUNCTION f() SET SCHEMA private STRICT",
        "CREATE DOMAIN d AS int DEFAULT NOT NULL CHECK (VALUE > 0)",
        "CREATE DOMAIN d AS boolean DEFAULT a = ANY(ARRAY[1])", "CREATE DOMAIN d AS int CHECK (*)",
        "CREATE DOMAIN d AS", "CREATE DOMAIN d AS int CHECK ()", "CREATE OR REPLACE DOMAIN d AS int",
        "ALTER DOMAIN d ADD DEFAULT 1", "ALTER DOMAIN d ADD UNIQUE", "ALTER DOMAIN d SET DEFAULT",
        "ALTER DOMAIN d ADD CHECK (VALUE > 0) DEFERRABLE", "ALTER DOMAIN d ADD NOT NULL NOT VALID",
        "ALTER DOMAIN d DROP CONSTRAINT", "ALTER DOMAIN d VALIDATE CONSTRAINT",
        "CREATE TYPE t AS ENUM ('a',)", "CREATE TYPE t AS ENUM (1)", "CREATE TYPE t AS (x)",
        "CREATE TYPE t AS (x int DEFAULT 1)", "CREATE TYPE t AS RANGE ()",
        "CREATE TYPE t (INPUT =)", "CREATE TYPE t (INPUT = f,)", "CREATE OR REPLACE TYPE t",
        "ALTER TYPE t ADD VALUE 1", "ALTER TYPE t DROP VALUE 'a'", "ALTER TYPE t RENAME VALUE 'a' TO",
        "ALTER TYPE t ADD ATTRIBUTE x int DEFAULT 1", "ALTER TYPE t ALTER ATTRIBUTE x TYPE",
        "ALTER TYPE t SET ()", "ALTER TYPE t ADD VALUE 'a', ADD VALUE 'b'",
        "CREATE SEQUENCE s START", "CREATE SEQUENCE s START WITH", "CREATE SEQUENCE s CACHE x",
        "CREATE SEQUENCE s INCREMENT 1 + 2", "CREATE SEQUENCE s AS int[]", "CREATE SEQUENCE s RESTART WITH",
        "CREATE OR REPLACE SEQUENCE s", "ALTER SEQUENCE s", "ALTER SEQUENCE s RESTART WITH 1, CACHE 2",
        "ALTER SCHEMA s SET SCHEMA other", "ALTER DATABASE db SET SCHEMA other",
        "DROP EXTENSION public.ext", "DROP TEXT SEARCH UNKNOWN x", "DROP POLICY p", "DROP ACCESS METHOD"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto r=parser.parse(sql,std::strlen(sql)); EXPECT_FALSE(r.ok() && r.full_input);
    }
}

TEST(PgObjectDdl, NamesTypesAndDefaultsRemainStructured) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql="CREATE DOMAIN \"Amount\" AS numeric(12,2) DEFAULT 2 + 3 CHECK (VALUE > 0)";
    auto r=parser.parse(sql,std::strlen(sql)); ASSERT_TRUE(r.ok() && r.full_input);
    AstNode* literal=nullptr; bool found_name=false,found_type=false;
    walk_ast(r.ast,[&](const AstNode& n,const AstVisitContext&) {
        if (n.type==NodeType::NODE_LITERAL_INT && n.value()==StringRef{"2",1}) literal=const_cast<AstNode*>(&n);
        if (n.type==NodeType::NODE_IDENTIFIER && n.value()==StringRef{"Amount",6}) found_name=(n.flags&FLAG_IDENT_DELIMITED)!=0;
        if (n.type==NodeType::NODE_TYPE_NAME && n.value()==StringRef{"numeric(12,2)",13}) found_type=true;
        return AstVisitAction::Continue;
    });
    EXPECT_TRUE(found_name); EXPECT_TRUE(found_type); ASSERT_NE(literal,nullptr);
    literal->set_value(StringRef{"7",1}); literal->set_source({});
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(r.ast); auto text=emitter.result();
    EXPECT_NE(std::string(text.ptr,text.len).find("7 + 3"),std::string::npos);
    Arena output; EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(r,output).error,AstError::UnsupportedRoot);
}

TEST(PgObjectDdl, ExhaustedArenasCannotDropObjectSyntax) {
    for (const char* sql : {
        "ALTER FUNCTION f(int) PARALLEL SAFE SET search_path TO public, pg_temp RESET ALL",
        "CREATE DOMAIN d AS int DEFAULT 1 CHECK (VALUE > 0)",
        "ALTER DOMAIN d ADD CONSTRAINT c CHECK (VALUE > 0) NOT VALID",
        "CREATE TYPE pair AS (x int, y text)", "CREATE TYPE mood AS ENUM ('a','b')",
        "CREATE TYPE t (INPUT = public.i, OUTPUT = public.o, INTERNALLENGTH = 4)",
        "ALTER TYPE pair ADD ATTRIBUTE z int CASCADE, DROP ATTRIBUTE x",
        "CREATE SEQUENCE s START 1 INCREMENT -2 OWNED BY t.id", "ALTER SEQUENCE s RESTART WITH 5",
        "DROP TEXT SEARCH CONFIGURATION public.cfg CASCADE"
    }) {
        auto count=[](const AstNode* ast) { size_t total=0; walk_ast(ast,[&](const AstNode&,const AstVisitContext&) {++total;return AstVisitAction::Continue;}); return total; };
        Parser<Dialect::PostgreSQL> baseline; auto valid=baseline.parse(sql,std::strlen(sql));
        ASSERT_TRUE(valid.ok() && valid.full_input); const size_t expected=count(valid.ast);
        for (size_t capacity=48;capacity<=1920;capacity+=48) {
            SCOPED_TRACE(sql);
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size=capacity; config.arena_max_size=capacity;
            Parser<Dialect::PostgreSQL> parser(config); auto r=parser.parse(sql,std::strlen(sql));
            if (r.ok() && r.full_input) EXPECT_EQ(count(r.ast),expected);
        }
    }
}
