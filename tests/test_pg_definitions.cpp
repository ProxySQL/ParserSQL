#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>
using namespace sql_parser;

static const char* const definition_sql[] = {
    "CREATE AGGREGATE a(VARIADIC \"decimal\".t[] ORDER BY VARIADIC \"decimal\".t[]) (sfunc=f)",
    "CREATE AGGREGATE a(int) (STYPE = \"decimal\".t)",
    "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS OPERATOR 0x10 <",
    "CREATE AGGREGATE a(VARIADIC numeric[] ORDER BY VARIADIC decimal[]) (sfunc=f,stype=int)",
    "CREATE AGGREGATE a(VARIADIC varchar[] ORDER BY VARIADIC character varying[]) (sfunc=f,stype=int)",
    "CREATE AGGREGATE varying(VARIADIC int[] ORDER BY VARIADIC INT []) (SFUNC=f)",
    "CREATE AGGREGATE varying(VARIADIC int[] ORDER BY VARIADIC integer[]) (SFUNC=f)",
    "CREATE AGGREGATE a(t.c%TYPE) (SFUNC=f)",
    "CREATE AGGREGATE a(x t.c%TYPE) (SFUNC=f)",
    "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 1 f(x OUT int, y INOUT int, z IN int)",
    "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 1 f(x VARIADIC int[])",
    "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 1 f(x SETOF int)",
    "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 1 f(join int, t.c%TYPE)",
    "CREATE AGGREGATE total(int) (SFUNC = int4pl, STYPE = int, INITCOND = '0')",
    "CREATE OR REPLACE AGGREGATE public.total(IN value int) (SFUNC = public.add, STYPE = int)",
    "CREATE AGGREGATE count_all(*) (SFUNC = count_step, STYPE = bigint)",
    "CREATE AGGREGATE percentile(float8 ORDER BY value int) (SFUNC = step, STYPE = internal, FINALFUNC_EXTRA, HYPOTHETICAL)",
    "CREATE AGGREGATE ordered(ORDER BY int, text) (SFUNC = step, STYPE = internal)",
    "CREATE AGGREGATE old_sum (basetype = int, sfunc = int4pl, stype = int, initcond = '0')",
    "CREATE AGGREGATE public.\"Sum\"(x IN int, VARIADIC y text[]) (SFUNC = step, STYPE = internal, SORTOP = OPERATOR(public.<))",
    "CREATE AGGREGATE varying(VARIADIC int[] ORDER BY VARIADIC int[]) (SFUNC = f)",
    "CREATE OPERATOR public.=== (FUNCTION = public.eq, LEFTARG = int, RIGHTARG = int, COMMUTATOR = OPERATOR(public.===), NEGATOR = <>, HASHES, MERGES)",
    "CREATE OPERATOR - (LEFTARG = hstore, RIGHTARG = text, PROCEDURE = delete)",
    "CREATE OPERATOR + (LEFTARG = _int4, RIGHTARG = _int4, COMMUTATOR = +, PROCEDURE = intarray_push_array)",
    "CREATE AGGREGATE balk(int4) (SFUNC = balkifnull(int8, int4), STYPE = int8, PARALLEL = SAFE, INITCOND = '0')",
    "CREATE OPERATOR ! (PROCEDURE = factorial, RIGHTARG = bigint)",
    "CREATE COLLATION public.c (PROVIDER = builtin, LOCALE = \"C\")",
    "CREATE COLLATION IF NOT EXISTS public.c FROM pg_catalog.\"C\"",
    "CREATE TEXT SEARCH DICTIONARY public.d (TEMPLATE = simple, STOPWORDS = english)",
    "CREATE TEXT SEARCH CONFIGURATION public.cfg (COPY = pg_catalog.english)",
    "CREATE TEXT SEARCH PARSER public.p (START = pstart, GETTOKEN = ptoken, END = pend, LEXTYPES = plextypes)",
    "CREATE TEXT SEARCH TEMPLATE public.t (INIT = tinit, LEXIZE = tlexize)",
    "CREATE OPERATOR FAMILY public.fam USING btree",
    "CREATE OPERATOR CLASS public.cls DEFAULT FOR TYPE int USING btree FAMILY public.fam AS OPERATOR 1 <, OPERATOR 3 = (int, int) FOR SEARCH, FUNCTION 1 cmp(int,int), STORAGE int",
    "CREATE OPERATOR CLASS cls FOR TYPE point USING gist AS OPERATOR 1 <-> (point, point) FOR ORDER BY float_ops, FUNCTION 1 (point, point) distance(point, point)",
    "CREATE OPERATOR CLASS cls FOR TYPE int USING btree AS FUNCTION 1 compare",
    "ALTER OPERATOR FAMILY public.fam USING btree ADD OPERATOR 1 < (int,int), FUNCTION 1 (int,int) cmp(int,int), STORAGE int",
    "ALTER OPERATOR FAMILY fam USING btree DROP OPERATOR 1 (int,int), FUNCTION 2 (int)",
    "ALTER OPERATOR public.=== (int,int) SET (RESTRICT = eqsel, JOIN = NONE)",
    "ALTER OPERATOR ! (NONE, int) OWNER TO CURRENT_USER",
    "ALTER OPERATOR ! (int, NONE) SET SCHEMA private",
    "ALTER OPERATOR CLASS cls USING btree RENAME TO other",
    "ALTER OPERATOR FAMILY fam USING btree OWNER TO alice",
    "ALTER OPERATOR FAMILY fam USING btree SET SCHEMA private",
    "ALTER AGGREGATE total(int) RENAME TO new_total",
    "ALTER AGGREGATE total(ORDER BY int) OWNER TO alice",
    "DROP AGGREGATE IF EXISTS public.total(int), count_all(*) CASCADE",
    "DROP OPERATOR IF EXISTS public.=== (int,int), ! (NONE,int) RESTRICT",
    "DROP OPERATOR CLASS IF EXISTS public.cls USING btree CASCADE",
    "DROP OPERATOR FAMILY public.fam USING btree"
};

TEST(PgDefinitions, NativeStructuredRoundTrips) {
    for (const char* sql : definition_sql) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(r.ok() && r.full_input);
        ASSERT_NE(r.ast, nullptr);
        EXPECT_EQ(r.ast->type, NodeType::NODE_PG_DDL_STMT);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(r.ast);
        auto output = emitter.result();
        Parser<Dialect::PostgreSQL> again;
        auto second = again.parse(output.ptr, output.len);
        EXPECT_TRUE(second.ok() && second.full_input) << std::string(output.ptr, output.len);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast));
    }
}

TEST(PgDefinitions, RejectsMalformedBoundaries) {
    for (const char* sql : {
        "CREATE AGGREGATE a(int) (STYPE = decimal.t)",
        "CREATE AGGREGATE a(VARIADIC decimal.t[] ORDER BY VARIADIC numeric.t[]) (sfunc=f)",
        "CREATE AGGREGATE a(VARIADIC char.t[] ORDER BY VARIADIC character.t[]) (sfunc=f)",
        "CREATE AGGREGATE a(VARIADIC decimal.t[] ORDER BY VARIADIC decimal.t[]) (sfunc=f)",
        "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS OPERATOR 2147483648 <",
        "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 0x80000000 cmp",
        "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 1 int(int)",
        "CREATE AGGREGATE a(int int) (SFUNC=f)",
        "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 1 f(int int)",
        "CREATE AGGREGATE a(VARIADIC \"Foo\"[] ORDER BY VARIADIC \"foo\"[]) (SFUNC=f)",
        "CREATE AGGREGATE a(VARIADIC \"Foo\"[] ORDER BY VARIADIC Foo[]) (SFUNC=f)",
        "CREATE AGGREGATE a(VARIADIC int[] ORDER BY VARIADIC \"int\"[]) (SFUNC=f)",
        "CREATE AGGREGATE a(VARIADIC SETOF int[] ORDER BY VARIADIC int[]) (SFUNC=f)",
        "CREATE AGGREGATE a(int=int)", "CREATE AGGREGATE a(language=int)", "CREATE AGGREGATE a(aggregate=int)",
        "CREATE COLLATION c ()", "CREATE COLLATION c FROM", "CREATE OR REPLACE COLLATION c FROM d",
        "CREATE COLLATION IF EXISTS c FROM d", "CREATE TEXT SEARCH DICTIONARY d ()",
        "CREATE TEXT SEARCH TEMPLATE t (LEXIZE =)", "CREATE TEXT SEARCH unknown x (COPY = y)",
        "CREATE OR REPLACE TEXT SEARCH CONFIGURATION c (COPY = d)",
        "CREATE AGGREGATE a() (SFUNC=f)", "CREATE AGGREGATE a(int DEFAULT 1) (SFUNC=f)",
        "CREATE AGGREGATE a(OUT int) (SFUNC=f)", "CREATE AGGREGATE a(x INOUT int) (SFUNC=f)",
        "CREATE AGGREGATE a(int, ORDER BY int) (SFUNC=f)", "CREATE AGGREGATE a(ORDER BY) (SFUNC=f)",
        "CREATE AGGREGATE a(int) ()", "CREATE AGGREGATE a(int) (SFUNC=)",
        "CREATE AGGREGATE a(basetype=int, sfunc=f,)", "CREATE AGGREGATE a(basetype)",
        "CREATE AGGREGATE a(VARIADIC int[] ORDER BY text) (SFUNC=f)",
        "CREATE AGGREGATE a(VARIADIC int[] ORDER BY VARIADIC text[]) (SFUNC=f)",
        "CREATE OPERATOR equals (FUNCTION=f)", "CREATE OR REPLACE OPERATOR = (FUNCTION=f)",
        "CREATE OPERATOR s. (FUNCTION=f)", "CREATE OPERATOR = (FUNCTION=f,)",
        "CREATE OPERATOR = (FUNCTION=f nonsense)", "CREATE TEMP OPERATOR FAMILY f USING btree",
        "CREATE OPERATOR FAMILY f USING", "CREATE OPERATOR FAMILY f USING btree AS",
        "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS", "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS OPERATOR -1 <",
        "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS OPERATOR 1 < (int)",
        "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS FUNCTION 1 () f(int)",
        "ALTER OPERATOR FAMILY f USING btree ADD", "ALTER OPERATOR FAMILY f USING btree DROP STORAGE int",
        "ALTER OPERATOR FAMILY f USING btree DROP OPERATOR 1 ()", "ALTER OPERATOR FAMILY f USING btree DROP OPERATOR 1 (int),",
        "ALTER OPERATOR = (NONE,NONE) SET (RESTRICT=NONE)", "ALTER OPERATOR = (int,int,int) SET (RESTRICT=NONE)",
        "ALTER OPERATOR = (int,int) RENAME TO eq", "ALTER OPERATOR = (int,int) SET ()",
        "DROP AGGREGATE a", "DROP AGGREGATE a()", "DROP OPERATOR =", "DROP OPERATOR = (int)",
        "DROP OPERATOR CLASS c, d USING btree", "DROP OPERATOR FAMILY f USING btree, g USING hash",
        "CREATE OPERATOR = (FUNCTION=f); nonsense"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql)); EXPECT_FALSE(r.ok() && r.full_input);
    }
}

TEST(PgDefinitions, RetainsNamesTypesAndLiterals) {
    const char* sql = "CREATE AGGREGATE public.\"Total\"(int) (SFUNC = public.step, STYPE = int, INITCOND = '0')";
    Parser<Dialect::PostgreSQL> parser; auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.ok() && r.full_input);
    bool name = false, type = false; AstNode* literal = nullptr;
    walk_ast(r.ast, [&](const AstNode& n, const AstVisitContext&) {
        if (n.type == NodeType::NODE_IDENTIFIER && n.value() == StringRef{"Total",5}) name = (n.flags & FLAG_IDENT_DELIMITED) != 0;
        if (n.type == NodeType::NODE_TYPE_NAME && n.value() == StringRef{"int",3}) type = true;
        if (n.type == NodeType::NODE_LITERAL_STRING) literal = const_cast<AstNode*>(&n);
        return AstVisitAction::Continue;
    });
    EXPECT_TRUE(name); EXPECT_TRUE(type); ASSERT_NE(literal, nullptr);
    literal->set_value({"42",2}); literal->set_source({});
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(r.ast); auto output = emitter.result();
    EXPECT_NE(std::string(output.ptr,output.len).find("'42'"), std::string::npos);
    Arena arena; EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(r,arena).error, AstError::UnsupportedRoot);
}

TEST(PgDefinitions, ExhaustionNeverOmitsDefinitionParts) {
    for (const char* sql : definition_sql) {
        auto count = [](const AstNode* ast) { size_t n=0; walk_ast(ast,[&](const AstNode&,const AstVisitContext&) { ++n; return AstVisitAction::Continue; }); return n; };
        Parser<Dialect::PostgreSQL> baseline; auto valid=baseline.parse(sql,std::strlen(sql));
        ASSERT_TRUE(valid.ok() && valid.full_input); size_t expected=count(valid.ast);
        for (size_t capacity=48;capacity<=2400;capacity+=48) {
            SCOPED_TRACE(sql);
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size=capacity; config.arena_max_size=capacity;
            Parser<Dialect::PostgreSQL> parser(config); auto r=parser.parse(sql,std::strlen(sql));
            if (r.ok() && r.full_input) EXPECT_EQ(count(r.ast),expected);
        }
    }
}
