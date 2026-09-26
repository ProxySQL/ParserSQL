#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>
using namespace sql_parser;

TEST(PgJsonXml, StructuredExpressionsRoundTrip) {
    for (const char* sql : {
        "SELECT JSON('{\"a\":1}' FORMAT JSON WITH UNIQUE KEYS)",
        "SELECT JSON_SCALAR(1 + 2), JSON_SERIALIZE(doc FORMAT JSON RETURNING text)",
        "SELECT JSON_ARRAY(1, doc FORMAT JSON, NULL ABSENT ON NULL RETURNING jsonb)",
        "SELECT JSON_ARRAY(), JSON_OBJECT(RETURNING jsonb)",
        "SELECT JSON_ARRAYAGG(i::text::json) FROM generate_series(1,5) i",
        "SELECT JSON_ARRAYAGG(i ORDER BY i DESC NULLS LAST ABSENT ON NULL RETURNING jsonb) FROM t",
        "SELECT JSON_ARRAYAGG(i) FILTER (WHERE i > 0) OVER (PARTITION BY k) FROM t",
        "SELECT JSON_OBJECTAGG(k VALUE v FORMAT JSON NULL ON NULL WITH UNIQUE KEYS RETURNING jsonb) FROM t",
        "SELECT JSON_ARRAY(SELECT n + 1 FROM t RETURNING jsonb)",
        "SELECT JSON_ARRAY(SELECT doc FROM t FORMAT JSON RETURNING json)",
        "SELECT JSON_OBJECT('a' VALUE n + 1, 'b': doc FORMAT JSON NULL ON NULL WITH UNIQUE KEYS RETURNING json)",
        "SELECT JSON_OBJECT(ARRAY['a','b'])",
        "SELECT JSON_QUERY(doc, '$.a' PASSING n AS x RETURNING jsonb WITH CONDITIONAL ARRAY WRAPPER KEEP QUOTES ON SCALAR STRING EMPTY ARRAY ON EMPTY ERROR ON ERROR)",
        "SELECT JSON_VALUE(doc FORMAT JSON, '$.a' RETURNING numeric(10,2) DEFAULT 2 + 3 ON EMPTY NULL ON ERROR)",
        "SELECT JSON_EXISTS(doc, '$.a' PASSING 1 AS x, 2 AS y FALSE ON ERROR)",
        "SELECT XMLPARSE(DOCUMENT '<a/>' PRESERVE WHITESPACE), XMLSERIALIZE(CONTENT doc AS text INDENT)",
        "SELECT XMLELEMENT(NAME a, XMLATTRIBUTES(x AS id, y), XMLFOREST(z AS child)), XMLPI(NAME p, 'x')",
        "SELECT XMLEXISTS('/a' PASSING BY REF doc BY VALUE), XMLROOT(doc, VERSION NO VALUE, STANDALONE YES)",
        "SELECT XMLCONCAT(a,b), XMLFOREST(a AS x,b)",
        "SELECT * FROM XMLTABLE('/a' PASSING doc COLUMNS x boolean DEFAULT (a = ANY(ARRAY[1])) NOT NULL) xt",
        "SELECT * FROM JSON_TABLE(doc, '$[*]' AS root PASSING 2 AS x COLUMNS (ord FOR ORDINALITY, val int PATH '$.v' DEFAULT 0 ON EMPTY, yes boolean EXISTS PATH '$.v' FALSE ON ERROR, NESTED PATH '$.a[*]' AS sub COLUMNS (value text PATH '$')) ERROR ON ERROR) AS jt",
        "SELECT * FROM XMLTABLE(XMLNAMESPACES('urn:x' AS x), '/x:r' PASSING BY REF doc COLUMNS n FOR ORDINALITY, value text PATH 'v' DEFAULT 'missing' NOT NULL) AS xt"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        if (!r.ok() || !r.full_input) { ADD_FAILURE() << "Incomplete parse"; continue; }
        Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(r.ast);
        auto out = emitter.result();
        Parser<Dialect::PostgreSQL> second;
        auto rr = second.parse(out.ptr, out.len);
        EXPECT_EQ(rr.status, ParseResult::OK) << std::string(out.ptr,out.len);
        EXPECT_TRUE(rr.full_input) << std::string(out.ptr,out.len);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast));
    }
}

TEST(PgJsonXml, RejectsMalformedProductions) {
    for (const char* sql : {
        "SELECT JSON()", "SELECT JSON_SCALAR()", "SELECT JSON_SERIALIZE(doc RETURNING)",
        "SELECT JSON_ARRAY(1,)", "SELECT JSON_OBJECT('a' VALUE)",
        "SELECT JSON_ARRAY(ABSENT ON NULL)", "SELECT JSON_OBJECT(WITH UNIQUE KEYS)",
        "SELECT JSON_QUERY(doc, '$' WITH CONDITIONAL)", "SELECT JSON_QUERY(doc, '$' OMIT QUOTES ON STRING)",
        "SELECT JSON_VALUE(doc, '$' NULL ON ERROR NULL ON EMPTY)", "SELECT JSON_VALUE(doc, '$' DEFAULT ON ERROR)",
        "SELECT JSON_EXISTS(doc, '$' RETURNING boolean)", "SELECT JSON_EXISTS(doc, '$' NULL ON EMPTY)",
        "SELECT JSON(doc FORMAT XML)", "SELECT JSON(doc FORMAT JSON ENCODING latin1)",
        "SELECT XMLPARSE(doc)", "SELECT XMLPARSE(DOCUMENT doc PRESERVE)",
        "SELECT XMLSERIALIZE(CONTENT doc AS text[])", "SELECT XMLSERIALIZE(CONTENT doc AS text ARRAY)",
        "SELECT XMLSERIALIZE(DOCUMENT doc)", "SELECT XMLELEMENT(a)", "SELECT XMLPI(NAME p, a, b)",
        "SELECT XMLEXISTS('/a' PASSING BY doc)", "SELECT XMLROOT(doc, VERSION NO)",
        "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS ()) jt",
        "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS (x int PATH 1)) jt",
        "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS (NESTED '$' COLUMNS ())) jt",
        "SELECT * FROM XMLTABLE('/a' PASSING doc COLUMNS x text PATH 'a' PATH 'b') xt",
        "SELECT * FROM XMLTABLE('/a' PASSING doc COLUMNS x text NULL NOT NULL) xt"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgJsonXml, DefaultsRemainEditableAndParameterizationIsConservative) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT JSON_VALUE(doc, '$.a' RETURNING int DEFAULT 2 + 3 ON EMPTY NULL ON ERROR)";
    auto r = parser.parse(sql,std::strlen(sql));
    ASSERT_TRUE(r.ok()); ASSERT_TRUE(r.full_input);
    AstNode* default_expr = nullptr;
    size_t strings = 0;
    walk_ast(r.ast, [&](const AstNode& node, const AstVisitContext&) {
        if (node.type == NodeType::NODE_LITERAL_STRING) ++strings;
        if (node.type == NodeType::NODE_BINARY_OP && node.value() == StringRef{"+",1})
            default_expr = const_cast<AstNode*>(&node);
        return AstVisitAction::Continue;
    });
    ASSERT_NE(default_expr,nullptr);
    EXPECT_EQ(strings,1u);
    ASSERT_EQ(default_expr->first_child->type,NodeType::NODE_LITERAL_INT);
    default_expr->first_child->set_value(StringRef{"42",2});
    default_expr->first_child->set_source({});
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(r.ast);
    auto out = emitter.result();
    EXPECT_NE(std::string(out.ptr,out.len).find("42 + 3"),std::string::npos);
    Arena target;
    EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(r,target).error,AstError::UnsupportedContext);
}

TEST(PgJsonXml, RejectsExpressionsWhereGrammarRequiresConstantsOrAtoms) {
    for (const char* sql : {
        "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS (x int PATH '$' || 'a')) jt",
        "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS (NESTED '$' || 'a' COLUMNS (x int))) jt",
        "SELECT JSON_OBJECT('a' || 'b' VALUE 1)",
        "SELECT XMLEXISTS('/a' || '/b' PASSING doc)",
        "SELECT XMLEXISTS('/a' PASSING doc || more)",
        "SELECT JSON_ARRAYAGG()", "SELECT JSON_ARRAYAGG(1,2)", "SELECT JSON_ARRAYAGG(1 ORDER BY)",
        "SELECT JSON_OBJECTAGG('a' VALUE)", "SELECT JSON_OBJECTAGG('a':1, 'b':2)",
        "SELECT JSON_SCALAR(*)", "SELECT XMLCONCAT(*)", "SELECT JSON_ARRAY(*)",
        "SELECT XMLEXISTS(a IS NULL PASSING doc)",
        "SELECT XMLEXISTS(CAST(a AS text)::int PASSING doc)",
        "SELECT * FROM XMLTABLE('/a' PASSING doc COLUMNS x text DEFAULT a AND b) xt",
        "SELECT * FROM XMLTABLE('/a' PASSING doc COLUMNS x int DEFAULT a = ANY(ARRAY[1])) xt",
        "SELECT * FROM XMLTABLE('/a' PASSING doc COLUMNS x text PATH a COLLATE b) xt",
        "SELECT XMLFOREST()", "SELECT JSON_QUERY(doc, '$' ERROR ON ERROR NULL ON EMPTY)"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto r=parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input);
    }
}

TEST(PgJsonXml, PropagatesArenaExhaustion) {
    for (const char* sql : {
        "SELECT JSON_ARRAY(1, 2, 3, 4, 5 ABSENT ON NULL RETURNING jsonb)",
        "SELECT JSON_QUERY(doc, '$' DEFAULT 1 ON EMPTY NULL ON ERROR)",
        "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS (x int, NESTED '$' COLUMNS (y int))) jt",
        "SELECT * FROM XMLTABLE('/a' PASSING doc COLUMNS x text PATH 'v' DEFAULT 'd') xt"
    }) {
        auto node_count = [](const AstNode* ast) {
            size_t count = 0;
            walk_ast(ast, [&](const AstNode&, const AstVisitContext&) { ++count; return AstVisitAction::Continue; });
            return count;
        };
        Parser<Dialect::PostgreSQL> baseline;
        auto valid = baseline.parse(sql,std::strlen(sql));
        ASSERT_TRUE(valid.ok() && valid.full_input);
        const size_t expected_nodes = node_count(valid.ast);
        for (size_t capacity = 48; capacity <= 1536; capacity += 48) {
            SCOPED_TRACE(sql);
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size = capacity; config.arena_max_size = capacity;
            Parser<Dialect::PostgreSQL> parser(config);
            auto result = parser.parse(sql,std::strlen(sql));
            if (result.ok() && result.full_input) {
                EXPECT_EQ(node_count(result.ast),expected_nodes);
            }
        }
    }
}
