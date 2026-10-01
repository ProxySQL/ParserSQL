#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

static size_t foreign_node_count(const AstNode* root) {
    size_t count = 0;
    walk_ast(root, [&](const AstNode&, const AstVisitContext&) { ++count; return AstVisitAction::Continue; });
    return count;
}

static void foreign_accept(const char* sql, StmtType type) {
    SCOPED_TRACE(sql);
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    ASSERT_EQ(result.stmt_type, type);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(result.ast->type, NodeType::NODE_PG_DDL_STMT);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
    auto output = emitter.result();
    Parser<Dialect::PostgreSQL> reparsed;
    auto again = reparsed.parse(output.ptr, output.len);
    EXPECT_TRUE(again.ok() && again.full_input) << std::string(output.ptr, output.len);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));
    Arena out;
    EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(result, out).error, AstError::UnsupportedRoot);
}

TEST(PgForeignDdl, WrapperAndServer) {
    for (const char* sql : {
        "CREATE FOREIGN DATA WRAPPER fdw",
        "CREATE FOREIGN DATA WRAPPER fdw HANDLER public.h VALIDATOR v OPTIONS (host 'localhost', \"Mixed\" E'value')",
        "CREATE FOREIGN DATA WRAPPER fdw NO HANDLER NO VALIDATOR",
        "ALTER FOREIGN DATA WRAPPER fdw NO HANDLER VALIDATOR public.v",
        "ALTER FOREIGN DATA WRAPPER fdw OPTIONS (ADD host 'a', SET port '2', DROP old, user 'x')",
        "ALTER FOREIGN DATA WRAPPER fdw HANDLER h OPTIONS (x 'y')",
        "ALTER FOREIGN DATA WRAPPER fdw RENAME TO fdw2",
        "ALTER FOREIGN DATA WRAPPER fdw OWNER TO CURRENT_USER",
        "DROP FOREIGN DATA WRAPPER IF EXISTS fdw, other CASCADE"
    }) foreign_accept(sql, std::strncmp(sql, "CREATE", 6) == 0 ? StmtType::CREATE :
                               std::strncmp(sql, "ALTER", 5) == 0 ? StmtType::ALTER : StmtType::DROP);
    for (const char* sql : {
        "CREATE SERVER s FOREIGN DATA WRAPPER fdw",
        "CREATE SERVER IF NOT EXISTS s TYPE 'postgres' VERSION '18' FOREIGN DATA WRAPPER fdw OPTIONS (host 'localhost')",
        "CREATE SERVER s VERSION NULL FOREIGN DATA WRAPPER fdw",
        "ALTER SERVER s VERSION '19' OPTIONS (SET host 'new', DROP port)",
        "ALTER SERVER s VERSION NULL", "ALTER SERVER s OPTIONS (ADD host 'x')",
        "ALTER SERVER s OPTIONS (set 'v', add E'v2', drop 'v3')",
        "ALTER SERVER s RENAME TO s2", "ALTER SERVER s OWNER TO SESSION_USER",
        "DROP SERVER IF EXISTS s, s2 RESTRICT"
    }) foreign_accept(sql, std::strncmp(sql, "CREATE", 6) == 0 ? StmtType::CREATE :
                               std::strncmp(sql, "ALTER", 5) == 0 ? StmtType::ALTER : StmtType::DROP);
}

TEST(PgForeignDdl, UserMappings) {
    for (const char* sql : {
        "CREATE USER MAPPING FOR alice SERVER s",
        "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER s OPTIONS (user 'alice', password 'secret')",
        "CREATE USER MAPPING FOR USER SERVER s",
        "CREATE USER MAPPING FOR \"user\" SERVER s OPTIONS (\"Option\" 'x')",
        "CREATE USER MAPPING FOR \"NONE\" SERVER s",
        "ALTER USER MAPPING FOR PUBLIC SERVER s OPTIONS (SET user 'a', DROP password)",
        "ALTER USER MAPPING FOR SESSION_USER SERVER s OPTIONS (ADD user 'b')",
        "DROP USER MAPPING FOR CURRENT_ROLE SERVER s",
        "DROP USER MAPPING IF EXISTS FOR \"Public\" SERVER s"
    }) foreign_accept(sql, std::strncmp(sql, "CREATE", 6) == 0 ? StmtType::CREATE :
                               std::strncmp(sql, "ALTER", 5) == 0 ? StmtType::ALTER : StmtType::DROP);
}

TEST(PgForeignDdl, TablesAndImport) {
    for (const char* sql : {
        "CREATE FOREIGN TABLE ft () SERVER s",
        "CREATE FOREIGN TABLE IF NOT EXISTS public.ft (a int OPTIONS (remote_name 'A') NOT NULL, b text CHECK (b <> ''), CHECK (a > 0)) INHERITS (parent) SERVER s OPTIONS (schema_name 'public')",
        "CREATE FOREIGN TABLE ft (LIKE source INCLUDING DEFAULTS) SERVER s",
        "CREATE FOREIGN TABLE ft (a text STORAGE PLAIN COMPRESSION pglz OPTIONS (remote_name 'A') COLLATE \"C\" NOT NULL) SERVER s",
        "CREATE FOREIGN TABLE fp PARTITION OF parent DEFAULT SERVER s",
        "CREATE FOREIGN TABLE fp PARTITION OF parent (a WITH OPTIONS NOT NULL, CHECK (a > 0)) FOR VALUES IN (1, 2) SERVER s OPTIONS (table_name 'p')",
        "CREATE FOREIGN TABLE fp PARTITION OF parent FOR VALUES WITH (MODULUS 4, REMAINDER 0) SERVER s",
        "ALTER FOREIGN TABLE IF EXISTS ft ADD COLUMN c int OPTIONS (remote_name 'C'), ALTER COLUMN a OPTIONS (ADD remote_name 'A', DROP old)",
        "ALTER FOREIGN TABLE ft OPTIONS (SET schema_name 'remote', DROP old)",
        "ALTER FOREIGN TABLE ft RENAME COLUMN a TO b", "ALTER FOREIGN TABLE ft OWNER TO alice",
        "ALTER FOREIGN TABLE ft OWNER TO alice, ADD COLUMN a int",
        "ALTER FOREIGN TABLE ft ADD COLUMN a int, OWNER TO alice",
        "ALTER FOREIGN TABLE ONLY (ft) ADD COLUMN a int",
        "ALTER FOREIGN TABLE ft* ADD COLUMN a int",
        "ALTER FOREIGN TABLE ft SET SCHEMA other", "ALTER FOREIGN TABLE ft RENAME TO renamed",
        "DROP FOREIGN TABLE IF EXISTS public.ft, fp CASCADE"
    }) foreign_accept(sql, std::strncmp(sql, "CREATE", 6) == 0 ? StmtType::CREATE :
                               std::strncmp(sql, "ALTER", 5) == 0 ? StmtType::ALTER : StmtType::DROP);
    for (const char* sql : {
        "IMPORT FOREIGN SCHEMA remote FROM SERVER s INTO local",
        "IMPORT FOREIGN SCHEMA remote LIMIT TO (a, public.b, ONLY c) FROM SERVER s INTO local OPTIONS (import_default 'true')",
        "IMPORT FOREIGN SCHEMA remote EXCEPT (a, b*) FROM SERVER s INTO local"
    }) foreign_accept(sql, StmtType::IMPORT_FOREIGN_SCHEMA);
    foreign_accept("CREATE TABLE t (a int OPTIONS (remote_name 'A'), b text)", StmtType::CREATE);
}

TEST(PgForeignDdl, RejectsMalformedProductions) {
    for (const char* sql : {
        "CREATE FOREIGN DATA WRAPPER fdw OPTIONS ()", "CREATE FOREIGN DATA WRAPPER fdw OPTIONS (host = 'x')",
        "CREATE FOREIGN DATA WRAPPER fdw OPTIONS (host 42)", "CREATE FOREIGN DATA WRAPPER fdw OPTIONS (host 'x',)",
        "CREATE FOREIGN DATA WRAPPER fdw OPTIONS (host 'x') HANDLER h", "ALTER FOREIGN DATA WRAPPER fdw",
        "ALTER FOREIGN DATA WRAPPER fdw OPTIONS ()", "ALTER FOREIGN DATA WRAPPER fdw OPTIONS (DROP host 'x')",
        "CREATE SERVER s TYPE postgres FOREIGN DATA WRAPPER fdw", "CREATE SERVER s VERSION '1' TYPE 'x' FOREIGN DATA WRAPPER fdw",
        "CREATE SERVER s FOREIGN DATA WRAPPER fdw TYPE 'x'", "ALTER SERVER s",
        "ALTER SERVER s OPTIONS (ADD x)", "CREATE USER MAPPING FOR alice",
        "ALTER USER MAPPING FOR alice SERVER s", "DROP USER MAPPING FOR alice SERVER s CASCADE",
        "CREATE USER MAPPING FOR NONE SERVER s", "CREATE USER MAPPING FOR NoNe SERVER s",
        "CREATE FOREIGN TABLE ft (a int)", "CREATE FOREIGN TABLE ft (a int OPTIONS (x 'y') OPTIONS (z 'w')) SERVER s",
        "CREATE FOREIGN TABLE ft (a int NOT NULL OPTIONS (x 'y')) SERVER s",
        "CREATE FOREIGN TABLE fp PARTITION OF parent (a int) DEFAULT SERVER s",
        "CREATE FOREIGN TABLE fp PARTITION OF parent (a OPTIONS (x 'y')) DEFAULT SERVER s",
        "CREATE FOREIGN TABLE ft (a int) SERVER s INHERITS (p)",
        "ALTER FOREIGN TABLE ft", "ALTER FOREIGN TABLE ft ALTER COLUMN a OPTIONS ()",
        "ALTER FOREIGN TABLE ft ADD COLUMN a int, RENAME TO ft2",
        "ALTER FOREIGN TABLE ft ADD COLUMN a int, SET SCHEMA s",
        "ALTER FOREIGN TABLE ONLY ft * ADD COLUMN a int",
        "CREATE FOREIGN TABLE fp PARTITION OF p (a COLLATE \"C\" COLLATE \"D\") DEFAULT SERVER s",
        "IMPORT FOREIGN SCHEMA remote ALL FROM SERVER s INTO local",
        "IMPORT FOREIGN SCHEMA remote LIMIT TO () FROM SERVER s INTO local",
        "IMPORT FOREIGN SCHEMA remote LIMIT TO (a,) FROM SERVER s INTO local",
        "IMPORT FOREIGN SCHEMA remote FROM SERVER s INTO local OPTIONS ()"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input);
    }
}

TEST(PgForeignDdl, ParenthesizedOnlyRelationRetainsMetadata) {
    const char* sql = "ALTER FOREIGN TABLE ONLY (public.ft) ADD COLUMN a int";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    EXPECT_EQ(result.schema_name, (StringRef{"public", 6}));
    EXPECT_EQ(result.table_name, (StringRef{"ft", 2}));
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
    auto output = emitter.result();
    EXPECT_NE(std::string(output.ptr, output.len).find("ONLY (public.ft)"), std::string::npos);
}

TEST(PgForeignDdl, StructuredTraversalAndAllocationFailure) {
    const char* sql = "CREATE FOREIGN TABLE \"T\" (a int OPTIONS (\"remote name\" 'A')) SERVER s OPTIONS (host 'example')";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    bool quoted = false; AstNode* literal = nullptr;
    walk_ast(result.ast, [&](const AstNode& n, const AstVisitContext&) {
        if (n.type == NodeType::NODE_IDENTIFIER && n.value() == StringRef{"remote name", 11}) quoted = (n.flags & FLAG_IDENT_DELIMITED) != 0;
        if (n.type == NodeType::NODE_LITERAL_STRING && n.value() == StringRef{"A", 1}) literal = const_cast<AstNode*>(&n);
        return AstVisitAction::Continue;
    });
    EXPECT_TRUE(quoted); ASSERT_NE(literal, nullptr);
    literal->set_value(StringRef{"B", 1}); literal->set_source({});
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
    auto output = emitter.result();
    EXPECT_NE(std::string(output.ptr, output.len).find("'B'"), std::string::npos);
    for (const char* statement : {
        sql,
        "ALTER FOREIGN DATA WRAPPER fdw HANDLER h OPTIONS (ADD host 'x', DROP old)",
        "CREATE SERVER s TYPE 'p' VERSION '1' FOREIGN DATA WRAPPER fdw OPTIONS (host 'x')",
        "ALTER USER MAPPING FOR CURRENT_USER SERVER s OPTIONS (SET user 'a', DROP password)",
        "IMPORT FOREIGN SCHEMA r LIMIT TO (a, ONLY b) FROM SERVER s INTO l OPTIONS (x 'y')"
    }) {
        Parser<Dialect::PostgreSQL> baseline;
        auto valid = baseline.parse(statement, std::strlen(statement));
        ASSERT_TRUE(valid.ok() && valid.full_input);
        const auto expected = foreign_node_count(valid.ast);
        for (size_t capacity = 48; capacity <= 1920; capacity += 48) {
            SCOPED_TRACE(statement);
            SCOPED_TRACE(capacity);
            ParserConfig cfg; cfg.arena_block_size = capacity; cfg.arena_max_size = capacity;
            Parser<Dialect::PostgreSQL> small(cfg); auto r = small.parse(statement, std::strlen(statement));
            if (r.ok() && r.full_input) { EXPECT_EQ(foreign_node_count(r.ast), expected); }
        }
    }
}
