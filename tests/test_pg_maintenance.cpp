#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_walk.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>
#include <vector>
using namespace sql_parser;

static void maintenance_roundtrip(const char* sql, StmtType type) {
    SCOPED_TRACE(sql);
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    EXPECT_EQ(result.stmt_type, type);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(result.ast->type, NodeType::NODE_PG_COMMAND_STMT);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));
    Arena copied;
    EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(result, copied).error, AstError::UnsupportedRoot);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
    auto output = emitter.result();
    Parser<Dialect::PostgreSQL> again;
    auto second = again.parse(output.ptr, output.len);
    EXPECT_TRUE(second.ok() && second.full_input) << std::string(output.ptr, output.len);
    EXPECT_EQ(second.stmt_type, type);
}

TEST(PgMaintenance, ReindexTargetsAndGenericOptions) {
    for (const char* sql : {
        "REINDEX INDEX i", "REINDEX TABLE public.t", "REINDEX INDEX CONCURRENTLY db.public.i",
        "REINDEX SCHEMA public", "REINDEX SCHEMA CONCURRENTLY \"Schema\"",
        "REINDEX DATABASE", "REINDEX SYSTEM", "REINDEX DATABASE CONCURRENTLY",
        "REINDEX SYSTEM CONCURRENTLY db", "REINDEX DATABASE db",
        "REINDEX (VERBOSE, TABLESPACE space, CONCURRENTLY true) TABLE t",
        "REINDEX (unknown, full false, freeze on, analyze off, analyse 'yes', format text) INDEX i",
        "REINDEX (authorization binary, between integer, \"select\" \"where\") TABLE \"Table\"",
        "REINDEX (n 1, n -2, n +3.5, n .5, n 1e3, n 2147483648) INDEX i",
        "REINDEX (n 0x10, n 0o17, n 0b10, n 2_000, n 0x_ffff_ffff, n 1_2.3_4e+5_6) INDEX i",
        "REINDEX (v e, v $$dollar value$$, v 'string') INDEX i",
        "REINDEX (v E'escaped\\nvalue') TABLE t", "REINDEX TABLE s.select;"
    }) maintenance_roundtrip(sql, StmtType::REINDEX);
}

TEST(PgMaintenance, ClusterModernAndLegacyForms) {
    for (const char* sql : {
        "CLUSTER", "CLUSTER VERBOSE", "CLUSTER public.t", "CLUSTER t USING i",
        "CLUSTER VERBOSE db.s.t USING \"Index\"", "CLUSTER i ON public.t",
        "CLUSTER VERBOSE \"Index\" ON \"Schema\".\"Table\"",
        "CLUSTER (verbose)", "CLUSTER (verbose false) public.t USING i",
        "CLUSTER (unknown 2, full on, format text, analyse false) t", "CLUSTER (v -0.5)"
    }) maintenance_roundtrip(sql, StmtType::CLUSTER);
}

TEST(PgMaintenance, RefreshAndLockRelationsAndModes) {
    for (const char* sql : {
        "REFRESH MATERIALIZED VIEW v", "REFRESH MATERIALIZED VIEW CONCURRENTLY s.v",
        "REFRESH MATERIALIZED VIEW db.s.v WITH DATA", "REFRESH MATERIALIZED VIEW CONCURRENTLY v WITH NO DATA"
    }) maintenance_roundtrip(sql, StmtType::REFRESH_MATERIALIZED_VIEW);
    for (const char* sql : {
        "LOCK t", "LOCK TABLE t", "LOCK ONLY t", "LOCK ONLY (public.t)", "LOCK t *",
        "LOCK TABLE ONLY (s.t), u *, db.s.v NOWAIT", "LOCK TABLE t IN ACCESS SHARE MODE",
        "LOCK t IN ROW SHARE MODE", "LOCK t IN ROW EXCLUSIVE MODE NOWAIT",
        "LOCK t IN SHARE UPDATE EXCLUSIVE MODE", "LOCK t IN SHARE MODE",
        "LOCK t IN SHARE ROW EXCLUSIVE MODE", "LOCK t IN EXCLUSIVE MODE",
        "LOCK t IN ACCESS EXCLUSIVE MODE NOWAIT"
    }) maintenance_roundtrip(sql, StmtType::LOCK);
}

TEST(PgMaintenance, RejectsMalformedAndMisorderedClauses) {
    for (const char* sql : {
        "REINDEX", "REINDEX INDEX", "REINDEX SCHEMA", "REINDEX TABLE ONLY t", "REINDEX TABLE t *",
        "REINDEX TABLE a.b.c.d", "REINDEX TABLE t[1]", "REINDEX TABLE t.*", "REINDEX SCHEMA s.t",
        "REINDEX SYSTEM s.db", "REINDEX CONCURRENTLY INDEX i", "REINDEX INDEX i CONCURRENTLY",
        "REINDEX () TABLE t", "REINDEX (verbose,) INDEX i", "REINDEX (verbose = true) TABLE t",
        "REINDEX (select) TABLE t", "REINDEX (v NULL) TABLE t", "REINDEX (v analyze) TABLE t",
        "REINDEX (v +true) TABLE t", "REINDEX (v 1 + 2) TABLE t", "REINDEX (v 1__2) TABLE t",
        "REINDEX (v 0x) TABLE t", "REINDEX (v 0b2) TABLE t", "REINDEX (v 1abc) TABLE t",
        "REINDEX (v 1e) TABLE t", "REINDEX (v --1\n) TABLE t trailing", "REINDEX TABLE t garbage",
        "CLUSTER ()", "CLUSTER USING i", "CLUSTER t USING", "CLUSTER t USING s.i",
        "CLUSTER s.i ON t", "CLUSTER (verbose) i ON t", "CLUSTER VERBOSE (verbose) t",
        "CLUSTER ONLY t", "CLUSTER t *", "CLUSTER t, u", "CLUSTER t USING i garbage",
        "REFRESH VIEW v", "REFRESH MATERIALIZED VIEW", "REFRESH MATERIALIZED VIEW ONLY v",
        "REFRESH MATERIALIZED VIEW v CONCURRENTLY", "REFRESH MATERIALIZED VIEW v WITH NO",
        "REFRESH MATERIALIZED VIEW v WITH NO DATA garbage", "REFRESH MATERIALIZED VIEW v *",
        "LOCK", "LOCK TABLE", "LOCK t,", "LOCK ONLY (t, u)", "LOCK ONLY t *",
        "LOCK t IN MODE", "LOCK t IN ACCESS MODE", "LOCK t IN ROW MODE", "LOCK t IN SHARE UPDATE MODE",
        "LOCK t IN SHARE EXCLUSIVE MODE", "LOCK t IN EXCLUSIVE", "LOCK t NOWAIT IN SHARE MODE",
        "LOCK t SKIP LOCKED", "LOCK TABLES t READ", "LOCK t AS alias", "LOCK t NOWAIT garbage"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(PgMaintenance, StructuredOperandsRemainTraversable) {
    const char* sql = "REINDEX (tablespace \"Space\", n -2) INDEX CONCURRENTLY \"Schema\".\"Index\"";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    std::vector<std::string> identifiers;
    bool literal = false, options = false;
    walk_ast(result.ast, [&](const AstNode& node, const AstVisitContext&) {
        if (node.type == NodeType::NODE_IDENTIFIER) {
            identifiers.emplace_back(node.value().ptr, node.value().len);
            if (identifiers.back() == "Space" || identifiers.back() == "Schema" || identifiers.back() == "Index") {
                EXPECT_TRUE(node.flags & FLAG_IDENT_DELIMITED);
            }
        }
        literal |= node.type == NodeType::NODE_LITERAL_INT;
        options |= node.type == NodeType::NODE_PG_DDL_LIST;
        return AstVisitAction::Continue;
    });
    EXPECT_EQ(identifiers, (std::vector<std::string>{"tablespace", "Space", "n", "Schema", "Index"}));
    EXPECT_TRUE(literal); EXPECT_TRUE(options);
}

TEST(PgMaintenance, AllocationFailureCannotOmitOperands) {
    for (const char* sql : {
        "REINDEX (verbose, tablespace \"Space\", n -2) INDEX CONCURRENTLY public.i",
        "CLUSTER (verbose false) public.t USING i", "CLUSTER VERBOSE i ON public.t",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY s.v WITH NO DATA",
        "LOCK TABLE ONLY (s.t), u * IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    }) {
        SCOPED_TRACE(sql);
        auto count = [](const AstNode* ast) {
            size_t n = 0;
            walk_ast(ast, [&](const AstNode&, const AstVisitContext&) { ++n; return AstVisitAction::Continue; });
            return n;
        };
        Parser<Dialect::PostgreSQL> baseline;
        auto complete = baseline.parse(sql, std::strlen(sql)); ASSERT_TRUE(complete.ok() && complete.full_input);
        size_t expected = count(complete.ast);
        bool saw_error = false, saw_success = false;
        for (size_t capacity = 48; capacity <= 3072; capacity += 48) {
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size = capacity; config.arena_max_size = capacity;
            Parser<Dialect::PostgreSQL> parser(config);
            auto result = parser.parse(sql, std::strlen(sql));
            if (result.ok() && result.full_input) { saw_success = true; EXPECT_EQ(count(result.ast), expected); }
            else saw_error = true;
        }
        EXPECT_TRUE(saw_error); EXPECT_TRUE(saw_success);
    }
}

TEST(PgMaintenance, MysqlLockBehaviorIsUnchanged) {
    for (const char* sql : {"LOCK TABLES t READ, u WRITE", "LOCK TABLES t READ LOCAL"}) {
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_TRUE(result.ok());
        EXPECT_FALSE(result.full_input);
        EXPECT_EQ(result.stmt_type, StmtType::LOCK);
        EXPECT_EQ(result.ast, nullptr);
    }
}
