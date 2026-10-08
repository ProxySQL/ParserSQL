#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_transform.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>

using namespace sql_parser;

namespace {
std::string text(StringRef s) { return s.len ? std::string(s.ptr, s.len) : ""; }
template<Dialect D> std::string emit(const AstNode* ast, Arena& arena) {
    Emitter<D> emitter(arena);
    emitter.emit(ast);
    return text(emitter.result());
}
std::vector<AstNode*> aliases(AstNode* root) {
    std::vector<AstNode*> result;
    walk_ast(root, [&](const AstNode& node, const AstVisitContext&) {
        if (node.type == NodeType::NODE_ALIAS) result.push_back(const_cast<AstNode*>(&node));
        return AstVisitAction::Continue;
    });
    return result;
}
template<Dialect D> void check_aliases(const char* sql, size_t count, const char* spelling) {
    SCOPED_TRACE(sql);
    Parser<D> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    auto names = aliases(result.ast);
    ASSERT_EQ(names.size(), count);
    for (auto* alias : names) {
        EXPECT_EQ(text(alias->value()), "Alias Name");
        EXPECT_EQ(text(alias->source()), spelling);
        EXPECT_NE(alias->flags & FLAG_IDENT_DELIMITED, 0);
    }
    auto canonical = emit<D>(result.ast, parser.arena());
    EXPECT_NE(canonical.find(spelling), std::string::npos);
    Parser<D> again;
    auto reparsed = again.parse(canonical.data(), canonical.size());
    ASSERT_TRUE(reparsed.ok() && reparsed.full_input);
    for (auto* alias : aliases(reparsed.ast)) EXPECT_EQ(text(alias->value()), "Alias Name");
    // AST clients edit the semantic identifier and clear the obsolete source spelling.
    names.front()->set_value({"new\"`name", 9});
    names.front()->set_source({});
    auto edited = emit<D>(result.ast, parser.arena());
    EXPECT_NE(edited.find(D == Dialect::PostgreSQL ? "\"new\"\"`name\"" : "`new\"``name`"), std::string::npos);
}
}

TEST(ReviewAst, RemovedMandatoryChildrenRemainEmittable) {
    const char* queries[] = {
        "SELECT sum(x) FILTER (WHERE x > 0) FROM t",
        "SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        "SELECT sum(x) OVER w FROM t WINDOW w AS (ORDER BY x)",
        "SELECT substring(x FROM 1 FOR 2), trim(both 'x' FROM x), x AT TIME ZONE 'UTC' FROM t",
        "SELECT * FROM a JOIN b ON a.id = b.id",
        "SELECT * FROM t TABLESAMPLE system(1) REPEATABLE(2)",
        "SELECT * FROM f() AS (x int)",
        "SELECT interval '1' day"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        EXPECT_EXIT(( [&] {
            Parser<Dialect::PostgreSQL> parser;
            auto parsed = parser.parse(sql, std::strlen(sql));
            if (!parsed.ok() || !parsed.full_input) std::exit(2);
            std::vector<AstNode*> nodes;
            walk_ast(parsed.ast, [&](const AstNode& n, const AstVisitContext&) {
                nodes.push_back(const_cast<AstNode*>(&n));
                return AstVisitAction::Continue;
            });
            for (auto* node : nodes) {
                if (!node->first_child) continue;
                Arena arena;
                auto cloned = clone_ast(node, arena);
                if (!cloned.ok()) std::exit(3);
                while (cloned.ast->first_child) {
                    if (replace_ast_subtree(cloned.ast, cloned.ast->first_child, nullptr) != AstError::None)
                        std::exit(4);
                    emit<Dialect::PostgreSQL>(cloned.ast, arena);
                }
            }
            std::exit(0);
        }()), ::testing::ExitedWithCode(0), "");
    }
}

TEST(ReviewAst, QuotedAliasesKeepSemanticValuesAndEditableSource) {
    check_aliases<Dialect::PostgreSQL>("SELECT x AS \"Alias Name\" FROM t AS \"Alias Name\"", 2, "\"Alias Name\"");
    check_aliases<Dialect::PostgreSQL>("SELECT x \"Alias Name\" FROM t \"Alias Name\"", 2, "\"Alias Name\"");
    check_aliases<Dialect::PostgreSQL>("SELECT * FROM t AS \"Alias Name\"(\"Column Name\")", 1, "\"Alias Name\"");
    check_aliases<Dialect::PostgreSQL>("UPDATE t AS \"Alias Name\" SET x=1 RETURNING x AS \"Alias Name\"", 2, "\"Alias Name\"");
    check_aliases<Dialect::PostgreSQL>("DELETE FROM t \"Alias Name\" RETURNING x \"Alias Name\"", 2, "\"Alias Name\"");
    check_aliases<Dialect::MySQL>("UPDATE t AS `Alias Name` SET x=1", 1, "`Alias Name`");
    check_aliases<Dialect::MySQL>("DELETE `Alias Name` FROM t AS `Alias Name` WHERE x=1", 1, "`Alias Name`");
    check_aliases<Dialect::MySQL>("INSERT INTO t SELECT x AS `Alias Name` FROM s", 1, "`Alias Name`");
    check_aliases<Dialect::MySQL>("SELECT x AS `Alias Name` FROM t AS `Alias Name`", 2, "`Alias Name`");
    check_aliases<Dialect::MySQL>("SELECT x `Alias Name` FROM t `Alias Name`", 2, "`Alias Name`");
}

TEST(ReviewAst, QuotedAliasesReachLocalPlansAsNames) {
    sql_engine::InMemoryCatalog catalog;
    catalog.add_table("", "t", {{"x", sql_engine::SqlType::make_int(), false}});
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT x AS `Alias Name` FROM t AS `Table Name`";
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    sql_engine::PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    auto* plan = builder.build(result.ast);
    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->type, sql_engine::PlanNodeType::PROJECT);
    ASSERT_NE(plan->project.aliases[0], nullptr);
    EXPECT_EQ(text(plan->project.aliases[0]->value()), "Alias Name");
    ASSERT_NE(plan->left, nullptr);
    ASSERT_EQ(plan->left->type, sql_engine::PlanNodeType::SCAN);
    ASSERT_NE(plan->left->scan.table, nullptr);
    EXPECT_EQ(text(plan->left->scan.table->alias), "Table Name");
}

TEST(ReviewAst, DmlRelationModifiersPreserveMetadataAndUnsupportedStatus) {
    for (const char* sql : {"UPDATE ONLY s.t SET x=1", "UPDATE ONLY (s.t) SET x=1",
            "UPDATE s.t * SET x=1", "DELETE FROM ONLY s.t", "DELETE FROM ONLY (s.t)", "DELETE FROM s.t *"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok() && result.full_input);
        EXPECT_EQ(text(result.schema_name), "s");
        EXPECT_EQ(text(result.table_name), "t");
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_dml_features(result.ast));
        Arena parameters;
        EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(result, parameters).error, AstError::UnsupportedContext);
        auto canonical = emit<Dialect::PostgreSQL>(result.ast, parser.arena());
        EXPECT_NE(canonical.find(std::strstr(sql, "ONLY") ? "ONLY s.t" : "s.t *"), std::string::npos);
        Parser<Dialect::PostgreSQL> again;
        auto reparsed = again.parse(canonical.data(), canonical.size());
        ASSERT_TRUE(reparsed.ok() && reparsed.full_input);
        EXPECT_EQ(text(reparsed.table_name), "t");
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_dml_features(reparsed.ast));
    }
}

TEST(ReviewAst, RejectsMalformedDmlRelationModifiers) {
    for (const char* sql : {"UPDATE ONLY t * SET x=1", "UPDATE ONLY (t *) SET x=1",
            "UPDATE (t) SET x=1", "UPDATE ONLY () SET x=1", "DELETE FROM ONLY (t) *",
            "DELETE FROM t **", "DELETE FROM ONLY ((t))"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(ReviewAst, JoinUsingAliasKeepsSemanticValueAndEditableSource) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT * FROM a JOIN b USING (x) AS \"Alias Name\"";
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    AstNode* alias = nullptr;
    walk_ast(result.ast, [&](const AstNode& node, const AstVisitContext&) {
        if (node.type == NodeType::NODE_PG_JOIN_USING) alias = const_cast<AstNode*>(&node);
        return AstVisitAction::Continue;
    });
    ASSERT_NE(alias, nullptr);
    EXPECT_EQ(text(alias->value()), "Alias Name");
    EXPECT_EQ(text(alias->source()), "\"Alias Name\"");
    EXPECT_NE(alias->flags & FLAG_IDENT_DELIMITED, 0);
    EXPECT_NE(emit<Dialect::PostgreSQL>(result.ast, parser.arena()).find("AS \"Alias Name\""), std::string::npos);
    alias->set_value({"New Name", 8});
    alias->set_source({});
    EXPECT_NE(emit<Dialect::PostgreSQL>(result.ast, parser.arena()).find("AS \"New Name\""), std::string::npos);
}
