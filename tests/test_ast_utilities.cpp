#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_transform.h"
#include "sql_parser/parameterize.h"
#include <string>
#include <vector>

using namespace sql_parser;

namespace {
std::string text(StringRef value) { return value.len ? std::string(value.ptr, value.len) : ""; }
template <Dialect D = Dialect::PostgreSQL>
std::string emit(const AstNode* node, Arena& arena) {
    Emitter<D> emitter(arena);
    emitter.emit(node);
    return text(emitter.result());
}
ParseResult parse(Parser<Dialect::PostgreSQL>& parser, const char* sql) {
    return parser.parse(sql, std::strlen(sql));
}
}

TEST(AstUtilities, PreorderSkipStopAndSubtreeBoundary) {
    Arena arena;
    auto root = make_node(arena, NodeType::NODE_SELECT_STMT);
    auto branch = make_node(arena, NodeType::NODE_FROM_CLAUSE);
    auto leaf = make_node(arena, NodeType::NODE_TABLE_REF);
    auto tail = make_node(arena, NodeType::NODE_WHERE_CLAUSE);
    root->add_child(branch);
    branch->add_child(leaf);
    root->add_child(tail);
    root->next_sibling = make_node(arena, NodeType::NODE_UNKNOWN);
    std::vector<NodeType> seen;
    auto result = walk_ast(root, [&](const AstNode& node, const AstVisitContext& ctx) {
        seen.push_back(node.type);
        if (&node == branch) {
            EXPECT_EQ(ctx.parent, root);
            EXPECT_EQ(ctx.depth, 1u);
            EXPECT_EQ(ctx.child_index, 0u);
            return AstVisitAction::SkipChildren;
        }
        return AstVisitAction::Continue;
    });
    EXPECT_EQ(result.status, AstWalkStatus::Completed);
    EXPECT_EQ(seen, (std::vector<NodeType>{NodeType::NODE_SELECT_STMT, NodeType::NODE_FROM_CLAUSE, NodeType::NODE_WHERE_CLAUSE}));
    seen.clear();
    result = walk_ast(root, [&](const AstNode& node, const AstVisitContext&) {
        seen.push_back(node.type);
        return &node == leaf ? AstVisitAction::Stop : AstVisitAction::Continue;
    });
    EXPECT_EQ(result.status, AstWalkStatus::Stopped);
    EXPECT_EQ(seen.size(), 3u);
}

TEST(AstUtilities, BoundedWalkHandlesCyclesAndDeepTrees) {
    Arena arena(65536, 2 * 1024 * 1024);
    auto root = make_node(arena, NodeType::NODE_EXPRESSION);
    auto node = root;
    for (size_t i = 0; i < 10000; ++i) {
        node->first_child = make_node(arena, NodeType::NODE_EXPRESSION);
        node = node->first_child;
    }
    auto visit = [](const AstNode&, const AstVisitContext&) { return AstVisitAction::Continue; };
    EXPECT_EQ(walk_ast(root, visit, {20000, 20000}).visited, 10001u);
    EXPECT_EQ(walk_ast(root, visit, {20000, 100}).status, AstWalkStatus::LimitExceeded);
    node->first_child = root;
    EXPECT_EQ(walk_ast(root, visit, {12000, 20000}).status, AstWalkStatus::LimitExceeded);
}

TEST(AstUtilities, CloneOwnsTextAndDoesNotCloneRootSibling) {
    Arena destination;
    AstNode* clone = nullptr;
    {
        std::string sql = "SELECT 'can''t', 27 FROM things WHERE n = 9";
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(parsed.ok());
        parsed.ast->next_sibling = make_node(parser.arena(), NodeType::NODE_UNKNOWN);
        auto result = clone_ast(parsed.ast, destination);
        ASSERT_TRUE(result.ok());
        clone = result.ast;
        ASSERT_NE(clone, nullptr);
        EXPECT_NE(clone, parsed.ast);
        EXPECT_EQ(clone->next_sibling, nullptr);
        parser.reset();
        sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit(clone, destination), "SELECT 'can''t', 27 FROM things WHERE n = 9");
}

TEST(AstUtilities, CloneRejectsCyclesAndArenaExhaustion) {
    Arena source;
    auto root = make_node(source, NodeType::NODE_EXPRESSION);
    root->first_child = root;
    Arena destination;
    EXPECT_EQ(clone_ast(root, destination).error, AstError::InvalidTree);
    root->first_child = nullptr;
    Arena tiny(8, 8);
    auto failed = clone_ast(root, tiny);
    EXPECT_EQ(failed.error, AstError::AllocationFailure);
    EXPECT_EQ(failed.ast, nullptr);
}

TEST(AstUtilities, ReplacementPreservesSiblingsAndRejectsAliasing) {
    Arena arena;
    auto root = make_node(arena, NodeType::NODE_TUPLE);
    auto a = make_node(arena, NodeType::NODE_LITERAL_INT, {"1", 1});
    auto b = make_node(arena, NodeType::NODE_LITERAL_INT, {"2", 1});
    auto c = make_node(arena, NodeType::NODE_LITERAL_INT, {"3", 1});
    root->add_child(a); root->add_child(b); root->add_child(c);
    std::string value = "42";
    auto replacement = make_owned_node(arena, NodeType::NODE_LITERAL_INT, {value.data(), 2});
    value.assign("xx");
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(replace_ast_subtree(root, b, replacement), AstError::None);
    EXPECT_EQ(emit(root, arena), "(1, 42, 3)");
    EXPECT_EQ(b->next_sibling, nullptr);
    EXPECT_EQ(replace_ast_subtree(root, a, root), AstError::InvalidReplacement);
    EXPECT_EQ(replace_ast_subtree(root, a, c), AstError::InvalidReplacement);
    EXPECT_EQ(emit(root, arena), "(1, 42, 3)");
    EXPECT_EQ(replace_ast_subtree(root, replacement, nullptr), AstError::None);
    EXPECT_EQ(emit(root, arena), "(1, 3)");
    EXPECT_EQ(replace_ast_subtree(root, b, nullptr), AstError::NotFound);
    EXPECT_EQ(replace_ast_subtree(root, root, b), AstError::None);
    EXPECT_EQ(emit(root, arena), "2");
}

TEST(AstParameterize, PreservesExistingPostgresBindingsAndOwnsExtractedText) {
    Arena arena;
    ParameterizeResult result;
    {
        Parser<Dialect::PostgreSQL> parser;
        std::string sql = "SELECT $3, 7, 'can''t', $1, 2.5, NULL, TRUE";
        auto parsed = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(parsed.ok());
        result = parameterize_ast<Dialect::PostgreSQL>(parsed.ast, arena);
        ASSERT_TRUE(result.ok()) << static_cast<int>(result.error);
        EXPECT_EQ(emit(parsed.ast, parser.arena()), sql);
        sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit(result.ast, arena), "SELECT $3, $4, $5, $1, $6, NULL, $7");
    ASSERT_EQ(result.parameters.size(), 4u);
    EXPECT_EQ(result.parameters[0].index, 4u);
    EXPECT_EQ(text(result.parameters[0].value), "7");
    EXPECT_EQ(text(result.parameters[1].source), "'can''t'");
    EXPECT_EQ(result.parameters[1].literal_type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(text(result.parameters[3].value), "TRUE");
    EXPECT_EQ(result.existing_parameters, (std::vector<uint32_t>{3, 1}));
}

TEST(AstParameterize, PreservesOrdinalsBooleanPredicatesAndTypeModifiers) {
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parse(parser, "SELECT 5, flag IS TRUE FROM t WHERE n = 9 GROUP BY 1, n + 2 ORDER BY 1, n + 3 LIMIT 4");
    ASSERT_TRUE(parsed.ok());
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(parsed.ast, arena);
    ASSERT_TRUE(result.ok()) << static_cast<int>(result.error);
    EXPECT_EQ(emit(result.ast, arena), "SELECT $1, flag IS TRUE FROM t WHERE n = $2 GROUP BY 1, n + $3 ORDER BY 1, n + $4 LIMIT $5");
    ASSERT_EQ(result.parameters.size(), 5u);
    EXPECT_EQ(text(result.parameters[0].value), "5");
    EXPECT_EQ(text(result.parameters[4].value), "4");
}

TEST(AstParameterize, WindowNumbersAreNotOrdinals) {
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parse(parser, "SELECT sum(x) OVER (PARTITION BY 2 ORDER BY 3) FROM t ORDER BY 1");
    ASSERT_TRUE(parsed.ok());
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(parsed.ast, arena);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(emit(result.ast, arena), "SELECT sum(x) OVER (PARTITION BY $1 ORDER BY $2) FROM t ORDER BY 1");
}

TEST(AstParameterize, NestedQueriesAndDml) {
    const char* inputs[] = {
        "SELECT x FROM t WHERE x IN (SELECT y FROM u WHERE z = 7) AND x = 9",
        "INSERT INTO t (x, y) VALUES (7, 'a'), (9, 'b') RETURNING 2",
        "UPDATE t SET x = 7 WHERE y = 9 RETURNING 2",
        "DELETE FROM t WHERE y = 9 RETURNING 2"
    };
    const char* outputs[] = {
        "SELECT x FROM t WHERE x IN (SELECT y FROM u WHERE z = $1) AND x = $2",
        "INSERT INTO t (x, y) VALUES ($1, $2), ($3, $4) RETURNING $5",
        "UPDATE t SET x = $1 WHERE y = $2 RETURNING $3",
        "DELETE FROM t WHERE y = $1 RETURNING $2"
    };
    for (size_t i = 0; i < 4; ++i) {
        SCOPED_TRACE(inputs[i]);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parse(parser, inputs[i]);
        ASSERT_TRUE(parsed.ok());
        Arena arena;
        auto result = parameterize_ast<Dialect::PostgreSQL>(parsed.ast, arena);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(emit(result.ast, arena), outputs[i]);
    }
}

TEST(AstParameterize, MysqlMappingCountsExistingQuestionMarks) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT ?, 7, ?, 'a'";
    auto parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok());
    Arena arena;
    auto result = parameterize_ast<Dialect::MySQL>(parsed.ast, arena);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(emit<Dialect::MySQL>(result.ast, arena), "SELECT ?, ?, ?, ?");
    ASSERT_EQ(result.parameters.size(), 2u);
    EXPECT_EQ(result.parameters[0].index, 2u);
    EXPECT_EQ(result.parameters[1].index, 4u);
    EXPECT_EQ(result.existing_parameters, (std::vector<uint32_t>{1, 3}));
}

TEST(AstParameterize, RejectsUtilitiesOpaqueContextsAndBadBindings) {
    for (const char* sql : {"SET work_mem = '8MB'", "BEGIN"}) {
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parse(parser, sql);
        ASSERT_NE(parsed.ast, nullptr);
        Arena arena;
        auto result = parameterize_ast<Dialect::PostgreSQL>(parsed.ast, arena);
        EXPECT_EQ(result.error, AstError::UnsupportedRoot);
        EXPECT_EQ(result.ast, nullptr);
        EXPECT_TRUE(result.parameters.empty());
    }
    Parser<Dialect::MySQL> mysql;
    const char* sql = "SELECT 1 INTO OUTFILE '/tmp/example'";
    auto parsed = mysql.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok());
    Arena arena;
    auto into = make_node(mysql.arena(), NodeType::NODE_INTO_CLAUSE);
    parsed.ast->add_child(into);
    EXPECT_EQ(parameterize_ast<Dialect::MySQL>(parsed.ast, arena).error, AstError::UnsupportedContext);
    Parser<Dialect::PostgreSQL> pg;
    parsed = parse(pg, "SELECT $0, 1");
    ASSERT_NE(parsed.ast, nullptr);
    EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(parsed.ast, arena).error, AstError::InvalidPlaceholder);
}

TEST(AstUtilities, NullTreesAndReplacementRootBoundary) {
    Arena arena;
    EXPECT_EQ(clone_ast(nullptr, arena).ast, nullptr);
    EXPECT_EQ(walk_ast(nullptr, [](const AstNode&, const AstVisitContext&) {
        ADD_FAILURE() << "Null traversal must not invoke visitor";
        return AstVisitAction::Continue;
    }).visited, 0u);
    AstNode* root = nullptr;
    auto one = make_node(arena, NodeType::NODE_LITERAL_INT, {"1", 1});
    auto two = make_node(arena, NodeType::NODE_LITERAL_INT, {"2", 1});
    EXPECT_EQ(replace_ast_subtree(root, one, two), AstError::NotFound);
    root = one;
    root->next_sibling = two;
    EXPECT_EQ(replace_ast_subtree(root, one, two), AstError::InvalidTree);
    EXPECT_EQ(root, one);
    EXPECT_EQ(one->next_sibling, two);
    EXPECT_EQ(two->next_sibling, nullptr);
}

TEST(AstParameterize, PreservesBinaryCastTypeSubtree) {
    Arena source;
    auto root = make_node(source, NodeType::NODE_SELECT_STMT);
    auto items = make_node(source, NodeType::NODE_SELECT_ITEM_LIST);
    auto item = make_node(source, NodeType::NODE_SELECT_ITEM);
    auto cast = make_node(source, NodeType::NODE_BINARY_OP, {"::", 2});
    auto type = make_node(source, NodeType::NODE_FUNCTION_CALL, {"numeric", 7});
    root->add_child(items); items->add_child(item); item->add_child(cast);
    cast->add_child(make_node(source, NodeType::NODE_LITERAL_INT, {"5", 1}));
    cast->add_child(type);
    type->add_child(make_node(source, NodeType::NODE_LITERAL_INT, {"10", 2}));
    type->add_child(make_node(source, NodeType::NODE_LITERAL_INT, {"2", 1}));
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(root, arena);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(emit(result.ast, arena), "SELECT $1 :: numeric(10, 2)");
    EXPECT_EQ(result.parameters.size(), 1u);
}

TEST(AstParameterize, RejectsCastFunctionsCteAndOpaqueIntervals) {
    for (const char* sql : {"SELECT CAST(1 AS integer)", "WITH q AS (SELECT 1) SELECT * FROM q", "SELECT INTERVAL '1' DAY"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parse(parser, sql);
        ASSERT_NE(parsed.ast, nullptr);
        Arena arena;
        auto result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
        EXPECT_FALSE(result.ok());
        EXPECT_EQ(result.ast, nullptr);
        EXPECT_TRUE(result.parameters.empty());
    }
}

TEST(AstParameterize, RejectsBindOverflowAndReturnsNoPartialMapping) {
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parse(parser, "SELECT $2147483647, 1");
    ASSERT_TRUE(parsed.ok());
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(parsed.ast, arena);
    EXPECT_EQ(result.error, AstError::ParameterOverflow);
    EXPECT_EQ(result.ast, nullptr);
    EXPECT_TRUE(result.parameters.empty());
    EXPECT_TRUE(result.existing_parameters.empty());
    Arena tiny(64, 64);
    result = parameterize_ast<Dialect::PostgreSQL>(parsed.ast, tiny);
    EXPECT_EQ(result.error, AstError::AllocationFailure);
    EXPECT_EQ(result.ast, nullptr);
}

TEST(AstParameterize, PreservesQuotedIdentifiersAfterOriginalInputExpires) {
    Arena arena;
    ParameterizeResult result;
    {
        Parser<Dialect::PostgreSQL> parser;
        std::string sql = "SELECT \"T Alias\".\"A\" AS \"Result Name\" FROM \"My Schema\".\"My Table\" AS \"T Alias\" WHERE \"T Alias\".\"A\" = 7";
        auto parsed = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(parsed.ok());
        ASSERT_TRUE(parsed.full_input);
        result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
        ASSERT_TRUE(result.ok());
        sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit(result.ast, arena), "SELECT \"T Alias\".\"A\" AS \"Result Name\" FROM \"My Schema\".\"My Table\" AS \"T Alias\" WHERE \"T Alias\".\"A\" = $1");
}

TEST(AstParameterize, ParseResultOverloadRejectsIncompleteInput) {
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parse(parser, "SELECT 1 unexpected trailing garbage");
    ASSERT_FALSE(parsed.full_input);
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
    EXPECT_EQ(result.error, AstError::UnsupportedContext);
    EXPECT_EQ(result.ast, nullptr);
    EXPECT_EQ(arena.bytes_used(), 0u);
    parsed = parse(parser, "SELECT 1");
    ASSERT_TRUE(parsed.full_input);
    result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(emit(result.ast, arena), "SELECT $1");
    parsed.status = ParseResult::PARTIAL;
    EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(parsed, arena).error, AstError::UnsupportedContext);
}

TEST(AstParameterize, PreservesDistinctOrdinalsAndBindsFilterAndFrameOffsets) {
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parse(parser, "SELECT DISTINCT ON (1) 5, sum(x) FILTER (WHERE x > 2) OVER (ORDER BY x ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM t ORDER BY 1");
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.full_input);
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(emit(result.ast, arena), "SELECT DISTINCT ON (1) $1, sum(x) FILTER (WHERE x > $2) OVER (ORDER BY x ROWS BETWEEN $3 PRECEDING AND CURRENT ROW) FROM t ORDER BY 1");
    ASSERT_EQ(result.parameters.size(), 3u);
    EXPECT_EQ(text(result.parameters[2].value), "3");
}

TEST(AstUtilities, ReplacementsRejectSiblingChainsAndCyclesWithoutMutation) {
    Arena arena;
    auto root = make_node(arena, NodeType::NODE_TUPLE);
    auto target = make_node(arena, NodeType::NODE_LITERAL_INT, {"1", 1});
    auto replacement = make_node(arena, NodeType::NODE_LITERAL_INT, {"2", 1});
    auto sibling = make_node(arena, NodeType::NODE_LITERAL_INT, {"3", 1});
    root->add_child(target);
    replacement->next_sibling = sibling;
    EXPECT_EQ(replace_ast_subtree(root, target, replacement), AstError::InvalidReplacement);
    EXPECT_EQ(root->first_child, target);
    replacement->next_sibling = nullptr;
    replacement->first_child = replacement;
    EXPECT_EQ(replace_ast_subtree(root, target, replacement), AstError::InvalidReplacement);
    EXPECT_EQ(root->first_child, target);
    replacement->first_child = nullptr;
    EXPECT_EQ(replace_ast_subtree(root, target, replacement, {1, 10}), AstError::LimitExceeded);
    EXPECT_EQ(root->first_child, target);
    Arena output;
    EXPECT_EQ(clone_ast(root, output, {1, 10}).error, AstError::LimitExceeded);
}

TEST(AstParameterize, PreservesDmlQuotedTargetsAndReturningAliases) {
    const char* inputs[] = {
        "UPDATE \"T\" SET \"X\" = 1 WHERE \"Y\" = 2 RETURNING \"X\" AS \"CamelCase\"",
        "INSERT INTO \"T\" (\"X\") VALUES (1) ON CONFLICT (\"X\") DO UPDATE SET \"X\" = 2 RETURNING \"X\" AS \"CamelCase\"",
        "DELETE FROM \"T\" WHERE \"X\" = 1 RETURNING \"X\" AS \"CamelCase\"",
        "INSERT INTO \"T\" (\"X\") VALUES (1) ON CONFLICT ON CONSTRAINT \"MyConstraint\" DO NOTHING"
    };
    const char* outputs[] = {
        "UPDATE \"T\" SET \"X\" = $1 WHERE \"Y\" = $2 RETURNING \"X\" AS \"CamelCase\"",
        "INSERT INTO \"T\" (\"X\") VALUES ($1) ON CONFLICT (\"X\") DO UPDATE SET \"X\" = $2 RETURNING \"X\" AS \"CamelCase\"",
        "DELETE FROM \"T\" WHERE \"X\" = $1 RETURNING \"X\" AS \"CamelCase\"",
        "INSERT INTO \"T\" (\"X\") VALUES ($1) ON CONFLICT ON CONSTRAINT \"MyConstraint\" DO NOTHING"
    };
    for (size_t i = 0; i < 4; ++i) {
        SCOPED_TRACE(inputs[i]);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parse(parser, inputs[i]);
        ASSERT_TRUE(parsed.ok());
        ASSERT_TRUE(parsed.full_input);
        Arena arena;
        auto result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(emit(result.ast, arena), outputs[i]);
    }
}

TEST(AstParameterize, PreservesMysqlQuotedDmlIdentifiers) {
    const char* inputs[] = {
        "UPDATE `T` SET `T`.`X` = 1",
        "INSERT INTO `T` (`X`) VALUES (1) ON DUPLICATE KEY UPDATE `X` = 2",
        "DELETE FROM `S`.`T` WHERE `X` = 1"
    };
    const char* outputs[] = {
        "UPDATE `T` SET `T`.`X` = ?",
        "INSERT INTO `T` (`X`) VALUES (?) ON DUPLICATE KEY UPDATE `X` = ?",
        "DELETE FROM `S`.`T` WHERE `X` = ?"
    };
    for (size_t i = 0; i < 3; ++i) {
        SCOPED_TRACE(inputs[i]);
        Parser<Dialect::MySQL> parser;
        auto parsed = parser.parse(inputs[i], std::strlen(inputs[i]));
        ASSERT_TRUE(parsed.ok());
        ASSERT_TRUE(parsed.full_input);
        Arena arena;
        auto result = parameterize_ast<Dialect::MySQL>(parsed, arena);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(emit<Dialect::MySQL>(result.ast, arena), outputs[i]);
    }
}

TEST(AstParameterize, PreservesDatetimePrecisionSyntaxConstants) {
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parse(parser, "SELECT CURRENT_TIMESTAMP(3), CURRENT_TIME(2), LOCALTIME(1), LOCALTIMESTAMP(4), 7");
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.full_input);
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(emit(result.ast, arena), "SELECT CURRENT_TIMESTAMP(3), CURRENT_TIME(2), LOCALTIME(1), LOCALTIMESTAMP(4), $1");
    ASSERT_EQ(result.parameters.size(), 1u);
    EXPECT_EQ(text(result.parameters[0].value), "7");
}

TEST(AstParameterize, RejectsTypedLiteralMisparsedAsStringAlias) {
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parse(parser, "SELECT TIMESTAMP '2020-01-01'");
    Arena arena;
    auto result = parameterize_ast<Dialect::PostgreSQL>(parsed, arena);
    EXPECT_EQ(result.error, AstError::UnsupportedContext);
    EXPECT_EQ(result.ast, nullptr);
}

TEST(AstParameterize, MysqlCommaLimitUsesCountThenOffsetAndRejectsOriginalBinds) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SELECT a FROM t LIMIT 2, 5";
    auto parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.full_input);
    Arena arena;
    auto result = parameterize_ast<Dialect::MySQL>(parsed, arena);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(emit<Dialect::MySQL>(result.ast, arena), "SELECT a FROM t LIMIT ? OFFSET ?");
    ASSERT_EQ(result.parameters.size(), 2u);
    EXPECT_EQ(text(result.parameters[0].value), "5");
    EXPECT_EQ(text(result.parameters[1].value), "2");
    for (const char* bound_sql : {"SELECT a FROM t LIMIT ?, ?", "SELECT a FROM t LIMIT ?, 5", "SELECT a FROM t LIMIT 2, ?"}) {
        SCOPED_TRACE(bound_sql);
        auto bound = parser.parse(bound_sql, std::strlen(bound_sql));
        ASSERT_TRUE(bound.ok());
        auto rejected = parameterize_ast<Dialect::MySQL>(bound, arena);
        EXPECT_EQ(rejected.error, AstError::UnsupportedContext);
        EXPECT_EQ(rejected.ast, nullptr);
        EXPECT_TRUE(rejected.existing_parameters.empty());
    }
}

TEST(AstParameterize, MysqlPrecisionAliasesStayLiteralOnlyInMysql) {
    const char* sql = "SELECT NOW(3), CURTIME(2), SYSDATE(3), UTC_TIME(2), UTC_TIMESTAMP(3), 7";
    Parser<Dialect::MySQL> mysql;
    auto parsed = mysql.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.full_input);
    Arena mysql_arena;
    auto mysql_result = parameterize_ast<Dialect::MySQL>(parsed, mysql_arena);
    ASSERT_TRUE(mysql_result.ok());
    EXPECT_EQ(emit<Dialect::MySQL>(mysql_result.ast, mysql_arena),
        "SELECT NOW(3), CURTIME(2), SYSDATE(3), UTC_TIME(2), UTC_TIMESTAMP(3), ?");
    ASSERT_EQ(mysql_result.parameters.size(), 1u);
    EXPECT_EQ(text(mysql_result.parameters[0].value), "7");

    Parser<Dialect::PostgreSQL> postgres;
    parsed = parse(postgres, sql);
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.full_input);
    Arena pg_arena;
    auto pg_result = parameterize_ast<Dialect::PostgreSQL>(parsed, pg_arena);
    ASSERT_TRUE(pg_result.ok());
    EXPECT_EQ(emit(pg_result.ast, pg_arena),
        "SELECT NOW($1), CURTIME($2), SYSDATE($3), UTC_TIME($4), UTC_TIMESTAMP($5), $6");
    EXPECT_EQ(pg_result.parameters.size(), 6u);
}

TEST(AstParameterize, QuotedPrecisionFunctionNamesRemainBindable) {
    Parser<Dialect::MySQL> mysql;
    const char* sql = "SELECT `NOW`(3), `CURRENT_TIMESTAMP`(2)";
    auto parsed = mysql.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.full_input);
    Arena mysql_arena;
    auto mysql_result = parameterize_ast<Dialect::MySQL>(parsed, mysql_arena);
    ASSERT_TRUE(mysql_result.ok());
    EXPECT_EQ(emit<Dialect::MySQL>(mysql_result.ast, mysql_arena),
        "SELECT `NOW`(?), `CURRENT_TIMESTAMP`(?)");
    EXPECT_EQ(mysql_result.parameters.size(), 2u);

    Parser<Dialect::PostgreSQL> postgres;
    parsed = parse(postgres, "SELECT \"NOW\"(3), \"CURRENT_TIMESTAMP\"(2)");
    ASSERT_TRUE(parsed.ok());
    ASSERT_TRUE(parsed.full_input);
    Arena pg_arena;
    auto pg_result = parameterize_ast<Dialect::PostgreSQL>(parsed, pg_arena);
    ASSERT_TRUE(pg_result.ok());
    EXPECT_EQ(emit(pg_result.ast, pg_arena), "SELECT \"NOW\"($1), \"CURRENT_TIMESTAMP\"($2)");
    EXPECT_EQ(pg_result.parameters.size(), 2u);
}
