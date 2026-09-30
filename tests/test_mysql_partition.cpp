#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_transform.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"

using namespace sql_parser;

namespace {
void accept_partition(const char* suffix) {
    std::string sql = std::string("CREATE TABLE db.t (id INT, region INT) ") + suffix;
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql.data(), sql.size());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    EXPECT_EQ(std::string(result.table_name.ptr, result.table_name.len), "t");
    EXPECT_EQ(std::string(result.schema_name.ptr, result.schema_name.len), "db");
    Emitter<Dialect::MySQL> emitter(parser.arena());
    emitter.emit(result.ast);
    std::string emitted(emitter.result().ptr, emitter.result().len);
    SCOPED_TRACE(emitted);
    Parser<Dialect::MySQL> again;
    auto reparsed = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(reparsed.ok());
    ASSERT_TRUE(reparsed.full_input);
    Emitter<Dialect::MySQL> second(again.arena());
    second.emit(reparsed.ast);
    EXPECT_EQ(std::string(second.result().ptr, second.result().len), emitted);
}
}

TEST(MySQLPartition, NativeMethodsCountsAndSubpartitionMethodsRoundTrip) {
    for (const char* sql : {
        "PARTITION BY HASH(id)", "PARTITION BY LINEAR HASH(id + 1) PARTITIONS 4",
        "PARTITION BY KEY()", "PARTITION BY LINEAR KEY ALGORITHM=1(id, region) PARTITIONS 4",
        "PARTITION BY KEY ALGORITHM=2(`id`) PARTITIONS 0x04",
        "PARTITION BY KEY ALGORITHM=X'01'() PARTITIONS X'04'",
        "PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        "PARTITION BY RANGE COLUMNS(id, region) (PARTITION p0 VALUES LESS THAN (10, MAXVALUE))",
        "PARTITION BY LIST(id) (PARTITION p0 VALUES IN (1, 2, NULL))",
        "PARTITION BY LIST COLUMNS(id, region) (PARTITION p0 VALUES IN ((1, 2), (3, 4)))",
        "PARTITION BY RANGE(id) SUBPARTITION BY LINEAR HASH(region) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10))",
        "PARTITION BY RANGE(id) SUBPARTITION BY KEY ALGORITHM=2(region) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (MAXVALUE))"
    }) accept_partition(sql);
}

TEST(MySQLPartition, DefinitionOptionsAndExplicitSubpartitionsRoundTrip) {
    accept_partition("PARTITION BY HASH(id) (PARTITION `select` STORAGE ENGINE='InnoDB' TABLESPACE=ts NODEGROUP=0 MAX_ROWS=18446744073709551615 MIN_ROWS=0 DATA DIRECTORY='/tmp/data' INDEX DIRECTORY='/tmp/index' COMMENT='part')");
    accept_partition("PARTITION BY HASH(id) (PARTITION p0 MAX_ROWS=0x10000000000000000 MIN_ROWS=X'010000000000000000')");
    accept_partition("PARTITION BY RANGE(id) SUBPARTITION BY KEY(region) (PARTITION p0 VALUES LESS THAN (10) ENGINE=InnoDB (SUBPARTITION s0 COMMENT='first', SUBPARTITION 's1' TABLESPACE ts), PARTITION p1 VALUES LESS THAN MAXVALUE (SUBPARTITION s2, SUBPARTITION s3))");
}

TEST(MySQLPartition, NativeBitExpressionBoundaryRoundTrips) {
    for (const char* sql : {
        "PARTITION BY HASH(id | 1)", "PARTITION BY HASH((id = 1))",
        "PARTITION BY HASH(ABS(id) + 1)", "PARTITION BY HASH((NOT id))",
        "PARTITION BY HASH((id || 2))",
        "PARTITION BY HASH(ARRAY)", "PARTITION BY HASH(ARRAY + 1)",
        "PARTITION BY HASH(`ALL`)", "PARTITION BY HASH(t.ALL)",
        "PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (1 + 2))",
        "PARTITION BY LIST COLUMNS(id, region) (PARTITION p0 VALUES IN (((1 = 1), 2), (3, 4)))"
    }) accept_partition(sql);
}

TEST(MySQLPartition, RejectsMalformedNativePartitionGrammar) {
    for (const char* suffix : {
        "PARTITION", "PARTITION BY", "PARTITION BY HASH", "PARTITION BY HASH()",
        "PARTITION BY HASH(id, region)", "PARTITION BY HASH(id +)",
        "PARTITION BY LINEAR RANGE(id)", "PARTITION BY RANGE COLUMNS()",
        "PARTITION BY KEY(id,)", "PARTITION BY KEY(1)", "PARTITION BY KEY('id')",
        "PARTITION BY KEY ALGORITHM 1(id)", "PARTITION BY KEY ALGORITHM=0(id)",
        "PARTITION BY KEY ALGORITHM=3(id)", "PARTITION BY KEY ALGORITHM=1.0(id)",
        "PARTITION BY KEY ALGORITHM=1e0(id)", "PARTITION BY KEY ALGORITHM=-1(id)",
        "PARTITION BY KEY ALGORITHM=0x10000000000000001(id)",
        "PARTITION BY HASH(id) PARTITIONS", "PARTITION BY HASH(id) PARTITIONS 0",
        "PARTITION BY HASH(id) PARTITIONS -1", "PARTITION BY HASH(id) PARTITIONS 2.0",
        "PARTITION BY HASH(id) PARTITIONS 2e0", "PARTITION BY HASH(id) PARTITIONS 18446744073709551616",
        "PARTITION BY HASH(id) SUBPARTITION BY RANGE(region)",
        "PARTITION BY HASH(id) SUBPARTITION BY KEY()", "PARTITION BY HASH(id) SUBPARTITIONS 2",
        "PARTITION BY HASH(id) SUBPARTITION BY HASH(region) SUBPARTITIONS 0",
        "PARTITION BY HASH(id) ()", "PARTITION BY HASH(id) (PARTITION)",
        "PARTITION BY HASH(id) (PARTITION 'p0')", "PARTITION BY HASH(id) (PARTITION p0,)",
        "PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS 1)",
        "PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN ())",
        "PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (1,))",
        "PARTITION BY LIST(id) (PARTITION p0 VALUES IN ())",
        "PARTITION BY LIST(id) (PARTITION p0 VALUES IN (1,))",
        "PARTITION BY LIST COLUMNS(id, region) (PARTITION p0 VALUES IN ((1, 2),))",
        "PARTITION BY HASH(id) (PARTITION p0 ENGINE=)",
        "PARTITION BY HASH(id) (PARTITION p0 COMMENT=1)",
        "PARTITION BY HASH(id) (PARTITION p0 MAX_ROWS=1.1)",
        "PARTITION BY HASH(id) (PARTITION p0 NODEGROUP=-1)",
        "PARTITION BY HASH(id) (PARTITION p0 DATA='/tmp')",
        "PARTITION BY HASH(id) (PARTITION p0 (SUBPARTITION s0,))",
        "PARTITION BY HASH(id) (PARTITION p0 (SUBPARTITION s0 VALUES IN (1)))",
        "PARTITION BY HASH(id = 1)", "PARTITION BY HASH(id AND region)",
        "PARTITION BY HASH(NOT id)", "PARTITION BY HASH(id + NOT region)",
        "PARTITION BY HASH(id || 2)",
        "PARTITION BY LIST(id) (PARTITION p0 VALUES IN (1 || 2))",
        "PARTITION BY HASH(DEFAULT)", "PARTITION BY HASH(t.*)",
        "PARTITION BY HASH(INTERVAL 1 garbage)",
        "PARTITION BY HASH(id + INTERVAL 1 garbage)",
        "PARTITION BY HASH(ABS(INTERVAL 1 garbage))",
        "PARTITION BY HASH((INTERVAL 1 garbage))",
        "PARTITION BY HASH(ALL(SELECT nonsense tokens))",
        "PARTITION BY HASH(ALL)", "PARTITION BY HASH(ABS(ALL))",
        "PARTITION BY HASH(ARRAY[1,2])",
        "PARTITION BY HASH((id)[1])", "PARTITION BY HASH((id).x)",
        "PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (INTERVAL 1 garbage))",
        "PARTITION BY LIST(id) (PARTITION p0 VALUES IN (1 = 1))",
        "PARTITION BY HASH(id) invented tail"
    }) {
        std::string sql = std::string("CREATE TABLE t (id INT, region INT) ") + suffix;
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLPartition, StructuredOperandsRemainEditableOwnedAndGuarded) {
    Arena owned;
    AstNode* copy = nullptr;
    {
        std::string sql = "CREATE TABLE db.t (id INT, region INT) PARTITION BY RANGE(id + 1) "
            "SUBPARTITION BY KEY(region) (PARTITION `p0` VALUES LESS THAN (10) "
            "(SUBPARTITION s0 COMMENT='first', SUBPARTITION s1))";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        const AstNode* partition = nullptr;
        size_t clauses = 0, definitions = 0, additions = 0, bounds = 0;
        auto walk = walk_ast(result.ast, [&](const AstNode& n, const AstVisitContext&) {
            if (n.type == NodeType::NODE_MYSQL_PARTITION_CLAUSE) { ++clauses; partition = &n; }
            definitions += n.type == NodeType::NODE_MYSQL_PARTITION_DEF;
            additions += n.type == NodeType::NODE_BINARY_OP && n.value().equals_ci("+", 1);
            if (n.type == NodeType::NODE_LITERAL_INT && n.value().equals_ci("10", 2)) {
                auto* bound = const_cast<AstNode*>(&n);
                bound->set_value({"20", 2}); bound->set_source({}); ++bounds;
            }
            if (n.type == NodeType::NODE_IDENTIFIER && n.value().equals_ci("p0", 2)) {
                auto* name = const_cast<AstNode*>(&n);
                name->set_value({"renamed", 7}); name->set_source({});
            }
            EXPECT_NE(n.type, NodeType::NODE_STATEMENT);
            return AstVisitAction::Continue;
        });
        EXPECT_EQ(walk.status, AstWalkStatus::Completed);
        EXPECT_EQ(clauses, 1u); EXPECT_EQ(definitions, 3u);
        EXPECT_EQ(additions, 1u); EXPECT_EQ(bounds, 1u);
        ASSERT_NE(partition, nullptr);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(partition));
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
        Arena parameters;
        EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, parameters).ok());
        auto cloned = clone_ast(result.ast, owned);
        ASSERT_TRUE(cloned.ok()); copy = cloned.ast;
        parser.reset();
        sql.assign(sql.size(), '!');
    }
    Emitter<Dialect::MySQL> emitter(owned);
    emitter.emit(copy);
    std::string emitted(emitter.result().ptr, emitter.result().len);
    EXPECT_NE(emitted.find("PARTITION `renamed` VALUES LESS THAN (20)"), std::string::npos);
    EXPECT_NE(emitted.find("SUBPARTITION s0 COMMENT 'first'"), std::string::npos);
    EXPECT_NE(emitted.find("RANGE (id + 1)"), std::string::npos);
    Parser<Dialect::MySQL> again;
    auto result = again.parse(emitted.data(), emitted.size());
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(result.full_input);
}
