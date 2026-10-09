#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include <cstring>
#include <string>
using namespace sql_parser;
TEST(MySQLPartitionAlter, NativeActionsRoundTrip) {
    for(const char* sql : {
        "ALTER TABLE t PARTITION BY HASH(id) PARTITIONS 4",
        "ALTER TABLE t ADD COLUMN n INT PARTITION BY KEY(id) PARTITIONS 2",
        "ALTER TABLE t ALGORITHM=COPY, LOCK=SHARED PARTITION BY HASH(id)",
        "ALTER TABLE t REMOVE PARTITIONING",
        "ALTER TABLE t ADD COLUMN n INT REMOVE PARTITIONING",
        "ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))",
        "ALTER TABLE t ADD PARTITION PARTITIONS 2",
        "ALTER TABLE t ADD PARTITION LOCAL PARTITIONS 0x2",
        "ALTER TABLE t ADD PARTITION",
        "ALTER TABLE t DROP PARTITION p0, p1",
        "ALTER TABLE t TRUNCATE PARTITION ALL",
        "ALTER TABLE t TRUNCATE PARTITION `p0`, p1",
        "ALTER TABLE t COALESCE PARTITION NO_WRITE_TO_BINLOG 2",
        "ALTER TABLE t REORGANIZE PARTITION",
        "ALTER TABLE t REORGANIZE PARTITION p0, p1 INTO (PARTITION p2 VALUES LESS THAN (20))",
        "ALTER TABLE t REBUILD PARTITION LOCAL ALL",
        "ALTER TABLE t OPTIMIZE PARTITION p0, p1",
        "ALTER TABLE t ANALYZE PARTITION NO_WRITE_TO_BINLOG ALL",
        "ALTER TABLE t CHECK PARTITION p0 QUICK FOR UPGRADE",
        "ALTER TABLE t REPAIR PARTITION LOCAL ALL EXTENDED",
        "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE db.u WITHOUT VALIDATION",
        "ALTER TABLE t ALGORITHM=INPLACE, LOCK=NONE, ADD PARTITION PARTITIONS 1",
        "ALTER TABLE t WITH VALIDATION, EXCHANGE PARTITION p0 WITH TABLE u"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql));
        ASSERT_TRUE(r.ok() && r.full_input); ASSERT_NE(r.ast,nullptr);
        EXPECT_EQ(std::string(r.table_name.ptr,r.table_name.len),"t");
        Emitter<Dialect::MySQL> e(p.arena()); e.emit(r.ast);
        std::string out(e.result().ptr,e.result().len); SCOPED_TRACE(out);
        Parser<Dialect::MySQL> q; auto again=q.parse(out.data(),out.size());
        ASSERT_TRUE(again.ok() && again.full_input);
        Emitter<Dialect::MySQL> f(q.arena()); f.emit(again.ast);
        EXPECT_EQ(std::string(f.result().ptr,f.result().len),out);
    }
}
TEST(MySQLPartitionAlter, RejectsMalformedActionsAndIllegalCombinations) {
    for(const char* sql : {
        "ALTER TABLE t ADD PARTITION ()", "ALTER TABLE t DROP PARTITION",
        "ALTER TABLE t DROP PARTITION ALL", "ALTER TABLE t DROP PARTITION 'p0'",
        "ALTER TABLE t DROP PARTITION p0,", "ALTER TABLE t TRUNCATE PARTITION",
        "ALTER TABLE t TRUNCATE PARTITION ALL, p0", "ALTER TABLE t COALESCE PARTITION -1",
        "ALTER TABLE t COALESCE PARTITION 1.5", "ALTER TABLE t ADD PARTITION PARTITIONS 1e2",
        "ALTER TABLE t REORGANIZE PARTITION p0", "ALTER TABLE t REORGANIZE PARTITION ALL INTO (PARTITION p1)",
        "ALTER TABLE t REORGANIZE PARTITION p0 INTO ()", "ALTER TABLE t EXCHANGE PARTITION p0 WITH u",
        "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE u WITH", "ALTER TABLE t REMOVE PARTITIONING, ADD n INT",
        "ALTER TABLE t ADD n INT, REMOVE PARTITIONING", "ALTER TABLE t ADD n INT, PARTITION BY HASH(n)",
        "ALTER TABLE t ADD n INT, ADD PARTITION PARTITIONS 2", "ALTER TABLE t ADD PARTITION PARTITIONS 2, ADD n INT",
        "ALTER TABLE t ADD PARTITION PARTITIONS 2, ALGORITHM=COPY", "ALTER TABLE t DROP PARTITION p0, ADD n INT",
        "ALTER TABLE t CHECK PARTITION LOCAL p0", "ALTER TABLE t REBUILD PARTITION ALL QUICK",
        "ALTER TABLE t CHECK PARTITION p0 FOR", "ALTER TABLE t REPAIR PARTITION ALL CHANGED"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input && r.ast);
    }
}

#include "sql_parser/ast_transform.h"
#include "sql_engine/plan_builder.h"
TEST(MySQLPartitionAlter, ActionsExposeOwnedOperandsAndPreserveTargetMetadata) {
    Arena owned;
    AstNode* cloned = nullptr;
    {
        std::string sql = "ALTER TABLE dst.t EXCHANGE PARTITION p0 WITH TABLE src.u WITHOUT VALIDATION";
        Parser<Dialect::MySQL> p; auto r=p.parse(sql.data(),sql.size());
        ASSERT_TRUE(r.ok() && r.full_input);
        EXPECT_EQ(std::string(r.table_name.ptr,r.table_name.len),"t");
        EXPECT_EQ(std::string(r.schema_name.ptr,r.schema_name.len),"dst");
        size_t count = 0;
        auto walk=walk_ast(r.ast,[&](const AstNode& n,const AstVisitContext&) {
            if(n.type==NodeType::NODE_MYSQL_PARTITION_ACTION) {
                ++count;
                EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(&n));
            }
            return AstVisitAction::Continue;
        });
        EXPECT_EQ(walk.status,AstWalkStatus::Completed); EXPECT_EQ(count,1u);
        auto copy=clone_ast(r.ast,owned); ASSERT_TRUE(copy.ok()); cloned=copy.ast;
        p.reset(); sql.assign(sql.size(),'!');
    }
    Emitter<Dialect::MySQL> emitter(owned); emitter.emit(cloned);
    EXPECT_EQ(std::string(emitter.result().ptr,emitter.result().len),
              "ALTER TABLE dst.t EXCHANGE PARTITION p0 WITH TABLE src.u WITHOUT VALIDATION");
}

TEST(MySQLPartitionAlter, ArrayKeywordRemainsANativeIdentifier) {
    const char* sql = "ALTER TABLE t PARTITION BY HASH (ARRAY + 1)";
    Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql));
    ASSERT_TRUE(r.ok() && r.full_input);
    Emitter<Dialect::MySQL> e(p.arena()); e.emit(r.ast);
    EXPECT_EQ(std::string(e.result().ptr,e.result().len),sql);
}
