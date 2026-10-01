#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_walk.h"

using namespace sql_parser;

TEST(PgSessionCommands, CursorCommandsRoundTrip) {
    for (const char* sql : {
        "DECLARE c CURSOR FOR SELECT 1", "DECLARE c CURSOR FOR SELECT LEFT(a,10), RIGHT(a,2) FROM t", "DECLARE \"Mixed Cursor\" BINARY INSENSITIVE NO SCROLL CURSOR WITH HOLD FOR SELECT id FROM t ORDER BY id",
        "DECLARE c ASENSITIVE SCROLL CURSOR WITHOUT HOLD FOR VALUES (1), (2)",
        "DECLARE c SCROLL SCROLL NO SCROLL CURSOR FOR WITH x AS (SELECT 1) SELECT * FROM x",
        "DECLARE c CURSOR FOR (SELECT 1 UNION SELECT 2)",
        "FETCH c", "FETCH next", "FETCH forward", "FETCH relative", "FETCH backward", "FETCH FROM c", "FETCH IN c", "FETCH NEXT FROM c", "FETCH PRIOR c",
        "FETCH FIRST IN c", "FETCH LAST c", "FETCH ABSOLUTE -3 FROM c", "FETCH RELATIVE +2 c",
        "FETCH 5 c", "FETCH 1\"c\"", "FETCH 0x10 c", "FETCH 0o10 c", "FETCH 0b10 c", "FETCH 2_000 c", "FETCH 0x_7fff_ffff c", "FETCH -2 c", "FETCH ALL c", "FETCH FORWARD c", "FETCH FORWARD ALL FROM c",
        "FETCH BACKWARD -2 c", "FETCH BACKWARD ALL IN c", "MOVE NEXT c", "MOVE 10 FROM c",
        "MOVE BACKWARD ALL c", "CLOSE c", "CLOSE ALL", "CLOSE \"all\""
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok() && result.full_input);
        ASSERT_NE(result.ast, nullptr);
        EXPECT_NE(result.ast->type, NodeType::NODE_STATEMENT);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
        auto out = emitter.result(); std::string emitted(out.ptr, out.len);
        SCOPED_TRACE(emitted);
        auto again = parser.parse(emitted.data(), emitted.size());
        EXPECT_TRUE(again.ok() && again.full_input);
    }
}

TEST(PgSessionCommands, PreparedStatementsRoundTrip) {
    for (const char* sql : {
        "PREPARE p AS SELECT 1", "PREPARE transaction AS SELECT 1", "PREPARE transaction (int) AS SELECT $1", "PREPARE \"Plan Name\" (integer, numeric(8,2), text[]) AS SELECT $1, $2, $3",
        "PREPARE p AS INSERT INTO t VALUES ($1) RETURNING *", "PREPARE p AS UPDATE t SET x = $1 RETURNING x",
        "PREPARE p AS DELETE FROM t WHERE x = $1", "PREPARE p AS WITH x AS (SELECT 1) SELECT * FROM x",
        "PREPARE p AS MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
        "EXECUTE p", "EXECUTE \"Plan Name\" (1 + 2, 'value', ARRAY[1,2])",
        "EXECUTE p ((SELECT 1), NULL, $1::integer)", "DEALLOCATE p", "DEALLOCATE PREPARE", "DEALLOCATE PREPARE p",
        "DEALLOCATE ALL", "DEALLOCATE PREPARE ALL", "DEALLOCATE \"all\""
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok() && result.full_input);
        ASSERT_NE(result.ast, nullptr); EXPECT_NE(result.ast->type, NodeType::NODE_STATEMENT);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
        auto out = emitter.result(); std::string emitted(out.ptr, out.len); SCOPED_TRACE(emitted);
        auto again = parser.parse(emitted.data(), emitted.size()); EXPECT_TRUE(again.ok() && again.full_input);
    }
}

TEST(PgSessionCommands, NotificationAndMaintenanceCommandsRoundTrip) {
    for (const char* sql : {"LISTEN channel", "LISTEN \"Mixed Channel\"", "NOTIFY channel", "NOTIFY channel, 'payload'",
        "NOTIFY channel, E'escaped\\ntext'", "UNLISTEN channel", "UNLISTEN *", "DISCARD ALL", "DISCARD PLANS",
        "DISCARD TEMP", "DISCARD TEMPORARY", "DISCARD SEQUENCES", "CHECKPOINT"}) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql)); ASSERT_TRUE(result.ok() && result.full_input);
        ASSERT_NE(result.ast, nullptr); EXPECT_NE(result.ast->type, NodeType::NODE_STATEMENT);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
        auto out = emitter.result(); std::string emitted(out.ptr, out.len); SCOPED_TRACE(emitted);
        auto again = parser.parse(emitted.data(), emitted.size()); EXPECT_TRUE(again.ok() && again.full_input);
    }
}

TEST(PgSessionCommands, RejectsMalformedCommands) {
    for (const char* sql : {
        "DECLARE", "DECLARE c FOR SELECT 1", "DECLARE c CURSOR SELECT 1", "DECLARE c NO CURSOR FOR SELECT 1",
        "DECLARE c CURSOR WITH FOR SELECT 1", "DECLARE c CURSOR FOR DELETE FROM t", "DECLARE c CURSOR FOR",
        "FETCH", "FETCH NEXT FROM", "FETCH ABSOLUTE c", "FETCH RELATIVE ALL c", "FETCH 1.5 c", "FETCH 2147483648 c", "FETCH 0x80000000 c", "FETCH 2__000 c", "FETCH 0x c", "FETCH 0b2 c", "FETCH 1c", "FETCH 1FROM c", "FETCH 1IN c", "FETCH 1é", "FETCH 1é c", "FETCH -2147483648 c", "FETCH NEXT 1 c",
        "FETCH ALL", "FETCH FORWARD ALL", "FETCH FORWARD 1.5 c", "FETCH FROM c garbage", "MOVE BACKWARD FROM",
        "CLOSE", "CLOSE c garbage", "CLOSE a.b", "PREPARE p () AS SELECT 1", "PREPARE p (int,) AS SELECT 1",
        "PREPARE p AS", "PREPARE p FROM 'SELECT 1'", "PREPARE p AS CREATE TABLE t (x int)",
        "EXECUTE", "EXECUTE p ()", "EXECUTE p (1,)", "EXECUTE p USING @x", "EXECUTE p (*)",
        "DEALLOCATE", "DEALLOCATE PREPARE ALL garbage", "DEALLOCATE ALL garbage", "LISTEN", "LISTEN a.b",
        "NOTIFY channel, NULL", "NOTIFY channel, 1", "NOTIFY channel,", "UNLISTEN", "UNLISTEN ALL",
        "DISCARD", "DISCARD TABLES", "CHECKPOINT garbage"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql)); EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(PgSessionCommands, QueryBodiesAndOperandsRemainTraversable) {
    const char* sql = "PREPARE \"Plan\" (integer) AS SELECT $1 + 2 FROM public.t";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    EXPECT_EQ(result.stmt_type, StmtType::PREPARE);
    ASSERT_EQ(result.ast->type, NodeType::NODE_PG_COMMAND_STMT);
    auto* name = result.ast->first_child;
    ASSERT_NE(name, nullptr); EXPECT_EQ(name->type, NodeType::NODE_IDENTIFIER);
    EXPECT_TRUE(name->flags & FLAG_IDENT_DELIMITED);
    ASSERT_NE(name->next_sibling, nullptr);
    EXPECT_EQ(name->next_sibling->type, NodeType::NODE_PG_DDL_LIST);
    ASSERT_NE(name->next_sibling->first_child, nullptr);
    EXPECT_EQ(name->next_sibling->first_child->type, NodeType::NODE_TYPE_NAME);
    bool query = false, operand = false;
    walk_ast(result.ast, [&](const AstNode& node, const AstVisitContext&) {
        query |= node.type == NodeType::NODE_SELECT_STMT;
        operand |= node.type == NodeType::NODE_BINARY_OP;
        return AstVisitAction::Continue;
    });
    EXPECT_TRUE(query); EXPECT_TRUE(operand);
}

TEST(PgSessionCommands, AllocationFailureCannotDropCommandOperands) {
    for (const char* sql : {"DECLARE c BINARY NO SCROLL CURSOR WITH HOLD FOR SELECT 1 + 2",
            "PREPARE p (integer, text[]) AS SELECT $1, $2", "EXECUTE p (1 + 2, 'text', ARRAY[1,2])",
            "FETCH ABSOLUTE -2 FROM c", "NOTIFY channel, 'payload'", "DEALLOCATE PREPARE ALL",
            "CHECKPOINT"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> baseline;
        auto complete = baseline.parse(sql, std::strlen(sql)); ASSERT_TRUE(complete.ok() && complete.full_input);
        size_t expected = 0;
        walk_ast(complete.ast, [&](const AstNode&, const AstVisitContext&) { ++expected; return AstVisitAction::Continue; });
        bool saw_error = false, saw_success = false;
        for (size_t capacity = 48; capacity <= 2048; capacity += 48) {
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size = capacity; config.arena_max_size = capacity;
            Parser<Dialect::PostgreSQL> parser(config);
            auto result = parser.parse(sql, std::strlen(sql));
            if (result.ok() && result.full_input) {
                saw_success = true; size_t actual = 0;
                walk_ast(result.ast, [&](const AstNode&, const AstVisitContext&) { ++actual; return AstVisitAction::Continue; });
                EXPECT_EQ(actual, expected);
            } else saw_error = true;
        }
        EXPECT_TRUE(saw_success);
        if (expected > 1) { EXPECT_TRUE(saw_error); }
    }
}
