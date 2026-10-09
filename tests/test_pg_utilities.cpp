#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

TEST(PgUtilities, TransactionsHaveCompleteRoundTripAst) {
    const char* queries[] = {
        "BEGIN",
        "BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE",
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE NOT DEFERRABLE",
        "COMMIT AND CHAIN",
        "ROLLBACK AND NO CHAIN",
        "SAVEPOINT \"My Point\"",
        "ROLLBACK TO SAVEPOINT \"My Point\"",
        "RELEASE SAVEPOINT \"My Point\"",
        "PREPARE TRANSACTION 'job-1'",
        "COMMIT PREPARED 'job-1'",
        "ROLLBACK PREPARED 'job-1'",
    };
    Parser<Dialect::PostgreSQL> parser;
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        ASSERT_TRUE(result.full_input);
        ASSERT_NE(result.ast, nullptr);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto emitted = emitter.result();
        std::string copy(emitted.ptr, emitted.len);
        SCOPED_TRACE(copy);
        auto again = parser.parse(copy.data(), copy.size());
        EXPECT_EQ(again.status, ParseResult::OK);
        EXPECT_TRUE(again.full_input);
    }
}

TEST(PgUtilities, CopyHasStructuredTargetAndOptions) {
    const char* queries[] = {
        "COPY public.users (id, name) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')",
        "COPY \"My Schema\".\"My Table\" TO STDOUT WITH (FORMAT binary)",
        "COPY (SELECT id FROM users WHERE id > 10) TO STDOUT WITH (FORMAT csv)",
        "COPY users FROM '/tmp/users.csv' WITH (FORMAT csv, NULL 'NULL', QUOTE '\"', ESCAPE '\"')",
        "COPY users TO PROGRAM 'cat >/tmp/users.csv' WITH (FORMAT csv, FORCE_QUOTE (name))",
        "COPY users FROM STDIN WHERE id > 0",
        "COPY users FROM STDIN WITH (FORMAT csv, HEADER ON, FORCE_NULL *, ON_ERROR ignore, LOG_VERBOSITY verbose)",
        "COPY users FROM E'/tmp/a\\\\b' WITH (NULL $$NULL$$)",
    };
    Parser<Dialect::PostgreSQL> parser;
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        ASSERT_TRUE(result.full_input);
        ASSERT_NE(result.ast, nullptr);
        ASSERT_NE(result.ast->first_child, nullptr);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto emitted = emitter.result();
        std::string copy(emitted.ptr, emitted.len);
        SCOPED_TRACE(copy);
        auto again = parser.parse(copy.data(), copy.size());
        EXPECT_EQ(again.status, ParseResult::OK);
        EXPECT_TRUE(again.full_input);
        ASSERT_NE(again.ast, nullptr);
    }
}

TEST(PgUtilities, RejectMalformedUtilityCommands) {
    const char* queries[] = {
        "BEGIN ISOLATION LEVEL", "BEGIN READ", "BEGIN NOT", "BEGIN,",
        "START", "COMMIT AND", "ROLLBACK TO", "SAVEPOINT", "RELEASE SAVEPOINT",
        "PREPARE TRANSACTION", "COMMIT PREPARED 12",
        "COMMIT WORK PREPARED 'job-1'", "ROLLBACK TRANSACTION PREPARED 'job-1'",
        "COPY", "COPY users FROM", "COPY users TO STDIN", "COPY users FROM STDOUT",
        "COPY (SELECT 1) FROM STDIN", "COPY users FROM STDIN WITH ()",
        "COPY users FROM STDIN WITH (FORMAT)", "COPY users FROM STDIN WITH (HEADER,)",
        "COPY users FROM STDIN WITH (FORCE_NULL ())",
        "COPY (SELECT 1 +) TO STDOUT",
        "COPY users FROM STDIN WHERE x = (SELECT 1 +)",
    };
    Parser<Dialect::PostgreSQL> parser;
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
}

TEST(PgUtilities, PreservesQuotedCopyOptionIdentifiers) {
    const std::string sql = "COPY users FROM STDIN WITH (ENCODING \"UTF-8\")";
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parser.parse(sql.data(), sql.size());
    ASSERT_EQ(parsed.status, ParseResult::OK);
    ASSERT_TRUE(parsed.full_input);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(parsed.ast);
    auto emitted = emitter.result();
    EXPECT_EQ(std::string(emitted.ptr, emitted.len), sql);
}
