// SET TRANSACTION / SET SESSION CHARACTERISTICS AS TRANSACTION, plus the
// PostgreSQL SET forms whose emitters used to drop content.
#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include <cstring>
#include <string>
#include <vector>

using namespace sql_parser;

namespace {

struct RoundTrip {
    const char* sql;
    const char* normal;
    const char* digest;  // nullptr: same as normal
};

template <Dialect D>
std::string emit(Parser<D>& parser, const AstNode* ast, EmitMode mode) {
    Emitter<D> emitter(parser.arena(), mode);
    emitter.emit(ast);
    StringRef out = emitter.result();
    return std::string(out.ptr, out.len);
}

template <Dialect D>
void expect_round_trip(const RoundTrip& c) {
    SCOPED_TRACE(c.sql);
    Parser<D> parser;
    auto r = parser.parse(c.sql, std::strlen(c.sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    EXPECT_TRUE(r.full_input);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    ASSERT_NE(r.ast, nullptr);
    const std::string normal = emit(parser, r.ast, EmitMode::NORMAL);
    EXPECT_EQ(normal, c.normal);
    EXPECT_EQ(emit(parser, r.ast, EmitMode::DIGEST), c.digest ? c.digest : c.normal);

    // The canonical output parses back to itself.
    Parser<D> again;
    auto r2 = again.parse(normal.c_str(), normal.size());
    ASSERT_EQ(r2.status, ParseResult::OK);
    EXPECT_TRUE(r2.full_input);
    EXPECT_EQ(emit(again, r2.ast, EmitMode::NORMAL), normal);
}

template <Dialect D>
void expect_rejected(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<D> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

template <Dialect D>
const AstNode* parse_txn(Parser<D>& parser, const char* sql) {
    auto r = parser.parse(sql, std::strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_TRUE(r.full_input);
    if (!r.ast || !r.ast->first_child) return nullptr;
    EXPECT_EQ(r.ast->first_child->next_sibling, nullptr);
    return r.ast->first_child;
}

std::string value(const AstNode* node) {
    return std::string(node->value_ptr, node->value_len);
}

}  // namespace

// ---- MySQL --------------------------------------------------------------

TEST(MySQLSetTransaction, ScopesAreKept) {
    const RoundTrip cases[] = {
        {"SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
         "SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED", nullptr},
        {"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
         "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", nullptr},
        {"SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
         "SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED", nullptr},
        {"SET PERSIST TRANSACTION ISOLATION LEVEL SERIALIZABLE",
         "SET PERSIST TRANSACTION ISOLATION LEVEL SERIALIZABLE", nullptr},
        {"SET PERSIST_ONLY TRANSACTION READ WRITE",
         "SET PERSIST_ONLY TRANSACTION READ WRITE", nullptr},
        {"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
         "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", nullptr},
        {"set global transaction read only", "SET GLOBAL TRANSACTION READ ONLY", nullptr},
        {"SET persist_only TRANSACTION READ ONLY", "SET PERSIST_ONLY TRANSACTION READ ONLY", nullptr},
        {"SET TRANSACTION READ WRITE;", "SET TRANSACTION READ WRITE", nullptr},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::MySQL>(c);
}

TEST(MySQLSetTransaction, AccessModeIsNotAnIsolationLevel) {
    const RoundTrip cases[] = {
        {"SET SESSION TRANSACTION READ  ONLY", "SET SESSION TRANSACTION READ ONLY", nullptr},
        {"SET SESSION TRANSACTION READ\n\tWRITE", "SET SESSION TRANSACTION READ WRITE", nullptr},
        {"SET TRANSACTION READ /* c */ ONLY", "SET TRANSACTION READ ONLY", nullptr},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::MySQL>(c);
}

TEST(MySQLSetTransaction, WhitespaceCaseAndCommentsAreNormalized) {
    const RoundTrip cases[] = {
        {"SET session transaction isolation level read   committed",
         "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", nullptr},
        {"SET TRANSACTION ISOLATION /*c*/ LEVEL REPEATABLE\nREAD",
         "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", nullptr},
        {"SET TRANSACTION ISOLATION LEVEL READ -- c\n UNCOMMITTED",
         "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", nullptr},
        {"SET\tGLOBAL\nTRANSACTION   ISOLATION LEVEL Serializable",
         "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE", nullptr},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::MySQL>(c);
}

TEST(MySQLSetTransaction, AllCharacteristicsInOrder) {
    const RoundTrip cases[] = {
        {"SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
         "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY", nullptr},
        {"SET SESSION TRANSACTION read write ,isolation level serializable",
         "SET SESSION TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE", nullptr},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::MySQL>(c);
}

TEST(MySQLSetTransaction, Ast) {
    Parser<Dialect::MySQL> parser;
    const AstNode* txn = parse_txn(parser,
        "SET global TRANSACTION isolation  level read committed, read only");
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->type, NodeType::NODE_SET_TRANSACTION);
    EXPECT_EQ(txn->flags & FLAG_SET_TRANSACTION_CHARACTERISTICS, 0);
    const AstNode* scope = txn->first_child;
    ASSERT_NE(scope, nullptr);
    EXPECT_EQ(scope->type, NodeType::NODE_IDENTIFIER);
    EXPECT_EQ(value(scope), "GLOBAL");
    EXPECT_EQ(std::string(scope->source_ptr, scope->source_len), "global");
    const AstNode* level = scope->next_sibling;
    ASSERT_NE(level, nullptr);
    EXPECT_EQ(level->type, NodeType::NODE_TRANSACTION_OPTION);
    EXPECT_EQ(value(level), "ISOLATION LEVEL READ COMMITTED");
    EXPECT_EQ(std::string(level->source_ptr, level->source_len),
              "isolation  level read committed");
    const AstNode* access = level->next_sibling;
    ASSERT_NE(access, nullptr);
    EXPECT_EQ(access->type, NodeType::NODE_TRANSACTION_OPTION);
    EXPECT_EQ(value(access), "READ ONLY");
    EXPECT_EQ(access->next_sibling, nullptr);

    // No scope: the first child is already a characteristic.
    txn = parse_txn(parser, "SET TRANSACTION READ WRITE");
    ASSERT_NE(txn, nullptr);
    ASSERT_NE(txn->first_child, nullptr);
    EXPECT_EQ(txn->first_child->type, NodeType::NODE_TRANSACTION_OPTION);
    EXPECT_EQ(value(txn->first_child), "READ WRITE");
}

TEST(MySQLSetTransaction, InvalidFormsRejected) {
    const char* queries[] = {
        "SET TRANSACTION",
        "SET GLOBAL TRANSACTION",
        "SET TRANSACTION READ ONLY,",
        "SET TRANSACTION , READ ONLY",
        "SET TRANSACTION READ ONLY,, ISOLATION LEVEL SERIALIZABLE",
        "SET TRANSACTION ISOLATION LEVEL",
        "SET TRANSACTION ISOLATION LEVEL READ",
        "SET TRANSACTION ISOLATION SERIALIZABLE",
        "SET TRANSACTION ISOLATION LEVEL `SERIALIZABLE`",
        "SET TRANSACTION ISOLATION LEVEL 'SERIALIZABLE'",
        "SET TRANSACTION READ",
        "SET TRANSACTION READ COMMITTED",
        // Whitespace-separated characteristics and repeats are PostgreSQL only.
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY",
        "SET TRANSACTION READ ONLY, READ WRITE",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, ISOLATION LEVEL SERIALIZABLE",
        "SET TRANSACTION DEFERRABLE",
        "SET TRANSACTION NOT DEFERRABLE",
        "SET TRANSACTION SNAPSHOT 'x'",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "SET TRANSACTION READ ONLY junk",
    };
    for (const char* sql : queries) expect_rejected<Dialect::MySQL>(sql);
}

// MySQL 8.4, 5.7 and MariaDB 10.11 reject SET TRANSACTION combined with any
// other SET item, in either order. A partial OK parse would let a consumer act
// on half of the statement.
TEST(MySQLSetTransaction, MixedWithOtherItemsRejected) {
    const char* queries[] = {
        "SET SESSION TRANSACTION READ ONLY, sql_mode='TRADITIONAL'",
        "SET sql_mode='TRADITIONAL', SESSION TRANSACTION READ ONLY",
        "SET TRANSACTION READ ONLY, sql_mode='TRADITIONAL'",
        "SET sql_mode='TRADITIONAL', TRANSACTION READ ONLY",
        "SET GLOBAL sql_mode='x', GLOBAL TRANSACTION READ ONLY",
        "SET @@SESSION.x=1, TRANSACTION READ ONLY",
        "SET @a=1, TRANSACTION READ ONLY",
        "SET TRANSACTION READ ONLY, @@x=1",
        "SET NAMES utf8mb4, TRANSACTION READ ONLY",
        "SET TRANSACTION READ ONLY, NAMES utf8mb4",
        "SET CHARACTER SET utf8, SESSION TRANSACTION READ ONLY",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, CHARACTER SET utf8",
    };
    for (const char* sql : queries) expect_rejected<Dialect::MySQL>(sql);
}

// Valid MySQL mixes of SET items must still parse completely.
TEST(MySQLSetTransaction, ValidMixesStillParseFully) {
    const RoundTrip cases[] = {
        {"SET NAMES utf8mb4, sql_mode='x'", "SET NAMES utf8mb4, sql_mode = 'x'",
         "SET NAMES utf8mb4, sql_mode = ?"},
        {"SET CHARACTER SET utf8, autocommit=0", "SET CHARACTER SET utf8, autocommit = 0",
         "SET CHARACTER SET utf8, autocommit = ?"},
        {"SET autocommit=0, NAMES utf8mb4", "SET autocommit = 0, NAMES utf8mb4",
         "SET autocommit = ?, NAMES utf8mb4"},
        {"SET GLOBAL a=1, SESSION b=2", "SET GLOBAL a = 1, SESSION b = 2",
         "SET GLOBAL a = ?, SESSION b = ?"},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::MySQL>(c);
}

TEST(MySQLSetTransaction, MultiStatementRemaining) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET TRANSACTION READ ONLY; SELECT 1";
    auto r = parser.parse(sql, std::strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_FALSE(r.full_input);
    EXPECT_EQ(std::string(r.remaining.ptr, r.remaining.len), "SELECT 1");
}

// ---- PostgreSQL ---------------------------------------------------------

TEST(PgSQLSetTransaction, ScopesAndCharacteristicsForm) {
    const RoundTrip cases[] = {
        {"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
         "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", nullptr},
        {"SET SESSION TRANSACTION READ ONLY", "SET SESSION TRANSACTION READ ONLY", nullptr},
        {"SET LOCAL TRANSACTION READ ONLY", "SET LOCAL TRANSACTION READ ONLY", nullptr},
        {"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
         "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE", nullptr},
        {"set session characteristics as transaction isolation level read   committed",
         "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED", nullptr},
        {"SET LOCAL SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
         "SET LOCAL SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", nullptr},
        {"SET SESSION SESSION CHARACTERISTICS AS TRANSACTION READ WRITE",
         "SET SESSION SESSION CHARACTERISTICS AS TRANSACTION READ WRITE", nullptr},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::PostgreSQL>(c);
}

TEST(PgSQLSetTransaction, AllModesCommaOrSpaceSeparated) {
    const RoundTrip cases[] = {
        {"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
         "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE", nullptr},
        {"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, NOT DEFERRABLE",
         "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY NOT DEFERRABLE", nullptr},
        {"SET TRANSACTION read write,not   deferrable isolation level repeatable read",
         "SET TRANSACTION READ WRITE NOT DEFERRABLE ISOLATION LEVEL REPEATABLE READ", nullptr},
        {"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ WRITE NOT DEFERRABLE",
         "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE NOT DEFERRABLE",
         nullptr},
        // PostgreSQL's grammar accepts repeated modes; keep them all.
        {"SET TRANSACTION READ ONLY READ WRITE", "SET TRANSACTION READ ONLY READ WRITE", nullptr},
        {"SET TRANSACTION ISOLATION /*c*/ LEVEL REPEATABLE\n  READ",
         "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", nullptr},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::PostgreSQL>(c);
}

TEST(PgSQLSetTransaction, Snapshot) {
    const RoundTrip cases[] = {
        {"SET TRANSACTION SNAPSHOT '00000003-0000001B-1'",
         "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'", "SET TRANSACTION SNAPSHOT ?"},
        {"SET LOCAL TRANSACTION SNAPSHOT 'x'", "SET LOCAL TRANSACTION SNAPSHOT 'x'",
         "SET LOCAL TRANSACTION SNAPSHOT ?"},
        {"set session transaction snapshot E'abc'", "SET SESSION TRANSACTION SNAPSHOT E'abc'",
         "SET SESSION TRANSACTION SNAPSHOT ?"},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::PostgreSQL>(c);

    Parser<Dialect::PostgreSQL> parser;
    const AstNode* txn = parse_txn(parser, "SET TRANSACTION SNAPSHOT 'abc'");
    ASSERT_NE(txn, nullptr);
    const AstNode* option = txn->first_child;
    ASSERT_NE(option, nullptr);
    EXPECT_EQ(option->type, NodeType::NODE_TRANSACTION_OPTION);
    EXPECT_EQ(value(option), "SNAPSHOT");
    ASSERT_NE(option->first_child, nullptr);
    EXPECT_EQ(option->first_child->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(value(option->first_child), "abc");
}

TEST(PgSQLSetTransaction, Ast) {
    Parser<Dialect::PostgreSQL> parser;
    const AstNode* txn = parse_txn(parser,
        "SET SESSION CHARACTERISTICS AS TRANSACTION read only not deferrable");
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->type, NodeType::NODE_SET_TRANSACTION);
    EXPECT_NE(txn->flags & FLAG_SET_TRANSACTION_CHARACTERISTICS, 0);
    // SESSION is part of the CHARACTERISTICS phrase, not a scope child.
    const AstNode* mode = txn->first_child;
    ASSERT_NE(mode, nullptr);
    EXPECT_EQ(mode->type, NodeType::NODE_TRANSACTION_OPTION);
    EXPECT_EQ(value(mode), "READ ONLY");
    ASSERT_NE(mode->next_sibling, nullptr);
    EXPECT_EQ(value(mode->next_sibling), "NOT DEFERRABLE");

    // Plain SET SESSION TRANSACTION affects only the current transaction in
    // PostgreSQL: scope child, no CHARACTERISTICS flag.
    txn = parse_txn(parser, "SET SESSION TRANSACTION READ ONLY");
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->flags & FLAG_SET_TRANSACTION_CHARACTERISTICS, 0);
    ASSERT_NE(txn->first_child, nullptr);
    EXPECT_EQ(txn->first_child->type, NodeType::NODE_IDENTIFIER);
    EXPECT_EQ(value(txn->first_child), "SESSION");

    txn = parse_txn(parser, "SET local SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");
    ASSERT_NE(txn, nullptr);
    EXPECT_NE(txn->flags & FLAG_SET_TRANSACTION_CHARACTERISTICS, 0);
    ASSERT_NE(txn->first_child, nullptr);
    EXPECT_EQ(value(txn->first_child), "LOCAL");
}

TEST(PgSQLSetTransaction, InvalidFormsRejected) {
    const char* queries[] = {
        "SET TRANSACTION",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE,",
        "SET TRANSACTION , READ ONLY",
        "SET TRANSACTION READ ONLY, , DEFERRABLE",
        "SET TRANSACTION ISOLATION SERIALIZABLE",
        "SET TRANSACTION ISOLATION LEVEL \"SERIALIZABLE\"",
        "SET TRANSACTION NOT",
        "SET TRANSACTION NOT READ ONLY",
        "SET SESSION CHARACTERISTICS AS TRANSACTION",
        "SET SESSION CHARACTERISTICS TRANSACTION READ ONLY",
        "SET SESSION CHARACTERISTICS AS READ ONLY",
        "SET SESSION CHARACTERISTICS AS TRANSACTION SNAPSHOT 'x'",
        "SET TRANSACTION SNAPSHOT x",
        "SET TRANSACTION SNAPSHOT",
        "SET TRANSACTION SNAPSHOT 'x', READ ONLY",
        "SET TRANSACTION SNAPSHOT 'x' READ ONLY",
        "SET TRANSACTION READ ONLY, SNAPSHOT 'x'",
        "SET LOCAL SESSION TRANSACTION READ ONLY",
    };
    for (const char* sql : queries) expect_rejected<Dialect::PostgreSQL>(sql);
}

// PostgreSQL has no GLOBAL scope. GLOBAL is an ordinary identifier there, so
// the generic SET path sees a variable named GLOBAL; the statement must never
// be reported as completely parsed.
TEST(PgSQLSetTransaction, GlobalScopeNotAccepted) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SET GLOBAL TRANSACTION READ ONLY";
    auto r = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
}

// PostgreSQL SET takes a single target: nothing may follow the mode list.
TEST(PgSQLSetTransaction, TrailingItemsRejected) {
    const char* queries[] = {
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY, search_path = a",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY junk",
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE, work_mem = 1",
        "SET TRANSACTION READ ONLY, search_path = a",
        "SET TRANSACTION READ ONLY search_path = a",
        "SET LOCAL TRANSACTION READ ONLY, TRANSACTION READ WRITE",
    };
    for (const char* sql : queries) expect_rejected<Dialect::PostgreSQL>(sql);
}

// ---- PostgreSQL SET forms whose emitters used to drop content -------------

TEST(PgSQLSetEmit, ListValuesRoleAuthorizationConstraints) {
    const RoundTrip cases[] = {
        {"SET search_path TO a,   b", "SET search_path = a, b", nullptr},
        {"SET search_path TO \"$user\", public", "SET search_path = \"$user\", public", nullptr},
        {"SET datestyle TO ISO,  DMY", "SET datestyle = ISO, DMY", nullptr},
        {"SET search_path = 'a', 'b'", "SET search_path = 'a', 'b'", "SET search_path = ?, ?"},
        {"SET LOCAL search_path = a, b", "SET LOCAL search_path = a, b", nullptr},
        {"SET SESSION search_path = a, b", "SET SESSION search_path = a, b", nullptr},
        {"SET ROLE   foo", "SET ROLE foo", nullptr},
        {"SET ROLE \"MixedCase\"", "SET ROLE \"MixedCase\"", nullptr},
        {"SET ROLE 'x'", "SET ROLE 'x'", "SET ROLE ?"},
        {"set local role none", "SET LOCAL ROLE none", nullptr},
        {"SET SESSION AUTHORIZATION default", "SET SESSION AUTHORIZATION DEFAULT", nullptr},
        {"SET SESSION AUTHORIZATION DEFAULT", "SET SESSION AUTHORIZATION DEFAULT", nullptr},
        {"SET SESSION AUTHORIZATION 'bob'", "SET SESSION AUTHORIZATION 'bob'",
         "SET SESSION AUTHORIZATION ?"},
        {"SET CONSTRAINTS ALL   deferred", "SET CONSTRAINTS ALL DEFERRED", nullptr},
        {"SET CONSTRAINTS a, \"B\" immediate", "SET CONSTRAINTS a, \"B\" IMMEDIATE", nullptr},
    };
    for (const auto& c : cases) expect_round_trip<Dialect::PostgreSQL>(c);
}
