#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_engine/session.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <string>
#include <vector>

using namespace sql_parser;
using namespace sql_engine;

namespace {

class RecordingTransactionManager : public TransactionManager {
public:
    std::vector<std::string> calls;
    bool active = false;
    bool auto_commit = true;
    bool begin() override { calls.emplace_back("begin"); active = true; return true; }
    bool commit() override { calls.emplace_back("commit"); active = false; return true; }
    bool rollback() override { calls.emplace_back("rollback"); active = false; return true; }
    bool savepoint(const char* name) override { calls.emplace_back(std::string("save:") + name); return active; }
    bool rollback_to(const char* name) override { calls.emplace_back(std::string("to:") + name); return active; }
    bool release_savepoint(const char* name) override { calls.emplace_back(std::string("release:") + name); return active; }
    bool in_transaction() const override { return active; }
    bool is_auto_commit() const override { return auto_commit; }
    void set_auto_commit(bool value) override { auto_commit = value; }
};

bool parsed(const char* sql) {
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    return result.ok() && result.full_input;
}

} // namespace

TEST(ReviewTransactions, UtilityIdentifiersUsePostgresColId) {
    for (const char* sql : {
        "COPY data FROM STDIN", "COPY t(id, format) FROM STDIN",
        "COPY public.data (id, format) TO STDOUT",
        "SAVEPOINT level", "ROLLBACK TO SAVEPOINT level", "RELEASE level"
    }) {
        SCOPED_TRACE(sql);
        EXPECT_TRUE(parsed(sql));
    }
    for (const char* sql : {
        "COPY t(select) FROM STDIN", "SAVEPOINT SELECT", "RELEASE SELECT"
    }) {
        SCOPED_TRACE(sql);
        EXPECT_FALSE(parsed(sql));
    }
}

TEST(ReviewTransactions, PreparedFormsRequireCanonicalVerbs) {
    EXPECT_TRUE(parsed("COMMIT PREPARED 'gid'"));
    EXPECT_TRUE(parsed("ROLLBACK PREPARED 'gid'"));
    EXPECT_FALSE(parsed("END PREPARED 'gid'"));
    EXPECT_FALSE(parsed("ABORT PREPARED 'gid'"));
}

TEST(ReviewTransactions, SavepointNamesAndEffects) {
    InMemoryCatalog catalog;
    RecordingTransactionManager manager;
    Session<Dialect::PostgreSQL> session(catalog, manager);
    EXPECT_TRUE(session.execute_statement("BEGIN").success);
    EXPECT_TRUE(session.execute_statement("SAVEPOINT \"My Point\"").success);
    EXPECT_TRUE(session.execute_statement("RELEASE SAVEPOINT \"My Point\"").success);
    EXPECT_TRUE(session.execute_statement("SAVEPOINT level").success);
    EXPECT_TRUE(session.execute_statement("ROLLBACK TO level").success);
    EXPECT_TRUE(session.execute_statement("SAVEPOINT Foo").success);
    EXPECT_TRUE(session.execute_statement("RELEASE foo").success);
    EXPECT_TRUE(session.execute_statement("SAVEPOINT \"a\"\"b\"").success);
    EXPECT_TRUE(session.execute_statement("RELEASE \"a\"\"b\"").success);
    EXPECT_TRUE(session.execute_statement("COMMIT").success);
    EXPECT_EQ(manager.calls, (std::vector<std::string>{"begin", "save:My Point", "release:My Point", "save:level", "to:level", "save:foo", "release:foo", "save:a\"b", "release:a\"b", "commit"}));
}

TEST(ReviewTransactions, InvalidAndUnsupportedControlsHaveNoEffects) {
    InMemoryCatalog catalog;
    RecordingTransactionManager manager;
    Session<Dialect::PostgreSQL> session(catalog, manager);
    for (const char* sql : {
        "BEGIN READ", "BEGIN READ ONLY", "COMMIT trailing", "COMMIT PREPARED 'gid'",
        "ROLLBACK PREPARED 'gid'", "COMMIT AND CHAIN", "ROLLBACK AND CHAIN",
        "SAVEPOINT", "ROLLBACK TO", "RELEASE SAVEPOINT"
    }) {
        SCOPED_TRACE(sql);
        EXPECT_FALSE(session.execute_statement(sql).success);
    }
    EXPECT_TRUE(manager.calls.empty());
}

TEST(ReviewTransactions, MySqlTransactionBaseline) {
    InMemoryCatalog catalog;
    RecordingTransactionManager manager;
    Session<Dialect::MySQL> session(catalog, manager);
    EXPECT_TRUE(session.execute_statement("BEGIN").success);
    EXPECT_TRUE(session.execute_statement("COMMIT").success);
    EXPECT_TRUE(session.execute_statement("BEGIN").success);
    EXPECT_TRUE(session.execute_statement("ROLLBACK").success);
    EXPECT_TRUE(session.execute_statement("BEGIN WORK").success);
    EXPECT_TRUE(session.execute_statement("SAVEPOINT named_point").success);
    EXPECT_TRUE(session.execute_statement("COMMIT WORK").success);
    EXPECT_TRUE(session.execute_statement("START TRANSACTION").success);
    EXPECT_TRUE(session.execute_statement("ROLLBACK WORK").success);
    EXPECT_FALSE(session.execute_statement("START").success);
    EXPECT_FALSE(session.execute_statement("SAVEPOINT").success);
    EXPECT_FALSE(session.execute_statement("SAVEPOINT named_point trailing").success);
    EXPECT_EQ(manager.calls, (std::vector<std::string>{"begin", "commit", "begin", "rollback",
        "begin", "save:named_point", "commit", "begin", "rollback"}));
}
