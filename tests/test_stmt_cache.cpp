#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/stmt_cache.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

// ========== StmtCache unit tests ==========

TEST(StmtCacheTest, StoreAndLookup) {
    StmtCache cache(16);
    Arena arena(4096);

    AstNode* node = make_node(arena, NodeType::NODE_SET_STMT, StringRef{"SET", 3});
    ASSERT_NE(node, nullptr);

    EXPECT_TRUE(cache.store(1, StmtType::SET, node));
    EXPECT_EQ(cache.size(), 1u);

    const CachedStmt* found = cache.lookup(1);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->stmt_id, 1u);
    EXPECT_EQ(found->stmt_type, StmtType::SET);
    ASSERT_NE(found->ast, nullptr);
    EXPECT_EQ(found->ast->type, NodeType::NODE_SET_STMT);
}

TEST(StmtCacheTest, LookupMiss) {
    StmtCache cache(16);
    EXPECT_EQ(cache.lookup(999), nullptr);
}

TEST(StmtCacheTest, Evict) {
    StmtCache cache(16);
    Arena arena(4096);

    AstNode* node = make_node(arena, NodeType::NODE_SELECT_STMT);
    cache.store(1, StmtType::SELECT, node);
    EXPECT_EQ(cache.size(), 1u);

    cache.evict(1);
    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.lookup(1), nullptr);
}

TEST(StmtCacheTest, LRUEviction) {
    StmtCache cache(2);  // capacity = 2
    Arena arena(4096);

    AstNode* n1 = make_node(arena, NodeType::NODE_SET_STMT);
    AstNode* n2 = make_node(arena, NodeType::NODE_SELECT_STMT);
    AstNode* n3 = make_node(arena, NodeType::NODE_SET_STMT);

    cache.store(1, StmtType::SET, n1);
    cache.store(2, StmtType::SELECT, n2);
    EXPECT_EQ(cache.size(), 2u);

    // Adding a third should evict the LRU (stmt 1)
    cache.store(3, StmtType::SET, n3);
    EXPECT_EQ(cache.size(), 2u);
    EXPECT_EQ(cache.lookup(1), nullptr);  // evicted
    EXPECT_NE(cache.lookup(2), nullptr);  // still there
    EXPECT_NE(cache.lookup(3), nullptr);  // just added
}

TEST(StmtCacheTest, LRUTouchOnLookup) {
    StmtCache cache(2);
    Arena arena(4096);

    AstNode* n1 = make_node(arena, NodeType::NODE_SET_STMT);
    AstNode* n2 = make_node(arena, NodeType::NODE_SELECT_STMT);
    AstNode* n3 = make_node(arena, NodeType::NODE_SET_STMT);

    cache.store(1, StmtType::SET, n1);
    cache.store(2, StmtType::SELECT, n2);

    // Touch stmt 1 to make it recently used
    cache.lookup(1);

    // Adding stmt 3 should evict stmt 2 (now the LRU)
    cache.store(3, StmtType::SET, n3);
    EXPECT_NE(cache.lookup(1), nullptr);  // touched, still alive
    EXPECT_EQ(cache.lookup(2), nullptr);  // evicted
    EXPECT_NE(cache.lookup(3), nullptr);
}

TEST(StmtCacheTest, DeepCopyPreservesTree) {
    Arena arena(4096);

    // Build a small tree: SET_STMT -> VAR_ASSIGNMENT -> (VAR_TARGET, LITERAL_INT)
    AstNode* root = make_node(arena, NodeType::NODE_SET_STMT);
    AstNode* assign = make_node(arena, NodeType::NODE_VAR_ASSIGNMENT);
    AstNode* target = make_node(arena, NodeType::NODE_VAR_TARGET);
    target->add_child(make_node(arena, NodeType::NODE_IDENTIFIER, StringRef{"autocommit", 10}));
    AstNode* value = make_node(arena, NodeType::NODE_LITERAL_INT, StringRef{"1", 1});
    assign->add_child(target);
    assign->add_child(value);
    root->add_child(assign);

    // Deep copy
    AstNode* copy = deep_copy_ast(root);
    ASSERT_NE(copy, nullptr);
    EXPECT_EQ(copy->type, NodeType::NODE_SET_STMT);

    // Verify tree structure is preserved
    ASSERT_NE(copy->first_child, nullptr);
    EXPECT_EQ(copy->first_child->type, NodeType::NODE_VAR_ASSIGNMENT);

    AstNode* copy_target = copy->first_child->first_child;
    ASSERT_NE(copy_target, nullptr);
    EXPECT_EQ(copy_target->type, NodeType::NODE_VAR_TARGET);

    AstNode* copy_name = copy_target->first_child;
    ASSERT_NE(copy_name, nullptr);
    EXPECT_EQ(std::string(copy_name->value_ptr, copy_name->value_len), "autocommit");

    // Verify it's a deep copy (different pointers)
    EXPECT_NE(copy, root);
    EXPECT_NE(copy->first_child, root->first_child);
    EXPECT_NE(copy_name->value_ptr, target->first_child->value_ptr);

    // Reset arena -- copy should still be valid
    arena.reset();
    EXPECT_EQ(std::string(copy_name->value_ptr, copy_name->value_len), "autocommit");

    free_ast(copy);
}

// ========== Parser integration tests ==========

TEST(PreparedStmtTest, ParseAndCache) {
    Parser<Dialect::MySQL> parser;

    auto r = parser.parse_and_cache("SELECT * FROM users WHERE id = ?", 32, 1);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);
}

TEST(PreparedStmtTest, ExecuteAfterCache) {
    Parser<Dialect::MySQL> parser;

    parser.parse_and_cache("SET autocommit = ?", 18, 42);

    // Build bindings
    BoundValue bv;
    bv.type = BoundValue::INT;
    bv.int_val = 0;
    ParamBindings bindings{&bv, 1};

    auto r = parser.execute(42, bindings);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.bindings.count, 1);
    EXPECT_EQ(r.bindings.values[0].int_val, 0);
}

TEST(PreparedStmtTest, ExecuteNotFound) {
    Parser<Dialect::MySQL> parser;

    BoundValue bv;
    bv.type = BoundValue::NULL_VAL;
    ParamBindings bindings{&bv, 1};

    auto r = parser.execute(999, bindings);
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

TEST(PreparedStmtTest, EvictAndExecuteFails) {
    Parser<Dialect::MySQL> parser;

    parser.parse_and_cache("SELECT 1", 8, 10);
    parser.prepare_cache_evict(10);

    BoundValue bv;
    bv.type = BoundValue::NULL_VAL;
    ParamBindings bindings{&bv, 1};

    auto r = parser.execute(10, bindings);
    EXPECT_EQ(r.status, ParseResult::ERROR);
}

TEST(PreparedStmtTest, CacheMultipleStatements) {
    Parser<Dialect::MySQL> parser;

    parser.parse_and_cache("SELECT 1", 8, 1);
    parser.parse_and_cache("SELECT 2", 8, 2);
    parser.parse_and_cache("SET autocommit = 0", 18, 3);

    BoundValue bv;
    bv.type = BoundValue::NULL_VAL;
    ParamBindings bindings{&bv, 0};

    auto r1 = parser.execute(1, bindings);
    EXPECT_EQ(r1.status, ParseResult::OK);
    EXPECT_EQ(r1.stmt_type, StmtType::SELECT);

    auto r3 = parser.execute(3, bindings);
    EXPECT_EQ(r3.status, ParseResult::OK);
    EXPECT_EQ(r3.stmt_type, StmtType::SET);
}

// ========== Emitter with bindings ==========

TEST(PreparedStmtTest, EmitWithBindings) {
    Parser<Dialect::MySQL> parser;

    parser.parse_and_cache("SET autocommit = ?", 18, 1);

    BoundValue bv;
    bv.type = BoundValue::INT;
    bv.int_val = 1;
    ParamBindings bindings{&bv, 1};

    auto r = parser.execute(1, bindings);
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);

    Emitter<Dialect::MySQL> emitter(parser.arena(), EmitMode::NORMAL, &r.bindings);
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SET autocommit = 1");
}

TEST(PreparedStmtTest, EmitWithStringBinding) {
    Parser<Dialect::MySQL> parser;

    parser.parse_and_cache("SET sql_mode = ?", 16, 2);

    const char* mode = "TRADITIONAL";
    BoundValue bv;
    bv.type = BoundValue::STRING;
    bv.str_val = StringRef{mode, 11};
    ParamBindings bindings{&bv, 1};

    auto r = parser.execute(2, bindings);
    ASSERT_EQ(r.status, ParseResult::OK);

    Emitter<Dialect::MySQL> emitter(parser.arena(), EmitMode::NORMAL, &r.bindings);
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SET sql_mode = 'TRADITIONAL'");
}

TEST(PreparedStmtTest, EmitWithNullBinding) {
    Parser<Dialect::MySQL> parser;

    parser.parse_and_cache("SET character_set_results = ?", 29, 3);

    BoundValue bv;
    bv.type = BoundValue::NULL_VAL;
    ParamBindings bindings{&bv, 1};

    auto r = parser.execute(3, bindings);
    ASSERT_EQ(r.status, ParseResult::OK);

    Emitter<Dialect::MySQL> emitter(parser.arena(), EmitMode::NORMAL, &r.bindings);
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SET character_set_results = NULL");
}
