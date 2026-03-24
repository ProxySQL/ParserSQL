# Prepared Statement Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add prepared statement cache and binary protocol support — `parse_and_cache()`, `execute()`, and `prepare_cache_evict()` — so ProxySQL can handle COM_STMT_PREPARE/EXECUTE efficiently.

**Architecture:** The statement cache is a fixed-capacity LRU map (keyed by statement ID) that stores deep-copied ASTs outside the arena. `parse_and_cache()` parses normally, then copies the AST to the cache. `execute()` looks up the cached AST and returns it with bound parameter bindings. The emitter is extended to materialize placeholders from bindings.

**Tech Stack:** C++17, existing parser/arena/emitter infrastructure

**Spec:** `docs/superpowers/specs/2026-03-24-sql-parser-design.md` (Binary Protocol section)

---

## Scope

1. `BoundValue` and `ParamBindings` structs (in `parse_result.h`)
2. AST deep-copy utility (copy AST tree from arena to heap for caching)
3. `StmtCache` — fixed-capacity LRU cache keyed by statement ID
4. `parse_and_cache()`, `execute()`, `prepare_cache_evict()` on `Parser<D>`
5. Emitter extension — materialize `NODE_PLACEHOLDER` from bindings
6. Tests for the full prepare/execute/evict lifecycle

**Not in scope:** Actual MySQL wire protocol decoding (ProxySQL handles that).

---

## File Structure

```
include/sql_parser/
    parse_result.h        — (modify) Add BoundValue, ParamBindings
    stmt_cache.h          — StmtCache class + AST deep-copy
    emitter.h             — (modify) Add bindings-aware placeholder emission
    parser.h              — (modify) Add parse_and_cache, execute, prepare_cache_evict

src/sql_parser/
    parser.cpp            — (modify) Implement new methods

tests/
    test_stmt_cache.cpp   — Prepared statement lifecycle tests

Makefile.new              — (modify) Add test_stmt_cache.cpp
```

---

### Task 1: BoundValue, ParamBindings, StmtCache, AST Deep-Copy

**Files:**
- Modify: `include/sql_parser/parse_result.h` — add BoundValue, ParamBindings
- Create: `include/sql_parser/stmt_cache.h` — StmtCache + AST deep-copy
- Create: `tests/test_stmt_cache.cpp`
- Modify: `Makefile.new`

- [ ] **Step 1: Add BoundValue and ParamBindings to parse_result.h**

Add to `include/sql_parser/parse_result.h` before `ParseResult`:
```cpp
struct BoundValue {
    enum Type : uint8_t { INT, FLOAT, DOUBLE, STRING, BLOB, NULL_VAL, DATETIME, DECIMAL };
    Type type = NULL_VAL;
    union {
        int64_t int_val;
        float float32_val;
        double float64_val;
        StringRef str_val;
    };
};
static_assert(std::is_trivially_copyable_v<BoundValue>);

struct ParamBindings {
    BoundValue* values = nullptr;
    uint16_t count = 0;
};
```

Also add a `ParamBindings` field to `ParseResult`:
```cpp
    ParamBindings bindings;    // populated by execute()
```

- [ ] **Step 2: Create stmt_cache.h**

Create `include/sql_parser/stmt_cache.h`:
```cpp
#ifndef SQL_PARSER_STMT_CACHE_H
#define SQL_PARSER_STMT_CACHE_H

#include "sql_parser/ast.h"
#include "sql_parser/common.h"
#include "sql_parser/parse_result.h"
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <list>

namespace sql_parser {

// Deep-copy an AST tree from arena to heap memory.
// The returned tree must be freed with free_ast().
inline AstNode* deep_copy_ast(const AstNode* src) {
    if (!src) return nullptr;

    AstNode* dst = static_cast<AstNode*>(std::malloc(sizeof(AstNode)));
    if (!dst) return nullptr;

    dst->type = src->type;
    dst->flags = src->flags;
    dst->first_child = nullptr;
    dst->next_sibling = nullptr;

    // Deep-copy value string to heap
    if (src->value_ptr && src->value_len > 0) {
        char* val_copy = static_cast<char*>(std::malloc(src->value_len));
        if (val_copy) {
            std::memcpy(val_copy, src->value_ptr, src->value_len);
        }
        dst->value_ptr = val_copy;
        dst->value_len = src->value_len;
    } else {
        dst->value_ptr = nullptr;
        dst->value_len = 0;
    }

    // Recursively copy children
    const AstNode* src_child = src->first_child;
    AstNode* prev_dst_child = nullptr;
    while (src_child) {
        AstNode* dst_child = deep_copy_ast(src_child);
        if (dst_child) {
            if (!dst->first_child) {
                dst->first_child = dst_child;
            } else if (prev_dst_child) {
                prev_dst_child->next_sibling = dst_child;
            }
            prev_dst_child = dst_child;
        }
        src_child = src_child->next_sibling;
    }

    return dst;
}

// Free a heap-allocated AST tree (produced by deep_copy_ast).
inline void free_ast(AstNode* node) {
    if (!node) return;
    // Free children first
    AstNode* child = node->first_child;
    while (child) {
        AstNode* next = child->next_sibling;
        free_ast(child);
        child = next;
    }
    // Free value string
    if (node->value_ptr) {
        std::free(const_cast<char*>(node->value_ptr));
    }
    std::free(node);
}

// Cached entry for a prepared statement.
struct CachedStmt {
    uint32_t stmt_id;
    StmtType stmt_type;
    AstNode* ast;       // heap-allocated deep copy

    ~CachedStmt() {
        free_ast(ast);
    }

    // Non-copyable
    CachedStmt(const CachedStmt&) = delete;
    CachedStmt& operator=(const CachedStmt&) = delete;
    CachedStmt(CachedStmt&& o) noexcept
        : stmt_id(o.stmt_id), stmt_type(o.stmt_type), ast(o.ast) {
        o.ast = nullptr;
    }
    CachedStmt& operator=(CachedStmt&& o) noexcept {
        if (this != &o) {
            free_ast(ast);
            stmt_id = o.stmt_id;
            stmt_type = o.stmt_type;
            ast = o.ast;
            o.ast = nullptr;
        }
        return *this;
    }

    CachedStmt() : stmt_id(0), stmt_type(StmtType::UNKNOWN), ast(nullptr) {}
    CachedStmt(uint32_t id, StmtType type, AstNode* a)
        : stmt_id(id), stmt_type(type), ast(a) {}
};

// Fixed-capacity LRU cache for prepared statements.
class StmtCache {
public:
    explicit StmtCache(size_t capacity = 128) : capacity_(capacity) {}

    ~StmtCache() { clear(); }

    // Non-copyable
    StmtCache(const StmtCache&) = delete;
    StmtCache& operator=(const StmtCache&) = delete;

    // Store a prepared statement. Deep-copies the AST from the arena.
    // Evicts LRU entry if at capacity.
    bool store(uint32_t stmt_id, StmtType stmt_type, const AstNode* ast) {
        // If already exists, remove old entry
        evict(stmt_id);

        AstNode* copy = deep_copy_ast(ast);
        if (!copy && ast) return false;

        // Evict LRU if at capacity
        if (lru_.size() >= capacity_) {
            auto& oldest = lru_.back();
            map_.erase(oldest.stmt_id);
            lru_.pop_back();
        }

        lru_.emplace_front(stmt_id, stmt_type, copy);
        map_[stmt_id] = lru_.begin();
        return true;
    }

    // Look up a cached statement. Returns nullptr if not found.
    // Moves the entry to front of LRU.
    const CachedStmt* lookup(uint32_t stmt_id) {
        auto it = map_.find(stmt_id);
        if (it == map_.end()) return nullptr;
        // Move to front (most recently used)
        lru_.splice(lru_.begin(), lru_, it->second);
        return &(*it->second);
    }

    // Evict a specific statement.
    void evict(uint32_t stmt_id) {
        auto it = map_.find(stmt_id);
        if (it != map_.end()) {
            lru_.erase(it->second);
            map_.erase(it);
        }
    }

    // Clear all entries.
    void clear() {
        lru_.clear();
        map_.clear();
    }

    size_t size() const { return map_.size(); }
    size_t capacity() const { return capacity_; }

private:
    size_t capacity_;
    std::list<CachedStmt> lru_;
    std::unordered_map<uint32_t, std::list<CachedStmt>::iterator> map_;
};

} // namespace sql_parser

#endif // SQL_PARSER_STMT_CACHE_H
```

- [ ] **Step 3: Write tests**

Create `tests/test_stmt_cache.cpp`:
```cpp
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

    // Reset arena — copy should still be valid
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

    Emitter<Dialect::MySQL> emitter(parser.arena(), &r.bindings);
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

    Emitter<Dialect::MySQL> emitter(parser.arena(), &r.bindings);
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SET sql_mode = 'TRADITIONAL'");
}

TEST(PreparedStmtTest, EmitWithNullBinding) {
    Parser<Dialect::MySQL> parser;

    parser.parse_and_cache("SET character_set_results = ?", 28, 3);

    BoundValue bv;
    bv.type = BoundValue::NULL_VAL;
    ParamBindings bindings{&bv, 1};

    auto r = parser.execute(3, bindings);
    ASSERT_EQ(r.status, ParseResult::OK);

    Emitter<Dialect::MySQL> emitter(parser.arena(), &r.bindings);
    emitter.emit(r.ast);
    StringRef result = emitter.result();
    std::string out(result.ptr, result.len);
    EXPECT_EQ(out, "SET character_set_results = NULL");
}
```

- [ ] **Step 4: Update Parser class — add new methods**

Modify `include/sql_parser/parser.h` — add declarations after existing public methods:
```cpp
    // Prepared statement support
    ParseResult parse_and_cache(const char* sql, size_t len, uint32_t stmt_id);
    ParseResult execute(uint32_t stmt_id, const ParamBindings& params);
    void prepare_cache_evict(uint32_t stmt_id);
```

Add private member:
```cpp
    StmtCache stmt_cache_;
```

Add include at top:
```cpp
#include "sql_parser/stmt_cache.h"
```

Update `ParserConfig` with cache capacity:
```cpp
struct ParserConfig {
    size_t arena_block_size = 65536;
    size_t arena_max_size = 1048576;
    size_t stmt_cache_capacity = 128;
};
```

Update constructor to use config:
```cpp
    explicit Parser(const ParserConfig& config = {});
```

Modify `src/sql_parser/parser.cpp` — update constructor and add implementations:

Update constructor:
```cpp
template <Dialect D>
Parser<D>::Parser(const ParserConfig& config)
    : arena_(config.arena_block_size, config.arena_max_size),
      stmt_cache_(config.stmt_cache_capacity) {}
```

Add new methods:
```cpp
template <Dialect D>
ParseResult Parser<D>::parse_and_cache(const char* sql, size_t len, uint32_t stmt_id) {
    ParseResult r = parse(sql, len);
    if (r.ast) {
        stmt_cache_.store(stmt_id, r.stmt_type, r.ast);
    }
    return r;
}

template <Dialect D>
ParseResult Parser<D>::execute(uint32_t stmt_id, const ParamBindings& params) {
    ParseResult r;
    const CachedStmt* cached = stmt_cache_.lookup(stmt_id);
    if (!cached) {
        r.status = ParseResult::ERROR;
        r.stmt_type = StmtType::UNKNOWN;
        return r;
    }
    r.status = ParseResult::OK;
    r.stmt_type = cached->stmt_type;
    r.ast = cached->ast;
    r.bindings = params;
    return r;
}

template <Dialect D>
void Parser<D>::prepare_cache_evict(uint32_t stmt_id) {
    stmt_cache_.evict(stmt_id);
}
```

- [ ] **Step 5: Extend Emitter to handle bindings**

Modify `include/sql_parser/emitter.h` — add bindings-aware constructor and placeholder emission:

Update constructor to optionally accept bindings:
```cpp
    explicit Emitter(Arena& arena, const ParamBindings* bindings = nullptr)
        : sb_(arena), bindings_(bindings), placeholder_index_(0) {}
```

Add private member:
```cpp
    const ParamBindings* bindings_;
    uint16_t placeholder_index_;
```

Update the `NODE_PLACEHOLDER` handling in `emit_node()` switch — change from `emit_value` to a new method:
```cpp
    case NodeType::NODE_PLACEHOLDER:
        emit_placeholder(node); break;
```

Add `emit_placeholder`:
```cpp
    void emit_placeholder(const AstNode* node) {
        if (bindings_ && placeholder_index_ < bindings_->count) {
            const BoundValue& bv = bindings_->values[placeholder_index_];
            ++placeholder_index_;
            switch (bv.type) {
                case BoundValue::INT:
                    // Convert int to string in arena
                    { char buf[32]; int n = snprintf(buf, sizeof(buf), "%lld", (long long)bv.int_val);
                      sb_.append(buf, n); }
                    break;
                case BoundValue::FLOAT:
                    { char buf[64]; int n = snprintf(buf, sizeof(buf), "%g", (double)bv.float32_val);
                      sb_.append(buf, n); }
                    break;
                case BoundValue::DOUBLE:
                    { char buf[64]; int n = snprintf(buf, sizeof(buf), "%g", bv.float64_val);
                      sb_.append(buf, n); }
                    break;
                case BoundValue::STRING:
                case BoundValue::DATETIME:
                case BoundValue::DECIMAL:
                    sb_.append_char('\'');
                    sb_.append(bv.str_val);
                    sb_.append_char('\'');
                    break;
                case BoundValue::BLOB:
                    sb_.append(bv.str_val);
                    break;
                case BoundValue::NULL_VAL:
                    sb_.append("NULL", 4);
                    break;
            }
        } else {
            // No binding available — emit placeholder as-is
            emit_value(node);
        }
    }
```

- [ ] **Step 6: Update Makefile.new and build**

Add `$(TEST_DIR)/test_stmt_cache.cpp` to TEST_SRCS.

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```

- [ ] **Step 7: Commit**

```bash
git add include/sql_parser/stmt_cache.h include/sql_parser/parse_result.h \
    include/sql_parser/parser.h include/sql_parser/emitter.h \
    src/sql_parser/parser.cpp tests/test_stmt_cache.cpp Makefile.new
git commit -m "feat: add prepared statement cache with parse_and_cache, execute, and bindings-aware emitter"
```
