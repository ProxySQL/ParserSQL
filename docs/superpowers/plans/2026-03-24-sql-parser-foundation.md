# SQL Parser Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the foundational parser pipeline — core types, arena allocator, tokenizer, classifier, and basic Tier 2 extractors — so that any SQL statement can be classified and key metadata extracted.

**Architecture:** Three-layer pipeline (Tokenizer → Classifier → Extractors) with compile-time dialect templating (`Dialect::MySQL`, `Dialect::PostgreSQL`). Arena allocator for zero-copy, sub-microsecond operation. This plan covers Layers 1-3 from the spec but defers Tier 1 deep parsers (SELECT, SET), the emitter, and prepared statement cache to subsequent plans.

**Tech Stack:** C++17, GNU Make, Google Test (header-only download for tests), Google Benchmark (for perf tests, deferred to later plan)

**Spec:** `docs/superpowers/specs/2026-03-24-sql-parser-design.md`

---

## Scope

This plan builds:
1. Build system (new Makefile for the new parser + tests)
2. Core types (`StringRef`, `Dialect` enum, `NodeType`, `StmtType`, `TokenType`)
3. Arena allocator (block-chained, reset, max size)
4. `AstNode` (32-byte, arena-allocated, intrusive linked list)
5. `ParseResult` and `ErrorInfo`
6. Tokenizer (dialect-templated, MySQL + PostgreSQL, keyword perfect hash)
7. Classifier (switch dispatch on first token)
8. Tier 2 extractors (extract table name / schema for DML + DDL, transaction type, USE database)

**Not in scope for this plan:** Tier 1 deep parsers (SELECT, SET), expression parser, emitter/reconstruction, prepared statement cache, benchmarks.

---

## File Structure

```
include/sql_parser/
    common.h              — StringRef, Dialect enum, StmtType, NodeType enums
    arena.h               — Arena class (block-chained allocator)
    token.h               — Token struct, TokenType enum
    ast.h                 — AstNode struct
    parse_result.h        — ParseResult, ErrorInfo
    tokenizer.h           — Tokenizer<D> template (declaration + inline impl)
    keywords_mysql.h      — MySQL keyword lookup table
    keywords_pgsql.h      — PostgreSQL keyword lookup table
    parser.h              — Parser<D> public API

src/sql_parser/
    arena.cpp             — Arena non-inline methods
    parser.cpp            — Parser<D> classifier + Tier 2 extractors + explicit instantiations

tests/
    test_main.cpp         — Google Test main()
    test_arena.cpp        — Arena unit tests
    test_tokenizer.cpp    — Tokenizer unit tests (MySQL + PostgreSQL)
    test_classifier.cpp   — Classifier + Tier 2 extractor tests

Makefile.new              — New build system (renamed to Makefile after old parser removal)
```

---

### Task 1: Build System Setup

**Files:**
- Create: `Makefile.new`
- Create: `tests/test_main.cpp`

This task sets up the new Makefile targeting the `include/sql_parser/` and `src/sql_parser/` layout, with a test target using Google Test. The old Makefile is left untouched.

- [ ] **Step 1: Download Google Test header-only**

Run:
```bash
mkdir -p third_party/googletest
git clone --depth 1 --branch v1.14.0 https://github.com/google/googletest.git third_party/googletest
```

- [ ] **Step 2: Create test_main.cpp**

Create `tests/test_main.cpp`:
```cpp
#include <gtest/gtest.h>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
```

- [ ] **Step 3: Create Makefile.new**

Create `Makefile.new`:
```makefile
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -g -O2
CPPFLAGS = -I./include -I./third_party/googletest/googletest/include

PROJECT_ROOT = .
SRC_DIR = $(PROJECT_ROOT)/src/sql_parser
INCLUDE_DIR = $(PROJECT_ROOT)/include/sql_parser
TEST_DIR = $(PROJECT_ROOT)/tests

# Library sources
LIB_SRCS = $(SRC_DIR)/arena.cpp $(SRC_DIR)/parser.cpp
LIB_OBJS = $(LIB_SRCS:.cpp=.o)
LIB_TARGET = $(PROJECT_ROOT)/libsqlparser.a

# Google Test library
GTEST_DIR = $(PROJECT_ROOT)/third_party/googletest/googletest
GTEST_SRC = $(GTEST_DIR)/src/gtest-all.cc
GTEST_OBJ = $(GTEST_DIR)/src/gtest-all.o
GTEST_CPPFLAGS = -I$(GTEST_DIR)/include -I$(GTEST_DIR)

# Test sources
TEST_SRCS = $(TEST_DIR)/test_main.cpp \
            $(TEST_DIR)/test_arena.cpp \
            $(TEST_DIR)/test_tokenizer.cpp \
            $(TEST_DIR)/test_classifier.cpp
TEST_OBJS = $(TEST_SRCS:.cpp=.o)
TEST_TARGET = $(PROJECT_ROOT)/run_tests

.PHONY: all lib test clean

all: lib test

lib: $(LIB_TARGET)

$(LIB_TARGET): $(LIB_OBJS)
	ar rcs $@ $^
	@echo "Built $@"

$(SRC_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@

# Google Test object
$(GTEST_OBJ): $(GTEST_SRC)
	$(CXX) $(CXXFLAGS) $(GTEST_CPPFLAGS) -c $< -o $@

# Test objects
$(TEST_DIR)/%.o: $(TEST_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(GTEST_CPPFLAGS) -c $< -o $@

test: $(TEST_TARGET)
	./$(TEST_TARGET)

$(TEST_TARGET): $(TEST_OBJS) $(GTEST_OBJ) $(LIB_TARGET)
	$(CXX) $(CXXFLAGS) -o $@ $(TEST_OBJS) $(GTEST_OBJ) -L$(PROJECT_ROOT) -lsqlparser -lpthread

clean:
	rm -f $(LIB_OBJS) $(LIB_TARGET) $(TEST_OBJS) $(GTEST_OBJ) $(TEST_TARGET)
	@echo "Cleaned."
```

- [ ] **Step 4: Create directory structure**

Run:
```bash
mkdir -p include/sql_parser src/sql_parser
```

- [ ] **Step 5: Commit**

```bash
git add Makefile.new tests/test_main.cpp third_party/
git commit -m "feat: add new build system and test infrastructure for sql_parser"
```

---

### Task 2: Core Types — StringRef, Enums

**Files:**
- Create: `include/sql_parser/common.h`

- [ ] **Step 1: Write common.h**

Create `include/sql_parser/common.h`:
```cpp
#ifndef SQL_PARSER_COMMON_H
#define SQL_PARSER_COMMON_H

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace sql_parser {

// -- Dialect --

enum class Dialect : uint8_t {
    MySQL,
    PostgreSQL
};

// -- StringRef: zero-copy view into input buffer --

struct StringRef {
    const char* ptr = nullptr;
    uint32_t len = 0;

    bool empty() const { return len == 0; }

    bool equals_ci(const char* s, uint32_t slen) const {
        if (len != slen) return false;
        for (uint32_t i = 0; i < len; ++i) {
            char a = ptr[i];
            char b = s[i];
            // fast ASCII tolower
            if (a >= 'A' && a <= 'Z') a += 32;
            if (b >= 'A' && b <= 'Z') b += 32;
            if (a != b) return false;
        }
        return true;
    }

    bool operator==(const StringRef& o) const {
        return len == o.len && (ptr == o.ptr || std::memcmp(ptr, o.ptr, len) == 0);
    }
    bool operator!=(const StringRef& o) const { return !(*this == o); }
};
static_assert(std::is_trivially_copyable_v<StringRef>);

// Case-insensitive comparison for keyword lookup (used by keyword tables)
inline int ci_cmp(const char* a, uint32_t alen, const char* b, uint8_t blen) {
    uint32_t minlen = alen < blen ? alen : blen;
    for (uint32_t i = 0; i < minlen; ++i) {
        char ca = a[i];
        char cb = b[i];
        if (ca >= 'a' && ca <= 'z') ca -= 32;
        if (cb >= 'a' && cb <= 'z') cb -= 32;
        if (ca != cb) return ca < cb ? -1 : 1;
    }
    if (alen < blen) return -1;
    if (alen > blen) return 1;
    return 0;
}

// -- Statement type (always set, even for PARTIAL/ERROR) --

enum class StmtType : uint8_t {
    UNKNOWN = 0,
    SELECT,
    INSERT,
    UPDATE,
    DELETE_STMT,  // avoid clash with delete keyword
    REPLACE,
    SET,
    USE,
    SHOW,
    BEGIN,
    START_TRANSACTION,
    COMMIT,
    ROLLBACK,
    SAVEPOINT,
    PREPARE,
    EXECUTE,
    DEALLOCATE,
    CREATE,
    ALTER,
    DROP,
    TRUNCATE,
    GRANT,
    REVOKE,
    LOCK,
    UNLOCK,
    LOAD_DATA,
    RESET,  // PostgreSQL RESET
};

// -- AST node types --

enum class NodeType : uint16_t {
    NODE_UNKNOWN = 0,

    // Tier 2 lightweight nodes
    NODE_STATEMENT,          // root wrapper with StmtType in flags
    NODE_TABLE_REF,          // table name
    NODE_SCHEMA_REF,         // schema/database name
    NODE_IDENTIFIER,
    NODE_QUALIFIED_NAME,     // schema.table or table.column

    // Tier 1 nodes (SELECT) — defined here, used in future plan
    NODE_SELECT_STMT,
    NODE_SELECT_OPTIONS,
    NODE_SELECT_ITEM_LIST,
    NODE_SELECT_ITEM,
    NODE_FROM_CLAUSE,
    NODE_JOIN_CLAUSE,
    NODE_WHERE_CLAUSE,
    NODE_GROUP_BY_CLAUSE,
    NODE_HAVING_CLAUSE,
    NODE_ORDER_BY_CLAUSE,
    NODE_ORDER_BY_ITEM,
    NODE_LIMIT_CLAUSE,
    NODE_LOCKING_CLAUSE,
    NODE_INTO_CLAUSE,
    NODE_ALIAS,

    // Tier 1 nodes (SET) — defined here, used in future plan
    NODE_SET_STMT,
    NODE_SET_NAMES,
    NODE_SET_CHARSET,
    NODE_SET_TRANSACTION,
    NODE_VAR_ASSIGNMENT,
    NODE_VAR_TARGET,

    // Expression nodes — defined here, used in future plan
    NODE_EXPRESSION,
    NODE_BINARY_OP,
    NODE_UNARY_OP,
    NODE_FUNCTION_CALL,
    NODE_LITERAL_INT,
    NODE_LITERAL_FLOAT,
    NODE_LITERAL_STRING,
    NODE_LITERAL_NULL,
    NODE_PLACEHOLDER,        // ? or $N
    NODE_SUBQUERY,
    NODE_COLUMN_REF,
    NODE_ASTERISK,
    NODE_IS_NULL,
    NODE_IS_NOT_NULL,
    NODE_BETWEEN,
    NODE_IN_LIST,
    NODE_CASE_WHEN,
};

} // namespace sql_parser

#endif // SQL_PARSER_COMMON_H
```

- [ ] **Step 2: Write a compile test**

This is a header-only file. Verify it compiles:

Run:
```bash
echo '#include "sql_parser/common.h"' > /tmp/test_common.cpp && \
echo 'int main() { sql_parser::StringRef s; return s.empty() ? 0 : 1; }' >> /tmp/test_common.cpp && \
g++ -std=c++17 -I./include -c /tmp/test_common.cpp -o /dev/null && echo "OK"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add include/sql_parser/common.h
git commit -m "feat: add core types — StringRef, Dialect, StmtType, NodeType enums"
```

---

### Task 3: Arena Allocator

**Files:**
- Create: `include/sql_parser/arena.h`
- Create: `src/sql_parser/arena.cpp`
- Create: `tests/test_arena.cpp`

- [ ] **Step 1: Write the failing test**

Create `tests/test_arena.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/arena.h"

using namespace sql_parser;

TEST(ArenaTest, AllocateAndReset) {
    Arena arena(4096);  // 4KB block
    void* p1 = arena.allocate(64);
    ASSERT_NE(p1, nullptr);
    void* p2 = arena.allocate(64);
    ASSERT_NE(p2, nullptr);
    EXPECT_NE(p1, p2);

    arena.reset();
    // After reset, next allocation reuses the same block
    void* p3 = arena.allocate(64);
    ASSERT_NE(p3, nullptr);
    EXPECT_EQ(p1, p3);  // same address — block was reused
}

TEST(ArenaTest, AllocateAligned) {
    Arena arena(4096);
    void* p1 = arena.allocate(1);  // 1 byte
    void* p2 = arena.allocate(8);  // should be 8-byte aligned
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p2) % 8, 0u);
}

TEST(ArenaTest, OverflowToNewBlock) {
    Arena arena(128);  // small block
    // Allocate more than one block's worth
    void* p1 = arena.allocate(100);
    ASSERT_NE(p1, nullptr);
    void* p2 = arena.allocate(100);  // should overflow to second block
    ASSERT_NE(p2, nullptr);
    EXPECT_NE(p1, p2);
}

TEST(ArenaTest, ResetFreesOverflowBlocks) {
    Arena arena(128);
    arena.allocate(100);
    arena.allocate(100);  // overflow block allocated
    arena.reset();
    // First allocation after reset should be in the primary block
    void* p = arena.allocate(64);
    ASSERT_NE(p, nullptr);
}

TEST(ArenaTest, MaxSizeEnforced) {
    Arena arena(128, 256);  // 128 block size, 256 max total
    void* p1 = arena.allocate(100);
    ASSERT_NE(p1, nullptr);
    void* p2 = arena.allocate(100);
    ASSERT_NE(p2, nullptr);
    // Third allocation exceeds 256 max
    void* p3 = arena.allocate(100);
    EXPECT_EQ(p3, nullptr);
}

TEST(ArenaTest, AllocateTyped) {
    Arena arena(4096);

    struct TestStruct {
        int a;
        double b;
    };

    TestStruct* ts = arena.allocate_typed<TestStruct>();
    ASSERT_NE(ts, nullptr);
    ts->a = 42;
    ts->b = 3.14;
    EXPECT_EQ(ts->a, 42);
    EXPECT_DOUBLE_EQ(ts->b, 3.14);
}

TEST(ArenaTest, AllocateString) {
    Arena arena(4096);
    const char* src = "hello world";
    StringRef ref = arena.allocate_string(src, 11);
    EXPECT_EQ(ref.len, 11u);
    EXPECT_EQ(std::memcmp(ref.ptr, "hello world", 11), 0);
    // Should be a copy, not the same pointer
    EXPECT_NE(ref.ptr, src);
}
```

- [ ] **Step 2: Write arena.h**

Create `include/sql_parser/arena.h`:
```cpp
#ifndef SQL_PARSER_ARENA_H
#define SQL_PARSER_ARENA_H

#include "sql_parser/common.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>

namespace sql_parser {

class Arena {
public:
    explicit Arena(size_t block_size = 65536, size_t max_size = 1048576);
    ~Arena();

    // Non-copyable, non-movable
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    Arena(Arena&&) = delete;
    Arena& operator=(Arena&&) = delete;

    // Allocate raw bytes (8-byte aligned). Returns nullptr if max_size exceeded.
    void* allocate(size_t bytes);

    // Allocate and default-construct a typed object.
    template <typename T>
    T* allocate_typed() {
        void* mem = allocate(sizeof(T));
        if (!mem) return nullptr;
        return new (mem) T{};
    }

    // Copy a string into the arena and return a StringRef to the copy.
    StringRef allocate_string(const char* src, uint32_t len);

    // Reset: rewind primary block, free overflow blocks. O(1) in common case.
    void reset();

    // Current total bytes allocated (across all blocks).
    size_t bytes_used() const;

private:
    struct Block {
        Block* next;
        size_t capacity;
        size_t used;
        // Data follows immediately after this header.
        char* data() { return reinterpret_cast<char*>(this) + sizeof(Block); }
    };

    Block* allocate_block(size_t capacity);

    Block* primary_;
    Block* current_;
    size_t block_size_;
    size_t max_size_;
    size_t total_allocated_;
};

} // namespace sql_parser

#endif // SQL_PARSER_ARENA_H
```

- [ ] **Step 3: Write arena.cpp**

Create `src/sql_parser/arena.cpp`:
```cpp
#include "sql_parser/arena.h"
#include <cstdlib>

namespace sql_parser {

Arena::Block* Arena::allocate_block(size_t capacity) {
    void* mem = std::malloc(sizeof(Block) + capacity);
    if (!mem) return nullptr;
    Block* block = static_cast<Block*>(mem);
    block->next = nullptr;
    block->capacity = capacity;
    block->used = 0;
    return block;
}

Arena::Arena(size_t block_size, size_t max_size)
    : block_size_(block_size), max_size_(max_size), total_allocated_(0) {
    primary_ = allocate_block(block_size_);
    current_ = primary_;
    total_allocated_ = block_size_;
}

Arena::~Arena() {
    Block* b = primary_;
    while (b) {
        Block* next = b->next;
        std::free(b);
        b = next;
    }
}

void* Arena::allocate(size_t bytes) {
    // Align to 8 bytes
    bytes = (bytes + 7) & ~size_t(7);

    // Try current block
    if (current_->used + bytes <= current_->capacity) {
        void* ptr = current_->data() + current_->used;
        current_->used += bytes;
        return ptr;
    }

    // Need a new block
    size_t new_cap = (bytes > block_size_) ? bytes : block_size_;
    if (total_allocated_ + new_cap > max_size_) {
        return nullptr;  // max size exceeded
    }

    Block* new_block = allocate_block(new_cap);
    if (!new_block) return nullptr;

    current_->next = new_block;
    current_ = new_block;
    total_allocated_ += new_cap;

    void* ptr = current_->data() + current_->used;
    current_->used += bytes;
    return ptr;
}

StringRef Arena::allocate_string(const char* src, uint32_t len) {
    void* mem = allocate(len);
    if (!mem) return StringRef{nullptr, 0};
    std::memcpy(mem, src, len);
    return StringRef{static_cast<const char*>(mem), len};
}

void Arena::reset() {
    // Free overflow blocks
    Block* b = primary_->next;
    while (b) {
        Block* next = b->next;
        std::free(b);
        b = next;
    }
    primary_->next = nullptr;
    primary_->used = 0;
    current_ = primary_;
    total_allocated_ = block_size_;
}

size_t Arena::bytes_used() const {
    size_t used = 0;
    const Block* b = primary_;
    while (b) {
        used += b->used;
        b = b->next;
    }
    return used;
}

} // namespace sql_parser
```

- [ ] **Step 4: Build and run tests**

Run:
```bash
make -f Makefile.new test
```
Expected: All ArenaTest tests PASS.

- [ ] **Step 5: Commit**

```bash
git add include/sql_parser/arena.h src/sql_parser/arena.cpp tests/test_arena.cpp
git commit -m "feat: add arena allocator with block chaining and max size"
```

---

### Task 4: AstNode and ParseResult

**Files:**
- Create: `include/sql_parser/ast.h`
- Create: `include/sql_parser/parse_result.h`

- [ ] **Step 1: Write ast.h**

Create `include/sql_parser/ast.h`:
```cpp
#ifndef SQL_PARSER_AST_H
#define SQL_PARSER_AST_H

#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include <cstdint>
#include <type_traits>

namespace sql_parser {

struct AstNode {
    AstNode* first_child;
    AstNode* next_sibling;
    const char* value_ptr;
    uint32_t value_len;
    NodeType type;
    uint16_t flags;

    // Convenience accessors
    StringRef value() const { return StringRef{value_ptr, value_len}; }

    void set_value(StringRef ref) {
        value_ptr = ref.ptr;
        value_len = ref.len;
    }

    // Append child to end of child list
    void add_child(AstNode* child) {
        if (!child) return;
        if (!first_child) {
            first_child = child;
            return;
        }
        AstNode* last = first_child;
        while (last->next_sibling) last = last->next_sibling;
        last->next_sibling = child;
    }
};
static_assert(sizeof(AstNode) == 32, "AstNode must be 32 bytes");
static_assert(std::is_trivially_copyable_v<AstNode>);

// Factory: allocate an AstNode from the arena
inline AstNode* make_node(Arena& arena, NodeType type, StringRef value = {},
                          uint16_t flags = 0) {
    AstNode* node = arena.allocate_typed<AstNode>();
    if (!node) return nullptr;
    node->type = type;
    node->flags = flags;
    node->value_ptr = value.ptr;
    node->value_len = value.len;
    return node;
}

} // namespace sql_parser

#endif // SQL_PARSER_AST_H
```

- [ ] **Step 2: Write parse_result.h**

Create `include/sql_parser/parse_result.h`:
```cpp
#ifndef SQL_PARSER_PARSE_RESULT_H
#define SQL_PARSER_PARSE_RESULT_H

#include "sql_parser/common.h"
#include "sql_parser/ast.h"

namespace sql_parser {

struct ErrorInfo {
    uint32_t offset = 0;
    StringRef message;
};

struct ParseResult {
    enum Status : uint8_t { OK = 0, PARTIAL, ERROR };

    Status status = ERROR;
    StmtType stmt_type = StmtType::UNKNOWN;
    AstNode* ast = nullptr;
    ErrorInfo error;
    StringRef remaining;     // unparsed input after semicolon

    // Tier 2 extracted metadata
    StringRef table_name;
    StringRef schema_name;
    StringRef database_name; // for USE statements

    bool ok() const { return status == OK; }
    bool has_remaining() const { return !remaining.empty(); }
};

} // namespace sql_parser

#endif // SQL_PARSER_PARSE_RESULT_H
```

- [ ] **Step 3: Compile test**

Run:
```bash
echo '#include "sql_parser/parse_result.h"' > /tmp/test_pr.cpp && \
echo 'int main() { sql_parser::ParseResult r; return r.ok() ? 0 : 1; }' >> /tmp/test_pr.cpp && \
g++ -std=c++17 -I./include -c /tmp/test_pr.cpp -o /dev/null && echo "OK"
```
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add include/sql_parser/ast.h include/sql_parser/parse_result.h
git commit -m "feat: add AstNode (32-byte) and ParseResult structs"
```

---

### Task 5: Token Types and Keyword Tables

**Files:**
- Create: `include/sql_parser/token.h`
- Create: `include/sql_parser/keywords_mysql.h`
- Create: `include/sql_parser/keywords_pgsql.h`

- [ ] **Step 1: Write token.h**

Create `include/sql_parser/token.h`:
```cpp
#ifndef SQL_PARSER_TOKEN_H
#define SQL_PARSER_TOKEN_H

#include "sql_parser/common.h"
#include <cstdint>

namespace sql_parser {

enum class TokenType : uint16_t {
    // End / error
    TK_EOF = 0,
    TK_ERROR,

    // Literals
    TK_IDENTIFIER,
    TK_INTEGER,
    TK_FLOAT,
    TK_STRING,

    // Punctuation
    TK_LPAREN,        // (
    TK_RPAREN,        // )
    TK_COMMA,         // ,
    TK_SEMICOLON,     // ;
    TK_DOT,           // .
    TK_ASTERISK,      // *
    TK_PLUS,          // +
    TK_MINUS,         // -
    TK_SLASH,         // /
    TK_PERCENT,       // %
    TK_EQUAL,         // =
    TK_NOT_EQUAL,     // != or <>
    TK_LESS,          // <
    TK_GREATER,       // >
    TK_LESS_EQUAL,    // <=
    TK_GREATER_EQUAL, // >=
    TK_AMPERSAND,     // &
    TK_PIPE,          // |
    TK_CARET,         // ^
    TK_TILDE,         // ~
    TK_EXCLAIM,       // !
    TK_COLON,         // :
    TK_QUESTION,      // ?
    TK_AT,            // @
    TK_DOUBLE_AT,     // @@
    TK_HASH,          // #

    // MySQL-specific operators
    TK_COLON_EQUAL,   // :=
    TK_DOUBLE_PIPE,   // || (also PgSQL string concat)

    // PostgreSQL-specific operators
    TK_DOUBLE_COLON,  // ::
    TK_DOLLAR_NUM,    // $1, $2 etc. (prepared stmt placeholder)

    // Keywords — DML
    TK_SELECT,
    TK_INSERT,
    TK_UPDATE,
    TK_DELETE,
    TK_REPLACE,
    TK_FROM,
    TK_WHERE,
    TK_SET,
    TK_INTO,
    TK_VALUES,
    TK_AS,
    TK_ON,
    TK_USING,

    // Keywords — clauses
    TK_JOIN,
    TK_INNER,
    TK_LEFT,
    TK_RIGHT,
    TK_FULL,
    TK_OUTER,
    TK_CROSS,
    TK_NATURAL,
    TK_ORDER,
    TK_BY,
    TK_GROUP,
    TK_HAVING,
    TK_LIMIT,
    TK_OFFSET,
    TK_FETCH,
    TK_ASC,
    TK_DESC,
    TK_DISTINCT,
    TK_ALL,

    // Keywords — logical / comparison
    TK_AND,
    TK_OR,
    TK_NOT,
    TK_IS,
    TK_NULL,
    TK_IN,
    TK_BETWEEN,
    TK_LIKE,
    TK_EXISTS,
    TK_CASE,
    TK_WHEN,
    TK_THEN,
    TK_ELSE,
    TK_END,
    TK_TRUE,
    TK_FALSE,

    // Keywords — SET
    TK_NAMES,
    TK_CHARACTER,
    TK_CHARSET,
    TK_COLLATE,
    TK_GLOBAL,
    TK_SESSION,
    TK_LOCAL,
    TK_PERSIST,
    TK_DEFAULT,
    TK_TRANSACTION,
    TK_ISOLATION,
    TK_LEVEL,
    TK_READ,
    TK_WRITE,
    TK_ONLY,
    TK_COMMITTED,
    TK_UNCOMMITTED,
    TK_REPEATABLE,
    TK_SERIALIZABLE,
    TK_TO,

    // Keywords — DDL
    TK_CREATE,
    TK_ALTER,
    TK_DROP,
    TK_TRUNCATE,
    TK_TABLE,
    TK_INDEX,
    TK_VIEW,
    TK_DATABASE,
    TK_SCHEMA,
    TK_IF,

    // Keywords — transaction
    TK_BEGIN,
    TK_START,
    TK_COMMIT,
    TK_ROLLBACK,
    TK_SAVEPOINT,

    // Keywords — other
    TK_USE,
    TK_SHOW,
    TK_PREPARE,
    TK_EXECUTE,
    TK_DEALLOCATE,
    TK_GRANT,
    TK_REVOKE,
    TK_LOCK,
    TK_UNLOCK,
    TK_LOAD,
    TK_DATA,
    TK_FOR,
    TK_SHARE,
    TK_NOWAIT,
    TK_SKIP,
    TK_LOCKED,
    TK_OUTFILE,
    TK_DUMPFILE,
    TK_IGNORE,
    TK_LOW_PRIORITY,
    TK_QUICK,
    TK_RESET,

    // MySQL-specific keywords
    TK_SQL_CALC_FOUND_ROWS,

    // Aggregate / functions (recognized as keywords for fast dispatch)
    TK_COUNT,
    TK_SUM,
    TK_AVG,
    TK_MIN,
    TK_MAX,
};

struct Token {
    TokenType type = TokenType::TK_EOF;
    StringRef text;
    uint32_t offset = 0;
};

} // namespace sql_parser

#endif // SQL_PARSER_TOKEN_H
```

- [ ] **Step 2: Write keywords_mysql.h**

Create `include/sql_parser/keywords_mysql.h`:

This uses a sorted array + binary search for keyword lookup. A perfect hash can be added later as an optimization.

```cpp
#ifndef SQL_PARSER_KEYWORDS_MYSQL_H
#define SQL_PARSER_KEYWORDS_MYSQL_H

#include "sql_parser/token.h"
#include <algorithm>
#include <cstring>

namespace sql_parser {
namespace mysql_keywords {

struct KeywordEntry {
    const char* text;
    uint8_t len;
    TokenType token;
};

// Sorted by text (case-insensitive) for binary search.
// Must stay sorted when adding entries.
inline constexpr KeywordEntry KEYWORDS[] = {
    {"ALL", 3, TokenType::TK_ALL},
    {"ALTER", 5, TokenType::TK_ALTER},
    {"AND", 3, TokenType::TK_AND},
    {"AS", 2, TokenType::TK_AS},
    {"ASC", 3, TokenType::TK_ASC},
    {"AVG", 3, TokenType::TK_AVG},
    {"BEGIN", 5, TokenType::TK_BEGIN},
    {"BETWEEN", 7, TokenType::TK_BETWEEN},
    {"BY", 2, TokenType::TK_BY},
    {"CASE", 4, TokenType::TK_CASE},
    {"CHARACTER", 9, TokenType::TK_CHARACTER},
    {"CHARSET", 7, TokenType::TK_CHARSET},
    {"COLLATE", 7, TokenType::TK_COLLATE},
    {"COMMIT", 6, TokenType::TK_COMMIT},
    {"COMMITTED", 9, TokenType::TK_COMMITTED},
    {"COUNT", 5, TokenType::TK_COUNT},
    {"CREATE", 6, TokenType::TK_CREATE},
    {"CROSS", 5, TokenType::TK_CROSS},
    {"DATA", 4, TokenType::TK_DATA},
    {"DATABASE", 8, TokenType::TK_DATABASE},
    {"DEALLOCATE", 10, TokenType::TK_DEALLOCATE},
    {"DEFAULT", 7, TokenType::TK_DEFAULT},
    {"DELETE", 6, TokenType::TK_DELETE},
    {"DESC", 4, TokenType::TK_DESC},
    {"DISTINCT", 8, TokenType::TK_DISTINCT},
    {"DROP", 4, TokenType::TK_DROP},
    {"DUMPFILE", 8, TokenType::TK_DUMPFILE},
    {"ELSE", 4, TokenType::TK_ELSE},
    {"END", 3, TokenType::TK_END},
    {"EXECUTE", 7, TokenType::TK_EXECUTE},
    {"EXISTS", 6, TokenType::TK_EXISTS},
    {"FALSE", 5, TokenType::TK_FALSE},
    {"FETCH", 5, TokenType::TK_FETCH},
    {"FOR", 3, TokenType::TK_FOR},
    {"FROM", 4, TokenType::TK_FROM},
    {"FULL", 4, TokenType::TK_FULL},
    {"GLOBAL", 6, TokenType::TK_GLOBAL},
    {"GRANT", 5, TokenType::TK_GRANT},
    {"GROUP", 5, TokenType::TK_GROUP},
    {"HAVING", 6, TokenType::TK_HAVING},
    {"IF", 2, TokenType::TK_IF},
    {"IGNORE", 6, TokenType::TK_IGNORE},
    {"IN", 2, TokenType::TK_IN},
    {"INDEX", 5, TokenType::TK_INDEX},
    {"INNER", 5, TokenType::TK_INNER},
    {"INSERT", 6, TokenType::TK_INSERT},
    {"INTO", 4, TokenType::TK_INTO},
    {"IS", 2, TokenType::TK_IS},
    {"ISOLATION", 9, TokenType::TK_ISOLATION},
    {"JOIN", 4, TokenType::TK_JOIN},
    {"LEFT", 4, TokenType::TK_LEFT},
    {"LEVEL", 5, TokenType::TK_LEVEL},
    {"LIKE", 4, TokenType::TK_LIKE},
    {"LIMIT", 5, TokenType::TK_LIMIT},
    {"LOAD", 4, TokenType::TK_LOAD},
    {"LOCAL", 5, TokenType::TK_LOCAL},
    {"LOCK", 4, TokenType::TK_LOCK},
    {"LOCKED", 6, TokenType::TK_LOCKED},
    {"LOW_PRIORITY", 12, TokenType::TK_LOW_PRIORITY},
    {"MAX", 3, TokenType::TK_MAX},
    {"MIN", 3, TokenType::TK_MIN},
    {"NAMES", 5, TokenType::TK_NAMES},
    {"NATURAL", 7, TokenType::TK_NATURAL},
    {"NOT", 3, TokenType::TK_NOT},
    {"NOWAIT", 6, TokenType::TK_NOWAIT},
    {"NULL", 4, TokenType::TK_NULL},
    {"OFFSET", 6, TokenType::TK_OFFSET},
    {"ON", 2, TokenType::TK_ON},
    {"ONLY", 4, TokenType::TK_ONLY},
    {"OR", 2, TokenType::TK_OR},
    {"ORDER", 5, TokenType::TK_ORDER},
    {"OUTER", 5, TokenType::TK_OUTER},
    {"OUTFILE", 7, TokenType::TK_OUTFILE},
    {"PERSIST", 7, TokenType::TK_PERSIST},
    {"PREPARE", 7, TokenType::TK_PREPARE},
    {"QUICK", 5, TokenType::TK_QUICK},
    {"READ", 4, TokenType::TK_READ},
    {"REPEATABLE", 10, TokenType::TK_REPEATABLE},
    {"REPLACE", 7, TokenType::TK_REPLACE},
    {"RESET", 5, TokenType::TK_RESET},
    {"REVOKE", 6, TokenType::TK_REVOKE},
    {"RIGHT", 5, TokenType::TK_RIGHT},
    {"ROLLBACK", 8, TokenType::TK_ROLLBACK},
    {"SAVEPOINT", 9, TokenType::TK_SAVEPOINT},
    {"SCHEMA", 6, TokenType::TK_SCHEMA},
    {"SELECT", 6, TokenType::TK_SELECT},
    {"SERIALIZABLE", 12, TokenType::TK_SERIALIZABLE},
    {"SESSION", 7, TokenType::TK_SESSION},
    {"SET", 3, TokenType::TK_SET},
    {"SHARE", 5, TokenType::TK_SHARE},
    {"SHOW", 4, TokenType::TK_SHOW},
    {"SKIP", 4, TokenType::TK_SKIP},
    {"SQL_CALC_FOUND_ROWS", 19, TokenType::TK_SQL_CALC_FOUND_ROWS},
    {"START", 5, TokenType::TK_START},
    {"SUM", 3, TokenType::TK_SUM},
    {"TABLE", 5, TokenType::TK_TABLE},
    {"THEN", 4, TokenType::TK_THEN},
    {"TO", 2, TokenType::TK_TO},
    {"TRANSACTION", 11, TokenType::TK_TRANSACTION},
    {"TRUE", 4, TokenType::TK_TRUE},
    {"TRUNCATE", 8, TokenType::TK_TRUNCATE},
    {"UNCOMMITTED", 11, TokenType::TK_UNCOMMITTED},
    {"UNLOCK", 6, TokenType::TK_UNLOCK},
    {"UPDATE", 6, TokenType::TK_UPDATE},
    {"USE", 3, TokenType::TK_USE},
    {"USING", 5, TokenType::TK_USING},
    {"VALUES", 6, TokenType::TK_VALUES},
    {"VIEW", 4, TokenType::TK_VIEW},
    {"WHEN", 4, TokenType::TK_WHEN},
    {"WHERE", 5, TokenType::TK_WHERE},
    {"WRITE", 5, TokenType::TK_WRITE},
};

inline constexpr size_t KEYWORD_COUNT = sizeof(KEYWORDS) / sizeof(KEYWORDS[0]);

// Returns TK_IDENTIFIER if not a keyword.
inline TokenType lookup(const char* text, uint32_t len) {
    size_t lo = 0, hi = KEYWORD_COUNT;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int cmp = sql_parser::ci_cmp(text, len, KEYWORDS[mid].text, KEYWORDS[mid].len);
        if (cmp == 0) return KEYWORDS[mid].token;
        if (cmp < 0) hi = mid;
        else lo = mid + 1;
    }
    return TokenType::TK_IDENTIFIER;
}

} // namespace mysql_keywords
} // namespace sql_parser

#endif // SQL_PARSER_KEYWORDS_MYSQL_H
```

- [ ] **Step 3: Write keywords_pgsql.h**

Create `include/sql_parser/keywords_pgsql.h`:

Same structure, with PostgreSQL-specific keyword set. Uses the same lookup pattern.

```cpp
#ifndef SQL_PARSER_KEYWORDS_PGSQL_H
#define SQL_PARSER_KEYWORDS_PGSQL_H

#include "sql_parser/token.h"

namespace sql_parser {
namespace pgsql_keywords {

struct KeywordEntry {
    const char* text;
    uint8_t len;
    TokenType token;
};

// Sorted by text (case-insensitive) for binary search.
// PostgreSQL shares most keywords with MySQL; main differences:
// - No SQL_CALC_FOUND_ROWS
// - RESET is a first-class keyword (not just for SET)
// - TO is used in SET x TO y
inline constexpr KeywordEntry KEYWORDS[] = {
    {"ALL", 3, TokenType::TK_ALL},
    {"ALTER", 5, TokenType::TK_ALTER},
    {"AND", 3, TokenType::TK_AND},
    {"AS", 2, TokenType::TK_AS},
    {"ASC", 3, TokenType::TK_ASC},
    {"AVG", 3, TokenType::TK_AVG},
    {"BEGIN", 5, TokenType::TK_BEGIN},
    {"BETWEEN", 7, TokenType::TK_BETWEEN},
    {"BY", 2, TokenType::TK_BY},
    {"CASE", 4, TokenType::TK_CASE},
    {"CHARACTER", 9, TokenType::TK_CHARACTER},
    {"COLLATE", 7, TokenType::TK_COLLATE},
    {"COMMIT", 6, TokenType::TK_COMMIT},
    {"COMMITTED", 9, TokenType::TK_COMMITTED},
    {"COUNT", 5, TokenType::TK_COUNT},
    {"CREATE", 6, TokenType::TK_CREATE},
    {"CROSS", 5, TokenType::TK_CROSS},
    {"DATA", 4, TokenType::TK_DATA},
    {"DATABASE", 8, TokenType::TK_DATABASE},
    {"DEALLOCATE", 10, TokenType::TK_DEALLOCATE},
    {"DEFAULT", 7, TokenType::TK_DEFAULT},
    {"DELETE", 6, TokenType::TK_DELETE},
    {"DESC", 4, TokenType::TK_DESC},
    {"DISTINCT", 8, TokenType::TK_DISTINCT},
    {"DROP", 4, TokenType::TK_DROP},
    {"ELSE", 4, TokenType::TK_ELSE},
    {"END", 3, TokenType::TK_END},
    {"EXECUTE", 7, TokenType::TK_EXECUTE},
    {"EXISTS", 6, TokenType::TK_EXISTS},
    {"FALSE", 5, TokenType::TK_FALSE},
    {"FETCH", 5, TokenType::TK_FETCH},
    {"FOR", 3, TokenType::TK_FOR},
    {"FROM", 4, TokenType::TK_FROM},
    {"FULL", 4, TokenType::TK_FULL},
    {"GRANT", 5, TokenType::TK_GRANT},
    {"GROUP", 5, TokenType::TK_GROUP},
    {"HAVING", 6, TokenType::TK_HAVING},
    {"IF", 2, TokenType::TK_IF},
    {"IN", 2, TokenType::TK_IN},
    {"INDEX", 5, TokenType::TK_INDEX},
    {"INNER", 5, TokenType::TK_INNER},
    {"INSERT", 6, TokenType::TK_INSERT},
    {"INTO", 4, TokenType::TK_INTO},
    {"IS", 2, TokenType::TK_IS},
    {"ISOLATION", 9, TokenType::TK_ISOLATION},
    {"JOIN", 4, TokenType::TK_JOIN},
    {"LEFT", 4, TokenType::TK_LEFT},
    {"LEVEL", 5, TokenType::TK_LEVEL},
    {"LIKE", 4, TokenType::TK_LIKE},
    {"LIMIT", 5, TokenType::TK_LIMIT},
    {"LOAD", 4, TokenType::TK_LOAD},
    {"LOCAL", 5, TokenType::TK_LOCAL},
    {"LOCK", 4, TokenType::TK_LOCK},
    {"MAX", 3, TokenType::TK_MAX},
    {"MIN", 3, TokenType::TK_MIN},
    {"NAMES", 5, TokenType::TK_NAMES},
    {"NATURAL", 7, TokenType::TK_NATURAL},
    {"NOT", 3, TokenType::TK_NOT},
    {"NULL", 4, TokenType::TK_NULL},
    {"OFFSET", 6, TokenType::TK_OFFSET},
    {"ON", 2, TokenType::TK_ON},
    {"ONLY", 4, TokenType::TK_ONLY},
    {"OR", 2, TokenType::TK_OR},
    {"ORDER", 5, TokenType::TK_ORDER},
    {"OUTER", 5, TokenType::TK_OUTER},
    {"PREPARE", 7, TokenType::TK_PREPARE},
    {"READ", 4, TokenType::TK_READ},
    {"REPEATABLE", 10, TokenType::TK_REPEATABLE},
    {"RESET", 5, TokenType::TK_RESET},
    {"REVOKE", 6, TokenType::TK_REVOKE},
    {"RIGHT", 5, TokenType::TK_RIGHT},
    {"ROLLBACK", 8, TokenType::TK_ROLLBACK},
    {"SAVEPOINT", 9, TokenType::TK_SAVEPOINT},
    {"SCHEMA", 6, TokenType::TK_SCHEMA},
    {"SELECT", 6, TokenType::TK_SELECT},
    {"SERIALIZABLE", 12, TokenType::TK_SERIALIZABLE},
    {"SESSION", 7, TokenType::TK_SESSION},
    {"SET", 3, TokenType::TK_SET},
    {"SHARE", 5, TokenType::TK_SHARE},
    {"SHOW", 4, TokenType::TK_SHOW},
    {"START", 5, TokenType::TK_START},
    {"SUM", 3, TokenType::TK_SUM},
    {"TABLE", 5, TokenType::TK_TABLE},
    {"THEN", 4, TokenType::TK_THEN},
    {"TO", 2, TokenType::TK_TO},
    {"TRANSACTION", 11, TokenType::TK_TRANSACTION},
    {"TRUE", 4, TokenType::TK_TRUE},
    {"TRUNCATE", 8, TokenType::TK_TRUNCATE},
    {"UNCOMMITTED", 11, TokenType::TK_UNCOMMITTED},
    {"UNLOCK", 6, TokenType::TK_UNLOCK},
    {"UPDATE", 6, TokenType::TK_UPDATE},
    {"USE", 3, TokenType::TK_USE},
    {"USING", 5, TokenType::TK_USING},
    {"VALUES", 6, TokenType::TK_VALUES},
    {"VIEW", 4, TokenType::TK_VIEW},
    {"WHEN", 4, TokenType::TK_WHEN},
    {"WHERE", 5, TokenType::TK_WHERE},
    {"WRITE", 5, TokenType::TK_WRITE},
};

inline constexpr size_t KEYWORD_COUNT = sizeof(KEYWORDS) / sizeof(KEYWORDS[0]);

// Uses ci_cmp from common.h
inline TokenType lookup(const char* text, uint32_t len) {
    size_t lo = 0, hi = KEYWORD_COUNT;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int cmp = sql_parser::ci_cmp(text, len, KEYWORDS[mid].text, KEYWORDS[mid].len);
        if (cmp == 0) return KEYWORDS[mid].token;
        if (cmp < 0) hi = mid;
        else lo = mid + 1;
    }
    return TokenType::TK_IDENTIFIER;
}

} // namespace pgsql_keywords
} // namespace sql_parser

#endif // SQL_PARSER_KEYWORDS_PGSQL_H
```

- [ ] **Step 4: Compile test**

Run:
```bash
echo '#include "sql_parser/keywords_mysql.h"' > /tmp/test_kw.cpp && \
echo '#include "sql_parser/keywords_pgsql.h"' >> /tmp/test_kw.cpp && \
echo '#include <cassert>' >> /tmp/test_kw.cpp && \
echo 'int main() {' >> /tmp/test_kw.cpp && \
echo '  assert(sql_parser::mysql_keywords::lookup("SELECT", 6) == sql_parser::TokenType::TK_SELECT);' >> /tmp/test_kw.cpp && \
echo '  assert(sql_parser::mysql_keywords::lookup("select", 6) == sql_parser::TokenType::TK_SELECT);' >> /tmp/test_kw.cpp && \
echo '  assert(sql_parser::mysql_keywords::lookup("foobar", 6) == sql_parser::TokenType::TK_IDENTIFIER);' >> /tmp/test_kw.cpp && \
echo '  return 0; }' >> /tmp/test_kw.cpp && \
g++ -std=c++17 -I./include /tmp/test_kw.cpp -o /dev/null && echo "OK"
```
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add include/sql_parser/token.h include/sql_parser/keywords_mysql.h include/sql_parser/keywords_pgsql.h
git commit -m "feat: add token types and keyword lookup tables for MySQL and PostgreSQL"
```

---

### Task 6: Tokenizer

**Files:**
- Create: `include/sql_parser/tokenizer.h`
- Create: `tests/test_tokenizer.cpp`

- [ ] **Step 1: Write the failing test**

Create `tests/test_tokenizer.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/tokenizer.h"

using namespace sql_parser;

// ========== MySQL Tokenizer Tests ==========

class MySQLTokenizerTest : public ::testing::Test {
protected:
    Tokenizer<Dialect::MySQL> tok;
};

TEST_F(MySQLTokenizerTest, SimpleSelect) {
    const char* sql = "SELECT * FROM users;";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_SELECT);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_ASTERISK);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_FROM);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
    EXPECT_EQ(t.text.len, 5u);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "users");

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_SEMICOLON);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_EOF);
}

TEST_F(MySQLTokenizerTest, CaseInsensitiveKeywords) {
    const char* sql = "select FROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, BacktickIdentifier) {
    const char* sql = "`my table`";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "my table");
}

TEST_F(MySQLTokenizerTest, SingleQuotedString) {
    const char* sql = "'hello world'";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_STRING);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "hello world");
}

TEST_F(MySQLTokenizerTest, IntegerLiteral) {
    const char* sql = "42";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_INTEGER);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "42");
}

TEST_F(MySQLTokenizerTest, FloatLiteral) {
    const char* sql = "3.14";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_FLOAT);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "3.14");
}

TEST_F(MySQLTokenizerTest, ComparisonOperators) {
    const char* sql = "= != < > <= >=";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_EQUAL);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_NOT_EQUAL);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_LESS);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_GREATER);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_LESS_EQUAL);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_GREATER_EQUAL);
}

TEST_F(MySQLTokenizerTest, DiamondNotEqual) {
    const char* sql = "<>";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_NOT_EQUAL);
}

TEST_F(MySQLTokenizerTest, AtVariables) {
    const char* sql = "@myvar @@global_var";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_AT);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_DOUBLE_AT);

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
}

TEST_F(MySQLTokenizerTest, Placeholder) {
    const char* sql = "?";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_QUESTION);
}

TEST_F(MySQLTokenizerTest, ColonEqual) {
    const char* sql = ":=";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_COLON_EQUAL);
}

TEST_F(MySQLTokenizerTest, LineComment) {
    const char* sql = "SELECT -- this is a comment\nFROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, HashComment) {
    const char* sql = "SELECT # comment\nFROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, BlockComment) {
    const char* sql = "SELECT /* comment */ FROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, PeekDoesNotConsume) {
    const char* sql = "SELECT FROM";
    tok.reset(sql, strlen(sql));

    Token peeked = tok.peek();
    EXPECT_EQ(peeked.type, TokenType::TK_SELECT);

    Token consumed = tok.next_token();
    EXPECT_EQ(consumed.type, TokenType::TK_SELECT);

    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(MySQLTokenizerTest, EmptyInput) {
    tok.reset("", 0);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_EOF);
}

TEST_F(MySQLTokenizerTest, WhitespaceOnly) {
    const char* sql = "   \t\n\r  ";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_EOF);
}

TEST_F(MySQLTokenizerTest, QualifiedIdentifier) {
    const char* sql = "myschema.orders";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_IDENTIFIER); // myschema
    EXPECT_EQ(tok.next_token().type, TokenType::TK_DOT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_IDENTIFIER); // orders
}

// ========== PostgreSQL Tokenizer Tests ==========

class PgSQLTokenizerTest : public ::testing::Test {
protected:
    Tokenizer<Dialect::PostgreSQL> tok;
};

TEST_F(PgSQLTokenizerTest, DoubleQuotedIdentifier) {
    const char* sql = "\"my table\"";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_IDENTIFIER);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "my table");
}

TEST_F(PgSQLTokenizerTest, DollarQuotedString) {
    const char* sql = "$$hello world$$";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_STRING);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "hello world");
}

TEST_F(PgSQLTokenizerTest, DoubleColonCast) {
    const char* sql = "::";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_DOUBLE_COLON);
}

TEST_F(PgSQLTokenizerTest, PositionalParam) {
    const char* sql = "$1 $23";
    tok.reset(sql, strlen(sql));

    Token t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_DOLLAR_NUM);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "$1");

    t = tok.next_token();
    EXPECT_EQ(t.type, TokenType::TK_DOLLAR_NUM);
    EXPECT_EQ(std::string(t.text.ptr, t.text.len), "$23");
}

TEST_F(PgSQLTokenizerTest, NestedBlockComment) {
    const char* sql = "SELECT /* outer /* inner */ still comment */ FROM";
    tok.reset(sql, strlen(sql));

    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_FROM);
}

TEST_F(PgSQLTokenizerTest, NoHashComment) {
    // PostgreSQL does NOT support # comments — # should be TK_HASH token
    const char* sql = "#";
    tok.reset(sql, strlen(sql));
    EXPECT_EQ(tok.next_token().type, TokenType::TK_HASH);
}
```

- [ ] **Step 2: Write tokenizer.h**

Create `include/sql_parser/tokenizer.h`. This is header-only for max inlining:

```cpp
#ifndef SQL_PARSER_TOKENIZER_H
#define SQL_PARSER_TOKENIZER_H

#include "sql_parser/token.h"
#include "sql_parser/keywords_mysql.h"
#include "sql_parser/keywords_pgsql.h"

namespace sql_parser {

template <Dialect D>
class Tokenizer {
public:
    void reset(const char* input, size_t len) {
        start_ = input;
        cursor_ = input;
        end_ = input + len;
        has_peeked_ = false;
    }

    Token next_token() {
        if (has_peeked_) {
            has_peeked_ = false;
            return peeked_;
        }
        return scan_token();
    }

    Token peek() {
        if (!has_peeked_) {
            peeked_ = scan_token();
            has_peeked_ = true;
        }
        return peeked_;
    }

    void skip() {
        if (has_peeked_) {
            has_peeked_ = false;
        } else {
            scan_token();
        }
    }

    // Expose end of input for remaining-input calculation
    const char* input_end() const { return end_; }

private:
    const char* start_ = nullptr;
    const char* cursor_ = nullptr;
    const char* end_ = nullptr;
    Token peeked_;
    bool has_peeked_ = false;

    uint32_t offset() const {
        return static_cast<uint32_t>(cursor_ - start_);
    }

    char current() const { return (cursor_ < end_) ? *cursor_ : '\0'; }
    char advance() {
        char c = current();
        if (cursor_ < end_) ++cursor_;
        return c;
    }
    char peek_char(size_t ahead = 0) const {
        const char* p = cursor_ + ahead;
        return (p < end_) ? *p : '\0';
    }

    void skip_whitespace_and_comments() {
        while (cursor_ < end_) {
            char c = *cursor_;

            // Whitespace
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                ++cursor_;
                continue;
            }

            // -- line comment (MySQL requires space after --, PgSQL doesn't but we handle both)
            if (c == '-' && peek_char(1) == '-') {
                cursor_ += 2;
                while (cursor_ < end_ && *cursor_ != '\n') ++cursor_;
                continue;
            }

            // # line comment (MySQL only)
            if constexpr (D == Dialect::MySQL) {
                if (c == '#') {
                    ++cursor_;
                    while (cursor_ < end_ && *cursor_ != '\n') ++cursor_;
                    continue;
                }
            }

            // /* block comment */
            if (c == '/' && peek_char(1) == '*') {
                cursor_ += 2;
                if constexpr (D == Dialect::PostgreSQL) {
                    // PostgreSQL supports nested block comments
                    int depth = 1;
                    while (cursor_ < end_ && depth > 0) {
                        if (*cursor_ == '/' && peek_char(1) == '*') {
                            ++depth;
                            cursor_ += 2;
                        } else if (*cursor_ == '*' && peek_char(1) == '/') {
                            --depth;
                            cursor_ += 2;
                        } else {
                            ++cursor_;
                        }
                    }
                } else {
                    // MySQL: no nesting
                    while (cursor_ < end_) {
                        if (*cursor_ == '*' && peek_char(1) == '/') {
                            cursor_ += 2;
                            break;
                        }
                        ++cursor_;
                    }
                }
                continue;
            }

            break;  // not whitespace or comment
        }
    }

    Token make_token(TokenType type, const char* start, uint32_t len) {
        return Token{type, StringRef{start, len},
                     static_cast<uint32_t>(start - start_)};
    }

    Token scan_identifier_or_keyword() {
        const char* start = cursor_;
        while (cursor_ < end_) {
            char c = *cursor_;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '_') {
                ++cursor_;
            } else {
                break;
            }
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - start);

        // Keyword lookup
        TokenType kw;
        if constexpr (D == Dialect::MySQL) {
            kw = mysql_keywords::lookup(start, len);
        } else {
            kw = pgsql_keywords::lookup(start, len);
        }
        return make_token(kw, start, len);
    }

    Token scan_number() {
        const char* start = cursor_;
        bool has_dot = false;
        while (cursor_ < end_) {
            char c = *cursor_;
            if (c >= '0' && c <= '9') {
                ++cursor_;
            } else if (c == '.' && !has_dot) {
                has_dot = true;
                ++cursor_;
            } else {
                break;
            }
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - start);
        return make_token(has_dot ? TokenType::TK_FLOAT : TokenType::TK_INTEGER,
                          start, len);
    }

    Token scan_single_quoted_string() {
        ++cursor_;  // skip opening quote
        const char* content_start = cursor_;
        while (cursor_ < end_ && *cursor_ != '\'') {
            if (*cursor_ == '\\') {
                ++cursor_;  // skip escaped char
                if (cursor_ < end_) ++cursor_;
            } else {
                ++cursor_;
            }
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        if (cursor_ < end_) ++cursor_;  // skip closing quote
        return make_token(TokenType::TK_STRING, content_start, len);
    }

    // MySQL: backtick-quoted identifier
    Token scan_backtick_identifier() {
        ++cursor_;  // skip opening backtick
        const char* content_start = cursor_;
        while (cursor_ < end_ && *cursor_ != '`') ++cursor_;
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        if (cursor_ < end_) ++cursor_;  // skip closing backtick
        return make_token(TokenType::TK_IDENTIFIER, content_start, len);
    }

    // PostgreSQL: double-quoted identifier
    Token scan_double_quoted_identifier() {
        ++cursor_;  // skip opening quote
        const char* content_start = cursor_;
        while (cursor_ < end_ && *cursor_ != '"') ++cursor_;
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        if (cursor_ < end_) ++cursor_;  // skip closing quote
        return make_token(TokenType::TK_IDENTIFIER, content_start, len);
    }

    // PostgreSQL: $$...$$ dollar-quoted string
    Token scan_dollar_string() {
        // We're at the first $. Simple form: $$content$$
        cursor_ += 2;  // skip opening $$
        const char* content_start = cursor_;
        while (cursor_ < end_) {
            if (*cursor_ == '$' && peek_char(1) == '$') {
                uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
                cursor_ += 2;  // skip closing $$
                return make_token(TokenType::TK_STRING, content_start, len);
            }
            ++cursor_;
        }
        // Unterminated — return what we have
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        return make_token(TokenType::TK_STRING, content_start, len);
    }

    Token scan_token() {
        skip_whitespace_and_comments();

        if (cursor_ >= end_) {
            return make_token(TokenType::TK_EOF, cursor_, 0);
        }

        char c = *cursor_;

        // Identifiers and keywords
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
            return scan_identifier_or_keyword();
        }

        // Numbers
        if (c >= '0' && c <= '9') {
            return scan_number();
        }

        // Dot — could be start of .123 float or just dot
        if (c == '.' && cursor_ + 1 < end_ &&
            peek_char(1) >= '0' && peek_char(1) <= '9') {
            return scan_number();
        }

        // String literals
        if (c == '\'') return scan_single_quoted_string();

        // MySQL: double-quoted strings; PostgreSQL: double-quoted identifiers
        if (c == '"') {
            if constexpr (D == Dialect::MySQL) {
                // In MySQL, double quotes are strings (unless ANSI_QUOTES mode)
                ++cursor_;
                const char* content_start = cursor_;
                while (cursor_ < end_ && *cursor_ != '"') {
                    if (*cursor_ == '\\') { ++cursor_; if (cursor_ < end_) ++cursor_; }
                    else ++cursor_;
                }
                uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
                if (cursor_ < end_) ++cursor_;
                return make_token(TokenType::TK_STRING, content_start, len);
            } else {
                return scan_double_quoted_identifier();
            }
        }

        // Backtick identifier (MySQL only)
        if constexpr (D == Dialect::MySQL) {
            if (c == '`') return scan_backtick_identifier();
        }

        // @ and @@
        if (c == '@') {
            if (peek_char(1) == '@') {
                const char* s = cursor_;
                cursor_ += 2;
                return make_token(TokenType::TK_DOUBLE_AT, s, 2);
            }
            const char* s = cursor_;
            ++cursor_;
            return make_token(TokenType::TK_AT, s, 1);
        }

        // $ — PostgreSQL: $N placeholder or $$string$$
        if constexpr (D == Dialect::PostgreSQL) {
            if (c == '$') {
                if (peek_char(1) == '$') {
                    return scan_dollar_string();
                }
                if (peek_char(1) >= '0' && peek_char(1) <= '9') {
                    const char* start = cursor_;
                    ++cursor_;  // skip $
                    while (cursor_ < end_ && *cursor_ >= '0' && *cursor_ <= '9')
                        ++cursor_;
                    uint32_t len = static_cast<uint32_t>(cursor_ - start);
                    return make_token(TokenType::TK_DOLLAR_NUM, start, len);
                }
            }
        }

        // Two-character operators
        if (cursor_ + 1 < end_) {
            char c2 = peek_char(1);

            if (c == '<' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_LESS_EQUAL, s, 2); }
            if (c == '>' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_GREATER_EQUAL, s, 2); }
            if (c == '!' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_NOT_EQUAL, s, 2); }
            if (c == '<' && c2 == '>') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_NOT_EQUAL, s, 2); }
            if (c == '|' && c2 == '|') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_DOUBLE_PIPE, s, 2); }

            if constexpr (D == Dialect::MySQL) {
                if (c == ':' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_COLON_EQUAL, s, 2); }
            }

            if constexpr (D == Dialect::PostgreSQL) {
                if (c == ':' && c2 == ':') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_DOUBLE_COLON, s, 2); }
            }
        }

        // Single-character operators/punctuation
        const char* s = cursor_;
        ++cursor_;
        switch (c) {
            case '(': return make_token(TokenType::TK_LPAREN, s, 1);
            case ')': return make_token(TokenType::TK_RPAREN, s, 1);
            case ',': return make_token(TokenType::TK_COMMA, s, 1);
            case ';': return make_token(TokenType::TK_SEMICOLON, s, 1);
            case '.': return make_token(TokenType::TK_DOT, s, 1);
            case '*': return make_token(TokenType::TK_ASTERISK, s, 1);
            case '+': return make_token(TokenType::TK_PLUS, s, 1);
            case '-': return make_token(TokenType::TK_MINUS, s, 1);
            case '/': return make_token(TokenType::TK_SLASH, s, 1);
            case '%': return make_token(TokenType::TK_PERCENT, s, 1);
            case '=': return make_token(TokenType::TK_EQUAL, s, 1);
            case '<': return make_token(TokenType::TK_LESS, s, 1);
            case '>': return make_token(TokenType::TK_GREATER, s, 1);
            case '&': return make_token(TokenType::TK_AMPERSAND, s, 1);
            case '|': return make_token(TokenType::TK_PIPE, s, 1);
            case '^': return make_token(TokenType::TK_CARET, s, 1);
            case '~': return make_token(TokenType::TK_TILDE, s, 1);
            case '!': return make_token(TokenType::TK_EXCLAIM, s, 1);
            case ':': return make_token(TokenType::TK_COLON, s, 1);
            case '?': return make_token(TokenType::TK_QUESTION, s, 1);
            case '#': return make_token(TokenType::TK_HASH, s, 1);
            default:  return make_token(TokenType::TK_ERROR, s, 1);
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_TOKENIZER_H
```

- [ ] **Step 3: Build and run tests**

Run:
```bash
make -f Makefile.new test
```
Expected: All MySQLTokenizerTest and PgSQLTokenizerTest tests PASS.

- [ ] **Step 4: Commit**

```bash
git add include/sql_parser/tokenizer.h tests/test_tokenizer.cpp
git commit -m "feat: add dialect-templated tokenizer with MySQL and PostgreSQL support"
```

---

### Task 7: Parser — Classifier and Tier 2 Extractors

**Files:**
- Create: `include/sql_parser/parser.h`
- Create: `src/sql_parser/parser.cpp`
- Create: `tests/test_classifier.cpp`

- [ ] **Step 1: Write the failing test**

Create `tests/test_classifier.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

// ========== MySQL Classifier Tests ==========

class MySQLClassifierTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;
};

TEST_F(MySQLClassifierTest, ClassifySelect) {
    auto r = parser.parse("SELECT * FROM users", 19);
    // SELECT is Tier 1 — for now returns PARTIAL until deep parser is implemented
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
}

TEST_F(MySQLClassifierTest, ClassifyInsert) {
    auto r = parser.parse("INSERT INTO users VALUES (1, 'a')", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyInsertQualified) {
    auto r = parser.parse("INSERT INTO mydb.users VALUES (1)", 33);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.schema_name.ptr, r.schema_name.len), "mydb");
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyUpdate) {
    auto r = parser.parse("UPDATE users SET name='x'", 25);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::UPDATE);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyDelete) {
    auto r = parser.parse("DELETE FROM users WHERE id=1", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifySet) {
    auto r = parser.parse("SET autocommit=0", 16);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
}

TEST_F(MySQLClassifierTest, ClassifyUse) {
    auto r = parser.parse("USE mydb", 8);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::USE);
    EXPECT_EQ(std::string(r.database_name.ptr, r.database_name.len), "mydb");
}

TEST_F(MySQLClassifierTest, ClassifyBegin) {
    auto r = parser.parse("BEGIN", 5);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::BEGIN);
}

TEST_F(MySQLClassifierTest, ClassifyStartTransaction) {
    auto r = parser.parse("START TRANSACTION", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::START_TRANSACTION);
}

TEST_F(MySQLClassifierTest, ClassifyCommit) {
    auto r = parser.parse("COMMIT", 6);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::COMMIT);
}

TEST_F(MySQLClassifierTest, ClassifyRollback) {
    auto r = parser.parse("ROLLBACK", 8);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::ROLLBACK);
}

TEST_F(MySQLClassifierTest, ClassifyCreateTable) {
    auto r = parser.parse("CREATE TABLE users (id INT)", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::CREATE);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyDropTable) {
    auto r = parser.parse("DROP TABLE users", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DROP);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyShow) {
    auto r = parser.parse("SHOW TABLES", 11);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SHOW);
}

TEST_F(MySQLClassifierTest, ClassifyReplace) {
    auto r = parser.parse("REPLACE INTO users VALUES (1)", 29);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::REPLACE);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyGrant) {
    auto r = parser.parse("GRANT SELECT ON users TO 'app'", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::GRANT);
}

TEST_F(MySQLClassifierTest, ClassifyRevoke) {
    auto r = parser.parse("REVOKE ALL ON users FROM 'app'", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::REVOKE);
}

TEST_F(MySQLClassifierTest, ClassifyLock) {
    auto r = parser.parse("LOCK TABLES users WRITE", 23);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::LOCK);
}

TEST_F(MySQLClassifierTest, ClassifyDeallocate) {
    auto r = parser.parse("DEALLOCATE PREPARE stmt1", 24);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DEALLOCATE);
}

TEST_F(MySQLClassifierTest, ClassifyUnknown) {
    auto r = parser.parse("EXPLAIN SELECT 1", 16);
    EXPECT_EQ(r.stmt_type, StmtType::UNKNOWN);
}

TEST_F(MySQLClassifierTest, EmptyInput) {
    auto r = parser.parse("", 0);
    EXPECT_EQ(r.status, ParseResult::ERROR);
    EXPECT_EQ(r.stmt_type, StmtType::UNKNOWN);
}

TEST_F(MySQLClassifierTest, MultiStatement) {
    const char* sql = "BEGIN; SELECT 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.stmt_type, StmtType::BEGIN);
    EXPECT_TRUE(r.has_remaining());
    // remaining should point to " SELECT 1"
    EXPECT_GT(r.remaining.len, 0u);
}

TEST_F(MySQLClassifierTest, CaseInsensitive) {
    auto r = parser.parse("insert into USERS values (1)", 28);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "USERS");
}

// ========== PostgreSQL Classifier Tests ==========

class PgSQLClassifierTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;
};

TEST_F(PgSQLClassifierTest, ClassifySelect) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
}

TEST_F(PgSQLClassifierTest, ClassifyInsert) {
    auto r = parser.parse("INSERT INTO users VALUES (1)", 28);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(PgSQLClassifierTest, ClassifyReset) {
    auto r = parser.parse("RESET ALL", 9);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::RESET);
}
```

- [ ] **Step 2: Write parser.h**

Create `include/sql_parser/parser.h`:
```cpp
#ifndef SQL_PARSER_PARSER_H
#define SQL_PARSER_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/parse_result.h"

namespace sql_parser {

struct ParserConfig {
    size_t arena_block_size = 65536;    // 64KB
    size_t arena_max_size = 1048576;    // 1MB
};

template <Dialect D>
class Parser {
public:
    explicit Parser(const ParserConfig& config = {});
    ~Parser() = default;

    // Non-copyable, non-movable
    Parser(const Parser&) = delete;
    Parser& operator=(const Parser&) = delete;

    // Parse a SQL string. Returns ParseResult with classification + metadata.
    // For Tier 1 statements (SELECT, SET), returns PARTIAL until deep parsers
    // are implemented (future plan).
    ParseResult parse(const char* sql, size_t len);

    // Reset the arena. Call after each query is fully processed.
    void reset();

private:
    Arena arena_;
    Tokenizer<D> tokenizer_;

    // Classifier: dispatches to the right extractor/parser
    ParseResult classify_and_dispatch();

    // Tier 1 stubs (return PARTIAL with stmt_type set)
    ParseResult parse_select();
    ParseResult parse_set();

    // Tier 2 extractors
    ParseResult extract_insert(const Token& first);
    ParseResult extract_update(const Token& first);
    ParseResult extract_delete(const Token& first);
    ParseResult extract_replace(const Token& first);
    ParseResult extract_transaction(const Token& first);
    ParseResult extract_use(const Token& first);
    ParseResult extract_show(const Token& first);
    ParseResult extract_prepare(const Token& first);
    ParseResult extract_execute(const Token& first);
    ParseResult extract_deallocate(const Token& first);
    ParseResult extract_ddl(const Token& first);
    ParseResult extract_acl(const Token& first);
    ParseResult extract_lock(const Token& first);
    ParseResult extract_load(const Token& first);
    ParseResult extract_reset(const Token& first);
    ParseResult extract_unknown(const Token& first);

    // Helpers
    // Read optional schema.table or just table. Returns table token.
    // If qualified (schema.table), sets schema_out.
    Token read_table_name(StringRef& schema_out);

    // Scan forward to semicolon or EOF, set result.remaining
    void scan_to_end(ParseResult& result);
};

} // namespace sql_parser

#endif // SQL_PARSER_PARSER_H
```

- [ ] **Step 3: Write parser.cpp**

Create `src/sql_parser/parser.cpp`:
```cpp
#include "sql_parser/parser.h"

namespace sql_parser {

template <Dialect D>
Parser<D>::Parser(const ParserConfig& config)
    : arena_(config.arena_block_size, config.arena_max_size) {}

template <Dialect D>
void Parser<D>::reset() {
    arena_.reset();
}

template <Dialect D>
ParseResult Parser<D>::parse(const char* sql, size_t len) {
    arena_.reset();
    tokenizer_.reset(sql, len);
    return classify_and_dispatch();
}

template <Dialect D>
ParseResult Parser<D>::classify_and_dispatch() {
    Token first = tokenizer_.next_token();

    if (first.type == TokenType::TK_EOF) {
        ParseResult r;
        r.status = ParseResult::ERROR;
        r.stmt_type = StmtType::UNKNOWN;
        return r;
    }

    switch (first.type) {
        case TokenType::TK_SELECT:   return parse_select();
        case TokenType::TK_SET:      return parse_set();
        case TokenType::TK_INSERT:   return extract_insert(first);
        case TokenType::TK_UPDATE:   return extract_update(first);
        case TokenType::TK_DELETE:   return extract_delete(first);
        case TokenType::TK_REPLACE:  return extract_replace(first);
        case TokenType::TK_BEGIN:
        case TokenType::TK_START:
        case TokenType::TK_COMMIT:
        case TokenType::TK_ROLLBACK:
        case TokenType::TK_SAVEPOINT:return extract_transaction(first);
        case TokenType::TK_USE:      return extract_use(first);
        case TokenType::TK_SHOW:     return extract_show(first);
        case TokenType::TK_PREPARE:  return extract_prepare(first);
        case TokenType::TK_EXECUTE:  return extract_execute(first);
        case TokenType::TK_DEALLOCATE: return extract_deallocate(first);
        case TokenType::TK_CREATE:
        case TokenType::TK_ALTER:
        case TokenType::TK_DROP:
        case TokenType::TK_TRUNCATE: return extract_ddl(first);
        case TokenType::TK_GRANT:
        case TokenType::TK_REVOKE:   return extract_acl(first);
        case TokenType::TK_LOCK:
        case TokenType::TK_UNLOCK:   return extract_lock(first);
        case TokenType::TK_LOAD:     return extract_load(first);
        case TokenType::TK_RESET:    return extract_reset(first);
        default:                     return extract_unknown(first);
    }
}

// ---- Tier 1 stubs ----

template <Dialect D>
ParseResult Parser<D>::parse_select() {
    ParseResult r;
    r.status = ParseResult::PARTIAL;
    r.stmt_type = StmtType::SELECT;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_set() {
    ParseResult r;
    r.status = ParseResult::PARTIAL;
    r.stmt_type = StmtType::SET;
    scan_to_end(r);
    return r;
}

// ---- Helpers ----

template <Dialect D>
Token Parser<D>::read_table_name(StringRef& schema_out) {
    Token name = tokenizer_.next_token();
    if (name.type != TokenType::TK_IDENTIFIER &&
        name.type != TokenType::TK_EOF) {
        // Keywords used as table names (e.g., CREATE TABLE `user`)
        // The tokenizer returns keyword tokens for reserved words.
        // Accept any non-punctuation token as a potential name.
    }

    // Check for qualified name: schema.table
    if (tokenizer_.peek().type == TokenType::TK_DOT) {
        schema_out = name.text;
        tokenizer_.skip();  // consume dot
        Token table = tokenizer_.next_token();
        return table;
    }

    schema_out = StringRef{};
    return name;
}

template <Dialect D>
void Parser<D>::scan_to_end(ParseResult& result) {
    while (true) {
        Token t = tokenizer_.next_token();
        if (t.type == TokenType::TK_EOF) break;
        if (t.type == TokenType::TK_SEMICOLON) {
            Token next = tokenizer_.peek();
            if (next.type != TokenType::TK_EOF) {
                const char* remaining_start = next.text.ptr;
                const char* input_end = tokenizer_.input_end();
                result.remaining = StringRef{
                    remaining_start,
                    static_cast<uint32_t>(input_end - remaining_start)
                };
            }
            break;
        }
    }
}

// ---- Tier 2 Extractors ----

template <Dialect D>
ParseResult Parser<D>::extract_insert(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::INSERT;

    // Expect optional INTO
    Token t = tokenizer_.peek();
    if (t.type == TokenType::TK_INTO) {
        tokenizer_.skip();
    }

    // Read table name
    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_update(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::UPDATE;

    // Optional LOW_PRIORITY / IGNORE
    Token t = tokenizer_.peek();
    while (t.type == TokenType::TK_LOW_PRIORITY || t.type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        t = tokenizer_.peek();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_delete(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::DELETE_STMT;

    // Optional LOW_PRIORITY / QUICK / IGNORE
    Token t = tokenizer_.peek();
    while (t.type == TokenType::TK_LOW_PRIORITY ||
           t.type == TokenType::TK_QUICK ||
           t.type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        t = tokenizer_.peek();
    }

    // Expect FROM
    if (tokenizer_.peek().type == TokenType::TK_FROM) {
        tokenizer_.skip();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_replace(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::REPLACE;

    if (tokenizer_.peek().type == TokenType::TK_INTO) {
        tokenizer_.skip();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_transaction(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;

    switch (first.type) {
        case TokenType::TK_BEGIN:
            r.stmt_type = StmtType::BEGIN;
            break;
        case TokenType::TK_START:
            r.stmt_type = StmtType::START_TRANSACTION;
            // consume TRANSACTION if present
            if (tokenizer_.peek().type == TokenType::TK_TRANSACTION)
                tokenizer_.skip();
            break;
        case TokenType::TK_COMMIT:
            r.stmt_type = StmtType::COMMIT;
            break;
        case TokenType::TK_ROLLBACK:
            r.stmt_type = StmtType::ROLLBACK;
            break;
        case TokenType::TK_SAVEPOINT:
            r.stmt_type = StmtType::SAVEPOINT;
            break;
        default:
            r.stmt_type = StmtType::UNKNOWN;
            break;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_use(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::USE;

    Token db = tokenizer_.next_token();
    r.database_name = db.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_show(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::SHOW;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_prepare(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::PREPARE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_execute(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::EXECUTE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_deallocate(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::DEALLOCATE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_ddl(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;

    switch (first.type) {
        case TokenType::TK_CREATE:   r.stmt_type = StmtType::CREATE; break;
        case TokenType::TK_ALTER:    r.stmt_type = StmtType::ALTER; break;
        case TokenType::TK_DROP:     r.stmt_type = StmtType::DROP; break;
        case TokenType::TK_TRUNCATE: r.stmt_type = StmtType::TRUNCATE; break;
        default:                     r.stmt_type = StmtType::UNKNOWN; break;
    }

    // Try to extract object name: CREATE/ALTER/DROP [IF EXISTS/NOT EXISTS] TABLE/INDEX/VIEW name
    Token t = tokenizer_.next_token();

    // Skip optional IF [NOT] EXISTS
    if (t.type == TokenType::TK_IF) {
        t = tokenizer_.next_token(); // NOT or EXISTS
        if (t.type == TokenType::TK_NOT) {
            t = tokenizer_.next_token(); // EXISTS
        }
        // Skip EXISTS
        t = tokenizer_.next_token(); // should be TABLE/INDEX/etc.
    }

    // Now t should be TABLE, INDEX, VIEW, DATABASE, SCHEMA, or a name
    if (t.type == TokenType::TK_TABLE || t.type == TokenType::TK_INDEX ||
        t.type == TokenType::TK_VIEW || t.type == TokenType::TK_DATABASE ||
        t.type == TokenType::TK_SCHEMA) {
        // Optional IF [NOT] EXISTS after object type for CREATE/DROP
        Token maybe_if = tokenizer_.peek();
        if (maybe_if.type == TokenType::TK_IF) {
            tokenizer_.skip(); // IF
            Token next = tokenizer_.next_token();
            if (next.type == TokenType::TK_NOT) {
                tokenizer_.skip(); // EXISTS
            }
        }
        Token name = read_table_name(r.schema_name);
        r.table_name = name.text;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_acl(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = (first.type == TokenType::TK_GRANT) ? StmtType::GRANT : StmtType::REVOKE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_lock(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = (first.type == TokenType::TK_LOCK) ? StmtType::LOCK : StmtType::UNLOCK;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_load(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::LOAD_DATA;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_reset(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::RESET;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_unknown(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::UNKNOWN;
    scan_to_end(r);
    return r;
}

// ---- Explicit template instantiations ----

template class Parser<Dialect::MySQL>;
template class Parser<Dialect::PostgreSQL>;

} // namespace sql_parser
```

- [ ] **Step 4: Build and run tests**

Run:
```bash
make -f Makefile.new test
```
Expected: All classifier and extractor tests PASS.

- [ ] **Step 5: Commit**

```bash
git add include/sql_parser/parser.h src/sql_parser/parser.cpp tests/test_classifier.cpp
git commit -m "feat: add classifier and Tier 2 extractors for all statement types"
```

---

### Task 8: Integration Smoke Test and .gitignore Update

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Update .gitignore**

Append to `.gitignore`:
```
# New parser build artifacts
libsqlparser.a
run_tests
third_party/
```

- [ ] **Step 2: Run full build from clean**

Run:
```bash
make -f Makefile.new clean && make -f Makefile.new all
```
Expected: Builds `libsqlparser.a`, runs all tests, all PASS.

- [ ] **Step 3: Commit**

```bash
git add .gitignore
git commit -m "chore: update .gitignore for new parser build artifacts"
```

---

## What's Next

After this plan is complete, the following plans should be created (in order):

1. **Plan 2: Expression Parser + SET Deep Parser** — Pratt expression parser, full SET statement parser with AST construction, round-trip tests.
2. **Plan 3: SELECT Deep Parser** — Full SELECT statement parsing with all clauses, using the expression parser from Plan 2.
3. **Plan 4: Query Emitter** — AST → SQL reconstruction, round-trip testing.
4. **Plan 5: Prepared Statement Cache** — Statement cache, binary protocol support, `parse_and_cache` / `execute` API.
5. **Plan 6: Performance Benchmarks + Optimization** — Google Benchmark integration, performance validation against targets, optimization passes (perfect hash for keywords, etc.).
