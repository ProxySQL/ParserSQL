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
    DELETE_STMT,
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
    RESET,
};

// -- AST node types --

enum class NodeType : uint16_t {
    NODE_UNKNOWN = 0,

    // Tier 2 lightweight nodes
    NODE_STATEMENT,
    NODE_TABLE_REF,
    NODE_SCHEMA_REF,
    NODE_IDENTIFIER,
    NODE_QUALIFIED_NAME,

    // Tier 1 nodes (SELECT)
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

    // Tier 1 nodes (SET)
    NODE_SET_STMT,
    NODE_SET_NAMES,
    NODE_SET_CHARSET,
    NODE_SET_TRANSACTION,
    NODE_VAR_ASSIGNMENT,
    NODE_VAR_TARGET,

    // Expression nodes
    NODE_EXPRESSION,
    NODE_BINARY_OP,
    NODE_UNARY_OP,
    NODE_FUNCTION_CALL,
    NODE_LITERAL_INT,
    NODE_LITERAL_FLOAT,
    NODE_LITERAL_STRING,
    NODE_LITERAL_NULL,
    NODE_PLACEHOLDER,
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
