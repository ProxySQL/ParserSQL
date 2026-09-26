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

// -- Flags for NODE_SET_OPERATION --
static constexpr uint16_t FLAG_SET_OP_ALL = 0x01;
// Parenthesized query expressions retain grouping during SQL emission.
static constexpr uint16_t FLAG_QUERY_PARENTHESIZED = 0x01;
static constexpr uint16_t FLAG_TABLE_ONLY = 0x01;
static constexpr uint16_t FLAG_TABLE_INHERIT = 0x02;
static constexpr uint16_t FLAG_FUNCTION_TABLE = 0x01;
static constexpr uint16_t FLAG_FUNCTION_DISTINCT = 0x02;
static constexpr uint16_t FLAG_FUNCTION_ALL = 0x04;
static constexpr uint16_t FLAG_FUNCTION_QUALIFIED = 0x08;
static constexpr uint16_t FLAG_FUNCTION_WITHIN_GROUP = 0x10;
// On binary/unary expressions: PostgreSQL operation without local engine support.
static constexpr uint16_t FLAG_PG_OPERATOR = 0x01;
static constexpr uint16_t FLAG_WINDOW_BETWEEN = 0x01;
static constexpr uint16_t FLAG_ORDER_NULLS = 0x01;
static constexpr uint16_t FLAG_LIMIT_COMMA = 0x01;
static constexpr uint16_t FLAG_CTE_RECURSIVE = 0x01;
static constexpr uint16_t FLAG_CTE_MATERIALIZED = 0x02;
static constexpr uint16_t FLAG_CTE_NOT_MATERIALIZED = 0x04;

// -- Flags for NODE_IDENTIFIER / NODE_COLUMN_REF --
// Set when the identifier was source-delimited (backtick `name` for MySQL,
// double-quoted "name" for PostgreSQL). Allows downstream consumers to
// distinguish PG's case-sensitive `"Name"` from case-insensitive `Name`,
// which matters for SHOW search_path / SHOW <var> canonical re-emission.
static constexpr uint16_t FLAG_IDENT_DELIMITED = 0x01;

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
    EXPLAIN,
    DESCRIBE,
    CALL,
    DO_STMT,
    COPY,
    RELEASE_SAVEPOINT,
    VACUUM,
    ANALYZE,
    MERGE,
    COMMENT,
    SECURITY_LABEL,
    DECLARE_CURSOR,
    FETCH,
    MOVE,
    CLOSE,
    LISTEN,
    NOTIFY,
    UNLISTEN,
    DISCARD,
    CHECKPOINT,
    IMPORT_FOREIGN_SCHEMA,
    REINDEX,
    CLUSTER,
    REFRESH_MATERIALIZED_VIEW,
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
    NODE_TUPLE,              // (expr, expr, ...) row constructor
    NODE_ARRAY_CONSTRUCTOR,  // ARRAY[val, val, ...]
    NODE_ARRAY_SUBSCRIPT,    // expr[index]
    NODE_FIELD_ACCESS,       // (expr).field postfix access

    // INSERT nodes
    NODE_INSERT_STMT,
    NODE_INSERT_COLUMNS,       // (col1, col2, ...)
    NODE_VALUES_CLAUSE,        // VALUES keyword wrapper
    NODE_VALUES_ROW,           // single (val1, val2, ...) row
    NODE_INSERT_SET_CLAUSE,    // MySQL INSERT ... SET col=val form
    NODE_ON_DUPLICATE_KEY,     // MySQL ON DUPLICATE KEY UPDATE
    NODE_ON_CONFLICT,          // PostgreSQL ON CONFLICT
    NODE_CONFLICT_TARGET,      // PostgreSQL conflict target (cols or ON CONSTRAINT)
    NODE_CONFLICT_ACTION,      // DO UPDATE SET ... or DO NOTHING
    NODE_RETURNING_CLAUSE,     // PostgreSQL RETURNING expr_list

    // UPDATE nodes
    NODE_UPDATE_STMT,
    NODE_UPDATE_SET_CLAUSE,    // SET col=expr, col=expr in UPDATE context

    // DELETE nodes
    NODE_DELETE_STMT,
    NODE_DELETE_USING_CLAUSE,  // PostgreSQL USING or MySQL USING form

    // Compound query nodes
    NODE_COMPOUND_QUERY,       // root for UNION/INTERSECT/EXCEPT
    NODE_SET_OPERATION,        // operator (UNION, INTERSECT, EXCEPT) with ALL flag

    // EXPLAIN/DESCRIBE
    NODE_EXPLAIN_STMT,
    NODE_EXPLAIN_OPTIONS,
    NODE_EXPLAIN_FORMAT,

    // CALL
    NODE_CALL_STMT,

    // DO
    NODE_DO_STMT,

    // LOAD DATA
    NODE_LOAD_DATA_STMT,
    NODE_LOAD_DATA_OPTIONS,

    // Window function nodes
    NODE_WINDOW_FUNCTION,      // expr OVER (...)
    NODE_WINDOW_SPEC,          // PARTITION BY ... ORDER BY ...
    NODE_WINDOW_PARTITION,     // PARTITION BY clause
    NODE_WINDOW_ORDER,         // ORDER BY clause within window

    // CTE nodes
    NODE_CTE,                  // WITH clause wrapper
    NODE_CTE_DEFINITION,       // name AS (SELECT ...)

    // Star modifiers (BigQuery-style)
    NODE_STAR_EXCEPT,          // SELECT * EXCEPT(col1, col2)
    NODE_STAR_REPLACE,         // SELECT * REPLACE(expr AS col)
    NODE_REPLACE_ITEM,         // single expr AS col inside REPLACE

    // Shared
    NODE_STMT_OPTIONS,         // LOW_PRIORITY, IGNORE, QUICK, DELAYED, etc.
    NODE_UPDATE_SET_ITEM,      // single col=expr pair (shared by INSERT SET and UPDATE SET)

    // PG-specific non-GUC SET forms. Appended at the end of the enum to
    // avoid renumbering existing values (some consumers use NodeType as an
    // array index). These look syntactically like SET variable assignments
    // but are semantically transaction/session control (not tracked GUC
    // parameters). Emitted with their value(s) as children so consumers
    // can either forward verbatim to the backend or apply form-specific
    // handling, without misclassifying them as unknown GUCs named ROLE /
    // SESSION / CONSTRAINTS.
    NODE_SET_ROLE,                  // SET [LOCAL] ROLE <name>|NONE|DEFAULT
    NODE_SET_SESSION_AUTHORIZATION, // SET SESSION AUTHORIZATION <name>|DEFAULT
    NODE_SET_CONSTRAINTS,           // SET CONSTRAINTS {ALL|<name>[,...]} {DEFERRED|IMMEDIATE}

    // MySQL lossless user-variable/literal nodes. Keep appended so existing
    // enum values remain stable for consumers that index by NodeType.
    NODE_USER_VARIABLE,
    NODE_LITERAL_HEX,
    NODE_LITERAL_BIT,
    NODE_TABLE_QUERY,           // PostgreSQL TABLE [ONLY] relation [*]
    NODE_TRANSACTION_STMT,
    NODE_TRANSACTION_OPTION,
    NODE_COPY_STMT,
    NODE_COPY_OPTION,
    NODE_COPY_ENDPOINT,
    NODE_DISTINCT_ON,           // expression children, inside SELECT_OPTIONS
    NODE_AGGREGATE_FILTER,      // function, predicate
    NODE_LATERAL,               // child TABLE_REF
    NODE_WINDOW_CLAUSE,        // WINDOW_DEFINITION children
    NODE_WINDOW_DEFINITION,    // value=name, child WINDOW_SPEC
    NODE_WINDOW_REFERENCE,     // value=name, standalone OVER or inside spec
    NODE_WINDOW_FRAME,         // value=ROWS/RANGE/GROUPS; bounds then exclusion
    NODE_WINDOW_BOUND,         // value=PRECEDING/FOLLOWING/CURRENT ROW/...; offset child
    NODE_WINDOW_EXCLUSION,     // value=CURRENT ROW/GROUP/TIES/NO OTHERS
    NODE_TYPE_CAST,            // expression, NODE_TYPE_NAME; canonical CAST emission
    NODE_TYPE_NAME,            // validated type syntax, including modifiers/bounds
    NODE_NAMED_ARGUMENT,       // value=argument name; one expression child
    NODE_AGGREGATE_ORDER_BY,   // ORDER_BY_ITEM children; constants are not ordinals
    NODE_CTE_COLUMNS,          // identifier children; follows body in CTE_DEFINITION
    // PG_GAPS_EXPRESSION_NODES
    NODE_PG_EXTRACT,
    NODE_PG_SUBSTRING,
    NODE_PG_TIME_ZONE,
    NODE_PG_INTERVAL,
    NODE_PG_TRIM,
    NODE_PG_ARRAY_QUERY,
    NODE_PG_QUANTIFIED_OPERAND,
    NODE_PG_NORMALIZE,
    // PG_GAPS_DML_NODES
    NODE_MERGE_STMT,            // target, USING source, ON expression, WHEN actions, RETURNING
    NODE_MERGE_WHEN,            // match spelling; optional AND expression, THEN action
    NODE_PG_DML_CLAUSE,         // validated fixed syntax; structured children separated by spaces
    NODE_CTE_SEARCH,            // value=DEPTH/BREADTH; columns then sequence column
    NODE_CTE_CYCLE,             // columns, mark column, optional values, path column
    NODE_PG_RETURNING_OPTIONS,  // OLD/NEW AS alias options
    NODE_PG_ASSIGNMENT_FIELD,   // target.field, without expression parentheses
    // PG_GAPS_DDL_NODES
    NODE_PG_DDL_STMT,           // command value; structured clause children
    NODE_PG_DDL_CLAUSE,         // grammar production, optional keyword prefix
    NODE_PG_DDL_LIST,           // comma-separated children; flag 1 = no parentheses
    NODE_PG_DDL_SYNTAX,         // validated syntax keyword or option, never an expression
    // PG_GAPS_QUERY_NODES
    NODE_GROUPING_SET,
    NODE_OFFSET_CLAUSE,
    NODE_FETCH_CLAUSE,
    NODE_ORDINALITY,
    NODE_FUNCTION_COLUMN,
    // PG_GAPS_JSON_XML_NODES
    NODE_PG_JSON_XML,
    NODE_PG_JSON_XML_SYNTAX,
    // PG_CONT_EXPRESSION_NODES
    NODE_PG_VARIADIC_ARGUMENT, // one value or named-argument child; final call argument
    NODE_PG_ARRAY_SLICE,       // base, optional lower/upper; flags 1/2 mark bounds
    NODE_PG_PATTERN_PREDICATE, // value=operator; subject, pattern, optional escape
    NODE_PG_POSITION,          // needle, haystack
    NODE_PG_OVERLAY,           // source, replacement, start, optional length; flag 1=plain call
    NODE_PG_JSON_PREDICATE,    // validated IS [NOT] JSON suffix; one expression child
    // PG_CONT_QUERY_NODES
    NODE_PG_EXPLAIN_OPTION,
    NODE_PG_JOIN_TREE,
    NODE_PG_TABLE_GROUP,
    NODE_PG_JOIN_USING,
    NODE_PG_TABLESAMPLE,
    NODE_PG_ROWS_FROM,
    NODE_PG_SORT_USING,
    NODE_PG_SELECT_INTO,
    NODE_PG_ROW_LOCK,
    NODE_PG_COMMAND_STMT, // validated command with structural operands/clauses

};

} // namespace sql_parser

#endif // SQL_PARSER_COMMON_H
