#ifndef SQL_ENGINE_PLAN_NODE_H
#define SQL_ENGINE_PLAN_NODE_H

#include "sql_engine/catalog.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include <cstdint>

namespace sql_engine {

enum class PlanNodeType : uint8_t {
    SCAN,       // read from data source
    FILTER,     // WHERE / HAVING condition
    PROJECT,    // SELECT expression list
    JOIN,       // JOIN two sources
    AGGREGATE,  // GROUP BY + aggregate functions
    SORT,       // ORDER BY
    LIMIT,      // LIMIT + OFFSET
    DISTINCT,   // remove duplicates
    SET_OP,     // UNION / INTERSECT / EXCEPT
};

// Join type constants
static constexpr uint8_t JOIN_INNER = 0;
static constexpr uint8_t JOIN_LEFT  = 1;
static constexpr uint8_t JOIN_RIGHT = 2;
static constexpr uint8_t JOIN_FULL  = 3;
static constexpr uint8_t JOIN_CROSS = 4;

// Set operation type constants
static constexpr uint8_t SET_OP_UNION     = 0;
static constexpr uint8_t SET_OP_INTERSECT = 1;
static constexpr uint8_t SET_OP_EXCEPT    = 2;

struct PlanNode {
    PlanNodeType type;
    PlanNode* left = nullptr;    // primary child (or left of join/union)
    PlanNode* right = nullptr;   // right of join/union (null for unary ops)

    union {
        struct {
            const TableInfo* table;
        } scan;

        struct {
            const sql_parser::AstNode* expr;        // WHERE/HAVING expression AST
        } filter;

        struct {
            const sql_parser::AstNode** exprs;      // SELECT expression list (AST nodes)
            const sql_parser::AstNode** aliases;     // alias AST nodes (parallel array, nullable entries)
            uint16_t count;
        } project;

        struct {
            uint8_t join_type;          // INNER=0, LEFT=1, RIGHT=2, FULL=3, CROSS=4
            const sql_parser::AstNode* condition;   // ON expression AST (null for CROSS/NATURAL)
        } join;

        struct {
            const sql_parser::AstNode** group_by;   // GROUP BY expression list
            uint16_t group_count;
            const sql_parser::AstNode** agg_exprs;  // aggregate expressions (COUNT, SUM, etc.)
            uint16_t agg_count;
        } aggregate;

        struct {
            const sql_parser::AstNode** keys;       // ORDER BY key expressions
            uint8_t* directions;        // 0=ASC, 1=DESC (parallel array)
            uint16_t count;
        } sort;

        struct {
            int64_t count;
            int64_t offset;
        } limit;

        struct {
            uint8_t op;                 // 0=UNION, 1=INTERSECT, 2=EXCEPT
            bool all;                   // UNION ALL vs UNION
        } set_op;
    };
};

inline PlanNode* make_plan_node(sql_parser::Arena& arena, PlanNodeType type) {
    PlanNode* node = static_cast<PlanNode*>(arena.allocate(sizeof(PlanNode)));
    if (!node) return nullptr;
    // Zero-initialize then set type
    std::memset(node, 0, sizeof(PlanNode));
    node->type = type;
    return node;
}

} // namespace sql_engine

#endif // SQL_ENGINE_PLAN_NODE_H
