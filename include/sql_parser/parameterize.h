#ifndef SQL_PARSER_PARAMETERIZE_H
#define SQL_PARSER_PARAMETERIZE_H

#include "sql_parser/ast_transform.h"
#include "sql_parser/parse_result.h"
#include <limits>
#include <string>
#include <vector>

namespace sql_parser {

struct ExtractedParameter {
    uint32_t index;       // One-based bind index in the emitted SQL.
    NodeType literal_type;
    StringRef value;      // Original lexical value (not decoded or type-coerced).
    StringRef source;     // Original source spelling, including quotes if present.
};

struct ParameterizeResult {
    AstNode* ast = nullptr;
    std::vector<ExtractedParameter> parameters; // In AST/emission order; no deduplication.
    std::vector<uint32_t> existing_parameters;  // Bind indices, in occurrence order.
    AstError error = AstError::None;
    bool ok() const { return error == AstError::None; }
};

namespace parameterize_detail {

inline bool supported_root(NodeType type) {
    switch (type) {
        case NodeType::NODE_SELECT_STMT:
        case NodeType::NODE_INSERT_STMT:
        case NodeType::NODE_UPDATE_STMT:
        case NodeType::NODE_DELETE_STMT:
        case NodeType::NODE_COMPOUND_QUERY:
        case NodeType::NODE_TABLE_QUERY:
            return true;
        default: return false;
    }
}

inline bool supported_node(const AstNode& node) {
    switch (node.type) {
        case NodeType::NODE_CTE:
        case NodeType::NODE_CTE_DEFINITION:
        case NodeType::NODE_UNKNOWN:
        case NodeType::NODE_STATEMENT:
        case NodeType::NODE_SCHEMA_REF:
        case NodeType::NODE_SET_STMT:
        case NodeType::NODE_SET_NAMES:
        case NodeType::NODE_SET_CHARSET:
        case NodeType::NODE_SET_TRANSACTION:
        case NodeType::NODE_VAR_ASSIGNMENT:
        case NodeType::NODE_VAR_TARGET:
        case NodeType::NODE_SET_ROLE:
        case NodeType::NODE_SET_SESSION_AUTHORIZATION:
        case NodeType::NODE_SET_CONSTRAINTS:
        case NodeType::NODE_INTO_CLAUSE:
        case NodeType::NODE_EXPLAIN_STMT:
        case NodeType::NODE_EXPLAIN_OPTIONS:
        case NodeType::NODE_EXPLAIN_FORMAT:
        case NodeType::NODE_CALL_STMT:
        case NodeType::NODE_DO_STMT:
        case NodeType::NODE_LOAD_DATA_STMT:
        case NodeType::NODE_LOAD_DATA_OPTIONS:
            return false;
        case NodeType::NODE_FUNCTION_CALL:
            // The current generic emitter cannot render CAST(expr AS type).
            return !node.value().equals_ci("CAST", 4);
        case NodeType::NODE_SUBQUERY:
            return node.first_child != nullptr; // Reject opaque SQL fragments.
        case NodeType::NODE_IDENTIFIER: {
            if (node.flags & FLAG_IDENT_DELIMITED) return true;
            StringRef value = node.value();
            // INTERVAL and quantified subqueries are currently opaque identifiers.
            if (value.len > 8 && StringRef{value.ptr, 8}.equals_ci("INTERVAL", 8) &&
                (value.ptr[8] == ' ' || value.ptr[8] == '\t')) return false;
            for (uint32_t i = 0; i < value.len; ++i) {
                if (value.ptr[i] == '(') return false;
            }
            return true;
        }
        case NodeType::NODE_TABLE_REF:
        case NodeType::NODE_QUALIFIED_NAME:
        case NodeType::NODE_SELECT_STMT:
        case NodeType::NODE_SELECT_OPTIONS:
        case NodeType::NODE_SELECT_ITEM_LIST:
        case NodeType::NODE_SELECT_ITEM:
        case NodeType::NODE_FROM_CLAUSE:
        case NodeType::NODE_JOIN_CLAUSE:
        case NodeType::NODE_WHERE_CLAUSE:
        case NodeType::NODE_GROUP_BY_CLAUSE:
        case NodeType::NODE_HAVING_CLAUSE:
        case NodeType::NODE_AGGREGATE_ORDER_BY:
        case NodeType::NODE_ORDER_BY_CLAUSE:
        case NodeType::NODE_ORDER_BY_ITEM:
        case NodeType::NODE_LIMIT_CLAUSE:
        case NodeType::NODE_LOCKING_CLAUSE:
        case NodeType::NODE_ALIAS:
        case NodeType::NODE_EXPRESSION:
        case NodeType::NODE_NAMED_ARGUMENT:
        case NodeType::NODE_TYPE_CAST:
        case NodeType::NODE_TYPE_NAME:
        case NodeType::NODE_BINARY_OP:
        case NodeType::NODE_UNARY_OP:
        case NodeType::NODE_LITERAL_INT:
        case NodeType::NODE_LITERAL_FLOAT:
        case NodeType::NODE_LITERAL_STRING:
        case NodeType::NODE_LITERAL_NULL:
        case NodeType::NODE_PLACEHOLDER:
        case NodeType::NODE_COLUMN_REF:
        case NodeType::NODE_ASTERISK:
        case NodeType::NODE_IS_NULL:
        case NodeType::NODE_IS_NOT_NULL:
        case NodeType::NODE_BETWEEN:
        case NodeType::NODE_IN_LIST:
        case NodeType::NODE_CASE_WHEN:
        case NodeType::NODE_TUPLE:
        case NodeType::NODE_ARRAY_CONSTRUCTOR:
        case NodeType::NODE_ARRAY_SUBSCRIPT:
        case NodeType::NODE_FIELD_ACCESS:
        case NodeType::NODE_INSERT_STMT:
        case NodeType::NODE_INSERT_COLUMNS:
        case NodeType::NODE_VALUES_CLAUSE:
        case NodeType::NODE_VALUES_ROW:
        case NodeType::NODE_INSERT_SET_CLAUSE:
        case NodeType::NODE_ON_DUPLICATE_KEY:
        case NodeType::NODE_ON_CONFLICT:
        case NodeType::NODE_CONFLICT_TARGET:
        case NodeType::NODE_CONFLICT_ACTION:
        case NodeType::NODE_RETURNING_CLAUSE:
        case NodeType::NODE_UPDATE_STMT:
        case NodeType::NODE_UPDATE_SET_CLAUSE:
        case NodeType::NODE_DELETE_STMT:
        case NodeType::NODE_DELETE_USING_CLAUSE:
        case NodeType::NODE_COMPOUND_QUERY:
        case NodeType::NODE_SET_OPERATION:
        case NodeType::NODE_WINDOW_FUNCTION:
        case NodeType::NODE_WINDOW_SPEC:
        case NodeType::NODE_WINDOW_PARTITION:
        case NodeType::NODE_WINDOW_ORDER:
        case NodeType::NODE_STAR_EXCEPT:
        case NodeType::NODE_STAR_REPLACE:
        case NodeType::NODE_REPLACE_ITEM:
        case NodeType::NODE_STMT_OPTIONS:
        case NodeType::NODE_UPDATE_SET_ITEM:
        case NodeType::NODE_USER_VARIABLE:
        case NodeType::NODE_LITERAL_HEX:
        case NodeType::NODE_LITERAL_BIT:
        case NodeType::NODE_TABLE_QUERY:
        case NodeType::NODE_DISTINCT_ON:
        case NodeType::NODE_AGGREGATE_FILTER:
        case NodeType::NODE_LATERAL:
        case NodeType::NODE_WINDOW_CLAUSE:
        case NodeType::NODE_WINDOW_DEFINITION:
        case NodeType::NODE_WINDOW_REFERENCE:
        case NodeType::NODE_WINDOW_FRAME:
        case NodeType::NODE_WINDOW_BOUND:
        case NodeType::NODE_WINDOW_EXCLUSION:
            return true;
        default: return false; // Future syntax must be reviewed for bindability.
    }
}

template <Dialect D>
inline bool datetime_precision(const AstNode& node) {
    if (node.type != NodeType::NODE_FUNCTION_CALL) return false;
    const StringRef name = node.value();
    if (name.equals_ci("CURRENT_TIMESTAMP", 17) || name.equals_ci("CURRENT_TIME", 12) ||
        name.equals_ci("LOCALTIME", 9) || name.equals_ci("LOCALTIMESTAMP", 14)) return true;
    if constexpr (D == Dialect::MySQL) {
        return name.equals_ci("NOW", 3) || name.equals_ci("CURTIME", 7) ||
            name.equals_ci("SYSDATE", 7) || name.equals_ci("UTC_TIME", 8) ||
            name.equals_ci("UTC_TIMESTAMP", 13);
    }
    return false;
}

inline bool postgres_alias(StringRef value) {
    if (!value.len) return false;
    if (value.ptr[0] == '"') return value.len >= 2 && value.ptr[value.len - 1] == '"';
    for (uint32_t i = 0; i < value.len; ++i) {
        const unsigned char c = static_cast<unsigned char>(value.ptr[i]);
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c >= 128) continue;
        if (i && ((c >= '0' && c <= '9') || c == '$')) continue;
        return false;
    }
    return true;
}

inline bool literal(NodeType type) {
    return type == NodeType::NODE_LITERAL_INT || type == NodeType::NODE_LITERAL_FLOAT ||
        type == NodeType::NODE_LITERAL_STRING || type == NodeType::NODE_LITERAL_HEX ||
        type == NodeType::NODE_LITERAL_BIT;
}

inline bool ordinal(const AstNode* node) {
    while (node->type == NodeType::NODE_EXPRESSION && node->first_child &&
           !node->first_child->next_sibling) node = node->first_child;
    if (node->type != NodeType::NODE_LITERAL_INT || !node->value_len) return false;
    for (uint32_t i = 0; i < node->value_len; ++i)
        if (node->value_ptr[i] < '0' || node->value_ptr[i] > '9') return false;
    return true;
}

inline bool postgres_bind(StringRef value, uint32_t& index) {
    if (value.len < 2 || value.ptr[0] != '$') return false;
    index = 0;
    constexpr uint32_t maximum = static_cast<uint32_t>(std::numeric_limits<int32_t>::max());
    for (uint32_t i = 1; i < value.len; ++i) {
        if (value.ptr[i] < '0' || value.ptr[i] > '9') return false;
        const uint32_t digit = static_cast<uint32_t>(value.ptr[i] - '0');
        if (index > (maximum - digit) / 10) return false;
        index = index * 10 + digit;
    }
    return index != 0;
}

} // namespace parameterize_detail

// Input must be a complete, successfully parsed AST in dialect D. This is a
// syntactic transformation, not type inference or validation against a catalog.
// Result nodes and mapping text belong to arena; vectors belong to the result.
// NULL, query ordinals, IS [NOT] TRUE/FALSE and cast type modifiers stay literal.
// Utility roots and unsupported/opaque contexts fail with no result AST/mapping.
template <Dialect D>
ParameterizeResult parameterize_ast(const AstNode* root, Arena& arena,
                                    AstWalkLimits limits = {}) {
    using namespace parameterize_detail;
    ParameterizeResult result;
    if (!root || !supported_root(root->type)) {
        result.error = AstError::UnsupportedRoot;
        return result;
    }
    auto cloned = clone_ast(root, arena, limits);
    if (!cloned.ok()) { result.error = cloned.error; return result; }
    uint32_t next_index = 0;
    std::vector<bool> comma_limit;
    walk_ast(cloned.ast, [&](const AstNode& node, const AstVisitContext& ctx) {
        comma_limit.resize(ctx.depth + 1);
        comma_limit[ctx.depth] = (ctx.depth && comma_limit[ctx.depth - 1]) ||
            (node.type == NodeType::NODE_LIMIT_CLAUSE && (node.flags & FLAG_LIMIT_COMMA));
        if (!supported_node(node) ||
            (D == Dialect::PostgreSQL && node.type == NodeType::NODE_ALIAS && !postgres_alias(node.value()))) {
            result.error = AstError::UnsupportedContext;
            return AstVisitAction::Stop;
        }
        if (node.type == NodeType::NODE_PLACEHOLDER) {
            if (D == Dialect::MySQL && comma_limit[ctx.depth]) {
                // Comma LIMIT is normalized to count/OFFSET order. Original
                // anonymous binds lack stable indices after this reordering.
                result.error = AstError::UnsupportedContext;
                return AstVisitAction::Stop;
            }
            if constexpr (D == Dialect::PostgreSQL) {
                uint32_t index;
                if (!postgres_bind(node.value(), index)) {
                    result.error = AstError::InvalidPlaceholder;
                    return AstVisitAction::Stop;
                }
                if (index > next_index) next_index = index;
            } else if (!(node.value() == StringRef{"?", 1})) {
                result.error = AstError::InvalidPlaceholder;
                return AstVisitAction::Stop;
            }
        }
        return AstVisitAction::Continue;
    }, limits);
    if (!result.ok()) return result;
    std::vector<const AstNode*> ancestors;
    walk_ast(cloned.ast, [&](const AstNode& node, const AstVisitContext& ctx) {
        ancestors.resize(ctx.depth + 1);
        ancestors[ctx.depth] = &node;
        if (datetime_precision<D>(node)) return AstVisitAction::SkipChildren;
        if (ctx.parent) {
            const AstNode* parent = ctx.parent;
            // PostgreSQL :: stores the type (and its modifiers) in the RHS.
            if (parent->type == NodeType::NODE_BINARY_OP && ctx.child_index == 1 &&
                (parent->value().equals_ci("::", 2) || parent->value().equals_ci("IS", 2) ||
                 parent->value().equals_ci("IS NOT", 6))) return AstVisitAction::SkipChildren;
            const bool group_item = parent->type == NodeType::NODE_GROUP_BY_CLAUSE ||
                parent->type == NodeType::NODE_DISTINCT_ON;
            const bool order_item = parent->type == NodeType::NODE_ORDER_BY_ITEM &&
                ctx.child_index == 0 && ctx.depth >= 2 &&
                ancestors[ctx.depth - 2]->type == NodeType::NODE_ORDER_BY_CLAUSE;
            if ((group_item || order_item) && ordinal(&node)) return AstVisitAction::SkipChildren;
        }
        if (node.type == NodeType::NODE_PLACEHOLDER) {
            uint32_t index;
            if constexpr (D == Dialect::PostgreSQL) postgres_bind(node.value(), index);
            else index = ++next_index;
            result.existing_parameters.push_back(index);
        } else if (literal(node.type)) {
            constexpr uint32_t maximum = static_cast<uint32_t>(std::numeric_limits<int32_t>::max());
            if (next_index == maximum) {
                result.error = AstError::ParameterOverflow;
                return AstVisitAction::Stop;
            }
            uint32_t index = ++next_index;
            const std::string placeholder = D == Dialect::PostgreSQL ? "$" + std::to_string(index) : "?";
            StringRef value = arena.allocate_string(placeholder.data(), static_cast<uint32_t>(placeholder.size()));
            if (!value.ptr) {
                result.error = AstError::AllocationFailure;
                return AstVisitAction::Stop;
            }
            result.parameters.push_back({index, node.type, node.value(), node.source()});
            AstNode& mutable_node = const_cast<AstNode&>(node);
            mutable_node.type = NodeType::NODE_PLACEHOLDER;
            mutable_node.flags = 0;
            mutable_node.set_value(value);
            mutable_node.set_source({});
        }
        return AstVisitAction::Continue;
    }, limits);
    if (result.ok()) result.ast = cloned.ast;
    else { result.parameters.clear(); result.existing_parameters.clear(); }
    return result;
}

template <Dialect D>
ParameterizeResult parameterize_ast(const ParseResult& parsed, Arena& arena,
                                    AstWalkLimits limits = {}) {
    if (!parsed.ok() || !parsed.full_input || parsed.has_remaining()) {
        ParameterizeResult result;
        result.error = AstError::UnsupportedContext;
        return result;
    }
    return parameterize_ast<D>(parsed.ast, arena, limits);
}

} // namespace sql_parser

#endif // SQL_PARSER_PARAMETERIZE_H
