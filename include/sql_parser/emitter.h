#ifndef SQL_PARSER_EMITTER_H
#define SQL_PARSER_EMITTER_H

#include "sql_parser/common.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/string_builder.h"

namespace sql_parser {

template <Dialect D>
class Emitter {
public:
    explicit Emitter(Arena& arena) : sb_(arena) {}

    void emit(const AstNode* node) {
        if (!node) return;
        emit_node(node);
    }

    StringRef result() { return sb_.finish(); }

private:
    StringBuilder sb_;

    void emit_node(const AstNode* node) {
        switch (node->type) {
            // ---- SET statement ----
            case NodeType::NODE_SET_STMT:     emit_set_stmt(node); break;
            case NodeType::NODE_SET_NAMES:    emit_set_names(node); break;
            case NodeType::NODE_SET_CHARSET:  emit_set_charset(node); break;
            case NodeType::NODE_SET_TRANSACTION: emit_set_transaction(node); break;
            case NodeType::NODE_VAR_ASSIGNMENT: emit_var_assignment(node); break;
            case NodeType::NODE_VAR_TARGET:   emit_var_target(node); break;

            // ---- SELECT statement ----
            case NodeType::NODE_SELECT_STMT:     emit_select_stmt(node); break;
            case NodeType::NODE_SELECT_OPTIONS:  emit_select_options(node); break;
            case NodeType::NODE_SELECT_ITEM_LIST:emit_select_item_list(node); break;
            case NodeType::NODE_SELECT_ITEM:     emit_select_item(node); break;
            case NodeType::NODE_FROM_CLAUSE:     emit_from_clause(node); break;
            case NodeType::NODE_JOIN_CLAUSE:     emit_join_clause(node); break;
            case NodeType::NODE_WHERE_CLAUSE:    emit_where_clause(node); break;
            case NodeType::NODE_GROUP_BY_CLAUSE: emit_group_by(node); break;
            case NodeType::NODE_HAVING_CLAUSE:   emit_having(node); break;
            case NodeType::NODE_ORDER_BY_CLAUSE: emit_order_by(node); break;
            case NodeType::NODE_ORDER_BY_ITEM:   emit_order_by_item(node); break;
            case NodeType::NODE_LIMIT_CLAUSE:    emit_limit(node); break;
            case NodeType::NODE_LOCKING_CLAUSE:  emit_locking(node); break;
            case NodeType::NODE_INTO_CLAUSE:     emit_into(node); break;

            // ---- Table references ----
            case NodeType::NODE_TABLE_REF:       emit_table_ref(node); break;
            case NodeType::NODE_ALIAS:           emit_alias(node); break;
            case NodeType::NODE_QUALIFIED_NAME:  emit_qualified_name(node); break;

            // ---- Expressions ----
            case NodeType::NODE_BINARY_OP:       emit_binary_op(node); break;
            case NodeType::NODE_UNARY_OP:        emit_unary_op(node); break;
            case NodeType::NODE_FUNCTION_CALL:   emit_function_call(node); break;
            case NodeType::NODE_IS_NULL:         emit_is_null(node); break;
            case NodeType::NODE_IS_NOT_NULL:     emit_is_not_null(node); break;
            case NodeType::NODE_BETWEEN:         emit_between(node); break;
            case NodeType::NODE_IN_LIST:         emit_in_list(node); break;
            case NodeType::NODE_CASE_WHEN:       emit_case_when(node); break;
            case NodeType::NODE_SUBQUERY:        emit_value(node); break;

            // ---- Leaf nodes (emit value directly) ----
            case NodeType::NODE_LITERAL_INT:
            case NodeType::NODE_LITERAL_FLOAT:
            case NodeType::NODE_LITERAL_NULL:
            case NodeType::NODE_COLUMN_REF:
            case NodeType::NODE_ASTERISK:
            case NodeType::NODE_PLACEHOLDER:
            case NodeType::NODE_IDENTIFIER:
                emit_value(node); break;

            case NodeType::NODE_LITERAL_STRING:
                emit_string_literal(node); break;

            default:
                emit_value(node); break;
        }
    }

    void emit_value(const AstNode* node) {
        sb_.append(node->value_ptr, node->value_len);
    }

    void emit_string_literal(const AstNode* node) {
        sb_.append_char('\'');
        sb_.append(node->value_ptr, node->value_len);
        sb_.append_char('\'');
    }

    // ---- SET ----

    void emit_set_stmt(const AstNode* node) {
        sb_.append("SET ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_set_names(const AstNode* node) {
        sb_.append("NAMES ");
        const AstNode* charset = node->first_child;
        if (charset) emit_node(charset);
        const AstNode* collation = charset ? charset->next_sibling : nullptr;
        if (collation) {
            sb_.append(" COLLATE ");
            emit_node(collation);
        }
    }

    void emit_set_charset(const AstNode* node) {
        // Always emit as CHARACTER SET (canonical form)
        sb_.append("CHARACTER SET ");
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_set_transaction(const AstNode* node) {
        sb_.append("TRANSACTION ");
        const AstNode* child = node->first_child;
        // First child may be scope (GLOBAL/SESSION)
        if (child && child->value_len > 0) {
            StringRef val = child->value();
            // Check if this is a scope keyword
            if (val.equals_ci("GLOBAL", 6) || val.equals_ci("SESSION", 7) ||
                val.equals_ci("LOCAL", 5)) {
                // This was already emitted before TRANSACTION by the SET stmt emitter
                child = child->next_sibling;
            }
        }
        if (child) {
            StringRef val = child->value();
            // Check if this is an isolation level or access mode
            if (val.equals_ci("READ ONLY", 9) || val.equals_ci("READ WRITE", 10)) {
                emit_node(child);
            } else {
                // It's an isolation level value
                sb_.append("ISOLATION LEVEL ");
                emit_node(child);
            }
        }
    }

    void emit_var_assignment(const AstNode* node) {
        const AstNode* target = node->first_child;
        const AstNode* rhs = target ? target->next_sibling : nullptr;

        if (target) emit_node(target);
        sb_.append(" = ");
        if (rhs) emit_node(rhs);
    }

    void emit_var_target(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    // ---- SELECT ----

    void emit_select_stmt(const AstNode* node) {
        sb_.append("SELECT ");
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
        }
    }

    void emit_select_options(const AstNode* node) {
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
            sb_.append_char(' ');
        }
    }

    void emit_select_item_list(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_select_item(const AstNode* node) {
        const AstNode* expr = node->first_child;
        if (expr) emit_node(expr);
        const AstNode* alias = expr ? expr->next_sibling : nullptr;
        if (alias && alias->type == NodeType::NODE_ALIAS) {
            emit_node(alias);
        }
    }

    void emit_from_clause(const AstNode* node) {
        sb_.append(" FROM ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_JOIN_CLAUSE) {
                sb_.append_char(' ');
                emit_node(child);
            } else {
                if (!first) sb_.append(", ");
                first = false;
                emit_node(child);
            }
        }
    }

    void emit_table_ref(const AstNode* node) {
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
        }
    }

    void emit_alias(const AstNode* node) {
        sb_.append(" AS ");
        emit_value(node);
    }

    void emit_qualified_name(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char('.');
            first = false;
            emit_node(child);
        }
    }

    void emit_join_clause(const AstNode* node) {
        // Join type stored in node value
        emit_value(node);
        sb_.append_char(' ');
        // Children: table_ref, [ON expr | USING (...)]
        const AstNode* table = node->first_child;
        if (table) {
            emit_node(table);
        }
        const AstNode* condition = table ? table->next_sibling : nullptr;
        if (condition) {
            if (condition->type == NodeType::NODE_IDENTIFIER &&
                condition->value_len == 5 &&
                std::memcmp(condition->value_ptr, "USING", 5) == 0) {
                sb_.append(" USING (");
                bool first = true;
                for (const AstNode* col = condition->first_child; col; col = col->next_sibling) {
                    if (!first) sb_.append(", ");
                    first = false;
                    emit_node(col);
                }
                sb_.append_char(')');
            } else {
                sb_.append(" ON ");
                emit_node(condition);
            }
        }
    }

    void emit_where_clause(const AstNode* node) {
        sb_.append(" WHERE ");
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_group_by(const AstNode* node) {
        sb_.append(" GROUP BY ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_having(const AstNode* node) {
        sb_.append(" HAVING ");
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_order_by(const AstNode* node) {
        sb_.append(" ORDER BY ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_order_by_item(const AstNode* node) {
        const AstNode* expr = node->first_child;
        if (expr) emit_node(expr);
        const AstNode* dir = expr ? expr->next_sibling : nullptr;
        if (dir) {
            sb_.append_char(' ');
            emit_node(dir);
        }
    }

    void emit_limit(const AstNode* node) {
        sb_.append(" LIMIT ");
        const AstNode* first_val = node->first_child;
        if (first_val) emit_node(first_val);
        const AstNode* second_val = first_val ? first_val->next_sibling : nullptr;
        if (second_val) {
            sb_.append(" OFFSET ");
            emit_node(second_val);
        }
    }

    void emit_locking(const AstNode* node) {
        sb_.append(" FOR ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    void emit_into(const AstNode* node) {
        sb_.append(" INTO ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    // ---- Expressions ----

    void emit_binary_op(const AstNode* node) {
        const AstNode* left = node->first_child;
        const AstNode* right = left ? left->next_sibling : nullptr;
        if (left) emit_node(left);
        sb_.append_char(' ');
        emit_value(node);
        sb_.append_char(' ');
        if (right) emit_node(right);
    }

    void emit_unary_op(const AstNode* node) {
        emit_value(node);
        // Add space for keyword operators like NOT, no space for - or +
        if (node->value_len > 1) sb_.append_char(' ');
        if (node->first_child) emit_node(node->first_child);
    }

    void emit_function_call(const AstNode* node) {
        emit_value(node);
        sb_.append_char('(');
        bool first = true;
        for (const AstNode* arg = node->first_child; arg; arg = arg->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(arg);
        }
        sb_.append_char(')');
    }

    void emit_is_null(const AstNode* node) {
        if (node->first_child) emit_node(node->first_child);
        sb_.append(" IS NULL");
    }

    void emit_is_not_null(const AstNode* node) {
        if (node->first_child) emit_node(node->first_child);
        sb_.append(" IS NOT NULL");
    }

    void emit_between(const AstNode* node) {
        const AstNode* expr = node->first_child;
        const AstNode* low = expr ? expr->next_sibling : nullptr;
        const AstNode* high = low ? low->next_sibling : nullptr;
        if (expr) emit_node(expr);
        sb_.append(" BETWEEN ");
        if (low) emit_node(low);
        sb_.append(" AND ");
        if (high) emit_node(high);
    }

    void emit_in_list(const AstNode* node) {
        const AstNode* expr = node->first_child;
        if (expr) emit_node(expr);
        sb_.append(" IN (");
        bool first = true;
        for (const AstNode* val = expr ? expr->next_sibling : nullptr; val; val = val->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(val);
        }
        sb_.append_char(')');
    }

    void emit_case_when(const AstNode* node) {
        sb_.append("CASE ");
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
            sb_.append_char(' ');
        }
        sb_.append("END");
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_EMITTER_H
