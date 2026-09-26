#ifndef SQL_PARSER_EMITTER_H
#define SQL_PARSER_EMITTER_H

#include "sql_parser/common.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/string_builder.h"
#include "sql_parser/parse_result.h"
#include <cstdio>

namespace sql_parser {

enum class EmitMode : uint8_t { NORMAL, DIGEST };

template <Dialect D>
class Emitter {
public:
    explicit Emitter(Arena& arena, EmitMode mode = EmitMode::NORMAL,
                     const ParamBindings* bindings = nullptr)
        : sb_(arena), bindings_(bindings), placeholder_index_(0), mode_(mode) {}

    void emit(const AstNode* node) {
        if (!node) return;
        emit_node(node);
    }

    StringRef result() { return sb_.finish(); }

private:
    StringBuilder sb_;
    const ParamBindings* bindings_;
    uint16_t placeholder_index_;
    EmitMode mode_;

    void emit_node(const AstNode* node) {
        switch (node->type) {
            case NodeType::NODE_TRANSACTION_STMT: emit_transaction_stmt(node); break;
            case NodeType::NODE_COPY_STMT: emit_copy_stmt(node); break;
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
            case NodeType::NODE_ORDER_BY_CLAUSE:
            case NodeType::NODE_AGGREGATE_ORDER_BY: emit_order_by(node); break;
            case NodeType::NODE_ORDER_BY_ITEM:   emit_order_by_item(node); break;
            case NodeType::NODE_LIMIT_CLAUSE:    emit_limit(node); break;
            case NodeType::NODE_LOCKING_CLAUSE:  emit_locking(node); break;
            case NodeType::NODE_INTO_CLAUSE:     emit_into(node); break;

            case NodeType::NODE_DISTINCT_ON:
                sb_.append("DISTINCT ON ("); emit_list(node, ", "); sb_.append_char(')'); break;
            case NodeType::NODE_AGGREGATE_FILTER:
                emit_node(node->first_child); sb_.append(" FILTER (WHERE ");
                emit_node(node->first_child->next_sibling); sb_.append_char(')'); break;
            case NodeType::NODE_LATERAL:
                sb_.append("LATERAL "); emit_node(node->first_child); break;
            case NodeType::NODE_WINDOW_FUNCTION:
                emit_node(node->first_child); sb_.append(" OVER ");
                emit_node(node->first_child->next_sibling); break;
            case NodeType::NODE_WINDOW_SPEC:
                sb_.append_char('('); emit_list(node, " "); sb_.append_char(')'); break;
            case NodeType::NODE_WINDOW_PARTITION:
                sb_.append("PARTITION BY "); emit_list(node, ", "); break;
            case NodeType::NODE_WINDOW_ORDER:
                sb_.append("ORDER BY "); emit_list(node, ", "); break;
            case NodeType::NODE_WINDOW_CLAUSE:
                sb_.append(" WINDOW "); emit_list(node, ", "); break;
            case NodeType::NODE_WINDOW_DEFINITION:
                emit_value(node); sb_.append(" AS "); emit_node(node->first_child); break;
            case NodeType::NODE_WINDOW_REFERENCE:
                emit_value(node); break;
            case NodeType::NODE_WINDOW_FRAME: emit_window_frame(node); break;
            case NodeType::NODE_WINDOW_BOUND:
                if (node->first_child) { emit_node(node->first_child); sb_.append_char(' '); }
                emit_value(node); break;
            case NodeType::NODE_WINDOW_EXCLUSION:
                sb_.append("EXCLUDE "); emit_value(node); break;

            // ---- INSERT statement ----
            case NodeType::NODE_INSERT_STMT:     emit_insert_stmt(node); break;
            case NodeType::NODE_INSERT_COLUMNS:  emit_insert_columns(node); break;
            case NodeType::NODE_VALUES_CLAUSE:   emit_values_clause(node); break;
            case NodeType::NODE_VALUES_ROW:      emit_values_row(node); break;
            case NodeType::NODE_INSERT_SET_CLAUSE: emit_insert_set_clause(node); break;
            case NodeType::NODE_ON_DUPLICATE_KEY: emit_on_duplicate_key(node); break;
            case NodeType::NODE_ON_CONFLICT:     emit_on_conflict(node); break;
            case NodeType::NODE_CONFLICT_TARGET: emit_conflict_target(node); break;
            case NodeType::NODE_CONFLICT_ACTION: emit_conflict_action(node); break;
            case NodeType::NODE_RETURNING_CLAUSE: emit_returning(node); break;
            case NodeType::NODE_STMT_OPTIONS:    emit_stmt_options(node); break;
            case NodeType::NODE_UPDATE_SET_ITEM: emit_update_set_item(node); break;

            // ---- Star modifiers ----
            case NodeType::NODE_STAR_EXCEPT:  emit_star_except(node); break;
            case NodeType::NODE_STAR_REPLACE: emit_star_replace(node); break;
            case NodeType::NODE_REPLACE_ITEM: emit_replace_item(node); break;

            case NodeType::NODE_CTE: emit_cte(node); break;
            case NodeType::NODE_CTE_COLUMNS: emit_list(node, ", "); break;

            // ---- Compound query ----
            case NodeType::NODE_COMPOUND_QUERY:  emit_compound_query(node); break;
            case NodeType::NODE_SET_OPERATION:   emit_set_operation(node); break;
            case NodeType::NODE_TABLE_QUERY:     emit_table_query(node); break;

            // ---- DELETE statement ----
            case NodeType::NODE_DELETE_STMT:          emit_delete_stmt(node); break;
            case NodeType::NODE_DELETE_USING_CLAUSE:  emit_delete_using(node); break;

            // ---- EXPLAIN / DESCRIBE ----
            case NodeType::NODE_EXPLAIN_STMT:         emit_explain_stmt(node); break;
            case NodeType::NODE_EXPLAIN_OPTIONS:      emit_explain_options(node); break;
            case NodeType::NODE_EXPLAIN_FORMAT:       emit_explain_format(node); break;

            // ---- CALL ----
            case NodeType::NODE_CALL_STMT:            emit_call_stmt(node); break;

            // ---- DO ----
            case NodeType::NODE_DO_STMT:              emit_do_stmt(node); break;

            // ---- LOAD DATA ----
            case NodeType::NODE_LOAD_DATA_STMT:       emit_load_data_stmt(node); break;
            case NodeType::NODE_LOAD_DATA_OPTIONS:    /* emitted inline */ break;

            // ---- UPDATE statement ----
            case NodeType::NODE_UPDATE_STMT:     emit_update_stmt(node); break;
            case NodeType::NODE_UPDATE_SET_CLAUSE: emit_update_set_clause(node); break;

            // ---- Table references ----
            case NodeType::NODE_TABLE_REF:       emit_table_ref(node); break;
            case NodeType::NODE_ALIAS:           emit_alias(node); break;
            case NodeType::NODE_QUALIFIED_NAME:  emit_qualified_name(node); break;

            case NodeType::NODE_TYPE_CAST:
                sb_.append("CAST(");
                if (node->first_child) {
                    emit_node(node->first_child);
                    sb_.append(" AS ");
                    if (node->first_child->next_sibling) emit_node(node->first_child->next_sibling);
                }
                sb_.append_char(')'); break;
            case NodeType::NODE_TYPE_NAME: emit_value(node); break;
            case NodeType::NODE_NAMED_ARGUMENT:
                emit_value(node); sb_.append(" => ");
                if (node->first_child) emit_node(node->first_child);
                break;
            // ---- Expressions ----
            case NodeType::NODE_BINARY_OP:       emit_binary_op(node); break;
            case NodeType::NODE_UNARY_OP:        emit_unary_op(node); break;
            case NodeType::NODE_FUNCTION_CALL:   emit_function_call(node); break;
            case NodeType::NODE_IS_NULL:         emit_is_null(node); break;
            case NodeType::NODE_IS_NOT_NULL:     emit_is_not_null(node); break;
            case NodeType::NODE_BETWEEN:         emit_between(node); break;
            case NodeType::NODE_IN_LIST:         emit_in_list(node); break;
            case NodeType::NODE_CASE_WHEN:       emit_case_when(node); break;
            case NodeType::NODE_TUPLE:           emit_tuple(node); break;
            case NodeType::NODE_ARRAY_CONSTRUCTOR: emit_array_constructor(node); break;
            case NodeType::NODE_ARRAY_SUBSCRIPT: emit_array_subscript(node); break;
            case NodeType::NODE_FIELD_ACCESS:    emit_field_access(node); break;
            case NodeType::NODE_SUBQUERY:        emit_subquery(node); break;
            case NodeType::NODE_EXPRESSION:      emit_parenthesized_expression(node); break;
            case NodeType::NODE_USER_VARIABLE:   emit_user_variable(node); break;

            // ---- Leaf nodes (emit value directly) ----
            case NodeType::NODE_PLACEHOLDER:
                emit_placeholder(node); break;

            // ---- Leaf nodes (emit value directly) ----
            case NodeType::NODE_LITERAL_INT:
            case NodeType::NODE_LITERAL_FLOAT:
            case NodeType::NODE_LITERAL_HEX:
            case NodeType::NODE_LITERAL_BIT:
                if (mode_ == EmitMode::DIGEST) { sb_.append_char('?'); break; }
                emit_value(node); break;
            case NodeType::NODE_COLUMN_REF:
            case NodeType::NODE_IDENTIFIER:
                emit_identifier(node); break;
            case NodeType::NODE_LITERAL_NULL:
            case NodeType::NODE_ASTERISK:
                emit_value(node); break;

            case NodeType::NODE_LITERAL_STRING:
                if (mode_ == EmitMode::DIGEST) { sb_.append_char('?'); break; }
                emit_string_literal(node); break;

            // PG_CONT_EXPRESSION_DISPATCH
            case NodeType::NODE_PG_VARIADIC_ARGUMENT:
                sb_.append("VARIADIC "); emit_node(node->first_child); break;
            case NodeType::NODE_PG_ARRAY_SLICE:
            case NodeType::NODE_PG_PATTERN_PREDICATE:
            case NodeType::NODE_PG_POSITION:
            case NodeType::NODE_PG_OVERLAY:
            case NodeType::NODE_PG_JSON_PREDICATE:
                emit_pg_continuation_expression(node); break;
            // PG_CONT_QUERY_DISPATCH
            case NodeType::NODE_PG_EXPLAIN_OPTION:
                emit_value(node);
                if (node->first_child) { sb_.append_char(' '); emit_node(node->first_child); }
                break;
            case NodeType::NODE_PG_JOIN_TREE: emit_pg_join_tree(node); break;
            case NodeType::NODE_PG_TABLE_GROUP:
                sb_.append_char('('); emit_node(node->first_child); sb_.append_char(')'); break;
            case NodeType::NODE_PG_JOIN_USING:
                sb_.append("USING (");
                for (const auto* col = node->first_child; col; col = col->next_sibling) {
                    if (col != node->first_child) sb_.append(", ");
                    emit_node(col);
                }
                sb_.append_char(')');
                if (!node->value().empty()) { sb_.append(" AS "); emit_value(node); }
                break;
            case NodeType::NODE_PG_TABLESAMPLE: emit_pg_tablesample(node); break;
            case NodeType::NODE_PG_ROWS_FROM:
                sb_.append("ROWS FROM (");
                for (const auto* item = node->first_child; item; item = item->next_sibling) {
                    if (item != node->first_child) sb_.append(", ");
                    emit_node(item);
                }
                sb_.append_char(')'); break;
            case NodeType::NODE_PG_SORT_USING:
                sb_.append("USING "); emit_value(node); break;
            case NodeType::NODE_PG_SELECT_INTO:
                sb_.append(" INTO "); emit_value(node); emit_node(node->first_child); break;
            case NodeType::NODE_PG_ROW_LOCK:
                sb_.append(" FOR "); emit_value(node);
                if (node->first_child) {
                    sb_.append(" OF ");
                    for (const auto* rel = node->first_child; rel; rel = rel->next_sibling) {
                        if (rel != node->first_child) sb_.append(", ");
                        emit_node(rel);
                    }
                }
                if (node->flags == 1) sb_.append(" NOWAIT");
                if (node->flags == 2) sb_.append(" SKIP LOCKED");
                break;
            // PG_GAPS_EXPRESSION_DISPATCH
            case NodeType::NODE_PG_EXTRACT:
            case NodeType::NODE_PG_SUBSTRING:
            case NodeType::NODE_PG_TIME_ZONE:
            case NodeType::NODE_PG_INTERVAL:
            case NodeType::NODE_PG_TRIM:
            case NodeType::NODE_PG_ARRAY_QUERY:
            case NodeType::NODE_PG_QUANTIFIED_OPERAND:
            case NodeType::NODE_PG_NORMALIZE:
                emit_pg_special_expression(node); break;
            // PG_GAPS_DML_DISPATCH
            case NodeType::NODE_MERGE_STMT: emit_pg_merge(node); break;
            case NodeType::NODE_MERGE_WHEN:
                sb_.append("WHEN "); emit_pg_dml_clause(node); break;
            case NodeType::NODE_PG_DML_CLAUSE: emit_pg_dml_clause(node); break;
            case NodeType::NODE_CTE_SEARCH: emit_pg_cte_search(node); break;
            case NodeType::NODE_CTE_CYCLE: emit_pg_cte_cycle(node); break;
            case NodeType::NODE_PG_RETURNING_OPTIONS:
                sb_.append("WITH ("); emit_list(node, ", "); sb_.append_char(')'); break;
            case NodeType::NODE_PG_ASSIGNMENT_FIELD:
                emit_node(node->first_child); sb_.append_char('.');
                emit_node(node->first_child ? node->first_child->next_sibling : nullptr); break;
            // PG_GAPS_DDL_DISPATCH
            case NodeType::NODE_PG_DDL_STMT:
            case NodeType::NODE_PG_DDL_CLAUSE: emit_pg_ddl_clause(node); break;
            case NodeType::NODE_PG_DDL_LIST:
                if (!(node->flags & 1)) sb_.append_char('(');
                emit_list(node, ", ");
                if (!(node->flags & 1)) sb_.append_char(')');
                break;
            case NodeType::NODE_PG_DDL_SYNTAX: emit_value(node); break;
            // PG_GAPS_QUERY_DISPATCH
            case NodeType::NODE_GROUPING_SET:
                emit_value(node); sb_.append(" ("); emit_list(node, ", "); sb_.append_char(')'); break;
            case NodeType::NODE_OFFSET_CLAUSE:
                sb_.append(" OFFSET "); emit_node(node->first_child); break;
            case NodeType::NODE_FETCH_CLAUSE:
                sb_.append(" FETCH FIRST "); emit_node(node->first_child);
                sb_.append(node->flags ? " ROWS WITH TIES" : " ROWS ONLY"); break;
            case NodeType::NODE_ORDINALITY: sb_.append(" WITH ORDINALITY"); break;
            case NodeType::NODE_FUNCTION_COLUMN:
                emit_node(node->first_child); sb_.append_char(' ');
                emit_node(node->first_child->next_sibling); break;
            // PG_GAPS_JSON_XML_DISPATCH
            case NodeType::NODE_PG_JSON_XML: emit_pg_json_xml(node); break;
            case NodeType::NODE_PG_JSON_XML_SYNTAX: emit_value(node); break;
            default:
                emit_value(node); break;
        }
    }

    // PG_CONT_EXPRESSION_EMITTER
    void emit_pg_continuation_expression(const AstNode* node) {
        const AstNode* first = node->first_child;
        if (node->type == NodeType::NODE_PG_ARRAY_SLICE) {
            emit_node(first); sb_.append_char('[');
            const AstNode* bound = first ? first->next_sibling : nullptr;
            if (node->flags & 1) { emit_node(bound); bound = bound ? bound->next_sibling : nullptr; }
            sb_.append_char(':');
            if (node->flags & 2) emit_node(bound);
            sb_.append_char(']');
        } else if (node->type == NodeType::NODE_PG_PATTERN_PREDICATE) {
            emit_node(first); sb_.append_char(' '); emit_value(node); sb_.append_char(' ');
            const AstNode* pattern = first ? first->next_sibling : nullptr;
            emit_node(pattern);
            if (pattern && pattern->next_sibling) {
                sb_.append(" ESCAPE "); emit_node(pattern->next_sibling);
            }
        } else if (node->type == NodeType::NODE_PG_JSON_PREDICATE) {
            emit_node(first); sb_.append_char(' '); emit_value(node);
        } else if (node->type == NodeType::NODE_PG_POSITION) {
            sb_.append("POSITION("); emit_node(first); sb_.append(" IN ");
            emit_node(first ? first->next_sibling : nullptr); sb_.append_char(')');
        } else {
            sb_.append("OVERLAY(");
            if (node->flags & 1) emit_list(node, ", ");
            else {
                const AstNode* replacement = first ? first->next_sibling : nullptr;
                const AstNode* start = replacement ? replacement->next_sibling : nullptr;
                emit_node(first); sb_.append(" PLACING "); emit_node(replacement);
                sb_.append(" FROM "); emit_node(start);
                if (start && start->next_sibling) { sb_.append(" FOR "); emit_node(start->next_sibling); }
            }
            sb_.append_char(')');
        }
    }
    // PG_CONT_QUERY_EMITTER
    void emit_pg_join_tree(const AstNode* node) {
        const auto* left = node->first_child;
        const auto* right = left->next_sibling;
        const auto* qual = right->next_sibling;
        sb_.append_char('('); emit_node(left); sb_.append_char(' ');
        emit_value(node); sb_.append_char(' '); emit_node(right);
        if (qual) {
            sb_.append(qual->type == NodeType::NODE_PG_JOIN_USING ? " " : " ON ");
            emit_node(qual);
        }
        sb_.append_char(')');
    }
    void emit_pg_tablesample(const AstNode* node) {
        const auto* name = node->first_child;
        const auto* args = name->next_sibling;
        sb_.append(" TABLESAMPLE "); emit_node(name); emit_node(args);
        if (args->next_sibling) {
            sb_.append(" REPEATABLE ("); emit_node(args->next_sibling); sb_.append_char(')');
        }
    }

    // PG_GAPS_EXPRESSION_EMITTER
    void emit_pg_special_expression(const AstNode* node) {
        const AstNode* first = node->first_child;
        if (node->type == NodeType::NODE_PG_NORMALIZE) {
            sb_.append("NORMALIZE("); emit_node(first);
            if (!node->value().empty()) { sb_.append(", "); emit_value(node); }
            sb_.append_char(')');
        } else if (node->type == NodeType::NODE_PG_QUANTIFIED_OPERAND) {
            emit_value(node);
            if (node->flags) emit_node(first);
            else { sb_.append_char('('); emit_node(first); sb_.append_char(')'); }
        } else if (node->type == NodeType::NODE_PG_ARRAY_QUERY) {
            sb_.append("ARRAY");
            emit_node(first);
        } else if (node->type == NodeType::NODE_PG_TRIM) {
            sb_.append("TRIM(");
            if (!node->value().empty()) { emit_value(node); sb_.append_char(' '); }
            if (node->flags & 2) {
                emit_node(first); sb_.append_char(' ');
                first = first->next_sibling;
            }
            if (node->flags & 1) sb_.append("FROM ");
            for (const auto* child = first; child; child = child->next_sibling) {
                if (child != first) sb_.append(", ");
                emit_node(child);
            }
            sb_.append_char(')');
        } else if (node->type == NodeType::NODE_PG_TIME_ZONE) {
            emit_node(first);
            sb_.append(node->flags ? " AT LOCAL" : " AT TIME ZONE ");
            if (!node->flags) emit_node(first->next_sibling);
        } else if (node->type == NodeType::NODE_PG_INTERVAL) {
            emit_value(node);
            sb_.append_char(' ');
            emit_node(first);
            if (first->next_sibling) {
                sb_.append_char(' ');
                emit_node(first->next_sibling);
            }
        } else if (node->type == NodeType::NODE_PG_EXTRACT) {
            sb_.append("EXTRACT(");
            emit_value(node); // field is grammar syntax, including quoted spelling
            sb_.append(" FROM ");
            emit_node(first);
            sb_.append_char(')');
        } else {
            sb_.append("SUBSTRING(");
            emit_node(first);
            const AstNode* second = first->next_sibling;
            sb_.append(node->flags == 2 ? " SIMILAR " : node->flags == 1 ? " FOR " : " FROM ");
            emit_node(second);
            if (second->next_sibling) {
                sb_.append(node->flags == 2 ? " ESCAPE " : node->flags == 1 ? " FROM " : " FOR ");
                emit_node(second->next_sibling);
            }
            sb_.append_char(')');
        }
    }
    // PG_GAPS_DML_EMITTER
    void emit_pg_dml_clause(const AstNode* node) {
        emit_value(node);
        bool separator = !node->value().empty();
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (separator) sb_.append_char(' ');
            emit_node(c);
            separator = true;
        }
    }

    void emit_pg_merge(const AstNode* node) {
        sb_.append("MERGE INTO ");
        bool first = true;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (!first) sb_.append_char(' ');
            emit_node(c);
            first = false;
        }
    }

    void emit_pg_cte_search(const AstNode* node) {
        sb_.append("SEARCH "); emit_value(node); sb_.append(" FIRST BY ");
        const AstNode* cols = node->first_child;
        emit_node(cols);
        sb_.append(" SET "); emit_node(cols ? cols->next_sibling : nullptr);
    }

    void emit_pg_cte_cycle(const AstNode* node) {
        sb_.append("CYCLE ");
        const AstNode* c = node->first_child;
        emit_node(c); c = c ? c->next_sibling : nullptr;
        sb_.append(" SET "); emit_node(c); c = c ? c->next_sibling : nullptr;
        if (node->flags & 1) {
            sb_.append(" TO "); emit_pg_cycle_constant(c); c = c ? c->next_sibling : nullptr;
            sb_.append(" DEFAULT "); emit_pg_cycle_constant(c); c = c ? c->next_sibling : nullptr;
        }
        sb_.append(" USING "); emit_node(c);
    }

    void emit_pg_cycle_constant(const AstNode* node) {
        // CYCLE requires AexprConst; CAST(...) is not accepted in this position.
        if (node && node->type == NodeType::NODE_TYPE_CAST && node->first_child) {
            emit_node(node->first_child->next_sibling);
            sb_.append_char(' ');
            emit_node(node->first_child);
        } else emit_node(node);
    }
    // PG_GAPS_DDL_EMITTER
    void emit_pg_ddl_clause(const AstNode* node) {
        emit_value(node);
        if (!node->value().empty() && node->first_child) sb_.append_char(' ');
        emit_list(node, " ");
    }
    // PG_GAPS_QUERY_EMITTER
    // PG_GAPS_JSON_XML_EMITTER
    void emit_pg_json_xml(const AstNode* node) {
        if (!node->value().empty()) emit_value(node);
        if (node->flags & 1) sb_.append_char('(');
        else if (!node->value().empty() && node->first_child) sb_.append_char(' ');
        emit_list(node, " ");
        if (node->flags & 1) sb_.append_char(')');
    }
    void emit_list(const AstNode* node, const char* separator) {
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child != node->first_child) sb_.append(separator);
            emit_node(child);
        }
    }

    void emit_window_frame(const AstNode* node) {
        emit_value(node);
        sb_.append(node->flags & FLAG_WINDOW_BETWEEN ? " BETWEEN " : " ");
        const AstNode* bound = node->first_child;
        emit_node(bound);
        bound = bound ? bound->next_sibling : nullptr;
        if (node->flags & FLAG_WINDOW_BETWEEN) {
            sb_.append(" AND "); emit_node(bound);
            bound = bound ? bound->next_sibling : nullptr;
        }
        if (bound) { sb_.append_char(' '); emit_node(bound); }
    }

    void emit_identifier(const AstNode* node) {
        if (!(node->flags & FLAG_IDENT_DELIMITED)) { emit_value(node); return; }
        if (!node->source().empty()) {
            sb_.append(node->source_ptr, node->source_len);
            return;
        }
        const char quote = D == Dialect::PostgreSQL ? '"' : '`';
        sb_.append_char(quote);
        for (uint32_t i = 0; i < node->value_len; ++i) {
            sb_.append_char(node->value_ptr[i]);
            if (node->value_ptr[i] == quote) sb_.append_char(quote);
        }
        sb_.append_char(quote);
    }

    void emit_value(const AstNode* node) {
        sb_.append(node->value());
    }

    void emit_utility_value(const AstNode* node) {
        // Utility values are syntax constants. Keep source string quoting,
        // including E'...' and dollar quotes, even in digest mode.
        if (node->type == NodeType::NODE_LITERAL_STRING && !node->source().empty()) {
            sb_.append(node->source_ptr, node->source_len);
        } else emit_node(node);
    }

    void emit_transaction_stmt(const AstNode* node) {
        emit_value(node);
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            sb_.append_char(' ');
            emit_utility_value(child);
        }
    }

    void emit_copy_stmt(const AstNode* node) {
        sb_.append("COPY ", 5);
        const AstNode* child = node->first_child;
        if (!child) return;
        emit_node(child);
        child = child->next_sibling;
        if (child && child->type == NodeType::NODE_INSERT_COLUMNS) {
            sb_.append_char(' ');
            emit_node(child);
            child = child->next_sibling;
        }
        sb_.append_char(' ');
        emit_value(node);
        sb_.append_char(' ');
        if (!child || child->type != NodeType::NODE_COPY_ENDPOINT) return;
        if (!child->value().equals_ci("FILE", 4)) emit_value(child);
        if (child->first_child) {
            if (child->value().equals_ci("PROGRAM", 7)) sb_.append_char(' ');
            emit_utility_value(child->first_child);
        }
        child = child->next_sibling;
        if (child && child->type == NodeType::NODE_COPY_OPTION) {
            sb_.append(" WITH (", 7);
            bool first = true;
            while (child && child->type == NodeType::NODE_COPY_OPTION) {
                if (!first) sb_.append(", ", 2);
                emit_value(child);
                if (child->first_child) {
                    sb_.append_char(' ');
                    emit_utility_value(child->first_child);
                }
                first = false;
                child = child->next_sibling;
            }
            sb_.append_char(')');
        }
        if (child && child->type == NodeType::NODE_WHERE_CLAUSE) {
            emit_node(child);
        }
    }

    void emit_user_variable(const AstNode* node) {
        if (mode_ == EmitMode::DIGEST) {
            StringRef source = node->source();
            if (source.len >= 2 &&
                (source.ptr[1] == '\'' || source.ptr[1] == '"' || source.ptr[1] == '`')) {
                sb_.append("@?", 2);
                return;
            }
        }
        StringRef source = node->source();
        if (!source.empty()) {
            sb_.append(source.ptr, source.len);
            return;
        }
        sb_.append_char('@');
        emit_value(node);
    }

    void emit_parenthesized_expression(const AstNode* node) {
        sb_.append_char('(');
        if (node->first_child) emit_node(node->first_child);
        sb_.append_char(')');
    }

    void emit_string_literal(const AstNode* node) {
        if constexpr (D == Dialect::PostgreSQL) {
            if (!node->source().empty() &&
                (node->source_ptr[0] == 'E' || node->source_ptr[0] == 'e' || node->source_ptr[0] == '$')) {
                sb_.append(node->source_ptr, node->source_len);
                return;
            }
        }
        sb_.append_char('\'');
        sb_.append(node->value_ptr, node->value_len);
        sb_.append_char('\'');
    }

    void emit_placeholder(const AstNode* node) {
        if (bindings_ && placeholder_index_ < bindings_->count) {
            const BoundValue& bv = bindings_->values[placeholder_index_];
            ++placeholder_index_;
            switch (bv.type) {
                case BoundValue::INT:
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
            // No binding available -- emit placeholder as-is
            emit_value(node);
        }
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
        if constexpr (D == Dialect::PostgreSQL) {
            if (node->flags & FLAG_TABLE_ONLY) sb_.append("ONLY ");
        }
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            emit_node(child);
            if constexpr (D == Dialect::PostgreSQL) {
                if (child == node->first_child && (node->flags & FLAG_TABLE_INHERIT)) sb_.append(" *");
            }
        }
    }

    void emit_alias(const AstNode* node) {
        if (mode_ == EmitMode::DIGEST) return;  // skip aliases in digest mode
        sb_.append(" AS ");
        emit_value(node);
        if (node->first_child) {
            sb_.append_char('('); emit_list(node, ", "); sb_.append_char(')');
        }
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
        if (!node->value().empty()) { emit_value(node); sb_.append_char(' '); }
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
        for (; dir; dir = dir->next_sibling) {
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

    // ---- INSERT ----

    void emit_insert_stmt(const AstNode* node) {
        // Check FLAG_REPLACE
        if (node->flags & 0x01) {
            sb_.append("REPLACE");
        } else {
            sb_.append("INSERT");
        }

        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            switch (child->type) {
                case NodeType::NODE_STMT_OPTIONS:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_TABLE_REF:
                    sb_.append(" INTO ");
                    emit_node(child);
                    break;
                case NodeType::NODE_INSERT_COLUMNS:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_VALUES_CLAUSE:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_SELECT_STMT:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_INSERT_SET_CLAUSE:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_ON_DUPLICATE_KEY:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_ON_CONFLICT:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_RETURNING_CLAUSE:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                default:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
            }
        }
    }

    void emit_stmt_options(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    void emit_insert_columns(const AstNode* node) {
        sb_.append_char('(');
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
        sb_.append_char(')');
    }

    void emit_values_clause(const AstNode* node) {
        // Check for DEFAULT VALUES (value stored in node)
        if (node->value_len > 0) {
            emit_value(node);  // "DEFAULT VALUES"
            return;
        }
        sb_.append("VALUES ");
        if (mode_ == EmitMode::DIGEST) {
            // Collapse to single row in digest mode
            if (node->first_child) emit_node(node->first_child);
        } else {
            bool first = true;
            for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
                if (!first) sb_.append(", ");
                first = false;
                emit_node(child);
            }
        }
    }

    void emit_values_row(const AstNode* node) {
        sb_.append_char('(');
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
        sb_.append_char(')');
    }

    void emit_insert_set_clause(const AstNode* node) {
        sb_.append("SET ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_update_set_item(const AstNode* node) {
        const AstNode* col = node->first_child;
        const AstNode* val = col ? col->next_sibling : nullptr;
        if (col) emit_node(col);
        sb_.append(" = ");
        if (val) emit_node(val);
    }

    void emit_on_duplicate_key(const AstNode* node) {
        sb_.append("ON DUPLICATE KEY UPDATE ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    void emit_on_conflict(const AstNode* node) {
        sb_.append("ON CONFLICT");
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            sb_.append_char(' ');
            emit_node(child);
        }
    }

    void emit_conflict_target(const AstNode* node) {
        if (node->value_len > 0) {
            // ON CONSTRAINT name
            emit_value(node);  // "ON CONSTRAINT"
            sb_.append_char(' ');
            if (node->first_child) emit_node(node->first_child);
        } else {
            // (col1, col2, ...)
            sb_.append_char('(');
            bool first = true;
            for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
                if (!first) sb_.append(", ");
                first = false;
                emit_node(child);
            }
            sb_.append_char(')');
        }
    }

    void emit_conflict_action(const AstNode* node) {
        sb_.append("DO ");
        StringRef action_type{node->value_ptr, node->value_len};
        if (action_type.equals_ci("NOTHING", 7)) {
            sb_.append("NOTHING");
        } else if (action_type.equals_ci("UPDATE", 6)) {
            sb_.append("UPDATE SET ");
            bool first = true;
            for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
                if (child->type == NodeType::NODE_WHERE_CLAUSE) {
                    emit_node(child);
                } else {
                    if (!first) sb_.append(", ");
                    first = false;
                    emit_node(child);
                }
            }
        }
    }

    void emit_returning(const AstNode* node) {
        sb_.append("RETURNING ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_PG_RETURNING_OPTIONS) {
                emit_node(child);
                sb_.append_char(' ');
            } else if (child->type == NodeType::NODE_ALIAS) {
                emit_node(child);
            } else {
                if (!first) sb_.append(", ");
                first = false;
                emit_node(child);
            }
        }
    }

    // ---- UPDATE ----

    void emit_update_stmt(const AstNode* node) {
        sb_.append("UPDATE");

        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            switch (child->type) {
                case NodeType::NODE_STMT_OPTIONS:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_TABLE_REF:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                case NodeType::NODE_FROM_CLAUSE:
                    {
                        // Check if FROM_CLAUSE is before SET_CLAUSE (MySQL multi-table)
                        // or after (PostgreSQL FROM)
                        bool is_before_set = false;
                        for (const AstNode* s = child->next_sibling; s; s = s->next_sibling) {
                            if (s->type == NodeType::NODE_UPDATE_SET_CLAUSE) {
                                is_before_set = true;
                                break;
                            }
                        }
                        if (is_before_set) {
                            // MySQL multi-table: emit table refs without FROM keyword
                            sb_.append_char(' ');
                            bool first = true;
                            for (const AstNode* c = child->first_child; c; c = c->next_sibling) {
                                if (c->type == NodeType::NODE_JOIN_CLAUSE) {
                                    sb_.append_char(' ');
                                    emit_node(c);
                                } else {
                                    if (!first) sb_.append(", ");
                                    first = false;
                                    emit_node(c);
                                }
                            }
                        } else {
                            // PostgreSQL FROM clause (emit_from_clause adds " FROM " prefix)
                            emit_node(child);
                        }
                    }
                    break;
                case NodeType::NODE_UPDATE_SET_CLAUSE:
                    sb_.append(" SET ");
                    emit_update_set_clause_inner(child);
                    break;
                case NodeType::NODE_WHERE_CLAUSE:
                    emit_node(child);
                    break;
                case NodeType::NODE_ORDER_BY_CLAUSE:
                    emit_node(child);
                    break;
                case NodeType::NODE_LIMIT_CLAUSE:
                    emit_node(child);
                    break;
                case NodeType::NODE_RETURNING_CLAUSE:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
                default:
                    sb_.append_char(' ');
                    emit_node(child);
                    break;
            }
        }
    }

    void emit_update_set_clause(const AstNode* node) {
        sb_.append("SET ");
        emit_update_set_clause_inner(node);
    }

    void emit_update_set_clause_inner(const AstNode* node) {
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    // ---- DELETE ----

    void emit_delete_stmt(const AstNode* node) {
        sb_.append("DELETE");

        // Flags determine the form
        bool is_multi = (node->flags & 0x01) != 0;
        bool is_form2 = (node->flags & 0x02) != 0;

        if (is_multi && !is_form2) {
            // Form 1: DELETE [opts] t1, t2 FROM table_refs [WHERE]
            for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
                switch (child->type) {
                    case NodeType::NODE_STMT_OPTIONS:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                    case NodeType::NODE_TABLE_REF: {
                        // Check if previous sibling is also TABLE_REF (needs comma)
                        bool prev_is_table = false;
                        for (const AstNode* p = node->first_child; p != child; p = p->next_sibling) {
                            if (p->type == NodeType::NODE_TABLE_REF) prev_is_table = true;
                            else prev_is_table = false;
                        }
                        if (prev_is_table) sb_.append(", ");
                        else sb_.append_char(' ');
                        emit_node(child);
                        break;
                    }
                    case NodeType::NODE_FROM_CLAUSE:
                        emit_node(child);
                        break;
                    case NodeType::NODE_WHERE_CLAUSE:
                        emit_node(child);
                        break;
                    default:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                }
            }
        } else if (is_multi && is_form2) {
            // Form 2: DELETE [opts] FROM t1, t2 USING table_refs [WHERE]
            bool first_table = true;
            for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
                switch (child->type) {
                    case NodeType::NODE_STMT_OPTIONS:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                    case NodeType::NODE_TABLE_REF:
                        if (first_table) {
                            sb_.append(" FROM ");
                            first_table = false;
                        } else {
                            sb_.append(", ");
                        }
                        emit_node(child);
                        break;
                    case NodeType::NODE_DELETE_USING_CLAUSE:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                    case NodeType::NODE_WHERE_CLAUSE:
                        emit_node(child);
                        break;
                    default:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                }
            }
        } else {
            // Single-table or PostgreSQL
            for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
                switch (child->type) {
                    case NodeType::NODE_STMT_OPTIONS:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                    case NodeType::NODE_TABLE_REF:
                        sb_.append(" FROM ");
                        emit_node(child);
                        break;
                    case NodeType::NODE_DELETE_USING_CLAUSE:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                    case NodeType::NODE_WHERE_CLAUSE:
                        emit_node(child);
                        break;
                    case NodeType::NODE_ORDER_BY_CLAUSE:
                        emit_node(child);
                        break;
                    case NodeType::NODE_LIMIT_CLAUSE:
                        emit_node(child);
                        break;
                    case NodeType::NODE_RETURNING_CLAUSE:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                    default:
                        sb_.append_char(' ');
                        emit_node(child);
                        break;
                }
            }
        }
    }

    void emit_delete_using(const AstNode* node) {
        sb_.append("USING ");
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

    // ---- EXPLAIN / DESCRIBE ----

    void emit_explain_stmt(const AstNode* node) {
        // Check if there's an inner statement or options (EXPLAIN) vs bare table ref (DESCRIBE)
        bool has_inner_stmt = false;
        bool has_options = false;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == NodeType::NODE_SELECT_STMT ||
                c->type == NodeType::NODE_INSERT_STMT ||
                c->type == NodeType::NODE_UPDATE_STMT ||
                c->type == NodeType::NODE_DELETE_STMT ||
                c->type == NodeType::NODE_COMPOUND_QUERY) {
                has_inner_stmt = true;
            }
            if (c->type == NodeType::NODE_EXPLAIN_OPTIONS) {
                has_options = true;
            }
        }

        if (D == Dialect::MySQL && !has_inner_stmt && !has_options) {
            sb_.append("DESCRIBE");
        } else {
            sb_.append("EXPLAIN");
        }

        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            sb_.append_char(' ');
            emit_node(child);
        }
    }

    void emit_explain_options(const AstNode* node) {
        if constexpr (D == Dialect::PostgreSQL) {
            if (node->flags & 1) {
                sb_.append_char('('); emit_list(node, ", "); sb_.append_char(')'); return;
            }
        }
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append_char(' ');
            first = false;
            emit_node(child);
        }
    }

    void emit_explain_format(const AstNode* node) {
        sb_.append("FORMAT = ");
        emit_value(node);
    }

    // ---- CALL ----

    void emit_call_stmt(const AstNode* node) {
        sb_.append("CALL ");
        const AstNode* name = node->first_child;
        if (!name) return;

        emit_node(name);
        sb_.append_char('(');

        bool first = true;
        for (const AstNode* arg = name->next_sibling; arg; arg = arg->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(arg);
        }
        sb_.append_char(')');
    }

    // ---- DO ----

    void emit_do_stmt(const AstNode* node) {
        sb_.append("DO ");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
    }

    // ---- LOAD DATA ----

    void emit_load_data_stmt(const AstNode* node) {
        sb_.append("LOAD DATA");

        // First pass: emit options that come before INFILE (LOW_PRIORITY/CONCURRENT, LOCAL)
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_LOAD_DATA_OPTIONS) {
                for (const AstNode* opt = child->first_child; opt; opt = opt->next_sibling) {
                    if (opt->type == NodeType::NODE_IDENTIFIER) {
                        StringRef val = opt->value();
                        if (val.equals_ci("LOW_PRIORITY", 12)) {
                            sb_.append(" LOW_PRIORITY");
                        } else if (val.equals_ci("CONCURRENT", 10)) {
                            sb_.append(" CONCURRENT");
                        } else if (val.equals_ci("LOCAL", 5)) {
                            sb_.append(" LOCAL");
                        }
                    }
                }
            }
        }

        // INFILE 'filename'
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_LITERAL_STRING) {
                sb_.append(" INFILE ");
                emit_string_literal(child);
                break;
            }
        }

        // REPLACE/IGNORE between filename and INTO TABLE
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_LOAD_DATA_OPTIONS) {
                for (const AstNode* opt = child->first_child; opt; opt = opt->next_sibling) {
                    if (opt->type == NodeType::NODE_IDENTIFIER) {
                        StringRef val = opt->value();
                        if (val.equals_ci("REPLACE", 7)) {
                            sb_.append(" REPLACE");
                        } else if (val.equals_ci("IGNORE", 6)) {
                            sb_.append(" IGNORE");
                        }
                    }
                }
            }
        }

        // INTO TABLE table_name
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_TABLE_REF) {
                sb_.append(" INTO TABLE ");
                emit_node(child);
                break;
            }
        }

        // FIELDS options
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_LOAD_DATA_OPTIONS) {
                bool fields_started = false;
                for (const AstNode* opt = child->first_child; opt; opt = opt->next_sibling) {
                    if (opt->type == NodeType::NODE_IDENTIFIER) {
                        StringRef val = opt->value();
                        if (val.equals_ci("TERMINATED", 10)) {
                            if (!fields_started) { sb_.append(" FIELDS"); fields_started = true; }
                            sb_.append(" TERMINATED BY ");
                            if (opt->first_child) emit_string_literal(opt->first_child);
                        } else if (val.equals_ci("ENCLOSED", 8)) {
                            if (!fields_started) { sb_.append(" FIELDS"); fields_started = true; }
                            sb_.append(" ENCLOSED BY ");
                            if (opt->first_child) emit_string_literal(opt->first_child);
                        } else if (val.equals_ci("ESCAPED", 7)) {
                            if (!fields_started) { sb_.append(" FIELDS"); fields_started = true; }
                            sb_.append(" ESCAPED BY ");
                            if (opt->first_child) emit_string_literal(opt->first_child);
                        }
                    }
                }
            }
        }

        // Column list
        bool has_cols = false;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_COLUMN_REF) {
                if (!has_cols) {
                    sb_.append(" (");
                    has_cols = true;
                } else {
                    sb_.append(", ");
                }
                emit_node(child);
            }
        }
        if (has_cols) sb_.append_char(')');
    }

    // ---- Compound query ----

    void emit_table_query(const AstNode* node) {
        sb_.append("TABLE ");
        if (node->flags & FLAG_TABLE_ONLY) sb_.append("ONLY ");
        const AstNode* name = node->first_child;
        if (!name) return;
        emit_node(name);
        if (node->flags & FLAG_TABLE_INHERIT) sb_.append(" *");
        for (const AstNode* clause = name->next_sibling; clause; clause = clause->next_sibling) {
            emit_node(clause);
        }
    }

    void emit_compound_query(const AstNode* node) {
        if (node->flags & FLAG_QUERY_PARENTHESIZED) sb_.append_char('(');
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_SET_OPERATION) {
                emit_set_operation(child);
            } else {
                // Trailing ORDER BY or LIMIT
                emit_node(child);
            }
        }
        if (node->flags & FLAG_QUERY_PARENTHESIZED) sb_.append_char(')');
    }

    void emit_set_operation(const AstNode* node) {
        const AstNode* left = node->first_child;
        const AstNode* right = left ? left->next_sibling : nullptr;

        if (left) emit_node(left);

        // Emit the operator: " UNION ", " UNION ALL ", " INTERSECT ", etc.
        sb_.append_char(' ');
        emit_value(node);  // operator keyword text (UNION, INTERSECT, EXCEPT)
        if (node->flags & FLAG_SET_OP_ALL) {
            sb_.append(" ALL");
        }
        sb_.append_char(' ');

        if (right) emit_node(right);
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
        const AstNode* child = node->first_child;
        if (is_not_operator(node) && child) {
            if (child->type == NodeType::NODE_IN_LIST) {
                emit_not_in_list(child);
                return;
            }
            if (child->type == NodeType::NODE_BETWEEN) {
                emit_not_between(child);
                return;
            }
            if (child->type == NodeType::NODE_BINARY_OP &&
                (child->value().equals_ci("LIKE", 4) ||
                 child->value().equals_ci("REGEXP", 6))) {
                emit_not_binary_op(child);
                return;
            }
        }

        emit_value(node);
        // Add space for keyword operators like NOT, no space for - or +
        if (node->value_len > 1 || (D == Dialect::PostgreSQL &&
            ((node->flags & FLAG_PG_OPERATOR) || (child && child->type == NodeType::NODE_UNARY_OP))))
            sb_.append_char(' ');
        if (child) emit_node(child);
    }

    void emit_cte(const AstNode* node) {
        sb_.append("WITH ");
        if (node->flags & FLAG_CTE_RECURSIVE) sb_.append("RECURSIVE ");
        const AstNode* child = node->first_child;
        bool first = true;
        for (; child && child->type == NodeType::NODE_CTE_DEFINITION; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_identifier(child);
            const AstNode* body = child->first_child;
            const AstNode* columns = body ? body->next_sibling : nullptr;
            if (columns && columns->type == NodeType::NODE_CTE_COLUMNS) {
                sb_.append_char('(');
                emit_node(columns);
                sb_.append_char(')');
            }
            sb_.append(" AS ");
            if (child->flags & FLAG_CTE_MATERIALIZED) sb_.append("MATERIALIZED ");
            else if (child->flags & FLAG_CTE_NOT_MATERIALIZED) sb_.append("NOT MATERIALIZED ");
            sb_.append_char('(');
            emit_node(body);
            sb_.append_char(')');
            for (const AstNode* option = body ? body->next_sibling : nullptr;
                 option; option = option->next_sibling) {
                if (option->type == NodeType::NODE_CTE_SEARCH || option->type == NodeType::NODE_CTE_CYCLE) {
                    sb_.append_char(' ');
                    emit_node(option);
                }
            }
        }
        sb_.append_char(' ');
        emit_node(child);
    }

    void emit_function_call(const AstNode* node) {
        const AstNode* arg = node->first_child;
        if (node->flags & FLAG_FUNCTION_TABLE) {
            emit_node(arg);
            arg = arg ? arg->next_sibling : nullptr;
        } else emit_value(node);
        sb_.append_char('(');
        if (node->flags & FLAG_FUNCTION_DISTINCT) sb_.append("DISTINCT ");
        else if (node->flags & FLAG_FUNCTION_ALL) sb_.append("ALL ");
        bool first = true;
        const AstNode* order = nullptr;
        for (; arg; arg = arg->next_sibling) {
            if (arg->type == NodeType::NODE_AGGREGATE_ORDER_BY) {
                order = arg;
                continue;
            }
            if (!first) sb_.append(", ");
            first = false;
            emit_node(arg);
        }
        if (order && !(node->flags & FLAG_FUNCTION_WITHIN_GROUP)) emit_node(order);
        sb_.append_char(')');
        if (order && (node->flags & FLAG_FUNCTION_WITHIN_GROUP)) {
            sb_.append(" WITHIN GROUP (ORDER BY ");
            emit_list(order, ", ");
            sb_.append_char(')');
        }
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
        const AstNode* first_val = expr ? expr->next_sibling : nullptr;
        if (expr) emit_node(expr);
        if (first_val && first_val->type == NodeType::NODE_SUBQUERY &&
            !first_val->next_sibling) {
            sb_.append(" IN ");
            emit_node(first_val);
            return;
        }
        sb_.append(" IN (");
        if (mode_ == EmitMode::DIGEST) {
            sb_.append_char('?');
        } else {
            bool first = true;
            for (const AstNode* val = first_val; val; val = val->next_sibling) {
                if (!first) sb_.append(", ");
                first = false;
                emit_node(val);
            }
        }
        sb_.append_char(')');
    }

    void emit_subquery(const AstNode* node) {
        if (node->flags == 1) {
            sb_.append("EXISTS ");
        }
        sb_.append_char('(');
        if (node->first_child) {
            emit_node(node->first_child);
        } else {
            emit_value(node);
        }
        sb_.append_char(')');
    }

    bool is_not_operator(const AstNode* node) const {
        return node && node->value().equals_ci("NOT", 3);
    }

    void emit_not_in_list(const AstNode* node) {
        const AstNode* expr = node->first_child;
        const AstNode* first_val = expr ? expr->next_sibling : nullptr;
        if (expr) emit_node(expr);
        if (first_val && first_val->type == NodeType::NODE_SUBQUERY &&
            !first_val->next_sibling) {
            sb_.append(" NOT IN ");
            emit_node(first_val);
            return;
        }
        sb_.append(" NOT IN (");
        if (mode_ == EmitMode::DIGEST) {
            sb_.append_char('?');
        } else {
            bool first = true;
            for (const AstNode* val = first_val; val; val = val->next_sibling) {
                if (!first) sb_.append(", ");
                first = false;
                emit_node(val);
            }
        }
        sb_.append_char(')');
    }

    void emit_not_between(const AstNode* node) {
        const AstNode* expr = node->first_child;
        const AstNode* low = expr ? expr->next_sibling : nullptr;
        const AstNode* high = low ? low->next_sibling : nullptr;
        if (expr) emit_node(expr);
        sb_.append(" NOT BETWEEN ");
        if (low) emit_node(low);
        sb_.append(" AND ");
        if (high) emit_node(high);
    }

    void emit_not_binary_op(const AstNode* node) {
        const AstNode* left = node->first_child;
        const AstNode* right = left ? left->next_sibling : nullptr;
        if (left) emit_node(left);
        sb_.append(" NOT ");
        emit_value(node);
        sb_.append_char(' ');
        if (right) emit_node(right);
    }

    void emit_case_when(const AstNode* node) {
        sb_.append("CASE ");
        const AstNode* child = node->first_child;
        if (node->flags && child) {
            emit_node(child);
            sb_.append_char(' ');
            child = child->next_sibling;
        }
        while (child && child->next_sibling) {
            sb_.append("WHEN "); emit_node(child);
            child = child->next_sibling;
            sb_.append(" THEN "); emit_node(child);
            sb_.append_char(' ');
            child = child->next_sibling;
        }
        if (child) {
            sb_.append("ELSE "); emit_node(child); sb_.append_char(' ');
        }
        sb_.append("END");
    }

    void emit_tuple(const AstNode* node) {
        // ROW keyword prefix if present
        if (node->value_len > 0) {
            emit_value(node);
        }
        sb_.append_char('(');
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
        sb_.append_char(')');
    }

    void emit_array_constructor(const AstNode* node) {
        sb_.append("ARRAY[");
        bool first = true;
        for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(child);
        }
        sb_.append_char(']');
    }

    void emit_array_subscript(const AstNode* node) {
        const AstNode* expr = node->first_child;
        const AstNode* index = expr ? expr->next_sibling : nullptr;
        if (expr) emit_node(expr);
        sb_.append_char('[');
        if (index) emit_node(index);
        sb_.append_char(']');
    }

    void emit_field_access(const AstNode* node) {
        const AstNode* expr = node->first_child;
        const AstNode* field = expr ? expr->next_sibling : nullptr;
        if (expr && (expr->type == NodeType::NODE_EXPRESSION ||
            (D == Dialect::PostgreSQL && (expr->type == NodeType::NODE_COLUMN_REF ||
             expr->type == NodeType::NODE_QUALIFIED_NAME || expr->type == NodeType::NODE_ARRAY_SUBSCRIPT ||
             expr->type == NodeType::NODE_PG_ARRAY_SLICE || expr->type == NodeType::NODE_FIELD_ACCESS)))) emit_node(expr);
        else {
            sb_.append_char('(');
            if (expr) emit_node(expr);
            sb_.append_char(')');
        }
        sb_.append_char('.');
        if (field) emit_node(field);
    }

    void emit_star_except(const AstNode* node) {
        const AstNode* star = node->first_child;
        if (star) emit_node(star);
        sb_.append(" EXCEPT(");
        bool first = true;
        for (const AstNode* col = star ? star->next_sibling : node->first_child;
             col; col = col->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(col);
        }
        sb_.append_char(')');
    }

    void emit_star_replace(const AstNode* node) {
        const AstNode* star = node->first_child;
        if (star) emit_node(star);
        sb_.append(" REPLACE(");
        bool first = true;
        for (const AstNode* item = star ? star->next_sibling : node->first_child;
             item; item = item->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(item);
        }
        sb_.append_char(')');
    }

    void emit_replace_item(const AstNode* node) {
        const AstNode* expr = node->first_child;
        const AstNode* col = expr ? expr->next_sibling : nullptr;
        if (expr) emit_node(expr);
        if (col) {
            sb_.append(" AS ");
            emit_node(col);
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_EMITTER_H
