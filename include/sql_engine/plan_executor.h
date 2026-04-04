#ifndef SQL_ENGINE_PLAN_EXECUTOR_H
#define SQL_ENGINE_PLAN_EXECUTOR_H

#include "sql_engine/plan_node.h"
#include "sql_engine/operator.h"
#include "sql_engine/data_source.h"
#include "sql_engine/result_set.h"
#include "sql_engine/catalog.h"
#include "sql_engine/function_registry.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/operators/scan_op.h"
#include "sql_engine/operators/filter_op.h"
#include "sql_engine/operators/project_op.h"
#include "sql_engine/operators/join_op.h"
#include "sql_engine/operators/aggregate_op.h"
#include "sql_engine/operators/sort_op.h"
#include "sql_engine/operators/limit_op.h"
#include "sql_engine/operators/distinct_op.h"
#include "sql_engine/operators/set_op_op.h"

#include <unordered_map>
#include <string>
#include <vector>
#include <memory>

namespace sql_engine {

template <sql_parser::Dialect D>
class PlanExecutor {
public:
    PlanExecutor(FunctionRegistry<D>& functions,
                 const Catalog& catalog,
                 sql_parser::Arena& arena)
        : functions_(functions), catalog_(catalog), arena_(arena) {}

    void add_data_source(const char* table_name, DataSource* source) {
        sources_[table_name] = source;
    }

    ResultSet execute(PlanNode* plan) {
        if (!plan) return {};

        operators_.clear();

        // Pre-process: push aggregate expressions from PROJECT into AGGREGATE
        preprocess_aggregates(plan);

        Operator* root = build_operator(plan);
        if (!root) return {};

        ResultSet rs;
        root->open();
        Row row{};
        while (root->next(row)) {
            rs.rows.push_back(row);
        }
        root->close();

        if (!rs.rows.empty()) {
            rs.column_count = rs.rows[0].column_count;
        }

        // Build column names from plan
        build_column_names(plan, rs);

        return rs;
    }

private:
    FunctionRegistry<D>& functions_;
    const Catalog& catalog_;
    sql_parser::Arena& arena_;
    std::unordered_map<std::string, DataSource*> sources_;
    std::vector<std::unique_ptr<Operator>> operators_;

    // Pre-process: for PROJECT -> AGGREGATE (or PROJECT -> FILTER -> AGGREGATE),
    // extract aggregate function expressions from the PROJECT select list
    // and push them into the AGGREGATE node.
    void preprocess_aggregates(PlanNode* node) {
        if (!node) return;
        preprocess_aggregates(node->left);
        preprocess_aggregates(node->right);

        if (node->type != PlanNodeType::PROJECT) return;

        // Find AGGREGATE child (possibly through a FILTER for HAVING)
        PlanNode* agg_node = node->left;
        if (agg_node && agg_node->type == PlanNodeType::FILTER)
            agg_node = agg_node->left;
        if (!agg_node || agg_node->type != PlanNodeType::AGGREGATE) return;
        if (agg_node->aggregate.agg_count > 0) return; // already populated

        // Scan the PROJECT expressions for function calls that are aggregates
        std::vector<const sql_parser::AstNode*> agg_exprs;
        std::vector<const sql_parser::AstNode*> group_exprs;

        for (uint16_t i = 0; i < node->project.count; ++i) {
            const sql_parser::AstNode* expr = node->project.exprs[i];
            if (is_aggregate_expr(expr)) {
                agg_exprs.push_back(expr);
            } else {
                group_exprs.push_back(expr);
            }
        }

        if (agg_exprs.empty()) return;

        // Store aggregate expressions in the AGGREGATE node
        uint16_t ac = static_cast<uint16_t>(agg_exprs.size());
        auto** arr = static_cast<const sql_parser::AstNode**>(
            arena_.allocate(sizeof(sql_parser::AstNode*) * ac));
        for (uint16_t i = 0; i < ac; ++i) arr[i] = agg_exprs[i];
        agg_node->aggregate.agg_exprs = arr;
        agg_node->aggregate.agg_count = ac;

        // Replace the PROJECT with one that reads from the AGGREGATE output:
        // group_by columns first, then aggregate columns
        // Rewrite the PROJECT expressions to be positional column refs
        // Actually, we'll handle this differently: remove the PROJECT node
        // and let the AGGREGATE produce the final columns directly.
        // We remove the project by replacing node's type.

        // The simplest approach: the AGGREGATE now has group_by + agg_exprs,
        // it produces [group_val_0, ..., group_val_n, agg_val_0, ..., agg_val_m].
        // We keep the PROJECT but reorder its expressions to match:
        // group-by exprs first, then agg exprs.
        // Actually, let's just remove the PROJECT entirely since the AGGREGATE
        // output matches what we need.

        // Overwrite this PROJECT node to become a pass-through:
        // Change it into the node below it.
        PlanNode* child = node->left;
        if (child && child->type == PlanNodeType::FILTER) {
            // PROJECT -> FILTER -> AGGREGATE: keep FILTER, remove PROJECT
            node->type = PlanNodeType::FILTER;
            node->filter.expr = child->filter.expr;
            node->left = child->left;
            node->right = child->right;
        } else {
            // PROJECT -> AGGREGATE: remove PROJECT, become AGGREGATE
            node->type = agg_node->type;
            node->aggregate = agg_node->aggregate;
            node->left = agg_node->left;
            node->right = agg_node->right;
        }
    }

    static bool is_aggregate_expr(const sql_parser::AstNode* expr) {
        if (!expr) return false;
        if (expr->type == sql_parser::NodeType::NODE_FUNCTION_CALL) {
            sql_parser::StringRef name = expr->value();
            if (name.equals_ci("COUNT", 5) || name.equals_ci("SUM", 3) ||
                name.equals_ci("AVG", 3) || name.equals_ci("MIN", 3) ||
                name.equals_ci("MAX", 3)) {
                return true;
            }
        }
        return false;
    }

    // Collect all tables referenced in a plan subtree
    void collect_tables(PlanNode* node, std::vector<const TableInfo*>& tables) {
        if (!node) return;
        if (node->type == PlanNodeType::SCAN) {
            if (node->scan.table) tables.push_back(node->scan.table);
            return;
        }
        collect_tables(node->left, tables);
        collect_tables(node->right, tables);
    }

    uint16_t count_columns(PlanNode* node) {
        if (!node) return 0;
        switch (node->type) {
            case PlanNodeType::SCAN: {
                if (node->scan.table) return node->scan.table->column_count;
                return 0;
            }
            case PlanNodeType::PROJECT:
                return node->project.count;
            case PlanNodeType::AGGREGATE:
                return node->aggregate.group_count + node->aggregate.agg_count;
            case PlanNodeType::JOIN:
                return count_columns(node->left) + count_columns(node->right);
            case PlanNodeType::FILTER:
            case PlanNodeType::SORT:
            case PlanNodeType::LIMIT:
            case PlanNodeType::DISTINCT:
                return count_columns(node->left);
            case PlanNodeType::SET_OP:
                return count_columns(node->left);
        }
        return 0;
    }

    Operator* build_operator(PlanNode* node) {
        if (!node) return nullptr;

        switch (node->type) {
            case PlanNodeType::SCAN:
                return build_scan(node);
            case PlanNodeType::FILTER:
                return build_filter(node);
            case PlanNodeType::PROJECT:
                return build_project(node);
            case PlanNodeType::JOIN:
                return build_join(node);
            case PlanNodeType::AGGREGATE:
                return build_aggregate(node);
            case PlanNodeType::SORT:
                return build_sort(node);
            case PlanNodeType::LIMIT:
                return build_limit(node);
            case PlanNodeType::DISTINCT:
                return build_distinct(node);
            case PlanNodeType::SET_OP:
                return build_set_op(node);
        }
        return nullptr;
    }

    Operator* build_scan(PlanNode* node) {
        const TableInfo* table = node->scan.table;
        if (!table) return nullptr;

        std::string name(table->table_name.ptr, table->table_name.len);
        // Lowercase for lookup
        for (auto& c : name) { if (c >= 'A' && c <= 'Z') c += 32; }

        auto it = sources_.find(name);
        if (it == sources_.end()) return nullptr;

        auto op = std::make_unique<ScanOperator>(it->second);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_filter(PlanNode* node) {
        Operator* child = build_operator(node->left);
        if (!child && node->left) return nullptr;

        std::vector<const TableInfo*> tables;
        collect_tables(node->left, tables);

        auto op = std::make_unique<FilterOperator<D>>(
            child, node->filter.expr, catalog_, tables, functions_, arena_);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_project(PlanNode* node) {
        Operator* child = nullptr;
        if (node->left) {
            child = build_operator(node->left);
            if (!child) return nullptr;
        }

        std::vector<const TableInfo*> tables;
        collect_tables(node->left, tables);

        auto op = std::make_unique<ProjectOperator<D>>(
            child, node->project.exprs, node->project.count,
            catalog_, tables, functions_, arena_);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_join(PlanNode* node) {
        Operator* left = build_operator(node->left);
        Operator* right = build_operator(node->right);
        if (!left || !right) return nullptr;

        std::vector<const TableInfo*> left_tables, right_tables;
        collect_tables(node->left, left_tables);
        collect_tables(node->right, right_tables);

        uint16_t left_cols = count_columns(node->left);
        uint16_t right_cols = count_columns(node->right);

        auto op = std::make_unique<NestedLoopJoinOperator<D>>(
            left, right, node->join.join_type, node->join.condition,
            left_cols, right_cols,
            catalog_, left_tables, right_tables, functions_, arena_);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_aggregate(PlanNode* node) {
        Operator* child = build_operator(node->left);
        if (!child && node->left) return nullptr;

        std::vector<const TableInfo*> tables;
        collect_tables(node->left, tables);

        // If agg_exprs not set by plan builder, try to extract from parent PROJECT
        // For now use what's in the plan node
        auto op = std::make_unique<AggregateOperator<D>>(
            child,
            node->aggregate.group_by, node->aggregate.group_count,
            node->aggregate.agg_exprs, node->aggregate.agg_count,
            catalog_, tables, functions_, arena_);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_sort(PlanNode* node) {
        Operator* child = build_operator(node->left);
        if (!child) return nullptr;

        std::vector<const TableInfo*> tables;
        collect_tables(node->left, tables);

        auto op = std::make_unique<SortOperator<D>>(
            child, node->sort.keys, node->sort.directions, node->sort.count,
            catalog_, tables, functions_, arena_);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_limit(PlanNode* node) {
        Operator* child = build_operator(node->left);
        if (!child) return nullptr;

        auto op = std::make_unique<LimitOperator>(
            child, node->limit.count, node->limit.offset);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_distinct(PlanNode* node) {
        Operator* child = build_operator(node->left);
        if (!child) return nullptr;

        auto op = std::make_unique<DistinctOperator>(child);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    Operator* build_set_op(PlanNode* node) {
        Operator* left = build_operator(node->left);
        Operator* right = build_operator(node->right);
        if (!left || !right) return nullptr;

        auto op = std::make_unique<SetOpOperator>(
            left, right, node->set_op.op, node->set_op.all);
        Operator* ptr = op.get();
        operators_.push_back(std::move(op));
        return ptr;
    }

    void build_column_names(PlanNode* plan, ResultSet& rs) {
        if (!plan) return;

        switch (plan->type) {
            case PlanNodeType::PROJECT: {
                for (uint16_t i = 0; i < plan->project.count; ++i) {
                    if (plan->project.aliases && plan->project.aliases[i]) {
                        sql_parser::StringRef av = plan->project.aliases[i]->value();
                        rs.column_names.emplace_back(av.ptr, av.len);
                    } else if (plan->project.exprs[i]) {
                        // Use expression text as column name
                        sql_parser::StringRef ev = plan->project.exprs[i]->value();
                        if (ev.ptr && ev.len > 0)
                            rs.column_names.emplace_back(ev.ptr, ev.len);
                        else
                            rs.column_names.push_back("?column?");
                    } else {
                        rs.column_names.push_back("?column?");
                    }
                }
                break;
            }
            case PlanNodeType::SCAN: {
                if (plan->scan.table) {
                    for (uint16_t i = 0; i < plan->scan.table->column_count; ++i) {
                        auto& cn = plan->scan.table->columns[i].name;
                        rs.column_names.emplace_back(cn.ptr, cn.len);
                    }
                }
                break;
            }
            default:
                // For wrapping operators, recurse to child
                build_column_names(plan->left, rs);
                break;
        }
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_PLAN_EXECUTOR_H
