#ifndef SQL_ENGINE_DISTRIBUTED_PLANNER_H
#define SQL_ENGINE_DISTRIBUTED_PLANNER_H

#include "sql_engine/plan_node.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/catalog.h"
#include "sql_engine/remote_query_builder.h"
#include "sql_engine/operators/merge_aggregate_op.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/arena.h"
#include "sql_parser/ast.h"
#include "sql_parser/common.h"
#include <cstring>
#include <cstdio>
#include <vector>
#include <unordered_map>
#include <functional>

namespace sql_engine {

template <sql_parser::Dialect D>
class DistributedPlanner {
public:
    DistributedPlanner(const ShardMap& shards, const Catalog& catalog, sql_parser::Arena& arena)
        : shards_(shards), catalog_(catalog), arena_(arena), qb_(arena) {}

    // Rewrite a logical plan for distributed execution.
    // Returns a new plan tree with RemoteScan/MergeAggregate/MergeSort nodes.
    PlanNode* distribute(PlanNode* plan) {
        if (!plan) return nullptr;
        return distribute_node(plan);
    }

    // Distribute a DML plan node for remote execution.
    // Returns a new plan tree with REMOTE_SCAN nodes (for DML, the remote
    // scan carries the DML SQL; the executor calls execute_dml on it).
    PlanNode* distribute_dml(PlanNode* plan) {
        if (!plan) return nullptr;

        switch (plan->type) {
            case PlanNodeType::INSERT_PLAN:
                return distribute_insert(plan);
            case PlanNodeType::UPDATE_PLAN:
                return distribute_update(plan);
            case PlanNodeType::DELETE_PLAN:
                return distribute_delete(plan);
            default:
                return plan;
        }
    }

private:
    const ShardMap& shards_;
    const Catalog& catalog_;
    sql_parser::Arena& arena_;
    RemoteQueryBuilder<D> qb_;

    // Push aggregate expressions from PROJECT into AGGREGATE node
    // (same logic as PlanExecutor::preprocess_aggregates)
    void push_agg_exprs_from_project(PlanNode* project_node, PlanNode* agg_node) {
        if (!project_node || !agg_node) return;
        if (agg_node->aggregate.agg_count > 0) return; // already populated

        std::vector<const sql_parser::AstNode*> agg_exprs;
        for (uint16_t i = 0; i < project_node->project.count; ++i) {
            const sql_parser::AstNode* expr = project_node->project.exprs[i];
            if (is_aggregate_expr(expr)) {
                agg_exprs.push_back(expr);
            }
        }
        if (agg_exprs.empty()) return;

        uint16_t ac = static_cast<uint16_t>(agg_exprs.size());
        auto** arr = static_cast<const sql_parser::AstNode**>(
            arena_.allocate(sizeof(sql_parser::AstNode*) * ac));
        for (uint16_t i = 0; i < ac; ++i) arr[i] = agg_exprs[i];
        agg_node->aggregate.agg_exprs = arr;
        agg_node->aggregate.agg_count = ac;
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

    // Main dispatcher: walk the plan tree and distribute.
    PlanNode* distribute_node(PlanNode* node) {
        if (!node) return nullptr;

        switch (node->type) {
            case PlanNodeType::SCAN:
                return distribute_scan(node, nullptr, nullptr, nullptr, nullptr, false);

            case PlanNodeType::FILTER: {
                // Check if child is a SCAN -- push filter to remote
                if (node->left && node->left->type == PlanNodeType::SCAN) {
                    return distribute_scan(node->left, node->filter.expr,
                                           nullptr, nullptr, nullptr, false);
                }
                // Otherwise, distribute child and wrap
                PlanNode* child = distribute_node(node->left);
                PlanNode* result = make_plan_node(arena_, PlanNodeType::FILTER);
                result->filter.expr = node->filter.expr;
                result->left = child;
                return result;
            }

            case PlanNodeType::PROJECT: {
                // Check for PROJECT -> AGGREGATE pattern (or PROJECT -> FILTER -> AGGREGATE)
                // For aggregate queries, we want to handle the whole thing in distribute_aggregate
                PlanNode* agg_child = node->left;
                if (agg_child && agg_child->type == PlanNodeType::FILTER)
                    agg_child = agg_child->left;
                if (agg_child && agg_child->type == PlanNodeType::AGGREGATE) {
                    // Extract aggregate info from the PROJECT select list
                    push_agg_exprs_from_project(node, agg_child);
                    PlanNode* dist_agg = distribute_aggregate(agg_child);
                    // If it was distributed (MERGE_AGGREGATE), no need for PROJECT wrapper
                    // because the merge already produces the right columns
                    if (dist_agg && dist_agg->type == PlanNodeType::MERGE_AGGREGATE) {
                        // Re-add FILTER (HAVING) if present
                        if (node->left && node->left->type == PlanNodeType::FILTER) {
                            PlanNode* having = make_plan_node(arena_, PlanNodeType::FILTER);
                            having->filter.expr = node->left->filter.expr;
                            having->left = dist_agg;
                            return having;
                        }
                        return dist_agg;
                    }
                    // For unsharded, the remote already computes everything
                    return dist_agg;
                }

                // Normal PROJECT: distribute child, wrap with PROJECT
                PlanNode* child = distribute_node(node->left);
                PlanNode* result = make_plan_node(arena_, PlanNodeType::PROJECT);
                result->project = node->project;
                result->left = child;
                return result;
            }

            case PlanNodeType::AGGREGATE:
                return distribute_aggregate(node);

            case PlanNodeType::SORT:
                return distribute_sort(node);

            case PlanNodeType::LIMIT:
                return distribute_limit(node);

            case PlanNodeType::DISTINCT:
                return distribute_distinct(node);

            case PlanNodeType::JOIN:
                return distribute_join(node);

            case PlanNodeType::SET_OP: {
                PlanNode* result = make_plan_node(arena_, PlanNodeType::SET_OP);
                result->set_op = node->set_op;
                result->left = distribute_node(node->left);
                result->right = distribute_node(node->right);
                return result;
            }

            default:
                return node;
        }
    }

    // Find the table referenced by a subtree (first SCAN's table).
    const TableInfo* find_table(PlanNode* node) {
        if (!node) return nullptr;
        if (node->type == PlanNodeType::SCAN) return node->scan.table;
        const TableInfo* t = find_table(node->left);
        if (t) return t;
        return find_table(node->right);
    }

    // Find a SCAN node in the subtree
    PlanNode* find_scan(PlanNode* node) {
        if (!node) return nullptr;
        if (node->type == PlanNodeType::SCAN) return node;
        PlanNode* s = find_scan(node->left);
        if (s) return s;
        return find_scan(node->right);
    }

    // Extract the WHERE expression from a Filter above a Scan
    const sql_parser::AstNode* extract_filter_above_scan(PlanNode* node, PlanNode*& scan_out) {
        if (!node) return nullptr;
        if (node->type == PlanNodeType::SCAN) {
            scan_out = node;
            return nullptr;
        }
        if (node->type == PlanNodeType::FILTER && node->left &&
            node->left->type == PlanNodeType::SCAN) {
            scan_out = node->left;
            return node->filter.expr;
        }
        // Filter -> Filter -> Scan etc -- just find the deepest scan
        scan_out = find_scan(node);
        if (node->type == PlanNodeType::FILTER) return node->filter.expr;
        return nullptr;
    }

    // Walk upward from scan to collect filter, looking through the subtree.
    // A more careful version that works for aggregate/sort/limit cases.
    struct ScanContext {
        PlanNode* scan = nullptr;
        const sql_parser::AstNode* where_expr = nullptr;
    };

    ScanContext extract_scan_context(PlanNode* node) {
        ScanContext ctx;
        if (!node) return ctx;
        if (node->type == PlanNodeType::SCAN) {
            ctx.scan = node;
            return ctx;
        }
        if (node->type == PlanNodeType::FILTER) {
            ctx = extract_scan_context(node->left);
            ctx.where_expr = node->filter.expr;
            return ctx;
        }
        ctx = extract_scan_context(node->left);
        return ctx;
    }

    // Case 1 & 2: Distribute a scan (possibly with filter pushed down)
    PlanNode* distribute_scan(PlanNode* scan_node,
                              const sql_parser::AstNode* where_expr,
                              const sql_parser::AstNode** order_keys,
                              uint8_t* order_dirs,
                              uint16_t* order_count_ptr,
                              bool /* unused */)
    {
        const TableInfo* table = scan_node->scan.table;
        if (!table) return scan_node;

        if (!shards_.has_table(table->table_name)) return scan_node;

        if (!shards_.is_sharded(table->table_name)) {
            // Case 1: Unsharded -- single RemoteScan
            int64_t limit = -1; // no limit pushed here
            uint16_t oc = order_count_ptr ? *order_count_ptr : 0;
            sql_parser::StringRef sql = qb_.build_select(
                table, where_expr, nullptr, 0, nullptr, 0,
                order_keys, order_dirs, oc, limit, false);

            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        // Case 2: Sharded -- N RemoteScans + UNION ALL
        const auto& shard_list = shards_.get_shards(table->table_name);
        return make_sharded_union(table, where_expr, nullptr, 0, nullptr, 0,
                                  nullptr, nullptr, 0, -1, false, shard_list);
    }

    // Build N RemoteScans with UNION ALL
    PlanNode* make_sharded_union(const TableInfo* table,
                                  const sql_parser::AstNode* where_expr,
                                  const sql_parser::AstNode** project_exprs,
                                  uint16_t project_count,
                                  const sql_parser::AstNode** group_by,
                                  uint16_t group_count,
                                  const sql_parser::AstNode** order_keys,
                                  uint8_t* order_dirs,
                                  uint16_t order_count,
                                  int64_t limit,
                                  bool distinct,
                                  const std::vector<ShardInfo>& shard_list)
    {
        if (shard_list.empty()) return nullptr;
        if (shard_list.size() == 1) {
            sql_parser::StringRef sql = qb_.build_select(
                table, where_expr, project_exprs, project_count,
                group_by, group_count, order_keys, order_dirs,
                order_count, limit, distinct);
            return make_remote_scan(shard_list[0].backend_name.c_str(), sql, table);
        }

        // Build a left-deep chain of UNION ALL nodes
        PlanNode* first_scan = nullptr;
        {
            sql_parser::StringRef sql = qb_.build_select(
                table, where_expr, project_exprs, project_count,
                group_by, group_count, order_keys, order_dirs,
                order_count, limit, distinct);
            first_scan = make_remote_scan(shard_list[0].backend_name.c_str(), sql, table);
        }

        PlanNode* current = first_scan;
        for (size_t i = 1; i < shard_list.size(); ++i) {
            sql_parser::StringRef sql = qb_.build_select(
                table, where_expr, project_exprs, project_count,
                group_by, group_count, order_keys, order_dirs,
                order_count, limit, distinct);
            PlanNode* rs = make_remote_scan(shard_list[i].backend_name.c_str(), sql, table);

            PlanNode* union_node = make_plan_node(arena_, PlanNodeType::SET_OP);
            union_node->set_op.op = SET_OP_UNION;
            union_node->set_op.all = true;
            union_node->left = current;
            union_node->right = rs;
            current = union_node;
        }

        return current;
    }

    PlanNode* make_remote_scan(const char* backend, sql_parser::StringRef sql,
                                const TableInfo* table) {
        PlanNode* node = make_plan_node(arena_, PlanNodeType::REMOTE_SCAN);
        // Copy backend name to arena
        uint32_t blen = static_cast<uint32_t>(std::strlen(backend));
        char* bn = static_cast<char*>(arena_.allocate(blen + 1));
        std::memcpy(bn, backend, blen + 1);
        node->remote_scan.backend_name = bn;
        node->remote_scan.remote_sql = sql.ptr;
        node->remote_scan.remote_sql_len = static_cast<uint16_t>(sql.len);
        node->remote_scan.table = table;
        return node;
    }

    // Case 3: Distributed aggregation
    PlanNode* distribute_aggregate(PlanNode* agg_node) {
        ScanContext ctx = extract_scan_context(agg_node->left);
        if (!ctx.scan || !ctx.scan->scan.table) {
            // Can't distribute -- just recurse
            PlanNode* result = make_plan_node(arena_, PlanNodeType::AGGREGATE);
            result->aggregate = agg_node->aggregate;
            result->left = distribute_node(agg_node->left);
            return result;
        }

        const TableInfo* table = ctx.scan->scan.table;
        if (!shards_.has_table(table->table_name) || !shards_.is_sharded(table->table_name)) {
            // Unsharded -- push the whole thing to remote
            return make_unsharded_aggregate(agg_node, ctx, table);
        }

        // Sharded aggregate: each shard computes partial aggregates.
        // Build remote project expressions: group-by cols + partial agg expressions
        const auto& shard_list = shards_.get_shards(table->table_name);

        // Build projection list for remote: group_by keys + decomposed aggs
        std::vector<const sql_parser::AstNode*> remote_projs;
        std::vector<const sql_parser::AstNode*> remote_group_by;
        std::vector<uint8_t> merge_ops;

        // Group-by expressions
        for (uint16_t i = 0; i < agg_node->aggregate.group_count; ++i) {
            remote_group_by.push_back(agg_node->aggregate.group_by[i]);
            remote_projs.push_back(agg_node->aggregate.group_by[i]);
        }

        // Aggregate expressions -- decompose for distributed execution
        for (uint16_t i = 0; i < agg_node->aggregate.agg_count; ++i) {
            const sql_parser::AstNode* expr = agg_node->aggregate.agg_exprs[i];
            decompose_aggregate(expr, remote_projs, merge_ops);
        }

        // Build remote SQL for each shard with GROUP BY
        const sql_parser::AstNode** proj_arr = nullptr;
        uint16_t proj_count = static_cast<uint16_t>(remote_projs.size());
        if (proj_count > 0) {
            proj_arr = static_cast<const sql_parser::AstNode**>(
                arena_.allocate(sizeof(sql_parser::AstNode*) * proj_count));
            for (uint16_t i = 0; i < proj_count; ++i) proj_arr[i] = remote_projs[i];
        }

        const sql_parser::AstNode** gb_arr = nullptr;
        uint16_t gb_count = static_cast<uint16_t>(remote_group_by.size());
        if (gb_count > 0) {
            gb_arr = static_cast<const sql_parser::AstNode**>(
                arena_.allocate(sizeof(sql_parser::AstNode*) * gb_count));
            for (uint16_t i = 0; i < gb_count; ++i) gb_arr[i] = remote_group_by[i];
        }

        // Create N RemoteScan children
        std::vector<PlanNode*> children;
        for (const auto& shard : shard_list) {
            sql_parser::StringRef sql = qb_.build_select(
                table, ctx.where_expr, proj_arr, proj_count,
                gb_arr, gb_count, nullptr, nullptr, 0, -1, false);
            children.push_back(make_remote_scan(shard.backend_name.c_str(), sql, table));
        }

        // Build MergeAggregate node
        PlanNode* merge = make_plan_node(arena_, PlanNodeType::MERGE_AGGREGATE);
        merge->merge_aggregate.child_count = static_cast<uint16_t>(children.size());
        merge->merge_aggregate.children = static_cast<PlanNode**>(
            arena_.allocate(sizeof(PlanNode*) * children.size()));
        for (size_t i = 0; i < children.size(); ++i) {
            merge->merge_aggregate.children[i] = children[i];
        }
        merge->merge_aggregate.group_key_count = agg_node->aggregate.group_count;
        merge->merge_aggregate.merge_op_count = static_cast<uint16_t>(merge_ops.size());
        merge->merge_aggregate.merge_ops = static_cast<uint8_t*>(
            arena_.allocate(merge_ops.size()));
        std::memcpy(merge->merge_aggregate.merge_ops, merge_ops.data(), merge_ops.size());

        // Set left to first child for compatibility with tree walkers
        if (!children.empty()) merge->left = children[0];

        return merge;
    }

    PlanNode* make_unsharded_aggregate(PlanNode* agg_node, const ScanContext& ctx,
                                        const TableInfo* table) {
        // Build remote SQL that includes the aggregation
        std::vector<const sql_parser::AstNode*> projs;
        for (uint16_t i = 0; i < agg_node->aggregate.group_count; ++i) {
            projs.push_back(agg_node->aggregate.group_by[i]);
        }
        for (uint16_t i = 0; i < agg_node->aggregate.agg_count; ++i) {
            projs.push_back(agg_node->aggregate.agg_exprs[i]);
        }

        std::vector<const sql_parser::AstNode*> gb;
        for (uint16_t i = 0; i < agg_node->aggregate.group_count; ++i) {
            gb.push_back(agg_node->aggregate.group_by[i]);
        }

        const char* backend = shards_.get_backend(table->table_name);
        sql_parser::StringRef sql = qb_.build_select(
            table, ctx.where_expr,
            projs.data(), static_cast<uint16_t>(projs.size()),
            gb.data(), static_cast<uint16_t>(gb.size()),
            nullptr, nullptr, 0, -1, false);
        return make_remote_scan(backend, sql, table);
    }

    void decompose_aggregate(const sql_parser::AstNode* expr,
                              std::vector<const sql_parser::AstNode*>& projs,
                              std::vector<uint8_t>& merge_ops) {
        if (!expr || expr->type != sql_parser::NodeType::NODE_FUNCTION_CALL) {
            projs.push_back(expr);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::SUM_OF_SUMS));
            return;
        }

        sql_parser::StringRef name = expr->value();

        if (name.equals_ci("COUNT", 5)) {
            // Remote: COUNT(*) or COUNT(col), Local: SUM of counts
            projs.push_back(expr);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::SUM_OF_COUNTS));
        } else if (name.equals_ci("SUM", 3)) {
            // Remote: SUM(col), Local: SUM of sums
            projs.push_back(expr);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::SUM_OF_SUMS));
        } else if (name.equals_ci("AVG", 3)) {
            // AVG decomposition: remote sends SUM(col) + COUNT(col)
            // Build SUM(col) node
            const sql_parser::AstNode* arg = expr->first_child;
            sql_parser::AstNode* sum_node = make_func_call("SUM", arg);
            sql_parser::AstNode* count_node = make_func_call("COUNT", arg);

            projs.push_back(sum_node);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::AVG_SUM));
            projs.push_back(count_node);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::AVG_COUNT));
        } else if (name.equals_ci("MIN", 3)) {
            projs.push_back(expr);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::MIN_OF_MINS));
        } else if (name.equals_ci("MAX", 3)) {
            projs.push_back(expr);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::MAX_OF_MAXES));
        } else {
            projs.push_back(expr);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::SUM_OF_SUMS));
        }
    }

    sql_parser::AstNode* make_func_call(const char* func_name,
                                          const sql_parser::AstNode* arg) {
        uint32_t nlen = static_cast<uint32_t>(std::strlen(func_name));
        char* name_buf = static_cast<char*>(arena_.allocate(nlen));
        std::memcpy(name_buf, func_name, nlen);

        sql_parser::AstNode* node = sql_parser::make_node(
            arena_, sql_parser::NodeType::NODE_FUNCTION_CALL,
            sql_parser::StringRef{name_buf, nlen});

        // Copy argument as child
        if (arg) {
            // Clone the argument subtree (shallow -- just link it)
            sql_parser::AstNode* arg_copy = sql_parser::make_node(
                arena_, arg->type, arg->value(), arg->flags);
            arg_copy->first_child = arg->first_child;
            node->add_child(arg_copy);
        }
        return node;
    }

    // Case 4: Distributed sort + limit
    PlanNode* distribute_sort(PlanNode* sort_node) {
        // Check if the child is a scan (possibly through filter) on a sharded table
        ScanContext ctx = extract_scan_context(sort_node->left);
        if (!ctx.scan || !ctx.scan->scan.table) {
            PlanNode* result = make_plan_node(arena_, PlanNodeType::SORT);
            result->sort = sort_node->sort;
            result->left = distribute_node(sort_node->left);
            return result;
        }

        const TableInfo* table = ctx.scan->scan.table;
        if (!shards_.has_table(table->table_name)) {
            PlanNode* result = make_plan_node(arena_, PlanNodeType::SORT);
            result->sort = sort_node->sort;
            result->left = distribute_node(sort_node->left);
            return result;
        }

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded -- push sort to remote
            sql_parser::StringRef sql = qb_.build_select(
                table, ctx.where_expr, nullptr, 0, nullptr, 0,
                sort_node->sort.keys, sort_node->sort.directions,
                sort_node->sort.count, -1, false);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        // Sharded sort: each shard sorts, then MergeSort locally
        return make_sharded_merge_sort(table, ctx.where_expr,
                                        sort_node->sort.keys,
                                        sort_node->sort.directions,
                                        sort_node->sort.count,
                                        -1);
    }

    PlanNode* make_sharded_merge_sort(const TableInfo* table,
                                       const sql_parser::AstNode* where_expr,
                                       const sql_parser::AstNode** sort_keys,
                                       uint8_t* sort_dirs,
                                       uint16_t sort_count,
                                       int64_t limit) {
        const auto& shard_list = shards_.get_shards(table->table_name);

        // Build N RemoteScans with ORDER BY [+ LIMIT]
        PlanNode** children = static_cast<PlanNode**>(
            arena_.allocate(sizeof(PlanNode*) * shard_list.size()));

        for (size_t i = 0; i < shard_list.size(); ++i) {
            sql_parser::StringRef sql = qb_.build_select(
                table, where_expr, nullptr, 0, nullptr, 0,
                sort_keys, sort_dirs, sort_count, limit, false);
            children[i] = make_remote_scan(shard_list[i].backend_name.c_str(), sql, table);
        }

        PlanNode* merge = make_plan_node(arena_, PlanNodeType::MERGE_SORT);
        merge->merge_sort.keys = sort_keys;
        merge->merge_sort.directions = sort_dirs;
        merge->merge_sort.key_count = sort_count;
        merge->merge_sort.children = children;
        merge->merge_sort.child_count = static_cast<uint16_t>(shard_list.size());
        merge->left = children[0];

        return merge;
    }

    // Distribute LIMIT node
    PlanNode* distribute_limit(PlanNode* limit_node) {
        // Check if child is Sort on sharded table
        if (limit_node->left && limit_node->left->type == PlanNodeType::SORT) {
            PlanNode* sort_node = limit_node->left;
            ScanContext ctx = extract_scan_context(sort_node->left);
            if (ctx.scan && ctx.scan->scan.table) {
                const TableInfo* table = ctx.scan->scan.table;
                if (shards_.has_table(table->table_name) &&
                    shards_.is_sharded(table->table_name)) {
                    // Case 4: Sharded sort + limit
                    // Each shard: ORDER BY + LIMIT, MergeSort, then outer Limit
                    int64_t remote_limit = limit_node->limit.count + limit_node->limit.offset;

                    PlanNode* merge = make_sharded_merge_sort(
                        table, ctx.where_expr,
                        sort_node->sort.keys, sort_node->sort.directions,
                        sort_node->sort.count, remote_limit);

                    PlanNode* local_limit = make_plan_node(arena_, PlanNodeType::LIMIT);
                    local_limit->limit.count = limit_node->limit.count;
                    local_limit->limit.offset = limit_node->limit.offset;
                    local_limit->left = merge;
                    return local_limit;
                }

                if (shards_.has_table(table->table_name) &&
                    !shards_.is_sharded(table->table_name)) {
                    // Unsharded: push sort+limit to remote
                    sql_parser::StringRef sql = qb_.build_select(
                        table, ctx.where_expr, nullptr, 0, nullptr, 0,
                        sort_node->sort.keys, sort_node->sort.directions,
                        sort_node->sort.count,
                        limit_node->limit.count + limit_node->limit.offset, false);
                    PlanNode* rs = make_remote_scan(
                        shards_.get_backend(table->table_name), sql, table);

                    if (limit_node->limit.offset > 0) {
                        PlanNode* local_limit = make_plan_node(arena_, PlanNodeType::LIMIT);
                        local_limit->limit.count = limit_node->limit.count;
                        local_limit->limit.offset = limit_node->limit.offset;
                        local_limit->left = rs;
                        return local_limit;
                    }
                    return rs;
                }
            }
        }

        // Check if child is scan on sharded/unsharded table (limit without sort)
        ScanContext ctx = extract_scan_context(limit_node->left);
        if (ctx.scan && ctx.scan->scan.table) {
            const TableInfo* table = ctx.scan->scan.table;
            if (shards_.has_table(table->table_name) &&
                !shards_.is_sharded(table->table_name)) {
                sql_parser::StringRef sql = qb_.build_select(
                    table, ctx.where_expr, nullptr, 0, nullptr, 0,
                    nullptr, nullptr, 0, limit_node->limit.count, false);
                return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
            }
        }

        // Default: distribute child and wrap with limit
        PlanNode* result = make_plan_node(arena_, PlanNodeType::LIMIT);
        result->limit = limit_node->limit;
        result->left = distribute_node(limit_node->left);
        return result;
    }

    // Case 5: Cross-backend join
    PlanNode* distribute_join(PlanNode* join_node) {
        // Get tables from each side
        const TableInfo* left_table = find_table(join_node->left);
        const TableInfo* right_table = find_table(join_node->right);

        PlanNode* left_dist = nullptr;
        PlanNode* right_dist = nullptr;

        // Distribute each side independently
        if (left_table && shards_.has_table(left_table->table_name)) {
            ScanContext lctx = extract_scan_context(join_node->left);
            if (lctx.scan && !shards_.is_sharded(left_table->table_name)) {
                sql_parser::StringRef sql = qb_.build_select(
                    left_table, lctx.where_expr, nullptr, 0, nullptr, 0,
                    nullptr, nullptr, 0, -1, false);
                left_dist = make_remote_scan(
                    shards_.get_backend(left_table->table_name), sql, left_table);
            } else if (lctx.scan && shards_.is_sharded(left_table->table_name)) {
                left_dist = distribute_scan(lctx.scan, lctx.where_expr,
                                             nullptr, nullptr, nullptr, false);
            } else {
                left_dist = distribute_node(join_node->left);
            }
        } else {
            left_dist = distribute_node(join_node->left);
        }

        if (right_table && shards_.has_table(right_table->table_name)) {
            ScanContext rctx = extract_scan_context(join_node->right);
            if (rctx.scan && !shards_.is_sharded(right_table->table_name)) {
                sql_parser::StringRef sql = qb_.build_select(
                    right_table, rctx.where_expr, nullptr, 0, nullptr, 0,
                    nullptr, nullptr, 0, -1, false);
                right_dist = make_remote_scan(
                    shards_.get_backend(right_table->table_name), sql, right_table);
            } else if (rctx.scan && shards_.is_sharded(right_table->table_name)) {
                right_dist = distribute_scan(rctx.scan, rctx.where_expr,
                                              nullptr, nullptr, nullptr, false);
            } else {
                right_dist = distribute_node(join_node->right);
            }
        } else {
            right_dist = distribute_node(join_node->right);
        }

        // Local join
        PlanNode* result = make_plan_node(arena_, PlanNodeType::JOIN);
        result->join = join_node->join;
        result->left = left_dist;
        result->right = right_dist;
        return result;
    }

    // ---- DML distribution ----

    PlanNode* distribute_insert(PlanNode* plan) {
        const auto& ip = plan->insert_plan;
        const TableInfo* table = ip.table;
        if (!table || !shards_.has_table(table->table_name)) return plan;

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded: single remote INSERT
            sql_parser::StringRef sql = qb_.build_insert(
                table, ip.columns, ip.column_count, ip.value_rows, ip.row_count);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        // Sharded: group rows by shard key value
        sql_parser::StringRef shard_key = shards_.get_shard_key(table->table_name);
        if (!shard_key.ptr) return plan;

        // Find shard key column ordinal in the column list
        int shard_col_idx = -1;
        if (ip.columns && ip.column_count > 0) {
            for (uint16_t i = 0; i < ip.column_count; ++i) {
                if (ip.columns[i] && ip.columns[i]->value().equals_ci(shard_key.ptr, shard_key.len)) {
                    shard_col_idx = static_cast<int>(i);
                    break;
                }
            }
        } else if (table) {
            // No explicit column list -- match by table column order
            for (uint16_t i = 0; i < table->column_count; ++i) {
                if (table->columns[i].name.equals_ci(shard_key.ptr, shard_key.len)) {
                    shard_col_idx = static_cast<int>(i);
                    break;
                }
            }
        }

        if (shard_col_idx < 0) {
            // Can't determine shard -- send to all (scatter)
            // For INSERT, this is an error in practice. Fall back to first shard.
            sql_parser::StringRef sql = qb_.build_insert(
                table, ip.columns, ip.column_count, ip.value_rows, ip.row_count);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        const auto& shard_list = shards_.get_shards(table->table_name);

        // Group rows by shard: evaluate the shard key value in each row,
        // hash to determine target shard
        // Map: shard_index -> list of row indices
        std::unordered_map<size_t, std::vector<uint16_t>> shard_rows;
        auto null_resolve = [](sql_parser::StringRef) -> Value { return value_null(); };

        for (uint16_t ri = 0; ri < ip.row_count; ++ri) {
            const sql_parser::AstNode* row_ast = ip.value_rows[ri];
            if (!row_ast) continue;

            // Get the shard key value expression (nth child of the row)
            const sql_parser::AstNode* expr = row_ast->first_child;
            for (int j = 0; j < shard_col_idx && expr; ++j) {
                expr = expr->next_sibling;
            }

            // Evaluate to get the value, then hash to determine shard
            size_t shard_idx = 0;
            if (expr) {
                // Simple hashing: convert to int64 and mod by shard count
                Value v = evaluate_shard_key_value(expr);
                if (v.tag == Value::TAG_INT64) {
                    shard_idx = static_cast<size_t>(
                        std::abs(v.int_val) % static_cast<int64_t>(shard_list.size()));
                } else if (v.tag == Value::TAG_STRING && v.str_val.ptr) {
                    // Simple string hash
                    uint64_t h = 0;
                    for (uint32_t k = 0; k < v.str_val.len; ++k) {
                        h = h * 31 + static_cast<uint8_t>(v.str_val.ptr[k]);
                    }
                    shard_idx = static_cast<size_t>(h % shard_list.size());
                }
            }
            shard_rows[shard_idx].push_back(ri);
        }

        // Generate per-shard INSERT SQL
        if (shard_rows.size() == 1) {
            auto it = shard_rows.begin();
            // If all rows go to one shard, send the original INSERT
            if (it->second.size() == ip.row_count) {
                sql_parser::StringRef sql = qb_.build_insert(
                    table, ip.columns, ip.column_count, ip.value_rows, ip.row_count);
                return make_remote_scan(shard_list[it->first].backend_name.c_str(), sql, table);
            }
        }

        // Build per-shard INSERT nodes, combine with UNION ALL (for plan structure)
        PlanNode* current = nullptr;
        for (auto& [shard_idx, row_indices] : shard_rows) {
            // Build a subset value_rows array
            uint16_t sub_count = static_cast<uint16_t>(row_indices.size());
            auto** sub_rows = static_cast<const sql_parser::AstNode**>(
                arena_.allocate(sizeof(sql_parser::AstNode*) * sub_count));
            for (uint16_t i = 0; i < sub_count; ++i) {
                sub_rows[i] = ip.value_rows[row_indices[i]];
            }

            sql_parser::StringRef sql = qb_.build_insert(
                table, ip.columns, ip.column_count, sub_rows, sub_count);
            PlanNode* rs = make_remote_scan(shard_list[shard_idx].backend_name.c_str(), sql, table);

            if (!current) {
                current = rs;
            } else {
                PlanNode* union_node = make_plan_node(arena_, PlanNodeType::SET_OP);
                union_node->set_op.op = SET_OP_UNION;
                union_node->set_op.all = true;
                union_node->left = current;
                union_node->right = rs;
                current = union_node;
            }
        }

        return current ? current : plan;
    }

    PlanNode* distribute_update(PlanNode* plan) {
        const auto& up = plan->update_plan;
        const TableInfo* table = up.table;
        if (!table || !shards_.has_table(table->table_name)) return plan;

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded: single remote UPDATE
            sql_parser::StringRef sql = qb_.build_update(
                table, up.set_columns, up.set_exprs, up.set_count, up.where_expr);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        // Sharded: check if WHERE references the shard key
        sql_parser::StringRef shard_key = shards_.get_shard_key(table->table_name);
        const auto& shard_list = shards_.get_shards(table->table_name);

        int target_shard = find_shard_from_where(up.where_expr, shard_key, shard_list.size());

        if (target_shard >= 0) {
            // Route to specific shard
            sql_parser::StringRef sql = qb_.build_update(
                table, up.set_columns, up.set_exprs, up.set_count, up.where_expr);
            return make_remote_scan(shard_list[target_shard].backend_name.c_str(), sql, table);
        }

        // Scatter to all shards
        return scatter_dml_to_shards(table, shard_list, [&]() {
            return qb_.build_update(
                table, up.set_columns, up.set_exprs, up.set_count, up.where_expr);
        });
    }

    PlanNode* distribute_delete(PlanNode* plan) {
        const auto& dp = plan->delete_plan;
        const TableInfo* table = dp.table;
        if (!table || !shards_.has_table(table->table_name)) return plan;

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded: single remote DELETE
            sql_parser::StringRef sql = qb_.build_delete(table, dp.where_expr);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        // Sharded: check if WHERE references the shard key
        sql_parser::StringRef shard_key = shards_.get_shard_key(table->table_name);
        const auto& shard_list = shards_.get_shards(table->table_name);

        int target_shard = find_shard_from_where(dp.where_expr, shard_key, shard_list.size());

        if (target_shard >= 0) {
            // Route to specific shard
            sql_parser::StringRef sql = qb_.build_delete(table, dp.where_expr);
            return make_remote_scan(shard_list[target_shard].backend_name.c_str(), sql, table);
        }

        // Scatter to all shards
        return scatter_dml_to_shards(table, shard_list, [&]() {
            return qb_.build_delete(table, dp.where_expr);
        });
    }

    // Evaluate a shard key expression from a VALUES row (simple: literal values only)
    Value evaluate_shard_key_value(const sql_parser::AstNode* expr) {
        if (!expr) return value_null();
        if (expr->type == sql_parser::NodeType::NODE_LITERAL_INT) {
            sql_parser::StringRef val = expr->value();
            int64_t n = 0;
            for (uint32_t i = 0; i < val.len; ++i) {
                char c = val.ptr[i];
                if (c >= '0' && c <= '9') n = n * 10 + (c - '0');
            }
            return value_int(n);
        }
        if (expr->type == sql_parser::NodeType::NODE_LITERAL_STRING) {
            return value_string(expr->value());
        }
        return value_null();
    }

    // Check if a WHERE expression contains shard_key = <literal>.
    // Returns the target shard index, or -1 if not determinable.
    int find_shard_from_where(const sql_parser::AstNode* where_expr,
                               sql_parser::StringRef shard_key,
                               size_t shard_count) {
        if (!where_expr || !shard_key.ptr || shard_count == 0) return -1;

        // Look for binary_op '=' with one side being the shard key column
        if (where_expr->type == sql_parser::NodeType::NODE_BINARY_OP) {
            sql_parser::StringRef op = where_expr->value();
            if (op.len == 1 && op.ptr[0] == '=') {
                const sql_parser::AstNode* left = where_expr->first_child;
                const sql_parser::AstNode* right = left ? left->next_sibling : nullptr;
                if (!left || !right) return -1;

                // Check if left is the shard key column and right is a literal (or vice versa)
                const sql_parser::AstNode* col_node = nullptr;
                const sql_parser::AstNode* val_node = nullptr;

                if (is_column_ref(left, shard_key)) {
                    col_node = left;
                    val_node = right;
                } else if (is_column_ref(right, shard_key)) {
                    col_node = right;
                    val_node = left;
                }

                if (col_node && val_node) {
                    Value v = evaluate_shard_key_value(val_node);
                    if (v.tag == Value::TAG_INT64) {
                        return static_cast<int>(
                            std::abs(v.int_val) % static_cast<int64_t>(shard_count));
                    }
                    if (v.tag == Value::TAG_STRING && v.str_val.ptr) {
                        uint64_t h = 0;
                        for (uint32_t k = 0; k < v.str_val.len; ++k) {
                            h = h * 31 + static_cast<uint8_t>(v.str_val.ptr[k]);
                        }
                        return static_cast<int>(h % shard_count);
                    }
                }
            }

            // Check AND: both sides might contain the shard key
            if (op.equals_ci("AND", 3)) {
                const sql_parser::AstNode* left = where_expr->first_child;
                const sql_parser::AstNode* right = left ? left->next_sibling : nullptr;
                int r = find_shard_from_where(left, shard_key, shard_count);
                if (r >= 0) return r;
                return find_shard_from_where(right, shard_key, shard_count);
            }
        }

        return -1;
    }

    bool is_column_ref(const sql_parser::AstNode* node, sql_parser::StringRef col_name) {
        if (!node) return false;
        if (node->type == sql_parser::NodeType::NODE_COLUMN_REF ||
            node->type == sql_parser::NodeType::NODE_IDENTIFIER) {
            return node->value().equals_ci(col_name.ptr, col_name.len);
        }
        return false;
    }

    // Scatter DML SQL to all shards, combining results via UNION ALL
    PlanNode* scatter_dml_to_shards(const TableInfo* table,
                                     const std::vector<ShardInfo>& shard_list,
                                     std::function<sql_parser::StringRef()> build_sql) {
        if (shard_list.empty()) return nullptr;

        PlanNode* current = nullptr;
        for (const auto& shard : shard_list) {
            sql_parser::StringRef sql = build_sql();
            PlanNode* rs = make_remote_scan(shard.backend_name.c_str(), sql, table);
            if (!current) {
                current = rs;
            } else {
                PlanNode* union_node = make_plan_node(arena_, PlanNodeType::SET_OP);
                union_node->set_op.op = SET_OP_UNION;
                union_node->set_op.all = true;
                union_node->left = current;
                union_node->right = rs;
                current = union_node;
            }
        }
        return current;
    }

    // Case 6: Distributed DISTINCT
    PlanNode* distribute_distinct(PlanNode* distinct_node) {
        // Check child for sharded scan
        PlanNode* child = distinct_node->left;

        // Look through PROJECT to find scan
        PlanNode* scan_search = child;
        const sql_parser::AstNode** proj_exprs = nullptr;
        uint16_t proj_count = 0;
        if (scan_search && scan_search->type == PlanNodeType::PROJECT) {
            proj_exprs = scan_search->project.exprs;
            proj_count = scan_search->project.count;
            scan_search = scan_search->left;
        }

        ScanContext ctx = extract_scan_context(scan_search);
        if (!ctx.scan || !ctx.scan->scan.table) {
            PlanNode* result = make_plan_node(arena_, PlanNodeType::DISTINCT);
            result->left = distribute_node(distinct_node->left);
            return result;
        }

        const TableInfo* table = ctx.scan->scan.table;
        if (!shards_.has_table(table->table_name) || !shards_.is_sharded(table->table_name)) {
            if (shards_.has_table(table->table_name) && !shards_.is_sharded(table->table_name)) {
                // Unsharded: push DISTINCT to remote
                sql_parser::StringRef sql = qb_.build_select(
                    table, ctx.where_expr, proj_exprs, proj_count,
                    nullptr, 0, nullptr, nullptr, 0, -1, true);
                return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
            }
            PlanNode* result = make_plan_node(arena_, PlanNodeType::DISTINCT);
            result->left = distribute_node(distinct_node->left);
            return result;
        }

        // Sharded DISTINCT: each shard computes DISTINCT, local DISTINCT deduplicates
        const auto& shard_list = shards_.get_shards(table->table_name);

        PlanNode* union_all = make_sharded_union(
            table, ctx.where_expr, proj_exprs, proj_count,
            nullptr, 0, nullptr, nullptr, 0, -1, true, shard_list);

        PlanNode* local_distinct = make_plan_node(arena_, PlanNodeType::DISTINCT);
        local_distinct->left = union_all;
        return local_distinct;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_DISTRIBUTED_PLANNER_H
