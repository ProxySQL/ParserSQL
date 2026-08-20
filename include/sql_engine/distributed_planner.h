#ifndef SQL_ENGINE_DISTRIBUTED_PLANNER_H
#define SQL_ENGINE_DISTRIBUTED_PLANNER_H

#include "sql_engine/plan_node.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/catalog.h"
#include "sql_engine/remote_query_builder.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/operators/merge_aggregate_op.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_engine/result_set.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/catalog_resolver.h"
#include "sql_parser/arena.h"
#include "sql_parser/ast.h"
#include "sql_parser/common.h"
#include "sql_parser/string_builder.h"
#include "sql_parser/emitter.h"
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <climits>
#include <vector>
#include <unordered_map>
#include <functional>
#include <utility>

namespace sql_engine {

template <sql_parser::Dialect D>
class DistributedPlanner {
public:
    DistributedPlanner(const ShardMap& shards, const Catalog& catalog, sql_parser::Arena& arena)
        : shards_(shards), catalog_(catalog), arena_(arena), qb_(arena),
          remote_executor_(nullptr), functions_(nullptr), error_(nullptr) {}

    // Extended constructor with remote executor for cross-shard subquery support
    DistributedPlanner(const ShardMap& shards, const Catalog& catalog, sql_parser::Arena& arena,
                       RemoteExecutor* remote_executor, FunctionRegistry<D>* functions)
        : shards_(shards), catalog_(catalog), arena_(arena), qb_(arena),
          remote_executor_(remote_executor), functions_(functions), error_(nullptr) {}

    const char* last_error() const { return error_; }

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
        error_ = nullptr;
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
    RemoteExecutor* remote_executor_;
    FunctionRegistry<D>* functions_;
    const char* error_;

    PlanNode* fail_dml(const char* message) {
        error_ = message;
        return nullptr;
    }

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
                // (but not if the filter contains a subquery, which must be
                // evaluated locally after distributed subquery execution)
                if (node->left && node->left->type == PlanNodeType::SCAN &&
                    !has_subquery(node->filter.expr)) {
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
                // PROJECT -> [SORT ->] [FILTER(HAVING) ->] AGGREGATE
                PlanNode* sort_node = nullptr;
                PlanNode* having_node = nullptr;
                PlanNode* agg_child = node->left;
                if (agg_child && agg_child->type == PlanNodeType::SORT) {
                    sort_node = agg_child;
                    agg_child = agg_child->left;
                }
                if (agg_child && agg_child->type == PlanNodeType::FILTER) {
                    having_node = agg_child;
                    agg_child = agg_child->left;
                }
                if (agg_child && agg_child->type == PlanNodeType::AGGREGATE) {
                    push_agg_exprs_from_project(node, agg_child);
                    PlanNode* dist_agg = distribute_aggregate(agg_child);
                    if (dist_agg && (dist_agg->type == PlanNodeType::MERGE_AGGREGATE ||
                                     dist_agg->type == PlanNodeType::AGGREGATE)) {
                        PlanNode* top = dist_agg;
                        if (having_node) {
                            PlanNode* having = make_plan_node(arena_, PlanNodeType::FILTER);
                            having->filter.expr = having_node->filter.expr;
                            having->left = top;
                            top = having;
                        }
                        if (sort_node) {
                            PlanNode* sort = make_plan_node(arena_, PlanNodeType::SORT);
                            sort->sort = sort_node->sort;
                            sort->left = top;
                            top = sort;
                        }
                        return top;
                    }
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

            case PlanNodeType::WINDOW: {
                PlanNode* result = make_plan_node(arena_, PlanNodeType::WINDOW);
                result->window = node->window;
                result->left = distribute_node(node->left);
                return result;
            }

            case PlanNodeType::DERIVED_SCAN: {
                PlanNode* result = make_plan_node(arena_, PlanNodeType::DERIVED_SCAN);
                result->derived_scan = node->derived_scan;
                result->derived_scan.inner_plan = distribute(node->derived_scan.inner_plan);
                return result;
            }

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

    static bool contains_type(const PlanNode* node, PlanNodeType type) {
        if (!node) return false;
        if (node->type == type) return true;
        if (contains_type(node->left, type)) return true;
        if (contains_type(node->right, type)) return true;
        if (node->type == PlanNodeType::MERGE_AGGREGATE) {
            for (uint16_t i = 0; i < node->merge_aggregate.child_count; ++i) {
                if (contains_type(node->merge_aggregate.children[i], type)) return true;
            }
        }
        if (node->type == PlanNodeType::MERGE_SORT) {
            for (uint16_t i = 0; i < node->merge_sort.child_count; ++i) {
                if (contains_type(node->merge_sort.children[i], type)) return true;
            }
        }
        if (node->type == PlanNodeType::DERIVED_SCAN) {
            return contains_type(node->derived_scan.inner_plan, type);
        }
        return false;
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

        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");

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
        // Optimization (#27): if WHERE contains shard_key = <literal> or
        // shard_key IN (<literals>), route to only the relevant shard(s).
        const auto& full_shard_list = shards_.get_shards(table->table_name);
        std::vector<ShardInfo> pruned = prune_shards(table, where_expr, full_shard_list);
        return make_sharded_union(table, where_expr, nullptr, 0, nullptr, 0,
                                  nullptr, nullptr, 0, -1, false, pruned);
    }

    // Shard pruning (#27): analyze WHERE for shard_key = <literal> or
    // shard_key IN (<literal_list>). Returns a subset of shards when pruning
    // is possible, otherwise returns the full shard list.
    std::vector<ShardInfo> prune_shards(const TableInfo* table,
                                         const sql_parser::AstNode* where_expr,
                                         const std::vector<ShardInfo>& all_shards) {
        if (!where_expr || all_shards.empty()) return all_shards;

        const auto& keys = shards_.get_shard_keys(table->table_name);
        if (keys.empty()) return all_shards;

        std::vector<size_t> target_indices;
        if (keys.size() > 1 &&
            shards_.routing_strategy(table->table_name) == RoutingStrategy::RANGE) {
            sql_parser::StringRef first{keys[0].c_str(),
                static_cast<uint32_t>(keys[0].size())};
            extract_shard_targets(where_expr, first, table->table_name,
                                  all_shards.size(), target_indices);
        } else if (keys.size() > 1) {
            extract_composite_targets(where_expr, keys, table->table_name, target_indices);
        } else {
            sql_parser::StringRef shard_key{keys[0].c_str(),
                static_cast<uint32_t>(keys[0].size())};
            extract_shard_targets(where_expr, shard_key, table->table_name,
                                  all_shards.size(), target_indices);
        }

        if (target_indices.empty()) return all_shards;

        // Deduplicate and collect matching shards
        std::vector<bool> included(all_shards.size(), false);
        for (size_t idx : target_indices) {
            if (idx < all_shards.size()) included[idx] = true;
        }
        std::vector<ShardInfo> result;
        for (size_t i = 0; i < all_shards.size(); ++i) {
            if (included[i]) result.push_back(all_shards[i]);
        }
        return result.empty() ? all_shards : result;
    }

    // Walk a WHERE expression looking for shard_key = <literal> or
    // shard_key IN (<literal>, ...). Populates target_indices with
    // the shard index for each matched literal.
    void extract_shard_targets(const sql_parser::AstNode* expr,
                                sql_parser::StringRef shard_key,
                                sql_parser::StringRef table_name,
                                size_t num_shards,
                                std::vector<size_t>& target_indices) {
        if (!expr) return;

        // Check for shard_key = <literal>
        if (expr->type == sql_parser::NodeType::NODE_BINARY_OP) {
            sql_parser::StringRef op = expr->value();
            if (op.len == 1 && op.ptr[0] == '=') {
                const sql_parser::AstNode* left_node = expr->first_child;
                const sql_parser::AstNode* right_node = left_node ? left_node->next_sibling : nullptr;
                if (left_node && right_node) {
                    // Check if one side is the shard key column and the other is a literal
                    const sql_parser::AstNode* col_node = nullptr;
                    const sql_parser::AstNode* lit_node = nullptr;
                    if (is_shard_key_ref(left_node, shard_key) && is_literal(right_node)) {
                        col_node = left_node; lit_node = right_node;
                    } else if (is_shard_key_ref(right_node, shard_key) && is_literal(left_node)) {
                        col_node = right_node; lit_node = left_node;
                    }
                    if (col_node && lit_node) {
                        size_t idx = 0;
                        if (try_literal_to_shard_index(lit_node, table_name, idx))
                            target_indices.push_back(idx);
                        return;
                    }
                }
            }
            if (is_compare_op(op) &&
                shards_.routing_strategy(table_name) == RoutingStrategy::RANGE) {
                const sql_parser::AstNode* left_node = expr->first_child;
                const sql_parser::AstNode* right_node = left_node ? left_node->next_sibling : nullptr;
                if (left_node && right_node) {
                    const sql_parser::AstNode* col = nullptr;
                    const sql_parser::AstNode* lit = nullptr;
                    bool key_on_left = false;
                    if (is_shard_key_ref(left_node, shard_key) && is_literal(right_node)) {
                        col = left_node; lit = right_node; key_on_left = true;
                    } else if (is_shard_key_ref(right_node, shard_key) && is_literal(left_node)) {
                        col = right_node; lit = left_node; key_on_left = false;
                    }
                    if (col && lit) {
                        int64_t v = literal_to_int(lit);
                        int64_t lo = INT64_MIN, hi = INT64_MAX;
                        char c0 = op.ptr[0];
                        bool has_eq = op.len == 2 && op.ptr[1] == '=';
                        if (c0 == '<' && key_on_left) {
                            hi = has_eq ? v : (v == INT64_MIN ? INT64_MIN : v - 1);
                        } else if (c0 == '>' && key_on_left) {
                            lo = has_eq ? v : (v == INT64_MAX ? INT64_MAX : v + 1);
                        } else if (c0 == '<' && !key_on_left) {
                            lo = has_eq ? v : (v == INT64_MAX ? INT64_MAX : v + 1);
                        } else if (c0 == '>' && !key_on_left) {
                            hi = has_eq ? v : (v == INT64_MIN ? INT64_MIN : v - 1);
                        }
                        shards_.collect_int_range_shards(table_name, lo, hi, target_indices);
                        return;
                    }
                }
            }
            if (op.len == 2 &&
                (op.ptr[0] == 'O' || op.ptr[0] == 'o') &&
                (op.ptr[1] == 'R' || op.ptr[1] == 'r')) {
                const sql_parser::AstNode* left_node = expr->first_child;
                const sql_parser::AstNode* right_node = left_node ? left_node->next_sibling : nullptr;
                std::vector<size_t> left_targets, right_targets;
                extract_shard_targets(left_node, shard_key, table_name, num_shards, left_targets);
                extract_shard_targets(right_node, shard_key, table_name, num_shards, right_targets);
                if (left_targets.empty() || right_targets.empty()) return;
                std::vector<bool> seen(num_shards, false);
                for (auto i : left_targets) {
                    if (i < num_shards && !seen[i]) {
                        seen[i] = true;
                        target_indices.push_back(i);
                    }
                }
                for (auto i : right_targets) {
                    if (i < num_shards && !seen[i]) {
                        seen[i] = true;
                        target_indices.push_back(i);
                    }
                }
                return;
            }
            // Recurse into AND branches
            if (op.len == 3 &&
                (op.ptr[0] == 'A' || op.ptr[0] == 'a') &&
                (op.ptr[1] == 'N' || op.ptr[1] == 'n') &&
                (op.ptr[2] == 'D' || op.ptr[2] == 'd')) {
                const sql_parser::AstNode* left_node = expr->first_child;
                const sql_parser::AstNode* right_node = left_node ? left_node->next_sibling : nullptr;
                // For AND, either branch matching is sufficient (both must be true,
                // so if one constrains the shard key, we can prune).
                std::vector<size_t> left_targets, right_targets;
                extract_shard_targets(left_node, shard_key, table_name, num_shards, left_targets);
                extract_shard_targets(right_node, shard_key, table_name, num_shards, right_targets);
                // Use whichever branch found shard targets (prefer the more selective one)
                if (!left_targets.empty() && !right_targets.empty()) {
                    // Intersect: both constraints must hold
                    std::vector<bool> lset(num_shards, false), rset(num_shards, false);
                    for (auto i : left_targets) if (i < num_shards) lset[i] = true;
                    for (auto i : right_targets) if (i < num_shards) rset[i] = true;
                    for (size_t i = 0; i < num_shards; ++i) {
                        if (lset[i] && rset[i]) target_indices.push_back(i);
                    }
                } else if (!left_targets.empty()) {
                    target_indices.insert(target_indices.end(), left_targets.begin(), left_targets.end());
                } else if (!right_targets.empty()) {
                    target_indices.insert(target_indices.end(), right_targets.begin(), right_targets.end());
                }
                return;
            }
        }

        // Check for shard_key IN (literal_list)
        if (expr->type == sql_parser::NodeType::NODE_IN_LIST) {
            const sql_parser::AstNode* col_expr = expr->first_child;
            if (col_expr && is_shard_key_ref(col_expr, shard_key)) {
                for (const sql_parser::AstNode* item = col_expr->next_sibling; item; item = item->next_sibling) {
                    if (is_literal(item)) {
                        size_t idx = 0;
                        if (try_literal_to_shard_index(item, table_name, idx))
                            target_indices.push_back(idx);
                    } else {
                        // Non-literal in IN list -- can't prune
                        target_indices.clear();
                        return;
                    }
                }
            }
        }

        if (expr->type == sql_parser::NodeType::NODE_BETWEEN) {
            RoutingStrategy strat = shards_.routing_strategy(table_name);
            if (strat == RoutingStrategy::RANGE || strat == RoutingStrategy::LIST) {
                const sql_parser::AstNode* col = expr->first_child;
                const sql_parser::AstNode* lo = col ? col->next_sibling : nullptr;
                const sql_parser::AstNode* hi = lo ? lo->next_sibling : nullptr;
                if (col && is_shard_key_ref(col, shard_key) && is_literal(lo) && is_literal(hi)) {
                    if (strat == RoutingStrategy::RANGE) {
                        shards_.collect_int_range_shards(table_name,
                                                         literal_to_int(lo), literal_to_int(hi),
                                                         target_indices);
                    } else {
                        shards_.collect_int_list_shards(table_name,
                                                        literal_to_int(lo), literal_to_int(hi),
                                                        target_indices);
                    }
                }
            }
        }
    }

    static bool is_compare_op(sql_parser::StringRef op) {
        if (op.len == 1) return op.ptr[0] == '<' || op.ptr[0] == '>';
        if (op.len == 2) return (op.ptr[0] == '<' || op.ptr[0] == '>') && op.ptr[1] == '=';
        return false;
    }

    static int64_t literal_to_int(const sql_parser::AstNode* lit) {
        if (!lit || !lit->value().ptr) return 0;
        return std::strtoll(lit->value().ptr, nullptr, 10);
    }

    bool is_shard_key_ref(const sql_parser::AstNode* node, sql_parser::StringRef shard_key) const {
        if (!node) return false;
        if (node->type == sql_parser::NodeType::NODE_COLUMN_REF ||
            node->type == sql_parser::NodeType::NODE_IDENTIFIER) {
            return node->value().equals_ci(shard_key.ptr, shard_key.len);
        }
        if (node->type == sql_parser::NodeType::NODE_QUALIFIED_NAME) {
            // table.column -- check the column part
            const sql_parser::AstNode* c = node->first_child;
            if (c && c->next_sibling) {
                return c->next_sibling->value().equals_ci(shard_key.ptr, shard_key.len);
            }
        }
        return false;
    }

    static bool is_literal(const sql_parser::AstNode* node) {
        if (!node) return false;
        return node->type == sql_parser::NodeType::NODE_LITERAL_INT ||
               node->type == sql_parser::NodeType::NODE_LITERAL_FLOAT ||
               node->type == sql_parser::NodeType::NODE_LITERAL_STRING;
    }

    bool try_literal_to_shard_index(const sql_parser::AstNode* lit,
                                    sql_parser::StringRef table_name,
                                    size_t& out) const {
        if (!lit) return false;
        if (lit->type == sql_parser::NodeType::NODE_LITERAL_INT) {
            sql_parser::StringRef sv = lit->value();
            int64_t val = 0;
            if (sv.ptr && sv.len > 0) val = std::strtoll(sv.ptr, nullptr, 10);
            return shards_.try_shard_index_for_int(table_name, val, out);
        }
        if (lit->type == sql_parser::NodeType::NODE_LITERAL_STRING) {
            sql_parser::StringRef sv = lit->value();
            return shards_.try_shard_index_for_string(table_name, sv.ptr, sv.len, out);
        }
        if (lit->type == sql_parser::NodeType::NODE_LITERAL_FLOAT) {
            sql_parser::StringRef sv = lit->value();
            double dv = sv.ptr ? std::strtod(sv.ptr, nullptr) : 0.0;
            return shards_.try_shard_index_for_int(
                table_name, static_cast<int64_t>(dv), out);
        }
        return false;
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
        if (!backend) return fail_dml("table not in shard map");
        PlanNode* node = make_plan_node(arena_, PlanNodeType::REMOTE_SCAN);
        // Copy backend name to arena
        uint32_t blen = static_cast<uint32_t>(std::strlen(backend));
        char* bn = static_cast<char*>(arena_.allocate(blen + 1));
        std::memcpy(bn, backend, blen + 1);
        node->remote_scan.backend_name = bn;
        node->remote_scan.remote_sql = sql.ptr;
        node->remote_scan.remote_sql_len = sql.len;
        node->remote_scan.table = table;
        // Caller is responsible for setting output_exprs when the remote SQL
        // is not a passthrough SELECT *. make_plan_node() already zero-fills
        // the union, so leaving these unset here means "fall back to the
        // table's catalog columns".
        return node;
    }

    PlanNode* make_remote_scan_with_outputs(
        const char* backend, sql_parser::StringRef sql, const TableInfo* table,
        const std::vector<const sql_parser::AstNode*>& output_exprs)
    {
        PlanNode* node = make_remote_scan(backend, sql, table);
        if (!output_exprs.empty()) {
            uint16_t n = static_cast<uint16_t>(output_exprs.size());
            auto** arr = static_cast<const sql_parser::AstNode**>(
                arena_.allocate(sizeof(sql_parser::AstNode*) * n));
            for (uint16_t i = 0; i < n; ++i) arr[i] = output_exprs[i];
            node->remote_scan.output_exprs = arr;
            node->remote_scan.output_expr_count = n;
        }
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
        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");
        if (!shards_.is_sharded(table->table_name)) {
            return make_unsharded_aggregate(agg_node, ctx, table);
        }

        if (!all_aggregates_two_phase(agg_node)) {
            PlanNode* result = make_plan_node(arena_, PlanNodeType::AGGREGATE);
            result->aggregate = agg_node->aggregate;
            result->left = distribute_node(agg_node->left);
            return result;
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

        // Store original output expressions for column naming:
        // group_by expressions + aggregate expressions
        uint16_t out_count = agg_node->aggregate.group_count + agg_node->aggregate.agg_count;
        auto** out_exprs = static_cast<const sql_parser::AstNode**>(
            arena_.allocate(sizeof(sql_parser::AstNode*) * out_count));
        for (uint16_t i = 0; i < agg_node->aggregate.group_count; ++i)
            out_exprs[i] = agg_node->aggregate.group_by[i];
        for (uint16_t i = 0; i < agg_node->aggregate.agg_count; ++i)
            out_exprs[agg_node->aggregate.group_count + i] = agg_node->aggregate.agg_exprs[i];
        merge->merge_aggregate.output_exprs = out_exprs;
        merge->merge_aggregate.output_expr_count = out_count;

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
        // Carry the projection expressions on the REMOTE_SCAN so the result
        // schema picks them up instead of mis-labelling with the source
        // table's catalog columns. Without this, "SELECT COUNT(*) FROM users"
        // against a single-shard config renders as the table's first
        // column ("id") rather than the aggregate.
        return make_remote_scan_with_outputs(backend, sql, table, projs);
    }

    static bool is_two_phase_aggregate(const sql_parser::AstNode* expr) {
        if (!expr || expr->type != sql_parser::NodeType::NODE_FUNCTION_CALL) return false;
        if (expr->flags & sql_parser::FLAG_FUNC_DISTINCT) return false;
        sql_parser::StringRef name = expr->value();
        return name.equals_ci("COUNT", 5) || name.equals_ci("SUM", 3) ||
               name.equals_ci("AVG", 3) || name.equals_ci("MIN", 3) ||
               name.equals_ci("MAX", 3);
    }

    bool all_aggregates_two_phase(const PlanNode* agg_node) const {
        if (!agg_node) return false;
        if (agg_node->aggregate.agg_count == 0) return true;
        for (uint16_t i = 0; i < agg_node->aggregate.agg_count; ++i) {
            if (!is_two_phase_aggregate(agg_node->aggregate.agg_exprs[i])) return false;
        }
        return true;
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
            projs.push_back(expr);
            merge_ops.push_back(static_cast<uint8_t>(MergeOp::SUM_OF_COUNTS));
        } else if (name.equals_ci("SUM", 3)) {
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
    PlanNode* local_sort(PlanNode* sort_node) {
        PlanNode* result = make_plan_node(arena_, PlanNodeType::SORT);
        result->sort = sort_node->sort;
        result->left = distribute_node(sort_node->left);
        return result;
    }

    int sort_key_table_ordinal(const sql_parser::AstNode* key, const TableInfo* table) const {
        if (!key || !table) return -1;
        if (key->type == sql_parser::NodeType::NODE_LITERAL_INT) {
            sql_parser::StringRef sv = key->value();
            if (!sv.ptr || sv.len == 0) return -1;
            int64_t n = std::strtoll(sv.ptr, nullptr, 10);
            if (n < 1 || n > static_cast<int64_t>(table->column_count)) return -1;
            return static_cast<int>(n - 1);
        }
        sql_parser::StringRef col_name;
        if (key->type == sql_parser::NodeType::NODE_COLUMN_REF ||
            key->type == sql_parser::NodeType::NODE_IDENTIFIER) {
            col_name = key->value();
        } else if (key->type == sql_parser::NodeType::NODE_QUALIFIED_NAME) {
            const sql_parser::AstNode* c = key->first_child;
            if (c && c->next_sibling) col_name = c->next_sibling->value();
            else if (c) col_name = c->value();
        } else {
            return -1;
        }
        if (!col_name.ptr) return -1;
        const ColumnInfo* col = catalog_.get_column(table, col_name);
        if (!col) return -1;
        return static_cast<int>(col->ordinal);
    }

    bool all_sort_keys_are_table_columns(const PlanNode* sort_node, const TableInfo* table) const {
        if (!sort_node || !table) return false;
        for (uint16_t i = 0; i < sort_node->sort.count; ++i) {
            if (sort_key_table_ordinal(sort_node->sort.keys[i], table) < 0) return false;
        }
        return true;
    }

    PlanNode* distribute_sort(PlanNode* sort_node) {
        if (contains_type(sort_node->left, PlanNodeType::WINDOW) ||
            contains_type(sort_node->left, PlanNodeType::DERIVED_SCAN) ||
            contains_type(sort_node->left, PlanNodeType::AGGREGATE) ||
            contains_type(sort_node->left, PlanNodeType::MERGE_AGGREGATE)) {
            return local_sort(sort_node);
        }

        ScanContext ctx = extract_scan_context(sort_node->left);
        if (!ctx.scan || !ctx.scan->scan.table) {
            PlanNode* result = make_plan_node(arena_, PlanNodeType::SORT);
            result->sort = sort_node->sort;
            result->left = distribute_node(sort_node->left);
            return result;
        }

        const TableInfo* table = ctx.scan->scan.table;
        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");

        if (!all_sort_keys_are_table_columns(sort_node, table)) {
            return local_sort(sort_node);
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
            if (contains_type(sort_node->left, PlanNodeType::WINDOW) ||
                contains_type(sort_node->left, PlanNodeType::DERIVED_SCAN) ||
                contains_type(sort_node->left, PlanNodeType::AGGREGATE) ||
                contains_type(sort_node->left, PlanNodeType::MERGE_AGGREGATE)) {
                PlanNode* result = make_plan_node(arena_, PlanNodeType::LIMIT);
                result->limit = limit_node->limit;
                result->left = distribute_node(limit_node->left);
                return result;
            }
            ScanContext ctx = extract_scan_context(sort_node->left);
            if (ctx.scan && ctx.scan->scan.table) {
                const TableInfo* table = ctx.scan->scan.table;
                if (shards_.has_table(table->table_name) &&
                    shards_.is_sharded(table->table_name)) {
                    if (!all_sort_keys_are_table_columns(sort_node, table)) {
                        PlanNode* result = make_plan_node(arena_, PlanNodeType::LIMIT);
                        result->limit = limit_node->limit;
                        result->left = distribute_node(limit_node->left);
                        return result;
                    }
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

        if (contains_type(limit_node->left, PlanNodeType::WINDOW) ||
            contains_type(limit_node->left, PlanNodeType::DERIVED_SCAN) ||
            contains_type(limit_node->left, PlanNodeType::AGGREGATE) ||
            contains_type(limit_node->left, PlanNodeType::MERGE_AGGREGATE)) {
            PlanNode* result = make_plan_node(arena_, PlanNodeType::LIMIT);
            result->limit = limit_node->limit;
            result->left = distribute_node(limit_node->left);
            return result;
        }

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

    bool join_on_shard_keys(const sql_parser::AstNode* cond,
                            sql_parser::StringRef left_key,
                            sql_parser::StringRef right_key) const {
        if (!cond || cond->type != sql_parser::NodeType::NODE_BINARY_OP) return false;
        sql_parser::StringRef op = cond->value();
        if (op.len != 1 || op.ptr[0] != '=') return false;
        const sql_parser::AstNode* l = cond->first_child;
        const sql_parser::AstNode* r = l ? l->next_sibling : nullptr;
        if (!l || !r) return false;
        return (is_shard_key_ref(l, left_key) && is_shard_key_ref(r, right_key)) ||
               (is_shard_key_ref(l, right_key) && is_shard_key_ref(r, left_key));
    }

    void collect_eq_pairs(const sql_parser::AstNode* expr,
                          std::vector<std::pair<const sql_parser::AstNode*,
                                                const sql_parser::AstNode*>>& out) const {
        if (!expr || expr->type != sql_parser::NodeType::NODE_BINARY_OP) return;
        sql_parser::StringRef op = expr->value();
        if (op.len == 1 && op.ptr[0] == '=') {
            const sql_parser::AstNode* l = expr->first_child;
            const sql_parser::AstNode* r = l ? l->next_sibling : nullptr;
            if (l && r) out.push_back({l, r});
            return;
        }
        if (op.len == 3 &&
            (op.ptr[0] == 'A' || op.ptr[0] == 'a') &&
            (op.ptr[1] == 'N' || op.ptr[1] == 'n') &&
            (op.ptr[2] == 'D' || op.ptr[2] == 'd')) {
            collect_eq_pairs(expr->first_child, out);
            if (expr->first_child)
                collect_eq_pairs(expr->first_child->next_sibling, out);
        }
    }

    bool join_covers_composite_keys(const sql_parser::AstNode* cond,
                                    const std::vector<std::string>& left_keys,
                                    const std::vector<std::string>& right_keys) const {
        if (left_keys.empty() || left_keys.size() != right_keys.size()) return false;
        if (left_keys.size() == 1) {
            sql_parser::StringRef lk{left_keys[0].c_str(),
                static_cast<uint32_t>(left_keys[0].size())};
            sql_parser::StringRef rk{right_keys[0].c_str(),
                static_cast<uint32_t>(right_keys[0].size())};
            return join_on_shard_keys(cond, lk, rk);
        }
        std::vector<std::pair<const sql_parser::AstNode*, const sql_parser::AstNode*>> eqs;
        collect_eq_pairs(cond, eqs);
        for (size_t i = 0; i < left_keys.size(); ++i) {
            sql_parser::StringRef lk{left_keys[i].c_str(),
                static_cast<uint32_t>(left_keys[i].size())};
            sql_parser::StringRef rk{right_keys[i].c_str(),
                static_cast<uint32_t>(right_keys[i].size())};
            bool found = false;
            for (const auto& eq : eqs) {
                if ((is_shard_key_ref(eq.first, lk) && is_shard_key_ref(eq.second, rk)) ||
                    (is_shard_key_ref(eq.first, rk) && is_shard_key_ref(eq.second, lk))) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    PlanNode* distribute_colocated_join(PlanNode* join_node,
                                        const TableInfo* left_table,
                                        const TableInfo* right_table) {
        ScanContext lctx = extract_scan_context(join_node->left);
        ScanContext rctx = extract_scan_context(join_node->right);
        const sql_parser::AstNode* where_expr = nullptr;
        if (lctx.where_expr && rctx.where_expr) {
            sql_parser::AstNode* and_node = sql_parser::make_node(
                arena_, sql_parser::NodeType::NODE_BINARY_OP,
                sql_parser::StringRef{"AND", 3});
            and_node->add_child(const_cast<sql_parser::AstNode*>(lctx.where_expr));
            and_node->add_child(const_cast<sql_parser::AstNode*>(rctx.where_expr));
            where_expr = and_node;
        } else if (lctx.where_expr) {
            where_expr = lctx.where_expr;
        } else {
            where_expr = rctx.where_expr;
        }

        const auto& shard_list = shards_.get_shards(left_table->table_name);
        PlanNode* current = nullptr;
        for (const auto& shard : shard_list) {
            sql_parser::StringRef sql = qb_.build_select_join(
                left_table, right_table, join_node->join.condition, where_expr);
            PlanNode* rs = make_remote_scan(shard.backend_name.c_str(), sql, left_table);
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
        return current ? current : join_node;
    }

    bool column_on_table(const sql_parser::AstNode* node, const TableInfo* table,
                         const TableInfo* other) const {
        if (!node || !table) return false;
        if (node->type == sql_parser::NodeType::NODE_QUALIFIED_NAME) {
            const sql_parser::AstNode* t = node->first_child;
            if (!t) return false;
            sql_parser::StringRef tn = t->value();
            if (table->table_name.equals_ci(tn.ptr, tn.len)) return true;
            if (table->alias.ptr && table->alias.equals_ci(tn.ptr, tn.len)) return true;
            return false;
        }
        if (node->type == sql_parser::NodeType::NODE_COLUMN_REF ||
            node->type == sql_parser::NodeType::NODE_IDENTIFIER) {
            if (!catalog_.get_column(table, node->value())) return false;
            if (other && catalog_.get_column(other, node->value())) return false;
            return true;
        }
        return false;
    }

    const sql_parser::AstNode* probe_key_in_join(const sql_parser::AstNode* cond,
                                                 const TableInfo* probe,
                                                 const TableInfo* build) const {
        if (!cond || !probe || !build) return nullptr;
        const auto& keys = shards_.get_shard_keys(probe->table_name);
        if (keys.size() != 1) return nullptr;
        sql_parser::StringRef sk{keys[0].c_str(), static_cast<uint32_t>(keys[0].size())};
        std::vector<std::pair<const sql_parser::AstNode*, const sql_parser::AstNode*>> eqs;
        collect_eq_pairs(cond, eqs);
        for (const auto& eq : eqs) {
            if (is_shard_key_ref(eq.first, sk) && column_on_table(eq.second, build, probe))
                return eq.first;
            if (is_shard_key_ref(eq.second, sk) && column_on_table(eq.first, build, probe))
                return eq.second;
        }
        return nullptr;
    }

    sql_parser::AstNode* make_in_list_on_column(const sql_parser::AstNode* col,
                                                const std::vector<Value>& values) {
        if (!col || values.empty()) return nullptr;
        sql_parser::AstNode* stub = sql_parser::make_node(
            arena_, sql_parser::NodeType::NODE_IN_LIST,
            sql_parser::StringRef{nullptr, 0});
        sql_parser::AstNode* col_copy = sql_parser::make_node(
            arena_, col->type, col->value(), col->flags);
        col_copy->first_child = col->first_child;
        stub->add_child(col_copy);
        return build_in_list_from_values(stub, values);
    }

    sql_parser::AstNode* and_preds(const sql_parser::AstNode* a,
                                   const sql_parser::AstNode* b) {
        if (!a) return const_cast<sql_parser::AstNode*>(b);
        if (!b) return const_cast<sql_parser::AstNode*>(a);
        sql_parser::AstNode* n = sql_parser::make_node(
            arena_, sql_parser::NodeType::NODE_BINARY_OP,
            sql_parser::StringRef{"AND", 3});
        n->add_child(const_cast<sql_parser::AstNode*>(a));
        n->add_child(const_cast<sql_parser::AstNode*>(b));
        return n;
    }

    std::vector<Value> collect_build_join_keys(const TableInfo* build,
                                               const sql_parser::AstNode* where_expr,
                                               const sql_parser::AstNode* join_eq_other) {
        std::vector<Value> out;
        if (!build || !join_eq_other || !remote_executor_) return out;
        const sql_parser::AstNode* proj[1] = {join_eq_other};
        const auto& shards = shards_.get_shards(build->table_name);
        if (shards.empty()) return out;
        std::vector<ShardInfo> targets = shards;
        if (shards_.is_sharded(build->table_name) && shards.size() > 1)
            return out;
        sql_parser::StringRef sql = qb_.build_select(
            build, where_expr, proj, 1, nullptr, 0,
            nullptr, nullptr, 0, -1, true);
        ResultSet rs = remote_executor_->execute(shards[0].backend_name.c_str(), sql);
        for (const auto& row : rs.rows) {
            if (row.column_count > 0 && value_is_routable(row.get(0)))
                out.push_back(copy_value_arena(row.get(0)));
        }
        return out;
    }

    const sql_parser::AstNode* other_eq_side(const sql_parser::AstNode* cond,
                                             const sql_parser::AstNode* probe_key) const {
        std::vector<std::pair<const sql_parser::AstNode*, const sql_parser::AstNode*>> eqs;
        collect_eq_pairs(cond, eqs);
        for (const auto& eq : eqs) {
            if (eq.first == probe_key) return eq.second;
            if (eq.second == probe_key) return eq.first;
        }
        return nullptr;
    }

    PlanNode* try_semijoin_prune(PlanNode* join_node,
                                 const TableInfo* left_table,
                                 const TableInfo* right_table) {
        if (!join_node || !remote_executor_ || !join_node->join.condition)
            return nullptr;
        if (!left_table || !right_table) return nullptr;

        bool ls = shards_.is_sharded(left_table->table_name);
        bool rs = shards_.is_sharded(right_table->table_name);
        if (ls == rs) return nullptr;

        const TableInfo* probe = ls ? left_table : right_table;
        const TableInfo* build = ls ? right_table : left_table;
        bool probe_is_left = ls;
        const sql_parser::AstNode* probe_key =
            probe_key_in_join(join_node->join.condition, probe, build);
        if (!probe_key) return nullptr;
        const sql_parser::AstNode* build_col =
            other_eq_side(join_node->join.condition, probe_key);
        if (!build_col) return nullptr;

        ScanContext bctx = extract_scan_context(
            probe_is_left ? join_node->right : join_node->left);
        std::vector<Value> keys = collect_build_join_keys(build, bctx.where_expr, build_col);
        if (keys.empty()) return nullptr;

        sql_parser::AstNode* in_list = make_in_list_on_column(probe_key, keys);
        if (!in_list) return nullptr;

        ScanContext pctx = extract_scan_context(
            probe_is_left ? join_node->left : join_node->right);
        if (!pctx.scan) return nullptr;
        const sql_parser::AstNode* probe_where = and_preds(pctx.where_expr, in_list);
        PlanNode* probe_dist = distribute_scan(pctx.scan, probe_where,
                                               nullptr, nullptr, nullptr, false);

        PlanNode* build_dist = nullptr;
        if (bctx.scan && !shards_.is_sharded(build->table_name)) {
            sql_parser::StringRef sql = qb_.build_select(
                build, bctx.where_expr, nullptr, 0, nullptr, 0,
                nullptr, nullptr, 0, -1, false);
            build_dist = make_remote_scan(
                shards_.get_backend(build->table_name), sql, build);
        } else {
            build_dist = distribute_node(probe_is_left ? join_node->right : join_node->left);
        }
        if (!probe_dist || !build_dist) return nullptr;

        PlanNode* result = make_plan_node(arena_, PlanNodeType::JOIN);
        result->join = join_node->join;
        result->left = probe_is_left ? probe_dist : build_dist;
        result->right = probe_is_left ? build_dist : probe_dist;
        return result;
    }

    PlanNode* distribute_join(PlanNode* join_node) {
        const TableInfo* left_table = find_table(join_node->left);
        const TableInfo* right_table = find_table(join_node->right);

        if (left_table && right_table &&
            shards_.is_sharded(left_table->table_name) &&
            shards_.is_sharded(right_table->table_name) &&
            shards_.same_routing(left_table->table_name, right_table->table_name) &&
            join_covers_composite_keys(join_node->join.condition,
                                       shards_.get_shard_keys(left_table->table_name),
                                       shards_.get_shard_keys(right_table->table_name))) {
            return distribute_colocated_join(join_node, left_table, right_table);
        }

        if (PlanNode* sj = try_semijoin_prune(join_node, left_table, right_table))
            return sj;

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
        if (!table) return plan;
        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");

        // Check for INSERT ... SELECT (select_source stores the SELECT AST)
        if (ip.select_source && ip.select_source->type == PlanNodeType::DERIVED_SCAN
            && ip.select_source->derived_scan.alias_len == 0xFFFF) {
            const sql_parser::AstNode* select_ast =
                reinterpret_cast<const sql_parser::AstNode*>(ip.select_source->derived_scan.alias);
            if (select_ast) {
                return distribute_insert_select(plan, select_ast);
            }
        }

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded: single remote INSERT
            sql_parser::StringRef sql = qb_.build_insert(
                table, ip.columns, ip.column_count, ip.value_rows, ip.row_count);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        const auto& key_names = shards_.get_shard_keys(table->table_name);
        if (key_names.empty()) return plan;

        std::vector<int> key_ords(key_names.size(), -1);
        for (size_t k = 0; k < key_names.size(); ++k) {
            sql_parser::StringRef kn{key_names[k].c_str(),
                static_cast<uint32_t>(key_names[k].size())};
            key_ords[k] = find_insert_key_ordinal(ip.columns, ip.column_count, table, kn);
        }
        for (int ord : key_ords) {
            if (ord < 0)
                return fail_dml("cannot route INSERT: shard key column is not present");
        }

        const auto& shard_list = shards_.get_shards(table->table_name);

        std::unordered_map<size_t, std::vector<uint16_t>> shard_rows;

        for (uint16_t ri = 0; ri < ip.row_count; ++ri) {
            const sql_parser::AstNode* row_ast = ip.value_rows[ri];
            if (!row_ast) continue;

            std::vector<ShardKeyPart> parts;
            parts.reserve(key_ords.size());
            for (int ord : key_ords) {
                const sql_parser::AstNode* expr = row_ast->first_child;
                for (int j = 0; j < ord && expr; ++j) expr = expr->next_sibling;
                if (!expr)
                    return fail_dml("cannot route INSERT: missing shard key value");
                Value v = evaluate_shard_key_value(expr);
                if (!value_is_routable(v))
                    return fail_dml("cannot route INSERT: shard key is not a literal");
                parts.push_back(value_to_part(v));
            }
            size_t shard_idx = 0;
            if (!shards_.try_shard_index_for_parts(table->table_name, parts.data(),
                                                   parts.size(), shard_idx)) {
                return fail_dml("cannot route INSERT: shard key value is not mapped");
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

        if (up.original_ast) {
            return distribute_multi_table_dml(up.original_ast, table, true);
        }

        if (!table) return plan;
        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");

        // Check for cross-shard subqueries in WHERE and rewrite
        const sql_parser::AstNode* where_expr = up.where_expr;
        if (where_expr && has_subquery(where_expr) && remote_executor_) {
            where_expr = rewrite_where_subquery(where_expr, table);
        }

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded: single remote UPDATE
            sql_parser::StringRef sql = qb_.build_update(
                table, up.set_columns, up.set_exprs, up.set_count, where_expr);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        if (assigns_any_shard_key(up.set_columns, up.set_count, table->table_name)) {
            return distribute_update_move(plan, table, where_expr);
        }

        const auto& shard_list = shards_.get_shards(table->table_name);
        std::vector<ShardInfo> pruned = prune_shards(table, where_expr, shard_list);
        if (pruned.size() == 1) {
            sql_parser::StringRef sql = qb_.build_update(
                table, up.set_columns, up.set_exprs, up.set_count, where_expr);
            return make_remote_scan(pruned[0].backend_name.c_str(), sql, table);
        }

        const sql_parser::AstNode* final_where = where_expr;
        return scatter_dml_to_shards(table, pruned, [&]() {
            return qb_.build_update(
                table, up.set_columns, up.set_exprs, up.set_count, final_where);
        });
    }

    PlanNode* distribute_delete(PlanNode* plan) {
        const auto& dp = plan->delete_plan;
        const TableInfo* table = dp.table;

        if (dp.original_ast) {
            return distribute_multi_table_dml(dp.original_ast, table, false);
        }

        if (!table) return plan;
        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");

        // Check for cross-shard subqueries in WHERE and rewrite
        const sql_parser::AstNode* where_expr = dp.where_expr;
        if (where_expr && has_subquery(where_expr) && remote_executor_) {
            where_expr = rewrite_where_subquery(where_expr, table);
        }

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded: single remote DELETE
            sql_parser::StringRef sql = qb_.build_delete(table, where_expr);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        const auto& shard_list = shards_.get_shards(table->table_name);
        std::vector<ShardInfo> pruned = prune_shards(table, where_expr, shard_list);
        if (pruned.size() == 1) {
            sql_parser::StringRef sql = qb_.build_delete(table, where_expr);
            return make_remote_scan(pruned[0].backend_name.c_str(), sql, table);
        }

        const sql_parser::AstNode* final_where = where_expr;
        return scatter_dml_to_shards(table, pruned, [&]() {
            return qb_.build_delete(table, final_where);
        });
    }

    Value evaluate_shard_key_value(const sql_parser::AstNode* expr) {
        if (!expr) return value_null();
        if (expr->type == sql_parser::NodeType::NODE_LITERAL_INT) {
            sql_parser::StringRef val = expr->value();
            if (!val.ptr || val.len == 0) return value_null();
            return value_int(std::strtoll(val.ptr, nullptr, 10));
        }
        if (expr->type == sql_parser::NodeType::NODE_LITERAL_FLOAT) {
            sql_parser::StringRef val = expr->value();
            double dv = val.ptr ? std::strtod(val.ptr, nullptr) : 0.0;
            return value_int(static_cast<int64_t>(dv));
        }
        if (expr->type == sql_parser::NodeType::NODE_LITERAL_STRING) {
            return value_string(expr->value());
        }
        if (expr->type == sql_parser::NodeType::NODE_UNARY_OP) {
            sql_parser::StringRef op = expr->value();
            if (op.len == 1 && op.ptr[0] == '-') {
                Value inner = evaluate_shard_key_value(expr->first_child);
                if (inner.tag == Value::TAG_INT64) return value_int(-inner.int_val);
            }
        }
        return value_null();
    }

    bool route_value(sql_parser::StringRef table_name, const Value& v, size_t& shard_idx) const {
        if (!shards_.has_table(table_name)) return false;
        if (v.tag == Value::TAG_INT64)
            return shards_.try_shard_index_for_int(table_name, v.int_val, shard_idx);
        if (v.tag == Value::TAG_UINT64) {
            return shards_.try_shard_index_for_int(
                table_name, static_cast<int64_t>(v.uint_val), shard_idx);
        }
        if (v.tag == Value::TAG_STRING && v.str_val.ptr) {
            return shards_.try_shard_index_for_string(
                table_name, v.str_val.ptr, v.str_val.len, shard_idx);
        }
        return false;
    }

    bool assigns_shard_key(const sql_parser::AstNode** set_columns, uint16_t set_count,
                           sql_parser::StringRef shard_key) const {
        if (!set_columns || !shard_key.ptr) return false;
        for (uint16_t i = 0; i < set_count; ++i) {
            if (is_column_ref(set_columns[i], shard_key)) return true;
        }
        return false;
    }

    bool assigns_any_shard_key(const sql_parser::AstNode** set_columns, uint16_t set_count,
                               sql_parser::StringRef table_name) const {
        for (const auto& k : shards_.get_shard_keys(table_name)) {
            sql_parser::StringRef kn{k.c_str(), static_cast<uint32_t>(k.size())};
            if (assigns_shard_key(set_columns, set_count, kn)) return true;
        }
        return false;
    }

    static bool value_is_routable(const Value& v) {
        return v.tag == Value::TAG_INT64 || v.tag == Value::TAG_UINT64 ||
               (v.tag == Value::TAG_STRING && v.str_val.ptr);
    }

    static ShardKeyPart value_to_part(const Value& v) {
        ShardKeyPart p;
        if (v.tag == Value::TAG_INT64) {
            p.is_int = true;
            p.int_val = v.int_val;
        } else if (v.tag == Value::TAG_UINT64) {
            p.is_int = true;
            p.int_val = static_cast<int64_t>(v.uint_val);
        } else {
            p.is_int = false;
            p.str = v.str_val.ptr;
            p.str_len = v.str_val.len;
        }
        return p;
    }

    static int find_insert_key_ordinal(const sql_parser::AstNode** columns,
                                       uint16_t column_count,
                                       const TableInfo* table,
                                       sql_parser::StringRef key) {
        if (columns && column_count > 0) {
            for (uint16_t i = 0; i < column_count; ++i) {
                if (columns[i] && columns[i]->value().equals_ci(key.ptr, key.len))
                    return static_cast<int>(i);
            }
            return -1;
        }
        if (!table) return -1;
        for (uint16_t i = 0; i < table->column_count; ++i) {
            if (table->columns[i].name.equals_ci(key.ptr, key.len))
                return static_cast<int>(i);
        }
        return -1;
    }

    void collect_key_eq_values(const sql_parser::AstNode* expr,
                               sql_parser::StringRef key,
                               std::vector<Value>& out) {
        if (!expr) return;
        if (expr->type == sql_parser::NodeType::NODE_BINARY_OP) {
            sql_parser::StringRef op = expr->value();
            if (op.len == 1 && op.ptr[0] == '=') {
                const sql_parser::AstNode* l = expr->first_child;
                const sql_parser::AstNode* r = l ? l->next_sibling : nullptr;
                if (l && r) {
                    const sql_parser::AstNode* lit = nullptr;
                    if (is_shard_key_ref(l, key) && is_literal(r)) lit = r;
                    else if (is_shard_key_ref(r, key) && is_literal(l)) lit = l;
                    if (lit) {
                        Value v = evaluate_shard_key_value(lit);
                        if (value_is_routable(v)) out.push_back(v);
                    }
                }
                return;
            }
            if (op.len == 3 &&
                (op.ptr[0] == 'A' || op.ptr[0] == 'a') &&
                (op.ptr[1] == 'N' || op.ptr[1] == 'n') &&
                (op.ptr[2] == 'D' || op.ptr[2] == 'd')) {
                collect_key_eq_values(expr->first_child, key, out);
                if (expr->first_child)
                    collect_key_eq_values(expr->first_child->next_sibling, key, out);
            }
            return;
        }
        if (expr->type == sql_parser::NodeType::NODE_IN_LIST) {
            const sql_parser::AstNode* col = expr->first_child;
            if (col && is_shard_key_ref(col, key)) {
                for (const sql_parser::AstNode* item = col->next_sibling; item;
                     item = item->next_sibling) {
                    if (!is_literal(item)) {
                        out.clear();
                        return;
                    }
                    Value v = evaluate_shard_key_value(item);
                    if (value_is_routable(v)) out.push_back(v);
                }
            }
        }
    }

    void extract_composite_targets(const sql_parser::AstNode* where_expr,
                                   const std::vector<std::string>& keys,
                                   sql_parser::StringRef table_name,
                                   std::vector<size_t>& target_indices) {
        std::vector<std::vector<Value>> dims(keys.size());
        for (size_t k = 0; k < keys.size(); ++k) {
            sql_parser::StringRef kn{keys[k].c_str(),
                static_cast<uint32_t>(keys[k].size())};
            collect_key_eq_values(where_expr, kn, dims[k]);
            if (dims[k].empty()) return;
        }
        std::vector<size_t> cursor(keys.size(), 0);
        for (;;) {
            std::vector<ShardKeyPart> parts(keys.size());
            for (size_t k = 0; k < keys.size(); ++k)
                parts[k] = value_to_part(dims[k][cursor[k]]);
            size_t idx = 0;
            if (shards_.try_shard_index_for_parts(table_name, parts.data(),
                                                  parts.size(), idx))
                target_indices.push_back(idx);
            size_t d = keys.size();
            while (d-- > 0) {
                if (++cursor[d] < dims[d].size()) break;
                cursor[d] = 0;
            }
            if (d == static_cast<size_t>(-1)) break;
        }
    }

    PlanNode* append_remote(PlanNode* current, PlanNode* next) {
        if (!next) return current;
        if (!current) return next;
        PlanNode* union_node = make_plan_node(arena_, PlanNodeType::SET_OP);
        union_node->set_op.op = SET_OP_UNION;
        union_node->set_op.all = true;
        union_node->left = current;
        union_node->right = next;
        return union_node;
    }

    Value copy_value_arena(const Value& v) {
        if ((v.tag == Value::TAG_STRING || v.tag == Value::TAG_DECIMAL ||
             v.tag == Value::TAG_BYTES || v.tag == Value::TAG_JSON) &&
            v.str_val.ptr && v.str_val.len > 0) {
            char* p = static_cast<char*>(arena_.allocate(v.str_val.len));
            std::memcpy(p, v.str_val.ptr, v.str_val.len);
            Value out = v;
            out.str_val = sql_parser::StringRef{p, v.str_val.len};
            return out;
        }
        return v;
    }

    Row copy_row_arena(const Row& src) {
        Row dst = make_row(arena_, src.column_count);
        for (uint16_t i = 0; i < src.column_count; ++i)
            dst.set(i, copy_value_arena(src.get(i)));
        return dst;
    }

    bool route_row_keys(const TableInfo* table, const Row& row, size_t& idx) const {
        const auto& keys = shards_.get_shard_keys(table->table_name);
        if (keys.empty()) return false;
        std::vector<ShardKeyPart> parts;
        parts.reserve(keys.size());
        for (const auto& k : keys) {
            sql_parser::StringRef kn{k.c_str(), static_cast<uint32_t>(k.size())};
            const ColumnInfo* col = catalog_.get_column(table, kn);
            if (!col || col->ordinal >= row.column_count) return false;
            Value v = row.get(col->ordinal);
            if (!value_is_routable(v)) return false;
            parts.push_back(value_to_part(v));
        }
        return shards_.try_shard_index_for_parts(
            table->table_name, parts.data(), parts.size(), idx);
    }

    Row apply_update_set(const Row& src, const TableInfo* table,
                         const sql_parser::AstNode** set_cols,
                         const sql_parser::AstNode** set_exprs,
                         uint16_t set_count) {
        Row dst = copy_row_arena(src);
        auto resolve = make_resolver(catalog_, table, src.values);
        for (uint16_t i = 0; i < set_count; ++i) {
            if (!set_cols[i]) continue;
            const ColumnInfo* col = catalog_.get_column(table, set_cols[i]->value());
            if (!col) continue;
            Value nv = value_null();
            if (functions_) {
                nv = evaluate_expression<D>(set_exprs[i], resolve, *functions_, arena_);
            } else {
                nv = evaluate_shard_key_value(set_exprs[i]);
            }
            dst.set(col->ordinal, copy_value_arena(nv));
        }
        return dst;
    }

    sql_parser::StringRef build_identity_pred(const TableInfo* table, const Row& row,
                                              sql_parser::StringBuilder& sb) {
        for (uint16_t i = 0; i < table->column_count && i < row.column_count; ++i) {
            if (i > 0) sb.append(" AND ");
            sb.append(table->columns[i].name.ptr, table->columns[i].name.len);
            if (row.get(i).is_null()) {
                sb.append(" IS NULL", 8);
            } else {
                sb.append(" = ");
                emit_value(row.get(i), sb);
            }
        }
        return sb.finish();
    }

    sql_parser::StringRef build_delete_identity(const TableInfo* table, const Row& row) {
        sql_parser::StringBuilder sb(arena_, 256);
        sb.append("DELETE FROM ");
        sb.append(table->table_name.ptr, table->table_name.len);
        sb.append(" WHERE ");
        return build_identity_pred(table, row, sb);
    }

    sql_parser::StringRef build_update_identity(const TableInfo* table,
                                                const sql_parser::AstNode** set_cols,
                                                const sql_parser::AstNode** set_exprs,
                                                uint16_t set_count,
                                                const Row& old_row) {
        sql_parser::StringBuilder sb(arena_, 256);
        sb.append("UPDATE ");
        sb.append(table->table_name.ptr, table->table_name.len);
        sb.append(" SET ");
        for (uint16_t i = 0; i < set_count; ++i) {
            if (i > 0) sb.append(", ");
            if (set_cols[i]) {
                sql_parser::StringRef cn = set_cols[i]->value();
                sb.append(cn.ptr, cn.len);
            }
            sb.append(" = ");
            if (set_exprs[i]) {
                sql_parser::Emitter<D> emitter(arena_);
                emitter.emit(set_exprs[i]);
                sql_parser::StringRef ev = emitter.result();
                sb.append(ev.ptr, ev.len);
            }
        }
        sb.append(" WHERE ");
        return build_identity_pred(table, old_row, sb);
    }

    PlanNode* distribute_update_move(PlanNode* plan, const TableInfo* table,
                                     const sql_parser::AstNode* where_expr) {
        if (!remote_executor_)
            return fail_dml("cannot UPDATE shard key without a remote executor");

        const auto& up = plan->update_plan;
        const auto& shard_list = shards_.get_shards(table->table_name);
        std::vector<ShardInfo> pruned = prune_shards(table, where_expr, shard_list);
        if (pruned.empty()) return plan;

        struct Move {
            size_t src = 0;
            size_t dst = 0;
            Row old_row{};
            Row new_row{};
        };
        std::vector<Move> moves;

        for (const auto& shard : pruned) {
            size_t src = 0;
            for (size_t i = 0; i < shard_list.size(); ++i) {
                if (shard_list[i].backend_name == shard.backend_name) {
                    src = i;
                    break;
                }
            }
            sql_parser::StringRef sql = qb_.build_select(
                table, where_expr, nullptr, 0, nullptr, 0,
                nullptr, nullptr, 0, -1, false);
            ResultSet rs = remote_executor_->execute(shard.backend_name.c_str(), sql);
            for (const auto& row : rs.rows) {
                Move m;
                m.src = src;
                m.old_row = copy_row_arena(row);
                m.new_row = apply_update_set(m.old_row, table, up.set_columns,
                                             up.set_exprs, up.set_count);
                if (!route_row_keys(table, m.new_row, m.dst))
                    return fail_dml("cannot UPDATE shard key: new key is not routable");
                moves.push_back(m);
            }
        }

        if (moves.empty()) {
            sql_parser::StringRef sql = qb_.build_update(
                table, up.set_columns, up.set_exprs, up.set_count, where_expr);
            return make_remote_scan(pruned[0].backend_name.c_str(), sql, table);
        }

        bool any_move = false;
        for (const auto& m : moves) {
            if (m.src != m.dst) { any_move = true; break; }
        }
        if (!any_move) {
            if (pruned.size() == 1) {
                sql_parser::StringRef sql = qb_.build_update(
                    table, up.set_columns, up.set_exprs, up.set_count, where_expr);
                return make_remote_scan(pruned[0].backend_name.c_str(), sql, table);
            }
            const sql_parser::AstNode* final_where = where_expr;
            return scatter_dml_to_shards(table, pruned, [&]() {
                return qb_.build_update(
                    table, up.set_columns, up.set_exprs, up.set_count, final_where);
            });
        }

        PlanNode* current = nullptr;
        std::unordered_map<size_t, std::vector<Row>> inserts;
        for (const auto& m : moves) {
            if (m.src == m.dst) {
                sql_parser::StringRef sql = build_update_identity(
                    table, up.set_columns, up.set_exprs, up.set_count, m.old_row);
                current = append_remote(
                    current, make_remote_scan(shard_list[m.src].backend_name.c_str(),
                                              sql, table));
            } else {
                current = append_remote(
                    current,
                    make_remote_scan(shard_list[m.src].backend_name.c_str(),
                                     build_delete_identity(table, m.old_row), table));
                inserts[m.dst].push_back(m.new_row);
            }
        }
        for (auto& kv : inserts) {
            current = append_remote(
                current,
                make_remote_scan(shard_list[kv.first].backend_name.c_str(),
                                 build_insert_from_rows(table, nullptr, 0, kv.second),
                                 table));
        }
        return current ? current : plan;
    }

    bool is_column_ref(const sql_parser::AstNode* node, sql_parser::StringRef col_name) const {
        if (!node) return false;
        if (node->type == sql_parser::NodeType::NODE_COLUMN_REF ||
            node->type == sql_parser::NodeType::NODE_IDENTIFIER) {
            return node->value().equals_ci(col_name.ptr, col_name.len);
        }
        return false;
    }

    void collect_ast_table_names(const sql_parser::AstNode* n,
                                 std::vector<sql_parser::StringRef>& out) const {
        if (!n) return;
        if (n->type == sql_parser::NodeType::NODE_TABLE_REF && n->first_child) {
            const sql_parser::AstNode* name = n->first_child;
            if (name->type == sql_parser::NodeType::NODE_IDENTIFIER) {
                out.push_back(name->value());
            } else if (name->type == sql_parser::NodeType::NODE_QUALIFIED_NAME) {
                const sql_parser::AstNode* schema = name->first_child;
                const sql_parser::AstNode* table = schema ? schema->next_sibling : nullptr;
                if (table) out.push_back(table->value());
                else if (schema) out.push_back(schema->value());
            }
        }
        for (const sql_parser::AstNode* c = n->first_child; c; c = c->next_sibling) {
            collect_ast_table_names(c, out);
        }
    }

    PlanNode* distribute_multi_table_dml(const sql_parser::AstNode* ast,
                                         const TableInfo* primary,
                                         bool is_update) {
        std::vector<sql_parser::StringRef> names;
        collect_ast_table_names(ast, names);
        const char* backend = nullptr;
        bool saw_mapped = false;
        for (sql_parser::StringRef name : names) {
            if (!shards_.has_table(name)) continue;
            saw_mapped = true;
            if (shards_.is_sharded(name)) {
                return fail_dml(is_update
                    ? "multi-table UPDATE is not supported on sharded tables"
                    : "multi-table DELETE is not supported on sharded tables");
            }
            const char* b = shards_.get_backend(name);
            if (backend && b && std::strcmp(backend, b) != 0) {
                return fail_dml(is_update
                    ? "multi-table UPDATE spans multiple backends"
                    : "multi-table DELETE spans multiple backends");
            }
            if (b) backend = b;
        }
        if (!backend && primary && shards_.has_table(primary->table_name)) {
            if (shards_.is_sharded(primary->table_name)) {
                return fail_dml(is_update
                    ? "multi-table UPDATE is not supported on sharded tables"
                    : "multi-table DELETE is not supported on sharded tables");
            }
            backend = shards_.get_backend(primary->table_name);
            saw_mapped = true;
        }
        if (!backend || !saw_mapped) {
            return fail_dml(is_update
                ? "multi-table UPDATE is not supported on sharded tables"
                : "multi-table DELETE is not supported on sharded tables");
        }
        sql_parser::StringRef sql = is_update
            ? qb_.build_update_from_ast(ast)
            : qb_.build_delete_from_ast(ast);
        return make_remote_scan(backend, sql, primary);
    }

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

    // ---- Cross-shard subquery helpers ----

    // Walk an AST expression tree looking for NODE_SUBQUERY nodes.
    bool has_subquery(const sql_parser::AstNode* expr) {
        if (!expr) return false;
        if (expr->type == sql_parser::NodeType::NODE_SUBQUERY) return true;
        if (has_subquery(expr->first_child)) return true;
        return has_subquery(expr->next_sibling);
    }

    // Find the table name referenced in a subquery's FROM clause.
    // Returns the table name StringRef, or empty if not found.
    sql_parser::StringRef find_subquery_table(const sql_parser::AstNode* subquery_node) {
        if (!subquery_node || subquery_node->type != sql_parser::NodeType::NODE_SUBQUERY)
            return {nullptr, 0};
        const sql_parser::AstNode* select_ast = subquery_node->first_child;
        if (!select_ast) return {nullptr, 0};
        // Walk select AST children to find FROM clause -> TABLE_REF
        for (const sql_parser::AstNode* c = select_ast->first_child; c; c = c->next_sibling) {
            if (c->type == sql_parser::NodeType::NODE_FROM_CLAUSE) {
                for (const sql_parser::AstNode* t = c->first_child; t; t = t->next_sibling) {
                    if (t->type == sql_parser::NodeType::NODE_TABLE_REF) {
                        const sql_parser::AstNode* name = t->first_child;
                        if (name && name->type == sql_parser::NodeType::NODE_IDENTIFIER) {
                            return name->value();
                        }
                    }
                }
            }
        }
        return {nullptr, 0};
    }

    // Check if a subquery references a table on a different backend than the DML target.
    bool is_cross_shard_subquery(const sql_parser::AstNode* subquery_node,
                                  const TableInfo* dml_table) {
        sql_parser::StringRef sub_table = find_subquery_table(subquery_node);
        if (!sub_table.ptr || !dml_table) return false;
        if (!shards_.has_table(sub_table)) return false;
        // Compare backends
        const char* dml_backend = shards_.get_backend(dml_table->table_name);
        const char* sub_backend = shards_.get_backend(sub_table);
        if (!dml_backend || !sub_backend) return false;
        return std::strcmp(dml_backend, sub_backend) != 0;
    }

    // Find a NODE_SUBQUERY within an expression tree (first occurrence).
    const sql_parser::AstNode* find_subquery_in_expr(const sql_parser::AstNode* expr) {
        if (!expr) return nullptr;
        if (expr->type == sql_parser::NodeType::NODE_SUBQUERY) return expr;
        const sql_parser::AstNode* found = find_subquery_in_expr(expr->first_child);
        if (found) return found;
        return find_subquery_in_expr(expr->next_sibling);
    }

    // Execute a subquery against its backend(s) and collect result values.
    // Returns values from the first column.
    std::vector<Value> materialize_subquery(const sql_parser::AstNode* subquery_node) {
        std::vector<Value> result;
        if (!remote_executor_ || !subquery_node) return result;

        const sql_parser::AstNode* select_ast = subquery_node->first_child;
        if (!select_ast) return result;

        // Build the SELECT SQL from the subquery AST
        sql_parser::StringRef sub_table_name = find_subquery_table(subquery_node);
        if (!sub_table_name.ptr) return result;

        // Build plan and generate SQL for the subquery
        PlanBuilder<D> builder(catalog_, arena_);
        PlanNode* plan = builder.build(select_ast);
        if (!plan) return result;

        // Distribute the subquery plan
        PlanNode* dist_plan = distribute_node(plan);

        // Execute: if it's a RemoteScan, execute via remote executor
        // Otherwise, need to execute locally
        ResultSet rs = execute_distributed_plan(dist_plan);

        for (const auto& row : rs.rows) {
            if (row.column_count > 0) {
                result.push_back(row.get(0));
            }
        }
        return result;
    }

    // Execute a distributed plan tree (recursively handles SET_OP / REMOTE_SCAN).
    ResultSet execute_distributed_plan(PlanNode* node) {
        if (!node || !remote_executor_) return {};

        if (node->type == PlanNodeType::REMOTE_SCAN) {
            sql_parser::StringRef sql{node->remote_scan.remote_sql,
                                       node->remote_scan.remote_sql_len};
            return remote_executor_->execute(node->remote_scan.backend_name, sql);
        }

        if (node->type == PlanNodeType::SET_OP) {
            // UNION ALL: concatenate results
            ResultSet left = execute_distributed_plan(node->left);
            ResultSet right = execute_distributed_plan(node->right);
            for (auto& row : right.rows) {
                left.rows.push_back(row);
            }
            if (!left.rows.empty()) {
                left.column_count = left.rows[0].column_count;
            }
            return left;
        }

        // For other node types that we can't execute remotely,
        // fall back to local execution if we have functions_
        if (functions_) {
            PlanExecutor<D> executor(*functions_, catalog_, arena_);
            executor.set_remote_executor(remote_executor_);
            return executor.execute(node);
        }
        return {};
    }

    // Build an IN-list expression with literal values, replacing a subquery.
    // Returns a new WHERE expression: "col IN (v1, v2, v3)"
    // where col is the left-hand side of the original IN expression.
    sql_parser::AstNode* build_in_list_from_values(
            const sql_parser::AstNode* original_in_list,
            const std::vector<Value>& values) {
        if (!original_in_list || values.empty()) return nullptr;

        // Clone the IN_LIST node structure:
        // NODE_IN_LIST: first_child = expr, then siblings = value items
        // We replace the NODE_SUBQUERY sibling with literal nodes.
        sql_parser::AstNode* new_in = sql_parser::make_node(
            arena_, sql_parser::NodeType::NODE_IN_LIST,
            original_in_list->value(), original_in_list->flags);

        // Copy the left-hand expression (first child)
        const sql_parser::AstNode* lhs = original_in_list->first_child;
        if (lhs) {
            sql_parser::AstNode* lhs_copy = sql_parser::make_node(
                arena_, lhs->type, lhs->value(), lhs->flags);
            lhs_copy->first_child = lhs->first_child;
            new_in->add_child(lhs_copy);
        }

        // Add literal value nodes for each materialized value
        for (const auto& v : values) {
            sql_parser::AstNode* lit = nullptr;
            if (v.tag == Value::TAG_INT64) {
                char buf[32];
                int n = snprintf(buf, sizeof(buf), "%lld", (long long)v.int_val);
                char* s = static_cast<char*>(arena_.allocate(n));
                std::memcpy(s, buf, n);
                lit = sql_parser::make_node(arena_, sql_parser::NodeType::NODE_LITERAL_INT,
                                             sql_parser::StringRef{s, static_cast<uint32_t>(n)});
            } else if (v.tag == Value::TAG_STRING && v.str_val.ptr) {
                lit = sql_parser::make_node(arena_, sql_parser::NodeType::NODE_LITERAL_STRING,
                                             v.str_val);
            } else if (v.tag == Value::TAG_DOUBLE) {
                char buf[64];
                int n = snprintf(buf, sizeof(buf), "%g", v.double_val);
                char* s = static_cast<char*>(arena_.allocate(n));
                std::memcpy(s, buf, n);
                lit = sql_parser::make_node(arena_, sql_parser::NodeType::NODE_LITERAL_FLOAT,
                                             sql_parser::StringRef{s, static_cast<uint32_t>(n)});
            } else {
                // NULL or unsupported type
                lit = sql_parser::make_node(arena_, sql_parser::NodeType::NODE_LITERAL_NULL,
                                             sql_parser::StringRef{nullptr, 0});
            }
            if (lit) new_in->add_child(lit);
        }

        return new_in;
    }

    // Rewrite a WHERE expression by replacing the first IN (subquery) with IN (literals).
    // Returns the rewritten expression, or the original if no rewrite needed.
    const sql_parser::AstNode* rewrite_where_subquery(
            const sql_parser::AstNode* where_expr,
            const TableInfo* dml_table) {
        if (!where_expr || !remote_executor_) return where_expr;

        // Check for IN_LIST with a subquery child
        if (where_expr->type == sql_parser::NodeType::NODE_IN_LIST) {
            // Check if any child is a subquery
            for (const sql_parser::AstNode* c = where_expr->first_child; c; c = c->next_sibling) {
                if (c->type == sql_parser::NodeType::NODE_SUBQUERY) {
                    // Materialize the subquery
                    std::vector<Value> values = materialize_subquery(c);
                    if (!values.empty()) {
                        return build_in_list_from_values(where_expr, values);
                    }
                    return where_expr;
                }
            }
        }

        // Check for NOT (IN (subquery)) -- NODE_UNARY_OP "NOT" -> NODE_IN_LIST
        if (where_expr->type == sql_parser::NodeType::NODE_UNARY_OP) {
            sql_parser::StringRef op = where_expr->value();
            if (op.equals_ci("NOT", 3) && where_expr->first_child) {
                const sql_parser::AstNode* inner = rewrite_where_subquery(
                    where_expr->first_child, dml_table);
                if (inner != where_expr->first_child) {
                    sql_parser::AstNode* new_not = sql_parser::make_node(
                        arena_, sql_parser::NodeType::NODE_UNARY_OP, op, where_expr->flags);
                    new_not->add_child(const_cast<sql_parser::AstNode*>(inner));
                    return new_not;
                }
            }
        }

        // Check for binary operators (AND, OR, comparisons with subquery)
        if (where_expr->type == sql_parser::NodeType::NODE_BINARY_OP) {
            const sql_parser::AstNode* left = where_expr->first_child;
            const sql_parser::AstNode* right = left ? left->next_sibling : nullptr;

            bool left_changed = false, right_changed = false;
            const sql_parser::AstNode* new_left = left;
            const sql_parser::AstNode* new_right = right;

            if (left && has_subquery(left)) {
                new_left = rewrite_where_subquery(left, dml_table);
                left_changed = (new_left != left);
            }
            if (right && has_subquery(right)) {
                new_right = rewrite_where_subquery(right, dml_table);
                right_changed = (new_right != right);
            }

            // Handle scalar subquery in comparison (e.g., col > (SELECT ...))
            if (right && right->type == sql_parser::NodeType::NODE_SUBQUERY) {
                std::vector<Value> values = materialize_subquery(right);
                if (!values.empty()) {
                    // Scalar: use first value
                    const Value& v = values[0];
                    sql_parser::AstNode* lit = nullptr;
                    if (v.tag == Value::TAG_INT64) {
                        char buf[32];
                        int n = snprintf(buf, sizeof(buf), "%lld", (long long)v.int_val);
                        char* s = static_cast<char*>(arena_.allocate(n));
                        std::memcpy(s, buf, n);
                        lit = sql_parser::make_node(arena_, sql_parser::NodeType::NODE_LITERAL_INT,
                                                     sql_parser::StringRef{s, static_cast<uint32_t>(n)});
                    } else if (v.tag == Value::TAG_DOUBLE) {
                        char buf[64];
                        int n = snprintf(buf, sizeof(buf), "%g", v.double_val);
                        char* s = static_cast<char*>(arena_.allocate(n));
                        std::memcpy(s, buf, n);
                        lit = sql_parser::make_node(arena_, sql_parser::NodeType::NODE_LITERAL_FLOAT,
                                                     sql_parser::StringRef{s, static_cast<uint32_t>(n)});
                    }
                    if (lit) {
                        new_right = lit;
                        right_changed = true;
                    }
                }
            }

            if (left_changed || right_changed) {
                sql_parser::AstNode* new_binop = sql_parser::make_node(
                    arena_, sql_parser::NodeType::NODE_BINARY_OP,
                    where_expr->value(), where_expr->flags);
                new_binop->add_child(const_cast<sql_parser::AstNode*>(new_left));
                new_binop->add_child(const_cast<sql_parser::AstNode*>(new_right));
                return new_binop;
            }
        }

        return where_expr;
    }

    // ---- INSERT ... SELECT distributed ----

    // Build a distributed INSERT ... SELECT plan.
    // 1. Execute the SELECT part distributedly
    // 2. For each result row, determine the target shard
    // 3. Group rows by shard and generate per-shard INSERT statements
    PlanNode* distribute_insert_select(PlanNode* plan, const sql_parser::AstNode* select_ast) {
        if (!remote_executor_ || !functions_ || !select_ast) return plan;

        const auto& ip = plan->insert_plan;
        const TableInfo* table = ip.table;
        if (!table) return plan;

        // Build and distribute the SELECT plan
        PlanBuilder<D> builder(catalog_, arena_);
        PlanNode* select_plan = builder.build(select_ast);
        if (!select_plan) return plan;

        PlanNode* dist_select = distribute_node(select_plan);
        ResultSet rs = execute_distributed_plan(dist_select);

        if (rs.rows.empty()) {
            // No rows to insert -- return a no-op
            // Just return the original plan (which will do nothing since select_source is null)
            return plan;
        }

        // Determine target shards for each row
        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");

        if (!shards_.is_sharded(table->table_name)) {
            // Unsharded: build a single INSERT with all rows
            sql_parser::StringRef sql = build_insert_from_rows(table, ip.columns,
                                                                 ip.column_count, rs.rows);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
        }

        const auto& key_names = shards_.get_shard_keys(table->table_name);
        if (key_names.empty()) return plan;

        std::vector<int> key_ords(key_names.size(), -1);
        for (size_t k = 0; k < key_names.size(); ++k) {
            sql_parser::StringRef kn{key_names[k].c_str(),
                static_cast<uint32_t>(key_names[k].size())};
            key_ords[k] = find_insert_key_ordinal(ip.columns, ip.column_count, table, kn);
        }
        for (int ord : key_ords) {
            if (ord < 0)
                return fail_dml("cannot route INSERT ... SELECT: shard key column is not present");
        }

        const auto& shard_list = shards_.get_shards(table->table_name);

        std::unordered_map<size_t, std::vector<size_t>> shard_rows;
        for (size_t ri = 0; ri < rs.rows.size(); ++ri) {
            std::vector<ShardKeyPart> parts;
            parts.reserve(key_ords.size());
            for (int ord : key_ords) {
                if (ord >= rs.rows[ri].column_count)
                    return fail_dml("cannot route INSERT ... SELECT: missing shard key value");
                Value v = rs.rows[ri].get(static_cast<uint16_t>(ord));
                if (!value_is_routable(v))
                    return fail_dml("cannot route INSERT ... SELECT: shard key is not a literal");
                parts.push_back(value_to_part(v));
            }
            size_t shard_idx = 0;
            if (!shards_.try_shard_index_for_parts(table->table_name, parts.data(),
                                                   parts.size(), shard_idx)) {
                return fail_dml("cannot route INSERT ... SELECT: shard key is not a literal");
            }
            shard_rows[shard_idx].push_back(ri);
        }

        // Generate per-shard INSERT statements
        PlanNode* current = nullptr;
        for (auto& [shard_idx, row_indices] : shard_rows) {
            std::vector<Row> subset;
            for (size_t idx : row_indices) {
                subset.push_back(rs.rows[idx]);
            }
            sql_parser::StringRef sql = build_insert_from_rows(table, ip.columns,
                                                                 ip.column_count, subset);
            PlanNode* node = make_remote_scan(shard_list[shard_idx].backend_name.c_str(), sql, table);
            if (!current) {
                current = node;
            } else {
                PlanNode* union_node = make_plan_node(arena_, PlanNodeType::SET_OP);
                union_node->set_op.op = SET_OP_UNION;
                union_node->set_op.all = true;
                union_node->left = current;
                union_node->right = node;
                current = union_node;
            }
        }

        return current ? current : plan;
    }

    // Build INSERT SQL from materialized rows (Value-based).
    sql_parser::StringRef build_insert_from_rows(
            const TableInfo* table,
            const sql_parser::AstNode** columns,
            uint16_t col_count,
            const std::vector<Row>& rows) {
        sql_parser::StringBuilder sb(arena_, 512);
        sb.append("INSERT INTO ");
        if (table) {
            sb.append(table->table_name.ptr, table->table_name.len);
        }

        // Column list
        if (columns && col_count > 0) {
            sb.append(" (");
            for (uint16_t i = 0; i < col_count; ++i) {
                if (i > 0) sb.append(", ");
                if (columns[i]) {
                    sql_parser::StringRef cn = columns[i]->value();
                    sb.append(cn.ptr, cn.len);
                }
            }
            sb.append_char(')');
        } else if (table && table->column_count > 0) {
            sb.append(" (");
            for (uint16_t i = 0; i < table->column_count; ++i) {
                if (i > 0) sb.append(", ");
                sb.append(table->columns[i].name.ptr, table->columns[i].name.len);
            }
            sb.append_char(')');
        }

        sb.append(" VALUES ");
        for (size_t ri = 0; ri < rows.size(); ++ri) {
            if (ri > 0) sb.append(", ");
            sb.append_char('(');
            const Row& row = rows[ri];
            for (uint16_t ci = 0; ci < row.column_count; ++ci) {
                if (ci > 0) sb.append(", ");
                emit_value(row.get(ci), sb);
            }
            sb.append_char(')');
        }

        return sb.finish();
    }

    // Emit a Value as SQL literal text
    void emit_value(const Value& v, sql_parser::StringBuilder& sb) {
        if (v.is_null()) {
            sb.append("NULL", 4);
        } else if (v.tag == Value::TAG_INT64) {
            char buf[32];
            int n = snprintf(buf, sizeof(buf), "%lld", (long long)v.int_val);
            sb.append(buf, n);
        } else if (v.tag == Value::TAG_DOUBLE) {
            char buf[64];
            int n = snprintf(buf, sizeof(buf), "%g", v.double_val);
            sb.append(buf, n);
        } else if (v.tag == Value::TAG_STRING && v.str_val.ptr) {
            sb.append_char('\'');
            // Simple escaping: double any single quotes
            for (uint32_t i = 0; i < v.str_val.len; ++i) {
                char c = v.str_val.ptr[i];
                if (c == '\'') sb.append_char('\'');
                sb.append_char(c);
            }
            sb.append_char('\'');
        } else if (v.tag == Value::TAG_BOOL) {
            if (v.bool_val) sb.append("TRUE", 4);
            else sb.append("FALSE", 5);
        } else {
            sb.append("NULL", 4);
        }
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
        if (!shards_.has_table(table->table_name))
            return fail_dml("table not in shard map");
        if (!shards_.is_sharded(table->table_name)) {
            sql_parser::StringRef sql = qb_.build_select(
                table, ctx.where_expr, proj_exprs, proj_count,
                nullptr, 0, nullptr, nullptr, 0, -1, true);
            return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
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
