// optimizer.h — Rule-based query optimizer
//
// Optimizer<D> applies four rewrite rules in sequence to transform a logical
// plan into a more efficient logical plan:
//   1. Predicate pushdown — push filters below joins
//   2. Projection pruning — annotate needed columns (no-op for now)
//   3. Constant folding — evaluate constant sub-expressions at plan time
//   4. Limit pushdown — push limits past filters toward scans

#ifndef SQL_ENGINE_OPTIMIZER_H
#define SQL_ENGINE_OPTIMIZER_H

#include "sql_engine/plan_node.h"
#include "sql_engine/catalog.h"
#include "sql_engine/function_registry.h"
#include "sql_engine/rules/predicate_pushdown.h"
#include "sql_engine/rules/projection_pruning.h"
#include "sql_engine/rules/constant_folding.h"
#include "sql_engine/rules/limit_pushdown.h"
#include "sql_parser/arena.h"
#include "sql_parser/common.h"

namespace sql_engine {

template <sql_parser::Dialect D>
class Optimizer {
public:
    Optimizer(const Catalog& catalog, FunctionRegistry<D>& functions)
        : catalog_(catalog), functions_(functions) {}

    PlanNode* optimize(PlanNode* plan, sql_parser::Arena& arena) {
        if (!plan) return nullptr;

        plan = rules::predicate_pushdown(plan, catalog_, arena);
        plan = rules::projection_pruning(plan, catalog_, arena);
        plan = rules::constant_folding<D>(plan, catalog_, functions_, arena);
        plan = rules::limit_pushdown(plan, catalog_, arena);

        return plan;
    }

private:
    const Catalog& catalog_;
    FunctionRegistry<D>& functions_;
};

} // namespace sql_engine

#endif // SQL_ENGINE_OPTIMIZER_H
