// projection_pruning.h — Annotate which columns are needed at each plan node.
//
// Initial implementation: if a Project immediately above a chain of
// Filter/Sort/Scan requests fewer columns than the Scan produces, this is
// already handled by the Project operator. Projection pruning is most valuable
// for longer chains, but requires changing Row layout per operator — deferred
// to a future iteration.
//
// For now this rule is a no-op pass that preserves correctness.

#ifndef SQL_ENGINE_RULES_PROJECTION_PRUNING_H
#define SQL_ENGINE_RULES_PROJECTION_PRUNING_H

#include "sql_engine/plan_node.h"
#include "sql_engine/catalog.h"
#include "sql_parser/arena.h"

namespace sql_engine {
namespace rules {

inline PlanNode* projection_pruning(PlanNode* node,
                                     const Catalog& /*catalog*/,
                                     sql_parser::Arena& /*arena*/) {
    // No-op for now. The Project operator already handles column subsetting.
    // Future: track needed columns and insert slimming Projects above Scans
    // in long chains (e.g., Project -> Sort -> Filter -> Scan).
    return node;
}

} // namespace rules
} // namespace sql_engine

#endif // SQL_ENGINE_RULES_PROJECTION_PRUNING_H
