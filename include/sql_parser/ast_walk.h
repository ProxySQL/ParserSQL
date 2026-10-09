#ifndef SQL_PARSER_AST_WALK_H
#define SQL_PARSER_AST_WALK_H

#include "sql_parser/ast.h"
#include <cstddef>
#include <vector>

namespace sql_parser {

enum class AstVisitAction { Continue, SkipChildren, Stop };
enum class AstWalkStatus { Completed, Stopped, LimitExceeded };

struct AstWalkLimits {
    size_t max_nodes = 1000000;
    size_t max_depth = 4096;
};

struct AstVisitContext {
    const AstNode* parent;
    size_t depth;        // Root is depth zero.
    size_t child_index;  // Zero-based index within the parent's children.
};

struct AstWalkResult {
    AstWalkStatus status = AstWalkStatus::Completed;
    size_t visited = 0;
};

// Preorder over one subtree, excluding root->next_sibling. The visitor must
// not change links during traversal. Bounds also terminate cyclic input.
// Uses O(depth) temporary storage and never recurses on the C++ stack.
template <typename Visitor>
AstWalkResult walk_ast(const AstNode* root, Visitor&& visitor,
                       AstWalkLimits limits = {}) {
    struct Pending { const AstNode* node; AstVisitContext context; };
    std::vector<Pending> pending;
    if (root) pending.push_back({root, {nullptr, 0, 0}});
    AstWalkResult result;
    while (!pending.empty()) {
        const Pending current = pending.back();
        pending.pop_back();
        if (result.visited >= limits.max_nodes || current.context.depth > limits.max_depth) {
            result.status = AstWalkStatus::LimitExceeded;
            return result;
        }
        ++result.visited;
        const AstVisitAction action = visitor(*current.node, current.context);
        if (action == AstVisitAction::Stop) {
            result.status = AstWalkStatus::Stopped;
            return result;
        }
        if (current.context.parent && current.node->next_sibling) {
            pending.push_back({current.node->next_sibling,
                {current.context.parent, current.context.depth, current.context.child_index + 1}});
        }
        if (action != AstVisitAction::SkipChildren && current.node->first_child) {
            pending.push_back({current.node->first_child,
                {current.node, current.context.depth + 1, 0}});
        }
    }
    return result;
}

} // namespace sql_parser

#endif // SQL_PARSER_AST_WALK_H
