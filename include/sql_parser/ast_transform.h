#ifndef SQL_PARSER_AST_TRANSFORM_H
#define SQL_PARSER_AST_TRANSFORM_H

#include "sql_parser/ast_walk.h"
#include <unordered_set>

namespace sql_parser {

enum class AstError {
    None, LimitExceeded, AllocationFailure, InvalidTree, InvalidReplacement,
    NotFound, UnsupportedRoot, UnsupportedContext, InvalidPlaceholder, ParameterOverflow
};

struct AstCloneResult {
    AstNode* ast = nullptr;
    AstError error = AstError::None;
    bool ok() const { return error == AstError::None; }
};

// Unlike make_node(), copies value and source into the destination arena.
inline AstNode* make_owned_node(Arena& arena, NodeType type, StringRef value = {},
                                uint16_t flags = 0, StringRef source = {}) {
    if ((value.len && !value.ptr) || (source.len && !source.ptr)) return nullptr;
    StringRef owned_value;
    StringRef owned_source;
    if (value.len) {
        owned_value = arena.allocate_string(value.ptr, value.len);
        if (!owned_value.ptr) return nullptr;
    }
    if (source.len) {
        if (source.ptr == value.ptr && source.len == value.len) owned_source = owned_value;
        else owned_source = arena.allocate_string(source.ptr, source.len);
        if (!owned_source.ptr) return nullptr;
    }
    AstNode* node = make_node(arena, type, owned_value, flags);
    if (node) node->set_source(owned_source);
    return node;
}

// Copies exactly one subtree, including all text. On failure ast is null;
// partial allocations remain in the destination arena until its next reset.
inline AstCloneResult clone_ast(const AstNode* root, Arena& arena,
                                AstWalkLimits limits = {}) {
    AstCloneResult result;
    std::unordered_set<const AstNode*> seen;
    std::vector<AstNode*> ancestors;
    std::vector<AstNode*> last_child;
    auto walked = walk_ast(root, [&](const AstNode& node, const AstVisitContext& ctx) {
        if (!seen.insert(&node).second || (node.value_len && !node.value_ptr) ||
            (node.source_len && !node.source_ptr)) {
            result.error = AstError::InvalidTree;
            return AstVisitAction::Stop;
        }
        AstNode* copy = make_owned_node(arena, node.type, node.value(), node.flags, node.source());
        if (!copy) {
            result.error = AstError::AllocationFailure;
            return AstVisitAction::Stop;
        }
        ancestors.resize(ctx.depth + 1);
        last_child.resize(ctx.depth + 1);
        ancestors[ctx.depth] = copy;
        last_child[ctx.depth] = nullptr;
        if (!ctx.parent) result.ast = copy;
        else {
            if (last_child[ctx.depth - 1]) last_child[ctx.depth - 1]->next_sibling = copy;
            else ancestors[ctx.depth - 1]->first_child = copy;
            last_child[ctx.depth - 1] = copy;
        }
        return AstVisitAction::Continue;
    }, limits);
    if (walked.status == AstWalkStatus::LimitExceeded) result.error = AstError::LimitExceeded;
    if (!result.ok()) result.ast = nullptr;
    return result;
}

// Replaces/removes target within root, preserving its sibling position. A
// replacement must be a detached subtree, disjoint from the whole input tree.
// root must be a standalone tree (no next_sibling). Validation completes
// before any mutation; target is detached on success.
inline AstError replace_ast_subtree(AstNode*& root, AstNode* target,
                                    AstNode* replacement, AstWalkLimits limits = {}) {
    if (!target) return AstError::NotFound;
    if (root && root->next_sibling) return AstError::InvalidTree;
    if (replacement && replacement->next_sibling) return AstError::InvalidReplacement;
    std::unordered_set<const AstNode*> original;
    AstNode* parent = nullptr;
    bool found = false;
    AstError error = AstError::None;
    auto walked = walk_ast(root, [&](const AstNode& node, const AstVisitContext& ctx) {
        if (!original.insert(&node).second) {
            error = AstError::InvalidTree;
            return AstVisitAction::Stop;
        }
        if (&node == target) {
            parent = const_cast<AstNode*>(ctx.parent);
            found = true;
        }
        return AstVisitAction::Continue;
    }, limits);
    if (walked.status == AstWalkStatus::LimitExceeded) return AstError::LimitExceeded;
    if (error != AstError::None) return error;
    if (!found) return AstError::NotFound;
    std::unordered_set<const AstNode*> incoming;
    walked = walk_ast(replacement, [&](const AstNode& node, const AstVisitContext&) {
        if (original.count(&node) || !incoming.insert(&node).second) {
            error = AstError::InvalidReplacement;
            return AstVisitAction::Stop;
        }
        return AstVisitAction::Continue;
    }, limits);
    if (walked.status == AstWalkStatus::LimitExceeded) return AstError::LimitExceeded;
    if (error != AstError::None) return error;
    AstNode** slot = parent ? &parent->first_child : &root;
    while (*slot != target) slot = &(*slot)->next_sibling;
    if (replacement) replacement->next_sibling = target->next_sibling;
    *slot = replacement ? replacement : target->next_sibling;
    target->next_sibling = nullptr;
    return AstError::None;
}

} // namespace sql_parser

#endif // SQL_PARSER_AST_TRANSFORM_H
