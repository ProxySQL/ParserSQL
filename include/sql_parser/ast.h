#ifndef SQL_PARSER_AST_H
#define SQL_PARSER_AST_H

#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include "sql_parser/token.h"
#include <cstdint>
#include <type_traits>

namespace sql_parser {

struct AstNode {
    AstNode* first_child;
    AstNode* next_sibling;
    const char* value_ptr;
    const char* source_ptr;
    uint32_t value_len;
    uint32_t source_len;
    NodeType type;
    uint16_t flags;

    StringRef value() const { return StringRef{value_ptr, value_len}; }
    StringRef source() const { return StringRef{source_ptr, source_len}; }

    void set_value(StringRef ref) {
        value_ptr = ref.ptr;
        value_len = ref.len;
    }

    void set_source(StringRef ref) {
        source_ptr = ref.ptr;
        source_len = ref.len;
    }

    void add_child(AstNode* child) {
        if (!child) return;
        if (!first_child) {
            first_child = child;
            return;
        }
        AstNode* last = first_child;
        while (last->next_sibling) last = last->next_sibling;
        last->next_sibling = child;
    }
};
static_assert(sizeof(AstNode) == 48, "AstNode layout changed unexpectedly");
static_assert(std::is_trivially_copyable_v<AstNode>);

inline AstNode* make_node(Arena& arena, NodeType type, StringRef value = {},
                          uint16_t flags = 0) {
    AstNode* node = arena.allocate_typed<AstNode>();
    if (!node) return nullptr;
    node->type = type;
    node->flags = flags;
    node->value_ptr = value.ptr;
    node->value_len = value.len;
    return node;
}

inline AstNode* make_node_from_token(Arena& arena, NodeType type,
                                     const Token& token, uint16_t flags = 0) {
    AstNode* node = make_node(arena, type, token.text, flags);
    if (node) node->set_source(token.source);
    return node;
}

} // namespace sql_parser

#endif // SQL_PARSER_AST_H
