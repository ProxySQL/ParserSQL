#ifndef SQL_PARSER_MYSQL_VALUE_SYNTAX_H
#define SQL_PARSER_MYSQL_VALUE_SYNTAX_H

#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/mysql_identifier.h"
#include <limits>

namespace sql_parser {

// DEFAULT is a complete row value, not an arbitrary expression operand.
// Qualified identifier components can themselves be reserved words.
inline bool mysql_value_expression(const AstNode* node, bool allow_default = false,
                                   bool allow_star = false) {
    if (node->type == NodeType::NODE_SUBQUERY) return true; // validated by its query parser
    if (node->type == NodeType::NODE_ASTERISK) return allow_star;
    if (node->type == NodeType::NODE_IDENTIFIER && !(node->flags & FLAG_IDENT_DELIMITED) &&
        node->value().equals_ci("DEFAULT", 7)) return allow_default;
    if (node->type == NodeType::NODE_QUALIFIED_NAME) {
        for (const auto* child = node->first_child; child; child = child->next_sibling)
            if (!(child->flags & FLAG_IDENT_DELIMITED) && child->value().equals_ci("*", 1)) return false;
        return true;
    }
    bool count = node->type == NodeType::NODE_FUNCTION_CALL && node->value().equals_ci("COUNT", 5);
    for (const auto* child = node->first_child; child; child = child->next_sibling)
        if (!mysql_value_expression(child, false, count)) return false;
    return true;
}

inline AstNode* mysql_limit_value(Tokenizer<Dialect::MySQL>& tok, Arena& arena) {
    Token token = tok.next_token();
    NodeType kind;
    uint16_t flags = 0;
    if (token.type == TokenType::TK_INTEGER) {
        uint64_t value = 0;
        for (uint32_t i = 0; i < token.text.len; ++i) {
            unsigned digit = static_cast<unsigned char>(token.text.ptr[i]) - '0';
            if (digit > 9 || value > (std::numeric_limits<uint64_t>::max() - digit) / 10)
                return nullptr;
            value = value * 10 + digit;
        }
        kind = NodeType::NODE_LITERAL_INT;
    } else if (token.type == TokenType::TK_QUESTION) kind = NodeType::NODE_PLACEHOLDER;
    else if (mysql_identifier_token(token)) {
        kind = NodeType::NODE_COLUMN_REF;
        if (token.source.ptr != token.text.ptr) flags |= FLAG_IDENT_DELIMITED;
    } else return nullptr;
    auto* value = make_node_from_token(arena, kind, token, flags);
    return value;
}

} // namespace sql_parser

#endif // SQL_PARSER_MYSQL_VALUE_SYNTAX_H
