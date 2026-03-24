#ifndef SQL_PARSER_EXPRESSION_PARSER_H
#define SQL_PARSER_EXPRESSION_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"

namespace sql_parser {

// Operator precedence levels for Pratt parsing
enum class Precedence : uint8_t {
    NONE = 0,
    OR,            // OR
    AND,           // AND
    NOT,           // NOT (prefix)
    COMPARISON,    // =, <, >, <=, >=, !=, <>, IS, LIKE, IN, BETWEEN
    ADDITION,      // +, -
    MULTIPLICATION,// *, /, %
    UNARY,         // - (prefix), NOT
    POSTFIX,       // IS NULL, IS NOT NULL
    CALL,          // function()
    PRIMARY,       // literals, identifiers
};

template <Dialect D>
class ExpressionParser {
public:
    ExpressionParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena) {}

    // Parse an expression with minimum precedence 0
    AstNode* parse(Precedence min_prec = Precedence::NONE) {
        AstNode* left = parse_atom();
        if (!left) return nullptr;

        while (true) {
            Precedence prec = infix_precedence(tok_.peek().type);
            if (prec <= min_prec) break;

            left = parse_infix(left, prec);
            if (!left) return nullptr;
        }

        return left;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;

    // Parse a primary expression (atom)
    AstNode* parse_atom() {
        Token t = tok_.peek();

        switch (t.type) {
            case TokenType::TK_INTEGER: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_INT, t.text);
            }
            case TokenType::TK_FLOAT: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_FLOAT, t.text);
            }
            case TokenType::TK_STRING: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_STRING, t.text);
            }
            case TokenType::TK_NULL: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_NULL, t.text);
            }
            case TokenType::TK_TRUE:
            case TokenType::TK_FALSE: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_INT, t.text);
            }
            case TokenType::TK_DEFAULT: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_IDENTIFIER, t.text);
            }
            case TokenType::TK_ASTERISK: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_ASTERISK, t.text);
            }
            case TokenType::TK_QUESTION: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_PLACEHOLDER, t.text);
            }
            case TokenType::TK_DOLLAR_NUM: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_PLACEHOLDER, t.text);
            }
            case TokenType::TK_AT: {
                // User variable: @name
                tok_.skip();
                Token name = tok_.next_token();
                // Build @name as a single COLUMN_REF with combined text
                // value_ptr points to @ in original input, len covers @name
                StringRef full{t.text.ptr,
                    static_cast<uint32_t>((name.text.ptr + name.text.len) - t.text.ptr)};
                return make_node(arena_, NodeType::NODE_COLUMN_REF, full);
            }
            case TokenType::TK_DOUBLE_AT: {
                // System variable: @@name or @@scope.name
                tok_.skip();
                Token name = tok_.next_token();
                StringRef full{t.text.ptr,
                    static_cast<uint32_t>((name.text.ptr + name.text.len) - t.text.ptr)};
                AstNode* node = make_node(arena_, NodeType::NODE_COLUMN_REF, full);
                // Check for @@scope.name
                if (tok_.peek().type == TokenType::TK_DOT) {
                    tok_.skip();
                    Token var_name = tok_.next_token();
                    full = StringRef{t.text.ptr,
                        static_cast<uint32_t>((var_name.text.ptr + var_name.text.len) - t.text.ptr)};
                    node->value_ptr = full.ptr;
                    node->value_len = full.len;
                }
                return node;
            }
            case TokenType::TK_MINUS: {
                // Unary minus
                tok_.skip();
                AstNode* operand = parse(Precedence::UNARY);
                if (!operand) return nullptr;
                AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text);
                node->add_child(operand);
                return node;
            }
            case TokenType::TK_PLUS: {
                // Unary plus
                tok_.skip();
                return parse(Precedence::UNARY);
            }
            case TokenType::TK_NOT: {
                tok_.skip();
                AstNode* operand = parse(Precedence::NOT);
                if (!operand) return nullptr;
                AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text);
                node->add_child(operand);
                return node;
            }
            case TokenType::TK_EXISTS: {
                tok_.skip();
                // EXISTS (subquery)
                AstNode* node = make_node(arena_, NodeType::NODE_SUBQUERY);
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    skip_to_matching_paren();
                }
                return node;
            }
            case TokenType::TK_CASE: {
                tok_.skip();
                return parse_case();
            }
            case TokenType::TK_LPAREN: {
                tok_.skip();
                // Could be subquery: (SELECT ...)
                if (tok_.peek().type == TokenType::TK_SELECT) {
                    // Subquery — for now, skip to matching paren
                    AstNode* node = make_node(arena_, NodeType::NODE_SUBQUERY);
                    skip_to_matching_paren();
                    return node;
                }
                AstNode* expr = parse();
                if (tok_.peek().type == TokenType::TK_RPAREN) {
                    tok_.skip();
                }
                return expr;
            }
            case TokenType::TK_IDENTIFIER: {
                tok_.skip();
                return parse_identifier_or_function(t);
            }
            // Keywords that can appear as identifiers in expression context
            // (e.g., column names that happen to be keywords)
            default: {
                if (is_keyword_as_identifier(t.type)) {
                    tok_.skip();
                    return parse_identifier_or_function(t);
                }
                return nullptr;  // not an expression
            }
        }
    }

    AstNode* parse_identifier_or_function(const Token& name_token) {
        // Check for function call: name(
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();  // consume (
            AstNode* func = make_node(arena_, NodeType::NODE_FUNCTION_CALL, name_token.text);
            // Parse argument list
            if (tok_.peek().type != TokenType::TK_RPAREN) {
                while (true) {
                    AstNode* arg = parse();
                    if (arg) func->add_child(arg);
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
            }
            if (tok_.peek().type == TokenType::TK_RPAREN) {
                tok_.skip();
            }
            return func;
        }

        // Check for qualified name: table.column
        if (tok_.peek().type == TokenType::TK_DOT) {
            tok_.skip();  // consume dot
            Token col = tok_.next_token();
            AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name_token.text));
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, col.text));
            return qname;
        }

        return make_node(arena_, NodeType::NODE_COLUMN_REF, name_token.text);
    }

    // Infix precedence for a token type.
    // Returns NONE if not an infix operator (stops the Pratt loop).
    static Precedence infix_precedence(TokenType type) {
        switch (type) {
            case TokenType::TK_OR:             return Precedence::OR;
            case TokenType::TK_AND:            return Precedence::AND;
            case TokenType::TK_NOT:            return Precedence::COMPARISON; // NOT IN/BETWEEN/LIKE
            case TokenType::TK_EQUAL:
            case TokenType::TK_NOT_EQUAL:
            case TokenType::TK_LESS:
            case TokenType::TK_GREATER:
            case TokenType::TK_LESS_EQUAL:
            case TokenType::TK_GREATER_EQUAL:
            case TokenType::TK_LIKE:           return Precedence::COMPARISON;
            case TokenType::TK_IS:             return Precedence::COMPARISON;
            case TokenType::TK_IN:             return Precedence::COMPARISON;
            case TokenType::TK_BETWEEN:        return Precedence::COMPARISON;
            case TokenType::TK_PLUS:
            case TokenType::TK_MINUS:          return Precedence::ADDITION;
            case TokenType::TK_ASTERISK:
            case TokenType::TK_SLASH:
            case TokenType::TK_PERCENT:        return Precedence::MULTIPLICATION;
            case TokenType::TK_DOUBLE_PIPE:    return Precedence::ADDITION; // string concat
            default:                           return Precedence::NONE;
        }
    }

    AstNode* parse_infix(AstNode* left, Precedence prec) {
        Token op = tok_.next_token();

        switch (op.type) {
            case TokenType::TK_NOT: {
                // NOT IN / NOT BETWEEN / NOT LIKE — compound negated infix
                Token actual_op = tok_.peek();
                if (actual_op.type == TokenType::TK_IN) {
                    tok_.skip();
                    AstNode* in_node = parse_in(left);
                    // Wrap in NOT
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(in_node);
                    return not_node;
                }
                if (actual_op.type == TokenType::TK_BETWEEN) {
                    tok_.skip();
                    AstNode* between_node = parse_between(left);
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(between_node);
                    return not_node;
                }
                if (actual_op.type == TokenType::TK_LIKE) {
                    tok_.skip();
                    AstNode* right = parse(prec);
                    AstNode* like_node = make_node(arena_, NodeType::NODE_BINARY_OP, actual_op.text);
                    like_node->add_child(left);
                    if (right) like_node->add_child(right);
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(like_node);
                    return not_node;
                }
                // Standalone NOT in infix position — shouldn't happen, return left
                return left;
            }
            case TokenType::TK_IS: {
                // IS [NOT] NULL
                bool is_not = false;
                if (tok_.peek().type == TokenType::TK_NOT) {
                    is_not = true;
                    tok_.skip();
                }
                if (tok_.peek().type == TokenType::TK_NULL) {
                    tok_.skip();
                    NodeType nt = is_not ? NodeType::NODE_IS_NOT_NULL : NodeType::NODE_IS_NULL;
                    AstNode* node = make_node(arena_, nt);
                    node->add_child(left);
                    return node;
                }
                // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE
                if (tok_.peek().type == TokenType::TK_TRUE || tok_.peek().type == TokenType::TK_FALSE) {
                    Token val = tok_.next_token();
                    AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP,
                        is_not ? StringRef{"IS NOT", 6} : StringRef{"IS", 2});
                    node->add_child(left);
                    node->add_child(make_node(arena_, NodeType::NODE_LITERAL_INT, val.text));
                    return node;
                }
                return left;
            }
            case TokenType::TK_IN:
                return parse_in(left);
            case TokenType::TK_BETWEEN:
                return parse_between(left);
            default: {
                // Standard binary operator
                AstNode* right = parse(prec);
                if (!right) return left;
                AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP, op.text);
                node->add_child(left);
                node->add_child(right);
                return node;
            }
        }
    }

    // IN (value_list) or IN (subquery)
    AstNode* parse_in(AstNode* left) {
        AstNode* node = make_node(arena_, NodeType::NODE_IN_LIST);
        node->add_child(left);
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SELECT) {
                AstNode* sq = make_node(arena_, NodeType::NODE_SUBQUERY);
                skip_to_matching_paren();
                node->add_child(sq);
            } else {
                while (true) {
                    AstNode* val = parse();
                    if (val) node->add_child(val);
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
                if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
            }
        }
        return node;
    }

    // BETWEEN low AND high
    AstNode* parse_between(AstNode* left) {
        AstNode* node = make_node(arena_, NodeType::NODE_BETWEEN);
        node->add_child(left);
        AstNode* low = parse(Precedence::COMPARISON);
        node->add_child(low);
        if (tok_.peek().type == TokenType::TK_AND) {
            tok_.skip();
        }
        AstNode* high = parse(Precedence::COMPARISON);
        node->add_child(high);
        return node;
    }

    // CASE [expr] WHEN ... THEN ... [ELSE ...] END
    AstNode* parse_case() {
        AstNode* node = make_node(arena_, NodeType::NODE_CASE_WHEN);
        // Optional simple CASE expression: CASE expr WHEN ...
        if (tok_.peek().type != TokenType::TK_WHEN) {
            AstNode* case_expr = parse();
            if (case_expr) node->add_child(case_expr);
        }
        // WHEN ... THEN ... pairs
        while (tok_.peek().type == TokenType::TK_WHEN) {
            tok_.skip();
            AstNode* when_expr = parse();
            if (when_expr) node->add_child(when_expr);
            if (tok_.peek().type == TokenType::TK_THEN) tok_.skip();
            AstNode* then_expr = parse();
            if (then_expr) node->add_child(then_expr);
        }
        // Optional ELSE
        if (tok_.peek().type == TokenType::TK_ELSE) {
            tok_.skip();
            AstNode* else_expr = parse();
            if (else_expr) node->add_child(else_expr);
        }
        // END
        if (tok_.peek().type == TokenType::TK_END) tok_.skip();
        return node;
    }

    // Skip tokens until matching closing paren (handles nesting)
    void skip_to_matching_paren() {
        int depth = 1;
        while (depth > 0) {
            Token t = tok_.next_token();
            if (t.type == TokenType::TK_LPAREN) ++depth;
            else if (t.type == TokenType::TK_RPAREN) --depth;
            else if (t.type == TokenType::TK_EOF) break;
        }
    }

    // Some keywords can appear as identifiers in expression context
    static bool is_keyword_as_identifier(TokenType type) {
        switch (type) {
            // Keywords commonly used as column/table names
            case TokenType::TK_COUNT:
            case TokenType::TK_SUM:
            case TokenType::TK_AVG:
            case TokenType::TK_MIN:
            case TokenType::TK_MAX:
            case TokenType::TK_IF:
            case TokenType::TK_VALUES:
            case TokenType::TK_DATABASE:
            case TokenType::TK_SCHEMA:
            case TokenType::TK_TABLE:
            case TokenType::TK_INDEX:
            case TokenType::TK_VIEW:
            case TokenType::TK_NAMES:
            case TokenType::TK_CHARACTER:
            case TokenType::TK_CHARSET:
            case TokenType::TK_GLOBAL:
            case TokenType::TK_SESSION:
            case TokenType::TK_LOCAL:
            case TokenType::TK_LEVEL:
            case TokenType::TK_READ:
            case TokenType::TK_WRITE:
            case TokenType::TK_ONLY:
            case TokenType::TK_TRANSACTION:
            case TokenType::TK_ISOLATION:
            case TokenType::TK_COMMITTED:
            case TokenType::TK_UNCOMMITTED:
            case TokenType::TK_REPEATABLE:
            case TokenType::TK_SERIALIZABLE:
            case TokenType::TK_SHARE:
            case TokenType::TK_DATA:
            case TokenType::TK_RESET:
            case TokenType::TK_KEY:
            case TokenType::TK_DO:
            case TokenType::TK_NOTHING:
            case TokenType::TK_CONFLICT:
            case TokenType::TK_CONSTRAINT:
            case TokenType::TK_RETURNING:
            case TokenType::TK_DUPLICATE:
            case TokenType::TK_DELAYED:
            case TokenType::TK_HIGH_PRIORITY:
                return true;
            default:
                return false;
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_EXPRESSION_PARSER_H
