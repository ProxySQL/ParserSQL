#ifndef SQL_PARSER_COMPOUND_QUERY_PARSER_H
#define SQL_PARSER_COMPOUND_QUERY_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/select_parser.h"
#include "sql_parser/expression_parser.h"

namespace sql_parser {

// Flag on NODE_SET_OPERATION to indicate ALL
static constexpr uint16_t FLAG_SET_OP_ALL = 0x01;

template <Dialect D>
class CompoundQueryParser {
public:
    CompoundQueryParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena) {}

    // Parse a compound query (or a plain SELECT if no set operator follows).
    // Returns NODE_SELECT_STMT for plain selects, NODE_COMPOUND_QUERY for compounds.
    AstNode* parse() {
        AstNode* result = parse_compound_expr(0);
        if (!result) return nullptr;

        // If the result is a set operation, wrap in COMPOUND_QUERY and parse trailing clauses
        if (result->type == NodeType::NODE_SET_OPERATION) {
            AstNode* compound = make_node(arena_, NodeType::NODE_COMPOUND_QUERY);
            if (!compound) return nullptr;
            compound->add_child(result);

            // Parse trailing ORDER BY (applies to whole compound)
            if (tok_.peek().type == TokenType::TK_ORDER) {
                tok_.skip();
                if (tok_.peek().type == TokenType::TK_BY) tok_.skip();
                AstNode* order_by = parse_order_by();
                if (order_by) compound->add_child(order_by);
            }

            // Parse trailing LIMIT (applies to whole compound)
            if (tok_.peek().type == TokenType::TK_LIMIT) {
                tok_.skip();
                AstNode* limit = parse_limit();
                if (limit) compound->add_child(limit);
            }

            return compound;
        }

        // No set operator found -- return the bare SELECT as-is
        return result;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;

    // Precedence levels
    static constexpr int PREC_UNION_EXCEPT = 1;
    static constexpr int PREC_INTERSECT = 2;

    // Get the precedence of a set operator token, or 0 if not a set operator
    static int get_set_op_precedence(TokenType type) {
        switch (type) {
            case TokenType::TK_UNION:     return PREC_UNION_EXCEPT;
            case TokenType::TK_EXCEPT:    return PREC_UNION_EXCEPT;
            case TokenType::TK_INTERSECT: return PREC_INTERSECT;
            default: return 0;
        }
    }

    // Check if a token is a set operator
    static bool is_set_operator(TokenType type) {
        return type == TokenType::TK_UNION ||
               type == TokenType::TK_INTERSECT ||
               type == TokenType::TK_EXCEPT;
    }

    // Parse a compound expression with minimum precedence (Pratt-style)
    AstNode* parse_compound_expr(int min_prec) {
        AstNode* left = parse_operand();
        if (!left) return nullptr;

        while (true) {
            Token t = tok_.peek();
            int prec = get_set_op_precedence(t.type);
            if (prec == 0 || prec <= min_prec) break;

            // Consume the set operator
            tok_.skip();
            StringRef op_text = t.text;

            // Check for optional ALL
            uint16_t flags = 0;
            if (tok_.peek().type == TokenType::TK_ALL) {
                tok_.skip();
                flags = FLAG_SET_OP_ALL;
            }

            // Parse right operand with current precedence as min (left-associative)
            AstNode* right = parse_compound_expr(prec);
            if (!right) return nullptr;

            // Build NODE_SET_OPERATION with left and right as children
            AstNode* setop = make_node(arena_, NodeType::NODE_SET_OPERATION, op_text);
            if (!setop) return nullptr;
            setop->flags = flags;
            setop->add_child(left);
            setop->add_child(right);

            left = setop;
        }

        return left;
    }

    // Parse a single operand: parenthesized compound or plain SELECT
    AstNode* parse_operand() {
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip(); // consume '('

            // Could be a parenthesized compound query or a parenthesized SELECT
            AstNode* inner = nullptr;
            if (tok_.peek().type == TokenType::TK_SELECT ||
                tok_.peek().type == TokenType::TK_LPAREN) {
                // Parse the inner compound expression recursively
                // Need to consume SELECT keyword first if present
                if (tok_.peek().type == TokenType::TK_SELECT) {
                    tok_.skip(); // consume SELECT
                    // Create a SelectParser that will parse from after SELECT
                    SelectParser<D> sp(tok_, arena_);
                    AstNode* select = sp.parse();

                    // Check if a set operator follows inside the parens
                    if (is_set_operator(tok_.peek().type)) {
                        // There's a compound inside the parens
                        // We need to continue parsing with select as the left operand
                        inner = continue_compound_from(select, 0);
                    } else {
                        inner = select;
                    }
                } else {
                    // Nested parenthesized: ((SELECT ...))
                    inner = parse_compound_expr(0);
                }
            }

            // Expect closing ')'
            if (tok_.peek().type == TokenType::TK_RPAREN) {
                tok_.skip();
            }

            return inner;
        }

        // Not parenthesized -- must be a plain SELECT
        // Note: SELECT keyword was already consumed by the classifier
        // The tokenizer is positioned right after SELECT
        SelectParser<D> sp(tok_, arena_);
        return sp.parse();
    }

    // Continue parsing compound from an already-parsed left operand
    AstNode* continue_compound_from(AstNode* left, int min_prec) {
        if (!left) return nullptr;

        while (true) {
            Token t = tok_.peek();
            int prec = get_set_op_precedence(t.type);
            if (prec == 0 || prec <= min_prec) break;

            tok_.skip();
            StringRef op_text = t.text;

            uint16_t flags = 0;
            if (tok_.peek().type == TokenType::TK_ALL) {
                tok_.skip();
                flags = FLAG_SET_OP_ALL;
            }

            // Inside parens, operand must start with SELECT or (
            AstNode* right = nullptr;
            if (tok_.peek().type == TokenType::TK_SELECT) {
                tok_.skip();
                SelectParser<D> sp(tok_, arena_);
                AstNode* rsel = sp.parse();
                // Check for more operators at higher precedence
                right = continue_compound_from(rsel, prec);
            } else if (tok_.peek().type == TokenType::TK_LPAREN) {
                right = parse_operand(); // handles nested parens
                right = continue_compound_from(right, prec);
            }

            if (!right) return nullptr;

            AstNode* setop = make_node(arena_, NodeType::NODE_SET_OPERATION, op_text);
            if (!setop) return nullptr;
            setop->flags = flags;
            setop->add_child(left);
            setop->add_child(right);

            left = setop;
        }

        return left;
    }

    // Parse trailing ORDER BY for compound result
    AstNode* parse_order_by() {
        AstNode* order_by = make_node(arena_, NodeType::NODE_ORDER_BY_CLAUSE);
        if (!order_by) return nullptr;

        while (true) {
            AstNode* expr = expr_parser_.parse();
            if (!expr) break;

            AstNode* item = make_node(arena_, NodeType::NODE_ORDER_BY_ITEM);
            item->add_child(expr);

            // Optional ASC/DESC
            Token dir = tok_.peek();
            if (dir.type == TokenType::TK_ASC || dir.type == TokenType::TK_DESC) {
                tok_.skip();
                item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, dir.text));
            }

            order_by->add_child(item);

            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }
        return order_by;
    }

    // Parse trailing LIMIT for compound result
    AstNode* parse_limit() {
        AstNode* limit = make_node(arena_, NodeType::NODE_LIMIT_CLAUSE);
        if (!limit) return nullptr;

        AstNode* first = expr_parser_.parse();
        if (first) limit->add_child(first);

        if (tok_.peek().type == TokenType::TK_OFFSET) {
            tok_.skip();
            AstNode* offset = expr_parser_.parse();
            if (offset) limit->add_child(offset);
        } else if (tok_.peek().type == TokenType::TK_COMMA) {
            // MySQL: LIMIT offset, count
            tok_.skip();
            AstNode* count = expr_parser_.parse();
            if (count) limit->add_child(count);
        }

        return limit;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_COMPOUND_QUERY_PARSER_H
