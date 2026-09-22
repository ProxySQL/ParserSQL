#ifndef SQL_PARSER_COMPOUND_QUERY_PARSER_H
#define SQL_PARSER_COMPOUND_QUERY_PARSER_H

#include "sql_parser/select_parser.h"

namespace sql_parser {

template <Dialect D>
class CompoundQueryParser {
public:
    CompoundQueryParser(Tokenizer<D>& tokenizer, Arena& arena,
                        bool require_complete_operands = false)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena, require_complete_operands),
          require_complete_operands_(require_complete_operands) {}

    void set_subquery_callback(SubqueryParseCallback<D> cb) {
        subquery_cb_ = cb;
        expr_parser_.set_subquery_callback(cb);
    }

    // The classifier normally consumes SELECT. Pass the consumed keyword for
    // TABLE/VALUES/'(', or TK_EOF to start at an unconsumed query operand.
    AstNode* parse(TokenType first = TokenType::TK_SELECT) {
        const bool require_operands = require_complete_operands_ ||
            first == TokenType::TK_VALUES || first == TokenType::TK_TABLE;
        AstNode* result = parse_compound_expr(0, first);
        if (!result) return nullptr;
        if (result->type == NodeType::NODE_SET_OPERATION ||
            result->type == NodeType::NODE_VALUES_CLAUSE ||
            (result->type == NodeType::NODE_COMPOUND_QUERY &&
             (result->flags & FLAG_QUERY_PARENTHESIZED))) {
            AstNode* wrapper = make_node(arena_, NodeType::NODE_COMPOUND_QUERY);
            if (!wrapper) return nullptr;
            wrapper->add_child(result);
            result = wrapper;
        }
        if (tok_.peek().type == TokenType::TK_ORDER) {
            tok_.skip();
            if (tok_.peek().type != TokenType::TK_BY) return nullptr;
            tok_.skip();
            AstNode* order = parse_order_by(require_operands);
            if (!order || !order->first_child) return nullptr;
            result->add_child(order);
        }
        if (tok_.peek().type == TokenType::TK_LIMIT) {
            tok_.skip();
            AstNode* limit = parse_limit(require_operands);
            if (!limit || !limit->first_child) return nullptr;
            result->add_child(limit);
        }
        if (result->type == NodeType::NODE_SELECT_STMT &&
            tok_.peek().type == TokenType::TK_FOR) {
            tok_.skip();
            AstNode* lock = make_node(arena_, NodeType::NODE_LOCKING_CLAUSE);
            if (!lock) return nullptr;
            Token strength = tok_.next_token();
            lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, strength.text));
            result->add_child(lock);
        }
        return result;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    SubqueryParseCallback<D> subquery_cb_ = nullptr;
    bool require_complete_operands_;

    static int get_set_op_precedence(TokenType type) {
        switch (type) {
            case TokenType::TK_UNION:
            case TokenType::TK_EXCEPT: return 1;
            case TokenType::TK_INTERSECT: return 2;
            default: return 0;
        }
    }

    AstNode* parse_compound_expr(int min_prec, TokenType first = TokenType::TK_EOF) {
        AstNode* left = parse_operand(first);
        if (!left) return nullptr;
        while (true) {
            Token op = tok_.peek();
            int prec = get_set_op_precedence(op.type);
            if (prec <= min_prec) break;
            tok_.skip();
            uint16_t flags = 0;
            if (tok_.peek().type == TokenType::TK_ALL) {
                tok_.skip();
                flags = FLAG_SET_OP_ALL;
            } else if (tok_.peek().type == TokenType::TK_DISTINCT) {
                tok_.skip(); // DISTINCT is the default for set operations.
            }
            AstNode* right = parse_compound_expr(prec);
            if (!right) return nullptr;
            AstNode* node = make_node(arena_, NodeType::NODE_SET_OPERATION, op.text, flags);
            if (!node) return nullptr;
            node->add_child(left);
            node->add_child(right);
            left = node;
        }
        return left;
    }

    AstNode* parse_operand(TokenType first) {
        if (first == TokenType::TK_EOF) first = tok_.next_token().type;
        if (first == TokenType::TK_LPAREN) {
            AstNode* inner = parse(TokenType::TK_EOF);
            if (!inner || tok_.peek().type != TokenType::TK_RPAREN) return nullptr;
            tok_.skip();
            AstNode* group = make_node(arena_, NodeType::NODE_COMPOUND_QUERY,
                                       {}, FLAG_QUERY_PARENTHESIZED);
            if (!group) return nullptr;
            group->add_child(inner);
            return group;
        }
        if (first == TokenType::TK_SELECT) {
            SelectParser<D> select(tok_, arena_, true, require_complete_operands_);
            select.set_subquery_callback(subquery_cb_);
            return select.parse();
        }
        if constexpr (D == Dialect::PostgreSQL) {
            if (first == TokenType::TK_VALUES) return parse_values();
            if (first == TokenType::TK_TABLE) return parse_table();
        }
        return nullptr;
    }

    AstNode* parse_values() {
        ExpressionParser<D> expressions(tok_, arena_, true);
        expressions.set_subquery_callback(subquery_cb_);
        AstNode* values = make_node(arena_, NodeType::NODE_VALUES_CLAUSE);
        if (!values) return nullptr;
        AstNode* last_row = nullptr;
        do {
            if (tok_.next_token().type != TokenType::TK_LPAREN) return nullptr;
            AstNode* row = make_node(arena_, NodeType::NODE_VALUES_ROW);
            if (!row) return nullptr;
            AstNode* last_value = nullptr;
            do {
                TokenType next = tok_.peek().type;
                if (next == TokenType::TK_RPAREN || next == TokenType::TK_COMMA ||
                    next == TokenType::TK_EOF) return nullptr;
                AstNode* value = expressions.parse();
                if (!value) return nullptr;
                if (last_value) last_value->next_sibling = value;
                else row->first_child = value;
                last_value = value;
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            } while (true);
            if (tok_.next_token().type != TokenType::TK_RPAREN) return nullptr;
            if (last_row) last_row->next_sibling = row;
            else values->first_child = row;
            last_row = row;
            if (tok_.peek().type != TokenType::TK_COMMA) break;
            tok_.skip();
        } while (true);
        return values;
    }

    AstNode* parse_table() {
        AstNode* table = make_node(arena_, NodeType::NODE_TABLE_QUERY);
        if (!table) return nullptr;
        if (tok_.peek().type == TokenType::TK_ONLY) {
            tok_.skip();
            table->flags |= FLAG_TABLE_ONLY;
        }
        AstNode* name = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
        if (!name) return nullptr;
        do {
            Token part = tok_.next_token();
            if (part.type != TokenType::TK_IDENTIFIER) return nullptr;
            // Keep source delimiters: quoted PostgreSQL identifiers are case sensitive.
            StringRef spelling = part.source.empty() ? part.text : part.source;
            AstNode* ident = make_node(arena_, NodeType::NODE_IDENTIFIER, spelling);
            if (!ident) return nullptr;
            name->add_child(ident);
            if (tok_.peek().type != TokenType::TK_DOT) break;
            tok_.skip();
        } while (true);
        table->add_child(name);
        if (!(table->flags & FLAG_TABLE_ONLY) &&
            tok_.peek().type == TokenType::TK_ASTERISK) {
            tok_.skip();
            table->flags |= FLAG_TABLE_INHERIT;
        }
        return table;
    }

    // Parse trailing ORDER BY for compound result
    AstNode* parse_order_by(bool require_operands) {
        ExpressionParser<D> expressions(tok_, arena_, require_operands);
        expressions.set_subquery_callback(subquery_cb_);
        AstNode* order_by = make_node(arena_, NodeType::NODE_ORDER_BY_CLAUSE);
        if (!order_by) return nullptr;

        while (true) {
            AstNode* expr = expressions.parse();
            if (!expr) return nullptr;

            AstNode* item = make_node(arena_, NodeType::NODE_ORDER_BY_ITEM);
            if (!item) return nullptr;
            item->add_child(expr);

            // Optional ASC/DESC
            Token dir = tok_.peek();
            if (dir.type == TokenType::TK_ASC || dir.type == TokenType::TK_DESC) {
                tok_.skip();
                item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, dir.text));
            }

            if constexpr (D == Dialect::PostgreSQL) {
                if (ExpressionParser<D>::keyword(tok_.peek(), "NULLS")) {
                    tok_.skip();
                    Token placement = tok_.peek();
                    bool first = ExpressionParser<D>::keyword(placement, "FIRST");
                    if (!first && !ExpressionParser<D>::keyword(placement, "LAST")) return nullptr;
                    tok_.skip();
                    item->flags |= FLAG_ORDER_NULLS;
                    item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                        first ? StringRef{"NULLS FIRST", 11} : StringRef{"NULLS LAST", 10}));
                }
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
    AstNode* parse_limit(bool require_operands) {
        ExpressionParser<D> expressions(tok_, arena_, require_operands);
        expressions.set_subquery_callback(subquery_cb_);
        AstNode* limit = make_node(arena_, NodeType::NODE_LIMIT_CLAUSE);
        if (!limit) return nullptr;

        AstNode* first = expressions.parse();
        if (!first) return nullptr;
        limit->add_child(first);

        if (tok_.peek().type == TokenType::TK_OFFSET) {
            tok_.skip();
            AstNode* offset = expressions.parse();
            if (!offset) return nullptr;
            limit->add_child(offset);
        } else if (tok_.peek().type == TokenType::TK_COMMA) {
            if constexpr (D == Dialect::PostgreSQL) return nullptr;
            // MySQL: LIMIT offset, count
            tok_.skip();
            AstNode* count = expressions.parse();
            if (!count) return nullptr;
            limit->flags |= FLAG_LIMIT_COMMA;
            limit->first_child = count;
            count->next_sibling = first;
        }

        return limit;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_COMPOUND_QUERY_PARSER_H
