#ifndef SQL_PARSER_PG_QUERY_CLAUSES_H
#define SQL_PARSER_PG_QUERY_CLAUSES_H
#include "sql_parser/expression_parser.h"
namespace sql_parser {

template <Dialect D>
class PgQueryClauses {
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D>& expr_;
    bool word(const char* s) { return ExpressionParser<D>::keyword(tok_.peek(), s); }
    // SQL FETCH and OFFSET ... ROWS use c_expr plus signed numeric constants,
    // unlike LIMIT and an OFFSET without ROWS, which allow arbitrary a_expr.
    static bool fetch_value(const AstNode* node, const Token& start) {
        if (!node) return false;
        switch (node->type) {
            case NodeType::NODE_UNARY_OP:
                return (node->value() == StringRef{"+", 1} || node->value() == StringRef{"-", 1}) &&
                    node->first_child && (node->first_child->type == NodeType::NODE_LITERAL_INT ||
                                          node->first_child->type == NodeType::NODE_LITERAL_FLOAT);
            case NodeType::NODE_TYPE_CAST:
                if (ExpressionParser<D>::keyword(start, "CAST")) return node->source_ptr == start.source.ptr;
                return node->first_child && node->first_child->type == NodeType::NODE_LITERAL_STRING &&
                    start.type != TokenType::TK_STRING;
            case NodeType::NODE_ASTERISK:
            case NodeType::NODE_BINARY_OP:
            case NodeType::NODE_IS_NULL:
            case NodeType::NODE_IS_NOT_NULL:
            case NodeType::NODE_IN_LIST:
            case NodeType::NODE_BETWEEN:
            case NodeType::NODE_PG_TIME_ZONE:
            case NodeType::NODE_PG_QUANTIFIED_OPERAND: return false;
            default: return true;
        }
    }
    static bool has_order(const AstNode* query) {
        for (const AstNode* child = query->first_child; child; child = child->next_sibling)
            if (child->type == NodeType::NODE_ORDER_BY_CLAUSE) return true;
        if (query->type == NodeType::NODE_COMPOUND_QUERY && query->first_child &&
            (query->first_child->type == NodeType::NODE_COMPOUND_QUERY ||
             query->first_child->type == NodeType::NODE_SELECT_STMT))
            return has_order(query->first_child);
        return false;
    }
public:
    PgQueryClauses(Tokenizer<D>& tok, Arena& arena, ExpressionParser<D>& expr)
        : tok_(tok), arena_(arena), expr_(expr) {}

    AstNode* grouping(bool nested = true) {
        Token token = tok_.peek();
        auto lookahead = tok_;
        lookahead.skip();
        if (token.type == TokenType::TK_LPAREN && lookahead.peek().type == TokenType::TK_RPAREN) {
            tok_.skip(); tok_.skip();
            return make_node(arena_, NodeType::NODE_TUPLE);
        }
        const bool sets = nested && word("GROUPING") && ExpressionParser<D>::keyword(lookahead.peek(), "SETS");
        const bool call = lookahead.peek().type == TokenType::TK_LPAREN;
        const bool rollup = nested && word("ROLLUP") && call;
        const bool cube = nested && word("CUBE") && call;
        if (!sets && !rollup && !cube) return expr_.parse_complete();
        tok_.skip();
        if (sets) {
            if (!word("SETS")) return expr_.syntax_error();
            tok_.skip();
        }
        // GROUPING(expr) is an ordinary function, not GROUPING SETS.
        if (tok_.peek().type != TokenType::TK_LPAREN) return expr_.syntax_error();
        tok_.skip();
        auto* node = make_node(arena_, NodeType::NODE_GROUPING_SET,
            sets ? StringRef{"GROUPING SETS", 13} : token.text);
        if (!node) return expr_.syntax_error();
        if (tok_.peek().type == TokenType::TK_RPAREN) return expr_.syntax_error();
        while (true) {
            auto* item = grouping(sets);
            if (!item) return expr_.syntax_error();
            node->add_child(item);
            if (tok_.peek().type != TokenType::TK_COMMA) break;
            tok_.skip();
        }
        if (tok_.peek().type != TokenType::TK_RPAREN) return expr_.syntax_error();
        tok_.skip();
        return node;
    }

    // Normalize LIMIT/OFFSET ordering, while retaining FETCH and offset-only
    // nodes explicitly so SQL reconstruction preserves PostgreSQL semantics.
    bool pagination(AstNode* query) {
        AstNode* count = nullptr;
        AstNode* offset = nullptr;
        AstNode* fetch = nullptr;
        while (true) {
            auto type = tok_.peek().type;
            if (type == TokenType::TK_LIMIT) {
                if (count || fetch) { expr_.syntax_error(); return false; }
                tok_.skip();
                if (word("ALL")) {
                    tok_.skip(); count = make_node(arena_, NodeType::NODE_LITERAL_NULL, {"NULL", 4});
                } else count = expr_.parse_complete();
                if (!count) { expr_.syntax_error(); return false; }
                if (tok_.peek().type == TokenType::TK_COMMA) { expr_.syntax_error(); return false; }
            } else if (type == TokenType::TK_OFFSET) {
                if (offset) { expr_.syntax_error(); return false; }
                tok_.skip();
                Token start = tok_.peek();
                offset = expr_.parse_complete();
                if (!offset) { expr_.syntax_error(); return false; }
                if (word("ROW") || word("ROWS")) {
                    if (!fetch_value(offset, start)) { expr_.syntax_error(); return false; }
                    tok_.skip();
                }
            } else if (type == TokenType::TK_FETCH) {
                if (count || fetch) { expr_.syntax_error(); return false; }
                tok_.skip();
                if (!word("FIRST") && !word("NEXT")) { expr_.syntax_error(); return false; }
                tok_.skip();
                Token start = tok_.peek();
                auto* value = (word("ROW") || word("ROWS"))
                    ? make_node(arena_, NodeType::NODE_LITERAL_INT, {"1", 1})
                    : expr_.parse_complete(Precedence::UNARY);
                if (!fetch_value(value, start) || (!word("ROW") && !word("ROWS"))) { expr_.syntax_error(); return false; }
                tok_.skip();
                fetch = make_node(arena_, NodeType::NODE_FETCH_CLAUSE);
                if (!fetch) { expr_.syntax_error(); return false; }
                fetch->add_child(value);
                if (word("ONLY")) tok_.skip();
                else {
                    if (!word("WITH")) { expr_.syntax_error(); return false; }
                    tok_.skip();
                    if (!word("TIES")) { expr_.syntax_error(); return false; }
                    tok_.skip(); fetch->flags = 1;
                }
            } else break;
        }
        if (fetch && fetch->flags && !has_order(query)) { expr_.syntax_error(); return false; }
        if (count) {
            auto* node = make_node(arena_, NodeType::NODE_LIMIT_CLAUSE);
            if (!node) { expr_.syntax_error(); return false; }
            node->add_child(count); node->add_child(offset); query->add_child(node);
        } else if (offset) {
            auto* node = make_node(arena_, NodeType::NODE_OFFSET_CLAUSE);
            if (!node) { expr_.syntax_error(); return false; }
            node->add_child(offset); query->add_child(node);
        }
        query->add_child(fetch);
        return true;
    }
};
}
#endif
