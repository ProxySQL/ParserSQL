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

    AstNode* explain_options() {
        auto* options = make_node(arena_, NodeType::NODE_EXPLAIN_OPTIONS);
        if (!options) return expr_.syntax_error();
        options->flags = 1;
        auto lookahead = tok_; lookahead.skip();
        if (tok_.peek().type == TokenType::TK_LPAREN &&
            !ExpressionParser<D>::starts_query(lookahead.peek().type) &&
            lookahead.peek().type != TokenType::TK_LPAREN) {
            tok_.skip();
            while (true) {
                auto key = tok_.peek();
                if (!pg_column_name(key) && !pg_type_function_name(key) &&
                    !word("ANALYZE") && !word("ANALYSE")) return expr_.syntax_error();
                tok_.skip();
                auto spelling = key.source.empty() ? key.text : key.source;
                if (ExpressionParser<D>::keyword(key, "ANALYSE")) spelling = {"ANALYZE", 7};
                auto* option = make_node(arena_, NodeType::NODE_PG_EXPLAIN_OPTION, spelling);
                if (!option) return expr_.syntax_error();
                auto value = tok_.peek();
                if (value.type != TokenType::TK_COMMA && value.type != TokenType::TK_RPAREN) {
                    AstNode* arg = nullptr;
                    if (value.type == TokenType::TK_INTEGER || value.type == TokenType::TK_FLOAT ||
                        value.type == TokenType::TK_PLUS || value.type == TokenType::TK_MINUS) {
                        arg = expr_.parse_complete(Precedence::UNARY);
                        if (!arg || (arg->type != NodeType::NODE_LITERAL_INT && arg->type != NodeType::NODE_LITERAL_FLOAT &&
                            !(arg->type == NodeType::NODE_UNARY_OP && arg->first_child &&
                              (arg->first_child->type == NodeType::NODE_LITERAL_INT ||
                               arg->first_child->type == NodeType::NODE_LITERAL_FLOAT)))) return expr_.syntax_error();
                    } else if (value.type == TokenType::TK_STRING) {
                        tok_.skip(); arg = make_node_from_token(arena_, NodeType::NODE_LITERAL_STRING, value);
                    } else if (pg_column_name(value) || pg_type_function_name(value) ||
                               word("TRUE") || word("FALSE") || word("ON")) {
                        tok_.skip(); arg = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, value,
                            value.type == TokenType::TK_IDENTIFIER && value.source.ptr != value.text.ptr ? FLAG_IDENT_DELIMITED : 0);
                    } else return expr_.syntax_error();
                    if (!arg) return expr_.syntax_error();
                    option->add_child(arg);
                }
                options->add_child(option);
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            }
            if (tok_.peek().type != TokenType::TK_RPAREN) return expr_.syntax_error();
            tok_.skip();
        } else {
            if (word("ANALYZE") || word("ANALYSE")) {
                tok_.skip(); auto* option = make_node(arena_, NodeType::NODE_PG_EXPLAIN_OPTION, {"ANALYZE", 7});
                if (!option) return expr_.syntax_error();
                options->add_child(option);
            }
            if (word("VERBOSE")) {
                tok_.skip(); auto* option = make_node(arena_, NodeType::NODE_PG_EXPLAIN_OPTION, {"VERBOSE", 7});
                if (!option) return expr_.syntax_error();
                options->add_child(option);
            }
        }
        return options;
    }

    AstNode* into() {
        tok_.skip(); // INTO
        StringRef persistence;
        if (word("LOCAL") || word("GLOBAL")) {
            tok_.skip();
            if (!word("TEMP") && !word("TEMPORARY")) return expr_.syntax_error();
        }
        if (word("TEMP") || word("TEMPORARY")) { tok_.skip(); persistence = {"TEMPORARY ", 10}; }
        else if (word("UNLOGGED")) { tok_.skip(); persistence = {"UNLOGGED ", 9}; }
        if (word("TABLE")) tok_.skip();
        auto* node = make_node(arena_, NodeType::NODE_PG_SELECT_INTO, persistence);
        auto* name = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
        if (!node || !name) return expr_.syntax_error();
        auto token = tok_.next_token();
        if (!pg_column_name(token)) return expr_.syntax_error();
        unsigned parts = 0;
        while (true) {
            if (++parts > 3) return expr_.syntax_error();
            auto* part = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token,
                token.type == TokenType::TK_IDENTIFIER && token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
            if (!part) return expr_.syntax_error();
            name->add_child(part);
            if (tok_.peek().type != TokenType::TK_DOT) break;
            tok_.skip(); token = tok_.next_token();
            if (!pg_column_label(token)) return expr_.syntax_error();
        }
        node->add_child(name); return node;
    }

    bool tail(AstNode* query) {
        // PostgreSQL permits the locking list before or after pagination.
        // Keep each list contiguous, as required by select_limit grammar.
        const bool locks_first = word("FOR");
        if (locks_first && !locking(query)) return false;
        if (!pagination(query)) return false;
        if (!locks_first && !locking(query)) return false;
        return true;
    }
    bool locking(AstNode* query) {
        while (word("FOR")) {
            tok_.skip();
            StringRef strength;
            if (word("UPDATE")) { tok_.skip(); strength = {"UPDATE", 6}; }
            else if (word("SHARE")) { tok_.skip(); strength = {"SHARE", 5}; }
            else if (word("NO")) {
                tok_.skip(); if (!word("KEY")) { expr_.syntax_error(); return false; }
                tok_.skip(); if (!word("UPDATE")) { expr_.syntax_error(); return false; }
                tok_.skip(); strength = {"NO KEY UPDATE", 13};
            } else if (word("KEY")) {
                tok_.skip(); if (!word("SHARE")) { expr_.syntax_error(); return false; }
                tok_.skip(); strength = {"KEY SHARE", 9};
            } else { expr_.syntax_error(); return false; }
            auto* lock = make_node(arena_, NodeType::NODE_PG_ROW_LOCK, strength);
            if (!lock) { expr_.syntax_error(); return false; }
            if (word("OF")) {
                tok_.skip();
                while (true) {
                    auto token = tok_.next_token();
                    if (!pg_column_name(token)) { expr_.syntax_error(); return false; }
                    auto* rel = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
                    auto* part = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token,
                token.type == TokenType::TK_IDENTIFIER && token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
                    if (!rel || !part) { expr_.syntax_error(); return false; }
                    rel->add_child(part);
                    unsigned parts = 1;
                    while (tok_.peek().type == TokenType::TK_DOT) {
                        if (++parts > 3) { expr_.syntax_error(); return false; }
                        tok_.skip(); token = tok_.next_token();
                        if (!pg_column_label(token)) { expr_.syntax_error(); return false; }
                        part = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token,
                token.type == TokenType::TK_IDENTIFIER && token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
                        if (!part) { expr_.syntax_error(); return false; }
                        rel->add_child(part);
                    }
                    lock->add_child(rel);
                    if (tok_.peek().type != TokenType::TK_COMMA) break;
                    tok_.skip();
                }
            }
            if (word("NOWAIT")) { tok_.skip(); lock->flags = 1; }
            else if (word("SKIP")) {
                tok_.skip(); if (!word("LOCKED")) { expr_.syntax_error(); return false; }
                tok_.skip(); lock->flags = 2;
            }
            query->add_child(lock);
        }
        return true;
    }

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
