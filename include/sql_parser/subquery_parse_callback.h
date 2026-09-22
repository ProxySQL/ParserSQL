// subquery_parse_callback.h -- Implementation of the subquery parse callback
//
// Shared query callback for expression and derived-table contexts. PostgreSQL
// supports compound SELECT/TABLE/VALUES queries and WITH; MySQL retains its
// existing simple SELECT callback.

#ifndef SQL_PARSER_SUBQUERY_PARSE_CALLBACK_H
#define SQL_PARSER_SUBQUERY_PARSE_CALLBACK_H

#include "sql_parser/common.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/compound_query_parser.h"

namespace sql_parser {

// These mutually recursive callbacks share the tokenizer and arena. Query
// operands stop before their closing parenthesis; the caller consumes it.
template <Dialect D>
AstNode* parse_subquery_select(Tokenizer<D>& tok, Arena& arena);

inline AstNode* parse_pg_with(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena) {
    using Expr = ExpressionParser<Dialect::PostgreSQL>;
    Expr error(tok, arena);
    AstNode* cte = make_node(arena, NodeType::NODE_CTE);
    if (!cte) return error.syntax_error();
    if (tok.peek().type == TokenType::TK_RECURSIVE) {
        tok.skip();
        cte->flags |= FLAG_CTE_RECURSIVE;
    }
    while (true) {
        Token name = tok.next_token();
        if (!pg_column_name(name)) return error.syntax_error();
        AstNode* def = make_node_from_token(arena, NodeType::NODE_CTE_DEFINITION, name,
            name.source.ptr != name.text.ptr ? FLAG_IDENT_DELIMITED : 0);
        if (!def) return error.syntax_error();
        AstNode* columns = nullptr;
        if (tok.peek().type == TokenType::TK_LPAREN) {
            tok.skip();
            columns = make_node(arena, NodeType::NODE_CTE_COLUMNS);
            if (!columns) return error.syntax_error();
            while (true) {
                Token column = tok.next_token();
                if (!pg_column_name(column)) return error.syntax_error();
                columns->add_child(make_node_from_token(arena, NodeType::NODE_IDENTIFIER, column,
                    column.source.ptr != column.text.ptr ? FLAG_IDENT_DELIMITED : 0));
                if (tok.peek().type != TokenType::TK_COMMA) break;
                tok.skip();
            }
            if (tok.peek().type != TokenType::TK_RPAREN) return error.syntax_error();
            tok.skip();
        }
        if (tok.peek().type != TokenType::TK_AS) return error.syntax_error();
        tok.skip();
        if (tok.peek().type == TokenType::TK_NOT) {
            tok.skip();
            if (!Expr::keyword(tok.peek(), "MATERIALIZED")) return error.syntax_error();
            tok.skip();
            def->flags |= FLAG_CTE_NOT_MATERIALIZED;
        } else if (Expr::keyword(tok.peek(), "MATERIALIZED")) {
            tok.skip();
            def->flags |= FLAG_CTE_MATERIALIZED;
        }
        if (tok.peek().type != TokenType::TK_LPAREN) return error.syntax_error();
        tok.skip();
        AstNode* body = parse_subquery_select<Dialect::PostgreSQL>(tok, arena);
        if (!body || tok.peek().type != TokenType::TK_RPAREN) return error.syntax_error();
        tok.skip();
        def->add_child(body); // Existing consumers expect the body first.
        def->add_child(columns);
        cte->add_child(def);
        if (tok.peek().type != TokenType::TK_COMMA) break;
        tok.skip();
    }
    // A second bare WITH is not a main query; nested WITH belongs in a body.
    if (tok.peek().type == TokenType::TK_WITH) return error.syntax_error();
    AstNode* query = parse_subquery_select<Dialect::PostgreSQL>(tok, arena);
    if (!query) return error.syntax_error();
    cte->add_child(query);
    return cte;
}

template <Dialect D>
AstNode* parse_subquery_select(Tokenizer<D>& tok, Arena& arena) {
    if constexpr (D == Dialect::PostgreSQL) {
        if (tok.peek().type == TokenType::TK_WITH) {
            tok.skip();
            return parse_pg_with(tok, arena);
        }
        TokenType first = tok.peek().type;
        if (first != TokenType::TK_SELECT && first != TokenType::TK_VALUES &&
            first != TokenType::TK_TABLE && first != TokenType::TK_LPAREN)
            return ExpressionParser<D>(tok, arena).syntax_error();
        CompoundQueryParser<D> query(tok, arena, true);
        query.set_subquery_callback(&parse_subquery_select<D>);
        return query.parse(TokenType::TK_EOF);
    } else {
        if (tok.peek().type == TokenType::TK_SELECT) tok.skip();
        SelectParser<D> sp(tok, arena, false);
        sp.set_subquery_callback(&parse_subquery_select<D>);
        return sp.parse();
    }
}

} // namespace sql_parser

#endif // SQL_PARSER_SUBQUERY_PARSE_CALLBACK_H
