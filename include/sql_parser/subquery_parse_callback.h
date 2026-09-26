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
#include "sql_parser/pg_merge_parser.h"

namespace sql_parser {

// These mutually recursive callbacks share the tokenizer and arena. Query
// operands stop before their closing parenthesis; the caller consumes it.
template <Dialect D>
AstNode* parse_subquery_select(Tokenizer<D>& tok, Arena& arena);

inline AstNode* parse_pg_statement(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena,
                                   bool allow_dml);

inline AstNode* parse_pg_with(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena,
                              bool allow_dml = false) {
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
                AstNode* item = make_node_from_token(arena, NodeType::NODE_IDENTIFIER, column,
                    column.source.ptr != column.text.ptr ? FLAG_IDENT_DELIMITED : 0);
                if (!item) return error.syntax_error();
                columns->add_child(item);
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
        AstNode* body = parse_pg_statement(tok, arena, allow_dml);
        if (!body || tok.peek().type != TokenType::TK_RPAREN) return error.syntax_error();
        tok.skip();
        def->add_child(body); // Existing consumers expect the body first.
        def->add_child(columns);
        auto take = [&](const char* word) {
            if (!Expr::keyword(tok.peek(), word)) return false;
            tok.skip(); return true;
        };
        auto identifier = [&]() -> AstNode* {
            Token token = tok.next_token();
            if (!pg_column_name(token)) return error.syntax_error();
            AstNode* result = make_node_from_token(arena, NodeType::NODE_IDENTIFIER, token,
                token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
            return result ? result : error.syntax_error();
        };
        auto column_list = [&]() -> AstNode* {
            AstNode* list = make_node(arena, NodeType::NODE_CTE_COLUMNS);
            if (!list) return error.syntax_error();
            do {
                AstNode* column = identifier();
                if (!column) return nullptr;
                list->add_child(column);
            } while (take(","));
            return list;
        };
        if (take("SEARCH")) {
            const char* kind = take("DEPTH") ? "DEPTH" : take("BREADTH") ? "BREADTH" : nullptr;
            if (!kind || !take("FIRST") || !take("BY")) return error.syntax_error();
            AstNode* search = make_node(arena, NodeType::NODE_CTE_SEARCH,
                StringRef{kind, static_cast<uint32_t>(std::strlen(kind))});
            if (!search) return error.syntax_error();
            AstNode* cols = column_list();
            if (!cols || !take("SET")) return error.syntax_error();
            AstNode* sequence = identifier();
            if (!sequence) return error.syntax_error();
            search->add_child(cols);
            search->add_child(sequence);
            def->add_child(search);
        }
        if (take("CYCLE")) {
            AstNode* cycle = make_node(arena, NodeType::NODE_CTE_CYCLE);
            if (!cycle) return error.syntax_error();
            AstNode* cols = column_list();
            if (!cols || !take("SET")) return error.syntax_error();
            AstNode* mark = identifier();
            if (!mark) return error.syntax_error();
            cycle->add_child(cols);
            cycle->add_child(mark);
            if (take("TO")) {
                cycle->flags |= 1;
                auto constant = [&]() -> AstNode* {
                    Token start = tok.peek();
                    AstNode* value = error.parse_complete();
                    if (!value) return nullptr;
                    switch (value->type) {
                        case NodeType::NODE_LITERAL_INT:
                        case NodeType::NODE_LITERAL_FLOAT:
                        case NodeType::NODE_LITERAL_STRING:
                        case NodeType::NODE_LITERAL_NULL:
                        case NodeType::NODE_LITERAL_HEX:
                        case NodeType::NODE_LITERAL_BIT:
                        case NodeType::NODE_PG_INTERVAL:
                            return value;
                        case NodeType::NODE_TYPE_CAST:
                            if (PgTypeParser::name_token(start) && !Expr::keyword(start, "CAST") &&
                                value->first_child && value->first_child->type == NodeType::NODE_LITERAL_STRING)
                                return value;
                            return error.syntax_error();
                        default: return error.syntax_error();
                    }
                };
                AstNode* value = constant();
                if (!value || !take("DEFAULT")) return error.syntax_error();
                AstNode* fallback = constant();
                if (!fallback) return error.syntax_error();
                cycle->add_child(value);
                cycle->add_child(fallback);
            }
            if (!take("USING")) return error.syntax_error();
            AstNode* path = identifier();
            if (!path) return error.syntax_error();
            cycle->add_child(path);
            def->add_child(cycle);
        }
        cte->add_child(def);
        if (tok.peek().type != TokenType::TK_COMMA) break;
        tok.skip();
    }
    // A second bare WITH is not a main query; nested WITH belongs in a body.
    if (tok.peek().type == TokenType::TK_WITH) return error.syntax_error();
    AstNode* query = parse_pg_statement(tok, arena, allow_dml);
    if (!query) return error.syntax_error();
    cte->add_child(query);
    return cte;
}

inline AstNode* parse_pg_statement(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena,
                                   bool allow_dml) {
    using Expr = ExpressionParser<Dialect::PostgreSQL>;
    if (tok.peek().type == TokenType::TK_WITH) {
        tok.skip();
        return parse_pg_with(tok, arena, allow_dml);
    }
    if (allow_dml) {
        PgDmlParser dml(tok, arena, &parse_subquery_select<Dialect::PostgreSQL>);
        if (Expr::keyword(tok.peek(), "INSERT")) { tok.skip(); return dml.insert(); }
        if (Expr::keyword(tok.peek(), "UPDATE")) { tok.skip(); return dml.update(); }
        if (Expr::keyword(tok.peek(), "DELETE")) { tok.skip(); return dml.remove(); }
        if (Expr::keyword(tok.peek(), "MERGE")) { tok.skip(); return dml.merge(); }
    }
    return parse_subquery_select<Dialect::PostgreSQL>(tok, arena);
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
