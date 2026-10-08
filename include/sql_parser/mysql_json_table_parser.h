#ifndef SQL_PARSER_MYSQL_JSON_TABLE_PARSER_H
#define SQL_PARSER_MYSQL_JSON_TABLE_PARSER_H

#include "sql_parser/ast.h"
#include "sql_parser/mysql_charset.h"
#include "sql_parser/mysql_identifier.h"
#include "sql_parser/mysql_type_parser.h"
#include "sql_parser/mysql_value_syntax.h"

namespace sql_parser {

// JSON_TABLE's document is an expression; paths and DEFAULT responses have
// narrower native literal productions. Never consume an unvalidated tail.
template <class Expr>
class MySQLJsonTableParser {
public:
    MySQLJsonTableParser(Tokenizer<Dialect::MySQL>& tok, Arena& arena, Expr& expr)
        : tok_(tok), arena_(arena), expr_(expr) {}

    AstNode* parse() {
        tok_.skip(); // JSON_TABLE
        if (!take(TokenType::TK_LPAREN)) return fail();
        auto* document = expr_.parse_complete();
        if (!document || !mysql_value_expression(document) || !take(TokenType::TK_COMMA)) return fail();
        auto* path = literal(false);
        auto* columns = path ? parse_columns(0) : nullptr;
        if (!columns || !take(TokenType::TK_RPAREN)) return fail();
        auto* node = make_node(arena_, NodeType::NODE_MYSQL_JSON_TABLE);
        if (!node) return fail();
        node->add_child(document); node->add_child(path); node->add_child(columns);
        return node;
    }

private:
    Tokenizer<Dialect::MySQL>& tok_;
    Arena& arena_;
    Expr& expr_;

    AstNode* fail() { return expr_.syntax_error(); }
    bool is(const char* value) { return Expr::keyword(tok_.peek(), value); }
    bool word(const char* value) {
        if (!is(value)) return false;
        tok_.skip(); return true;
    }
    bool take(TokenType type) {
        if (tok_.peek().type != type) return false;
        tok_.skip(); return true;
    }

    AstNode* literal(bool defaults) {
        const Token start = tok_.peek();
        Token last = start;
        auto consume = [&]() { last = tok_.next_token(); };
        bool text = false;
        if (start.type == TokenType::TK_STRING) { consume(); text = true; }
        else if (Expr::keyword(start, "N")) {
            consume();
            const Token value = tok_.peek();
            if (value.type != TokenType::TK_STRING || value.source.ptr[0] != '\'' ||
                start.source.ptr + start.source.len != value.source.ptr) return fail();
            consume(); text = true;
        } else if (mysql_charset_introducer(start)) {
            consume();
            const Token value = tok_.peek();
            text = value.type == TokenType::TK_STRING;
            if (!text && (!defaults || !base_literal(value))) return fail();
            consume();
        } else if (defaults) {
            if (start.type == TokenType::TK_PLUS || start.type == TokenType::TK_MINUS) {
                consume();
                if (tok_.peek().type != TokenType::TK_INTEGER && tok_.peek().type != TokenType::TK_FLOAT)
                    return fail();
                consume();
            } else if (start.type == TokenType::TK_INTEGER || start.type == TokenType::TK_FLOAT ||
                       Expr::keyword(start, "TRUE") || Expr::keyword(start, "FALSE") || base_literal(start)) {
                consume();
            } else if (Expr::keyword(start, "DATE") || Expr::keyword(start, "TIME") ||
                       Expr::keyword(start, "TIMESTAMP")) {
                consume();
                if (tok_.peek().type != TokenType::TK_STRING) return fail();
                consume();
            } else return fail();
        } else return fail();
        if (text) while (tok_.peek().type == TokenType::TK_STRING) consume();
        if (tok_.has_error()) return fail();
        StringRef source{start.source.ptr,
            static_cast<uint32_t>(last.source.ptr + last.source.len - start.source.ptr)};
        auto* node = make_node(arena_, NodeType::NODE_MYSQL_JSON_TABLE_LITERAL, source);
        if (!node) return fail();
        node->set_source(source);
        return node;
    }

    static bool base_literal(const Token& value) {
        if (value.type != TokenType::TK_HEX_LITERAL && value.type != TokenType::TK_BIT_LITERAL) return false;
        return !(value.source.len >= 2 && value.source.ptr[0] == '0' &&
                 (value.source.ptr[1] == 'X' || value.source.ptr[1] == 'B'));
    }

    AstNode* parse_columns(unsigned depth) {
        // A dedicated bound protects recursive NESTED COLUMNS independently
        // from general expression nesting and maliciously deep input.
        if (depth >= 128 || !word("COLUMNS") || !take(TokenType::TK_LPAREN)) return fail();
        auto* columns = make_node(arena_, NodeType::NODE_MYSQL_JSON_TABLE_COLUMNS);
        if (!columns) return fail();
        AstNode* tail = nullptr;
        do {
            auto* column = parse_column(depth);
            if (!column) return fail();
            if (tail) tail->next_sibling = column;
            else columns->first_child = column;
            tail = column;
        } while (take(TokenType::TK_COMMA));
        if (!take(TokenType::TK_RPAREN)) return fail();
        return columns;
    }

    AstNode* parse_column(unsigned depth) {
        if (word("NESTED")) {
            if (!word("PATH")) return fail();
            auto* path = literal(false);
            auto* columns = path ? parse_columns(depth + 1) : nullptr;
            if (!columns) return fail();
            auto* nested = make_node(arena_, NodeType::NODE_MYSQL_JSON_TABLE_NESTED);
            if (!nested) return fail();
            nested->add_child(path); nested->add_child(columns);
            return nested;
        }
        const Token name = tok_.next_token();
        if (!mysql_identifier_token(name)) return fail();
        auto* column = make_node(arena_, NodeType::NODE_MYSQL_JSON_TABLE_COLUMN);
        auto* identifier = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, name,
            name.source.ptr != name.text.ptr ? FLAG_IDENT_DELIMITED : 0);
        if (!column || !identifier) return fail();
        column->add_child(identifier);
        if (take(TokenType::TK_FOR)) {
            if (!word("ORDINALITY")) return fail();
            column->flags = 1;
            return column;
        }
        StringRef type = MySQLTypeParser(tok_).parse();
        if (type.empty()) return fail();
        if (take(TokenType::TK_COLLATE)) {
            const Token collation = tok_.next_token();
            if (mysql_charset_introducer(collation) ||
                (!mysql_identifier_token(collation) && collation.type != TokenType::TK_STRING &&
                 !Expr::keyword(collation, "BINARY"))) return fail();
            type.len = static_cast<uint32_t>(collation.source.ptr + collation.source.len - type.ptr);
        }
        auto* type_node = make_node(arena_, NodeType::NODE_TYPE_NAME, type);
        if (!type_node) return fail();
        column->add_child(type_node);
        if (word("EXISTS")) column->flags |= 2;
        if (!word("PATH")) return fail();
        auto* path = literal(false);
        if (!path) return fail();
        column->add_child(path);
        unsigned seen = 0;
        while (is("NULL") || is("ERROR") || is("DEFAULT")) {
            const bool null_response = word("NULL");
            const bool error_response = !null_response && word("ERROR");
            AstNode* value = nullptr;
            if (!null_response && !error_response) {
                tok_.skip(); // DEFAULT
                value = literal(true);
                if (!value) return fail();
            }
            if (!take(TokenType::TK_ON)) return fail();
            const bool empty = word("EMPTY");
            if (!empty && !word("ERROR")) return fail();
            const unsigned bit = empty ? 1 : 2;
            if (seen & bit) return fail();
            seen |= bit;
            StringRef phrase = null_response ? (empty ? StringRef{"NULL ON EMPTY", 13} : StringRef{"NULL ON ERROR", 13}) :
                error_response ? (empty ? StringRef{"ERROR ON EMPTY", 14} : StringRef{"ERROR ON ERROR", 14}) :
                (empty ? StringRef{"DEFAULT ON EMPTY", 16} : StringRef{"DEFAULT ON ERROR", 16});
            auto* response = make_node(arena_, NodeType::NODE_MYSQL_JSON_TABLE_RESPONSE, phrase);
            if (!response) return fail();
            if (value) response->add_child(value);
            column->add_child(response);
        }
        return column;
    }
};

} // namespace sql_parser
#endif // SQL_PARSER_MYSQL_JSON_TABLE_PARSER_H
