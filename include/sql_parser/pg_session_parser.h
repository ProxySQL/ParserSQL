#ifndef SQL_PARSER_PG_SESSION_PARSER_H
#define SQL_PARSER_PG_SESSION_PARSER_H

#include "sql_parser/subquery_parse_callback.h"
#include "sql_parser/parse_result.h"
#include "sql_parser/pg_integer_literal.h"

namespace sql_parser {

// PostgreSQL session commands retain typed operands and native query trees.
class PgSessionParser {
public:
    PgSessionParser(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena)
        : tok_(tok), arena_(arena) {}

    static bool handles(const Token& first) {
        for (const char* keyword : {"DECLARE", "FETCH", "MOVE", "CLOSE", "PREPARE", "EXECUTE",
                "DEALLOCATE", "LISTEN", "NOTIFY", "UNLISTEN", "DISCARD", "CHECKPOINT"})
            if (word(first, keyword)) return true;
        return false;
    }

    ParseResult parse(const Token& first) {
        ParseResult result;
        AstNode* root = token_node(NodeType::NODE_PG_COMMAND_STMT, first);
        if (word(first, "DECLARE")) {
            result.stmt_type = StmtType::DECLARE_CURSOR; declare_cursor(root);
        } else if (word(first, "FETCH") || word(first, "MOVE")) {
            result.stmt_type = word(first, "MOVE") ? StmtType::MOVE : StmtType::FETCH;
            fetch(root);
        } else if (word(first, "CLOSE")) {
            result.stmt_type = StmtType::CLOSE;
            if (!take("ALL", root)) add(root, identifier());
        } else if (word(first, "PREPARE")) {
            result.stmt_type = StmtType::PREPARE; prepare(root);
        } else if (word(first, "EXECUTE")) {
            result.stmt_type = StmtType::EXECUTE; execute(root);
        } else if (word(first, "DEALLOCATE")) {
            result.stmt_type = StmtType::DEALLOCATE;
            auto look = tok_; look.skip();
            if (look.peek().type != TokenType::TK_EOF && look.peek().type != TokenType::TK_SEMICOLON)
                take("PREPARE", root);
            if (!take("ALL", root)) add(root, identifier());
        } else if (word(first, "LISTEN")) {
            result.stmt_type = StmtType::LISTEN; add(root, identifier());
        } else if (word(first, "NOTIFY")) {
            result.stmt_type = StmtType::NOTIFY; add(root, identifier());
            if (at(TokenType::TK_COMMA)) { syntax(root); add(root, string_literal()); }
        } else if (word(first, "UNLISTEN")) {
            result.stmt_type = StmtType::UNLISTEN;
            if (at(TokenType::TK_ASTERISK)) syntax(root); else add(root, identifier());
        } else if (word(first, "DISCARD")) {
            result.stmt_type = StmtType::DISCARD;
            if (!take("ALL", root) && !take("TEMP", root) && !take("TEMPORARY", root) &&
                    !take("PLANS", root) && !take("SEQUENCES", root)) fail();
        } else if (word(first, "CHECKPOINT")) result.stmt_type = StmtType::CHECKPOINT;
        else fail();
        if (!at(TokenType::TK_EOF) && !at(TokenType::TK_SEMICOLON)) fail();
        result.ast = root;
        result.status = failed_ || tok_.has_error() ? ParseResult::ERROR : ParseResult::OK;
        return result;
    }

private:
    Tokenizer<Dialect::PostgreSQL>& tok_;
    Arena& arena_;
    bool failed_ = false;

    static bool word(const Token& token, const char* value) {
        return token.source.ptr == token.text.ptr &&
            token.text.equals_ci(value, static_cast<uint32_t>(std::strlen(value)));
    }
    bool is(const char* value) { return word(tok_.peek(), value); }
    bool at(TokenType type) { return tok_.peek().type == type; }
    void fail() { failed_ = true; tok_.flag_error_at(tok_.peek().source); }
    AstNode* node(NodeType type) {
        AstNode* n = make_node(arena_, type);
        if (!n) fail();
        return n;
    }
    AstNode* token_node(NodeType type, const Token& token) {
        AstNode* n = make_node_from_token(arena_, type, token);
        if (!n) fail();
        else if (type == NodeType::NODE_IDENTIFIER && token.source.ptr != token.text.ptr)
            n->flags |= FLAG_IDENT_DELIMITED;
        return n;
    }
    void add(AstNode* parent, AstNode* child) {
        if (!parent || !child) { fail(); return; }
        parent->add_child(child);
    }
    void syntax(AstNode* parent) { add(parent, token_node(NodeType::NODE_PG_DDL_SYNTAX, tok_.next_token())); }
    bool take(const char* value, AstNode* parent) {
        if (!is(value)) return false;
        syntax(parent); return true;
    }
    bool take(TokenType type) {
        if (!at(type)) return false;
        tok_.skip(); return true;
    }
    void require(const char* value, AstNode* parent) { if (!take(value, parent)) fail(); }
    void require(TokenType type) { if (!take(type)) fail(); }
    AstNode* identifier() {
        Token token = tok_.peek();
        if (!pg_column_name(token)) { fail(); return nullptr; }
        tok_.skip(); return token_node(NodeType::NODE_IDENTIFIER, token);
    }
    void declare_cursor(AstNode* root) {
        add(root, identifier());
        while (!failed_) {
            if (take("NO", root)) require("SCROLL", root);
            else if (!take("SCROLL", root) && !take("BINARY", root) &&
                    !take("ASENSITIVE", root) && !take("INSENSITIVE", root)) break;
        }
        require("CURSOR", root);
        if (take("WITH", root) || take("WITHOUT", root)) require("HOLD", root);
        require("FOR", root);
        if (!failed_) add(root, parse_subquery_select<Dialect::PostgreSQL>(tok_, arena_));
    }
    bool integer_start() {
        return at(TokenType::TK_INTEGER) || at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS);
    }
    void signed_integer(AstNode* root) {
        if (at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS)) syntax(root);
        Token token;
        if (!pg_integer_literal(tok_, token)) { fail(); return; }
        add(root, token_node(NodeType::NODE_LITERAL_INT, token));
    }
    void fetch(AstNode* root) {
        // Direction words are also unreserved cursor names when used alone.
        auto look = tok_;
        Token candidate = look.next_token();
        if (pg_column_name(candidate) && (look.peek().type == TokenType::TK_EOF ||
                look.peek().type == TokenType::TK_SEMICOLON)) {
            add(root, identifier()); return;
        }
        if (take("ABSOLUTE", root) || take("RELATIVE", root)) signed_integer(root);
        else if (take("FORWARD", root) || take("BACKWARD", root)) {
            if (!take("ALL", root) && integer_start()) signed_integer(root);
        } else if (!take("NEXT", root) && !take("PRIOR", root) && !take("FIRST", root) &&
                   !take("LAST", root) && !take("ALL", root) && integer_start()) signed_integer(root);
        if (!take("FROM", root)) take("IN", root);
        add(root, identifier());
    }
    void prepare(AstNode* root) {
        add(root, identifier());
        if (take(TokenType::TK_LPAREN)) {
            AstNode* list = node(NodeType::NODE_PG_DDL_LIST);
            do {
                StringRef text = PgTypeParser(tok_).parse();
                if (text.empty()) { fail(); break; }
                add(list, make_node(arena_, NodeType::NODE_TYPE_NAME, text));
            } while (!failed_ && take(TokenType::TK_COMMA));
            require(TokenType::TK_RPAREN); add(root, list);
        }
        require("AS", root);
        if (!failed_) add(root, parse_pg_statement(tok_, arena_, true));
    }
    void execute(AstNode* root) {
        add(root, identifier());
        if (take(TokenType::TK_LPAREN)) {
            AstNode* list = node(NodeType::NODE_PG_DDL_LIST);
            do {
                ExpressionParser<Dialect::PostgreSQL> parser(tok_, arena_, true);
                parser.set_subquery_callback(&parse_subquery_select<Dialect::PostgreSQL>);
                AstNode* value = parser.parse_complete();
                if (!value || value->type == NodeType::NODE_ASTERISK) { fail(); break; }
                add(list, value);
            } while (!failed_ && take(TokenType::TK_COMMA));
            require(TokenType::TK_RPAREN); add(root, list);
        }
    }
    AstNode* string_literal() {
        if (is("E")) {
            Token prefix = tok_.next_token(), literal = tok_.peek();
            if (literal.type != TokenType::TK_STRING || prefix.source.ptr + prefix.source.len != literal.source.ptr) {
                fail(); return nullptr;
            }
            tok_.skip(); literal.source = {prefix.source.ptr, prefix.source.len + literal.source.len};
            return token_node(NodeType::NODE_LITERAL_STRING, literal);
        }
        if (!at(TokenType::TK_STRING)) { fail(); return nullptr; }
        return token_node(NodeType::NODE_LITERAL_STRING, tok_.next_token());
    }
};

} // namespace sql_parser
#endif // SQL_PARSER_PG_SESSION_PARSER_H
