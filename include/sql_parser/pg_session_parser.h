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
                "DEALLOCATE", "LISTEN", "NOTIFY", "UNLISTEN", "DISCARD", "CHECKPOINT",
                "REINDEX", "CLUSTER", "REFRESH", "LOCK"})
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
        } else if (word(first, "REINDEX")) {
            result.stmt_type = StmtType::REINDEX; reindex(root);
        } else if (word(first, "CLUSTER")) {
            result.stmt_type = StmtType::CLUSTER; cluster(root);
        } else if (word(first, "REFRESH")) {
            result.stmt_type = StmtType::REFRESH_MATERIALIZED_VIEW; refresh(root);
        } else if (word(first, "LOCK")) {
            result.stmt_type = StmtType::LOCK; lock(root);
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
    bool terminal() { return at(TokenType::TK_EOF) || at(TokenType::TK_SEMICOLON); }
    static bool nonreserved(const Token& token) {
        return pg_column_name(token) || pg_type_function_name(token);
    }
    AstNode* qualified_name() {
        AstNode* name = node(NodeType::NODE_PG_DDL_CLAUSE);
        add(name, identifier());
        unsigned parts = 1;
        while (!failed_ && at(TokenType::TK_DOT)) {
            syntax(name);
            if (++parts > 3 || !pg_column_label(tok_.peek())) { fail(); break; }
            add(name, token_node(NodeType::NODE_IDENTIFIER, tok_.next_token()));
        }
        return name;
    }
    static unsigned numeric_digit(char c) {
        return c >= '0' && c <= '9' ? static_cast<unsigned>(c - '0') :
            c >= 'a' && c <= 'f' ? static_cast<unsigned>(c - 'a' + 10) :
            c >= 'A' && c <= 'F' ? static_cast<unsigned>(c - 'A' + 10) : 16;
    }
    static bool numeric_digits(const char*& p, const char* end, unsigned base) {
        if (p == end || numeric_digit(*p) >= base) return false;
        ++p;
        while (p < end) {
            if (numeric_digit(*p) < base) ++p;
            else if (*p == '_' && p + 1 < end && numeric_digit(p[1]) < base) p += 2;
            else break;
        }
        return true;
    }
    void numeric_option(AstNode* option) {
        if (at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS)) syntax(option);
        Token value = tok_.peek();
        const char* begin = value.source.ptr;
        const char* end = tok_.input_end();
        const char* p = begin;
        if (!p || p == end) { fail(); return; }
        bool floating = false;
        unsigned base = 10;
        if (end - p >= 2 && p[0] == '0') {
            if (p[1] == 'x' || p[1] == 'X') base = 16;
            else if (p[1] == 'o' || p[1] == 'O') base = 8;
            else if (p[1] == 'b' || p[1] == 'B') base = 2;
        }
        if (base != 10) {
            p += 2;
            if (p < end && *p == '_') ++p;
            if (!numeric_digits(p, end, base)) { fail(); return; }
        } else {
            bool digits = numeric_digits(p, end, 10);
            if (p < end && *p == '.') {
                floating = true; ++p;
                digits = numeric_digits(p, end, 10) || digits;
            }
            if (!digits) { fail(); return; }
            if (p < end && (*p == 'e' || *p == 'E')) {
                floating = true; ++p;
                if (p < end && (*p == '+' || *p == '-')) ++p;
                if (!numeric_digits(p, end, 10)) { fail(); return; }
            }
        }
        // Consume the source span because the shared tokenizer splits PG base
        // prefixes and digit separators into adjacent tokens.
        while (!failed_ && tok_.peek().source.ptr < p) {
            Token part = tok_.next_token();
            if (part.type == TokenType::TK_ERROR || part.source.ptr + part.source.len > p) fail();
        }
        value.source = value.text = {begin, static_cast<uint32_t>(p - begin)};
        add(option, token_node(floating ? NodeType::NODE_LITERAL_FLOAT : NodeType::NODE_LITERAL_INT, value));
    }
    bool option_string_start() {
        if (at(TokenType::TK_STRING)) return true;
        if (!is("E")) return false;
        auto look = tok_;
        Token prefix = look.next_token();
        Token literal = look.peek();
        return literal.type == TokenType::TK_STRING &&
            prefix.source.ptr + prefix.source.len == literal.source.ptr;
    }
    void utility_options(AstNode* root) {
        require(TokenType::TK_LPAREN);
        AstNode* list = node(NodeType::NODE_PG_DDL_LIST);
        do {
            AstNode* option = node(NodeType::NODE_PG_DDL_CLAUSE);
            Token key = tok_.peek();
            if (!nonreserved(key) && !is("ANALYZE") && !is("ANALYSE")) { fail(); break; }
            add(option, token_node(NodeType::NODE_IDENTIFIER, tok_.next_token()));
            if (!at(TokenType::TK_COMMA) && !at(TokenType::TK_RPAREN)) {
                Token value = tok_.peek();
                if (value.type == TokenType::TK_INTEGER || value.type == TokenType::TK_FLOAT ||
                        value.type == TokenType::TK_DOT || value.type == TokenType::TK_PLUS ||
                        value.type == TokenType::TK_MINUS) numeric_option(option);
                else if (option_string_start()) add(option, string_literal());
                else if (nonreserved(value) || is("TRUE") || is("FALSE") || is("ON"))
                    add(option, token_node(NodeType::NODE_IDENTIFIER, tok_.next_token()));
                else fail();
            }
            add(list, option);
        } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN);
        add(root, list);
    }
    void reindex(AstNode* root) {
        if (at(TokenType::TK_LPAREN)) utility_options(root);
        if (take("INDEX", root) || take("TABLE", root)) {
            take("CONCURRENTLY", root); add(root, qualified_name());
        } else if (take("SCHEMA", root)) {
            take("CONCURRENTLY", root); add(root, identifier());
        } else if (take("DATABASE", root) || take("SYSTEM", root)) {
            take("CONCURRENTLY", root);
            if (!terminal()) add(root, identifier());
        } else fail();
    }
    void cluster(AstNode* root) {
        const bool options = at(TokenType::TK_LPAREN);
        if (options) utility_options(root); else take("VERBOSE", root);
        if (terminal()) return;
        auto look = tok_; look.skip();
        if (!options && word(look.peek(), "ON")) {
            add(root, identifier()); require("ON", root); add(root, qualified_name());
        } else {
            add(root, qualified_name());
            if (take("USING", root)) add(root, identifier());
        }
    }
    void refresh(AstNode* root) {
        require("MATERIALIZED", root); require("VIEW", root);
        take("CONCURRENTLY", root); add(root, qualified_name());
        if (take("WITH", root)) { take("NO", root); require("DATA", root); }
    }
    void lock(AstNode* root) {
        take("TABLE", root);
        AstNode* relations = node(NodeType::NODE_PG_DDL_LIST);
        if (relations) relations->flags = 1;
        do {
            AstNode* relation = node(NodeType::NODE_PG_DDL_CLAUSE);
            if (take("ONLY", relation)) {
                const bool parens = at(TokenType::TK_LPAREN);
                if (parens) syntax(relation);
                add(relation, qualified_name());
                if (parens) {
                    if (at(TokenType::TK_RPAREN)) syntax(relation); else fail();
                }
            } else {
                add(relation, qualified_name());
                if (at(TokenType::TK_ASTERISK)) syntax(relation);
            }
            add(relations, relation);
        } while (!failed_ && take(TokenType::TK_COMMA));
        add(root, relations);
        if (take("IN", root)) {
            if (take("ACCESS", root) || take("ROW", root)) {
                if (!take("SHARE", root)) require("EXCLUSIVE", root);
            } else if (take("SHARE", root)) {
                if (take("UPDATE", root) || take("ROW", root)) require("EXCLUSIVE", root);
            } else require("EXCLUSIVE", root);
            require("MODE", root);
        }
        take("NOWAIT", root);
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
