#ifndef SQL_PARSER_PG_ADMIN_PARSER_H
#define SQL_PARSER_PG_ADMIN_PARSER_H

#include "sql_parser/pg_ddl_parser.h"
#include "sql_parser/pg_integer_literal.h"

namespace sql_parser {

// Role administration and object annotations use validated productions with
// structural names, signatures, option lists and literals.
class PgAdminParser {
public:
    PgAdminParser(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena)
        : tok_(tok), arena_(arena) {}

    static bool word(const Token& token, const char* value) { return PgDdlParser::word(token, value); }
    static bool handles(const Token& first, Tokenizer<Dialect::PostgreSQL> look) {
        if (word(first, "COMMENT") || word(first, "SECURITY")) return true;
        if (!word(first, "CREATE") && !word(first, "ALTER") && !word(first, "DROP")) return false;
        const auto kind = look.next_token();
        if (word(kind, "USER") && word(look.peek(), "MAPPING")) return false;
        return word(kind, "ROLE") || word(kind, "USER") || word(kind, "GROUP");
    }
    ParseResult parse(const Token& first) {
        ParseResult result;
        bool annotation = word(first, "COMMENT") || word(first, "SECURITY");
        auto* root = token_node(annotation ? NodeType::NODE_PG_COMMAND_STMT : NodeType::NODE_PG_DDL_STMT, first);
        if (annotation) {
            const bool security = word(first, "SECURITY");
            result.stmt_type = security ? StmtType::SECURITY_LABEL : StmtType::COMMENT;
            comment(root, security);
        } else {
            result.stmt_type = word(first, "CREATE") ? StmtType::CREATE : word(first, "ALTER") ? StmtType::ALTER : StmtType::DROP;
            role_command(root, result.stmt_type);
        }
        result.ast = root;
        result.status = failed_ || tok_.has_error() ? ParseResult::ERROR : ParseResult::OK;
        return result;
    }

private:
    Tokenizer<Dialect::PostgreSQL>& tok_;
    Arena& arena_;
    bool failed_ = false;
    bool is(const char* value) { return word(tok_.peek(), value); }
    bool at(TokenType type) { return tok_.peek().type == type; }
    bool terminal() { return at(TokenType::TK_EOF) || at(TokenType::TK_SEMICOLON); }
    void fail() { failed_ = true; tok_.flag_error_at(tok_.peek().source); }
    AstNode* node(NodeType type, const char* value = "") {
        auto* result = make_node(arena_, type, {value, static_cast<uint32_t>(std::strlen(value))});
        if (!result) fail();
        return result;
    }
    AstNode* token_node(NodeType type, const Token& token) {
        auto* result = make_node_from_token(arena_, type, token,
            type == NodeType::NODE_IDENTIFIER && token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
        if (!result) fail();
        return result;
    }
    void add(AstNode* parent, AstNode* child) {
        if (!parent || !child) { fail(); return; }
        parent->add_child(child);
    }
    void syntax(AstNode* parent) { add(parent, token_node(NodeType::NODE_PG_DDL_SYNTAX, tok_.next_token())); }
    bool take(const char* value, AstNode* parent = nullptr) {
        if (!is(value)) return false;
        if (parent) syntax(parent); else tok_.skip();
        return true;
    }
    bool take(TokenType type) {
        if (!at(type)) return false;
        tok_.skip(); return true;
    }
    void require(const char* value, AstNode* parent) { if (!take(value, parent)) fail(); }
    void require(TokenType type) { if (!take(type)) fail(); }
    static bool nonreserved(const Token& token) { return pg_column_name(token) || pg_type_function_name(token); }
    AstNode* identifier(bool broad = false, bool label = false) {
        auto token = tok_.peek();
        if (!(label ? pg_column_label(token) : broad ? nonreserved(token) : pg_column_name(token))) { fail(); return nullptr; }
        tok_.skip(); return token_node(NodeType::NODE_IDENTIFIER, token);
    }
    AstNode* name() {
        auto* first = identifier();
        if (!at(TokenType::TK_DOT)) return first;
        auto* qualified = node(NodeType::NODE_QUALIFIED_NAME); add(qualified, first);
        while (!failed_ && take(TokenType::TK_DOT)) add(qualified, identifier(false, true));
        return qualified;
    }
    AstNode* type() {
        ExpressionParser<Dialect::PostgreSQL> expr(tok_, arena_, true);
        auto text = expr.parse_type_name();
        if (text.empty()) { fail(); return nullptr; }
        auto* result = make_node(arena_, NodeType::NODE_TYPE_NAME, text);
        if (!result) fail();
        return result;
    }
    bool string_start() {
        if (at(TokenType::TK_STRING)) return true;
        if (!is("E")) return false;
        auto look = tok_; auto prefix = look.next_token(); auto literal = look.peek();
        return literal.type == TokenType::TK_STRING && prefix.source.ptr + prefix.source.len == literal.source.ptr;
    }
    AstNode* string_literal() {
        if (!string_start()) { fail(); return nullptr; }
        auto start = tok_.next_token();
        auto token = start;
        if (word(start, "E")) token = tok_.next_token();
        auto* result = token_node(NodeType::NODE_LITERAL_STRING, token);
        if (result && start.source.ptr != token.source.ptr)
            result->set_source({start.source.ptr, static_cast<uint32_t>(token.source.ptr + token.source.len - start.source.ptr)});
        return result;
    }
    // The grammar normalizes unquoted identifiers; quoted role names keep case.
    static bool named(const Token& token, const char* text) {
        const auto length = static_cast<uint32_t>(std::strlen(text));
        return token.source.ptr != token.text.ptr ? token.text == StringRef{text, length} : token.text.equals_ci(text, length);
    }
    static bool special_role(const Token& token) {
        return word(token, "CURRENT_USER") || word(token, "CURRENT_ROLE") || word(token, "SESSION_USER");
    }
    AstNode* role(bool concrete = false) {
        auto token = tok_.peek();
        bool special = special_role(token);
        if ((!special && !nonreserved(token)) || named(token, "none") ||
            (concrete && (special || named(token, "public")))) { fail(); return nullptr; }
        tok_.skip(); return token_node(NodeType::NODE_IDENTIFIER, token);
    }
    AstNode* roles() {
        auto* list = node(NodeType::NODE_PG_DDL_LIST); if (list) list->flags = 1;
        do { add(list, role()); } while (!failed_ && take(TokenType::TK_COMMA));
        return list;
    }
    void integer(AstNode* parent, bool signed_value) {
        if (signed_value && (at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS))) syntax(parent);
        Token token;
        if (!pg_integer_literal(tok_, token)) { fail(); return; }
        add(parent, token_node(NodeType::NODE_LITERAL_INT, token));
    }
    void role_command(AstNode* root, StmtType statement) {
        const bool group = is("GROUP"); syntax(root);
        if (statement == StmtType::DROP) {
            if (take("IF", root)) require("EXISTS", root);
            add(root, roles()); return;
        }
        bool all = statement == StmtType::ALTER && !group && take("ALL", root);
        const auto role_token = tok_.peek();
        if (!all) add(root, role(statement == StmtType::CREATE));
        if (failed_) return;
        if (statement == StmtType::ALTER) {
            if (take("RENAME", root)) {
                if (all || special_role(role_token) || named(role_token, "public")) { fail(); return; }
                require("TO", root); add(root, role(true)); return;
            }
            bool database = take("IN", root);
            if (database) { require("DATABASE", root); add(root, identifier()); }
            if (is("SET") || is("RESET")) {
                if (group) { fail(); return; }
                add(root, PgDdlParser(tok_, arena_).parse_setting_clause()); return;
            }
            if (all || database) { fail(); return; }
            if (group) {
                if (!take("ADD", root)) require("DROP", root);
                require("USER", root); add(root, roles()); return;
            }
        }
        take("WITH", root);
        while (!failed_ && !terminal()) {
            auto* option = node(NodeType::NODE_PG_DDL_CLAUSE);
            if (take("ENCRYPTED", option)) { require("PASSWORD", option); add(option, string_literal()); }
            else if (take("PASSWORD", option)) {
                if (!take("NULL", option)) add(option, string_literal());
            } else if (take("CONNECTION", option)) { require("LIMIT", option); integer(option, true); }
            else if (take("VALID", option)) { require("UNTIL", option); add(option, string_literal()); }
            else if (take("USER", option)) add(option, roles());
            else if (statement == StmtType::CREATE && take("SYSID", option)) integer(option, false);
            else if (statement == StmtType::CREATE && (take("ADMIN", option) || take("ROLE", option))) add(option, roles());
            else if (statement == StmtType::CREATE && take("IN", option)) {
                if (!take("ROLE", option)) require("GROUP", option);
                add(option, roles());
            } else {
                if (take("INHERIT", option)) { add(root, option); continue; }
                static constexpr const char* flags[] = {"superuser", "nosuperuser", "createrole", "nocreaterole",
                    "replication", "noreplication", "createdb", "nocreatedb", "login", "nologin", "bypassrls",
                    "nobypassrls", "noinherit"};
                if (!pg_column_label(tok_.peek())) { fail(); return; }
                bool found = false;
                for (auto flag : flags) if (named(tok_.peek(), flag)) { found = true; break; }
                if (!found) { fail(); return; }
                syntax(option);
            }
            add(root, option);
        }
    }
    void annotation_target(AstNode* root, bool security) {
        if (take("TYPE", root) || take("DOMAIN", root)) { add(root, type()); return; }
        if (take("FUNCTION", root) || take("PROCEDURE", root) || take("ROUTINE", root)) {
            add(root, PgDdlParser(tok_, arena_).parse_function_signature()); return;
        }
        if (take("AGGREGATE", root)) { add(root, PgDdlParser(tok_, arena_).parse_aggregate_signature()); return; }
        if (take("LARGE", root)) {
            require("OBJECT", root);
            ExpressionParser<Dialect::PostgreSQL> expr(tok_, arena_, true);
            auto* value = expr.parse_complete(Precedence::UNARY);
            if (value && value->type == NodeType::NODE_UNARY_OP &&
                value->value() != StringRef{"+", 1} && value->value() != StringRef{"-", 1}) { fail(); return; }
            const auto* base = value && value->type == NodeType::NODE_UNARY_OP ? value->first_child : value;
            if (!base || (base->type != NodeType::NODE_LITERAL_INT && base->type != NodeType::NODE_LITERAL_FLOAT)) { fail(); return; }
            add(root, value); return;
        }
        if (!security) {
            if (take("OPERATOR", root)) {
                if (take("CLASS", root) || take("FAMILY", root)) {
                    add(root, name()); require("USING", root); add(root, identifier());
                } else add(root, PgDdlParser(tok_, arena_).parse_operator_signature());
                return;
            }
            if (is("CONSTRAINT") || is("POLICY") || is("RULE") || is("TRIGGER")) {
                bool constraint = is("CONSTRAINT"); syntax(root); add(root, identifier()); require("ON", root);
                if (constraint) take("DOMAIN", root);
                add(root, name()); return;
            }
            if (take("CAST", root)) {
                require(TokenType::TK_LPAREN);
                auto* cast = node(NodeType::NODE_PG_DDL_LIST);
                auto* body = node(NodeType::NODE_PG_DDL_CLAUSE);
                add(body, type()); require("AS", body); add(body, type());
                require(TokenType::TK_RPAREN); add(cast, body); add(root, cast); return;
            }
            if (take("TRANSFORM", root)) {
                require("FOR", root); add(root, type()); require("LANGUAGE", root); add(root, identifier()); return;
            }
        }
        bool qualified = true;
        if (take("TABLE", root) || take("SEQUENCE", root) || take("VIEW", root) || take("INDEX", root) ||
            take("COLUMN", root) || take("COLLATION", root) || take("CONVERSION", root) || take("STATISTICS", root)) {}
        else if (take("MATERIALIZED", root)) require("VIEW", root);
        else if (take("TEXT", root)) {
            require("SEARCH", root);
            if (!take("PARSER", root) && !take("DICTIONARY", root) && !take("TEMPLATE", root)) require("CONFIGURATION", root);
        } else if (take("FOREIGN", root)) {
            if (!take("TABLE", root)) { require("DATA", root); require("WRAPPER", root); qualified = false; }
        } else {
            qualified = false;
            if (take("ACCESS", root)) require("METHOD", root);
            else if (take("EVENT", root)) require("TRIGGER", root);
            else if (take("PROCEDURAL", root)) require("LANGUAGE", root);
            else if (!take("DATABASE", root) && !take("ROLE", root) && !take("SUBSCRIPTION", root) &&
                     !take("TABLESPACE", root) && !take("EXTENSION", root) && !take("LANGUAGE", root) &&
                     !take("PUBLICATION", root) && !take("SCHEMA", root) && !take("SERVER", root)) { fail(); return; }
        }
        add(root, qualified ? name() : identifier());
    }
    void comment(AstNode* root, bool security) {
        if (security) {
            require("LABEL", root);
            if (take("FOR", root)) add(root, string_start() ? string_literal() : identifier(true));
        }
        require("ON", root); if (failed_) return;
        annotation_target(root, security); if (failed_) return;
        require("IS", root);
        if (take("NULL", root)) return;
        add(root, string_literal());
    }
};

} // namespace sql_parser
#endif
