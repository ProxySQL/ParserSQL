#ifndef SQL_PARSER_PG_DDL_PARSER_H
#define SQL_PARSER_PG_DDL_PARSER_H

#include "sql_parser/subquery_parse_callback.h"
#include "sql_parser/parse_result.h"
#include "sql_parser/pg_integer_literal.h"

namespace sql_parser {

// Explicit PostgreSQL DDL productions. Clauses carry their keyword prefix and
// typed operands; lists retain their grouping. No production accepts an opaque
// statement tail. Function bodies are SQL string literals, as in PostgreSQL's
// outer grammar; procedural language validation belongs to the server.
class PgDdlParser {
public:
    PgDdlParser(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena,
                SubqueryParseCallback<Dialect::PostgreSQL> callback = nullptr)
        : tok_(tok), arena_(arena), callback_(callback ? callback : &parse_subquery_select<Dialect::PostgreSQL>) {}

    static bool word(const Token& token, const char* value) {
        return token.source.ptr == token.text.ptr &&
            token.text.equals_ci(value, static_cast<uint32_t>(std::strlen(value)));
    }
    static bool handles(const Token& first) {
        return word(first, "CREATE") || word(first, "ALTER") || word(first, "DROP") ||
            word(first, "GRANT") || word(first, "REVOKE") || word(first, "VACUUM") ||
            word(first, "ANALYZE") || word(first, "ANALYSE") || word(first, "TRUNCATE");
    }
    ParseResult parse(const Token& first) {
        ParseResult result;
        AstNode* root = token_node(NodeType::NODE_PG_DDL_STMT, first);
        if (word(first, "CREATE")) { result.stmt_type = StmtType::CREATE; create(root); }
        else if (word(first, "ALTER")) { result.stmt_type = StmtType::ALTER; alter(root); }
        else if (word(first, "DROP")) { result.stmt_type = StmtType::DROP; drop(root); }
        else if (word(first, "GRANT") || word(first, "REVOKE")) {
            bool revoke = word(first, "REVOKE");
            result.stmt_type = revoke ? StmtType::REVOKE : StmtType::GRANT;
            grant(root, revoke);
        } else if (word(first, "TRUNCATE")) {
            result.stmt_type = StmtType::TRUNCATE; truncate(root);
        } else {
            bool vacuum = word(first, "VACUUM");
            result.stmt_type = vacuum ? StmtType::VACUUM : StmtType::ANALYZE;
            maintenance(root, vacuum);
        }
        result.ast = root;
        result.table_name = table_name_;
        result.schema_name = schema_name_;
        result.status = failed_ || tok_.has_error() ? ParseResult::ERROR : ParseResult::OK;
        return result;
    }

    // Shared native subproductions used by COMMENT and role settings.
    AstNode* parse_setting_clause() {
        auto* c = clause();
        bool reset = take("RESET", c);
        if (!reset) require("SET", c);
        routine_setting(c, reset);
        return failed_ ? nullptr : c;
    }
    AstNode* parse_function_signature() {
        auto* c = clause(); add(c, routine_name(true));
        if (at(TokenType::TK_LPAREN)) add(c, arguments(false));
        return failed_ ? nullptr : c;
    }
    AstNode* parse_aggregate_signature() {
        auto* c = clause(); add(c, routine_name()); add(c, aggregate_arguments());
        return failed_ ? nullptr : c;
    }
    AstNode* parse_operator_signature() {
        auto* c = clause(); add(c, operator_name()); add(c, operator_arguments());
        return failed_ ? nullptr : c;
    }

private:
    Tokenizer<Dialect::PostgreSQL>& tok_;
    Arena& arena_;
    SubqueryParseCallback<Dialect::PostgreSQL> callback_;
    bool failed_ = false;
    StringRef table_name_, schema_name_;

    static AstNode* last(AstNode* n) {
        AstNode* child = n ? n->first_child : nullptr;
        while (child && child->next_sibling) child = child->next_sibling;
        return child;
    }
    bool is(const char* text) { return word(tok_.peek(), text); }
    bool at(TokenType type) { return tok_.peek().type == type; }
    void fail() { failed_ = true; tok_.flag_error_at(tok_.peek().source); }
    AstNode* node(NodeType type, const char* value = "") {
        AstNode* n = make_node(arena_, type, {value, static_cast<uint32_t>(std::strlen(value))});
        if (!n) fail();
        return n;
    }
    AstNode* clause(const char* prefix = "") { return node(NodeType::NODE_PG_DDL_CLAUSE, prefix); }
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
    bool take(const char* text, AstNode* parent = nullptr) {
        if (!is(text)) return false;
        if (parent) syntax(parent); else tok_.skip();
        return true;
    }
    bool take(TokenType type) {
        if (!at(type)) return false;
        tok_.skip(); return true;
    }
    void require(const char* text, AstNode* parent = nullptr) { if (!take(text, parent)) fail(); }
    void require(TokenType type) { if (!take(type)) fail(); }
    bool terminal() { return at(TokenType::TK_EOF) || at(TokenType::TK_SEMICOLON); }
    void if_exists(AstNode* parent, bool create = false) {
        if (take("IF", parent)) {
            if (create) require("NOT", parent);
            require("EXISTS", parent);
        }
    }
    void behavior(AstNode* parent) { if (!take("CASCADE", parent)) take("RESTRICT", parent); }
    AstNode* identifier(bool label = false) {
        Token t = tok_.peek();
        if (!(label ? pg_column_label(t) : pg_column_name(t))) { fail(); return nullptr; }
        tok_.skip(); return token_node(NodeType::NODE_IDENTIFIER, t);
    }
    AstNode* name(bool relation = false) {
        AstNode* n = identifier();
        if (take(TokenType::TK_DOT)) {
            AstNode* q = node(NodeType::NODE_QUALIFIED_NAME);
            add(q, n); add(q, identifier(true));
            while (take(TokenType::TK_DOT) && !failed_) add(q, identifier(true));
            n = q;
        }
        if (relation && table_name_.empty() && n && !failed_) {
            if (n->type == NodeType::NODE_QUALIFIED_NAME) {
                if (!n->first_child) { fail(); return n; }
                auto* c = n->first_child;
                schema_name_ = c->value();
                while (c->next_sibling) c = c->next_sibling;
                table_name_ = c->value();
            } else table_name_ = n->value();
        }
        return n;
    }
    AstNode* type() {
        StringRef text = PgTypeParser(tok_).parse();
        if (text.empty()) { fail(); return nullptr; }
        auto* n = make_node(arena_, NodeType::NODE_TYPE_NAME, text);
        if (!n) fail();
        return n;
    }
    AstNode* expr(Precedence minimum = Precedence::NONE) {
        ExpressionParser<Dialect::PostgreSQL> parser(tok_, arena_, true);
        parser.set_subquery_callback(callback_);
        AstNode* n = parser.parse_complete(minimum);
        if (!n || n->type == NodeType::NODE_ASTERISK) { fail(); return nullptr; }
        return n;
    }
    static bool restricted_default_operand(const AstNode* n) {
        if (!n || n->type == NodeType::NODE_ASTERISK || n->type == NodeType::NODE_IS_NULL ||
            n->type == NodeType::NODE_IS_NOT_NULL || n->type == NodeType::NODE_IN_LIST ||
            n->type == NodeType::NODE_BETWEEN || n->type == NodeType::NODE_PG_TIME_ZONE ||
            n->type == NodeType::NODE_PG_QUANTIFIED_OPERAND) return false;
        if (n->type == NodeType::NODE_BINARY_OP || n->type == NodeType::NODE_UNARY_OP || n->type == NodeType::NODE_TYPE_CAST) {
            for (const char* op : {"NOT", "AND", "OR", "COLLATE", "LIKE", "ILIKE", "SIMILAR TO"})
                if (n->value().equals_ci(op, static_cast<uint32_t>(std::strlen(op)))) return false;
            for (const AstNode* c = n->first_child; c; c = c->next_sibling)
                if (!restricted_default_operand(c)) return false;
        }
        return true; // Parentheses/functions contain independent full expressions.
    }
    AstNode* default_expr() {
        ExpressionParser<Dialect::PostgreSQL> parser(tok_, arena_, true);
        parser.set_subquery_callback(callback_);
        AstNode* n = parser.parse_json_xml_restricted();
        if (!restricted_default_operand(n)) { fail(); return nullptr; }
        return n;
    }
    bool string_start() {
        if (at(TokenType::TK_STRING)) return true;
        if (!is("E")) return false;
        auto look = tok_; Token prefix = look.next_token(); const Token& literal = look.peek();
        return literal.type == TokenType::TK_STRING && prefix.source.ptr + prefix.source.len == literal.source.ptr;
    }
    AstNode* string_literal() {
        if (is("E")) {
            Token prefix = tok_.next_token();
            Token literal = tok_.peek();
            if (literal.type != TokenType::TK_STRING || prefix.source.ptr + prefix.source.len != literal.source.ptr) {
                fail(); return nullptr;
            }
            tok_.skip();
            literal.source = {prefix.source.ptr, prefix.source.len + literal.source.len};
            return token_node(NodeType::NODE_LITERAL_STRING, literal);
        }
        if (!at(TokenType::TK_STRING)) { fail(); return nullptr; }
        return token_node(NodeType::NODE_LITERAL_STRING, tok_.next_token());
    }
    AstNode* names(bool parentheses = true, bool qualified = false) {
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        if (parentheses) require(TokenType::TK_LPAREN); else if (list) list->flags = 1;
        do { add(list, qualified ? name() : identifier()); }
        while (!failed_ && take(TokenType::TK_COMMA));
        if (parentheses) require(TokenType::TK_RPAREN);
        return list;
    }
    AstNode* expression_group() {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        add(list, expr());
        require(TokenType::TK_RPAREN);
        return list;
    }
    AstNode* options(bool assignments = true) {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        do {
            auto* option = clause();
            add(option, name());
            if (take(TokenType::TK_EQUAL)) {
                add(option, node(NodeType::NODE_PG_DDL_SYNTAX, "="));
                option_value(option);
            } else if (assignments) fail();
            add(list, option);
        } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN);
        return list;
    }
    void option_value(AstNode* option) {
        if (string_start()) { add(option, string_literal()); return; }
        if (at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS)) syntax(option);
        Token t = tok_.peek();
        if (t.type == TokenType::TK_INTEGER || t.type == TokenType::TK_FLOAT || pg_column_label(t)) syntax(option);
        else fail();
    }
    void deferrability(AstNode* c) {
        for (unsigned i = 0; i < 2 && !failed_; ++i) {
            if (take("DEFERRABLE", c)) {}
            else if (is("NOT")) {
                auto look = tok_; look.skip();
                if (!word(look.peek(), "DEFERRABLE")) break;
                syntax(c); require("DEFERRABLE", c);
            } else if (take("INITIALLY", c)) {
                if (!take("DEFERRED", c)) require("IMMEDIATE", c);
            } else break;
        }
    }
    void references(AstNode* c) {
        require("REFERENCES", c); add(c, name());
        if (at(TokenType::TK_LPAREN)) add(c, names());
        if (take("MATCH", c)) {
            if (!take("FULL", c) && !take("PARTIAL", c)) require("SIMPLE", c);
        }
        unsigned actions = 0;
        while (take("ON", c) && !failed_) {
            unsigned bit = 0;
            if (take("UPDATE", c)) bit = 1;
            else { require("DELETE", c); bit = 2; }
            if (actions & bit) fail();
            actions |= bit;
            if (take("NO", c)) require("ACTION", c);
            else if (take("SET", c)) {
                if (!take("NULL", c)) require("DEFAULT", c);
                if (at(TokenType::TK_LPAREN)) add(c, names());
            } else if (!take("CASCADE", c)) require("RESTRICT", c);
        }
    }
    bool constraint_start() {
        return is("CONSTRAINT") || is("PRIMARY") || is("UNIQUE") || is("CHECK") || is("FOREIGN");
    }
    AstNode* constraint(bool table) {
        auto* c = clause();
        if (take("CONSTRAINT", c)) add(c, identifier());
        bool primary_key = is("PRIMARY");
        if (take("CHECK", c)) {
            add(c, expression_group());
            if (take("NO", c)) require("INHERIT", c);
        } else if (take("PRIMARY", c) || take("UNIQUE", c)) {
            if (c && last(c) && last(c)->value().equals_ci("PRIMARY", 7)) require("KEY", c);
            if (take("NULLS", c)) {
                if (primary_key) fail();
                take("NOT", c); require("DISTINCT", c);
            }
            if (table) add(c, names());
            if (take("INCLUDE", c)) add(c, names());
            if (take("WITH", c)) add(c, options());
            if (take("USING", c)) { require("INDEX", c); require("TABLESPACE", c); add(c, identifier()); }
        } else if (table && take("FOREIGN", c)) {
            require("KEY", c); add(c, names()); references(c);
        } else if (!table && is("REFERENCES")) references(c);
        else if (!table && take("NOT", c)) {
            require("NULL", c); if (take("NO", c)) require("INHERIT", c);
        }
        else if (!table && take("NULL", c)) {}
        else if (!table && take("DEFAULT", c)) add(c, default_expr());
        else if (!table && take("GENERATED", c)) {
            bool always = take("ALWAYS", c);
            if (!always) { require("BY", c); require("DEFAULT", c); }
            require("AS", c);
            if (take("IDENTITY", c)) {
                if (at(TokenType::TK_LPAREN)) identity_options(c);
            } else {
                if (!always) fail();
                add(c, expression_group());
                if (!take("STORED", c)) take("VIRTUAL", c);
            }
        } else fail();
        deferrability(c);
        if (!table) {
            if (!take("ENFORCED", c) && is("NOT")) {
                auto look = tok_; look.skip();
                if (word(look.peek(), "ENFORCED")) { syntax(c); require("ENFORCED", c); }
            }
            deferrability(c);
        }
        return c;
    }
    void identity_options(AstNode* parent) {
        require(TokenType::TK_LPAREN);
        auto* group = node(NodeType::NODE_PG_DDL_LIST);
        auto* sequence = clause();
        while (!at(TokenType::TK_RPAREN) && !failed_) {
            if (take("START", sequence)) { take("WITH", sequence); integer(sequence); }
            else if (take("INCREMENT", sequence)) { take("BY", sequence); integer(sequence); }
            else if (take("MINVALUE", sequence) || take("MAXVALUE", sequence) || take("CACHE", sequence)) integer(sequence);
            else if (take("NO", sequence)) {
                if (!take("MINVALUE", sequence) && !take("MAXVALUE", sequence)) require("CYCLE", sequence);
            } else if (!take("CYCLE", sequence)) fail();
        }
        if (sequence && !sequence->first_child) fail();
        add(group, sequence); add(parent, group); require(TokenType::TK_RPAREN);
    }
    void integer(AstNode* parent) {
        if (at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS)) syntax(parent);
        if (at(TokenType::TK_INTEGER)) syntax(parent); else fail();
    }
    AstNode* column() {
        auto* c = clause(); add(c, identifier()); add(c, type());
        if (take("COLLATE", c)) add(c, name());
        while (!failed_ && (constraint_start() || is("NOT") || is("NULL") || is("DEFAULT") ||
               is("GENERATED") || is("REFERENCES"))) add(c, constraint(false));
        return c;
    }
    AstNode* table_elements() {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        if (!at(TokenType::TK_RPAREN)) {
            do {
                if (is("LIKE")) {
                    auto* c = clause(); syntax(c); add(c, name());
                    while (!failed_ && (is("INCLUDING") || is("EXCLUDING"))) {
                        syntax(c);
                        if (!take("ALL", c) && !take("COMMENTS", c) && !take("COMPRESSION", c) &&
                            !take("CONSTRAINTS", c) && !take("DEFAULTS", c) && !take("GENERATED", c) &&
                            !take("IDENTITY", c) && !take("INDEXES", c) && !take("STATISTICS", c) &&
                            !take("STORAGE", c)) fail();
                    }
                    add(list, c);
                } else add(list, constraint_start() ? constraint(true) : column());
            } while (!failed_ && take(TokenType::TK_COMMA));
        }
        require(TokenType::TK_RPAREN);
        return list;
    }
    // Object DDL uses the same explicit clause/list nodes as table DDL.
    // NonReservedWord includes both PostgreSQL identifier keyword categories.
    static bool nonreserved(const Token& t) {
        return pg_column_name(t) || pg_type_function_name(t);
    }
    AstNode* nonreserved_name() {
        if (!nonreserved(tok_.peek())) { fail(); return nullptr; }
        return token_node(NodeType::NODE_IDENTIFIER, tok_.next_token());
    }
    void numeric(AstNode* parent) {
        if (at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS)) syntax(parent);
        Token t = tok_.peek();
        if (t.type != TokenType::TK_INTEGER && t.type != TokenType::TK_FLOAT) { fail(); return; }
        add(parent, token_node(t.type == TokenType::TK_INTEGER ? NodeType::NODE_LITERAL_INT : NodeType::NODE_LITERAL_FLOAT, tok_.next_token()));
    }
    bool numeric_start() {
        return at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS) || at(TokenType::TK_INTEGER) || at(TokenType::TK_FLOAT);
    }
    AstNode* simple_type() {
        ExpressionParser<Dialect::PostgreSQL> parser(tok_, arena_, true);
        parser.set_subquery_callback(callback_);
        StringRef text = parser.parse_type_name(false);
        if (text.empty()) { fail(); return nullptr; }
        auto* n = make_node(arena_, NodeType::NODE_TYPE_NAME, text);
        if (!n) fail();
        return n;
    }
    AstNode* variable_name() {
        auto* first = identifier();
        if (!at(TokenType::TK_DOT)) return first;
        auto* qualified = node(NodeType::NODE_QUALIFIED_NAME); add(qualified, first);
        while (!failed_ && take(TokenType::TK_DOT)) add(qualified, identifier());
        return qualified;
    }
    void variable_value(AstNode* parent) {
        if (string_start()) add(parent, string_literal());
        else if (numeric_start()) numeric(parent);
        else if (!take("TRUE", parent) && !take("FALSE", parent) && !take("ON", parent)) add(parent, nonreserved_name());
    }
    void routine_setting(AstNode* parent, bool reset) {
        auto look = tok_; look.skip(); const Token next = look.peek();
        if (reset) {
            if (take("ALL", parent)) return;
            if (is("TIME") && word(next, "ZONE")) { syntax(parent); require("ZONE", parent); return; }
            if (is("SESSION") && word(next, "AUTHORIZATION")) { syntax(parent); require("AUTHORIZATION", parent); return; }
            if (is("TRANSACTION") && word(next, "ISOLATION")) { syntax(parent); require("ISOLATION", parent); require("LEVEL", parent); return; }
            add(parent, variable_name()); return;
        }
        // Generic GUC names may also be the first word of a special SET form.
        bool generic = next.type == TokenType::TK_DOT || next.type == TokenType::TK_EQUAL ||
            word(next, "TO") || word(next, "FROM");
        if (!generic) {
        if (take("TIME", parent)) {
            require("ZONE", parent);
            if (take("LOCAL", parent) || take("DEFAULT", parent)) return;
            if (is("INTERVAL")) {
                // zone_value permits qualifiers whose mask contains only HOUR/MINUTE.
                auto* interval = expr();
                if (!interval || interval->type != NodeType::NODE_PG_INTERVAL) { fail(); return; }
                auto* amount = interval->first_child;
                auto* qualifier = amount ? amount->next_sibling : nullptr;
                if (qualifier && !qualifier->value().equals_ci("HOUR", 4) &&
                    !qualifier->value().equals_ci("HOUR TO MINUTE", 14) &&
                    !qualifier->value().equals_ci("MINUTE", 6)) { fail(); return; }
                add(parent, interval);
            } else if (numeric_start()) numeric(parent);
            else if (string_start()) add(parent, string_literal());
            else if (at(TokenType::TK_IDENTIFIER)) add(parent, identifier());
            else fail();
            return;
        }
        if (take("SCHEMA", parent)) { add(parent, string_literal()); return; }
        if (take("NAMES", parent)) {
            if (string_start()) add(parent, string_literal());
            else take("DEFAULT", parent);
            return;
        }
        if (take("ROLE", parent)) {
            if (string_start()) add(parent, string_literal());
            else add(parent, nonreserved_name());
            return;
        }
        if (take("SESSION", parent)) {
            require("AUTHORIZATION", parent);
            if (!take("DEFAULT", parent)) {
                if (string_start()) add(parent, string_literal());
                else add(parent, nonreserved_name());
            }
            return;
        }
        if (take("XML", parent)) {
            require("OPTION", parent); if (!take("DOCUMENT", parent)) require("CONTENT", parent); return;
        }
        if (take("TRANSACTION", parent)) {
            require("SNAPSHOT", parent); add(parent, string_literal()); return;
        }
        }
        add(parent, variable_name());
        if (take("FROM", parent)) { require("CURRENT", parent); return; }
        if (!take("TO", parent)) {
            require(TokenType::TK_EQUAL); add(parent, node(NodeType::NODE_PG_DDL_SYNTAX, "="));
        }
        if (take("DEFAULT", parent)) return;
        auto* values = node(NodeType::NODE_PG_DDL_LIST); if (values) values->flags = 1;
        do { auto* value = clause(); variable_value(value); add(values, value); }
        while (!failed_ && take(TokenType::TK_COMMA));
        add(parent, values);
    }
    bool routine_property(AstNode* parent) {
        auto* c = clause();
        if (take("IMMUTABLE", c) || take("STABLE", c) || take("VOLATILE", c) ||
            take("STRICT", c) || take("LEAKPROOF", c)) {}
        else if (take("NOT", c)) require("LEAKPROOF", c);
        else if (take("CALLED", c)) { require("ON", c); require("NULL", c); require("INPUT", c); }
        else if (take("RETURNS", c)) { require("NULL", c); require("ON", c); require("NULL", c); require("INPUT", c); }
        else if (take("EXTERNAL", c) || is("SECURITY")) {
            require("SECURITY", c); if (!take("DEFINER", c)) require("INVOKER", c);
        } else if (take("COST", c) || take("ROWS", c)) numeric(c);
        else if (take("SUPPORT", c)) add(c, name());
        else if (take("PARALLEL", c)) add(c, identifier());
        else if (take("SET", c)) routine_setting(c, false);
        else if (take("RESET", c)) routine_setting(c, true);
        else return false;
        add(parent, c); return true;
    }
    // Name-only changes are standalone statements, not property-list entries.
    bool object_identity(AstNode* root, bool schema = true, bool owner = true) {
        if (take("RENAME", root)) { require("TO", root); add(root, identifier()); return true; }
        if (owner && take("OWNER", root)) { require("TO", root); role(root); return true; }
        auto look = tok_; look.skip();
        if (schema && is("SET") && word(look.peek(), "SCHEMA")) {
            look.skip();
            if (look.peek().type == TokenType::TK_STRING || look.peek().type == TokenType::TK_EQUAL ||
                look.peek().type == TokenType::TK_DOT ||
                word(look.peek(), "TO") || word(look.peek(), "FROM")) return false; // routine GUC syntax
            syntax(root); require("SCHEMA", root); add(root, identifier()); return true;
        }
        return false;
    }
    void alter_routine(AstNode* root) {
        // function_with_argtypes permits omitted signatures and both keyword categories.
        auto* function = nonreserved_name();
        if (at(TokenType::TK_DOT)) {
            auto* qualified = node(NodeType::NODE_QUALIFIED_NAME); add(qualified, function);
            while (!failed_ && take(TokenType::TK_DOT)) add(qualified, identifier(true));
            function = qualified;
        }
        add(root, function);
        if (at(TokenType::TK_LPAREN)) add(root, arguments(false));
        if (object_identity(root)) return;
        if (take("NO", root) || is("DEPENDS")) {
            require("DEPENDS", root); require("ON", root); require("EXTENSION", root); add(root, identifier()); return;
        }
        bool had = false;
        while (!failed_ && routine_property(root)) had = true;
        if (!had) fail();
        take("RESTRICT", root);
    }
    void create_domain(AstNode* root) {
        add(root, name()); take("AS", root); add(root, type());
        while (!failed_) {
            if (take("COLLATE", root)) add(root, name());
            else if (constraint_start() || is("NOT") || is("NULL") || is("DEFAULT") || is("GENERATED") || is("REFERENCES"))
                add(root, constraint(false));
            else break;
        }
    }
    void alter_domain(AstNode* root) {
        add(root, name());
        if (is("RENAME")) {
            syntax(root);
            if (take("CONSTRAINT", root)) add(root, identifier());
            require("TO", root); add(root, identifier()); return;
        }
        if (object_identity(root)) return;
        if (take("SET", root)) {
            if (take("DEFAULT", root)) add(root, expr());
            else { require("NOT", root); require("NULL", root); }
        } else if (take("DROP", root)) {
            if (take("CONSTRAINT", root)) { if_exists(root); add(root, identifier()); behavior(root); }
            else if (take("NOT", root)) require("NULL", root);
            else require("DEFAULT", root);
        } else if (take("VALIDATE", root)) { require("CONSTRAINT", root); add(root, identifier()); }
        else if (take("ADD", root)) {
            if (take("CONSTRAINT", root)) add(root, identifier());
            if (take("CHECK", root)) {
                add(root, expression_group());
                // ConstraintAttributeSpec allows these CHECK attributes in either order.
                unsigned seen = 0;
                while (!failed_ && (is("NOT") || is("NO"))) {
                    bool valid = take("NOT", root);
                    unsigned bit = valid ? 1 : 2;
                    if (seen & bit) { fail(); break; } seen |= bit;
                    if (valid) require("VALID", root);
                    else { require("NO", root); require("INHERIT", root); }
                }
            } else { require("NOT", root); require("NULL", root); }
        } else fail();
    }
    AstNode* type_attribute() {
        auto* attribute = clause(); add(attribute, identifier()); add(attribute, type());
        if (take("COLLATE", attribute)) add(attribute, name());
        return attribute;
    }
    static bool operator_symbol(const Token& t) {
        switch (t.type) {
            case TokenType::TK_PG_OPERATOR: case TokenType::TK_PLUS: case TokenType::TK_MINUS:
            case TokenType::TK_ASTERISK: case TokenType::TK_SLASH: case TokenType::TK_PERCENT:
            case TokenType::TK_CARET: case TokenType::TK_LESS: case TokenType::TK_GREATER:
            case TokenType::TK_EQUAL: case TokenType::TK_LESS_EQUAL: case TokenType::TK_GREATER_EQUAL:
            case TokenType::TK_NOT_EQUAL: return true;
            default: return false;
        }
    }
    void definition_value(AstNode* parent) {
        if (string_start()) { add(parent, string_literal()); return; }
        auto number = tok_;
        if (number.peek().type == TokenType::TK_PLUS || number.peek().type == TokenType::TK_MINUS) number.skip();
        if (number.peek().type == TokenType::TK_INTEGER || number.peek().type == TokenType::TK_FLOAT) { numeric(parent); return; }
        if (take("NONE", parent)) return;
        if (take("OPERATOR", parent)) {
            require(TokenType::TK_LPAREN);
            auto* op = node(NodeType::NODE_PG_DDL_LIST);
            add(op, operator_name());
            require(TokenType::TK_RPAREN); add(parent, op); return;
        }
        if (operator_symbol(tok_.peek())) { syntax(parent); return; }
        if (take("SETOF", parent)) { add(parent, definition_type()); return; }
        if (is("DOUBLE")) {
            auto look = tok_; look.skip();
            if (!word(look.peek(), "PRECISION")) { add(parent, identifier()); return; }
        }
        if (PgTypeParser::name_token(tok_.peek()) || pg_type_function_name(tok_.peek())) { add(parent, definition_type()); return; }
        // reserved_keyword is an explicit def_arg production.
        if (pg_column_label(tok_.peek()) && !nonreserved(tok_.peek())) syntax(parent);
        else fail();
    }
    static bool definition_modifier(Tokenizer<Dialect::PostgreSQL>& tok, void* context) {
        auto* owner = static_cast<PgDdlParser*>(context);
        ExpressionParser<Dialect::PostgreSQL> parser(tok, owner->arena_, true);
        parser.set_subquery_callback(owner->callback_);
        auto* value = parser.parse_complete();
        return value && value->type != NodeType::NODE_ASTERISK;
    }
    AstNode* definition_type() {
        Token first = tok_.peek();
        auto qualifier = tok_; qualifier.skip();
        if (qualifier.peek().type == TokenType::TK_DOT && !pg_type_function_name(first)) { fail(); return nullptr; }
        if (!PgTypeParser::name_token(first) && !pg_type_function_name(first)) { fail(); return nullptr; }
        // Type/function keyword categories do not depend on the token enum.
        // PostgreSQL accepts names such as DELETE as generic type names.
        if (!PgTypeParser::name_token(first)) first.type = TokenType::TK_IDENTIFIER;
        tok_.skip();
        StringRef text = PgTypeParser(tok_, definition_modifier, this).parse(true, false, &first);
        if (text.empty()) { fail(); return nullptr; }
        auto* result = make_node(arena_, NodeType::NODE_TYPE_NAME, text);
        if (!result) fail();
        return result;
    }
    AstNode* definition() {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        do {
            auto* item = clause(); add(item, identifier(true));
            if (take(TokenType::TK_EQUAL)) {
                add(item, node(NodeType::NODE_PG_DDL_SYNTAX, "=")); definition_value(item);
            }
            add(list, item);
        } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN); return list;
    }
    AstNode* routine_name(bool signature = false) {
        Token first = tok_.peek();
        auto look = tok_; look.skip();
        bool qualified = look.peek().type == TokenType::TK_DOT;
        if (!(qualified ? pg_column_name(first) : (signature && look.peek().type != TokenType::TK_LPAREN) ? nonreserved(first) : pg_type_function_name(first))) {
            fail(); return nullptr;
        }
        tok_.skip();
        AstNode* result = token_node(NodeType::NODE_IDENTIFIER, first);
        if (qualified) {
            auto* q = node(NodeType::NODE_QUALIFIED_NAME); add(q, result);
            while (!failed_ && take(TokenType::TK_DOT)) add(q, identifier(true));
            result = q;
        }
        return result;
    }
    AstNode* operator_name() {
        auto* q = node(NodeType::NODE_QUALIFIED_NAME);
        while (!failed_ && !operator_symbol(tok_.peek())) {
            add(q, identifier()); require(TokenType::TK_DOT);
        }
        if (operator_symbol(tok_.peek())) syntax(q); else fail();
        return q;
    }
    AstNode* operator_arguments() {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        bool left_none = take("NONE", list);
        if (!left_none) add(list, type());
        require(TokenType::TK_COMMA);
        bool right_none = take("NONE", list);
        if (!right_none) add(list, type());
        if (left_none && right_none) fail();
        require(TokenType::TK_RPAREN); return list;
    }
    AstNode* type_list() {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        do { add(list, type()); } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN); return list;
    }
    StringRef function_type_text(Tokenizer<Dialect::PostgreSQL>& tok) {
        Token begin = tok.peek();
        if (word(tok.peek(), "SETOF")) tok.skip();
        // func_type's column reference form is narrower than Typename.
        auto look = tok;
        if (pg_type_function_name(look.peek())) {
            look.skip(); bool qualified = false;
            while (look.peek().type == TokenType::TK_DOT) {
                qualified = true; look.skip();
                if (!pg_column_label(look.peek())) return {};
                look.skip();
            }
            if (qualified && look.peek().type == TokenType::TK_PERCENT) {
                look.skip();
                if (!word(look.peek(), "TYPE")) return {};
                Token end = look.next_token(); tok = look;
                return {begin.source.ptr, static_cast<uint32_t>(end.source.ptr + end.source.len - begin.source.ptr)};
            }
        }
        Token first = tok.peek();
        auto qualifier = tok; qualifier.skip();
        if (qualifier.peek().type == TokenType::TK_DOT && !pg_type_function_name(first)) return {};
        if (!PgTypeParser::name_token(first) && !pg_type_function_name(first)) return {};
        if (!PgTypeParser::name_token(first)) first.type = TokenType::TK_IDENTIFIER;
        tok.skip();
        StringRef parsed = PgTypeParser(tok, definition_modifier, this).parse(true, false, &first);
        if (parsed.empty()) return {};
        return {begin.source.ptr, static_cast<uint32_t>(parsed.ptr + parsed.len - begin.source.ptr)};
    }
    bool argument_mode(AstNode* arg, bool aggregate) {
        if (take("IN", arg)) {
            if (take("OUT", arg) && aggregate) fail();
            return true;
        }
        if (take("VARIADIC", arg)) return true;
        if (take("OUT", arg) || take("INOUT", arg)) {
            if (aggregate) fail();
            return true;
        }
        return false;
    }
    AstNode* routine_argument(bool definitions, bool aggregate) {
        auto* arg = clause();
        bool mode = argument_mode(arg, aggregate);
        auto look = tok_;
        StringRef possible = function_type_text(look);
        bool just_type = !possible.empty() && (look.peek().type == TokenType::TK_COMMA ||
            look.peek().type == TokenType::TK_RPAREN || word(look.peek(), "ORDER") ||
            word(look.peek(), "DEFAULT") || look.peek().type == TokenType::TK_EQUAL);
        if (!just_type) {
            if (!pg_type_function_name(tok_.peek())) { fail(); return arg; }
            add(arg, token_node(NodeType::NODE_IDENTIFIER, tok_.next_token()));
            if (!mode) argument_mode(arg, aggregate);
        }
        StringRef spelling = function_type_text(tok_);
        if (spelling.empty()) fail();
        else add(arg, make_node(arena_, NodeType::NODE_TYPE_NAME, spelling));
        if (definitions && (is("DEFAULT") || at(TokenType::TK_EQUAL))) {
            syntax(arg); add(arg, expr());
        }
        return arg;
    }
    AstNode* aggregate_argument_list() {
        auto* list = node(NodeType::NODE_PG_DDL_LIST); if (list) list->flags = 1;
        do { add(list, routine_argument(false, true)); } while (!failed_ && take(TokenType::TK_COMMA));
        return list;
    }
    static bool variadic_argument(const AstNode* arg) {
        for (auto* c = arg ? arg->first_child : nullptr; c; c = c->next_sibling)
            if (c->type == NodeType::NODE_PG_DDL_SYNTAX && c->value().equals_ci("VARIADIC", 8)) return true;
        return false;
    }
    static bool same_argument_type(StringRef left, StringRef right) {
        Tokenizer<Dialect::PostgreSQL> a, b;
        a.reset(left.ptr, left.len); b.reset(right.ptr, right.len);
        bool first = true;
        while (true) {
            Token x = a.next_token(), y = b.next_token();
            bool x_quoted = x.source.ptr != x.text.ptr && x.type == TokenType::TK_IDENTIFIER;
            bool y_quoted = y.source.ptr != y.text.ptr && y.type == TokenType::TK_IDENTIFIER;
            if (first && !x_quoted && !y_quoted) {
                // Canonical aliases emitted by PostgreSQL's builtin type grammar.
                auto canonical = [](Token& t, Tokenizer<Dialect::PostgreSQL>& tok) {
                    if (tok.peek().type == TokenType::TK_DOT) return;
                    if (word(t, "INT")) t.text = {"integer", 7};
                    else if (word(t, "DEC") || word(t, "DECIMAL")) t.text = {"numeric", 7};
                    else if (word(t, "CHAR") || word(t, "CHARACTER")) {
                        if (word(tok.peek(), "VARYING")) { tok.skip(); t.text = {"varchar", 7}; }
                        else t.text = {"character", 9};
                    }
                };
                canonical(x, a); canonical(y, b);
            }
            first = false;
            if (x_quoted || y_quoted) {
                if (x_quoted != y_quoted && !pg_type_function_name(x_quoted ? y : x)) return false;
                if (x.text.len != y.text.len) return false;
                for (uint32_t i = 0; i < x.text.len; ++i) {
                    unsigned char xc = x.text.ptr[i], yc = y.text.ptr[i];
                    if (!x_quoted && xc >= 'A' && xc <= 'Z') xc += 'a' - 'A';
                    if (!y_quoted && yc >= 'A' && yc <= 'Z') yc += 'a' - 'A';
                    if (xc != yc) return false;
                }
            } else if (x.type == TokenType::TK_STRING || y.type == TokenType::TK_STRING) {
                if (x.text != y.text) return false;
            } else if (!x.text.equals_ci(y.text.ptr, y.text.len)) return false;
            if (x.type == TokenType::TK_EOF || y.type == TokenType::TK_EOF)
                return x.type == y.type;
        }
    }
    AstNode* aggregate_arguments() {
        require(TokenType::TK_LPAREN);
        auto* group = node(NodeType::NODE_PG_DDL_LIST);
        if (at(TokenType::TK_ASTERISK)) syntax(group);
        else {
            auto* contents = clause();
            AstNode* direct = nullptr;
            if (!is("ORDER")) { direct = aggregate_argument_list(); add(contents, direct); }
            if (take("ORDER", contents)) {
                require("BY", contents);
                auto* ordered = aggregate_argument_list(); add(contents, ordered);
                // PostgreSQL validates duplicate VARIADIC ordered-set arguments
                // during raw parsing, before catalog/type resolution.
                auto* d = last(direct);
                auto* o = ordered ? ordered->first_child : nullptr;
                if (variadic_argument(d) && (!o || o->next_sibling || !variadic_argument(o) ||
                    !last(d) || !last(o) || !same_argument_type(last(d)->value(), last(o)->value()))) fail();
            }
            add(group, contents);
        }
        require(TokenType::TK_RPAREN); return group;
    }
    void create_aggregate(AstNode* root) {
        add(root, routine_name());
        auto look = tok_; look.skip(); look.skip();
        bool old_style = look.peek().type == TokenType::TK_EQUAL;
        if (!old_style) { add(root, aggregate_arguments()); add(root, definition()); return; }
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        do {
            auto* item = clause();
            if (!at(TokenType::TK_IDENTIFIER) || pg_keyword(tok_.peek())) fail();
            add(item, identifier()); require("=", item); definition_value(item); add(list, item);
        } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN); add(root, list);
    }
    AstNode* opclass_items(bool dropping = false) {
        auto* list = node(NodeType::NODE_PG_DDL_LIST); if (list) list->flags = 1;
        do {
            auto* item = clause();
            if (!dropping && take("STORAGE", item)) add(item, type());
            else {
                bool op = take("OPERATOR", item);
                if (!op) require("FUNCTION", item);
                Token number;
                if (pg_integer_literal(tok_, number)) add(item, token_node(NodeType::NODE_LITERAL_INT, number));
                else fail();
                if (dropping) add(item, type_list());
                else if (op) {
                    add(item, operator_name());
                    if (at(TokenType::TK_LPAREN)) add(item, operator_arguments());
                    if (take("FOR", item) && !take("SEARCH", item)) {
                        require("ORDER", item); require("BY", item); add(item, name());
                    }
                } else {
                    if (at(TokenType::TK_LPAREN)) add(item, type_list());
                    add(item, parse_function_signature());
                }
            }
            add(list, item);
        } while (!failed_ && take(TokenType::TK_COMMA));
        return list;
    }
    void create_collation(AstNode* root) {
        if_exists(root, true); add(root, name());
        if (take("FROM", root)) add(root, name());
        else add(root, definition());
    }
    void create_text_search(AstNode* root) {
        require("SEARCH", root);
        if (!take("PARSER", root) && !take("DICTIONARY", root) && !take("TEMPLATE", root)) require("CONFIGURATION", root);
        add(root, name()); add(root, definition());
    }
    void create_operator(AstNode* root) {
        bool family = take("FAMILY", root);
        if (family || take("CLASS", root)) {
            add(root, name());
            if (!family) { take("DEFAULT", root); require("FOR", root); require("TYPE", root); add(root, type()); }
            require("USING", root); add(root, identifier());
            if (!family) {
                if (take("FAMILY", root)) add(root, name());
                require("AS", root); add(root, opclass_items());
            }
        } else { add(root, operator_name()); add(root, definition()); }
    }
    void alter_operator(AstNode* root) {
        bool family = take("FAMILY", root);
        if (family || take("CLASS", root)) {
            add(root, name()); require("USING", root); add(root, identifier());
            if (object_identity(root)) return;
            if (family && take("ADD", root)) add(root, opclass_items());
            else if (family && take("DROP", root)) add(root, opclass_items(true));
            else fail();
        } else {
            add(root, parse_operator_signature());
            if (is("RENAME")) { fail(); return; }
            if (object_identity(root)) return;
            require("SET", root); add(root, definition());
        }
    }
    void drop_definition(AstNode* root, bool aggregate) {
        if (!aggregate && (take("CLASS", root) || take("FAMILY", root))) {
            if_exists(root); add(root, name()); require("USING", root); add(root, identifier()); behavior(root); return;
        }
        if_exists(root);
        auto* list = node(NodeType::NODE_PG_DDL_LIST); if (list) list->flags = 1;
        do { add(list, aggregate ? parse_aggregate_signature() : parse_operator_signature()); }
        while (!failed_ && take(TokenType::TK_COMMA));
        add(root, list); behavior(root);
    }
    void create_type(AstNode* root) {
        add(root, name());
        if (take("AS", root)) {
            if (take("RANGE", root)) { add(root, definition()); return; }
            bool enumeration = take("ENUM", root);
            require(TokenType::TK_LPAREN);
            auto* list = node(NodeType::NODE_PG_DDL_LIST);
            if (!at(TokenType::TK_RPAREN)) {
                do { add(list, enumeration ? string_literal() : type_attribute()); }
                while (!failed_ && take(TokenType::TK_COMMA));
            }
            require(TokenType::TK_RPAREN); add(root, list);
        } else if (at(TokenType::TK_LPAREN)) add(root, definition());
    }
    void alter_type(AstNode* root) {
        add(root, name());
        if (take("RENAME", root)) {
            if (take("VALUE", root)) { add(root, string_literal()); require("TO", root); add(root, string_literal()); }
            else if (take("ATTRIBUTE", root)) { add(root, identifier()); require("TO", root); add(root, identifier()); behavior(root); }
            else { require("TO", root); add(root, identifier()); }
            return;
        }
        if (object_identity(root)) return;
        if (take("SET", root)) { add(root, definition()); return; }
        auto look = tok_; look.skip();
        if (is("ADD") && word(look.peek(), "VALUE")) {
            syntax(root); require("VALUE", root); if_exists(root, true); add(root, string_literal());
            if (take("BEFORE", root) || take("AFTER", root)) add(root, string_literal());
            return;
        }
        auto* commands = node(NodeType::NODE_PG_DDL_LIST); if (commands) commands->flags = 1;
        do {
            auto* c = clause();
            if (take("ADD", c)) { require("ATTRIBUTE", c); add(c, type_attribute()); }
            else if (take("DROP", c)) { require("ATTRIBUTE", c); if_exists(c); add(c, identifier()); }
            else if (take("ALTER", c)) {
                require("ATTRIBUTE", c); add(c, identifier());
                if (take("SET", c)) require("DATA", c);
                require("TYPE", c); add(c, type());
                if (take("COLLATE", c)) add(c, name());
            } else fail();
            behavior(c); add(commands, c);
        } while (!failed_ && take(TokenType::TK_COMMA));
        add(root, commands);
    }
    bool sequence_option(AstNode* root) {
        auto* c = clause();
        if (take("AS", c)) add(c, simple_type());
        else if (take("CACHE", c) || take("MINVALUE", c) || take("MAXVALUE", c)) numeric(c);
        else if (take("INCREMENT", c)) { take("BY", c); numeric(c); }
        else if (take("START", c)) { take("WITH", c); numeric(c); }
        else if (take("RESTART", c)) { if (take("WITH", c) || numeric_start()) numeric(c); }
        else if (take("NO", c)) { if (!take("MINVALUE", c) && !take("MAXVALUE", c)) require("CYCLE", c); }
        else if (take("CYCLE", c) || take("LOGGED", c) || take("UNLOGGED", c)) {}
        else if (take("OWNED", c)) { require("BY", c); add(c, name()); }
        else if (take("SEQUENCE", c)) { require("NAME", c); add(c, name()); }
        else return false;
        add(root, c); return true;
    }
    void create_sequence(AstNode* root) {
        if_exists(root, true); add(root, name(true));
        while (!failed_ && sequence_option(root)) {}
    }
    void alter_sequence(AstNode* root) {
        if_exists(root); add(root, name(true));
        if (object_identity(root)) return;
        if (take("SET", root)) {
            if (!take("LOGGED", root)) require("UNLOGGED", root);
            return;
        }
        bool had = false;
        while (!failed_ && sequence_option(root)) had = true;
        if (!had) fail();
    }
    void alter_identity_object(AstNode* root, bool qualified, bool schema, bool owner, bool missing, bool columns = false) {
        if (missing) if_exists(root);
        add(root, qualified ? name(true) : identifier());
        if (columns && take("RENAME", root)) {
            if (!take("TO", root)) { take("COLUMN", root); add(root, identifier()); require("TO", root); }
            add(root, identifier()); return;
        }
        if (!object_identity(root, schema, owner)) fail();
    }
    void create(AstNode* root) {
        bool replace = take("OR", root);
        if (replace) require("REPLACE", root);
        bool temporary = take("TEMP", root) || take("TEMPORARY", root);
        bool unlogged = !temporary && take("UNLOGGED", root);
        bool unique = take("UNIQUE", root);
        bool materialized = take("MATERIALIZED", root);
        bool constraint_trigger = take("CONSTRAINT", root);
        if (take("TABLE", root) && !replace && !unique && !materialized && !constraint_trigger) create_table(root);
        else if (take("INDEX", root) && !replace && !temporary && !unlogged && !materialized && !constraint_trigger) create_index(root);
        else if (take("VIEW", root) && !unlogged && !unique && !constraint_trigger && !(materialized && (replace || temporary))) create_view(root, materialized);
        else if ((take("FUNCTION", root) || take("PROCEDURE", root)) && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_function(root);
        else if (take("TRIGGER", root) && !temporary && !unlogged && !unique && !materialized && !(replace && constraint_trigger)) create_trigger(root, constraint_trigger);
        else if (take("SCHEMA", root) && !replace && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_schema(root);
        else if (take("DATABASE", root) && !replace && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) add(root, identifier());
        else if (take("DOMAIN", root) && !replace && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_domain(root);
        else if (take("TYPE", root) && !replace && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_type(root);
        else if (take("AGGREGATE", root) && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_aggregate(root);
        else if (take("COLLATION", root) && !replace && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_collation(root);
        else if (take("TEXT", root) && !replace && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_text_search(root);
        else if (take("OPERATOR", root) && !replace && !temporary && !unlogged && !unique && !materialized && !constraint_trigger) create_operator(root);
        else if (take("SEQUENCE", root) && !replace && !unique && !materialized && !constraint_trigger) create_sequence(root);
        else fail();
    }
    void create_schema(AstNode* root) {
        if_exists(root, true);
        if (!is("AUTHORIZATION")) add(root, identifier());
        if (take("AUTHORIZATION", root)) {
            if (!take("CURRENT_ROLE", root) && !take("CURRENT_USER", root) && !take("SESSION_USER", root)) {
                // RoleSpec's NonReservedWord accepts both column and
                // type/function keyword categories, preserving quoted names.
                Token name = tok_.peek();
                if (!pg_column_name(name) && !pg_type_function_name(name)) fail();
                else add(root, token_node(NodeType::NODE_IDENTIFIER, tok_.next_token()));
            }
        }
    }
    void create_table(AstNode* root) {
        if_exists(root, true); add(root, name(true));
        bool partition = take("PARTITION", root);
        if (partition) { require("OF", root); add(root, name()); partition_bound(root); }
        bool as_query = !at(TokenType::TK_LPAREN);
        if (partition) as_query = false;
        else if (!as_query) {
            auto look = tok_; look.skip();
            if (pg_column_name(look.peek())) {
                look.skip();
                as_query = look.peek().type == TokenType::TK_COMMA || look.peek().type == TokenType::TK_RPAREN;
            }
            add(root, as_query ? names() : table_elements());
        }
        if (take("INHERITS", root)) add(root, names(true, true));
        if (take("PARTITION", root)) {
            require("BY", root);
            if (!take("RANGE", root) && !take("LIST", root)) require("HASH", root);
            add(root, index_elements(true));
        }
        if (take("USING", root)) add(root, identifier());
        if (take("WITH", root)) add(root, options());
        if (take("ON", root)) {
            require("COMMIT", root);
            if (!take("DROP", root)) {
                if (!take("DELETE", root)) require("PRESERVE", root);
                require("ROWS", root);
            }
        }
        if (take("TABLESPACE", root)) add(root, identifier());
        if (as_query) { require("AS", root); add(root, callback_(tok_, arena_)); with_data(root); }
    }
    AstNode* expressions() {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        do { add(list, expr()); } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN); return list;
    }
    void partition_bound(AstNode* parent) {
        if (take("DEFAULT", parent)) return;
        require("FOR", parent); require("VALUES", parent);
        if (take("IN", parent)) add(parent, expressions());
        else if (take("FROM", parent)) {
            add(parent, expressions()); require("TO", parent); add(parent, expressions());
        } else if (take("WITH", parent)) {
            require(TokenType::TK_LPAREN);
            auto* list = node(NodeType::NODE_PG_DDL_LIST);
            unsigned seen = 0;
            do {
                auto* option = clause();
                unsigned bit = 0;
                if (take("MODULUS", option)) bit = 1;
                else if (take("REMAINDER", option)) bit = 2;
                else fail();
                if (seen & bit) fail();
                seen |= bit;
                integer(option); add(list, option);
            } while (!failed_ && take(TokenType::TK_COMMA));
            if (seen != 3) fail();
            require(TokenType::TK_RPAREN); add(parent, list);
        } else fail();
    }
    void with_data(AstNode* root) {
        if (take("WITH", root)) { take("NO", root); require("DATA", root); }
    }
    AstNode* index_elements(bool partition_key = false) {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        do {
            auto* e = clause();
            if (at(TokenType::TK_LPAREN)) add(e, expression_group());
            else {
                auto look = tok_; look.skip();
                while (look.peek().type == TokenType::TK_DOT) {
                    look.skip();
                    if (!pg_column_label(look.peek())) break;
                    look.skip();
                }
                if (look.peek().type == TokenType::TK_LPAREN)
                    add(e, expr(Precedence::COLLATION));
                else add(e, identifier());
            }
            if (take("COLLATE", e)) add(e, name());
            if (pg_column_name(tok_.peek()) && !is("ASC") && !is("DESC") && !is("NULLS")) {
                add(e, name());
                if (!partition_key && at(TokenType::TK_LPAREN)) add(e, options());
            }
            if (!partition_key) {
                if (!take("ASC", e)) take("DESC", e);
                if (take("NULLS", e)) { if (!take("FIRST", e)) require("LAST", e); }
            }
            add(list, e);
        } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN); return list;
    }
    void create_index(AstNode* root) {
        take("CONCURRENTLY", root);
        bool named = is("IF"); if_exists(root, true);
        if (named || !is("ON")) add(root, identifier());
        require("ON", root); take("ONLY", root); add(root, name(true));
        if (take("USING", root)) add(root, identifier());
        add(root, index_elements());
        if (take("INCLUDE", root)) add(root, names());
        if (take("NULLS", root)) { take("NOT", root); require("DISTINCT", root); }
        if (take("WITH", root)) add(root, options());
        if (take("TABLESPACE", root)) add(root, identifier());
        if (take("WHERE", root)) add(root, expr());
    }
    void create_view(AstNode* root, bool materialized) {
        if (materialized) if_exists(root, true);
        add(root, name(true));
        if (at(TokenType::TK_LPAREN)) add(root, names());
        if (materialized && take("USING", root)) add(root, identifier());
        if (take("WITH", root)) add(root, options());
        if (materialized && take("TABLESPACE", root)) add(root, identifier());
        require("AS", root); add(root, callback_(tok_, arena_));
        if (materialized) with_data(root);
        else if (take("WITH", root)) {
            if (!take("LOCAL", root)) take("CASCADED", root);
            require("CHECK", root); require("OPTION", root);
        }
    }
    AstNode* arguments(bool definitions) {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_PG_DDL_LIST);
        if (!at(TokenType::TK_RPAREN)) {
            do { add(list, routine_argument(definitions, false)); }
            while (!failed_ && take(TokenType::TK_COMMA));
        }
        require(TokenType::TK_RPAREN); return list;
    }
    void create_function(AstNode* root) {
        bool procedure = root && last(root) && last(root)->value().equals_ci("PROCEDURE", 9);
        add(root, name()); add(root, arguments(true));
        if (take("RETURNS", root)) {
            if (procedure) fail();
            if (take("TABLE", root)) {
                require(TokenType::TK_LPAREN);
                auto* list = node(NodeType::NODE_PG_DDL_LIST);
                do { auto* c = clause(); add(c, identifier()); add(c, type()); add(list, c); }
                while (!failed_ && take(TokenType::TK_COMMA));
                require(TokenType::TK_RPAREN); add(root, list);
            } else { take("SETOF", root); add(root, type()); }
        }
        bool had_option = false;
        while (!failed_) {
            auto* c = clause();
            if (take("AS", c)) {
                add(c, string_literal());
                if (take(TokenType::TK_COMMA)) {
                    add(c, node(NodeType::NODE_PG_DDL_SYNTAX, ",")); add(c, string_literal());
                }
            } else if (take("LANGUAGE", c)) {
                if (at(TokenType::TK_STRING)) add(c, string_literal()); else add(c, identifier());
            } else if (take("TRANSFORM", c)) {
                auto* transforms = node(NodeType::NODE_PG_DDL_LIST);
                if (transforms) transforms->flags = 1;
                do {
                    auto* transform = clause(); require("FOR", transform); require("TYPE", transform);
                    add(transform, type()); add(transforms, transform);
                } while (!failed_ && take(TokenType::TK_COMMA));
                add(c, transforms);
            } else if (take("IMMUTABLE", c) || take("STABLE", c) || take("VOLATILE", c) ||
                       take("STRICT", c) || take("LEAKPROOF", c) || take("WINDOW", c)) {}
            else if (take("NOT", c)) require("LEAKPROOF", c);
            else if (take("CALLED", c)) { require("ON", c); require("NULL", c); require("INPUT", c); }
            else if (take("RETURNS", c)) { require("NULL", c); require("ON", c); require("NULL", c); require("INPUT", c); }
            else if (take("SECURITY", c)) { if (!take("DEFINER", c)) require("INVOKER", c); }
            else if (take("PARALLEL", c)) {
                if (!take("SAFE", c) && !take("RESTRICTED", c)) require("UNSAFE", c);
            } else if (take("COST", c) || take("ROWS", c)) {
                if (at(TokenType::TK_INTEGER) || at(TokenType::TK_FLOAT)) syntax(c); else fail();
            } else if (take("SUPPORT", c)) add(c, name());
            else if (take("SET", c)) {
                add(c, name());
                if (take("FROM", c)) require("CURRENT", c);
                else {
                    if (!take("TO", c)) { require(TokenType::TK_EQUAL); add(c, node(NodeType::NODE_PG_DDL_SYNTAX, "=")); }
                    auto* values = node(NodeType::NODE_PG_DDL_LIST); if (values) values->flags = 1;
                    do { auto* value = clause(); option_value(value); add(values, value); }
                    while (!failed_ && take(TokenType::TK_COMMA));
                    add(c, values);
                }
            } else break;
            add(root, c); had_option = true;
        }
        if (!had_option) fail();
    }
    void create_trigger(AstNode* root, bool constraint_trigger) {
        add(root, identifier());
        if (constraint_trigger) require("AFTER", root);
        else if (!take("BEFORE", root) && !take("AFTER", root)) { require("INSTEAD", root); require("OF", root); }
        do {
            auto* event = clause();
            if (take("UPDATE", event)) { if (take("OF", event)) add(event, names(false)); }
            else if (!take("INSERT", event) && !take("DELETE", event)) require("TRUNCATE", event);
            add(root, event);
            if (!take("OR", root)) break;
        } while (!failed_);
        require("ON", root); add(root, name(true));
        if (constraint_trigger) {
            if (take("FROM", root)) add(root, name());
            deferrability(root);
        }
        if (take("REFERENCING", root)) {
            if (constraint_trigger) fail();
            bool had = false;
            while (!failed_ && (is("OLD") || is("NEW"))) {
                syntax(root); require("TABLE", root); take("AS", root); add(root, identifier()); had = true;
            }
            if (!had) fail();
        }
        if (take("FOR", root)) {
            if (constraint_trigger) { require("EACH", root); require("ROW", root); }
            else { take("EACH", root); if (!take("ROW", root)) require("STATEMENT", root); }
        } else if (constraint_trigger) fail();
        if (take("WHEN", root)) add(root, expression_group());
        require("EXECUTE", root);
        if (!take("FUNCTION", root)) require("PROCEDURE", root);
        add(root, name()); require(TokenType::TK_LPAREN);
        auto* args = node(NodeType::NODE_PG_DDL_LIST);
        if (!at(TokenType::TK_RPAREN)) {
            do {
                if (at(TokenType::TK_STRING)) add(args, string_literal());
                else if (at(TokenType::TK_INTEGER) || at(TokenType::TK_FLOAT))
                    add(args, token_node(NodeType::NODE_PG_DDL_SYNTAX, tok_.next_token()));
                else add(args, identifier());
            } while (!failed_ && take(TokenType::TK_COMMA));
        }
        require(TokenType::TK_RPAREN); add(root, args);
    }
    void alter(AstNode* root) {
        if (take("TABLE", root)) {
            if_exists(root); take("ONLY", root); add(root, name(true));
            if (at(TokenType::TK_ASTERISK)) syntax(root);
            auto* commands = node(NodeType::NODE_PG_DDL_LIST); if (commands) commands->flags = 1;
            auto look = tok_; look.skip();
            bool standalone = is("RENAME") || (is("SET") && word(look.peek(), "SCHEMA"));
            do { add(commands, alter_table_command()); }
            while (!failed_ && !standalone && take(TokenType::TK_COMMA));
            add(root, commands);
        } else if (take("FUNCTION", root) || take("PROCEDURE", root) || take("ROUTINE", root)) alter_routine(root);
        else if (take("AGGREGATE", root)) { add(root, parse_aggregate_signature()); if (!object_identity(root)) fail(); }
        else if (take("OPERATOR", root)) alter_operator(root);
        else if (take("DOMAIN", root)) alter_domain(root);
        else if (take("TYPE", root)) alter_type(root);
        else if (take("SEQUENCE", root)) alter_sequence(root);
        else if (take("SCHEMA", root) || take("DATABASE", root)) alter_identity_object(root, false, false, true, false);
        else if (take("VIEW", root)) alter_identity_object(root, true, true, true, true, true);
        else if (take("MATERIALIZED", root)) { require("VIEW", root); alter_identity_object(root, true, true, true, true, true); }
        else if (take("INDEX", root)) alter_identity_object(root, true, false, true, true);
        else if (take("COLLATION", root) || take("CONVERSION", root) || take("STATISTICS", root))
            alter_identity_object(root, true, true, true, false);
        else fail();
    }
    AstNode* alter_table_command() {
        auto* c = clause();
        if (take("ADD", c)) {
            if (constraint_start()) {
                add(c, constraint(true));
                if (take("NOT", c)) require("VALID", c);
            } else { take("COLUMN", c); if_exists(c, true); add(c, column()); }
        } else if (take("DROP", c)) {
            if (!take("CONSTRAINT", c)) take("COLUMN", c);
            if_exists(c); add(c, identifier()); behavior(c);
        } else if (take("ALTER", c)) {
            take("COLUMN", c); add(c, identifier());
            if (take("TYPE", c)) {
                add(c, type()); if (take("COLLATE", c)) add(c, name());
                if (take("USING", c)) add(c, expr());
            } else if (take("SET", c)) {
                if (take("DATA", c)) {
                    require("TYPE", c); add(c, type()); if (take("COLLATE", c)) add(c, name());
                    if (take("USING", c)) add(c, expr());
                } else if (take("DEFAULT", c)) add(c, expr());
                else if (take("NOT", c)) require("NULL", c);
                else if (take("STATISTICS", c)) integer(c);
                else if (take("STORAGE", c)) {
                    if (!take("PLAIN", c) && !take("EXTERNAL", c) && !take("EXTENDED", c)) require("MAIN", c);
                } else if (take("COMPRESSION", c)) { if (!take("DEFAULT", c)) add(c, identifier()); }
                else if (at(TokenType::TK_LPAREN)) add(c, options());
                else fail();
            } else if (take("DROP", c)) {
                if (take("NOT", c)) require("NULL", c);
                else if (take("IDENTITY", c) || take("EXPRESSION", c)) if_exists(c);
                else require("DEFAULT", c);
            } else if (take("ADD", c)) {
                require("GENERATED", c);
                if (!take("ALWAYS", c)) { require("BY", c); require("DEFAULT", c); }
                require("AS", c); require("IDENTITY", c);
                if (at(TokenType::TK_LPAREN)) identity_options(c);
            } else fail();
        } else if (take("RENAME", c)) {
            if (take("TO", c)) add(c, identifier());
            else {
                if (!take("CONSTRAINT", c)) take("COLUMN", c);
                add(c, identifier()); require("TO", c); add(c, identifier());
            }
        } else if (take("OWNER", c)) { require("TO", c); role(c); }
        else if (take("VALIDATE", c)) { require("CONSTRAINT", c); add(c, identifier()); }
        else if (take("SET", c)) {
            if (take("SCHEMA", c) || take("TABLESPACE", c)) add(c, identifier());
            else if (take("LOGGED", c) || take("UNLOGGED", c)) {}
            else if (take("WITHOUT", c)) require("CLUSTER", c);
            else if (at(TokenType::TK_LPAREN)) add(c, options());
            else fail();
        } else if (take("RESET", c)) add(c, names());
        else if (take("ENABLE", c) || take("DISABLE", c)) {
            bool enable = c && last(c) && last(c)->value().equals_ci("ENABLE", 6);
            if (take("ROW", c)) { require("LEVEL", c); require("SECURITY", c); }
            else {
                if (enable && !take("ALWAYS", c)) take("REPLICA", c);
                if (!take("TRIGGER", c)) require("RULE", c);
                if (!take("ALL", c) && !take("USER", c)) add(c, identifier());
            }
        } else if (take("FORCE", c)) { require("ROW", c); require("LEVEL", c); require("SECURITY", c); }
        else if (take("NO", c)) {
            if (take("FORCE", c)) { require("ROW", c); require("LEVEL", c); require("SECURITY", c); }
            else { require("INHERIT", c); add(c, name()); }
        } else if (take("INHERIT", c)) add(c, name());
        else if (take("CLUSTER", c)) { require("ON", c); add(c, identifier()); }
        else if (take("ATTACH", c)) {
            require("PARTITION", c); add(c, name()); partition_bound(c);
        } else if (take("DETACH", c)) {
            require("PARTITION", c); add(c, name());
            if (!take("CONCURRENTLY", c)) take("FINALIZE", c);
        }
        else fail();
        return c;
    }
    void drop(AstNode* root) {
        if (take("AGGREGATE", root)) { drop_definition(root, true); return; }
        if (take("OPERATOR", root)) { drop_definition(root, false); return; }
        bool routine = false, trigger = false, index = false;
        bool database = false, roles = false, schema = false, type_name = false;
        if (take("MATERIALIZED", root)) require("VIEW", root);
        else if (take("FUNCTION", root) || take("PROCEDURE", root) || take("ROUTINE", root)) routine = true;
        else if (take("TRIGGER", root) || take("RULE", root) || take("POLICY", root)) trigger = true;
        else if (take("INDEX", root)) index = true;
        else if (take("DATABASE", root)) database = true;
        else if (take("SCHEMA", root)) schema = true;
        else if (take("ROLE", root) || take("USER", root)) roles = true;
        else if (take("TYPE", root) || take("DOMAIN", root)) type_name = true;
        else if (take("COLLATION", root) || take("CONVERSION", root) || take("STATISTICS", root)) {}
        else if (take("TEXT", root)) {
            require("SEARCH", root);
            if (!take("PARSER", root) && !take("DICTIONARY", root) && !take("TEMPLATE", root)) require("CONFIGURATION", root);
        } else if (take("FOREIGN", root)) {
            if (!take("TABLE", root)) { require("DATA", root); require("WRAPPER", root); schema = true; }
        } else if (take("ACCESS", root)) { require("METHOD", root); schema = true; }
        else if (take("EVENT", root)) { require("TRIGGER", root); schema = true; }
        else if (take("PROCEDURAL", root)) { require("LANGUAGE", root); schema = true; }
        else if (take("EXTENSION", root) || take("LANGUAGE", root) || take("PUBLICATION", root) || take("SERVER", root)) schema = true;
        else if (!take("TABLE", root) && !take("VIEW", root) && !take("SEQUENCE", root)) { fail(); return; }
        if (index) take("CONCURRENTLY", root);
        if_exists(root);
        auto* list = node(NodeType::NODE_PG_DDL_LIST); if (list) list->flags = 1;
        do {
            auto* item = clause(); add(item, type_name ? type() : database || roles || schema || trigger ? identifier() : name(true));
            if (routine && at(TokenType::TK_LPAREN)) add(item, arguments(false));
            add(list, item);
        } while (!failed_ && !trigger && !database && take(TokenType::TK_COMMA));
        add(root, list);
        if (trigger) { require("ON", root); add(root, name(true)); }
        if (!database && !roles) behavior(root);
    }
    void role(AstNode* parent) {
        if (!take("PUBLIC", parent) && !take("CURRENT_USER", parent) &&
            !take("CURRENT_ROLE", parent) && !take("SESSION_USER", parent)) add(parent, identifier());
    }
    void grant(AstNode* root, bool revoke) {
        // Membership grants use role names where object grants use privilege
        // keywords. Recognize the complete role-name list and its TO/FROM
        // delimiter before choosing this production.
        auto look = tok_;
        if (revoke && (word(look.peek(), "ADMIN") || word(look.peek(), "INHERIT") || word(look.peek(), "SET"))) {
            look.skip();
            if (word(look.peek(), "OPTION")) {
                look.skip();
                if (word(look.peek(), "FOR")) {
                    syntax(root); require("OPTION", root); require("FOR", root);
                    membership(root, revoke); return;
                }
            }
        }
        look = tok_;
        while (pg_column_name(look.peek())) {
            look.skip();
            if (word(look.peek(), revoke ? "FROM" : "TO")) { membership(root, revoke); return; }
            if (look.peek().type != TokenType::TK_COMMA) break;
            look.skip();
        }
        if (revoke && take("GRANT", root)) { require("OPTION", root); require("FOR", root); }
        auto* privileges = node(NodeType::NODE_PG_DDL_LIST); if (privileges) privileges->flags = 1;
        do {
            auto* privilege = clause();
            bool all = take("ALL", privilege);
            if (all) take("PRIVILEGES", privilege);
            else if (!take("SELECT", privilege) && !take("INSERT", privilege) && !take("UPDATE", privilege) &&
                !take("DELETE", privilege) && !take("TRUNCATE", privilege) && !take("REFERENCES", privilege) &&
                !take("TRIGGER", privilege) && !take("CREATE", privilege) && !take("CONNECT", privilege) &&
                !take("TEMPORARY", privilege) && !take("TEMP", privilege) && !take("EXECUTE", privilege) &&
                !take("USAGE", privilege) && !take("SET", privilege) && !take("ALTER", privilege) &&
                !take("MAINTAIN", privilege)) fail();
            if (privilege && last(privilege) && last(privilege)->value().equals_ci("ALTER", 5)) require("SYSTEM", privilege);
            if (at(TokenType::TK_LPAREN)) add(privilege, names());
            add(privileges, privilege);
            if (all && privileges && (privileges->first_child != privilege || at(TokenType::TK_COMMA))) fail();
        } while (!failed_ && take(TokenType::TK_COMMA));
        add(root, privileges); require("ON", root);
        bool routine = false;
        if (take("ALL", root)) {
            if (!take("TABLES", root) && !take("SEQUENCES", root) && !take("FUNCTIONS", root) && !take("PROCEDURES", root)) require("ROUTINES", root);
            require("IN", root); require("SCHEMA", root); add(root, names(false));
        } else {
            if (take("FUNCTION", root) || take("PROCEDURE", root) || take("ROUTINE", root)) routine = true;
            else if (take("FOREIGN", root)) { if (!take("SERVER", root)) { require("DATA", root); require("WRAPPER", root); } }
            else if (take("LARGE", root)) require("OBJECT", root);
            else if (!take("TABLE", root) && !take("SEQUENCE", root) && !take("DATABASE", root) &&
                !take("SCHEMA", root) && !take("LANGUAGE", root) && !take("TABLESPACE", root) &&
                !take("TYPE", root) && !take("DOMAIN", root)) take("PARAMETER", root);
            auto* objects = node(NodeType::NODE_PG_DDL_LIST); if (objects) objects->flags = 1;
            do {
                auto* object = clause();
                if (at(TokenType::TK_INTEGER)) syntax(object); else add(object, name());
                if (routine && at(TokenType::TK_LPAREN)) add(object, arguments(false));
                add(objects, object);
            } while (!failed_ && take(TokenType::TK_COMMA));
            add(root, objects);
        }
        require(revoke ? "FROM" : "TO", root);
        auto* roles = node(NodeType::NODE_PG_DDL_LIST); if (roles) roles->flags = 1;
        do { auto* r = clause(); role(r); add(roles, r); }
        while (!failed_ && take(TokenType::TK_COMMA));
        add(root, roles);
        if (!revoke && take("WITH", root)) { require("GRANT", root); require("OPTION", root); }
        if (take("GRANTED", root)) { require("BY", root); role(root); }
        if (revoke) behavior(root);
    }
    void membership(AstNode* root, bool revoke) {
        add(root, names(false)); require(revoke ? "FROM" : "TO", root);
        auto* roles = node(NodeType::NODE_PG_DDL_LIST); if (roles) roles->flags = 1;
        do { auto* r = clause(); role(r); add(roles, r); } while (!failed_ && take(TokenType::TK_COMMA));
        add(root, roles);
        if (!revoke && take("WITH", root)) {
            auto* list = node(NodeType::NODE_PG_DDL_LIST); if (list) list->flags = 1;
            do {
                auto* option = clause();
                if (!take("ADMIN", option) && !take("INHERIT", option)) require("SET", option);
                if (!take("OPTION", option) && !take("TRUE", option)) require("FALSE", option);
                add(list, option);
            } while (!failed_ && take(TokenType::TK_COMMA));
            add(root, list);
        }
        if (take("GRANTED", root)) { require("BY", root); role(root); }
        if (revoke) behavior(root);
    }
    void truncate(AstNode* root) {
        take("TABLE", root);
        auto* tables = node(NodeType::NODE_PG_DDL_LIST); if (tables) tables->flags = 1;
        do {
            auto* table = clause(); take("ONLY", table); add(table, name(true));
            if (at(TokenType::TK_ASTERISK)) syntax(table);
            add(tables, table);
        } while (!failed_ && take(TokenType::TK_COMMA));
        add(root, tables);
        if (take("RESTART", root) || take("CONTINUE", root)) require("IDENTITY", root);
        behavior(root);
    }
    void maintenance(AstNode* root, bool vacuum) {
        if (take(TokenType::TK_LPAREN)) {
            auto* list = node(NodeType::NODE_PG_DDL_LIST);
            do {
                auto* option = clause();
                bool numeric = false, index_cleanup = false;
                if (vacuum && (is("PARALLEL") || is("BUFFER_USAGE_LIMIT"))) { syntax(option); numeric = true; }
                else if (!vacuum && is("BUFFER_USAGE_LIMIT")) { syntax(option); numeric = true; }
                else if (vacuum && take("INDEX_CLEANUP", option)) index_cleanup = true;
                else if (!take("VERBOSE", option) && !take("SKIP_LOCKED", option) &&
                    !(vacuum && (take("FULL", option) || take("FREEZE", option) || take("ANALYZE", option) ||
                     take("ANALYSE", option) || take("DISABLE_PAGE_SKIPPING", option) || take("TRUNCATE", option) ||
                     take("PROCESS_MAIN", option) || take("PROCESS_TOAST", option) || take("SKIP_DATABASE_STATS", option) ||
                     take("ONLY_DATABASE_STATS", option)))) fail();
                if (numeric) {
                    if (at(TokenType::TK_STRING)) add(option, string_literal()); else integer(option);
                } else if (!at(TokenType::TK_COMMA) && !at(TokenType::TK_RPAREN)) {
                    if (at(TokenType::TK_INTEGER)) syntax(option);
                    else if (!take("TRUE", option) && !take("FALSE", option) && !take("ON", option) && !take("OFF", option) &&
                        !(index_cleanup && take("AUTO", option))) fail();
                }
                add(list, option);
            } while (!failed_ && take(TokenType::TK_COMMA));
            require(TokenType::TK_RPAREN); add(root, list);
        } else {
            if (vacuum) {
                take("FULL", root); take("FREEZE", root); take("VERBOSE", root);
                if (!take("ANALYZE", root)) take("ANALYSE", root);
            } else take("VERBOSE", root);
        }
        if (!terminal() && !failed_) {
            auto* list = node(NodeType::NODE_PG_DDL_LIST); if (list) list->flags = 1;
            do {
                auto* relation = clause(); add(relation, name(true));
                if (at(TokenType::TK_LPAREN)) add(relation, names());
                add(list, relation);
            } while (!failed_ && take(TokenType::TK_COMMA));
            add(root, list);
        }
    }
};

} // namespace sql_parser
#endif
