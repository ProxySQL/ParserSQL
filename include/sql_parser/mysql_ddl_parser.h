#ifndef SQL_PARSER_MYSQL_DDL_PARSER_H
#define SQL_PARSER_MYSQL_DDL_PARSER_H

#include "sql_parser/expression_parser.h"
#include "sql_parser/mysql_type_parser.h"
#include "sql_parser/mysql_partition_parser.h"
#include "sql_parser/mysql_value_syntax.h"
#include "sql_parser/parse_result.h"
#include "sql_parser/subquery_parse_callback.h"

namespace sql_parser {

// Bounded native CREATE/ALTER TABLE grammar. Unsupported specialized table
// and index options remain explicit errors.
// Lists, columns, expressions, constraints and options have separate AST nodes;
// syntax leaves are only emitted after matching their native production.
class MySQLDdlParser {
public:
    MySQLDdlParser(Tokenizer<Dialect::MySQL>& tok, Arena& arena,
                  SubqueryParseCallback<Dialect::MySQL> callback = nullptr)
        : tok_(tok), arena_(arena), callback_(callback) {}

    static bool word(const Token& token, std::string_view value) {
        return token.source.ptr == token.text.ptr &&
            token.text.equals_ci(value.data(), static_cast<uint32_t>(value.size()));
    }

    ParseResult parse(const Token& first) {
        const bool create = word(first, "CREATE");
        auto* root = make_node_from_token(arena_, create ? NodeType::NODE_MYSQL_CREATE_TABLE :
                                                        NodeType::NODE_MYSQL_ALTER_TABLE, first);
        if (!root) fail();
        if (create) take("TEMPORARY", root);
        require("TABLE", root);
        if (create && take("IF", root)) { require("NOT", root); require("EXISTS", root); }
        add(root, name(true));
        if (create) {
            create_body(root);
        } else {
            alter_body(root);
        }
        if (!terminal()) fail();
        ParseResult result;
        result.ast = root;
        result.stmt_type = create ? StmtType::CREATE : StmtType::ALTER;
        result.table_name = table_name_;
        result.schema_name = schema_name_;
        result.status = failed_ || tok_.has_error() ? ParseResult::ERROR : ParseResult::OK;
        return result;
    }

private:
    Tokenizer<Dialect::MySQL>& tok_;
    Arena& arena_;
    SubqueryParseCallback<Dialect::MySQL> callback_;
    bool failed_ = false;
    StringRef table_name_, schema_name_;

    bool is(const char* value) { return word(tok_.peek(), value); }
    bool one_of(std::initializer_list<const char*> values) {
        for (const auto* value : values) if (is(value)) return true;
        return false;
    }
    bool at(TokenType type) { return tok_.peek().type == type; }
    bool terminal() { return at(TokenType::TK_EOF) || at(TokenType::TK_SEMICOLON); }
    void fail() { failed_ = true; tok_.flag_error_at(tok_.peek().source); }
    AstNode* node(NodeType type, const char* value = "") {
        auto* n = make_node(arena_, type, {value, static_cast<uint32_t>(std::strlen(value))});
        if (!n) fail();
        return n;
    }
    AstNode* clause(const char* value = "") { return node(NodeType::NODE_MYSQL_DDL_CLAUSE, value); }
    void add(AstNode* parent, AstNode* child) {
        if (!parent || !child) { fail(); return; }
        parent->add_child(child);
    }
    AstNode* token_node(NodeType type) {
        auto token = tok_.next_token();
        auto* n = make_node_from_token(arena_, type, token);
        if (!n) fail();
        else if (type == NodeType::NODE_IDENTIFIER && token.source.ptr != token.text.ptr)
            n->flags |= FLAG_IDENT_DELIMITED;
        return n;
    }
    void syntax(AstNode* n) { add(n, token_node(NodeType::NODE_MYSQL_DDL_SYNTAX)); }
    bool take(const char* value, AstNode* n = nullptr) {
        if (!is(value)) return false;
        if (n) syntax(n); else tok_.skip();
        return true;
    }
    bool take(TokenType type) {
        if (!at(type)) return false;
        tok_.skip(); return true;
    }
    void require(const char* value, AstNode* n = nullptr) { if (!take(value, n)) fail(); }
    void require(TokenType type) { if (!take(type)) fail(); }
    AstNode* identifier(bool qualified = false) {
        const auto& token = tok_.peek();
        if (!(qualified ? mysql_identifier_word(token) : mysql_identifier_token(token))) {
            fail(); return nullptr;
        }
        return token_node(NodeType::NODE_IDENTIFIER);
    }
    AstNode* name(bool relation = false) {
        auto* n = identifier();
        if (take(TokenType::TK_DOT)) {
            auto* q = node(NodeType::NODE_QUALIFIED_NAME);
            add(q, n); add(q, identifier(true)); n = q;
        }
        if (relation && n && !failed_) {
            if (n->type == NodeType::NODE_QUALIFIED_NAME) {
                schema_name_ = n->first_child->value();
                table_name_ = n->first_child->next_sibling->value();
            } else table_name_ = n->value();
        }
        return n;
    }
    // Only distinguish the query branch from column definitions here. The
    // strict query parser validates grouping, operands and clause boundaries.
    bool query_start() {
        auto look = tok_;
        while (look.peek().type == TokenType::TK_LPAREN) look.skip();
        return ExpressionParser<Dialect::MySQL>::starts_query(look.peek().type);
    }
    bool query_suffix() { return query_start() || one_of({"AS", "IGNORE", "REPLACE"}); }
    void create_body(AstNode* root) {
        auto look = tok_;
        const bool parenthesized = look.next_token().type == TokenType::TK_LPAREN &&
                                   word(look.peek(), "LIKE");
        if (is("LIKE") || parenthesized) {
            if (parenthesized) tok_.skip();
            require("LIKE");
            auto* source = node(NodeType::NODE_MYSQL_CREATE_LIKE, "LIKE");
            add(source, name()); // Source must not replace target routing metadata.
            add(root, source);
            if (parenthesized) require(TokenType::TK_RPAREN);
            return;
        }
        if (at(TokenType::TK_LPAREN) && !query_start()) {
            tok_.skip();
            auto* definitions = node(NodeType::NODE_MYSQL_DDL_LIST);
            do { add(definitions, definition()); } while (!failed_ && take(TokenType::TK_COMMA));
            require(TokenType::TK_RPAREN);
            add(root, definitions);
        }
        while (!failed_ && !terminal() && !query_suffix() && !is("PARTITION")) {
            add(root, table_option());
            // A comma separates table options, never the following query.
            if (take(TokenType::TK_COMMA) && (terminal() || query_suffix() || is("PARTITION"))) fail();
        }
        if (!failed_ && is("PARTITION"))
            add(root, MySQLPartitionParser(tok_, arena_, callback_).parse());
        if (failed_ || terminal()) return;
        const char* prefix = "AS";
        if (take("IGNORE")) prefix = "IGNORE AS";
        else if (take("REPLACE")) {
            prefix = "REPLACE AS";
            // Native duplicate REPLACE ignores the lexer-attached hint list.
            if (at(TokenType::TK_MYSQL_OPTIMIZER_HINT)) tok_.skip();
        }
        take("AS");
        if (!query_start()) { fail(); return; }
        auto* source = node(NodeType::NODE_MYSQL_CREATE_QUERY, prefix);
        auto callback = callback_ ? callback_ : &parse_subquery_select<Dialect::MySQL>;
        add(source, callback(tok_, arena_));
        add(root, source);
    }

    bool partition_tail() { return is("PARTITION") || is("REMOVE"); }
    bool partition_action_start() {
        if (!one_of({"ADD", "DROP", "REBUILD", "OPTIMIZE", "ANALYZE", "CHECK",
                     "REPAIR", "COALESCE", "TRUNCATE", "REORGANIZE", "EXCHANGE"})) return false;
        auto look = tok_; look.skip();
        return word(look.peek(), "PARTITION");
    }
    bool alter_modifier() { return one_of({"ALGORITHM", "LOCK", "WITH", "WITHOUT"}); }
    AstNode* validation() {
        auto* result = clause();
        if (!take("WITH", result) && !take("WITHOUT", result)) fail();
        require("VALIDATION", result); return result;
    }
    AstNode* partition_names(bool allow_all) {
        if (allow_all && is("ALL")) return token_node(NodeType::NODE_MYSQL_DDL_SYNTAX);
        auto* names = node(NodeType::NODE_MYSQL_PARTITION_NAMES);
        do { add(names, identifier()); } while (!failed_ && take(TokenType::TK_COMMA));
        return names;
    }
    AstNode* partition_action() {
        const Token first = tok_.peek();
        auto* result = token_node(NodeType::NODE_MYSQL_PARTITION_ACTION);
        require("PARTITION", result);
        const bool binlog = word(first, "ADD") || word(first, "REBUILD") || word(first, "OPTIMIZE") ||
            word(first, "ANALYZE") || word(first, "REPAIR") || word(first, "COALESCE") ||
            word(first, "REORGANIZE");
        if (binlog && !take("NO_WRITE_TO_BINLOG", result)) take("LOCAL", result);
        MySQLPartitionParser partition(tok_, arena_, callback_);
        if (word(first, "ADD")) {
            if (take("PARTITIONS", result)) add(result, partition.number());
            else if (at(TokenType::TK_LPAREN)) add(result, partition.definitions());
        } else if (word(first, "COALESCE")) add(result, partition.number());
        else if (word(first, "REORGANIZE")) {
            if (!terminal()) {
                add(result, partition_names(false)); require("INTO", result);
                add(result, partition.definitions());
            }
        } else if (word(first, "EXCHANGE")) {
            add(result, identifier()); require("WITH", result); require("TABLE", result);
            add(result, name());
            if (one_of({"WITH", "WITHOUT"})) add(result, validation());
        } else {
            add(result, partition_names(!word(first, "DROP")));
            if (word(first, "CHECK")) {
                while (!failed_ && one_of({"QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED", "FOR"})) {
                    if (take("FOR", result)) require("UPGRADE", result); else syntax(result);
                }
            } else if (word(first, "REPAIR")) {
                while (!failed_ && one_of({"QUICK", "EXTENDED", "USE_FRM"})) syntax(result);
            }
        }
        return result;
    }
    void alter_body(AstNode* root) {
        auto* actions = node(NodeType::NODE_MYSQL_ALTER_ACTIONS);
        bool modifiers_only = true;
        while (!failed_ && !partition_tail()) {
            if (partition_action_start()) {
                if (!modifiers_only) { fail(); break; }
                add(actions, partition_action());
                // Standalone partition actions cannot have trailing modifiers
                // or ordinary column/index actions.
                if (!terminal()) fail();
                break;
            }
            const bool modifier = alter_modifier();
            add(actions, one_of({"WITH", "WITHOUT"}) ? validation() : action());
            modifiers_only = modifiers_only && modifier;
            if (!take(TokenType::TK_COMMA)) break;
            if (partition_tail()) { fail(); break; }
        }
        if (actions && actions->first_child) add(root, actions);
        if (failed_) return;
        // Native repartition/removal follows ordinary actions without a comma.
        if (is("PARTITION")) add(root, MySQLPartitionParser(tok_, arena_, callback_).parse());
        else if (is("REMOVE")) {
            auto* remove = clause(); require("REMOVE", remove); require("PARTITIONING", remove);
            add(root, remove);
        }
    }

    AstNode* expression() {
        ExpressionParser<Dialect::MySQL> parser(tok_, arena_, true);
        if (callback_) parser.set_subquery_callback(callback_);
        auto* n = parser.parse_complete();
        if (!n || !mysql_value_expression(n)) fail();
        return n;
    }
    AstNode* parenthesized_expression() {
        require(TokenType::TK_LPAREN);
        auto* n = node(NodeType::NODE_MYSQL_DDL_LIST);
        add(n, expression());
        require(TokenType::TK_RPAREN);
        return n;
    }
    AstNode* string() {
        if (!at(TokenType::TK_STRING)) { fail(); return nullptr; }
        return token_node(NodeType::NODE_LITERAL_STRING);
    }
    AstNode* integer(bool native_num = false) {
        const auto& t = tok_.peek();
        if (t.type != TokenType::TK_INTEGER) { fail(); return nullptr; }
        uint32_t value = 0;
        for (uint32_t i = 0; i < t.source.len; ++i) {
            const char ch = t.source.ptr[i];
            if (ch < '0' || ch > '9') { fail(); return nullptr; }
            if (native_num) {
                const auto digit = static_cast<uint32_t>(ch - '0');
                if (value > (2147483647u - digit) / 10) { fail(); return nullptr; }
                value = value * 10 + digit;
            }
        }
        return token_node(NodeType::NODE_LITERAL_INT);
    }
    AstNode* charset_name() {
        if (mysql_charset_introducer(tok_.peek())) { fail(); return nullptr; }
        if (at(TokenType::TK_STRING)) return string();
        if (is("BINARY")) return token_node(NodeType::NODE_MYSQL_DDL_SYNTAX);
        return identifier();
    }
    AstNode* now() {
        if (!one_of({"CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP"})) {
            fail(); return nullptr;
        }
        const Token token = tok_.next_token();
        const bool needs_parentheses = word(token, "NOW");
        if (needs_parentheses && (!at(TokenType::TK_LPAREN) ||
            token.source.ptr + token.source.len != tok_.peek().source.ptr)) {
            fail(); return nullptr;
        }
        const bool parentheses = take(TokenType::TK_LPAREN);
        auto* n = make_node_from_token(arena_, parentheses ? NodeType::NODE_FUNCTION_CALL :
                                                            NodeType::NODE_MYSQL_DDL_SYNTAX, token);
        if (!n) { fail(); return nullptr; }
        if (parentheses) {
            if (!at(TokenType::TK_RPAREN)) add(n, integer(true));
            require(TokenType::TK_RPAREN);
        }
        return n;
    }
    AstNode* default_value() {
        if (at(TokenType::TK_LPAREN)) return parenthesized_expression();
        if (one_of({"CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP"})) return now();
        auto* value = clause();
        bool signed_value = at(TokenType::TK_PLUS) || at(TokenType::TK_MINUS);
        if (signed_value) syntax(value);
        if (at(TokenType::TK_INTEGER)) add(value, token_node(NodeType::NODE_LITERAL_INT));
        else if (at(TokenType::TK_FLOAT)) add(value, token_node(NodeType::NODE_LITERAL_FLOAT));
        else if (!signed_value && at(TokenType::TK_STRING)) add(value, string());
        else if (!signed_value && one_of({"NULL", "TRUE", "FALSE"})) syntax(value);
        else { fail(); return value; }
        return value;
    }
    void enforcement(AstNode* n) {
        if (is("NOT")) {
            auto look = tok_; look.skip();
            if (word(look.peek(), "ENFORCED")) { syntax(n); syntax(n); }
        } else take("ENFORCED", n);
    }
    AstNode* check() {
        auto* c = clause();
        require("CHECK", c); add(c, parenthesized_expression()); enforcement(c);
        return c;
    }
    AstNode* column() {
        auto* c = node(NodeType::NODE_MYSQL_COLUMN_DEF);
        add(c, identifier());
        const auto type = MySQLTypeParser(tok_).parse();
        if (type.empty()) { fail(); return c; }
        auto* type_node = make_node(arena_, NodeType::NODE_TYPE_NAME, type);
        if (type_node) type_node->set_source(type);
        add(c, type_node);
        bool generated = false, attributes_started = false;
        unsigned collations = 0;
        while (!failed_) {
            const bool collate = is("COLLATE");
            auto* attr = clause();
            if (take("NOT", attr)) {
                if (!take("NULL", attr)) require("SECONDARY", attr);
            } else if (take("NULL", attr) || take("AUTO_INCREMENT", attr) ||
                       take("VISIBLE", attr) || take("INVISIBLE", attr)) {
            } else if (take("DEFAULT", attr)) add(attr, default_value());
            else if (take("ON", attr)) { require("UPDATE", attr); add(attr, now()); }
            else if (take("PRIMARY", attr)) require("KEY", attr);
            else if (take("KEY", attr)) {}
            else if (take("UNIQUE", attr)) take("KEY", attr);
            else if (take("COMMENT", attr)) add(attr, string());
            else if (take("COLLATE", attr)) add(attr, charset_name());
            else if (take("SRID", attr)) add(attr, integer());
            else if (take("COLUMN_FORMAT", attr)) {
                if (!one_of({"DEFAULT", "FIXED", "DYNAMIC"})) fail(); else syntax(attr);
            } else if (take("STORAGE", attr)) {
                if (!one_of({"DEFAULT", "DISK", "MEMORY"})) fail(); else syntax(attr);
            } else if (take("SERIAL", attr)) { require("DEFAULT", attr); require("VALUE", attr); }
            else if (is("CHECK")) attr = check();
            else if (is("CONSTRAINT")) {
                syntax(attr);
                if (!is("CHECK")) add(attr, identifier());
                add(attr, check());
            } else if (is("GENERATED") || is("AS")) {
                if (generated || attributes_started) { fail(); return c; }
                generated = true;
                if (take("GENERATED", attr)) require("ALWAYS", attr);
                require("AS", attr); add(attr, parenthesized_expression());
                if (!take("STORED", attr)) take("VIRTUAL", attr);
            } else break;
            add(c, attr);
            if (collate) ++collations;
            if (!collate || collations > 1 || attributes_started) attributes_started = true;
        }
        return c;
    }
    AstNode* key_list(bool prefix = true) {
        auto* list = node(NodeType::NODE_MYSQL_DDL_LIST);
        require(TokenType::TK_LPAREN);
        do {
            auto* part = clause();
            add(part, identifier());
            if (prefix && take(TokenType::TK_LPAREN)) {
                auto* length = node(NodeType::NODE_MYSQL_DDL_LIST);
                add(length, integer(true)); require(TokenType::TK_RPAREN); add(part, length);
            }
            if (prefix && !take("ASC", part)) take("DESC", part);
            add(list, part);
        } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN);
        return list;
    }
    void index_method(AstNode* n) {
        if (take("USING", n)) {
            if (!one_of({"BTREE", "HASH"})) fail(); else syntax(n);
        }
    }
    void reference_action(AstNode* n) {
        if (take("SET", n)) require("NULL", n);
        else if (take("NO", n)) require("ACTION", n);
        else if (!take("RESTRICT", n) && !take("CASCADE", n)) fail();
    }
    AstNode* constraint() {
        auto* n = clause();
        bool named = take("CONSTRAINT", n);
        if (named && !one_of({"PRIMARY", "UNIQUE", "FOREIGN", "CHECK"})) add(n, identifier());
        if (is("CHECK")) { add(n, check()); return n; }
        if (take("FOREIGN", n)) {
            require("KEY", n);
            if (!at(TokenType::TK_LPAREN)) add(n, identifier());
            add(n, key_list()); require("REFERENCES", n); add(n, name());
            add(n, key_list(false));
            if (take("MATCH", n)) {
                if (!one_of({"FULL", "PARTIAL", "SIMPLE"})) fail(); else syntax(n);
            }
            bool update = false, deletion = false;
            while (take("ON", n) && !failed_) {
                if (take("UPDATE", n) && !update) update = true;
                else if (take("DELETE", n) && !deletion) deletion = true;
                else { fail(); break; }
                reference_action(n);
            }
            return n;
        }
        bool specialized = false;
        if (take("PRIMARY", n)) {
            require("KEY", n);
            if (!at(TokenType::TK_LPAREN) && !is("USING")) add(n, identifier());
        }
        else if (take("UNIQUE", n)) {
            if (!take("KEY", n)) take("INDEX", n);
            if (!at(TokenType::TK_LPAREN) && !is("USING")) add(n, identifier());
        } else {
            if (named) { fail(); return n; }
            if (take("FULLTEXT", n) || take("SPATIAL", n)) {
                specialized = true;
                if (!take("KEY", n)) take("INDEX", n);
            } else if (!take("KEY", n) && !take("INDEX", n)) { fail(); return n; }
            if (!at(TokenType::TK_LPAREN) && !is("USING")) add(n, identifier());
        }
        if (!specialized) index_method(n);
        add(n, key_list());
        while (!failed_) {
            if (!specialized && is("USING")) index_method(n);
            else if (take("COMMENT", n)) add(n, string());
            else if (take("VISIBLE", n) || take("INVISIBLE", n)) {}
            else if (take("KEY_BLOCK_SIZE", n)) { take(TokenType::TK_EQUAL); add(n, integer()); }
            else break;
        }
        return n;
    }
    AstNode* definition() {
        return one_of({"CONSTRAINT", "PRIMARY", "UNIQUE", "KEY", "INDEX", "FULLTEXT", "SPATIAL", "CHECK", "FOREIGN"})
            ? constraint() : column();
    }
    AstNode* table_option(bool alter = false) {
        auto* n = clause();
        if (take("ENGINE", n)) {
            take(TokenType::TK_EQUAL); add(n, at(TokenType::TK_STRING) ? string() : identifier());
        } else if (take("SECONDARY_ENGINE", n)) {
            take(TokenType::TK_EQUAL);
            if (!take("NULL", n)) add(n, at(TokenType::TK_STRING) ? string() : identifier());
        } else if (take("DEFAULT", n)) {
            if (take("CHARACTER", n) || take("CHAR", n)) require("SET", n);
            else if (!take("CHARSET", n) && !take("COLLATE", n)) { fail(); return n; }
            take(TokenType::TK_EQUAL); add(n, charset_name());
        } else if (one_of({"CHARACTER", "CHAR", "CHARSET", "COLLATE"})) {
            if (take("CHARACTER", n) || take("CHAR", n)) require("SET", n); else syntax(n);
            take(TokenType::TK_EQUAL); add(n, charset_name());
        } else if (one_of({"AUTO_INCREMENT", "AVG_ROW_LENGTH", "CHECKSUM", "DELAY_KEY_WRITE", "KEY_BLOCK_SIZE", "MAX_ROWS", "MIN_ROWS", "STATS_SAMPLE_PAGES"})) {
            syntax(n); take(TokenType::TK_EQUAL); add(n, integer());
        } else if (one_of({"COMMENT", "COMPRESSION", "ENCRYPTION", "CONNECTION"})) {
            syntax(n); take(TokenType::TK_EQUAL); add(n, string());
        } else if (take("ROW_FORMAT", n)) {
            take(TokenType::TK_EQUAL);
            if (!one_of({"DEFAULT", "DYNAMIC", "FIXED", "COMPRESSED", "REDUNDANT", "COMPACT"})) fail(); else syntax(n);
        } else if (alter && take("ALGORITHM", n)) {
            take(TokenType::TK_EQUAL);
            if (!one_of({"DEFAULT", "INPLACE", "COPY", "INSTANT"})) fail(); else syntax(n);
        } else if (alter && take("LOCK", n)) {
            take(TokenType::TK_EQUAL);
            if (!one_of({"DEFAULT", "NONE", "SHARED", "EXCLUSIVE"})) fail(); else syntax(n);
        } else fail();
        return n;
    }
    void placement(AstNode* n) {
        if (take("AFTER", n)) add(n, identifier());
        else take("FIRST", n);
    }
    AstNode* action() {
        auto* n = clause();
        if (take("ADD", n)) {
            bool explicit_column = take("COLUMN", n);
            if (take(TokenType::TK_LPAREN)) {
                auto* list = node(NodeType::NODE_MYSQL_DDL_LIST);
                do { add(list, column()); } while (!failed_ && take(TokenType::TK_COMMA));
                require(TokenType::TK_RPAREN); add(n, list);
            } else {
                auto* d = explicit_column ? column() : definition();
                add(n, d);
                if (d && d->type == NodeType::NODE_MYSQL_COLUMN_DEF) placement(n);
            }
        } else if (take("MODIFY", n)) {
            take("COLUMN", n); add(n, column()); placement(n);
        } else if (take("CHANGE", n)) {
            take("COLUMN", n); add(n, identifier()); add(n, column()); placement(n);
        } else if (take("DROP", n)) {
            if (take("PRIMARY", n)) require("KEY", n);
            else if (take("FOREIGN", n)) { require("KEY", n); add(n, identifier()); }
            else if (take("INDEX", n) || take("KEY", n) || take("CHECK", n) || take("CONSTRAINT", n)) add(n, identifier());
            else { take("COLUMN", n); add(n, identifier()); }
        } else if (take("RENAME", n)) {
            if (take("COLUMN", n) || take("INDEX", n) || take("KEY", n)) {
                add(n, identifier()); require("TO", n); add(n, identifier());
            } else {
                if (!take("TO", n)) take("AS", n);
                add(n, name());
            }
        } else return table_option(true);
        return n;
    }
};

} // namespace sql_parser
#endif
