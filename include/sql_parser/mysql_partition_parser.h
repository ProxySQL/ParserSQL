#ifndef SQL_PARSER_MYSQL_PARTITION_PARSER_H
#define SQL_PARSER_MYSQL_PARTITION_PARSER_H

#include "sql_parser/expression_parser.h"
#include "sql_parser/mysql_value_syntax.h"
#include "sql_parser/subquery_parse_callback.h"
#include <limits>

namespace sql_parser {

// Native MySQL partition productions, shared by CREATE and ALTER TABLE.
// Partition expressions, names and values remain independently editable.
class MySQLPartitionParser {
public:
    MySQLPartitionParser(Tokenizer<Dialect::MySQL>& tok, Arena& arena,
                         SubqueryParseCallback<Dialect::MySQL> callback = nullptr)
        : tok_(tok), arena_(arena), callback_(callback) {}

    AstNode* parse() {
        require("PARTITION");
        require("BY");
        auto* root = node(NodeType::NODE_MYSQL_PARTITION_CLAUSE, "PARTITION BY");
        add(root, method(false));
        count(root, "PARTITIONS");
        if (take("SUBPARTITION")) {
            require("BY");
            auto* sub = clause("SUBPARTITION BY");
            add(sub, method(true));
            count(sub, "SUBPARTITIONS");
            add(root, sub);
        }
        // Native grammar shifts '(' into partition definitions here. A grouped
        // CREATE query without definitions needs AS to disambiguate it.
        if (at(TokenType::TK_LPAREN)) add(root, definitions());
        return failed_ || tok_.has_error() ? nullptr : root;
    }

    AstNode* definitions() {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_MYSQL_DDL_LIST);
        do { add(list, definition(false)); } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN);
        return failed_ || tok_.has_error() ? nullptr : list;
    }

    // real_ulong_num / real_ulonglong_num: decimal integers or native hex.
    AstNode* number() {
        const Token token = tok_.peek();
        const bool hex = token.type == TokenType::TK_HEX_LITERAL;
        if (!hex && token.type != TokenType::TK_INTEGER) { fail(); return nullptr; }
        const uint32_t begin = hex ? 2 : 0;
        const uint32_t end = token.source.len - (hex && token.source.ptr[1] == '\'' ? 1 : 0);
        const unsigned base = hex ? 16 : 10;
        uint64_t value = 0;
        for (uint32_t i = begin; i < end; ++i) {
            const char ch = token.source.ptr[i];
            const unsigned digit = ch >= '0' && ch <= '9' ? ch - '0' :
                ch >= 'a' && ch <= 'f' ? ch - 'a' + 10 :
                ch >= 'A' && ch <= 'F' ? ch - 'A' + 10 : base;
            if (digit >= base) { fail(); return nullptr; }
            if (value > (std::numeric_limits<uint64_t>::max() - digit) / base) {
                // HEX_NUM has no decimal-token width restriction. Retain its
                // spelling, saturating only our zero/algorithm validation value.
                if (!hex) { fail(); return nullptr; }
                value = std::numeric_limits<uint64_t>::max();
            } else value = value * base + digit;
        }
        number_value_ = value;
        return token_node(hex ? NodeType::NODE_LITERAL_HEX : NodeType::NODE_LITERAL_INT);
    }

private:
    Tokenizer<Dialect::MySQL>& tok_;
    Arena& arena_;
    SubqueryParseCallback<Dialect::MySQL> callback_;
    bool failed_ = false;
    uint64_t number_value_ = 0;

    static bool word(const Token& token, const char* value) {
        return token.source.ptr == token.text.ptr &&
            token.text.equals_ci(value, static_cast<uint32_t>(std::strlen(value)));
    }
    bool is(const char* value) { return word(tok_.peek(), value); }
    bool at(TokenType type) { return tok_.peek().type == type; }
    void fail() { failed_ = true; tok_.flag_error_at(tok_.peek().source); }
    bool take(const char* value) {
        if (!is(value)) return false;
        tok_.skip(); return true;
    }
    bool take(TokenType type) {
        if (!at(type)) return false;
        tok_.skip(); return true;
    }
    void require(const char* value) { if (!take(value)) fail(); }
    void require(TokenType type) { if (!take(type)) fail(); }
    AstNode* node(NodeType type, const char* value = "") {
        auto* n = make_node(arena_, type, {value, static_cast<uint32_t>(std::strlen(value))});
        if (!n) fail();
        return n;
    }
    AstNode* clause(const char* value) { return node(NodeType::NODE_MYSQL_DDL_CLAUSE, value); }
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
    AstNode* identifier() {
        if (!mysql_identifier_token(tok_.peek())) { fail(); return nullptr; }
        return token_node(NodeType::NODE_IDENTIFIER);
    }
    AstNode* string() {
        if (!at(TokenType::TK_STRING)) { fail(); return nullptr; }
        return token_node(NodeType::NODE_LITERAL_STRING);
    }
    AstNode* identifier_or_text() { return at(TokenType::TK_STRING) ? string() : identifier(); }
    AstNode* names(bool empty) {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_MYSQL_DDL_LIST);
        if (!empty || !at(TokenType::TK_RPAREN)) {
            do { add(list, identifier()); } while (!failed_ && take(TokenType::TK_COMMA));
        }
        require(TokenType::TK_RPAREN);
        return list;
    }
    // NOT has lower precedence than bit_expr; explicit parentheses and function
    // arguments start full expression productions and retain their own boundary.
    static bool bit_expression(const AstNode* n) {
        if (n->type != NodeType::NODE_BINARY_OP && n->type != NodeType::NODE_UNARY_OP) return true;
        if (n->type == NodeType::NODE_UNARY_OP && n->value().equals_ci("NOT", 3)) return false;
        // The shared expression parser also supports concatenation; MySQL's
        // default SQL mode interprets || as logical OR, outside bit_expr.
        if (n->type == NodeType::NODE_BINARY_OP && n->value().equals_ci("||", 2)) return false;
        for (const auto* child = n->first_child; child; child = child->next_sibling)
            if (!bit_expression(child)) return false;
        return true;
    }
    // Some shared legacy expression branches retain unchecked SQL as an
    // identifier or accept PostgreSQL postfix syntax. They are not native
    // partition operands, even inside explicit grouping or function arguments.
    static bool validated_expression(const AstNode* n) {
        if (n->type == NodeType::NODE_ARRAY_CONSTRUCTOR ||
            n->type == NodeType::NODE_ARRAY_SUBSCRIPT ||
            n->type == NodeType::NODE_FIELD_ACCESS) return false;
        if (n->type == NodeType::NODE_QUALIFIED_NAME) return true;
        if (n->type == NodeType::NODE_IDENTIFIER && !(n->flags & FLAG_IDENT_DELIMITED)) {
            Tokenizer<Dialect::MySQL> identifier;
            identifier.reset(n->value().ptr, n->value().len);
            auto first = identifier.next_token();
            if (word(first, "INTERVAL") || word(first, "ALL") ||
                identifier.peek().type != TokenType::TK_EOF || identifier.has_error()) return false;
        }
        for (const auto* child = n->first_child; child; child = child->next_sibling)
            if (!validated_expression(child)) return false;
        return true;
    }
    AstNode* expression() {
        ExpressionParser<Dialect::MySQL> parser(tok_, arena_, true);
        if (callback_) parser.set_subquery_callback(callback_);
        auto* n = parser.parse_complete(Precedence::PG_OPERATOR);
        if (!n || !mysql_value_expression(n) || !bit_expression(n) || !validated_expression(n)) fail();
        return n;
    }
    void count(AstNode* root, const char* keyword) {
        if (!take(keyword)) return;
        auto* n = clause(keyword);
        add(n, number());
        if (!number_value_) fail();
        add(root, n);
    }
    AstNode* method(bool subpartition) {
        const bool linear = take("LINEAR");
        if (take("KEY")) {
            auto* n = clause(linear ? "LINEAR KEY" : "KEY");
            if (take("ALGORITHM")) {
                require(TokenType::TK_EQUAL);
                auto* algorithm = clause("ALGORITHM =");
                add(algorithm, number());
                if (number_value_ != 1 && number_value_ != 2) fail();
                add(n, algorithm);
            }
            add(n, names(!subpartition));
            return n;
        }
        const char* name = nullptr;
        if (take("HASH")) name = linear ? "LINEAR HASH" : "HASH";
        else if (!subpartition && !linear && take("RANGE")) name = "RANGE";
        else if (!subpartition && !linear && take("LIST")) name = "LIST";
        else { fail(); return nullptr; }
        if (name[0] != 'H' && !linear && take("COLUMNS")) {
            auto* n = clause(name[0] == 'R' ? "RANGE COLUMNS" : "LIST COLUMNS");
            add(n, names(false));
            return n;
        }
        auto* n = clause(name);
        require(TokenType::TK_LPAREN);
        auto* operands = node(NodeType::NODE_MYSQL_DDL_LIST);
        add(operands, expression());
        require(TokenType::TK_RPAREN);
        add(n, operands);
        return n;
    }
    AstNode* values(bool in) {
        require(TokenType::TK_LPAREN);
        auto* list = node(NodeType::NODE_MYSQL_DDL_LIST);
        // MySQL's VALUES IN tuple alternative requires each row to be enclosed.
        const bool tuples = in && at(TokenType::TK_LPAREN);
        do {
            if (tuples) add(list, values(false));
            else if (is("MAXVALUE")) add(list, token_node(NodeType::NODE_MYSQL_DDL_SYNTAX));
            else add(list, expression());
        } while (!failed_ && take(TokenType::TK_COMMA));
        require(TokenType::TK_RPAREN);
        return list;
    }
    bool option_start() {
        return is("TABLESPACE") || is("STORAGE") || is("ENGINE") || is("NODEGROUP") ||
            is("MAX_ROWS") || is("MIN_ROWS") || is("DATA") || is("INDEX") || is("COMMENT");
    }
    AstNode* option() {
        const char* keyword;
        enum class Operand { Identifier, Text, Number, IdentifierOrText } operand;
        if (take("TABLESPACE")) { keyword = "TABLESPACE"; operand = Operand::Identifier; }
        else if (take("STORAGE")) {
            require("ENGINE"); keyword = "STORAGE ENGINE"; operand = Operand::IdentifierOrText;
        } else if (take("ENGINE")) { keyword = "ENGINE"; operand = Operand::IdentifierOrText; }
        else if (take("NODEGROUP")) { keyword = "NODEGROUP"; operand = Operand::Number; }
        else if (take("MAX_ROWS")) { keyword = "MAX_ROWS"; operand = Operand::Number; }
        else if (take("MIN_ROWS")) { keyword = "MIN_ROWS"; operand = Operand::Number; }
        else if (take("DATA")) { require("DIRECTORY"); keyword = "DATA DIRECTORY"; operand = Operand::Text; }
        else if (take("INDEX")) { require("DIRECTORY"); keyword = "INDEX DIRECTORY"; operand = Operand::Text; }
        else { require("COMMENT"); keyword = "COMMENT"; operand = Operand::Text; }
        take(TokenType::TK_EQUAL);
        auto* n = clause(keyword);
        switch (operand) {
            case Operand::Identifier: add(n, identifier()); break;
            case Operand::Text: add(n, string()); break;
            case Operand::Number: add(n, number()); break;
            case Operand::IdentifierOrText: add(n, identifier_or_text()); break;
        }
        return n;
    }
    AstNode* definition(bool subpartition) {
        require(subpartition ? "SUBPARTITION" : "PARTITION");
        auto* n = node(NodeType::NODE_MYSQL_PARTITION_DEF, subpartition ? "SUBPARTITION" : "PARTITION");
        add(n, subpartition ? identifier_or_text() : identifier());
        if (!subpartition && take("VALUES")) {
            if (take("LESS")) {
                require("THAN");
                auto* bound = clause("VALUES LESS THAN");
                add(bound, is("MAXVALUE") ? token_node(NodeType::NODE_MYSQL_DDL_SYNTAX) : values(false));
                add(n, bound);
            } else {
                require("IN");
                auto* bound = clause("VALUES IN");
                add(bound, values(true));
                add(n, bound);
            }
        }
        while (!failed_ && option_start()) add(n, option());
        if (!subpartition && take(TokenType::TK_LPAREN)) {
            auto* list = node(NodeType::NODE_MYSQL_DDL_LIST);
            do { add(list, definition(true)); } while (!failed_ && take(TokenType::TK_COMMA));
            require(TokenType::TK_RPAREN);
            add(n, list);
        }
        return n;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_MYSQL_PARTITION_PARSER_H
