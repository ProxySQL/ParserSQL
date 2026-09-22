#ifndef SQL_PARSER_EXPRESSION_PARSER_H
#define SQL_PARSER_EXPRESSION_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/user_variable.h"
#include "sql_parser/pg_type_parser.h"
#include "sql_parser/pg_identifier.h"

namespace sql_parser {

// Operator precedence levels for Pratt parsing
enum class Precedence : uint8_t {
    NONE = 0,
    OR,            // OR
    XOR,           // XOR
    AND,           // AND
    NOT,           // NOT (prefix)
    PG_IS,
    PG_COMPARISON,
    PG_PREDICATE,
    COMPARISON,    // =, <, >, <=, >=, !=, <>, IS, LIKE, IN, BETWEEN
    PG_OPERATOR,
    BIT_OR,        // |
    BIT_XOR,       // ^
    BIT_AND,       // &
    SHIFT,         // <<, >>
    ADDITION,      // +, -
    MULTIPLICATION,// *, /, %
    EXPONENT,
    COLLATION,
    UNARY,         // - (prefix), NOT
    POSTFIX,       // IS NULL, IS NOT NULL
    CALL,          // function()
    PRIMARY,       // literals, identifiers
};

// Callback type for parsing subqueries inside expressions.
// When set, called instead of skip_to_matching_paren when (SELECT ...) is encountered.
// The tokenizer is positioned ON the SELECT keyword (not yet consumed).
// The callback should consume SELECT and parse the full statement, returning its AST.
// The closing ')' should NOT be consumed by the callback.
template <Dialect D>
using SubqueryParseCallback = AstNode*(*)(Tokenizer<D>&, Arena&);

template <Dialect D>
class ExpressionParser {
public:
    ExpressionParser(Tokenizer<D>& tokenizer, Arena& arena,
                     bool require_complete_operands = false)
        : tok_(tokenizer), arena_(arena), require_complete_operands_(require_complete_operands) {}

    // Set a callback for parsing subqueries. When set and SELECT is encountered
    // inside parens, calls it instead of skipping.
    void set_subquery_callback(SubqueryParseCallback<D> cb) { subquery_cb_ = cb; }

    static bool keyword(const Token& token, const char* word) {
        return token.source.ptr == token.text.ptr &&
               token.text.equals_ci(word, static_cast<uint32_t>(std::strlen(word)));
    }

    AstNode* syntax_error() {
        StringRef source = tok_.peek().source;
        if (source.empty() && tok_.input_end() > tok_.input_begin())
            source = StringRef{tok_.input_end() - 1, 1};
        tok_.flag_fatal_error_at(source);
        return nullptr;
    }

    bool has_operand_error() const { return operand_error_; }

    AstNode* parse_complete(Precedence precedence = Precedence::NONE) {
        bool previous = require_complete_operands_;
        bool previous_error = operand_error_;
        operand_error_ = false;
        require_complete_operands_ = true;
        AstNode* result = parse(precedence);
        require_complete_operands_ = previous;
        operand_error_ = previous_error || operand_error_;
        return result;
    }

    // Parse an expression with minimum precedence 0
    AstNode* parse(Precedence min_prec = Precedence::NONE) {
        AstNode* left = parse_atom();
        if (!left) {
            operand_error_ = true;
            return nullptr;
        }
        if (require_complete_operands_ && operand_error_) return nullptr;
        if constexpr (D == Dialect::PostgreSQL) {
            left = parse_postfix(left);
            if (!left) return nullptr;
        }

        while (true) {
            Precedence prec = infix_precedence(tok_.peek().type);
            if (prec <= min_prec) break;

            left = parse_infix(left, prec);
            if (!left) {
                operand_error_ = true;
                return nullptr;
            }
            if (require_complete_operands_ && operand_error_) return nullptr;
            if constexpr (D == Dialect::PostgreSQL) {
                if (tok_.peek().type == TokenType::TK_DOUBLE_COLON) {
                    left = parse_postfix(left);
                    if (!left) return nullptr;
                }
            }
        }

        return left;
    }

    // Named arguments are only valid inside function/procedure argument lists.
    AstNode* parse_argument(bool complete = false) {
        if constexpr (D == Dialect::PostgreSQL) {
            Token name = tok_.peek();
            {
                auto lookahead = tok_;
                lookahead.skip();
                Token separator = lookahead.peek();
                if (separator.type == TokenType::TK_NAMED_ARGUMENT || separator.type == TokenType::TK_COLON_EQUAL) {
                    if (!pg_type_function_name(name)) return syntax_error();
                    lookahead.skip();
                    tok_ = lookahead;
                    AstNode* value = parse_complete();
                    if (!value) return syntax_error();
                    AstNode* node = make_node(arena_, NodeType::NODE_NAMED_ARGUMENT,
                        name.source.empty() ? name.text : name.source);
                    if (!node) return syntax_error();
                    node->add_child(value);
                    return node;
                }
            }
        }
        return complete ? parse_complete() : parse();
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    SubqueryParseCallback<D> subquery_cb_ = nullptr;
    // Existing callers tolerate partially understood PostgreSQL operators.
    // VALUES requires complete operands rather than silently dropping an operator.
    bool require_complete_operands_;
    bool operand_error_ = false;

    // Parse a subquery: if callback is set, use it; otherwise skip.
    // The tokenizer is positioned right after '(' and on the SELECT keyword.
    // Returns a NODE_SUBQUERY node, possibly with a parsed SELECT child.
    AstNode* parse_subquery_inner() {
        AstNode* node = make_node(arena_, NodeType::NODE_SUBQUERY);
        if (subquery_cb_) {
            // Callback parses from current position (on SELECT keyword).
            // It should consume everything up to but NOT including ')'.
            AstNode* inner = subquery_cb_(tok_, arena_);
            if (!inner && require_complete_operands_) return syntax_error();
            if (inner) node->add_child(inner);
            // Consume the closing ')'
            if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
        } else {
            // Legacy: skip to matching paren
            const char* start = tok_.peek().text.ptr;
            const char* end = skip_to_matching_paren();
            if (start && end && end >= start) {
                node->set_value(StringRef{start,
                    static_cast<uint32_t>(end - start)});
            }
        }
        return node;
    }

    // Parse a primary expression (atom)
    AstNode* parse_atom() {
        Token t = tok_.peek();
        if constexpr (D == Dialect::PostgreSQL) {
            if (PgTypeParser::name_token(t) && !keyword(t, "INTERVAL")) {
                auto lookahead = tok_;
                lookahead.skip();
                const Token next = lookahead.peek();
                // Avoid scanning a complete type for ordinary column references.
                if (next.type == TokenType::TK_STRING || next.type == TokenType::TK_LPAREN ||
                    next.type == TokenType::TK_DOT || keyword(t, "TIMESTAMP") ||
                    keyword(t, "TIME") || keyword(t, "DOUBLE") || keyword(t, "CHARACTER") ||
                    keyword(t, "CHAR") || keyword(t, "NCHAR") || keyword(t, "NATIONAL") || keyword(t, "BIT")) {
                    lookahead = tok_;
                    StringRef type = PgTypeParser(lookahead).parse(false, true);
                    if (!type.empty() && lookahead.peek().type == TokenType::TK_STRING) {
                        Token literal = lookahead.next_token();
                        tok_ = lookahead;
                        AstNode* value = make_node_from_token(arena_, NodeType::NODE_LITERAL_STRING, literal);
                        return make_cast(value, type);
                    }
                }
            }
        }

        switch (t.type) {
            case TokenType::TK_INTEGER: {
                tok_.skip();
                return make_node_from_token(arena_, NodeType::NODE_LITERAL_INT, t);
            }
            case TokenType::TK_FLOAT: {
                tok_.skip();
                return make_node_from_token(arena_, NodeType::NODE_LITERAL_FLOAT, t);
            }
            case TokenType::TK_HEX_LITERAL: {
                tok_.skip();
                return make_node_from_token(arena_, NodeType::NODE_LITERAL_HEX, t);
            }
            case TokenType::TK_BIT_LITERAL: {
                tok_.skip();
                return make_node_from_token(arena_, NodeType::NODE_LITERAL_BIT, t);
            }
            case TokenType::TK_STRING: {
                tok_.skip();
                return make_node_from_token(arena_, NodeType::NODE_LITERAL_STRING, t);
            }
            case TokenType::TK_NULL: {
                tok_.skip();
                return make_node_from_token(arena_, NodeType::NODE_LITERAL_NULL, t);
            }
            case TokenType::TK_TRUE:
            case TokenType::TK_FALSE: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_LITERAL_INT, t.text);
            }
            case TokenType::TK_DEFAULT: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_IDENTIFIER, t.text);
            }
            case TokenType::TK_INTERVAL: {
                return parse_interval_literal(t);
            }
            case TokenType::TK_ALL: {
                return parse_quantified_subquery(t);
            }
            case TokenType::TK_ASTERISK: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_ASTERISK, t.text);
            }
            case TokenType::TK_QUESTION: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_PLACEHOLDER, t.text);
            }
            case TokenType::TK_DOLLAR_NUM: {
                tok_.skip();
                return make_node(arena_, NodeType::NODE_PLACEHOLDER, t.text);
            }
            case TokenType::TK_AT: {
                // User variable: @name
                tok_.skip();
                Token name = tok_.next_token();
                // Build @name as a single COLUMN_REF with combined text
                // value_ptr points to @ in original input, len covers @name
                StringRef full{t.text.ptr,
                    static_cast<uint32_t>((name.text.ptr + name.text.len) - t.text.ptr)};
                return make_node(arena_, NodeType::NODE_COLUMN_REF, full);
            }
            case TokenType::TK_USER_VARIABLE: {
                tok_.skip();
                return make_mysql_user_variable_node(arena_, t);
            }
            case TokenType::TK_DOUBLE_AT: {
                // System variable: @@name or @@scope.name
                tok_.skip();
                Token name = tok_.next_token();
                StringRef full{t.text.ptr,
                    static_cast<uint32_t>((name.text.ptr + name.text.len) - t.text.ptr)};
                AstNode* node = make_node(arena_, NodeType::NODE_COLUMN_REF, full);
                // Check for @@scope.name
                if (tok_.peek().type == TokenType::TK_DOT) {
                    tok_.skip();
                    Token var_name = tok_.next_token();
                    full = StringRef{t.text.ptr,
                        static_cast<uint32_t>((var_name.text.ptr + var_name.text.len) - t.text.ptr)};
                    node->value_ptr = full.ptr;
                    node->value_len = full.len;
                }
                return node;
            }
            case TokenType::TK_PG_OPERATOR: {
                if constexpr (D == Dialect::PostgreSQL) {
                    tok_.skip();
                    AstNode* operand = parse_complete(Precedence::PG_OPERATOR);
                    if (!operand) return syntax_error();
                    AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text, FLAG_PG_OPERATOR);
                    if (!node) return syntax_error();
                    node->add_child(operand);
                    return node;
                }
                return nullptr;
            }
            case TokenType::TK_MINUS: {
                // Unary minus
                tok_.skip();
                AstNode* operand = parse(Precedence::UNARY);
                if (!operand) return nullptr;
                AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text);
                set_span_through_node_(node, t.source, operand);
                node->add_child(operand);
                return node;
            }
            case TokenType::TK_PLUS: {
                // Unary plus
                tok_.skip();
                AstNode* operand = parse(Precedence::UNARY);
                if (!operand) return nullptr;
                AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text);
                set_span_through_node_(node, t.source, operand);
                node->add_child(operand);
                return node;
            }
            case TokenType::TK_NOT: {
                tok_.skip();
                AstNode* operand = parse(Precedence::NOT);
                if (!operand) return nullptr;
                AstNode* node = make_node(arena_, NodeType::NODE_UNARY_OP, t.text);
                set_span_through_node_(node, t.source, operand);
                node->add_child(operand);
                return node;
            }
            case TokenType::TK_EXISTS: {
                tok_.skip();
                // EXISTS (subquery)
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    // We expect SELECT inside
                    if (tok_.peek().type == TokenType::TK_SELECT) {
                        AstNode* node = parse_subquery_inner();
                        // Mark as EXISTS subquery via flags
                        node->flags = 1; // 1 = EXISTS context
                        return node;
                    }
                    // Fallback: skip
                    AstNode* node = make_node(arena_, NodeType::NODE_SUBQUERY);
                    node->flags = 1;
                    skip_to_matching_paren();
                    return node;
                }
                AstNode* node = make_node(arena_, NodeType::NODE_SUBQUERY);
                node->flags = 1;
                return node;
            }
            case TokenType::TK_ARRAY: {
                tok_.skip();
                return parse_array_constructor();
            }
            case TokenType::TK_ROW: {
                // ROW(expr, expr, ...) — explicit row constructor
                tok_.skip();
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    AstNode* tuple = make_node(arena_, NodeType::NODE_TUPLE, t.text);
                    if (tok_.peek().type != TokenType::TK_RPAREN) {
                        while (true) {
                            AstNode* elem = parse();
                            if (elem) tuple->add_child(elem);
                            if (tok_.peek().type == TokenType::TK_COMMA) tok_.skip();
                            else break;
                        }
                    }
                    if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                    return parse_postfix(tuple);
                }
                return make_node(arena_, NodeType::NODE_IDENTIFIER, t.text);
            }
            case TokenType::TK_CASE: {
                tok_.skip();
                return parse_case();
            }
            case TokenType::TK_LPAREN: {
                tok_.skip();
                // Could be subquery: (SELECT ...)
                if (tok_.peek().type == TokenType::TK_SELECT) {
                    AstNode* node = parse_subquery_inner();
                    return parse_postfix(node);
                }
                // Empty tuple: ()
                if (tok_.peek().type == TokenType::TK_RPAREN) {
                    tok_.skip();
                    AstNode* tuple = make_node(arena_, NodeType::NODE_TUPLE);
                    return parse_postfix(tuple);
                }
                AstNode* expr = parse();
                if (tok_.peek().type == TokenType::TK_COMMA) {
                    // Tuple: (expr, expr, ...)
                    AstNode* tuple = make_node(arena_, NodeType::NODE_TUPLE);
                    if (expr) tuple->add_child(expr);
                    while (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                        AstNode* elem = parse();
                        if (elem) tuple->add_child(elem);
                    }
                    if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                    return parse_postfix(tuple);
                }
                if (tok_.peek().type == TokenType::TK_RPAREN) {
                    Token close = tok_.next_token();
                    AstNode* wrapper = make_node(arena_, NodeType::NODE_EXPRESSION);
                    wrapper->set_source(StringRef{t.source.ptr,
                        static_cast<uint32_t>(close.source.ptr + close.source.len - t.source.ptr)});
                    wrapper->add_child(expr);
                    return parse_postfix(wrapper);
                }
                // Check for postfix: (expr).field or (expr)[index]
                return parse_postfix(expr);
            }
            case TokenType::TK_IDENTIFIER: {
                tok_.skip();
                return parse_identifier_or_function(t);
            }
            // Keywords that can appear as identifiers in expression context
            // (e.g., column names that happen to be keywords)
            default: {
                if (is_keyword_as_identifier(t.type)) {
                    tok_.skip();
                    return parse_identifier_or_function(t);
                }
                return nullptr;  // not an expression
            }
        }
    }

    static void set_span_through_node_(AstNode* node, StringRef start,
                                       const AstNode* end_node) {
        if (!node || !start.ptr || !end_node) return;
        StringRef end = end_node->source();
        if (end.empty()) end = end_node->value();
        if (!end.ptr || end.ptr < start.ptr) return;
        node->set_source(StringRef{start.ptr,
            static_cast<uint32_t>(end.ptr + end.len - start.ptr)});
    }

    AstNode* parse_identifier_or_function(const Token& name_token) {
        if constexpr (D == Dialect::PostgreSQL) {
            if (keyword(name_token, "CAST") && tok_.peek().type == TokenType::TK_LPAREN) {
                tok_.skip();
                AstNode* value = parse_complete();
                if (!value || tok_.peek().type != TokenType::TK_AS) return syntax_error();
                tok_.skip();
                StringRef type = PgTypeParser(tok_).parse();
                if (type.empty() || tok_.peek().type != TokenType::TK_RPAREN) return syntax_error();
                tok_.skip();
                return make_cast(value, type);
            }
        }
        // Check for function call: name(
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();  // consume (
            AstNode* func = make_node(arena_, NodeType::NODE_FUNCTION_CALL, name_token.source.empty() ? name_token.text : name_token.source);
            // CAST uses `CAST(expr AS type)` rather than a comma-separated
            // argument list. Model it as a function call so consumers can
            // reject or handle the expression without leaving valid input
            // unconsumed.
            if (D == Dialect::MySQL && name_token.text.equals_ci("CAST", 4)) {
                AstNode* arg = parse();
                if (!arg || tok_.peek().type != TokenType::TK_AS) return func;
                func->add_child(arg);
                tok_.skip();
                Token type = tok_.next_token();
                if (type.type == TokenType::TK_EOF ||
                    type.type == TokenType::TK_RPAREN) {
                    return func;
                }
                func->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, type.text));
                if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                return func;
            }
            // Parse argument list
            if (tok_.peek().type != TokenType::TK_RPAREN) {
                while (true) {
                    AstNode* arg = parse_argument();
                    if constexpr (D == Dialect::PostgreSQL) {
                        if (!arg) return syntax_error();
                    }
                    if (arg) func->add_child(arg);
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
            }
            if (tok_.peek().type == TokenType::TK_RPAREN) {
                tok_.skip();
            }
            if constexpr (D == Dialect::PostgreSQL) {
                if (keyword(tok_.peek(), "FILTER")) {
                    tok_.skip();
                    if (tok_.peek().type != TokenType::TK_LPAREN) return syntax_error();
                    tok_.skip();
                    if (tok_.peek().type != TokenType::TK_WHERE) return syntax_error();
                    tok_.skip();
                    AstNode* predicate = parse_complete();
                    if (!predicate || tok_.peek().type != TokenType::TK_RPAREN)
                        return syntax_error();
                    tok_.skip();
                    AstNode* filter = make_node(arena_, NodeType::NODE_AGGREGATE_FILTER);
                    filter->add_child(func);
                    filter->add_child(predicate);
                    func = filter;
                }
            }
            // Check for OVER clause (window function)
            if (tok_.peek().type == TokenType::TK_OVER) {
                return parse_window_function(func);
            }
            return func;
        }

        // Check for qualified name: table.column
        if (tok_.peek().type == TokenType::TK_DOT) {
            tok_.skip();  // consume dot
            Token col = tok_.next_token();
            AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            AstNode* schema_node = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, name_token);
            AstNode* col_node = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, col);
            if (schema_node && token_was_delimited_(name_token))
                schema_node->flags |= FLAG_IDENT_DELIMITED;
            if (col_node && token_was_delimited_(col))
                col_node->flags |= FLAG_IDENT_DELIMITED;
            qname->add_child(schema_node);
            qname->add_child(col_node);
            return qname;
        }

        AstNode* col_ref = make_node_from_token(arena_, NodeType::NODE_COLUMN_REF, name_token);
        if (col_ref && token_was_delimited_(name_token))
            col_ref->flags |= FLAG_IDENT_DELIMITED;
        return col_ref;
    }

    // True iff `t` is a TK_IDENTIFIER token whose source bytes were
    // surrounded by backticks (MySQL) or double quotes (PostgreSQL).
    // Uses lookback against the tokenizer's input buffer so the check is
    // safe even when t.text starts at the very beginning of input.
    bool token_was_delimited_(const Token& t) const {
        if (t.type != TokenType::TK_IDENTIFIER) return false;
        if (!t.text.ptr || t.text.ptr <= tok_.input_begin()) return false;
        char prev = *(t.text.ptr - 1);
        return prev == '`' || prev == '"';
    }

    // Infix precedence for a token type.
    // Returns NONE if not an infix operator (stops the Pratt loop).
    static Precedence infix_precedence(TokenType type) {
        if constexpr (D == Dialect::PostgreSQL) {
            switch (type) {
                case TokenType::TK_PG_OPERATOR: return Precedence::PG_OPERATOR;
                case TokenType::TK_CARET: return Precedence::EXPONENT;
                case TokenType::TK_COLLATE: return Precedence::COLLATION;
                case TokenType::TK_IS: return Precedence::PG_IS;
                case TokenType::TK_EQUAL: case TokenType::TK_NOT_EQUAL:
                case TokenType::TK_LESS: case TokenType::TK_GREATER:
                case TokenType::TK_LESS_EQUAL: case TokenType::TK_GREATER_EQUAL:
                    return Precedence::PG_COMPARISON;
                case TokenType::TK_IN: case TokenType::TK_BETWEEN:
                case TokenType::TK_LIKE: case TokenType::TK_NOT:
                    return Precedence::PG_PREDICATE;
                default: break;
            }
        }
        switch (type) {
            case TokenType::TK_OR:             return Precedence::OR;
            case TokenType::TK_XOR:            return Precedence::XOR;
            case TokenType::TK_AND:            return Precedence::AND;
            case TokenType::TK_NOT:            return Precedence::COMPARISON; // NOT IN/BETWEEN/LIKE
            case TokenType::TK_EQUAL:
            case TokenType::TK_NOT_EQUAL:
            case TokenType::TK_LESS:
            case TokenType::TK_GREATER:
            case TokenType::TK_LESS_EQUAL:
            case TokenType::TK_GREATER_EQUAL:
            case TokenType::TK_REGEXP:
            case TokenType::TK_SOUNDS:
            case TokenType::TK_MEMBER:
            case TokenType::TK_LIKE:           return Precedence::COMPARISON;
            case TokenType::TK_IS:             return Precedence::COMPARISON;
            case TokenType::TK_IN:             return Precedence::COMPARISON;
            case TokenType::TK_BETWEEN:        return Precedence::COMPARISON;
            case TokenType::TK_PIPE:           return Precedence::BIT_OR;
            case TokenType::TK_CARET:          return Precedence::BIT_XOR;
            case TokenType::TK_AMPERSAND:      return Precedence::BIT_AND;
            case TokenType::TK_SHIFT_LEFT:
            case TokenType::TK_SHIFT_RIGHT:    return Precedence::SHIFT;
            case TokenType::TK_PLUS:
            case TokenType::TK_MINUS:          return Precedence::ADDITION;
            case TokenType::TK_ASTERISK:
            case TokenType::TK_SLASH:
            case TokenType::TK_PERCENT:
            case TokenType::TK_DIV:
            case TokenType::TK_MOD:            return Precedence::MULTIPLICATION;
            case TokenType::TK_DOUBLE_PIPE:    return Precedence::ADDITION; // string concat
            default:                           return Precedence::NONE;
        }
    }

    AstNode* parse_infix(AstNode* left, Precedence prec) {
        Token op = tok_.next_token();

        switch (op.type) {
            case TokenType::TK_COLLATE: {
                if constexpr (D == Dialect::PostgreSQL) {
                    Token name = tok_.peek();
                    if (name.type != TokenType::TK_IDENTIFIER) return syntax_error();
                    tok_.skip();
                    StringRef span = name.source;
                    while (tok_.peek().type == TokenType::TK_DOT) {
                        tok_.skip();
                        Token field = tok_.peek();
                        if (field.type != TokenType::TK_IDENTIFIER) return syntax_error();
                        tok_.skip();
                        span.len = static_cast<uint32_t>(field.source.ptr + field.source.len - span.ptr);
                    }
                    AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP, StringRef{"COLLATE", 7}, FLAG_PG_OPERATOR);
                    node->add_child(left);
                    node->add_child(make_node(arena_, NodeType::NODE_TYPE_NAME, span));
                    return node;
                }
                return nullptr;
            }
            case TokenType::TK_NOT: {
                // NOT IN / NOT BETWEEN / NOT LIKE / NOT REGEXP — compound negated infix
                Token actual_op = tok_.peek();
                if (actual_op.type == TokenType::TK_IN) {
                    tok_.skip();
                    AstNode* in_node = parse_in(left);
                    // Wrap in NOT
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(in_node);
                    return not_node;
                }
                if (actual_op.type == TokenType::TK_BETWEEN) {
                    tok_.skip();
                    AstNode* between_node = parse_between(left);
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(between_node);
                    return not_node;
                }
                if (actual_op.type == TokenType::TK_LIKE ||
                    actual_op.type == TokenType::TK_REGEXP) {
                    tok_.skip();
                    AstNode* right = parse(prec);
                    AstNode* like_node = make_node(arena_, NodeType::NODE_BINARY_OP, actual_op.text);
                    like_node->add_child(left);
                    if (right) like_node->add_child(right);
                    AstNode* not_node = make_node(arena_, NodeType::NODE_UNARY_OP, op.text);
                    not_node->add_child(like_node);
                    return not_node;
                }
                // Standalone NOT is incomplete in an operand-checked context.
                return require_complete_operands_ ? nullptr : left;
            }
            case TokenType::TK_IS: {
                // IS [NOT] NULL
                bool is_not = false;
                if (tok_.peek().type == TokenType::TK_NOT) {
                    is_not = true;
                    tok_.skip();
                }
                if (tok_.peek().type == TokenType::TK_NULL) {
                    tok_.skip();
                    NodeType nt = is_not ? NodeType::NODE_IS_NOT_NULL : NodeType::NODE_IS_NULL;
                    AstNode* node = make_node(arena_, nt);
                    node->add_child(left);
                    return node;
                }
                // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE
                if (tok_.peek().type == TokenType::TK_TRUE || tok_.peek().type == TokenType::TK_FALSE) {
                    Token val = tok_.next_token();
                    AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP,
                        is_not ? StringRef{"IS NOT", 6} : StringRef{"IS", 2});
                    node->add_child(left);
                    node->add_child(make_node(arena_, NodeType::NODE_LITERAL_INT, val.text));
                    return node;
                }
                return require_complete_operands_ ? nullptr : left;
            }
            case TokenType::TK_IN:
                return parse_in(left);
            case TokenType::TK_BETWEEN:
                return parse_between(left);
            case TokenType::TK_SOUNDS:
                return parse_sounds_like(left, op, prec);
            case TokenType::TK_MEMBER:
                return parse_member_of(left, op, prec);
            default: {
                // Standard binary operator
                const bool pg_operator = D == Dialect::PostgreSQL &&
                    (op.type == TokenType::TK_PG_OPERATOR || op.type == TokenType::TK_CARET);
                AstNode* right = pg_operator ? parse_complete(prec) : parse(prec);
                if (!right) return pg_operator ? syntax_error() : (require_complete_operands_ ? nullptr : left);
                AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP, op.text,
                    pg_operator ? FLAG_PG_OPERATOR : 0);
                node->add_child(left);
                node->add_child(right);
                return node;
            }
        }
    }

    // IN (value_list) or IN (subquery)
    AstNode* parse_in(AstNode* left) {
        AstNode* node = make_node(arena_, NodeType::NODE_IN_LIST);
        node->add_child(left);
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SELECT) {
                AstNode* sq = parse_subquery_inner();
                node->add_child(sq);
            } else {
                // Expression roots are single, unattached nodes. Keep the tail
                // locally so appending N IN-list values takes O(N), not O(N^2).
                AstNode* tail = left;
                while (true) {
                    AstNode* val = parse();
                    if (val) {
                        tail->next_sibling = val;
                        tail = val;
                    }
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
                if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
            }
        }
        return node;
    }

    AstNode* parse_sounds_like(AstNode* left, const Token& sounds, Precedence prec) {
        Token like = tok_.peek();
        StringRef op_text = sounds.text;
        if (like.type == TokenType::TK_LIKE) {
            tok_.skip();
            op_text = StringRef{sounds.text.ptr,
                static_cast<uint32_t>((like.text.ptr + like.text.len) - sounds.text.ptr)};
        }
        AstNode* right = parse(prec);
        AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP, op_text);
        node->add_child(left);
        if (right) node->add_child(right);
        return node;
    }

    AstNode* parse_member_of(AstNode* left, const Token& member, Precedence prec) {
        Token of = tok_.peek();
        StringRef op_text = member.text;
        if (of.type == TokenType::TK_OF) {
            tok_.skip();
            op_text = StringRef{member.text.ptr,
                static_cast<uint32_t>((of.text.ptr + of.text.len) - member.text.ptr)};
        }

        AstNode* right = nullptr;
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();
            AstNode* tuple = make_node(arena_, NodeType::NODE_TUPLE);
            if (tok_.peek().type != TokenType::TK_RPAREN) {
                while (true) {
                    AstNode* elem = parse();
                    if (elem) tuple->add_child(elem);
                    if (tok_.peek().type == TokenType::TK_COMMA) tok_.skip();
                    else break;
                }
            }
            if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
            right = tuple;
        } else {
            right = parse(prec);
        }

        AstNode* node = make_node(arena_, NodeType::NODE_BINARY_OP, op_text);
        node->add_child(left);
        if (right) node->add_child(right);
        return node;
    }

    // BETWEEN low AND high
    AstNode* parse_between(AstNode* left) {
        AstNode* node = make_node(arena_, NodeType::NODE_BETWEEN);
        node->add_child(left);
        AstNode* low = parse(Precedence::COMPARISON);
        node->add_child(low);
        if (tok_.peek().type == TokenType::TK_AND) {
            tok_.skip();
        }
        AstNode* high = parse(Precedence::COMPARISON);
        node->add_child(high);
        return node;
    }

    AstNode* parse_interval_literal(const Token& interval) {
        tok_.skip();
        Token amount = tok_.next_token();
        if (amount.type == TokenType::TK_EOF || amount.type == TokenType::TK_ERROR) {
            return make_node(arena_, NodeType::NODE_IDENTIFIER, interval.text);
        }
        Token unit = tok_.next_token();
        if (unit.type == TokenType::TK_EOF || unit.type == TokenType::TK_ERROR) {
            StringRef span{interval.text.ptr,
                static_cast<uint32_t>((amount.text.ptr + amount.text.len) - interval.text.ptr)};
            return make_node(arena_, NodeType::NODE_IDENTIFIER, span);
        }
        StringRef span{interval.text.ptr,
            static_cast<uint32_t>((unit.text.ptr + unit.text.len) - interval.text.ptr)};
        return make_node(arena_, NodeType::NODE_IDENTIFIER, span);
    }

    AstNode* parse_quantified_subquery(const Token& quantifier) {
        tok_.skip();
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SELECT) {
                const char* close = skip_to_matching_paren();
                const char* end = close ? close + 1 : tok_.input_end();
                return make_node(arena_, NodeType::NODE_IDENTIFIER,
                    StringRef{quantifier.text.ptr,
                        static_cast<uint32_t>(end - quantifier.text.ptr)});
            }
        }
        return make_node(arena_, NodeType::NODE_IDENTIFIER, quantifier.text);
    }

    // CASE [expr] WHEN ... THEN ... [ELSE ...] END
    AstNode* parse_case() {
        AstNode* node = make_node(arena_, NodeType::NODE_CASE_WHEN);
        // Optional simple CASE expression: CASE expr WHEN ...
        if (tok_.peek().type != TokenType::TK_WHEN) {
            node->flags = 1;  // simple CASE (has case_expr)
            AstNode* case_expr = parse();
            if (case_expr) node->add_child(case_expr);
        }
        // WHEN ... THEN ... pairs
        while (tok_.peek().type == TokenType::TK_WHEN) {
            tok_.skip();
            AstNode* when_expr = parse();
            if (when_expr) node->add_child(when_expr);
            if (tok_.peek().type == TokenType::TK_THEN) tok_.skip();
            AstNode* then_expr = parse();
            if (then_expr) node->add_child(then_expr);
        }
        // Optional ELSE
        if (tok_.peek().type == TokenType::TK_ELSE) {
            tok_.skip();
            AstNode* else_expr = parse();
            if (else_expr) node->add_child(else_expr);
        }
        // END
        if (tok_.peek().type == TokenType::TK_END) tok_.skip();
        return node;
    }

    // ARRAY[val, val, ...] constructor
    AstNode* parse_array_constructor() {
        AstNode* arr = make_node(arena_, NodeType::NODE_ARRAY_CONSTRUCTOR);
        if (tok_.peek().type == TokenType::TK_LBRACKET) {
            tok_.skip();
            if (tok_.peek().type != TokenType::TK_RBRACKET) {
                while (true) {
                    AstNode* elem = parse();
                    if (elem) arr->add_child(elem);
                    if (tok_.peek().type == TokenType::TK_COMMA) tok_.skip();
                    else break;
                }
            }
            if (tok_.peek().type == TokenType::TK_RBRACKET) tok_.skip();
        }
        return parse_postfix(arr);
    }

    AstNode* make_cast(AstNode* value, StringRef type) {
        AstNode* node = make_node(arena_, NodeType::NODE_TYPE_CAST);
        AstNode* name = make_node(arena_, NodeType::NODE_TYPE_NAME, type);
        if (!node || !name) return syntax_error();
        node->add_child(value);
        node->add_child(name);
        return node;
    }

    // Handle postfix operators: ::type, .field, [index]
    AstNode* parse_postfix(AstNode* expr) {
        while (true) {
            Token t = tok_.peek();
            if constexpr (D == Dialect::PostgreSQL) {
                if (t.type == TokenType::TK_DOUBLE_COLON) {
                    tok_.skip();
                    StringRef type = PgTypeParser(tok_).parse();
                    if (type.empty()) return syntax_error();
                    expr = make_cast(expr, type);
                    if (!expr) return nullptr;
                    continue;
                }
            }
            if (t.type == TokenType::TK_DOT) {
                // Field access: (expr).field or (expr).*
                tok_.skip();
                Token field = tok_.next_token();
                AstNode* access = make_node(arena_, NodeType::NODE_FIELD_ACCESS);
                access->add_child(expr);
                AstNode* field_node = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, field);
                if (field_node && token_was_delimited_(field)) field_node->flags |= FLAG_IDENT_DELIMITED;
                access->add_child(field_node);
                expr = access;
            } else if (t.type == TokenType::TK_LBRACKET) {
                // Array subscript: expr[index]
                tok_.skip();
                AstNode* index = parse();
                if (tok_.peek().type == TokenType::TK_RBRACKET) tok_.skip();
                AstNode* subscript = make_node(arena_, NodeType::NODE_ARRAY_SUBSCRIPT);
                subscript->add_child(expr);
                if (index) subscript->add_child(index);
                expr = subscript;
            } else {
                break;
            }
        }
        return expr;
    }

    AstNode* parse_window_function(AstNode* func) {
        tok_.skip(); // OVER
        AstNode* win = make_node(arena_, NodeType::NODE_WINDOW_FUNCTION);
        win->add_child(func);
        if constexpr (D == Dialect::PostgreSQL) {
            if (tok_.peek().type == TokenType::TK_IDENTIFIER) {
                Token name = tok_.next_token();
                win->add_child(make_node(arena_, NodeType::NODE_WINDOW_REFERENCE,
                    name.source.empty() ? name.text : name.source));
                return win;
            }
        }
        AstNode* spec = parse_window_spec();
        if (!spec) return syntax_error();
        win->add_child(spec);
        return win;
    }

public:
    // Shared by OVER (...) and WINDOW name AS (...).
    AstNode* parse_window_spec() {
        AstNode* spec = make_node(arena_, NodeType::NODE_WINDOW_SPEC);
        if (tok_.peek().type != TokenType::TK_LPAREN) return syntax_error();
        tok_.skip();
        if constexpr (D == Dialect::PostgreSQL) {
            Token name = tok_.peek();
            if (name.type == TokenType::TK_IDENTIFIER &&
                !keyword(name, "ROWS") && !keyword(name, "RANGE") && !keyword(name, "GROUPS")) {
                tok_.skip();
                spec->add_child(make_node(arena_, NodeType::NODE_WINDOW_REFERENCE,
                    name.source.empty() ? name.text : name.source));
            }
        }
        if (tok_.peek().type == TokenType::TK_PARTITION) {
            tok_.skip();
            if (tok_.peek().type != TokenType::TK_BY) return syntax_error();
            tok_.skip();
            AstNode* part = make_node(arena_, NodeType::NODE_WINDOW_PARTITION);
            while (true) {
                AstNode* expr = parse_complete();
                if (!expr) return syntax_error();
                part->add_child(expr);
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            }
            spec->add_child(part);
        }
        if (tok_.peek().type == TokenType::TK_ORDER) {
            tok_.skip();
            if (tok_.peek().type != TokenType::TK_BY) return syntax_error();
            tok_.skip();
            AstNode* ord = make_node(arena_, NodeType::NODE_WINDOW_ORDER);
            while (true) {
                AstNode* expr = parse_complete();
                if (!expr) return syntax_error();
                AstNode* item = make_node(arena_, NodeType::NODE_ORDER_BY_ITEM);
                item->add_child(expr);
                Token dir = tok_.peek();
                if (dir.type == TokenType::TK_ASC || dir.type == TokenType::TK_DESC) {
                    tok_.skip();
                    item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, dir.text));
                }
                if (keyword(tok_.peek(), "NULLS")) {
                    tok_.skip();
                    item->flags |= FLAG_ORDER_NULLS;
                    Token placement = tok_.peek();
                    if (!keyword(placement, "FIRST") && !keyword(placement, "LAST"))
                        return syntax_error();
                    tok_.skip();
                    item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                        keyword(placement, "FIRST") ? StringRef{"NULLS FIRST", 11} : StringRef{"NULLS LAST", 10}));
                }
                ord->add_child(item);
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            }
            spec->add_child(ord);
        }
        if constexpr (D == Dialect::PostgreSQL) {
            if (keyword(tok_.peek(), "ROWS") || keyword(tok_.peek(), "RANGE") ||
                keyword(tok_.peek(), "GROUPS")) {
                AstNode* frame = parse_window_frame();
                if (!frame) return nullptr;
                spec->add_child(frame);
            }
        }
        if (tok_.peek().type != TokenType::TK_RPAREN) return syntax_error();
        tok_.skip();
        return spec;
    }

private:
    AstNode* parse_window_bound(int& rank) {
        AstNode* bound = make_node(arena_, NodeType::NODE_WINDOW_BOUND);
        if (keyword(tok_.peek(), "CURRENT")) {
            tok_.skip();
            if (!keyword(tok_.peek(), "ROW")) return syntax_error();
            tok_.skip();
            bound->set_value(StringRef{"CURRENT ROW", 11});
            rank = 2;
            return bound;
        }
        bool unbounded = keyword(tok_.peek(), "UNBOUNDED");
        if (unbounded) tok_.skip();
        else {
            AstNode* offset = parse_complete(Precedence::COMPARISON);
            if (!offset) return syntax_error();
            bound->add_child(offset);
        }
        Token direction = tok_.peek();
        if (!keyword(direction, "PRECEDING") && !keyword(direction, "FOLLOWING"))
            return syntax_error();
        tok_.skip();
        bool preceding = keyword(direction, "PRECEDING");
        rank = preceding ? (unbounded ? 0 : 1) : (unbounded ? 4 : 3);
        bound->set_value(unbounded ?
            (preceding ? StringRef{"UNBOUNDED PRECEDING", 19} : StringRef{"UNBOUNDED FOLLOWING", 19}) :
            (preceding ? StringRef{"PRECEDING", 9} : StringRef{"FOLLOWING", 9}));
        return bound;
    }

    AstNode* parse_window_frame() {
        Token unit = tok_.next_token();
        AstNode* frame = make_node(arena_, NodeType::NODE_WINDOW_FRAME, unit.text);
        bool between = tok_.peek().type == TokenType::TK_BETWEEN;
        if (between) { tok_.skip(); frame->flags = FLAG_WINDOW_BETWEEN; }
        int start_rank = 0, end_rank = 2;
        AstNode* start = parse_window_bound(start_rank);
        if (!start || start_rank == 4) return syntax_error();
        frame->add_child(start);
        if (between) {
            if (tok_.peek().type != TokenType::TK_AND) return syntax_error();
            tok_.skip();
            AstNode* end = parse_window_bound(end_rank);
            if (!end || end_rank == 0) return syntax_error();
            frame->add_child(end);
        }
        if (start_rank > end_rank) return syntax_error();
        if (keyword(tok_.peek(), "EXCLUDE")) {
            tok_.skip();
            Token exclusion = tok_.next_token();
            StringRef value;
            if (keyword(exclusion, "CURRENT")) {
                if (!keyword(tok_.peek(), "ROW")) return syntax_error();
                tok_.skip(); value = StringRef{"CURRENT ROW", 11};
            } else if (keyword(exclusion, "NO")) {
                if (!keyword(tok_.peek(), "OTHERS")) return syntax_error();
                tok_.skip(); value = StringRef{"NO OTHERS", 9};
            } else if (keyword(exclusion, "GROUP") || keyword(exclusion, "TIES")) {
                value = exclusion.text;
            } else return syntax_error();
            frame->add_child(make_node(arena_, NodeType::NODE_WINDOW_EXCLUSION, value));
        }
        return frame;
    }

    // Skip tokens until matching closing paren (handles nesting)
    const char* skip_to_matching_paren() {
        int depth = 1;
        while (depth > 0) {
            Token t = tok_.next_token();
            if (t.type == TokenType::TK_LPAREN) ++depth;
            else if (t.type == TokenType::TK_RPAREN) {
                --depth;
                if (depth == 0) return t.text.ptr;
            }
            else if (t.type == TokenType::TK_EOF) break;
        }
        return tok_.input_end();
    }

    // Some keywords can appear as identifiers in expression context
    static bool is_keyword_as_identifier(TokenType type) {
        switch (type) {
            // Keywords commonly used as column/table names
            case TokenType::TK_COUNT:
            case TokenType::TK_SUM:
            case TokenType::TK_AVG:
            case TokenType::TK_MIN:
            case TokenType::TK_MAX:
            case TokenType::TK_IF:
            case TokenType::TK_VALUES:
            case TokenType::TK_DATABASE:
            case TokenType::TK_SCHEMA:
            case TokenType::TK_TABLE:
            case TokenType::TK_INDEX:
            case TokenType::TK_VIEW:
            case TokenType::TK_NAMES:
            case TokenType::TK_CHARACTER:
            case TokenType::TK_CHARSET:
            case TokenType::TK_GLOBAL:
            case TokenType::TK_SESSION:
            case TokenType::TK_LOCAL:
            case TokenType::TK_LEVEL:
            case TokenType::TK_READ:
            case TokenType::TK_WRITE:
            case TokenType::TK_ONLY:
            case TokenType::TK_TRANSACTION:
            case TokenType::TK_ISOLATION:
            case TokenType::TK_COMMITTED:
            case TokenType::TK_UNCOMMITTED:
            case TokenType::TK_REPEATABLE:
            case TokenType::TK_SERIALIZABLE:
            case TokenType::TK_SHARE:
            case TokenType::TK_DATA:
            case TokenType::TK_RESET:
            case TokenType::TK_KEY:
            case TokenType::TK_ON:
            case TokenType::TK_DO:
            case TokenType::TK_NOTHING:
            case TokenType::TK_CONFLICT:
            case TokenType::TK_CONSTRAINT:
            case TokenType::TK_RETURNING:
            case TokenType::TK_DUPLICATE:
            case TokenType::TK_DELAYED:
            case TokenType::TK_HIGH_PRIORITY:
            case TokenType::TK_EXPLAIN:
            case TokenType::TK_DESCRIBE:
            case TokenType::TK_CALL:
            case TokenType::TK_PROCEDURE:
            case TokenType::TK_FORMAT:
            case TokenType::TK_ANALYZE:
            case TokenType::TK_VERBOSE:
            case TokenType::TK_COSTS:
            case TokenType::TK_SETTINGS:
            case TokenType::TK_BUFFERS:
            case TokenType::TK_WAL:
            case TokenType::TK_TIMING:
            case TokenType::TK_SUMMARY:
            case TokenType::TK_INFILE:
            case TokenType::TK_LINES:
            case TokenType::TK_TERMINATED:
            case TokenType::TK_ENCLOSED:
            case TokenType::TK_ESCAPED:
            case TokenType::TK_OPTIONALLY:
            case TokenType::TK_CONCURRENT:
            case TokenType::TK_STARTING:
            case TokenType::TK_COLUMNS:
            case TokenType::TK_FIELDS:
            case TokenType::TK_ROWS:
            case TokenType::TK_ARRAY:
            case TokenType::TK_ROW:
            case TokenType::TK_ROW_NUMBER:
            case TokenType::TK_RANK:
            case TokenType::TK_DENSE_RANK:
            case TokenType::TK_LAG:
            case TokenType::TK_LEAD:
            case TokenType::TK_FIRST_VALUE:
            case TokenType::TK_LAST_VALUE:
            case TokenType::TK_PARTITION:
            case TokenType::TK_RECURSIVE:
            case TokenType::TK_REPLACE:
            case TokenType::TK_DIV:
            case TokenType::TK_MOD:
                return true;
            default:
                return false;
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_EXPRESSION_PARSER_H
