#ifndef SQL_PARSER_SET_PARSER_H
#define SQL_PARSER_SET_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"

#include <cstring>

namespace sql_parser {

template <Dialect D>
class SetParser {
public:
    SetParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena) {}

    void set_subquery_callback(SubqueryParseCallback<D> cb) {
        expr_parser_.set_subquery_callback(cb);
    }

    // Parse a SET statement (SET keyword already consumed by classifier).
    // Returns the root NODE_SET_STMT node, or nullptr on failure.
    AstNode* parse() {
        reject_statement_ = false;
        AstNode* root = parse_statement_();
        return reject_statement_ ? nullptr : root;
    }

private:
    AstNode* parse_statement_() {
        AstNode* root = make_node(arena_, NodeType::NODE_SET_STMT);
        if (!root) return nullptr;

        Token next = tok_.peek();

        // SET NAMES ...
        if (next.type == TokenType::TK_NAMES) {
            tok_.skip();
            AstNode* names_node = parse_set_names();
            if (names_node) root->add_child(names_node);
            while (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                AstNode* next_assign = parse_comma_item();
                if (next_assign) root->add_child(next_assign);
            }
            if (!root->first_child) return nullptr;
            return root;
        }

        // SET CHARACTER SET ... or SET CHARSET ...
        if (next.type == TokenType::TK_CHARACTER) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SET) {
                tok_.skip();
            }
            AstNode* charset_node = parse_set_charset();
            if (charset_node) root->add_child(charset_node);
            while (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                AstNode* next_assign = parse_comma_item();
                if (next_assign) root->add_child(next_assign);
            }
            if (!root->first_child) return nullptr;
            return root;
        }
        if (next.type == TokenType::TK_CHARSET) {
            tok_.skip();
            AstNode* charset_node = parse_set_charset();
            if (charset_node) root->add_child(charset_node);
            while (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                AstNode* next_assign = parse_comma_item();
                if (next_assign) root->add_child(next_assign);
            }
            if (!root->first_child) return nullptr;
            return root;
        }

        // SET [scope] TRANSACTION ... and PostgreSQL
        // SET [scope] SESSION CHARACTERISTICS AS TRANSACTION ...
        // These forms own the whole statement; see parse_set_transaction().
        if (next.type == TokenType::TK_TRANSACTION) {
            tok_.skip();
            return wrap_set_transaction_(root, parse_set_transaction(nullptr, false));
        }

        if (is_set_transaction_scope_(next)) {
            Token scope_tok = tok_.next_token();
            if (tok_.peek().type == TokenType::TK_TRANSACTION) {
                tok_.skip();
                return wrap_set_transaction_(root, parse_set_transaction(&scope_tok, false));
            }
            if constexpr (D == Dialect::PostgreSQL) {
                Token after = tok_.peek();
                // SET SESSION CHARACTERISTICS AS TRANSACTION ...: SESSION is
                // part of the phrase here, not a scope.
                if (next.type == TokenType::TK_SESSION &&
                    ExpressionParser<D>::keyword(after, "CHARACTERISTICS")) {
                    tok_.skip();
                    return wrap_set_transaction_(root, parse_session_characteristics_(nullptr));
                }
                // SET { LOCAL | SESSION } SESSION CHARACTERISTICS AS TRANSACTION ...
                // (GLOBAL is not a PostgreSQL keyword.)
                if (after.type == TokenType::TK_SESSION) {
                    tok_.skip();
                    if (!ExpressionParser<D>::keyword(tok_.peek(), "CHARACTERISTICS")) {
                        tok_.flag_error_at(tok_.peek().source);
                        return nullptr;
                    }
                    tok_.skip();
                    return wrap_set_transaction_(root,
                        parse_session_characteristics_(&scope_tok));
                }
                if (next.type == TokenType::TK_LOCAL) {
                    return parse_pg_local_(root, scope_tok);
                }
            }
            // PostgreSQL: SET SESSION AUTHORIZATION { 'name' | name | DEFAULT }
            // -- role-switching command, NOT a GUC assignment. Emit a
            // dedicated NODE_SET_SESSION_AUTHORIZATION node.
            if constexpr (D == Dialect::PostgreSQL) {
                if (next.type == TokenType::TK_SESSION) {
                    Token after = tok_.peek();
                    if (after.type == TokenType::TK_IDENTIFIER &&
                        after.text.equals_ci("AUTHORIZATION", 13)) {
                        tok_.skip();
                        AstNode* node = make_node(arena_,
                            NodeType::NODE_SET_SESSION_AUTHORIZATION);
                        if (!node) return nullptr;
                        AstNode* val = parse_session_role_value_();
                        if (!val) return nullptr;
                        node->add_child(val);
                        root->add_child(node);
                        return root;
                    }
                }
            }
            // Not TRANSACTION -- it's SET <scope> var = expr
            AstNode* assignment = parse_variable_assignment(&scope_tok);
            if (assignment) root->add_child(assignment);
            if constexpr (D == Dialect::PostgreSQL) {
                if (!parse_pg_value_list_(assignment)) return nullptr;
            } else {
                // Parse remaining comma-separated assignments
                while (tok_.peek().type == TokenType::TK_COMMA) {
                    tok_.skip();
                    AstNode* next_assign = parse_comma_item();
                    if (next_assign) root->add_child(next_assign);
                }
            }
            if (!root->first_child) return nullptr;
            return root;
        }

        // PostgreSQL: SET ROLE { <name> | 'name' | NONE | DEFAULT }
        // This is a role-switching command, not a GUC assignment. Emit as a
        // distinct node so consumers can forward to the backend instead of
        // misclassifying it as an unknown variable named "ROLE".
        if constexpr (D == Dialect::PostgreSQL) {
            if (next.type == TokenType::TK_IDENTIFIER &&
                next.text.equals_ci("ROLE", 4)) {
                tok_.skip();
                AstNode* node = parse_pg_set_role_value_(/*local=*/false, Token{});
                if (node) root->add_child(node);
                if (!root->first_child) return nullptr;
                return root;
            }
        }

        // PostgreSQL: SET CONSTRAINTS { ALL | <name>[, <name>, ...] }
        //   { DEFERRED | IMMEDIATE }
        // Transaction-control command, not a GUC. The first child is the
        // target list (IDENTIFIER[ALL] or one-or-more IDENTIFIERs), the
        // last child is the mode (IDENTIFIER[DEFERRED|IMMEDIATE]).
        if constexpr (D == Dialect::PostgreSQL) {
            if (next.type == TokenType::TK_IDENTIFIER &&
                next.text.equals_ci("CONSTRAINTS", 11)) {
                tok_.skip();
                AstNode* node = make_node(arena_, NodeType::NODE_SET_CONSTRAINTS);
                if (!node) return nullptr;
                Token first = tok_.peek();
                if (first.type == TokenType::TK_ALL) {
                    tok_.skip();
                    node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                        StringRef{"ALL", 3}));
                } else {
                    // name[, name, ...] -- read identifiers separated by commas
                    while (true) {
                        Token name = tok_.peek();
                        if (name.type != TokenType::TK_IDENTIFIER) break;
                        tok_.skip();
                        node->add_child(make_identifier_(name));
                        if (tok_.peek().type != TokenType::TK_COMMA) break;
                        tok_.skip();
                    }
                    if (!node->first_child) return nullptr;
                }
                Token mode = tok_.next_token();
                if (mode.type != TokenType::TK_IDENTIFIER ||
                    !(mode.text.equals_ci("DEFERRED", 8) ||
                      mode.text.equals_ci("IMMEDIATE", 9))) {
                    return nullptr;
                }
                node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                    mode.text.equals_ci("DEFERRED", 8) ? StringRef{"DEFERRED", 8}
                                                       : StringRef{"IMMEDIATE", 9}));
                root->add_child(node);
                return root;
            }
        }

        // PostgreSQL: SET SCHEMA 'name' / SET SCHEMA name
        // Per PG docs this is a shorthand for SET search_path TO name. Emit
        // the canonical SET search_path form so downstream consumers see a
        // tracked-variable assignment instead of a literal "SCHEMA" target.
        if constexpr (D == Dialect::PostgreSQL) {
            if (next.type == TokenType::TK_SCHEMA) {
                tok_.skip();
                AstNode* assignment = make_node(arena_, NodeType::NODE_VAR_ASSIGNMENT);
                AstNode* target = make_node(arena_, NodeType::NODE_VAR_TARGET);
                if (!assignment || !target) return nullptr;
                target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                    StringRef{"search_path", 11}));
                assignment->add_child(target);
                AstNode* rhs = expr_parser_.parse();
                if (!rhs) return nullptr;
                assignment->add_child(rhs);
                root->add_child(assignment);
                return root;
            }
        }

        // PostgreSQL: SET SEED N
        // Per PG docs this is a shorthand for SELECT setseed(N) -- it does
        // not configure a GUC. Emit a regular VAR_ASSIGNMENT with the
        // synthetic name "seed"; downstream consumers can decide to ignore
        // it (no tracked state) or forward it to the backend verbatim.
        // The previous behaviour parsed it as `VAR_TARGET[SEED]` which
        // looks like a tracked-variable name and confused consumers.
        if constexpr (D == Dialect::PostgreSQL) {
            if (next.type == TokenType::TK_IDENTIFIER && next.text.equals_ci("SEED", 4)) {
                tok_.skip();
                AstNode* assignment = make_node(arena_, NodeType::NODE_VAR_ASSIGNMENT);
                AstNode* target = make_node(arena_, NodeType::NODE_VAR_TARGET);
                if (!assignment || !target) return nullptr;
                target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                    StringRef{"seed", 4}));
                assignment->add_child(target);
                AstNode* rhs = expr_parser_.parse();
                if (!rhs) return nullptr;
                assignment->add_child(rhs);
                root->add_child(assignment);
                return root;
            }
        }

        // PostgreSQL: SET TIME ZONE <value>
        // Per the PG docs this is an alias for SET TimeZone = <value>. The
        // tokenizer has no dedicated TK_TIME / TK_ZONE keywords so the lookahead
        // matches identifier text case-insensitively. <value> can be a string
        // literal, DEFAULT, LOCAL, an interval expression, or any other
        // expression accepted by ExpressionParser.
        if constexpr (D == Dialect::PostgreSQL) {
            if (next.type == TokenType::TK_IDENTIFIER && next.text.equals_ci("TIME", 4)) {
                tok_.skip();
                Token zone = tok_.peek();
                if (zone.type == TokenType::TK_IDENTIFIER && zone.text.equals_ci("ZONE", 4)) {
                    tok_.skip();
                    AstNode* assignment = make_node(arena_, NodeType::NODE_VAR_ASSIGNMENT);
                    AstNode* target = make_node(arena_, NodeType::NODE_VAR_TARGET);
                    if (!assignment || !target) return nullptr;
                    // Synthetic variable name; string literal lives in static storage.
                    target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                        StringRef{"timezone", 8}));
                    assignment->add_child(target);
                    Token rhs_tok = tok_.peek();
                    if (rhs_tok.type == TokenType::TK_DEFAULT ||
                        rhs_tok.type == TokenType::TK_LOCAL) {
                        tok_.skip();
                        assignment->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, rhs_tok.text));
                    } else if (rhs_tok.type == TokenType::TK_IDENTIFIER &&
                               rhs_tok.text.equals_ci("INTERVAL", 8)) {
                        // SET TIME ZONE INTERVAL '<value>' <UNIT> -- the
                        // INTERVAL keyword + string + unit is a single
                        // PG interval literal. ExpressionParser doesn't
                        // model this so it would drop everything after
                        // INTERVAL. Capture the source bytes spanning
                        // the whole literal and emit a single
                        // LITERAL_STRING value so consumers can
                        // re-emit it intact.
                        Token start = tok_.next_token();  // INTERVAL
                        Token val = tok_.next_token();    // '1'
                        Token unit = tok_.next_token();   // HOUR
                        if (val.type != TokenType::TK_STRING ||
                            unit.type != TokenType::TK_IDENTIFIER) {
                            return nullptr;
                        }
                        const char* span_end = unit.text.ptr + unit.text.len;
                        StringRef whole{start.text.ptr,
                            static_cast<uint32_t>(span_end - start.text.ptr)};
                        assignment->add_child(make_node(arena_,
                            NodeType::NODE_LITERAL_STRING, whole));
                    } else {
                        AstNode* rhs = expr_parser_.parse();
                        if (!rhs) return nullptr;
                        assignment->add_child(rhs);
                    }
                    root->add_child(assignment);
                    return root;
                }
                // TIME without ZONE is not a valid PG SET form.
                return nullptr;
            }
        }

        // SET var = expr [, var = expr, ...]
        AstNode* assignment = parse_variable_assignment(nullptr);
        if (assignment) root->add_child(assignment);
        if constexpr (D == Dialect::PostgreSQL) {
            if (!parse_pg_value_list_(assignment)) return nullptr;
        } else {
            while (tok_.peek().type == TokenType::TK_COMMA) {
                Token comma = tok_.next_token();
                AstNode* next_assign = parse_comma_item();
                if (next_assign) {
                    root->add_child(next_assign);
                } else {
                    tok_.flag_error_at(comma.source);
                    break;
                }
            }
        }

        if (!root->first_child) return nullptr;
        return root;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    // Set when an item that must own the whole SET statement (SET
    // TRANSACTION) appears after another item; the statement is rejected.
    bool reject_statement_ = false;

    AstNode* wrap_set_transaction_(AstNode* root, AstNode* txn) {
        if (!txn) return nullptr;
        root->add_child(txn);
        return root;
    }

    // PostgreSQL: after SESSION CHARACTERISTICS, expect AS TRANSACTION.
    AstNode* parse_session_characteristics_(const Token* scope) {
        if (tok_.peek().type != TokenType::TK_AS) return fail_set_transaction_(tok_.peek());
        tok_.skip();
        if (tok_.peek().type != TokenType::TK_TRANSACTION) return fail_set_transaction_(tok_.peek());
        tok_.skip();
        return parse_set_transaction(scope, true);
    }

    // PG SET is single-variable; commas after the first value are list
    // continuation, not new assignments (see PG docs:
    //   SET configuration_parameter { TO | = } { value | 'value' | DEFAULT }
    //   "Some configuration parameters take a list of values, such as
    //    search_path and datestyle.")
    // Each extra value is appended as another child of the same
    // VAR_ASSIGNMENT node, alongside the first RHS expression.
    // Returns false when the statement must be rejected.
    bool parse_pg_value_list_(AstNode* assignment) {
        while (assignment && tok_.peek().type == TokenType::TK_COMMA) {
            tok_.skip();
            AstNode* extra_val = parse_setting_value();
            if (!extra_val) {
                if (tok_.has_error()) return false;
                break;
            }
            assignment->add_child(extra_val);
        }
        return true;
    }

    // PostgreSQL: SET LOCAL ... (LOCAL already consumed) -- the LOCAL scope
    // applies to whatever form follows: SET LOCAL ROLE / SET LOCAL <var> = ...
    // The LOCAL token is fed into the parsed node as scope info so
    // downstream consumers can preserve the LOCAL semantics.
    AstNode* parse_pg_local_(AstNode* root, const Token& scope_tok) {
        Token after = tok_.peek();
        if (after.type == TokenType::TK_IDENTIFIER &&
            after.text.equals_ci("ROLE", 4)) {
            tok_.skip();
            AstNode* node = parse_pg_set_role_value_(/*local=*/true, scope_tok);
            if (node) root->add_child(node);
            if (!root->first_child) return nullptr;
            return root;
        }
        AstNode* assignment = parse_variable_assignment(&scope_tok);
        if (assignment) root->add_child(assignment);
        if (!parse_pg_value_list_(assignment)) return nullptr;
        if (!root->first_child) return nullptr;
        return root;
    }

    static bool is_set_transaction_scope_(const Token& tok) {
        if constexpr (D == Dialect::PostgreSQL) {
            if (tok.type == TokenType::TK_LOCAL) return true;
        }
        return is_set_assignment_scope_(tok);
    }

    AstNode* parse_setting_value() {
        const Token start = tok_.peek();
        AstNode* value = expr_parser_.parse();
        if (!value) return nullptr;
        if constexpr (D == Dialect::PostgreSQL) {
            // PostgreSQL generic_set uses var_value, not a_expr. In particular,
            // custom operators must not turn malformed SET values into an OK
            // result that a proxy can mistake for a session-variable update.
            bool valid = value && !value->first_child &&
                (value->type == NodeType::NODE_IDENTIFIER ||
                 value->type == NodeType::NODE_COLUMN_REF ||
                 value->type == NodeType::NODE_LITERAL_STRING ||
                 value->type == NodeType::NODE_LITERAL_INT ||
                 value->type == NodeType::NODE_LITERAL_FLOAT);
            if (value && value->type == NodeType::NODE_UNARY_OP &&
                (value->value().equals_ci("+", 1) || value->value().equals_ci("-", 1))) {
                const AstNode* number = value->first_child;
                valid = number && !number->first_child && !number->next_sibling &&
                    (number->type == NodeType::NODE_LITERAL_INT ||
                     number->type == NodeType::NODE_LITERAL_FLOAT) &&
                    !number->value().equals_ci("true", 4) &&
                    !number->value().equals_ci("false", 5);
            }
            if (!valid) {
                tok_.flag_error_at(start.source);
                return nullptr;
            }
        }
        return value;
    }

    // Build "<prefix><name_content>" in the arena, dropping any
    // backtick/double-quote delimiters that surrounded `name` in source.
    // Used for assembling @@var / @var so the canonical AST name matches
    // both `@@var` and ``@@`var``` forms.
    StringRef build_scoped_identifier_(const char* prefix, const Token& name) {
        size_t prefix_len = std::strlen(prefix);
        size_t total = prefix_len + name.text.len;
        char* buf = static_cast<char*>(arena_.allocate(total));
        if (!buf) return StringRef{nullptr, 0};
        std::memcpy(buf, prefix, prefix_len);
        std::memcpy(buf + prefix_len, name.text.ptr, name.text.len);
        return StringRef{buf, static_cast<uint32_t>(total)};
    }

    // Same as build_scoped_identifier_ but for the dotted form
    // "<prefix><scope>.<name>" (e.g. @@session.sql_mode).
    StringRef build_scoped_dotted_identifier_(const char* prefix,
        const Token& scope, const Token& name) {
        size_t prefix_len = std::strlen(prefix);
        size_t total = prefix_len + scope.text.len + 1 + name.text.len;
        char* buf = static_cast<char*>(arena_.allocate(total));
        if (!buf) return StringRef{nullptr, 0};
        size_t off = 0;
        std::memcpy(buf + off, prefix, prefix_len); off += prefix_len;
        std::memcpy(buf + off, scope.text.ptr, scope.text.len); off += scope.text.len;
        buf[off++] = '.';
        std::memcpy(buf + off, name.text.ptr, name.text.len); off += name.text.len;
        return StringRef{buf, static_cast<uint32_t>(total)};
    }

    // Parse the value part of SET ROLE / SET LOCAL ROLE and return a
    // NODE_SET_ROLE node carrying:
    //   - optional first child IDENTIFIER[LOCAL] when invoked with local=true
    //   - value child: IDENTIFIER for NONE / DEFAULT / unquoted name, or
    //     LITERAL_STRING for the single-quoted form
    AstNode* parse_pg_set_role_value_(bool local, const Token& scope_tok) {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_ROLE);
        if (!node) return nullptr;
        if (local) {
            AstNode* scope = make_node(arena_, NodeType::NODE_IDENTIFIER,
                StringRef{"LOCAL", 5});
            if (!scope) return nullptr;
            scope->set_source(scope_tok.source);
            node->add_child(scope);
        }
        AstNode* val = parse_session_role_value_();
        if (!val) return nullptr;
        node->add_child(val);
        return node;
    }

    // Parse the right-hand-side of a SET ROLE / SET SESSION AUTHORIZATION:
    // accepts a quoted string literal, the keywords NONE / DEFAULT, or a
    // bare identifier role name. Returns nullptr on syntactic mismatch.
    AstNode* parse_session_role_value_() {
        Token tok = tok_.peek();
        if (tok.type == TokenType::TK_STRING) {
            tok_.skip();
            return make_node_from_token(arena_, NodeType::NODE_LITERAL_STRING, tok);
        }
        if (tok.type == TokenType::TK_DEFAULT) {
            tok_.skip();
            return make_node(arena_, NodeType::NODE_IDENTIFIER, StringRef{"DEFAULT", 7});
        }
        if (tok.type == TokenType::TK_IDENTIFIER) {
            tok_.skip();
            // NONE / DEFAULT / <name> are all just identifiers here.
            return make_identifier_(tok);
        }
        return nullptr;
    }

    AstNode* parse_comma_item() {
        Token peek = tok_.peek();
        // SET TRANSACTION cannot follow another SET item (the server
        // grammar rejects it); reject the whole statement, not just the item.
        if (peek.type == TokenType::TK_TRANSACTION) return reject_statement_at_(peek);
        if (is_set_assignment_scope_(peek)) {
            Token scope_tok = tok_.next_token();
            if (tok_.peek().type == TokenType::TK_TRANSACTION)
                return reject_statement_at_(tok_.peek());
            return parse_variable_assignment(&scope_tok);
        }
        if (peek.type == TokenType::TK_NAMES) {
            tok_.skip();
            return parse_set_names();
        }
        if (peek.type == TokenType::TK_CHARSET) {
            tok_.skip();
            return parse_set_charset();
        }
        if (peek.type == TokenType::TK_CHARACTER) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SET) {
                tok_.skip();
                return parse_set_charset();
            }
            return nullptr;
        }
        if (peek.type == TokenType::TK_COMMA || peek.type == TokenType::TK_SEMICOLON
            || peek.type == TokenType::TK_RPAREN || peek.type == TokenType::TK_EOF) {
            return nullptr;
        }
        return parse_variable_assignment(nullptr);
    }

    // SET NAMES charset [COLLATE collation]
    AstNode* parse_set_names() {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_NAMES);
        if (!node) return nullptr;

        // charset name or DEFAULT
        Token charset = tok_.next_token();
        node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, charset.text));

        // Optional COLLATE
        if (tok_.peek().type == TokenType::TK_COLLATE) {
            tok_.skip();
            Token collation = tok_.next_token();
            node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, collation.text));
        }
        return node;
    }

    // SET CHARACTER SET charset / SET CHARSET charset
    AstNode* parse_set_charset() {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_CHARSET);
        if (!node) return nullptr;

        Token charset = tok_.next_token();
        node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, charset.text));
        return node;
    }

    // SET [scope] TRANSACTION characteristic [, characteristic ...]
    // PostgreSQL also: SET [scope] SESSION CHARACTERISTICS AS TRANSACTION ...
    // and SET [scope] TRANSACTION SNAPSHOT 'id'.
    //
    // AST: NODE_SET_TRANSACTION (FLAG_SET_TRANSACTION_CHARACTERISTICS for the
    // PostgreSQL SESSION CHARACTERISTICS AS form), optional first child
    // NODE_IDENTIFIER scope with a canonical uppercase value, then one
    // NODE_TRANSACTION_OPTION per characteristic in input order with a
    // canonical value ("ISOLATION LEVEL READ COMMITTED", "READ ONLY",
    // "NOT DEFERRABLE", ...). SNAPSHOT carries its string literal as a child.
    // Values never reference the input text, so comments, case and
    // whitespace between keywords cannot leak into the AST. Returns nullptr
    // and flags an error for anything the server grammar rejects.
    AstNode* parse_set_transaction(const Token* scope, bool characteristics) {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_TRANSACTION, {},
            characteristics ? FLAG_SET_TRANSACTION_CHARACTERISTICS : 0);
        if (!node) return fail_set_transaction_(tok_.peek());

        if (scope) {
            AstNode* scope_node = make_node(arena_, NodeType::NODE_IDENTIFIER,
                canonical_set_scope_(*scope));
            if (!scope_node || scope_node->value_len == 0)
                return fail_set_transaction_(*scope);
            scope_node->set_source(scope->source);
            node->add_child(scope_node);
        }

        if constexpr (D == Dialect::PostgreSQL) {
            // TRANSACTION SNAPSHOT is a standalone form: no other modes, and
            // not valid after SESSION CHARACTERISTICS AS TRANSACTION.
            Token snap = tok_.peek();
            if (!characteristics && ExpressionParser<D>::keyword(snap, "SNAPSHOT")) {
                tok_.skip();
                Token id = tok_.peek();
                if (id.type != TokenType::TK_STRING) return fail_set_transaction_(id);
                tok_.skip();
                AstNode* option = make_option_(StringRef{"SNAPSHOT", 8}, snap, id);
                AstNode* literal = make_node_from_token(arena_,
                    NodeType::NODE_LITERAL_STRING, id);
                if (!option || !literal) return fail_set_transaction_(id);
                option->add_child(literal);
                node->add_child(option);
                return finish_set_transaction_(node);
            }
        }

        bool have_isolation = false;
        bool have_access = false;
        while (true) {
            Token first = tok_.peek();
            Token last = first;
            StringRef mode;
            auto take = [&](const char* word) {
                Token t = tok_.peek();
                if (!ExpressionParser<D>::keyword(t, word)) return false;
                tok_.skip();
                last = t;
                return true;
            };
            if (take("ISOLATION")) {
                if (!take("LEVEL")) return fail_set_transaction_(tok_.peek());
                if (take("SERIALIZABLE")) {
                    mode = {"ISOLATION LEVEL SERIALIZABLE", 28};
                } else if (take("REPEATABLE")) {
                    if (!take("READ")) return fail_set_transaction_(tok_.peek());
                    mode = {"ISOLATION LEVEL REPEATABLE READ", 31};
                } else if (take("READ")) {
                    if (take("COMMITTED")) mode = {"ISOLATION LEVEL READ COMMITTED", 30};
                    else if (take("UNCOMMITTED")) mode = {"ISOLATION LEVEL READ UNCOMMITTED", 32};
                    else return fail_set_transaction_(tok_.peek());
                } else {
                    return fail_set_transaction_(tok_.peek());
                }
                // MySQL allows each characteristic at most once.
                if (D == Dialect::MySQL && have_isolation) return fail_set_transaction_(first);
                have_isolation = true;
            } else if (take("READ")) {
                if (take("ONLY")) mode = {"READ ONLY", 9};
                else if (take("WRITE")) mode = {"READ WRITE", 10};
                else return fail_set_transaction_(tok_.peek());
                if (D == Dialect::MySQL && have_access) return fail_set_transaction_(first);
                have_access = true;
            } else if (D == Dialect::PostgreSQL && take("NOT")) {
                if (!take("DEFERRABLE")) return fail_set_transaction_(tok_.peek());
                mode = {"NOT DEFERRABLE", 14};
            } else if (D == Dialect::PostgreSQL && take("DEFERRABLE")) {
                mode = {"DEFERRABLE", 10};
            } else {
                // Empty list, dangling comma, or a non-characteristic item
                // (e.g. a variable assignment) after a comma.
                return fail_set_transaction_(first);
            }

            AstNode* option = make_option_(mode, first, last);
            if (!option) return fail_set_transaction_(first);
            node->add_child(option);

            Token sep = tok_.peek();
            if (sep.type == TokenType::TK_COMMA) {
                tok_.skip();
                continue;
            }
            // PostgreSQL also separates transaction modes with whitespace.
            if constexpr (D == Dialect::PostgreSQL) {
                if (ExpressionParser<D>::keyword(sep, "ISOLATION") ||
                    ExpressionParser<D>::keyword(sep, "READ") ||
                    ExpressionParser<D>::keyword(sep, "NOT") ||
                    ExpressionParser<D>::keyword(sep, "DEFERRABLE")) {
                    continue;
                }
            }
            break;
        }
        return finish_set_transaction_(node);
    }

    // A SET TRANSACTION statement cannot be combined with any other SET item:
    // only end of statement may follow the characteristic list.
    AstNode* finish_set_transaction_(AstNode* node) {
        Token after = tok_.peek();
        if (after.type != TokenType::TK_EOF && after.type != TokenType::TK_SEMICOLON)
            return fail_set_transaction_(after);
        return node;
    }

    AstNode* fail_set_transaction_(const Token& at) {
        tok_.flag_error_at(at.source.ptr ? at.source : at.text);
        return nullptr;
    }

    AstNode* make_option_(StringRef mode, const Token& first, const Token& last) {
        AstNode* option = make_node(arena_, NodeType::NODE_TRANSACTION_OPTION, mode);
        if (!option) return nullptr;
        const char* begin = first.source.ptr ? first.source.ptr : first.text.ptr;
        const char* end = last.source.ptr ? last.source.ptr + last.source.len
                                          : last.text.ptr + last.text.len;
        if (begin && end >= begin)
            option->set_source(StringRef{begin, static_cast<uint32_t>(end - begin)});
        return option;
    }

    static StringRef canonical_set_scope_(const Token& tok) {
        switch (tok.type) {
            case TokenType::TK_GLOBAL: return {"GLOBAL", 6};
            case TokenType::TK_SESSION: return {"SESSION", 7};
            case TokenType::TK_LOCAL: return {"LOCAL", 5};
            case TokenType::TK_PERSIST: return {"PERSIST", 7};
            default:
                if (tok.text.equals_ci("PERSIST_ONLY", 12)) return {"PERSIST_ONLY", 12};
                return {};
        }
    }

    // Identifier node that keeps PostgreSQL "quoted" spelling for emission.
    AstNode* make_identifier_(const Token& tok) {
        AstNode* n = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, tok);
        if (n && tok.source.len && tok.source.ptr[0] == '"') n->flags |= FLAG_IDENT_DELIMITED;
        return n;
    }

    AstNode* reject_statement_at_(const Token& at) {
        reject_statement_ = true;
        tok_.flag_error_at(at.source.ptr ? at.source : at.text);
        return nullptr;
    }

    // Parse a single variable assignment: [scope] target = expr
    // scope_token is non-null if a SET-assignment scope was already consumed.
    AstNode* parse_variable_assignment(const Token* scope_token) {
        AstNode* assignment = make_node(arena_, NodeType::NODE_VAR_ASSIGNMENT);
        if (!assignment) return nullptr;

        // Build the variable target
        AstNode* target = make_node(arena_, NodeType::NODE_VAR_TARGET);
        if (!target) return nullptr;

        if (scope_token) {
            target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, scope_token->text));
        }

        Token var = tok_.peek();
        bool user_variable_target = false;
        if (var.type == TokenType::TK_USER_VARIABLE) {
            user_variable_target = true;
            tok_.skip();
            AstNode* variable = make_mysql_user_variable_node(arena_, var);
            if (!variable) {
                tok_.flag_error_at(var.source);
                return nullptr;
            }
            target->add_child(variable);
        } else if (var.type == TokenType::TK_AT) {
            // User variable @name. The name may be backtick/double-quoted;
            // in that case the source bytes between `@` and the name include
            // the opening delimiter (and the closing delimiter sits one past
            // name.text.len), so a naive StringRef-from-source produces
            // `@name` -- missing the closing delimiter. Build the canonical
            // `@name` form in the arena instead, dropping the delimiters.
            tok_.skip();
            Token name = tok_.next_token();
            if (tok_.peek().type == TokenType::TK_DOT) {
                tok_.skip();
                Token actual_name = tok_.next_token();
                target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                    build_scoped_dotted_identifier_("@", name, actual_name)));
            } else {
                target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                    build_scoped_identifier_("@", name)));
            }
        } else if (var.type == TokenType::TK_DOUBLE_AT) {
            // System variable @@[scope.]name -- same delimiter handling
            // concern as the @name branch above. Allocate `@@name` (or
            // `@@scope.name`) in the arena to drop any backtick/double-quote
            // delimiters from the canonical form.
            tok_.skip();
            Token name = tok_.next_token();
            if (tok_.peek().type == TokenType::TK_DOT) {
                tok_.skip();
                Token actual_name = tok_.next_token();
                target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                    build_scoped_dotted_identifier_("@@", name, actual_name)));
            } else {
                target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                    build_scoped_identifier_("@@", name)));
            }
        } else {
            // Plain variable name. PostgreSQL also accepts schema-qualified
            // GUC names like `pg_catalog.search_path` or `myapp.setting`
            // (the standard way to set custom application parameters). Emit
            // the combined `<schema>.<name>` as one identifier so downstream
            // normalization sees a single canonical name.
            Token name = tok_.next_token();
            // Validate the name token actually looks like an identifier --
            // `SET = 1` / `SET ,` / `SET ;` would otherwise stuff `=` / `,`
            // / `;` into the IDENTIFIER node and confuse downstream code.
            // Empty / EOF input was already rejected by parse_set() before
            // we got here.
            if (name.type != TokenType::TK_IDENTIFIER) {
                tok_.flag_error();
                return nullptr;
            }
            if constexpr (D == Dialect::PostgreSQL) {
                if (tok_.peek().type == TokenType::TK_DOT) {
                    tok_.skip();
                    Token rhs = tok_.next_token();
                    if (rhs.type != TokenType::TK_IDENTIFIER) {
                        tok_.flag_error();
                        return nullptr;
                    }
                    target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                        build_scoped_dotted_identifier_("", name, rhs)));
                } else {
                    target->add_child(make_node(arena_,
                        NodeType::NODE_IDENTIFIER, name.text));
                }
            } else {
                target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
            }
        }

        assignment->add_child(target);

        // Expect = or := (MySQL) or TO (PostgreSQL)
        Token eq = tok_.peek();
        bool has_assignment_operator = false;
        if (eq.type == TokenType::TK_EQUAL || eq.type == TokenType::TK_COLON_EQUAL) {
            tok_.skip();
            has_assignment_operator = true;
        } else if constexpr (D == Dialect::PostgreSQL) {
            if (eq.type == TokenType::TK_TO) {
                tok_.skip();
                has_assignment_operator = true;
            }
        }
        if (user_variable_target && !has_assignment_operator) {
            tok_.flag_error_at(eq.source);
            return nullptr;
        }

        // Parse RHS expression. If the parser couldn't produce one --
        // typically because the input is truncated (`SET x =`), starts
        // with a separator (`SET x = ,foo`, `SET x = ;`), or otherwise
        // malformed -- flag a parse error so the eventual ParseResult is
        // ERROR rather than PARTIAL with a missing-RHS AST.
        AstNode* rhs = parse_setting_value();
        if (rhs) {
            assignment->add_child(rhs);
        } else {
            tok_.flag_error();
            return nullptr;
        }

        return assignment;
    }

    static bool is_set_assignment_scope_(const Token& tok) {
        if (tok.type == TokenType::TK_GLOBAL || tok.type == TokenType::TK_SESSION) {
            return true;
        }
        if constexpr (D == Dialect::MySQL) {
            return tok.type == TokenType::TK_LOCAL ||
                   tok.type == TokenType::TK_PERSIST ||
                   (tok.type == TokenType::TK_IDENTIFIER &&
                    tok.text.equals_ci("PERSIST_ONLY", 12));
        }
        return false;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_SET_PARSER_H
