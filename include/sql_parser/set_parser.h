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

    // Parse a SET statement (SET keyword already consumed by classifier).
    // Returns the root NODE_SET_STMT node, or nullptr on failure.
    AstNode* parse() {
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

        // SET [GLOBAL|SESSION] TRANSACTION ...
        // Need to check for scope + TRANSACTION or just TRANSACTION
        if (next.type == TokenType::TK_TRANSACTION) {
            tok_.skip();
            AstNode* txn_node = parse_set_transaction(StringRef{});
            if (txn_node) root->add_child(txn_node);
            if (!root->first_child) return nullptr;
            return root;
        }

        if (next.type == TokenType::TK_GLOBAL || next.type == TokenType::TK_SESSION) {
            Token scope_tok = tok_.next_token();
            if (tok_.peek().type == TokenType::TK_TRANSACTION) {
                tok_.skip();
                AstNode* txn_node = parse_set_transaction(scope_tok.text);
                if (txn_node) root->add_child(txn_node);
                if (!root->first_child) return nullptr;
                return root;
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
                    // SET SESSION CHARACTERISTICS AS TRANSACTION ...
                    // -- emit as NODE_SET_TRANSACTION with SESSION scope so
                    // consumers handle it uniformly with plain SET SESSION
                    // TRANSACTION.
                    if (after.type == TokenType::TK_IDENTIFIER &&
                        after.text.equals_ci("CHARACTERISTICS", 15)) {
                        tok_.skip();
                        if (tok_.peek().type == TokenType::TK_AS) {
                            tok_.skip();
                            if (tok_.peek().type == TokenType::TK_TRANSACTION) {
                                tok_.skip();
                                AstNode* txn_node =
                                    parse_set_transaction(scope_tok.text);
                                if (txn_node) root->add_child(txn_node);
                                if (!root->first_child) return nullptr;
                                return root;
                            }
                        }
                        return nullptr;
                    }
                }
            }
            // Not TRANSACTION — it's SET GLOBAL var = expr
            // Fall through to variable assignment with scope
            AstNode* assignment = parse_variable_assignment(&scope_tok);
            if (assignment) root->add_child(assignment);
            // Parse remaining comma-separated assignments
            while (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                AstNode* next_assign = parse_comma_item();
                if (next_assign) root->add_child(next_assign);
            }
            if (!root->first_child) return nullptr;
            return root;
        }

        // PostgreSQL: SET LOCAL ... -- the LOCAL scope applies to whatever
        // form follows. SET LOCAL ROLE / SET LOCAL <var> = ... etc. We peek
        // past LOCAL to dispatch correctly; the LOCAL token itself is fed
        // into the parsed node as scope info so downstream consumers can
        // preserve the LOCAL semantics.
        if constexpr (D == Dialect::PostgreSQL) {
            if (next.type == TokenType::TK_LOCAL) {
                Token scope_tok = tok_.next_token();
                Token after = tok_.peek();
                // SET LOCAL ROLE ...
                if (after.type == TokenType::TK_IDENTIFIER &&
                    after.text.equals_ci("ROLE", 4)) {
                    tok_.skip();
                    AstNode* node = parse_pg_set_role_value_(/*local=*/true, scope_tok);
                    if (node) root->add_child(node);
                    if (!root->first_child) return nullptr;
                    return root;
                }
                // SET LOCAL <var> = expr -- fall to generic path with scope
                AstNode* assignment = parse_variable_assignment(&scope_tok);
                if (assignment) root->add_child(assignment);
                if (!root->first_child) return nullptr;
                return root;
            }
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
                        node->add_child(make_node(arena_,
                            NodeType::NODE_IDENTIFIER, name.text));
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
                node->add_child(make_node(arena_,
                    NodeType::NODE_IDENTIFIER, mode.text));
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
            // PG SET is single-variable; commas after the first value are list
            // continuation, not new assignments (see PG docs:
            //   SET configuration_parameter { TO | = } { value | 'value' | DEFAULT }
            //   "Some configuration parameters take a list of values, such as
            //    search_path and datestyle.")
            // Each extra value is appended as another child of the same
            // VAR_ASSIGNMENT node, alongside the first RHS expression.
            while (assignment && tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                AstNode* extra_val = expr_parser_.parse();
                if (!extra_val) break;
                assignment->add_child(extra_val);
            }
        } else {
            while (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                AstNode* next_assign = parse_comma_item();
                if (next_assign) root->add_child(next_assign);
            }
        }

        if (!root->first_child) return nullptr;
        return root;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;

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
            node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                scope_tok.text));
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
            return make_node(arena_, NodeType::NODE_LITERAL_STRING, tok.text);
        }
        if (tok.type == TokenType::TK_DEFAULT) {
            tok_.skip();
            return make_node(arena_, NodeType::NODE_IDENTIFIER, tok.text);
        }
        if (tok.type == TokenType::TK_IDENTIFIER) {
            tok_.skip();
            // NONE / DEFAULT / <name> are all just identifiers here.
            return make_node(arena_, NodeType::NODE_IDENTIFIER, tok.text);
        }
        return nullptr;
    }

    AstNode* parse_comma_item() {
        Token peek = tok_.peek();
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

    // SET [GLOBAL|SESSION] TRANSACTION ...
    AstNode* parse_set_transaction(StringRef scope) {
        AstNode* node = make_node(arena_, NodeType::NODE_SET_TRANSACTION);
        if (!node) return nullptr;

        if (!scope.empty()) {
            node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, scope));
        }

        // ISOLATION LEVEL ... or READ ONLY/WRITE
        Token next = tok_.peek();
        if (next.type == TokenType::TK_ISOLATION) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_LEVEL) tok_.skip();

            // READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE
            Token level = tok_.next_token();
            if (level.type == TokenType::TK_READ) {
                Token sublevel = tok_.next_token();
                // Combine "READ COMMITTED" or "READ UNCOMMITTED"
                StringRef combined{level.text.ptr,
                    static_cast<uint32_t>((sublevel.text.ptr + sublevel.text.len) - level.text.ptr)};
                node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, combined));
            } else if (level.type == TokenType::TK_REPEATABLE) {
                Token read_tok = tok_.next_token(); // READ
                StringRef combined{level.text.ptr,
                    static_cast<uint32_t>((read_tok.text.ptr + read_tok.text.len) - level.text.ptr)};
                node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, combined));
            } else {
                // SERIALIZABLE
                node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, level.text));
            }
        } else if (next.type == TokenType::TK_READ) {
            tok_.skip();
            Token rw = tok_.next_token(); // ONLY or WRITE
            StringRef combined{next.text.ptr,
                static_cast<uint32_t>((rw.text.ptr + rw.text.len) - next.text.ptr)};
            node->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, combined));
        }

        return node;
    }

    // Parse a single variable assignment: [scope] target = expr
    // scope_token is non-null if GLOBAL/SESSION/LOCAL was already consumed
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
        if (var.type == TokenType::TK_AT) {
            // User variable @name. The name may be backtick/double-quoted;
            // in that case the source bytes between `@` and the name include
            // the opening delimiter (and the closing delimiter sits one past
            // name.text.len), so a naive StringRef-from-source produces
            // `@name` -- missing the closing delimiter. Build the canonical
            // `@name` form in the arena instead, dropping the delimiters.
            tok_.skip();
            Token name = tok_.next_token();
            target->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                build_scoped_identifier_("@", name)));
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
        if (eq.type == TokenType::TK_EQUAL || eq.type == TokenType::TK_COLON_EQUAL) {
            tok_.skip();
        } else if constexpr (D == Dialect::PostgreSQL) {
            if (eq.type == TokenType::TK_TO) {
                tok_.skip();
            }
        }

        // Parse RHS expression. If the parser couldn't produce one --
        // typically because the input is truncated (`SET x =`), starts
        // with a separator (`SET x = ,foo`, `SET x = ;`), or otherwise
        // malformed -- flag a parse error so the eventual ParseResult is
        // ERROR rather than PARTIAL with a missing-RHS AST.
        AstNode* rhs = expr_parser_.parse();
        if (rhs) {
            assignment->add_child(rhs);
        } else {
            tok_.flag_error();
            return nullptr;
        }

        return assignment;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_SET_PARSER_H
