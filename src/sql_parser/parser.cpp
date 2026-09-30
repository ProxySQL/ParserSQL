#include "sql_parser/parser.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/set_parser.h"
#include "sql_parser/select_parser.h"
#include "sql_parser/compound_query_parser.h"
#include "sql_parser/subquery_parse_callback.h"
#include "sql_parser/insert_parser.h"
#include "sql_parser/update_parser.h"
#include "sql_parser/delete_parser.h"
#include "sql_parser/pg_utility_parser.h"
#include "sql_parser/pg_ddl_parser.h"
#include "sql_parser/mysql_ddl_parser.h"
#include "sql_parser/mysql_procedure_parser.h"
#include "sql_parser/pg_admin_parser.h"
#include "sql_parser/pg_session_parser.h"
#include <limits>

namespace sql_parser {

template <Dialect D>
Parser<D>::Parser(const ParserConfig& config)
    : arena_(config.arena_block_size, config.arena_max_size),
      stmt_cache_(config.stmt_cache_capacity) {}

template <Dialect D>
void Parser<D>::reset() {
    arena_.reset();
}

template <Dialect D>
ParseResult Parser<D>::parse(const char* sql, size_t len) {
    arena_.reset();
    if (len > std::numeric_limits<uint32_t>::max()) {
        ParseResult result;
        result.error.message = {"SQL input exceeds 32-bit source span limit", 42};
        return result;
    }
    bool has_user_variables = false;
    if constexpr (D == Dialect::MySQL) {
        Tokenizer<D> detector;
        detector.reset(sql, len);
        while (detector.next_token().type != TokenType::TK_EOF) {}
        has_user_variables = detector.has_user_variables();
    }
    tokenizer_.reset(sql, len);
    ParseResult result = classify_and_dispatch();
    result.has_user_variables = has_user_variables || tokenizer_.has_user_variables();
    return result;
}

template <Dialect D>
BatchParseResult Parser<D>::parse_all(const char* sql, size_t len) {
    arena_.reset();
    BatchParseResult batch;
    if (len > std::numeric_limits<uint32_t>::max()) {
        ParsedStatement rejected;
        rejected.result.error.message = {"SQL input exceeds 32-bit source span limit", 42};
        batch.statements.push_back(rejected);
        return batch;
    }
    size_t cursor = 0;
    while (cursor < len) {
        Tokenizer<D> scanner;
        scanner.reset(sql + cursor, len - cursor);
        Token first = scanner.next_token();
        if (first.type == TokenType::TK_SEMICOLON) {
            cursor = static_cast<size_t>(first.source.ptr - sql) + first.source.len;
            continue;
        }
        if (first.type == TokenType::TK_EOF && !scanner.has_error()) break;

        const char* start = first.type == TokenType::TK_EOF && scanner.has_error()
            ? scanner.error_source().ptr : first.source.ptr;
        if (!start) start = sql + cursor;
        Token last = first;
        if constexpr (D == Dialect::MySQL) {
            if (first.type == TokenType::TK_CREATE && MySQLProcedureParser::handles(scanner.peek())) {
                auto routine_scanner = scanner;
                Arena boundary_arena;
                auto* routine = MySQLProcedureParser(routine_scanner, boundary_arena,
                    &parse_subquery_select<D>).parse();
                if (routine && !routine_scanner.has_error()) {
                    scanner = routine_scanner;
                    last = scanner.next_token();
                }
            }
        }
        while (last.type != TokenType::TK_EOF && last.type != TokenType::TK_SEMICOLON)
            last = scanner.next_token();
        const char* end = last.type == TokenType::TK_SEMICOLON
            ? last.source.ptr + last.source.len : sql + len;
        ParsedStatement statement;
        statement.offset = static_cast<uint32_t>(start - sql);
        statement.source = StringRef{start, static_cast<uint32_t>(end - start)};
        if (scanner.has_error()) {
            statement.result.status = ParseResult::ERROR;
            statement.result.remaining = scanner.error_source();
            statement.result.error.message = StringRef{"Invalid SQL token", 17};
            statement.result.error.offset = scanner.error_source().ptr
                ? static_cast<uint32_t>(scanner.error_source().ptr - sql) : statement.offset;
        } else {
            tokenizer_.reset(start, static_cast<size_t>(end - start));
            statement.result = classify_and_dispatch();
            statement.result.has_user_variables = scanner.has_user_variables();
            if (!statement.result.ok() || !statement.result.full_input) {
                StringRef error = tokenizer_.error_source();
                const char* at = error.ptr ? error.ptr : statement.result.remaining.ptr;
                statement.result.error.offset = at
                    ? static_cast<uint32_t>(at - sql) : statement.offset;
            }
        }
        batch.statements.push_back(statement);
        cursor = static_cast<size_t>(end - sql);
    }
    return batch;
}

template <Dialect D>
ParseResult Parser<D>::classify_and_dispatch() {
    Token first = tokenizer_.next_token();

    if (first.type == TokenType::TK_EOF) {
        ParseResult r;
        r.status = ParseResult::ERROR;
        r.stmt_type = StmtType::UNKNOWN;
        return r;
    }

    // Common statements have unambiguous token kinds. Avoid probing the
    // PostgreSQL utility grammars before dispatching these hot paths.
    switch (first.type) {
        case TokenType::TK_SELECT:   return parse_select();
        case TokenType::TK_WITH:     return parse_with();
        case TokenType::TK_TABLE:
        case TokenType::TK_VALUES:
            return parse_query_expression(first.type);
        case TokenType::TK_LPAREN: {
            // Parenthesized SELECT / compound query: (SELECT ...) UNION ...
            Token next = tokenizer_.peek();
            if (next.type == TokenType::TK_SELECT || next.type == TokenType::TK_LPAREN ||
                next.type == TokenType::TK_VALUES || next.type == TokenType::TK_TABLE ||
                (D == Dialect::MySQL && next.type == TokenType::TK_WITH)) {
                return parse_query_expression(TokenType::TK_LPAREN);
            }
            return extract_unknown(first);
        }
        case TokenType::TK_SET:      return parse_set();
        case TokenType::TK_INSERT:   return parse_insert(false);
        case TokenType::TK_UPDATE:   return parse_update();
        case TokenType::TK_DELETE:   return parse_delete();
        case TokenType::TK_REPLACE:  return parse_insert(true);
        case TokenType::TK_BEGIN:
        case TokenType::TK_START:
        case TokenType::TK_COMMIT:
        case TokenType::TK_ROLLBACK:
        case TokenType::TK_SAVEPOINT:return extract_transaction(first);
        case TokenType::TK_USE:      return extract_use(first);
        case TokenType::TK_SHOW:     return extract_show(first);
        default: break;
    }

    if constexpr (D == Dialect::PostgreSQL) {
        if (PgUtilityParser::word(first, "MERGE")) return parse_merge();
        if (PgAdminParser::handles(first, tokenizer_)) {
            ParseResult r = PgAdminParser(tokenizer_, arena_).parse(first);
            scan_to_end(r); return r;
        }
        auto session_look = tokenizer_;
        const bool prepare_transaction = first.type == TokenType::TK_PREPARE &&
            session_look.next_token().type == TokenType::TK_TRANSACTION &&
            session_look.peek().type != TokenType::TK_AS && session_look.peek().type != TokenType::TK_LPAREN;
        if (PgSessionParser::handles(first) && !prepare_transaction) {
            ParseResult r = PgSessionParser(tokenizer_, arena_).parse(first);
            scan_to_end(r); return r;
        }
        if (PgDdlParser::handles(first)) {
            PgDdlParser ddl(tokenizer_, arena_, &parse_subquery_select<D>);
            ParseResult r = ddl.parse(first);
            scan_to_end(r);
            return r;
        }
        if (first.type == TokenType::TK_IDENTIFIER && PgUtilityParser::word(first, "COPY")) {
            PgUtilityParser utility(tokenizer_, arena_);
            ParseResult r = utility.copy();
            scan_to_end(r);
            return r;
        }
        if (((first.type == TokenType::TK_IDENTIFIER || first.type == TokenType::TK_END) &&
            (PgUtilityParser::word(first, "RELEASE") || PgUtilityParser::word(first, "END") ||
             PgUtilityParser::word(first, "ABORT"))) ||
            (first.type == TokenType::TK_PREPARE && tokenizer_.peek().type == TokenType::TK_TRANSACTION)) {
            PgUtilityParser utility(tokenizer_, arena_);
            ParseResult r = utility.transaction(first);
            scan_to_end(r);
            return r;
        }
    }

    if constexpr (D == Dialect::MySQL) {
        if (first.type == TokenType::TK_CREATE && MySQLProcedureParser::handles(tokenizer_.peek())) {
            ParseResult r;
            r.stmt_type = StmtType::CREATE;
            r.ast = MySQLProcedureParser(tokenizer_, arena_, &parse_subquery_select<D>).parse();
            r.status = r.ast ? ParseResult::OK : ParseResult::PARTIAL;
            scan_to_end(r); return r;
        }
        if ((first.type == TokenType::TK_CREATE || first.type == TokenType::TK_ALTER) &&
            (tokenizer_.peek().type == TokenType::TK_TABLE ||
             (first.type == TokenType::TK_CREATE && ExpressionParser<D>::keyword(tokenizer_.peek(), "TEMPORARY")))) {
            ParseResult r = MySQLDdlParser(tokenizer_, arena_, &parse_subquery_select<D>).parse(first);
            scan_to_end(r); return r;
        }
    }

    switch (first.type) {
        case TokenType::TK_PREPARE:  return extract_prepare(first);
        case TokenType::TK_EXECUTE:  return extract_execute(first);
        case TokenType::TK_DEALLOCATE: return extract_deallocate(first);
        case TokenType::TK_CREATE:
        case TokenType::TK_ALTER:
        case TokenType::TK_DROP:
        case TokenType::TK_TRUNCATE: return extract_ddl(first);
        case TokenType::TK_GRANT:
        case TokenType::TK_REVOKE:   return extract_acl(first);
        case TokenType::TK_LOCK:
        case TokenType::TK_UNLOCK:   return extract_lock(first);
        case TokenType::TK_EXPLAIN:  return parse_explain(false);
        case TokenType::TK_DESCRIBE:
        case TokenType::TK_DESC:     return parse_explain(true);
        case TokenType::TK_CALL:     return parse_call();
        case TokenType::TK_DO:       return parse_do();
        case TokenType::TK_LOAD:     return parse_load_data();
        case TokenType::TK_RESET:    return extract_reset(first);
        default:                     return extract_unknown(first);
    }
}

// ---- Tier 1 stubs ----

template <Dialect D>
ParseResult Parser<D>::parse_select() {
    ParseResult r;
    r.stmt_type = StmtType::SELECT;

    CompoundQueryParser<D> compound_parser(tokenizer_, arena_);
    compound_parser.set_subquery_callback(&parse_subquery_select<D>);
    AstNode* ast = compound_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_query_expression(TokenType first) {
    ParseResult r;
    r.stmt_type = StmtType::SELECT;
    CompoundQueryParser<D> parser(tokenizer_, arena_, D == Dialect::MySQL);
    parser.set_subquery_callback(&parse_subquery_select<D>);
    r.ast = parser.parse(first);
    r.status = r.ast ? ParseResult::OK : ParseResult::PARTIAL;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_set() {
    ParseResult r;
    r.stmt_type = StmtType::SET;

    SetParser<D> set_parser(tokenizer_, arena_);
    set_parser.set_subquery_callback(&parse_subquery_select<D>);
    AstNode* ast = set_parser.parse();

    if (ast && ast->first_child) {
        r.status = ParseResult::OK;
        r.ast = ast;
    } else {
        r.status = ParseResult::PARTIAL;
        r.ast = ast;
    }

    // Downgrade PARTIAL -> ERROR only when the parse produced NO
    // assignments at all AND the tokenizer flagged an error. Keeps
    // ERROR clear for top-level malformed input (`SET = 1`,
    // `SET datestyle = ;`, bare `$word`) while leaving multi-assignment
    // SETs that contain one malformed element alongside well-formed
    // ones at PARTIAL -- the well-formed assignments are still in the
    // AST and the consumer can decide what to do with them.
    if (r.status == ParseResult::PARTIAL && tokenizer_.has_error() &&
        (!ast || !ast->first_child)) {
        r.status = ParseResult::ERROR;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_insert(bool is_replace) {
    ParseResult r;
    r.stmt_type = is_replace ? StmtType::REPLACE : StmtType::INSERT;

    InsertParser<D> insert_parser(tokenizer_, arena_, is_replace);
    insert_parser.set_subquery_callback(&parse_subquery_select<D>);
    AstNode* ast = insert_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;

        // Extract table_name/schema_name from AST for backward compatibility
        for (const AstNode* child = ast->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_TABLE_REF) {
                const AstNode* name_node = child->first_child;
                if (name_node && name_node->type == NodeType::NODE_QUALIFIED_NAME) {
                    // schema.table
                    const AstNode* schema = name_node->first_child;
                    const AstNode* table = schema ? schema->next_sibling : nullptr;
                    if (schema) r.schema_name = schema->value();
                    if (table) r.table_name = table->value();
                } else if (name_node && name_node->type == NodeType::NODE_IDENTIFIER) {
                    r.table_name = name_node->value();
                }
                break;
            }
        }
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

namespace {
// Keep routing metadata consistent for standalone and WITH-prefixed DML.
void extract_dml_target(const AstNode* statement, ParseResult& result) {
    for (const auto* child = statement->first_child; child; child = child->next_sibling) {
        if (child->type != NodeType::NODE_TABLE_REF) continue;
        const auto* name = child->first_child;
        if (name && name->type == NodeType::NODE_QUALIFIED_NAME) {
            const auto* first = name->first_child;
            const auto* second = first ? first->next_sibling : nullptr;
            if (second && second->type == NodeType::NODE_ASTERISK) {
                result.table_name = first->value(); // DELETE table.* FROM ...
            } else {
                if (first) result.schema_name = first->value();
                if (second) result.table_name = second->value();
            }
        } else if (name && name->type == NodeType::NODE_IDENTIFIER) {
            result.table_name = name->value();
        }
        break;
    }
}
} // namespace

template <Dialect D>
ParseResult Parser<D>::parse_update() {
    ParseResult r;
    r.stmt_type = StmtType::UPDATE;

    UpdateParser<D> update_parser(tokenizer_, arena_);
    update_parser.set_subquery_callback(&parse_subquery_select<D>);
    AstNode* ast = update_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;

        extract_dml_target(ast, r);
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_delete() {
    ParseResult r;
    r.stmt_type = StmtType::DELETE_STMT;

    DeleteParser<D> delete_parser(tokenizer_, arena_);
    delete_parser.set_subquery_callback(&parse_subquery_select<D>);
    AstNode* ast = delete_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;

        extract_dml_target(ast, r);
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_merge() {
    ParseResult r;
    r.stmt_type = StmtType::MERGE;
    if constexpr (D == Dialect::PostgreSQL) {
        r.ast = PgDmlParser(tokenizer_, arena_, &parse_subquery_select<D>).merge();
        r.status = r.ast ? ParseResult::OK : ParseResult::ERROR;
        if (r.ast) extract_dml_target(r.ast, r);
        scan_to_end(r);
    }
    return r;
}

// ---- EXPLAIN / DESCRIBE ----

template <Dialect D>
ParseResult Parser<D>::parse_explain(bool is_describe) {
    ParseResult r;
    r.stmt_type = is_describe ? StmtType::DESCRIBE : StmtType::EXPLAIN;

    AstNode* root = make_node(arena_, NodeType::NODE_EXPLAIN_STMT);
    if (!root) { r.status = ParseResult::ERROR; scan_to_end(r); return r; }

    if constexpr (D == Dialect::MySQL) {
        auto failure = [&]() {
            tokenizer_.flag_fatal_error_at(tokenizer_.peek().source);
            r.status = ParseResult::ERROR; r.ast = root;
            scan_to_end(r); return r;
        };
        auto identifier = [&](const Token& token) {
            return make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token,
                token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
        };
        auto* options = make_node(arena_, NodeType::NODE_EXPLAIN_OPTIONS);
        if (!options) return failure();
        if (tokenizer_.peek().type == TokenType::TK_ANALYZE) {
            auto* analyze = identifier(tokenizer_.next_token());
            if (!analyze) return failure();
            options->add_child(analyze);
        }
        auto format_lookahead = tokenizer_; format_lookahead.skip();
        if (tokenizer_.peek().type == TokenType::TK_FORMAT && format_lookahead.peek().type == TokenType::TK_EQUAL) {
            tokenizer_.skip();
            if (tokenizer_.peek().type != TokenType::TK_EQUAL) return failure();
            tokenizer_.skip();
            const Token format = tokenizer_.next_token();
            if ((!mysql_identifier_token(format) && format.type != TokenType::TK_STRING) ||
                mysql_charset_introducer(format)) return failure();
            // The native grammar accepts ident_or_text; format availability and
            // INTO's JSON requirement remain server semantic checks.
            auto* node = make_node(arena_, NodeType::NODE_EXPLAIN_FORMAT, format.source);
            if (!node) return failure();
            options->add_child(node);
        }
        if (tokenizer_.peek().type == TokenType::TK_INTO) {
            tokenizer_.skip();
            auto* destination = make_mysql_user_variable_node(arena_, tokenizer_.next_token());
            auto* into = make_node(arena_, NodeType::NODE_MYSQL_EXPLAIN_INTO);
            if (!destination || !into) return failure();
            into->add_child(destination); options->add_child(into);
        }
        if (options->first_child) root->add_child(options);

        const Token first = tokenizer_.peek();
        const bool query = ExpressionParser<D>::starts_query(first.type) || first.type == TokenType::TK_LPAREN;
        if (query && first.type != TokenType::TK_WITH) {
            auto* inner = parse_subquery_select<D>(tokenizer_, arena_);
            if (!inner) return failure();
            root->add_child(inner); r.ast = root; r.status = ParseResult::OK;
            scan_to_end(r); return r;
        }
        if (query || first.type == TokenType::TK_INSERT || first.type == TokenType::TK_REPLACE ||
            first.type == TokenType::TK_UPDATE || first.type == TokenType::TK_DELETE) {
            ParseResult inner = classify_and_dispatch();
            if (inner.ast) root->add_child(inner.ast);
            r.ast = root; r.status = inner.status;
            r.full_input = inner.full_input; r.remaining = inner.remaining;
            return r;
        }
        // EXPLAIN, DESC and DESCRIBE share the table-description form. Options
        // belong to statement explanation, never to this shorthand.
        if (options->first_child || !mysql_identifier_token(first) || mysql_charset_introducer(first))
            return failure();
        tokenizer_.skip();
        auto* table = make_node(arena_, NodeType::NODE_TABLE_REF);
        auto* name = identifier(first);
        if (!table || !name) return failure();
        r.table_name = first.text;
        if (tokenizer_.peek().type == TokenType::TK_DOT) {
            tokenizer_.skip();
            const Token second = tokenizer_.next_token();
            if (!mysql_identifier_word(second)) return failure();
            auto* qualified = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            auto* component = identifier(second);
            if (!qualified || !component) return failure();
            qualified->add_child(name); qualified->add_child(component); name = qualified;
            r.schema_name = first.text; r.table_name = second.text;
        }
        table->add_child(name); root->add_child(table);
        const Token column = tokenizer_.peek();
        if (column.type != TokenType::TK_EOF && column.type != TokenType::TK_SEMICOLON) {
            const bool text = column.type == TokenType::TK_STRING;
            const bool hex = column.type == TokenType::TK_HEX_LITERAL;
            const bool bit = column.type == TokenType::TK_BIT_LITERAL;
            if ((!mysql_identifier_token(column) && !text && !hex && !bit) ||
                mysql_charset_introducer(column)) return failure();
            tokenizer_.skip();
            auto* field = (text || hex || bit)
                ? make_node_from_token(arena_, text ? NodeType::NODE_LITERAL_STRING :
                    hex ? NodeType::NODE_LITERAL_HEX : NodeType::NODE_LITERAL_BIT, column) : identifier(column);
            if (!field) return failure();
            root->add_child(field);
        }
        r.ast = root; r.status = ParseResult::OK;
        scan_to_end(r); return r;
    }

    if (is_describe) {
        // DESCRIBE table_name [column_name]
        // DESC table_name [column_name]
        Token name = tokenizer_.next_token();
        if (name.type == TokenType::TK_EOF || name.type == TokenType::TK_SEMICOLON) {
            r.status = ParseResult::PARTIAL;
            r.ast = root;
            return r;
        }

        // Table name (possibly qualified)
        AstNode* table_ref = make_node(arena_, NodeType::NODE_TABLE_REF);
        if (tokenizer_.peek().type == TokenType::TK_DOT) {
            tokenizer_.skip();
            Token table_tok = tokenizer_.next_token();
            AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, table_tok.text));
            table_ref->add_child(qname);
            r.schema_name = name.text;
            r.table_name = table_tok.text;
        } else {
            table_ref->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
            r.table_name = name.text;
        }
        root->add_child(table_ref);

        // Optional column name
        Token next = tokenizer_.peek();
        if (next.type != TokenType::TK_EOF && next.type != TokenType::TK_SEMICOLON) {
            Token col = tokenizer_.next_token();
            root->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, col.text));
        }

        r.status = ParseResult::OK;
        r.ast = root;
        scan_to_end(r);
        return r;
    }

    if constexpr (D == Dialect::PostgreSQL) {
        ExpressionParser<D> expr(tokenizer_, arena_, true);
        auto* options = PgQueryClauses<D>(tokenizer_, arena_, expr).explain_options();
        if (!options) { r.status = ParseResult::ERROR; scan_to_end(r); return r; }
        if (options->first_child) root->add_child(options);
        auto first = tokenizer_.peek().type;
        if (!ExpressionParser<D>::starts_query(first) && first != TokenType::TK_LPAREN &&
            first != TokenType::TK_INSERT && first != TokenType::TK_UPDATE &&
            first != TokenType::TK_DELETE && !ExpressionParser<D>::keyword(tokenizer_.peek(), "MERGE") &&
            !ExpressionParser<D>::keyword(tokenizer_.peek(), "DECLARE") &&
            first != TokenType::TK_CREATE && first != TokenType::TK_EXECUTE) {
            r.status = ParseResult::ERROR; scan_to_end(r); return r;
        }
        ParseResult inner = classify_and_dispatch();
        if (first == TokenType::TK_CREATE && inner.ast) {
            bool table = false, materialized = false, query = false;
            for (const auto* child = inner.ast->first_child; child; child = child->next_sibling) {
                if (child->type == NodeType::NODE_PG_DDL_SYNTAX) {
                    table |= child->value().equals_ci("TABLE", 5);
                    materialized |= child->value().equals_ci("MATERIALIZED", 12);
                }
                query |= child->type == NodeType::NODE_SELECT_STMT || child->type == NodeType::NODE_COMPOUND_QUERY ||
                    child->type == NodeType::NODE_CTE || child->type == NodeType::NODE_TABLE_QUERY;
            }
            if ((!table && !materialized) || !query) inner.status = ParseResult::ERROR;
        }
        root->add_child(inner.ast);
        r.status = inner.status; r.ast = root;
        r.full_input = inner.full_input; r.remaining = inner.remaining;
        return r;
    }

    r.status = ParseResult::ERROR;
    scan_to_end(r);
    return r;
}

// ---- CALL ----

template <Dialect D>
ParseResult Parser<D>::parse_call() {
    ParseResult r;
    r.stmt_type = StmtType::CALL;

    AstNode* root = make_node(arena_, NodeType::NODE_CALL_STMT);
    if (!root) { r.status = ParseResult::ERROR; scan_to_end(r); return r; }

    // Parse procedure name (possibly qualified: schema.procedure)
    Token name = tokenizer_.next_token();
    if (name.type == TokenType::TK_EOF) {
        r.status = ParseResult::PARTIAL;
        r.ast = root;
        return r;
    }

    if (tokenizer_.peek().type == TokenType::TK_DOT) {
        tokenizer_.skip();
        Token proc_name = tokenizer_.next_token();
        AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
        qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
        qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, proc_name.text));
        root->add_child(qname);
    } else {
        root->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
    }

    // Parse argument list: (arg1, arg2, ...)
    if (tokenizer_.peek().type == TokenType::TK_LPAREN) {
        tokenizer_.skip();
        ExpressionParser<D> expr_parser(tokenizer_, arena_);
        if (tokenizer_.peek().type != TokenType::TK_RPAREN) {
            while (true) {
                AstNode* arg = expr_parser.parse_argument();
                if constexpr (D == Dialect::PostgreSQL) {
                    if (!arg) { expr_parser.syntax_error(); break; }
                }
                if (arg) root->add_child(arg);
                if (tokenizer_.peek().type == TokenType::TK_COMMA) {
                    tokenizer_.skip();
                } else {
                    break;
                }
            }
        }
        if (tokenizer_.peek().type == TokenType::TK_RPAREN) {
            tokenizer_.skip();
        }
    }

    r.status = ParseResult::OK;
    r.ast = root;
    scan_to_end(r);
    return r;
}

// ---- DO ----

template <Dialect D>
ParseResult Parser<D>::parse_do() {
    ParseResult r;
    r.stmt_type = StmtType::DO_STMT;

    AstNode* root = make_node(arena_, NodeType::NODE_DO_STMT);
    if (!root) { r.status = ParseResult::ERROR; scan_to_end(r); return r; }

    // Parse expression list: expr [, expr, ...]
    ExpressionParser<D> expr_parser(tokenizer_, arena_);
    while (true) {
        AstNode* expr = expr_parser.parse();
        if (!expr) break;
        root->add_child(expr);
        if (tokenizer_.peek().type == TokenType::TK_COMMA) {
            tokenizer_.skip();
        } else {
            break;
        }
    }

    r.status = ParseResult::OK;
    r.ast = root;
    scan_to_end(r);
    return r;
}

// ---- LOAD DATA ----

template <Dialect D>
ParseResult Parser<D>::parse_load_data() {
    ParseResult r;
    r.stmt_type = StmtType::LOAD_DATA;

    AstNode* root = make_node(arena_, NodeType::NODE_LOAD_DATA_STMT);
    if (!root) { r.status = ParseResult::ERROR; scan_to_end(r); return r; }

    // Expect DATA keyword
    if (tokenizer_.peek().type == TokenType::TK_DATA) {
        tokenizer_.skip();
    } else {
        // Not LOAD DATA -- fall back to partial
        r.status = ParseResult::PARTIAL;
        r.ast = root;
        scan_to_end(r);
        return r;
    }

    AstNode* options = make_node(arena_, NodeType::NODE_LOAD_DATA_OPTIONS);
    bool has_options = false;

    // Optional: LOW_PRIORITY | CONCURRENT
    Token t = tokenizer_.peek();
    if (t.type == TokenType::TK_LOW_PRIORITY || t.type == TokenType::TK_CONCURRENT) {
        tokenizer_.skip();
        options->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, t.text));
        has_options = true;
    }

    // Optional: LOCAL
    if (tokenizer_.peek().type == TokenType::TK_LOCAL) {
        Token local_tok = tokenizer_.next_token();
        options->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, local_tok.text));
        has_options = true;
    }

    // Expect INFILE 'filename'
    if (tokenizer_.peek().type == TokenType::TK_INFILE) {
        tokenizer_.skip();
    }

    Token filename = tokenizer_.next_token();  // string literal
    root->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, filename.text));

    // Optional: REPLACE | IGNORE
    t = tokenizer_.peek();
    if (t.type == TokenType::TK_REPLACE || t.type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        options->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, t.text));
        has_options = true;
    }

    // Expect INTO TABLE table_name
    if (tokenizer_.peek().type == TokenType::TK_INTO) {
        tokenizer_.skip();
    }
    if (tokenizer_.peek().type == TokenType::TK_TABLE) {
        tokenizer_.skip();
    }

    // Parse table name
    Token table_name = tokenizer_.next_token();
    AstNode* table_ref = make_node(arena_, NodeType::NODE_TABLE_REF);
    if (tokenizer_.peek().type == TokenType::TK_DOT) {
        tokenizer_.skip();
        Token actual_table = tokenizer_.next_token();
        AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
        qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, table_name.text));
        qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, actual_table.text));
        table_ref->add_child(qname);
        r.schema_name = table_name.text;
        r.table_name = actual_table.text;
    } else {
        table_ref->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, table_name.text));
        r.table_name = table_name.text;
    }
    root->add_child(table_ref);

    // Optional CHARACTER SET
    if (tokenizer_.peek().type == TokenType::TK_CHARACTER) {
        tokenizer_.skip();
        if (tokenizer_.peek().type == TokenType::TK_SET) {
            tokenizer_.skip();
        }
        Token charset = tokenizer_.next_token();
        options->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, charset.text));
        has_options = true;
    }

    // Optional FIELDS/COLUMNS clause
    t = tokenizer_.peek();
    if (t.type == TokenType::TK_FIELDS || t.type == TokenType::TK_COLUMNS) {
        tokenizer_.skip();

        // TERMINATED BY 'string'
        if (tokenizer_.peek().type == TokenType::TK_TERMINATED) {
            tokenizer_.skip();
            if (tokenizer_.peek().type == TokenType::TK_BY) tokenizer_.skip();
            Token delim = tokenizer_.next_token();
            // Store as "TERMINATED:<delim>" in an identifier node
            AstNode* term = make_node(arena_, NodeType::NODE_IDENTIFIER);
            // Build a combined reference: "FIELDS TERMINATED BY" + value
            // We'll store the delimiter value as a string literal child
            term->set_value(StringRef{"TERMINATED", 10});
            term->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, delim.text));
            options->add_child(term);
            has_options = true;
        }

        // [OPTIONALLY] ENCLOSED BY 'char'
        if (tokenizer_.peek().type == TokenType::TK_OPTIONALLY) {
            tokenizer_.skip();
        }
        if (tokenizer_.peek().type == TokenType::TK_ENCLOSED) {
            tokenizer_.skip();
            if (tokenizer_.peek().type == TokenType::TK_BY) tokenizer_.skip();
            Token encl = tokenizer_.next_token();
            AstNode* enc = make_node(arena_, NodeType::NODE_IDENTIFIER);
            enc->set_value(StringRef{"ENCLOSED", 8});
            enc->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, encl.text));
            options->add_child(enc);
            has_options = true;
        }

        // ESCAPED BY 'char'
        if (tokenizer_.peek().type == TokenType::TK_ESCAPED) {
            tokenizer_.skip();
            if (tokenizer_.peek().type == TokenType::TK_BY) tokenizer_.skip();
            Token esc = tokenizer_.next_token();
            AstNode* esc_node = make_node(arena_, NodeType::NODE_IDENTIFIER);
            esc_node->set_value(StringRef{"ESCAPED", 7});
            esc_node->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, esc.text));
            options->add_child(esc_node);
            has_options = true;
        }
    }

    // Optional LINES clause
    if (tokenizer_.peek().type == TokenType::TK_LINES) {
        tokenizer_.skip();

        // STARTING BY 'string'
        if (tokenizer_.peek().type == TokenType::TK_STARTING) {
            tokenizer_.skip();
            if (tokenizer_.peek().type == TokenType::TK_BY) tokenizer_.skip();
            Token start_str = tokenizer_.next_token();
            AstNode* start_node = make_node(arena_, NodeType::NODE_IDENTIFIER);
            start_node->set_value(StringRef{"STARTING", 8});
            start_node->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, start_str.text));
            options->add_child(start_node);
            has_options = true;
        }

        // TERMINATED BY 'string'
        if (tokenizer_.peek().type == TokenType::TK_TERMINATED) {
            tokenizer_.skip();
            if (tokenizer_.peek().type == TokenType::TK_BY) tokenizer_.skip();
            Token term_str = tokenizer_.next_token();
            AstNode* term_node = make_node(arena_, NodeType::NODE_IDENTIFIER);
            term_node->set_value(StringRef{"LINES_TERMINATED", 16});
            term_node->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, term_str.text));
            options->add_child(term_node);
            has_options = true;
        }
    }

    // Optional IGNORE number LINES/ROWS
    if (tokenizer_.peek().type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        Token num = tokenizer_.next_token();
        options->add_child(make_node(arena_, NodeType::NODE_LITERAL_INT, num.text));
        // consume LINES or ROWS
        t = tokenizer_.peek();
        if (t.type == TokenType::TK_LINES || t.type == TokenType::TK_ROWS) {
            tokenizer_.skip();
        }
        has_options = true;
    }

    // Optional column list: (col1, col2, ...)
    if (tokenizer_.peek().type == TokenType::TK_LPAREN) {
        tokenizer_.skip();
        while (tokenizer_.peek().type != TokenType::TK_RPAREN &&
               tokenizer_.peek().type != TokenType::TK_EOF) {
            Token col = tokenizer_.next_token();
            if (col.type == TokenType::TK_COMMA) continue;
            root->add_child(make_node(arena_, NodeType::NODE_COLUMN_REF, col.text));
        }
        if (tokenizer_.peek().type == TokenType::TK_RPAREN) {
            tokenizer_.skip();
        }
    }

    if (has_options) {
        root->add_child(options);
    }

    r.status = ParseResult::OK;
    r.ast = root;
    scan_to_end(r);
    return r;
}

// ---- Helpers ----

template <Dialect D>
Token Parser<D>::read_table_name(StringRef& schema_out) {
    Token name = tokenizer_.next_token();
    if (name.type != TokenType::TK_IDENTIFIER &&
        name.type != TokenType::TK_EOF) {
        // Keywords used as table names (e.g., CREATE TABLE `user`)
        // The tokenizer returns keyword tokens for reserved words.
        // Accept any non-punctuation token as a potential name.
    }

    // Check for qualified name: schema.table
    if (tokenizer_.peek().type == TokenType::TK_DOT) {
        schema_out = name.text;
        tokenizer_.skip();  // consume dot
        Token table = tokenizer_.next_token();
        return table;
    }

    schema_out = StringRef{};
    return name;
}

template <Dialect D>
void Parser<D>::scan_to_end(ParseResult& result) {
    Token first = tokenizer_.next_token();
    if (first.type == TokenType::TK_EOF) {
        StringRef error_source = tokenizer_.error_source();
        if (!error_source.empty()) {
            if (tokenizer_.has_fatal_error()) result.status = ParseResult::ERROR;
            result.remaining = StringRef{error_source.ptr,
                static_cast<uint32_t>(tokenizer_.input_end() - error_source.ptr)};
        } else {
            result.full_input = true;
        }
        return;
    }

    if (first.type == TokenType::TK_SEMICOLON) {
        Token next = tokenizer_.next_token();
        if (next.type == TokenType::TK_EOF) {
            StringRef error_source = tokenizer_.error_source();
            if (tokenizer_.has_fatal_error() && !error_source.empty()) {
                result.status = ParseResult::ERROR;
                result.remaining = StringRef{error_source.ptr,
                    static_cast<uint32_t>(tokenizer_.input_end() - error_source.ptr)};
                return;
            }
            result.full_input = true;
            return;
        }
        first = next;
    }

    const char* remaining_start = first.source.ptr ? first.source.ptr : first.text.ptr;
    if (remaining_start) {
        result.remaining = StringRef{remaining_start,
            static_cast<uint32_t>(tokenizer_.input_end() - remaining_start)};
    }
}

// ---- Tier 2 Extractors ----

template <Dialect D>
ParseResult Parser<D>::extract_insert(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::INSERT;

    // Expect optional INTO
    Token t = tokenizer_.peek();
    if (t.type == TokenType::TK_INTO) {
        tokenizer_.skip();
    }

    // Read table name
    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_update(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::UPDATE;

    // Optional LOW_PRIORITY / IGNORE
    Token t = tokenizer_.peek();
    while (t.type == TokenType::TK_LOW_PRIORITY || t.type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        t = tokenizer_.peek();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_delete(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::DELETE_STMT;

    // Optional LOW_PRIORITY / QUICK / IGNORE
    Token t = tokenizer_.peek();
    while (t.type == TokenType::TK_LOW_PRIORITY ||
           t.type == TokenType::TK_QUICK ||
           t.type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        t = tokenizer_.peek();
    }

    // Expect FROM
    if (tokenizer_.peek().type == TokenType::TK_FROM) {
        tokenizer_.skip();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_replace(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::REPLACE;

    if (tokenizer_.peek().type == TokenType::TK_INTO) {
        tokenizer_.skip();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_transaction(const Token& first) {
    if constexpr (D == Dialect::PostgreSQL) {
        PgUtilityParser utility(tokenizer_, arena_);
        ParseResult result = utility.transaction(first);
        scan_to_end(result);
        return result;
    }
    ParseResult r;
    r.status = ParseResult::OK;

    switch (first.type) {
        case TokenType::TK_BEGIN:
            r.stmt_type = StmtType::BEGIN;
            break;
        case TokenType::TK_START:
            r.stmt_type = StmtType::START_TRANSACTION;
            if (tokenizer_.peek().type == TokenType::TK_TRANSACTION)
                tokenizer_.skip();
            else r.status = ParseResult::ERROR;
            break;
        case TokenType::TK_COMMIT:
            r.stmt_type = StmtType::COMMIT;
            break;
        case TokenType::TK_ROLLBACK:
            r.stmt_type = StmtType::ROLLBACK;
            break;
        case TokenType::TK_SAVEPOINT:
            r.stmt_type = StmtType::SAVEPOINT;
            if (tokenizer_.peek().type == TokenType::TK_IDENTIFIER)
                r.table_name = tokenizer_.next_token().text;
            else r.status = ParseResult::ERROR;
            break;
        default:
            r.stmt_type = StmtType::UNKNOWN;
            break;
    }

    if (r.stmt_type == StmtType::BEGIN || r.stmt_type == StmtType::COMMIT ||
        r.stmt_type == StmtType::ROLLBACK) {
        const Token next = tokenizer_.peek();
        if (next.source.ptr == next.text.ptr && next.text.equals_ci("WORK", 4))
            tokenizer_.skip();
    }
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_use(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::USE;

    Token db = tokenizer_.next_token();
    r.database_name = db.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_show(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::SHOW;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_prepare(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::PREPARE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_execute(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::EXECUTE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_deallocate(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::DEALLOCATE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_ddl(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;

    switch (first.type) {
        case TokenType::TK_CREATE:   r.stmt_type = StmtType::CREATE; break;
        case TokenType::TK_ALTER:    r.stmt_type = StmtType::ALTER; break;
        case TokenType::TK_DROP:     r.stmt_type = StmtType::DROP; break;
        case TokenType::TK_TRUNCATE: r.stmt_type = StmtType::TRUNCATE; break;
        default:                     r.stmt_type = StmtType::UNKNOWN; break;
    }

    // Try to extract object name: CREATE/ALTER/DROP [IF EXISTS/NOT EXISTS] TABLE/INDEX/VIEW name
    Token t = tokenizer_.next_token();

    // Skip optional IF [NOT] EXISTS
    if (t.type == TokenType::TK_IF) {
        t = tokenizer_.next_token(); // NOT or EXISTS
        if (t.type == TokenType::TK_NOT) {
            t = tokenizer_.next_token(); // EXISTS
        }
        // Skip EXISTS
        t = tokenizer_.next_token(); // should be TABLE/INDEX/etc.
    }

    // Now t should be TABLE, INDEX, VIEW, DATABASE, SCHEMA, or a name
    if (t.type == TokenType::TK_TABLE || t.type == TokenType::TK_INDEX ||
        t.type == TokenType::TK_VIEW || t.type == TokenType::TK_DATABASE ||
        t.type == TokenType::TK_SCHEMA) {
        // Optional IF [NOT] EXISTS after object type for CREATE/DROP
        Token maybe_if = tokenizer_.peek();
        if (maybe_if.type == TokenType::TK_IF) {
            tokenizer_.skip(); // IF
            Token next = tokenizer_.next_token();
            if (next.type == TokenType::TK_NOT) {
                tokenizer_.skip(); // EXISTS
            }
        }
        Token name = read_table_name(r.schema_name);
        r.table_name = name.text;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_acl(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = (first.type == TokenType::TK_GRANT) ? StmtType::GRANT : StmtType::REVOKE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_lock(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = (first.type == TokenType::TK_LOCK) ? StmtType::LOCK : StmtType::UNLOCK;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_reset(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::RESET;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_unknown(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::UNKNOWN;
    scan_to_end(r);
    return r;
}

// ---- Prepared statement support ----

template <Dialect D>
ParseResult Parser<D>::parse_and_cache(const char* sql, size_t len, uint32_t stmt_id) {
    ParseResult r = parse(sql, len);
    if (r.ast) {
        stmt_cache_.store(stmt_id, r.stmt_type, r.ast);
    }
    return r;
}

template <Dialect D>
ParseResult Parser<D>::execute(uint32_t stmt_id, const ParamBindings& params) {
    ParseResult r;
    const CachedStmt* cached = stmt_cache_.lookup(stmt_id);
    if (!cached) {
        r.status = ParseResult::ERROR;
        r.stmt_type = StmtType::UNKNOWN;
        return r;
    }
    r.status = ParseResult::OK;
    r.stmt_type = cached->stmt_type;
    r.ast = cached->ast;
    r.bindings = params;
    return r;
}

template <Dialect D>
void Parser<D>::prepare_cache_evict(uint32_t stmt_id) {
    stmt_cache_.evict(stmt_id);
}

// ---- WITH (CTE) ----

template <Dialect D>
ParseResult Parser<D>::parse_with() {
    ParseResult r;
    r.stmt_type = StmtType::SELECT;

    // WITH keyword already consumed by classifier.
    if constexpr (D == Dialect::PostgreSQL) {
        r.ast = parse_pg_with(tokenizer_, arena_, true);
        r.status = r.ast ? ParseResult::OK : ParseResult::ERROR;
        if (r.ast) {
            const AstNode* main = r.ast->first_child;
            while (main && main->type == NodeType::NODE_CTE_DEFINITION) main = main->next_sibling;
            if (main) {
                switch (main->type) {
                    case NodeType::NODE_INSERT_STMT: r.stmt_type = StmtType::INSERT; break;
                    case NodeType::NODE_UPDATE_STMT: r.stmt_type = StmtType::UPDATE; break;
                    case NodeType::NODE_DELETE_STMT: r.stmt_type = StmtType::DELETE_STMT; break;
                    case NodeType::NODE_MERGE_STMT: r.stmt_type = StmtType::MERGE; break;
                    default: break;
                }
                if (r.stmt_type != StmtType::SELECT) extract_dml_target(main, r);
            }
        }
        scan_to_end(r);
        return r;
    }
    if constexpr (D == Dialect::MySQL) {
        r.ast = parse_mysql_with(tokenizer_, arena_, true);
        if (r.ast) {
            const AstNode* main = r.ast->first_child;
            while (main && main->type == NodeType::NODE_CTE_DEFINITION) main = main->next_sibling;
            if (main && main->type == NodeType::NODE_UPDATE_STMT) r.stmt_type = StmtType::UPDATE;
            if (main && main->type == NodeType::NODE_DELETE_STMT) r.stmt_type = StmtType::DELETE_STMT;
            if (main && (main->type == NodeType::NODE_UPDATE_STMT ||
                         main->type == NodeType::NODE_DELETE_STMT)) extract_dml_target(main, r);
        }
        r.status = r.ast ? ParseResult::OK : ParseResult::ERROR;
    }

    scan_to_end(r);
    return r;
}

namespace {

bool is_forbidden_user_variable_context(NodeType type) {
    switch (type) {
        case NodeType::NODE_FUNCTION_CALL:
        case NodeType::NODE_CALL_STMT:
        case NodeType::NODE_DO_STMT:
        case NodeType::NODE_PLACEHOLDER:
        case NodeType::NODE_SUBQUERY:
            return true;
        default:
            return false;
    }
}

bool is_allowed_read_ancestor(NodeType type) {
    switch (type) {
        case NodeType::NODE_SELECT_STMT:
        case NodeType::NODE_SELECT_ITEM_LIST:
        case NodeType::NODE_SELECT_ITEM:
        case NodeType::NODE_WHERE_CLAUSE:
        case NodeType::NODE_GROUP_BY_CLAUSE:
        case NodeType::NODE_HAVING_CLAUSE:
        case NodeType::NODE_ORDER_BY_CLAUSE:
        case NodeType::NODE_ORDER_BY_ITEM:
        case NodeType::NODE_LIMIT_CLAUSE:
        case NodeType::NODE_EXPRESSION:
        case NodeType::NODE_BINARY_OP:
        case NodeType::NODE_UNARY_OP:
        case NodeType::NODE_IS_NULL:
        case NodeType::NODE_IS_NOT_NULL:
        case NodeType::NODE_BETWEEN:
        case NodeType::NODE_IN_LIST:
            return true;
        default:
            return false;
    }
}

struct UsageWalk {
    bool found = false;
    bool unsafe = false;
};

void walk_user_variable_usage(const AstNode* node, bool path_is_read_safe,
                              bool write_context, UsageWalk& walk) {
    if (!node || walk.unsafe) return;
    if (is_forbidden_user_variable_context(node->type)) {
        walk.unsafe = true;
        return;
    }

    bool child_write_context = write_context ||
        node->type == NodeType::NODE_VAR_TARGET ||
        node->type == NodeType::NODE_INTO_CLAUSE;

    if (node->type == NodeType::NODE_USER_VARIABLE) {
        walk.found = true;
        if (write_context || !path_is_read_safe) walk.unsafe = true;
        return;
    }

    bool child_path_is_read_safe = path_is_read_safe &&
        is_allowed_read_ancestor(node->type);
    for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
        walk_user_variable_usage(child, child_path_is_read_safe,
                                 child_write_context, walk);
    }
}

} // namespace

UserVariableUsage classify_mysql_user_variable_usage(const ParseResult& result) {
    if (!result.has_user_variables) return UserVariableUsage::NO_USER_VARIABLE;
    if (result.status != ParseResult::OK || !result.full_input || !result.ast) {
        return UserVariableUsage::UNSAFE_OR_UNKNOWN;
    }

    UsageWalk walk;
    walk_user_variable_usage(result.ast, true, false, walk);
    if (walk.unsafe || !walk.found) return UserVariableUsage::UNSAFE_OR_UNKNOWN;
    return UserVariableUsage::READ_ONLY;
}

// ---- Explicit template instantiations ----

template class Parser<Dialect::MySQL>;
template class Parser<Dialect::PostgreSQL>;

} // namespace sql_parser
