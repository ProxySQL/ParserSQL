#include "sql_parser/parser.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/set_parser.h"

namespace sql_parser {

template <Dialect D>
Parser<D>::Parser(const ParserConfig& config)
    : arena_(config.arena_block_size, config.arena_max_size) {}

template <Dialect D>
void Parser<D>::reset() {
    arena_.reset();
}

template <Dialect D>
ParseResult Parser<D>::parse(const char* sql, size_t len) {
    arena_.reset();
    tokenizer_.reset(sql, len);
    return classify_and_dispatch();
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

    switch (first.type) {
        case TokenType::TK_SELECT:   return parse_select();
        case TokenType::TK_SET:      return parse_set();
        case TokenType::TK_INSERT:   return extract_insert(first);
        case TokenType::TK_UPDATE:   return extract_update(first);
        case TokenType::TK_DELETE:   return extract_delete(first);
        case TokenType::TK_REPLACE:  return extract_replace(first);
        case TokenType::TK_BEGIN:
        case TokenType::TK_START:
        case TokenType::TK_COMMIT:
        case TokenType::TK_ROLLBACK:
        case TokenType::TK_SAVEPOINT:return extract_transaction(first);
        case TokenType::TK_USE:      return extract_use(first);
        case TokenType::TK_SHOW:     return extract_show(first);
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
        case TokenType::TK_LOAD:     return extract_load(first);
        case TokenType::TK_RESET:    return extract_reset(first);
        default:                     return extract_unknown(first);
    }
}

// ---- Tier 1 stubs ----

template <Dialect D>
ParseResult Parser<D>::parse_select() {
    ParseResult r;
    r.status = ParseResult::PARTIAL;
    r.stmt_type = StmtType::SELECT;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_set() {
    ParseResult r;
    r.stmt_type = StmtType::SET;

    SetParser<D> set_parser(tokenizer_, arena_);
    AstNode* ast = set_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;
    } else {
        r.status = ParseResult::PARTIAL;
    }

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
    while (true) {
        Token t = tokenizer_.next_token();
        if (t.type == TokenType::TK_EOF) break;
        if (t.type == TokenType::TK_SEMICOLON) {
            Token next = tokenizer_.peek();
            if (next.type != TokenType::TK_EOF) {
                const char* remaining_start = next.text.ptr;
                const char* input_end = tokenizer_.input_end();
                result.remaining = StringRef{
                    remaining_start,
                    static_cast<uint32_t>(input_end - remaining_start)
                };
            }
            break;
        }
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
    ParseResult r;
    r.status = ParseResult::OK;

    switch (first.type) {
        case TokenType::TK_BEGIN:
            r.stmt_type = StmtType::BEGIN;
            break;
        case TokenType::TK_START:
            r.stmt_type = StmtType::START_TRANSACTION;
            // consume TRANSACTION if present
            if (tokenizer_.peek().type == TokenType::TK_TRANSACTION)
                tokenizer_.skip();
            break;
        case TokenType::TK_COMMIT:
            r.stmt_type = StmtType::COMMIT;
            break;
        case TokenType::TK_ROLLBACK:
            r.stmt_type = StmtType::ROLLBACK;
            break;
        case TokenType::TK_SAVEPOINT:
            r.stmt_type = StmtType::SAVEPOINT;
            break;
        default:
            r.stmt_type = StmtType::UNKNOWN;
            break;
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
ParseResult Parser<D>::extract_load(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::LOAD_DATA;
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

// ---- Explicit template instantiations ----

template class Parser<Dialect::MySQL>;
template class Parser<Dialect::PostgreSQL>;

} // namespace sql_parser
