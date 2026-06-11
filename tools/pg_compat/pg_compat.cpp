#include "sql_parser/parser.h"
#include "tools/pg_compat/result.h"
#include "tools/pg_compat/statement_type_map.h"

#include "pg_query.h"
#include "protobuf/pg_query.pb-c.h"

#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using pg_compat::CompatibilityResult;
using pg_compat::MappingKind;
using pg_compat::StatementTypeMapping;

constexpr const char* USAGE =
    "Usage: pg_compat_runner --input FILE --branch NAME --commit SHA\n";

class UsageError : public std::runtime_error {
public:
    UsageError() : std::runtime_error("invalid command line") {}
};

struct Options {
    std::string input;
    std::string branch;
    std::string commit;
};

struct Candidate {
    std::size_t offset = 0;
    std::size_t line = 1;
    std::string splitter;
    std::string sql;
};

class PgQueryLifecycle {
public:
    PgQueryLifecycle() = default;

    ~PgQueryLifecycle() {
        if (initialized_) {
            pg_query_exit();
        }
    }

    PgQueryLifecycle(const PgQueryLifecycle&) = delete;
    PgQueryLifecycle& operator=(const PgQueryLifecycle&) = delete;

    void mark_initialized() {
        initialized_ = true;
    }

private:
    bool initialized_ = false;
};

class SplitResultOwner {
public:
    explicit SplitResultOwner(PgQuerySplitResult result)
        : result_(result), owns_(true) {}

    ~SplitResultOwner() {
        reset();
    }

    SplitResultOwner(const SplitResultOwner&) = delete;
    SplitResultOwner& operator=(const SplitResultOwner&) = delete;

    const PgQuerySplitResult& get() const {
        return result_;
    }

    void reset() {
        if (owns_) {
            pg_query_free_split_result(result_);
            owns_ = false;
        }
    }

private:
    PgQuerySplitResult result_{};
    bool owns_ = false;
};

class ScanResultOwner {
public:
    explicit ScanResultOwner(PgQueryScanResult result) : result_(result) {}

    ~ScanResultOwner() {
        pg_query_free_scan_result(result_);
    }

    ScanResultOwner(const ScanResultOwner&) = delete;
    ScanResultOwner& operator=(const ScanResultOwner&) = delete;

    const PgQueryScanResult& get() const {
        return result_;
    }

private:
    PgQueryScanResult result_{};
};

class ParseResultOwner {
public:
    explicit ParseResultOwner(PgQueryProtobufParseResult result)
        : result_(result) {}

    ~ParseResultOwner() {
        pg_query_free_protobuf_parse_result(result_);
    }

    ParseResultOwner(const ParseResultOwner&) = delete;
    ParseResultOwner& operator=(const ParseResultOwner&) = delete;

    const PgQueryProtobufParseResult& get() const {
        return result_;
    }

private:
    PgQueryProtobufParseResult result_{};
};

struct ScanUnpackedDeleter {
    void operator()(PgQuery__ScanResult* result) const {
        if (result != nullptr) {
            pg_query__scan_result__free_unpacked(result, nullptr);
        }
    }
};

struct ParseUnpackedDeleter {
    void operator()(PgQuery__ParseResult* result) const {
        if (result != nullptr) {
            pg_query__parse_result__free_unpacked(result, nullptr);
        }
    }
};

using ScanUnpackedPtr =
    std::unique_ptr<PgQuery__ScanResult, ScanUnpackedDeleter>;
using ParseUnpackedPtr =
    std::unique_ptr<PgQuery__ParseResult, ParseUnpackedDeleter>;

Options parse_options(int argc, char** argv) {
    Options options;
    bool has_input = false;
    bool has_branch = false;
    bool has_commit = false;

    for (int i = 1; i < argc;) {
        std::string_view option(argv[i]);
        std::string* destination = nullptr;
        bool* seen = nullptr;

        if (option == "--input") {
            destination = &options.input;
            seen = &has_input;
        } else if (option == "--branch") {
            destination = &options.branch;
            seen = &has_branch;
        } else if (option == "--commit") {
            destination = &options.commit;
            seen = &has_commit;
        } else {
            throw UsageError();
        }

        if (*seen || i + 1 >= argc) {
            throw UsageError();
        }
        const std::string_view value(argv[i + 1]);
        if (value.empty() || value.substr(0, 2) == "--") {
            throw UsageError();
        }
        *seen = true;
        *destination = value;
        i += 2;
    }

    if (!has_input || !has_branch || !has_commit) {
        throw UsageError();
    }
    return options;
}

std::string read_input(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open input file: " + path);
    }

    std::string contents{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    if (input.bad()) {
        throw std::runtime_error("failed while reading input file: " + path);
    }

    const std::size_t nul_offset = contents.find('\0');
    if (nul_offset != std::string::npos) {
        throw std::runtime_error(
            "input contains a NUL byte at offset " +
            std::to_string(nul_offset));
    }
    return contents;
}

void append_byte_escape(std::string& output, unsigned char value) {
    static constexpr char HEX[] = "0123456789abcdef";
    output += "\\u00";
    output.push_back(HEX[value >> 4]);
    output.push_back(HEX[value & 0x0f]);
}

std::size_t valid_utf8_sequence_length(std::string_view input,
                                       std::size_t offset) {
    const auto byte = [&](std::size_t index) {
        return static_cast<unsigned char>(input[index]);
    };
    const unsigned char first = byte(offset);
    std::size_t length = 0;

    if (first >= 0xc2 && first <= 0xdf) {
        length = 2;
    } else if (first >= 0xe0 && first <= 0xef) {
        length = 3;
    } else if (first >= 0xf0 && first <= 0xf4) {
        length = 4;
    } else {
        return 0;
    }
    if (offset + length > input.size()) {
        return 0;
    }
    for (std::size_t i = 1; i < length; ++i) {
        if ((byte(offset + i) & 0xc0) != 0x80) {
            return 0;
        }
    }

    const unsigned char second = byte(offset + 1);
    if ((first == 0xe0 && second < 0xa0) ||
        (first == 0xed && second >= 0xa0) ||
        (first == 0xf0 && second < 0x90) ||
        (first == 0xf4 && second >= 0x90)) {
        return 0;
    }
    return length;
}

std::string json_escape(std::string_view input) {
    std::string output;
    output.reserve(input.size());

    for (std::size_t i = 0; i < input.size(); ++i) {
        const unsigned char value = static_cast<unsigned char>(input[i]);
        switch (value) {
        case '"':
            output += "\\\"";
            break;
        case '\\':
            output += "\\\\";
            break;
        case '\b':
            output += "\\b";
            break;
        case '\f':
            output += "\\f";
            break;
        case '\n':
            output += "\\n";
            break;
        case '\r':
            output += "\\r";
            break;
        case '\t':
            output += "\\t";
            break;
        default:
            if (value < 0x20) {
                append_byte_escape(output, value);
            } else if (value < 0x80) {
                output.push_back(static_cast<char>(value));
            } else {
                const std::size_t length =
                    valid_utf8_sequence_length(input, i);
                if (length == 0) {
                    append_byte_escape(output, value);
                } else {
                    output.append(input.substr(i, length));
                    i += length - 1;
                }
            }
        }
    }
    return output;
}

std::string oracle_error_message(const PgQueryError* error) {
    if (error == nullptr || error->message == nullptr) {
        return "unknown libpg_query error";
    }
    return error->message;
}

std::size_t line_for_offset(std::string_view input, std::size_t offset) {
    if (offset > input.size()) {
        throw std::runtime_error("statement offset exceeds input size");
    }

    std::size_t line = 1;
    for (std::size_t i = 0; i < offset; ++i) {
        if (input[i] == '\n') {
            ++line;
        }
    }
    return line;
}

std::vector<Candidate> extract_candidates(
    std::string_view input,
    const PgQuerySplitResult& split,
    const char* splitter) {
    if (split.n_stmts < 0) {
        throw std::runtime_error("splitter returned a negative statement count");
    }
    if (split.n_stmts > 0 && split.stmts == nullptr) {
        throw std::runtime_error("splitter returned no statement array");
    }

    std::vector<Candidate> candidates;
    candidates.reserve(static_cast<std::size_t>(split.n_stmts));
    for (int i = 0; i < split.n_stmts; ++i) {
        const PgQuerySplitStmt* statement = split.stmts[i];
        if (statement == nullptr) {
            throw std::runtime_error("splitter returned a null statement");
        }
        if (statement->stmt_location < 0 || statement->stmt_len < 0) {
            throw std::runtime_error("splitter returned a negative byte range");
        }

        const std::size_t offset =
            static_cast<std::size_t>(statement->stmt_location);
        if (offset > input.size()) {
            throw std::runtime_error("splitter statement offset is out of range");
        }

        const std::size_t length = statement->stmt_len == 0
            ? input.size() - offset
            : static_cast<std::size_t>(statement->stmt_len);
        if (length > input.size() - offset) {
            throw std::runtime_error("splitter statement length is out of range");
        }

        candidates.push_back({
            offset,
            line_for_offset(input, offset),
            splitter,
            std::string(input.substr(offset, length)),
        });
    }
    return candidates;
}

std::vector<Candidate> split_candidates(
    std::string_view input,
    PgQueryLifecycle& lifecycle) {
    std::string owned_input(input);
    lifecycle.mark_initialized();
    SplitResultOwner parser_split(
        pg_query_split_with_parser(owned_input.c_str()));
    if (parser_split.get().error == nullptr) {
        return extract_candidates(input, parser_split.get(), "parser");
    }

    const std::string parser_error =
        oracle_error_message(parser_split.get().error);
    parser_split.reset();

    SplitResultOwner scanner_split(
        pg_query_split_with_scanner(owned_input.c_str()));
    if (scanner_split.get().error != nullptr) {
        throw std::runtime_error(
            "statement splitting failed; parser: " + parser_error +
            "; scanner: " +
            oracle_error_message(scanner_split.get().error));
    }
    return extract_candidates(input, scanner_split.get(), "scanner");
}

std::string normalize_sql(std::string_view sql) {
    std::string owned_sql(sql);
    ScanResultOwner raw(pg_query_scan(owned_sql.c_str()));
    if (raw.get().error != nullptr) {
        throw std::runtime_error(
            "oracle scanner rejected candidate: " +
            oracle_error_message(raw.get().error));
    }
    if (raw.get().pbuf.len > 0 && raw.get().pbuf.data == nullptr) {
        throw std::runtime_error("oracle scanner returned an empty buffer");
    }

    ScanUnpackedPtr scan(pg_query__scan_result__unpack(
        nullptr,
        raw.get().pbuf.len,
        reinterpret_cast<const uint8_t*>(raw.get().pbuf.data)));
    if (scan == nullptr) {
        throw std::runtime_error("unable to unpack oracle scan protobuf");
    }
    if (scan->n_tokens > 0 && scan->tokens == nullptr) {
        throw std::runtime_error("oracle scanner returned no token array");
    }

    std::string normalized;
    for (std::size_t i = 0; i < scan->n_tokens; ++i) {
        const PgQuery__ScanToken* token = scan->tokens[i];
        if (token == nullptr || token->start < 0 || token->end < token->start) {
            throw std::runtime_error("oracle scanner returned an invalid token");
        }

        const std::size_t start = static_cast<std::size_t>(token->start);
        const std::size_t end = static_cast<std::size_t>(token->end);
        if (start > sql.size() || end > sql.size()) {
            throw std::runtime_error(
                "oracle scanner token range is out of bounds");
        }

        std::string text(sql.substr(start, end - start));
        if (token->keyword_kind != PG_QUERY__KEYWORD_KIND__NO_KEYWORD) {
            for (char& c : text) {
                if (c >= 'a' && c <= 'z') {
                    c = static_cast<char>(c - ('a' - 'A'));
                }
            }
        }
        if (!normalized.empty()) {
            normalized.push_back(' ');
        }
        normalized += text;
    }
    return normalized;
}

bool has_meaningful_remaining(sql_parser::StringRef remaining) {
    if (remaining.len > 0 && remaining.ptr == nullptr) {
        throw std::runtime_error("ParserSQL returned an invalid remaining view");
    }
    for (uint32_t i = 0; i < remaining.len; ++i) {
        const char c = remaining.ptr[i];
        const bool is_whitespace =
            c == ' ' || c == '\t' || c == '\n' ||
            c == '\r' || c == '\f' || c == '\v';
        if (!is_whitespace && c != ';') {
            return true;
        }
    }
    return false;
}

std::string copy_remaining(sql_parser::StringRef remaining) {
    if (remaining.len == 0) {
        return {};
    }
    if (remaining.ptr == nullptr) {
        throw std::runtime_error("ParserSQL returned an invalid remaining view");
    }
    return std::string(remaining.ptr, remaining.len);
}

const char* parser_status_name(sql_parser::ParseResult::Status status) {
    switch (status) {
    case sql_parser::ParseResult::OK:
        return "OK";
    case sql_parser::ParseResult::PARTIAL:
        return "PARTIAL";
    case sql_parser::ParseResult::ERROR:
        return "ERROR";
    }
    return "ERROR";
}

std::string generated_node_case_name(PgQuery__Node__NodeCase node_case) {
    const unsigned case_number = static_cast<unsigned>(node_case);
    for (unsigned i = 0; i < pg_query__node__descriptor.n_fields; ++i) {
        const ProtobufCFieldDescriptor& field =
            pg_query__node__descriptor.fields[i];
        if (field.id != case_number) {
            continue;
        }

        std::string name = "PG_QUERY__NODE__NODE_";
        for (const char* c = field.name; *c != '\0'; ++c) {
            name.push_back(
                *c >= 'a' && *c <= 'z'
                    ? static_cast<char>(*c - ('a' - 'A'))
                    : *c);
        }
        return name;
    }
    return "UNKNOWN_NODE_CASE_" + std::to_string(case_number);
}

CompatibilityResult classify(
    const sql_parser::ParseResult& parsed,
    const StatementTypeMapping& mapping,
    std::string_view oracle_node) {
    if (parsed.status == sql_parser::ParseResult::ERROR) {
        return CompatibilityResult::Error;
    }
    if (parsed.status == sql_parser::ParseResult::PARTIAL) {
        return CompatibilityResult::Partial;
    }
    if (mapping.kind == MappingKind::Unmapped) {
        throw std::runtime_error(
            "unmapped PostgreSQL top-level node: " +
            std::string(oracle_node));
    }
    if (mapping.kind == MappingKind::NoEquivalent ||
        parsed.stmt_type != mapping.type) {
        return CompatibilityResult::TypeMismatch;
    }
    if (has_meaningful_remaining(parsed.remaining)) {
        return CompatibilityResult::TrailingInput;
    }
    return parsed.ast != nullptr
        ? CompatibilityResult::DeepSupported
        : CompatibilityResult::ClassifiedOnly;
}

void emit_string_field(const char* name,
                       std::string_view value,
                       bool& first) {
    if (!first) {
        std::cout << ',';
    }
    first = false;
    std::cout << '"' << name << "\":\"" << json_escape(value) << '"';
}

void emit_size_field(const char* name, std::size_t value, bool& first) {
    if (!first) {
        std::cout << ',';
    }
    first = false;
    std::cout << '"' << name << "\":" << value;
}

void emit_bool_field(const char* name, bool value, bool& first) {
    if (!first) {
        std::cout << ',';
    }
    first = false;
    std::cout << '"' << name << "\":" << (value ? "true" : "false");
}

void emit_oracle_rejected(const Options& options,
                          const Candidate& candidate,
                          std::string_view message) {
    bool first = true;
    std::cout << '{';
    emit_string_field("source_file", options.input, first);
    emit_size_field("offset", candidate.offset, first);
    emit_size_field("line", candidate.line, first);
    emit_string_field("splitter", candidate.splitter, first);
    emit_string_field("sql", candidate.sql, first);
    emit_string_field(
        "result",
        pg_compat::result_name(CompatibilityResult::OracleRejected),
        first);
    emit_string_field("branch", options.branch, first);
    emit_string_field("commit", options.commit, first);
    emit_string_field("oracle_error", message, first);
    std::cout << "}\n";
}

void emit_accepted(const Options& options,
                   const Candidate& candidate,
                   std::string_view normalized,
                   std::string_view oracle_node,
                   const StatementTypeMapping& mapping,
                   const sql_parser::ParseResult& parsed,
                   std::string_view remaining,
                   CompatibilityResult result) {
    bool first = true;
    std::cout << '{';
    emit_string_field("source_file", options.input, first);
    emit_size_field("offset", candidate.offset, first);
    emit_size_field("line", candidate.line, first);
    emit_string_field("splitter", candidate.splitter, first);
    emit_string_field("sql", candidate.sql, first);
    emit_string_field("normalized_sql", normalized, first);
    emit_string_field("oracle_node", oracle_node, first);
    emit_string_field(
        "expected_stmt_type",
        pg_compat::stmt_type_name(mapping.type),
        first);
    emit_string_field(
        "parser_status",
        parser_status_name(parsed.status),
        first);
    emit_string_field(
        "parser_stmt_type",
        pg_compat::stmt_type_name(parsed.stmt_type),
        first);
    emit_bool_field("has_ast", parsed.ast != nullptr, first);
    emit_string_field("remaining", remaining, first);
    emit_string_field("result", pg_compat::result_name(result), first);
    emit_string_field("branch", options.branch, first);
    emit_string_field("commit", options.commit, first);
    std::cout << "}\n";
}

void run(const Options& options, PgQueryLifecycle& lifecycle) {
    const std::string input = read_input(options.input);
    const std::vector<Candidate> candidates =
        split_candidates(input, lifecycle);
    sql_parser::Parser<sql_parser::Dialect::PostgreSQL> parser;

    for (const Candidate& candidate : candidates) {
        ParseResultOwner raw(pg_query_parse_protobuf(candidate.sql.c_str()));
        if (raw.get().error != nullptr) {
            emit_oracle_rejected(
                options,
                candidate,
                oracle_error_message(raw.get().error));
            continue;
        }
        if (raw.get().parse_tree.len > 0 &&
            raw.get().parse_tree.data == nullptr) {
            throw std::runtime_error("oracle parser returned an empty buffer");
        }

        ParseUnpackedPtr parsed_oracle(pg_query__parse_result__unpack(
            nullptr,
            raw.get().parse_tree.len,
            reinterpret_cast<const uint8_t*>(
                raw.get().parse_tree.data)));
        if (parsed_oracle == nullptr ||
            parsed_oracle->n_stmts != 1 ||
            parsed_oracle->stmts == nullptr ||
            parsed_oracle->stmts[0] == nullptr ||
            parsed_oracle->stmts[0]->stmt == nullptr) {
            throw std::runtime_error(
                "oracle did not produce exactly one RawStmt");
        }

        const PgQuery__Node& root = *parsed_oracle->stmts[0]->stmt;
        const StatementTypeMapping mapping =
            pg_compat::expected_stmt_type(root);
        const std::string oracle_node =
            pg_compat::oracle_node_name(root.node_case);
        const std::string node_case_name =
            generated_node_case_name(root.node_case);
        const std::string normalized = normalize_sql(candidate.sql);

        const sql_parser::ParseResult parsed =
            parser.parse(candidate.sql.data(), candidate.sql.size());
        const std::string remaining = copy_remaining(parsed.remaining);
        const CompatibilityResult result =
            classify(parsed, mapping, node_case_name);
        emit_accepted(
            options,
            candidate,
            normalized,
            oracle_node,
            mapping,
            parsed,
            remaining,
            result);
        parser.reset();
    }
}

} // namespace

int main(int argc, char** argv) {
    PgQueryLifecycle lifecycle;
    try {
        const Options options = parse_options(argc, argv);
        run(options, lifecycle);
        return 0;
    } catch (const UsageError&) {
        std::cerr << USAGE;
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "infrastructure error: " << error.what() << '\n';
        return 1;
    }
}
