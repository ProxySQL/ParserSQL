// One hex-encoded SQL byte string per line; one TSV record per input.
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include <iostream>
#include <string>

using namespace sql_parser;

static int hex_digit(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static void print_hex(StringRef text) {
    static constexpr char digits[] = "0123456789abcdef";
    for (uint32_t i = 0; i < text.len; ++i) {
        const auto c = static_cast<unsigned char>(text.ptr[i]);
        std::cout << digits[c >> 4] << digits[c & 15];
    }
}

int main() {
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.size() % 2) {
            std::cerr << "Odd-length hex input\n";
            return 2;
        }
        std::string sql;
        sql.reserve(line.size() / 2);
        for (size_t i = 0; i < line.size(); i += 2) {
            const int hi = hex_digit(line[i]);
            const int lo = hex_digit(line[i + 1]);
            if (hi < 0 || lo < 0) {
                std::cerr << "Invalid hex input\n";
                return 2;
            }
            sql.push_back(static_cast<char>((hi << 4) | lo));
        }
        // Input, parser arena and emitter remain alive until all spans are printed.
        Parser<Dialect::MySQL> parser;
        const auto result = parser.parse(sql.data(), sql.size());
        std::cout << static_cast<int>(result.status) << '\t' << result.full_input
                  << '\t' << (result.ast != nullptr) << '\t';
        print_hex(result.remaining);
        std::cout << '\t';
        if (result.ast) {
            Emitter<Dialect::MySQL> emitter(parser.arena());
            emitter.emit(result.ast);
            print_hex(emitter.result());
        }
        std::cout << '\n';
    }
    return std::cin.bad() || !std::cout ? 2 : 0;
}
