// mysql_server.cpp -- MySQL wire protocol server wrapping our Session<D> API
//
// Implements a minimal MySQL text protocol server so that standard MySQL
// clients (mysql CLI, libmysqlclient, JDBC, etc.) can connect to our engine.
//
// Supports:
//   - MySQL handshake + auth (accepts any credentials)
//   - COM_QUERY (text protocol)
//   - COM_QUIT
//   - COM_PING
//   - COM_INIT_DB
//
// Does NOT support:
//   - SSL/TLS
//   - Prepared statements (COM_STMT_PREPARE)
//   - Compression
//   - Authentication plugins (always OK)
//
// Usage:
//   ./mysql_server --port 13309 \
//       --backend "mysql://root:test@127.0.0.1:13306/vt_testks?name=shard1" \
//       --backend "mysql://root:test@127.0.0.1:13307/vt_testks?name=shard2" \
//       --shard "users:id:shard1,shard2" \
//       --shard "orders:user_id:shard1,shard2"

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <algorithm>
#include <atomic>
#include <thread>
#include <random>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>

#include "sql_parser/parser.h"
#include "sql_parser/common.h"
#include "sql_engine/session.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_engine/local_txn.h"
#include "sql_engine/multi_remote_executor.h"
#include "sql_engine/thread_safe_executor.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/backend_config.h"
#include "sql_engine/tool_config_parser.h"
#include "sql_engine/result_set.h"
#include "sql_engine/dml_result.h"
#include "sql_engine/value.h"
#include "sql_engine/datetime_parse.h"

using namespace sql_parser;
using namespace sql_engine;

// ============================================================
// Value to string for MySQL result formatting
// ============================================================

static std::string value_to_string(const Value& v) {
    switch (v.tag) {
        case Value::TAG_NULL:
            return "";  // NULL handled separately
        case Value::TAG_BOOL:
            return v.bool_val ? "1" : "0";
        case Value::TAG_INT64:
            return std::to_string(v.int_val);
        case Value::TAG_UINT64:
            return std::to_string(v.uint_val);
        case Value::TAG_DOUBLE: {
            std::ostringstream oss;
            oss << v.double_val;
            return oss.str();
        }
        case Value::TAG_STRING:
        case Value::TAG_DECIMAL:
        case Value::TAG_JSON:
            if (v.str_val.ptr && v.str_val.len > 0)
                return std::string(v.str_val.ptr, v.str_val.len);
            return "";
        case Value::TAG_BYTES:
            if (v.str_val.ptr && v.str_val.len > 0)
                return std::string(v.str_val.ptr, v.str_val.len);
            return "";
        case Value::TAG_DATE: {
            char buf[16];
            size_t n = sql_engine::datetime_parse::format_date(v.date_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        case Value::TAG_TIME: {
            char buf[32];
            size_t n = sql_engine::datetime_parse::format_time(v.time_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        case Value::TAG_DATETIME: {
            char buf[32];
            size_t n = sql_engine::datetime_parse::format_datetime(v.datetime_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        case Value::TAG_TIMESTAMP: {
            char buf[32];
            size_t n = sql_engine::datetime_parse::format_datetime(v.timestamp_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        default:
            return "";
    }
}

// ============================================================
// MySQL Wire Protocol helpers
// ============================================================

// MySQL capability flags (prefixed MY_ to avoid conflict with mysql headers)
static constexpr uint32_t MY_CLIENT_LONG_PASSWORD    = 0x00000001;
static constexpr uint32_t MY_CLIENT_FOUND_ROWS       = 0x00000002;
static constexpr uint32_t MY_CLIENT_LONG_FLAG        = 0x00000004;
static constexpr uint32_t MY_CLIENT_CONNECT_WITH_DB  = 0x00000008;
static constexpr uint32_t MY_CLIENT_PROTOCOL_41      = 0x00000200;
static constexpr uint32_t MY_CLIENT_SECURE_CONNECTION = 0x00008000;
static constexpr uint32_t MY_CLIENT_PLUGIN_AUTH       = 0x00080000;

// Server status flags
static constexpr uint16_t MY_SERVER_STATUS_AUTOCOMMIT = 0x0002;

// MySQL column types
static constexpr uint8_t MY_TYPE_DECIMAL    = 0x00;
static constexpr uint8_t MY_TYPE_DOUBLE     = 0x05;
static constexpr uint8_t MY_TYPE_NULL       = 0x06;
static constexpr uint8_t MY_TYPE_TIMESTAMP  = 0x07;
static constexpr uint8_t MY_TYPE_LONGLONG   = 0x08;
static constexpr uint8_t MY_TYPE_DATE       = 0x0A;
static constexpr uint8_t MY_TYPE_TIME       = 0x0B;
static constexpr uint8_t MY_TYPE_DATETIME   = 0x0C;
static constexpr uint8_t MY_TYPE_VAR_STRING = 0xFD;

// COM_* command bytes
static constexpr uint8_t MY_COM_QUIT     = 0x01;
static constexpr uint8_t MY_COM_INIT_DB  = 0x02;
static constexpr uint8_t MY_COM_QUERY    = 0x03;
static constexpr uint8_t MY_COM_PING     = 0x0E;

// Global stop flag
static std::atomic<bool> g_stop{false};

// ============================================================
// Packet I/O
// ============================================================

struct Connection {
    int fd;
    uint8_t seq_id;

    explicit Connection(int fd_) : fd(fd_), seq_id(0) {}

    // Read exactly n bytes
    bool read_exact(void* buf, size_t n) {
        uint8_t* p = static_cast<uint8_t*>(buf);
        size_t remaining = n;
        while (remaining > 0) {
            ssize_t r = ::read(fd, p, remaining);
            if (r <= 0) return false;
            p += r;
            remaining -= static_cast<size_t>(r);
        }
        return true;
    }

    // Write exactly n bytes
    bool write_exact(const void* buf, size_t n) {
        const uint8_t* p = static_cast<const uint8_t*>(buf);
        size_t remaining = n;
        while (remaining > 0) {
            ssize_t w = ::write(fd, p, remaining);
            if (w <= 0) return false;
            p += w;
            remaining -= static_cast<size_t>(w);
        }
        return true;
    }

    // Read a MySQL packet: 3-byte length + 1-byte seq + payload
    bool read_packet(std::vector<uint8_t>& payload) {
        uint8_t header[4];
        if (!read_exact(header, 4)) return false;

        uint32_t len = static_cast<uint32_t>(header[0])
                     | (static_cast<uint32_t>(header[1]) << 8)
                     | (static_cast<uint32_t>(header[2]) << 16);
        seq_id = header[3];

        payload.resize(len);
        if (len > 0 && !read_exact(payload.data(), len)) return false;
        return true;
    }

    // Write a MySQL packet
    bool write_packet(const uint8_t* data, size_t len) {
        uint8_t header[4];
        header[0] = static_cast<uint8_t>(len & 0xFF);
        header[1] = static_cast<uint8_t>((len >> 8) & 0xFF);
        header[2] = static_cast<uint8_t>((len >> 16) & 0xFF);
        header[3] = seq_id++;

        if (!write_exact(header, 4)) return false;
        if (len > 0 && !write_exact(data, len)) return false;
        return true;
    }

    bool write_packet(const std::vector<uint8_t>& data) {
        return write_packet(data.data(), data.size());
    }
};

// ============================================================
// Packet builders
// ============================================================

// Write length-encoded integer
static void write_lenenc_int(std::vector<uint8_t>& buf, uint64_t val) {
    if (val < 251) {
        buf.push_back(static_cast<uint8_t>(val));
    } else if (val < 65536) {
        buf.push_back(0xFC);
        buf.push_back(static_cast<uint8_t>(val & 0xFF));
        buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
    } else if (val < 16777216) {
        buf.push_back(0xFD);
        buf.push_back(static_cast<uint8_t>(val & 0xFF));
        buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
    } else {
        buf.push_back(0xFE);
        for (int i = 0; i < 8; i++) {
            buf.push_back(static_cast<uint8_t>((val >> (i * 8)) & 0xFF));
        }
    }
}

// Write length-encoded string
static void write_lenenc_string(std::vector<uint8_t>& buf, const char* s, size_t len) {
    write_lenenc_int(buf, len);
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(s),
               reinterpret_cast<const uint8_t*>(s) + len);
}

static void write_lenenc_string(std::vector<uint8_t>& buf, const std::string& s) {
    write_lenenc_string(buf, s.c_str(), s.size());
}

// Write null-terminated string
static void write_null_string(std::vector<uint8_t>& buf, const char* s) {
    size_t len = std::strlen(s);
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(s),
               reinterpret_cast<const uint8_t*>(s) + len);
    buf.push_back(0);
}

// Write fixed-length integer (little-endian)
static void write_fixed_int(std::vector<uint8_t>& buf, uint32_t val, int bytes) {
    for (int i = 0; i < bytes; i++) {
        buf.push_back(static_cast<uint8_t>((val >> (i * 8)) & 0xFF));
    }
}

// ============================================================
// Handshake
// ============================================================

static bool send_handshake(Connection& conn, uint32_t connection_id) {
    conn.seq_id = 0;

    std::vector<uint8_t> pkt;
    pkt.reserve(128);

    // Protocol version: 10
    pkt.push_back(10);

    // Server version string (null-terminated)
    write_null_string(pkt, "ParserSQL 1.0");

    // Connection ID (4 bytes, little-endian)
    write_fixed_int(pkt, connection_id, 4);

    // Auth plugin data part 1 (8 bytes) + filler 0x00
    std::mt19937 rng(static_cast<uint32_t>(connection_id) ^ 0xDEADBEEFu);
    uint8_t auth_data[20];
    for (int i = 0; i < 20; i++) {
        auth_data[i] = static_cast<uint8_t>(rng() & 0xFF);
        if (auth_data[i] == 0) auth_data[i] = 1; // avoid null bytes
    }
    pkt.insert(pkt.end(), auth_data, auth_data + 8);
    pkt.push_back(0x00); // filler

    // Capability flags lower 2 bytes
    uint32_t caps = MY_CLIENT_LONG_PASSWORD | MY_CLIENT_FOUND_ROWS | MY_CLIENT_LONG_FLAG
                  | MY_CLIENT_CONNECT_WITH_DB | MY_CLIENT_PROTOCOL_41
                  | MY_CLIENT_SECURE_CONNECTION | MY_CLIENT_PLUGIN_AUTH;
    write_fixed_int(pkt, caps & 0xFFFF, 2);

    // Character set: utf8 general ci = 33
    pkt.push_back(33);

    // Status flags (2 bytes)
    write_fixed_int(pkt, MY_SERVER_STATUS_AUTOCOMMIT, 2);

    // Capability flags upper 2 bytes
    write_fixed_int(pkt, (caps >> 16) & 0xFFFF, 2);

    // Length of auth plugin data (1 byte): total 20 + 1 for null
    pkt.push_back(21);

    // Reserved (10 bytes of 0x00)
    for (int i = 0; i < 10; i++) pkt.push_back(0x00);

    // Auth plugin data part 2 (12 data bytes + null terminator)
    pkt.insert(pkt.end(), auth_data + 8, auth_data + 20);
    pkt.push_back(0x00); // null terminator

    // Auth plugin name (null-terminated)
    write_null_string(pkt, "mysql_native_password");

    return conn.write_packet(pkt);
}

static bool receive_auth_response(Connection& conn) {
    std::vector<uint8_t> payload;
    if (!conn.read_packet(payload)) return false;
    // We accept any auth response
    return true;
}

// ============================================================
// OK and Error packets
// ============================================================

static bool send_ok(Connection& conn, uint64_t affected_rows = 0,
                     uint64_t last_insert_id = 0, uint16_t status = MY_SERVER_STATUS_AUTOCOMMIT) {
    std::vector<uint8_t> pkt;
    pkt.reserve(32);

    pkt.push_back(0x00); // OK marker
    write_lenenc_int(pkt, affected_rows);
    write_lenenc_int(pkt, last_insert_id);

    // Status flags (2 bytes)
    write_fixed_int(pkt, status, 2);
    // Warnings (2 bytes)
    write_fixed_int(pkt, 0, 2);

    return conn.write_packet(pkt);
}

static bool send_error(Connection& conn, uint16_t error_code,
                        const std::string& sql_state, const std::string& message) {
    std::vector<uint8_t> pkt;
    pkt.reserve(32 + message.size());

    pkt.push_back(0xFF); // ERR marker
    write_fixed_int(pkt, error_code, 2);

    // SQL state marker + 5-char state
    pkt.push_back('#');
    std::string state = sql_state;
    while (state.size() < 5) state += ' ';
    pkt.insert(pkt.end(), state.begin(), state.begin() + 5);

    // Error message (rest of packet, no length prefix)
    pkt.insert(pkt.end(), message.begin(), message.end());

    return conn.write_packet(pkt);
}

// ============================================================
// EOF packet
// ============================================================

static bool send_eof(Connection& conn, uint16_t warnings = 0,
                      uint16_t status = MY_SERVER_STATUS_AUTOCOMMIT) {
    std::vector<uint8_t> pkt;
    pkt.push_back(0xFE); // EOF marker
    write_fixed_int(pkt, warnings, 2);
    write_fixed_int(pkt, status, 2);
    return conn.write_packet(pkt);
}

// ============================================================
// Column definition packet (COM_QUERY response)
// ============================================================

static uint8_t sql_type_to_mysql(const Value& v) {
    switch (v.tag) {
        case Value::TAG_INT64:
        case Value::TAG_UINT64:
            return MY_TYPE_LONGLONG;
        case Value::TAG_DOUBLE:
            return MY_TYPE_DOUBLE;
        case Value::TAG_DECIMAL:
            return MY_TYPE_DECIMAL;
        case Value::TAG_NULL:
            return MY_TYPE_NULL;
        case Value::TAG_DATE:
            return MY_TYPE_DATE;
        case Value::TAG_TIME:
            return MY_TYPE_TIME;
        case Value::TAG_DATETIME:
            return MY_TYPE_DATETIME;
        case Value::TAG_TIMESTAMP:
            return MY_TYPE_TIMESTAMP;
        default:
            return MY_TYPE_VAR_STRING;
    }
}

static bool send_column_def(Connection& conn, const std::string& name, uint8_t mysql_type) {
    std::vector<uint8_t> pkt;
    pkt.reserve(64 + name.size());

    // catalog: "def"
    write_lenenc_string(pkt, "def");
    // schema: ""
    write_lenenc_string(pkt, "");
    // table: ""
    write_lenenc_string(pkt, "");
    // org_table: ""
    write_lenenc_string(pkt, "");
    // name
    write_lenenc_string(pkt, name);
    // org_name
    write_lenenc_string(pkt, name);

    // Fixed-length fields marker (0x0C = 12 bytes follow)
    pkt.push_back(0x0C);

    // Character set: utf8 general ci = 33 (2 bytes)
    write_fixed_int(pkt, 33, 2);

    // Column length (4 bytes)
    write_fixed_int(pkt, 255, 4);

    // Column type (1 byte)
    pkt.push_back(mysql_type);

    // Flags (2 bytes)
    write_fixed_int(pkt, 0, 2);

    // Decimals (1 byte)
    pkt.push_back(0);

    // Filler (2 bytes)
    write_fixed_int(pkt, 0, 2);

    return conn.write_packet(pkt);
}

// ============================================================
// Result set row (text protocol)
// ============================================================

static bool send_text_row(Connection& conn, const Row& row, uint16_t ncols) {
    std::vector<uint8_t> pkt;
    pkt.reserve(256);

    for (uint16_t c = 0; c < ncols; c++) {
        if (c < row.column_count && !row.get(c).is_null()) {
            std::string val = value_to_string(row.get(c));
            write_lenenc_string(pkt, val);
        } else {
            // NULL is encoded as 0xFB
            pkt.push_back(0xFB);
        }
    }

    return conn.write_packet(pkt);
}

// ============================================================
// Send full result set
// ============================================================

static bool send_result_set(Connection& conn, const ResultSet& rs) {
    uint16_t ncols = rs.column_count;
    if (ncols == 0) ncols = static_cast<uint16_t>(rs.column_names.size());
    if (ncols == 0 && !rs.rows.empty()) ncols = rs.rows[0].column_count;
    if (ncols == 0) {
        // Empty result with no columns - send OK instead
        return send_ok(conn);
    }

    // 1. Column count packet
    std::vector<uint8_t> count_pkt;
    write_lenenc_int(count_pkt, ncols);
    if (!conn.write_packet(count_pkt)) return false;

    // 2. Column definition packets
    for (uint16_t c = 0; c < ncols; c++) {
        std::string name;
        if (c < rs.column_names.size()) {
            name = rs.column_names[c];
        } else {
            name = "col" + std::to_string(c);
        }

        uint8_t mysql_type = MY_TYPE_VAR_STRING;
        if (!rs.rows.empty() && c < rs.rows[0].column_count) {
            mysql_type = sql_type_to_mysql(rs.rows[0].get(c));
        }

        if (!send_column_def(conn, name, mysql_type)) return false;
    }

    // 3. EOF packet (end of column definitions)
    if (!send_eof(conn)) return false;

    // 4. Row data packets
    for (auto& row : rs.rows) {
        if (!send_text_row(conn, row, ncols)) return false;
    }

    // 5. EOF packet (end of rows)
    if (!send_eof(conn)) return false;

    return true;
}

// ============================================================
// Determine if a statement is a query (SELECT/SHOW/etc.)
// ============================================================

static bool is_query_statement(StmtType st) {
    switch (st) {
        case StmtType::SELECT:
        case StmtType::SHOW:
        case StmtType::DESCRIBE:
        case StmtType::EXPLAIN:
            return true;
        default:
            return false;
    }
}

// ============================================================
// Handle a single client connection
// ============================================================

struct ServerContext {
    const InMemoryCatalog& catalog;
    const ShardMap& shard_map;
    const std::vector<BackendConfig>& backends;
    bool has_shards;
};

static void handle_connection(int client_fd, uint32_t conn_id, const ServerContext& ctx) {
    Connection conn(client_fd);

    // 1. Send handshake
    if (!send_handshake(conn, conn_id)) {
        close(client_fd);
        return;
    }

    // 2. Receive auth response
    if (!receive_auth_response(conn)) {
        close(client_fd);
        return;
    }

    // 3. Send OK for successful auth
    conn.seq_id++;
    if (!send_ok(conn)) {
        close(client_fd);
        return;
    }

    // Set up per-connection session
    Arena txn_arena{65536, 1048576};
    LocalTransactionManager txn_mgr(txn_arena);

    ThreadSafeMultiRemoteExecutor remote_exec;
    for (auto& bc : ctx.backends) {
        remote_exec.add_backend(bc);
    }

    Session<Dialect::MySQL> session(ctx.catalog, txn_mgr);
    session.set_remote_executor(&remote_exec);
    session.set_parallel_open(true);  // thread-safe executor enables parallel shard I/O
    if (ctx.has_shards) {
        session.set_shard_map(&ctx.shard_map);
    }

    // 4. Command loop
    while (!g_stop.load(std::memory_order_relaxed)) {
        std::vector<uint8_t> payload;
        if (!conn.read_packet(payload)) break;
        if (payload.empty()) break;

        uint8_t cmd = payload[0];
        conn.seq_id++;

        switch (cmd) {
            case MY_COM_QUIT:
                goto done;

            case MY_COM_PING:
                if (!send_ok(conn)) goto done;
                break;

            case MY_COM_INIT_DB:
                // Accept any database selection
                if (!send_ok(conn)) goto done;
                break;

            case MY_COM_QUERY: {
                if (payload.size() < 2) {
                    if (!send_error(conn, 1065, "42000", "Query was empty"))
                        goto done;
                    break;
                }

                std::string sql(reinterpret_cast<const char*>(payload.data() + 1),
                                payload.size() - 1);

                // Strip trailing whitespace and semicolons
                while (!sql.empty() && (sql.back() == ' ' || sql.back() == '\t'
                       || sql.back() == '\r' || sql.back() == '\n'
                       || sql.back() == ';')) {
                    sql.pop_back();
                }

                if (sql.empty()) {
                    if (!send_ok(conn)) goto done;
                    break;
                }

                // Classify the statement
                Parser<Dialect::MySQL> classifier;
                auto pr = classifier.parse(sql.c_str(), sql.size());

                if (pr.status == ParseResult::ERROR) {
                    std::string err_msg = "Parse error";
                    if (pr.error.message.ptr && pr.error.message.len > 0)
                        err_msg.assign(pr.error.message.ptr, pr.error.message.len);
                    if (!send_error(conn, 1064, "42000", err_msg))
                        goto done;
                    break;
                }

                if (is_query_statement(pr.stmt_type)) {
                    // Execute as query
                    ResultSet rs = session.execute_query(sql.c_str(), sql.size());
                    if (!send_result_set(conn, rs)) goto done;
                } else {
                    // Execute as DML/DDL/transaction
                    DmlResult dr = session.execute_statement(sql.c_str(), sql.size());
                    if (dr.success) {
                        if (!send_ok(conn, dr.affected_rows, dr.last_insert_id))
                            goto done;
                    } else {
                        if (!send_error(conn, 1064, "42000",
                                        dr.error_message.empty() ? "Query failed" : dr.error_message))
                            goto done;
                    }
                }
                break;
            }

            default:
                // Unsupported command
                if (!send_error(conn, 1047, "08S01",
                                "Unsupported command: " + std::to_string(cmd)))
                    goto done;
                break;
        }
    }

done:
    remote_exec.disconnect_all();
    close(client_fd);
}

// ============================================================
// Schema auto-discovery (same as sqlengine.cpp)
// ============================================================

static void discover_schemas(InMemoryCatalog& catalog,
                              MultiRemoteExecutor& remote_exec,
                              const std::vector<TableShardConfig>& shards) {
    for (auto& sc : shards) {
        const char* first_backend = sc.shards.empty()
            ? nullptr : sc.shards[0].backend_name.c_str();
        if (!first_backend) continue;

        std::string show_sql = "SHOW COLUMNS FROM " + sc.table_name;
        StringRef sql_ref{show_sql.c_str(), static_cast<uint32_t>(show_sql.size())};
        try {
            ResultSet cols = remote_exec.execute(first_backend, sql_ref);
            std::vector<ColumnDef> col_defs;
            for (size_t i = 0; i < cols.row_count(); ++i) {
                const Row& r = cols.rows[i];
                std::string col_name;
                if (r.column_count > 0 && !r.get(0).is_null())
                    col_name.assign(r.get(0).str_val.ptr, r.get(0).str_val.len);
                std::string col_type_str;
                if (r.column_count > 1 && !r.get(1).is_null())
                    col_type_str.assign(r.get(1).str_val.ptr, r.get(1).str_val.len);
                SqlType st;
                if (col_type_str.find("int") != std::string::npos ||
                    col_type_str.find("INT") != std::string::npos)
                    st = SqlType::make_int();
                else if (col_type_str.find("decimal") != std::string::npos ||
                         col_type_str.find("DECIMAL") != std::string::npos)
                    st = SqlType::make_decimal(10, 2);
                else if (col_type_str.find("date") != std::string::npos ||
                         col_type_str.find("DATE") != std::string::npos)
                    st = SqlType{SqlType::DATE};
                else
                    st = SqlType::make_varchar(255);
                bool nullable = true;
                if (r.column_count > 2 && !r.get(2).is_null()) {
                    std::string null_str(r.get(2).str_val.ptr, r.get(2).str_val.len);
                    nullable = (null_str == "YES");
                }
                col_defs.push_back(ColumnDef{strdup(col_name.c_str()), st, nullable});
            }
            if (!col_defs.empty())
                catalog.add_table("", sc.table_name.c_str(), col_defs);
        } catch (...) {
            std::cerr << "Warning: schema discovery failed for "
                      << sc.table_name << "\n";
        }
    }
}

// ============================================================
// Signal handler
// ============================================================

static void signal_handler(int sig) {
    (void)sig;
    g_stop.store(true, std::memory_order_release);
}

// ============================================================
// Main
// ============================================================

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "Options:\n"
              << "  --port PORT         Listen port (default: 13309)\n"
              << "  --backend URL       Add a backend (mysql://...)\n"
              << "  --shard SPEC        Shard config (table:key:shard1,shard2)\n"
              << "  --max-connections N Max connections (default: 1024)\n"
              << "  --help              Show this help\n";
}

int main(int argc, char* argv[]) {
    std::vector<BackendConfig> backends;
    std::vector<TableShardConfig> shards;
    uint16_t listen_port = 13309;
    int max_connections = 1024;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--port" && i + 1 < argc) {
            ++i;
            listen_port = static_cast<uint16_t>(std::stoi(argv[i]));
        } else if (arg == "--backend" && i + 1 < argc) {
            ++i;
            auto pb = parse_backend_url(argv[i]);
            if (!pb.ok) {
                std::cerr << "Error: " << pb.error << "\n";
                return 1;
            }
            backends.push_back(std::move(pb.config));
        } else if (arg == "--shard" && i + 1 < argc) {
            ++i;
            auto ps = parse_shard_spec(argv[i]);
            if (!ps.ok) {
                std::cerr << "Error: " << ps.error << "\n";
                return 1;
            }
            shards.push_back(std::move(ps.config));
        } else if (arg == "--max-connections" && i + 1 < argc) {
            ++i;
            max_connections = std::stoi(argv[i]);
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    // Set up catalog
    InMemoryCatalog catalog;

    // Set up shard map
    ShardMap shard_map;
    for (auto& sc : shards) {
        shard_map.add_table(sc);
    }
    bool has_shards = !shards.empty();

    // Schema discovery using a temporary executor
    if (!backends.empty() && has_shards) {
        MultiRemoteExecutor setup_exec;
        for (auto& bc : backends) {
            setup_exec.add_backend(bc);
        }
        discover_schemas(catalog, setup_exec, shards);
        setup_exec.disconnect_all();
    }

    // Set up signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    // Create server socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Error: socket() failed: " << strerror(errno) << "\n";
        return 1;
    }

    // SO_REUSEADDR
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(listen_port);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&addr),
             sizeof(addr)) < 0) {
        std::cerr << "Error: bind() failed on port " << listen_port
                  << ": " << strerror(errno) << "\n";
        close(server_fd);
        return 1;
    }

    // Listen
    if (listen(server_fd, 128) < 0) {
        std::cerr << "Error: listen() failed: " << strerror(errno) << "\n";
        close(server_fd);
        return 1;
    }

    std::cerr << "ParserSQL MySQL Server listening on port "
              << listen_port << "\n";
    if (!backends.empty()) {
        std::cerr << "Backends: " << backends.size() << "\n";
        for (auto& b : backends)
            std::cerr << "  - " << b.name << " (" << b.host << ":"
                      << b.port << "/" << b.database << ")\n";
    }
    if (has_shards) {
        std::cerr << "Shards:\n";
        for (auto& sc : shards)
            std::cerr << "  - " << sc.table_name << " (key="
                      << sc.shard_key << ", " << sc.shards.size()
                      << " shards)\n";
    }
    std::cerr << "Ready for connections. Ctrl+C to stop.\n";

    ServerContext ctx{catalog, shard_map, backends, has_shards};

    std::atomic<uint32_t> conn_counter{1};
    (void)max_connections;

    // Accept loop
    while (!g_stop.load(std::memory_order_relaxed)) {
        // Use select with a timeout so we can check g_stop
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(server_fd, &rfds);

        int sel = select(server_fd + 1, &rfds, nullptr, nullptr, &tv);
        if (sel <= 0) continue;

        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd,
            reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            std::cerr << "Warning: accept() failed: "
                      << strerror(errno) << "\n";
            continue;
        }

        // TCP_NODELAY for lower latency
        int tcp_opt = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
                   &tcp_opt, sizeof(tcp_opt));

        uint32_t cid = conn_counter.fetch_add(1,
            std::memory_order_relaxed);

        // Spawn a detached thread for this connection
        std::thread(handle_connection, client_fd, cid,
                    std::cref(ctx)).detach();
    }

    std::cerr << "\nShutting down...\n";
    close(server_fd);

    return 0;
}
