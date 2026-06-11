#include "tools/pg_compat/statement_type_map.h"

namespace pg_compat {

#define PG_COMPAT_SIMPLE_STATEMENT_TYPE_MAPPINGS(X) \
    X(PG_QUERY__NODE__NODE_SELECT_STMT, Equivalent, SELECT) \
    X(PG_QUERY__NODE__NODE_INSERT_STMT, Equivalent, INSERT) \
    X(PG_QUERY__NODE__NODE_UPDATE_STMT, Equivalent, UPDATE) \
    X(PG_QUERY__NODE__NODE_DELETE_STMT, Equivalent, DELETE_STMT) \
    X(PG_QUERY__NODE__NODE_VARIABLE_SET_STMT, Equivalent, SET) \
    X(PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT, Equivalent, SHOW) \
    X(PG_QUERY__NODE__NODE_PREPARE_STMT, Equivalent, PREPARE) \
    X(PG_QUERY__NODE__NODE_EXECUTE_STMT, Equivalent, EXECUTE) \
    X(PG_QUERY__NODE__NODE_DEALLOCATE_STMT, Equivalent, DEALLOCATE) \
    X(PG_QUERY__NODE__NODE_EXPLAIN_STMT, Equivalent, EXPLAIN) \
    X(PG_QUERY__NODE__NODE_CALL_STMT, Equivalent, CALL) \
    X(PG_QUERY__NODE__NODE_DO_STMT, Equivalent, DO_STMT) \
    X(PG_QUERY__NODE__NODE_TRUNCATE_STMT, Equivalent, TRUNCATE) \
    X(PG_QUERY__NODE__NODE_LOCK_STMT, Equivalent, LOCK) \
    X(PG_QUERY__NODE__NODE_CREATE_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_INDEX_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_ROLE_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_SEQ_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_ENUM_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATE_RANGE_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_CREATEDB_STMT, Equivalent, CREATE) \
    X(PG_QUERY__NODE__NODE_ALTER_TABLE_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_ALTER_ROLE_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_ALTER_SEQ_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_ALTER_ENUM_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_RENAME_STMT, Equivalent, ALTER) \
    X(PG_QUERY__NODE__NODE_DROP_STMT, Equivalent, DROP) \
    X(PG_QUERY__NODE__NODE_DROPDB_STMT, Equivalent, DROP) \
    X(PG_QUERY__NODE__NODE_DROP_ROLE_STMT, Equivalent, DROP) \
    X(PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT, Equivalent, DROP) \
    X(PG_QUERY__NODE__NODE_COPY_STMT, NoEquivalent, UNKNOWN) \
    X(PG_QUERY__NODE__NODE_MERGE_STMT, NoEquivalent, UNKNOWN) \
    X(PG_QUERY__NODE__NODE_VACUUM_STMT, NoEquivalent, UNKNOWN) \
    X(PG_QUERY__NODE__NODE_NOTIFY_STMT, NoEquivalent, UNKNOWN) \
    X(PG_QUERY__NODE__NODE_LISTEN_STMT, NoEquivalent, UNKNOWN) \
    X(PG_QUERY__NODE__NODE_UNLISTEN_STMT, NoEquivalent, UNKNOWN)

StatementTypeMapping expected_stmt_type(const PgQuery__Node& node) {
    using sql_parser::StmtType;

    switch (node.node_case) {
#define PG_COMPAT_MAPPING_CASE(node_case, mapping_kind, stmt_type) \
    case node_case: \
        return {MappingKind::mapping_kind, StmtType::stmt_type};
    PG_COMPAT_SIMPLE_STATEMENT_TYPE_MAPPINGS(PG_COMPAT_MAPPING_CASE)
#undef PG_COMPAT_MAPPING_CASE
    case PG_QUERY__NODE__NODE_TRANSACTION_STMT:
        if (node.transaction_stmt == nullptr) {
            return {};
        }
        switch (node.transaction_stmt->kind) {
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_BEGIN:
            return {MappingKind::Equivalent, StmtType::BEGIN};
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_START:
            return {MappingKind::Equivalent, StmtType::START_TRANSACTION};
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT_PREPARED:
            return {MappingKind::Equivalent, StmtType::COMMIT};
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_TO:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_PREPARED:
            return {MappingKind::Equivalent, StmtType::ROLLBACK};
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_SAVEPOINT:
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_RELEASE:
            return {MappingKind::Equivalent, StmtType::SAVEPOINT};
        case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_PREPARE:
            return {MappingKind::Equivalent, StmtType::PREPARE};
        default:
            return {};
        }
    case PG_QUERY__NODE__NODE_GRANT_STMT:
        if (node.grant_stmt == nullptr) {
            return {};
        }
        return {
            MappingKind::Equivalent,
            node.grant_stmt->is_grant ? StmtType::GRANT : StmtType::REVOKE,
        };
    case PG_QUERY__NODE__NODE_GRANT_ROLE_STMT:
        if (node.grant_role_stmt == nullptr) {
            return {};
        }
        return {
            MappingKind::Equivalent,
            node.grant_role_stmt->is_grant ? StmtType::GRANT : StmtType::REVOKE,
        };
    default:
        return {};
    }
}

const char* oracle_node_name(PgQuery__Node__NodeCase node_case) {
    switch (node_case) {
#define PG_COMPAT_NODE_NAME_CASE(node_case, mapping_kind, stmt_type) \
    case node_case: \
        return #node_case;
    PG_COMPAT_SIMPLE_STATEMENT_TYPE_MAPPINGS(PG_COMPAT_NODE_NAME_CASE)
#undef PG_COMPAT_NODE_NAME_CASE
    case PG_QUERY__NODE__NODE_TRANSACTION_STMT:
        return "PG_QUERY__NODE__NODE_TRANSACTION_STMT";
    case PG_QUERY__NODE__NODE_GRANT_STMT:
        return "PG_QUERY__NODE__NODE_GRANT_STMT";
    case PG_QUERY__NODE__NODE_GRANT_ROLE_STMT:
        return "PG_QUERY__NODE__NODE_GRANT_ROLE_STMT";
    default:
        return "UNMAPPED_NODE_CASE";
    }
}

const char* stmt_type_name(sql_parser::StmtType type) {
    using sql_parser::StmtType;

    // Missing enum cases are errors so StmtType additions cannot drift silently.
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch"
#endif
    switch (type) {
    case StmtType::UNKNOWN:
        return "UNKNOWN";
    case StmtType::SELECT:
        return "SELECT";
    case StmtType::INSERT:
        return "INSERT";
    case StmtType::UPDATE:
        return "UPDATE";
    case StmtType::DELETE_STMT:
        return "DELETE";
    case StmtType::REPLACE:
        return "REPLACE";
    case StmtType::SET:
        return "SET";
    case StmtType::USE:
        return "USE";
    case StmtType::SHOW:
        return "SHOW";
    case StmtType::BEGIN:
        return "BEGIN";
    case StmtType::START_TRANSACTION:
        return "START_TRANSACTION";
    case StmtType::COMMIT:
        return "COMMIT";
    case StmtType::ROLLBACK:
        return "ROLLBACK";
    case StmtType::SAVEPOINT:
        return "SAVEPOINT";
    case StmtType::PREPARE:
        return "PREPARE";
    case StmtType::EXECUTE:
        return "EXECUTE";
    case StmtType::DEALLOCATE:
        return "DEALLOCATE";
    case StmtType::CREATE:
        return "CREATE";
    case StmtType::ALTER:
        return "ALTER";
    case StmtType::DROP:
        return "DROP";
    case StmtType::TRUNCATE:
        return "TRUNCATE";
    case StmtType::GRANT:
        return "GRANT";
    case StmtType::REVOKE:
        return "REVOKE";
    case StmtType::LOCK:
        return "LOCK";
    case StmtType::UNLOCK:
        return "UNLOCK";
    case StmtType::LOAD_DATA:
        return "LOAD_DATA";
    case StmtType::RESET:
        return "RESET";
    case StmtType::EXPLAIN:
        return "EXPLAIN";
    case StmtType::DESCRIBE:
        return "DESCRIBE";
    case StmtType::CALL:
        return "CALL";
    case StmtType::DO_STMT:
        return "DO";
    }
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
    return "OTHER";
}

#undef PG_COMPAT_SIMPLE_STATEMENT_TYPE_MAPPINGS

} // namespace pg_compat
