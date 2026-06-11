#include "tools/pg_compat/statement_type_map.h"

namespace pg_compat {

StatementTypeMapping expected_stmt_type(const PgQuery__Node& node) {
    using sql_parser::StmtType;

    switch (node.node_case) {
    case PG_QUERY__NODE__NODE_SELECT_STMT:
        return {MappingKind::Equivalent, StmtType::SELECT};
    case PG_QUERY__NODE__NODE_INSERT_STMT:
        return {MappingKind::Equivalent, StmtType::INSERT};
    case PG_QUERY__NODE__NODE_UPDATE_STMT:
        return {MappingKind::Equivalent, StmtType::UPDATE};
    case PG_QUERY__NODE__NODE_DELETE_STMT:
        return {MappingKind::Equivalent, StmtType::DELETE_STMT};
    case PG_QUERY__NODE__NODE_VARIABLE_SET_STMT:
        return {MappingKind::Equivalent, StmtType::SET};
    case PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT:
        return {MappingKind::Equivalent, StmtType::SHOW};
    case PG_QUERY__NODE__NODE_PREPARE_STMT:
        return {MappingKind::Equivalent, StmtType::PREPARE};
    case PG_QUERY__NODE__NODE_EXECUTE_STMT:
        return {MappingKind::Equivalent, StmtType::EXECUTE};
    case PG_QUERY__NODE__NODE_DEALLOCATE_STMT:
        return {MappingKind::Equivalent, StmtType::DEALLOCATE};
    case PG_QUERY__NODE__NODE_EXPLAIN_STMT:
        return {MappingKind::Equivalent, StmtType::EXPLAIN};
    case PG_QUERY__NODE__NODE_CALL_STMT:
        return {MappingKind::Equivalent, StmtType::CALL};
    case PG_QUERY__NODE__NODE_DO_STMT:
        return {MappingKind::Equivalent, StmtType::DO_STMT};
    case PG_QUERY__NODE__NODE_TRUNCATE_STMT:
        return {MappingKind::Equivalent, StmtType::TRUNCATE};
    case PG_QUERY__NODE__NODE_LOCK_STMT:
        return {MappingKind::Equivalent, StmtType::LOCK};
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
    case PG_QUERY__NODE__NODE_CREATE_STMT:
    case PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT:
    case PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT:
    case PG_QUERY__NODE__NODE_INDEX_STMT:
    case PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT:
    case PG_QUERY__NODE__NODE_CREATE_ROLE_STMT:
    case PG_QUERY__NODE__NODE_CREATE_SEQ_STMT:
    case PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT:
    case PG_QUERY__NODE__NODE_CREATE_ENUM_STMT:
    case PG_QUERY__NODE__NODE_CREATE_RANGE_STMT:
    case PG_QUERY__NODE__NODE_CREATEDB_STMT:
        return {MappingKind::Equivalent, StmtType::CREATE};
    case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT:
    case PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT:
    case PG_QUERY__NODE__NODE_ALTER_ROLE_STMT:
    case PG_QUERY__NODE__NODE_ALTER_SEQ_STMT:
    case PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT:
    case PG_QUERY__NODE__NODE_ALTER_ENUM_STMT:
    case PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT:
    case PG_QUERY__NODE__NODE_RENAME_STMT:
        return {MappingKind::Equivalent, StmtType::ALTER};
    case PG_QUERY__NODE__NODE_DROP_STMT:
    case PG_QUERY__NODE__NODE_DROPDB_STMT:
    case PG_QUERY__NODE__NODE_DROP_ROLE_STMT:
    case PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT:
        return {MappingKind::Equivalent, StmtType::DROP};
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
    case PG_QUERY__NODE__NODE_COPY_STMT:
    case PG_QUERY__NODE__NODE_MERGE_STMT:
    case PG_QUERY__NODE__NODE_VACUUM_STMT:
    case PG_QUERY__NODE__NODE_NOTIFY_STMT:
    case PG_QUERY__NODE__NODE_LISTEN_STMT:
    case PG_QUERY__NODE__NODE_UNLISTEN_STMT:
        return {MappingKind::NoEquivalent, StmtType::UNKNOWN};
    default:
        return {};
    }
}

const char* oracle_node_name(PgQuery__Node__NodeCase node_case) {
    switch (node_case) {
    case PG_QUERY__NODE__NODE_SELECT_STMT:
        return "PG_QUERY__NODE__NODE_SELECT_STMT";
    case PG_QUERY__NODE__NODE_INSERT_STMT:
        return "PG_QUERY__NODE__NODE_INSERT_STMT";
    case PG_QUERY__NODE__NODE_UPDATE_STMT:
        return "PG_QUERY__NODE__NODE_UPDATE_STMT";
    case PG_QUERY__NODE__NODE_DELETE_STMT:
        return "PG_QUERY__NODE__NODE_DELETE_STMT";
    case PG_QUERY__NODE__NODE_VARIABLE_SET_STMT:
        return "PG_QUERY__NODE__NODE_VARIABLE_SET_STMT";
    case PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT:
        return "PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT";
    case PG_QUERY__NODE__NODE_PREPARE_STMT:
        return "PG_QUERY__NODE__NODE_PREPARE_STMT";
    case PG_QUERY__NODE__NODE_EXECUTE_STMT:
        return "PG_QUERY__NODE__NODE_EXECUTE_STMT";
    case PG_QUERY__NODE__NODE_DEALLOCATE_STMT:
        return "PG_QUERY__NODE__NODE_DEALLOCATE_STMT";
    case PG_QUERY__NODE__NODE_EXPLAIN_STMT:
        return "PG_QUERY__NODE__NODE_EXPLAIN_STMT";
    case PG_QUERY__NODE__NODE_CALL_STMT:
        return "PG_QUERY__NODE__NODE_CALL_STMT";
    case PG_QUERY__NODE__NODE_DO_STMT:
        return "PG_QUERY__NODE__NODE_DO_STMT";
    case PG_QUERY__NODE__NODE_TRUNCATE_STMT:
        return "PG_QUERY__NODE__NODE_TRUNCATE_STMT";
    case PG_QUERY__NODE__NODE_LOCK_STMT:
        return "PG_QUERY__NODE__NODE_LOCK_STMT";
    case PG_QUERY__NODE__NODE_TRANSACTION_STMT:
        return "PG_QUERY__NODE__NODE_TRANSACTION_STMT";
    case PG_QUERY__NODE__NODE_CREATE_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_STMT";
    case PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT";
    case PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT";
    case PG_QUERY__NODE__NODE_INDEX_STMT:
        return "PG_QUERY__NODE__NODE_INDEX_STMT";
    case PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT";
    case PG_QUERY__NODE__NODE_CREATE_ROLE_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_ROLE_STMT";
    case PG_QUERY__NODE__NODE_CREATE_SEQ_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_SEQ_STMT";
    case PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT";
    case PG_QUERY__NODE__NODE_CREATE_ENUM_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_ENUM_STMT";
    case PG_QUERY__NODE__NODE_CREATE_RANGE_STMT:
        return "PG_QUERY__NODE__NODE_CREATE_RANGE_STMT";
    case PG_QUERY__NODE__NODE_CREATEDB_STMT:
        return "PG_QUERY__NODE__NODE_CREATEDB_STMT";
    case PG_QUERY__NODE__NODE_ALTER_TABLE_STMT:
        return "PG_QUERY__NODE__NODE_ALTER_TABLE_STMT";
    case PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT:
        return "PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT";
    case PG_QUERY__NODE__NODE_ALTER_ROLE_STMT:
        return "PG_QUERY__NODE__NODE_ALTER_ROLE_STMT";
    case PG_QUERY__NODE__NODE_ALTER_SEQ_STMT:
        return "PG_QUERY__NODE__NODE_ALTER_SEQ_STMT";
    case PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT:
        return "PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT";
    case PG_QUERY__NODE__NODE_ALTER_ENUM_STMT:
        return "PG_QUERY__NODE__NODE_ALTER_ENUM_STMT";
    case PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT:
        return "PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT";
    case PG_QUERY__NODE__NODE_RENAME_STMT:
        return "PG_QUERY__NODE__NODE_RENAME_STMT";
    case PG_QUERY__NODE__NODE_DROP_STMT:
        return "PG_QUERY__NODE__NODE_DROP_STMT";
    case PG_QUERY__NODE__NODE_DROPDB_STMT:
        return "PG_QUERY__NODE__NODE_DROPDB_STMT";
    case PG_QUERY__NODE__NODE_DROP_ROLE_STMT:
        return "PG_QUERY__NODE__NODE_DROP_ROLE_STMT";
    case PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT:
        return "PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT";
    case PG_QUERY__NODE__NODE_GRANT_STMT:
        return "PG_QUERY__NODE__NODE_GRANT_STMT";
    case PG_QUERY__NODE__NODE_GRANT_ROLE_STMT:
        return "PG_QUERY__NODE__NODE_GRANT_ROLE_STMT";
    case PG_QUERY__NODE__NODE_COPY_STMT:
        return "PG_QUERY__NODE__NODE_COPY_STMT";
    case PG_QUERY__NODE__NODE_MERGE_STMT:
        return "PG_QUERY__NODE__NODE_MERGE_STMT";
    case PG_QUERY__NODE__NODE_VACUUM_STMT:
        return "PG_QUERY__NODE__NODE_VACUUM_STMT";
    case PG_QUERY__NODE__NODE_NOTIFY_STMT:
        return "PG_QUERY__NODE__NODE_NOTIFY_STMT";
    case PG_QUERY__NODE__NODE_LISTEN_STMT:
        return "PG_QUERY__NODE__NODE_LISTEN_STMT";
    case PG_QUERY__NODE__NODE_UNLISTEN_STMT:
        return "PG_QUERY__NODE__NODE_UNLISTEN_STMT";
    default:
        return "UNMAPPED_NODE_CASE";
    }
}

const char* stmt_type_name(sql_parser::StmtType type) {
    using sql_parser::StmtType;

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
    return "OTHER";
}

} // namespace pg_compat
