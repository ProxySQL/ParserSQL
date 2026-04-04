#ifndef SQL_ENGINE_TRANSACTION_MANAGER_H
#define SQL_ENGINE_TRANSACTION_MANAGER_H

namespace sql_engine {

// Abstract interface for transaction management.
// Implementations: LocalTransactionManager, SingleBackendTransactionManager,
// DistributedTransactionManager.
class TransactionManager {
public:
    virtual ~TransactionManager() = default;

    // Transaction lifecycle
    virtual bool begin() = 0;
    virtual bool commit() = 0;
    virtual bool rollback() = 0;

    // Savepoints
    virtual bool savepoint(const char* name) = 0;
    virtual bool rollback_to(const char* name) = 0;
    virtual bool release_savepoint(const char* name) = 0;

    // State
    virtual bool in_transaction() const = 0;
    virtual bool is_auto_commit() const = 0;
    virtual void set_auto_commit(bool ac) = 0;
};

} // namespace sql_engine

#endif // SQL_ENGINE_TRANSACTION_MANAGER_H
