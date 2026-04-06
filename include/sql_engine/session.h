#ifndef SQL_ENGINE_SESSION_H
#define SQL_ENGINE_SESSION_H

#include "sql_engine/transaction_manager.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/optimizer.h"
#include "sql_engine/distributed_planner.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/catalog.h"
#include "sql_engine/function_registry.h"
#include "sql_engine/result_set.h"
#include "sql_engine/dml_result.h"
#include "sql_engine/mutable_data_source.h"
#include "sql_parser/parser.h"
#include "sql_parser/common.h"

#include <cstring>
#include <string>
#include <unordered_map>
#include <memory>

namespace sql_engine {

// Session<D> is the high-level API that ties together parsing, planning,
// optimization, execution, and transaction management.
//
// Usage:
//   Session<Dialect::MySQL> session(catalog, txn_mgr);
//   session.add_data_source("users", &users_source);
//   auto rs = session.execute_query("SELECT * FROM users");
//   auto dr = session.execute_statement("INSERT INTO users VALUES (1, 'a')");
//   session.begin();
//   session.commit();
template <sql_parser::Dialect D>
class Session {
public:
    Session(const Catalog& catalog, TransactionManager& txn_mgr)
        : catalog_(catalog), txn_mgr_(txn_mgr),
          functions_(), optimizer_(catalog, functions_) {
        functions_.register_builtins();
    }

    // Register data sources
    void add_data_source(const char* table_name, DataSource* source) {
        sources_[table_name] = source;
    }

    void add_mutable_data_source(const char* table_name, MutableDataSource* source) {
        mutable_sources_[table_name] = source;
        sources_[table_name] = source;
    }

    void set_remote_executor(RemoteExecutor* exec) {
        remote_executor_ = exec;
    }

    // Enable parallel opening of RemoteScan children. Only safe when the
    // RemoteExecutor is thread-safe (e.g. ThreadSafeMultiRemoteExecutor).
    void set_parallel_open(bool enabled) {
        parallel_open_enabled_ = enabled;
    }

    void set_shard_map(const ShardMap* sm) {
        shard_map_ = sm;
    }

    // Set a shared thread pool for parallel shard I/O dispatch.
    // The pool must outlive this session. Use with set_parallel_open(true).
    void set_thread_pool(ThreadPool* pool) {
        pool_ = pool;
    }

    // Execute a SELECT query. Returns a ResultSet.
    // Uses plan caching: repeated identical SQL strings skip parse/plan/optimize/distribute.
    ResultSet execute_query(const char* sql, size_t len) {
        // Check plan cache first
        std::string sql_key(sql, len);
        auto cache_it = plan_cache_.find(sql_key);
        if (cache_it != plan_cache_.end()) {
            // Cache hit: reuse the cached plan. Use exec_arena_ for per-query
            // allocations (rows, operator internals). Reset it each time.
            exec_arena_.reset();
            auto& entry = cache_it->second;
            PlanExecutor<D> executor(functions_, catalog_, exec_arena_);
            wire_executor(executor);
            return executor.execute(entry.plan);
        }

        // Cache miss: full parse -> plan -> optimize -> distribute pipeline
        auto cached_parser = std::make_unique<sql_parser::Parser<D>>();
        auto pr = cached_parser->parse(sql, len);
        if (pr.status != sql_parser::ParseResult::OK || !pr.ast) {
            return {};
        }

        PlanBuilder<D> builder(catalog_, cached_parser->arena());
        PlanNode* plan = builder.build(pr.ast);
        if (!plan) return {};

        plan = optimizer_.optimize(plan, cached_parser->arena());

        // Distribute across shards if shard map is configured
        if (shard_map_ && remote_executor_) {
            DistributedPlanner<D> dplanner(*shard_map_, catalog_, cached_parser->arena(), remote_executor_, &functions_);
            plan = dplanner.distribute(plan);
        }

        // Execute first using the parser's arena. preprocess_aggregates (called
        // inside execute()) may allocate into the arena to modify the plan in-place.
        // Those allocations must persist in the parser arena (not exec_arena_) so
        // the cached plan remains valid across calls.
        PlanExecutor<D> executor(functions_, catalog_, cached_parser->arena());
        wire_executor(executor);
        ResultSet rs = executor.execute(plan);

        // Cache the plan (parser arena keeps plan tree and strings alive)
        CachedPlan entry;
        entry.parser = std::move(cached_parser);
        entry.plan = plan;
        plan_cache_.emplace(std::move(sql_key), std::move(entry));

        return rs;
    }

    ResultSet execute_query(const char* sql) {
        return execute_query(sql, std::strlen(sql));
    }

    // Execute a DML statement (INSERT/UPDATE/DELETE). Returns DmlResult.
    DmlResult execute_statement(const char* sql, size_t len) {
        parser_.reset();
        auto pr = parser_.parse(sql, len);

        // Check for transaction control statements first (parser may not
        // produce an AST for these — they are classified by stmt_type).
        switch (pr.stmt_type) {
            case sql_parser::StmtType::BEGIN:
            case sql_parser::StmtType::START_TRANSACTION: {
                DmlResult dr;
                dr.success = txn_mgr_.begin();
                if (!dr.success) dr.error_message = "BEGIN failed";
                return dr;
            }
            case sql_parser::StmtType::COMMIT: {
                DmlResult dr;
                dr.success = txn_mgr_.commit();
                if (!dr.success) dr.error_message = "COMMIT failed";
                return dr;
            }
            case sql_parser::StmtType::ROLLBACK: {
                DmlResult dr;
                dr.success = txn_mgr_.rollback();
                if (!dr.success) dr.error_message = "ROLLBACK failed";
                return dr;
            }
            case sql_parser::StmtType::SAVEPOINT: {
                DmlResult dr;
                // The savepoint name is in the AST value or table_name
                std::string name;
                if (pr.table_name.ptr && pr.table_name.len > 0)
                    name.assign(pr.table_name.ptr, pr.table_name.len);
                else if (pr.ast && pr.ast->value_ptr && pr.ast->value_len > 0)
                    name.assign(pr.ast->value_ptr, pr.ast->value_len);
                else
                    name = "sp";
                dr.success = txn_mgr_.savepoint(name.c_str());
                if (!dr.success) dr.error_message = "SAVEPOINT failed";
                return dr;
            }
            default:
                break;
        }

        // For non-transaction statements, need a valid parse + AST
        if (pr.status != sql_parser::ParseResult::OK || !pr.ast) {
            DmlResult dr;
            dr.error_message = "parse error";
            return dr;
        }

        // Regular DML
        DmlPlanBuilder<D> dml_builder(catalog_, parser_.arena());
        PlanNode* plan = dml_builder.build(pr.ast);
        if (!plan) {
            DmlResult dr;
            dr.error_message = "plan build error";
            return dr;
        }

        // Auto-commit: wrap in implicit transaction
        bool implicit_txn = txn_mgr_.is_auto_commit() && !txn_mgr_.in_transaction();
        if (implicit_txn) txn_mgr_.begin();

        PlanExecutor<D> executor(functions_, catalog_, parser_.arena());
        wire_executor(executor);
        DmlResult result = executor.execute_dml(plan);

        if (implicit_txn) {
            if (result.success)
                txn_mgr_.commit();
            else
                txn_mgr_.rollback();
        }

        return result;
    }

    DmlResult execute_statement(const char* sql) {
        return execute_statement(sql, std::strlen(sql));
    }

    // Transaction control
    bool begin() { return txn_mgr_.begin(); }
    bool commit() { return txn_mgr_.commit(); }
    bool rollback() { return txn_mgr_.rollback(); }
    bool savepoint(const char* name) { return txn_mgr_.savepoint(name); }
    bool rollback_to(const char* name) { return txn_mgr_.rollback_to(name); }

    void set_auto_commit(bool ac) { txn_mgr_.set_auto_commit(ac); }
    bool is_auto_commit() const { return txn_mgr_.is_auto_commit(); }
    bool in_transaction() const { return txn_mgr_.in_transaction(); }

    // Access internals (for testing)
    TransactionManager& txn_manager() { return txn_mgr_; }
    const Catalog& catalog() const { return catalog_; }

private:
    const Catalog& catalog_;
    TransactionManager& txn_mgr_;
    sql_parser::Parser<D> parser_;
    FunctionRegistry<D> functions_;
    Optimizer<D> optimizer_;
    RemoteExecutor* remote_executor_ = nullptr;
    const ShardMap* shard_map_ = nullptr;
    bool parallel_open_enabled_ = false;
    std::unordered_map<std::string, DataSource*> sources_;
    std::unordered_map<std::string, MutableDataSource*> mutable_sources_;

    // Per-query execution arena. Reset before each query execution so
    // per-query allocations (rows, operator internals) don't accumulate.
    sql_parser::Arena exec_arena_{65536, 1048576};

    // Thread pool for lightweight parallel shard I/O dispatch.
    // Externally owned; set via set_thread_pool(). Shared across sessions.
    ThreadPool* pool_ = nullptr;

    // Plan cache: maps SQL string → cached plan + parser arena.
    // The parser's arena keeps all AST/plan string pointers valid.
    struct CachedPlan {
        std::unique_ptr<sql_parser::Parser<D>> parser;
        PlanNode* plan;
    };
    std::unordered_map<std::string, CachedPlan> plan_cache_;

    void wire_executor(PlanExecutor<D>& executor) {
        for (auto& kv : sources_)
            executor.add_data_source(kv.first.c_str(), kv.second);
        for (auto& kv : mutable_sources_)
            executor.add_mutable_data_source(kv.first.c_str(), kv.second);
        if (remote_executor_)
            executor.set_remote_executor(remote_executor_);
        if (parallel_open_enabled_) {
            executor.set_parallel_open(true);
            if (pool_)
                executor.set_thread_pool(pool_);
        }
        // If sharding is configured, provide a distribute callback so that
        // subqueries also go through the distributed planner.
        if (shard_map_ && remote_executor_) {
            executor.set_distribute_fn(
                [this](PlanNode* plan) -> PlanNode* {
                    DistributedPlanner<D> dp(*shard_map_, catalog_, parser_.arena(),
                                             remote_executor_, &functions_);
                    return dp.distribute(plan);
                });
        }
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_SESSION_H
