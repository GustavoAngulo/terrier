#pragma once

#include <catalog/catalog.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "common/dedicated_thread_owner.h"
#include "storage/recovery/abstract_log_provider.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"

namespace terrier {
class RecoveryBenchmark;
}

namespace terrier::storage {

/**
 * Recovery Manager
 * TODO(Gus): Add more documentation when API is finalized
 */
class RecoveryManager : public common::DedicatedThreadOwner {
  /**
   * Task in charge of initializing recovery. This way recovery can be non-blocking in a background thread.
   */
  class RecoveryTask : public common::DedicatedThreadTask {
   public:
    /**
     * @param recovery_manager pointer to recovery manager who initialized task
     */
    explicit RecoveryTask(RecoveryManager *recovery_manager) : recovery_manager_(recovery_manager) {}

    /**
     * Runs the recovery task. Our task only calls Recover on the log manager.
     */
    void RunTask() override { recovery_manager_->Recover(); }

    /**
     * Terminate does nothing, the task will terminate when RunTask() returns. In the future if we need to support
     * interrupting recovery, this can be handled here.
     */
    void Terminate() override {}

   private:
    RecoveryManager *recovery_manager_;
  };

 public:
  /**
   * @param log_provider arbitrary provider to receive logs from
   * @param catalog system catalog to interface with sql tables
   * @param txn_manager txn manager to use for re-executing recovered transactions
   * @param thread_registry thread registry to register tasks
   * @param store block store used for SQLTable creation during recovery
   */
  explicit RecoveryManager(AbstractLogProvider *log_provider, common::ManagedPointer<catalog::Catalog> catalog,
                           transaction::TransactionManager *txn_manager,
                           common::ManagedPointer<terrier::common::DedicatedThreadRegistry> thread_registry,
                           BlockStore *store)
      : DedicatedThreadOwner(thread_registry),
        log_provider_(log_provider),
        catalog_(catalog),
        txn_manager_(txn_manager),
        block_store_(store),
        recovered_txns_(0) {}

  /**
   * Starts a background recovery task. Recovery will fully recover until the log provider stops providing logs.
   */
  void StartRecovery() {
    TERRIER_ASSERT(recovery_task_ == nullptr, "Recovery already started");
    recovery_task_ =
        thread_registry_->RegisterDedicatedThread<RecoveryTask>(this /* dedicated thread owner */, this /* task arg */);
  }

  /**
   * Blocks until recovery finishes, if it has not already, and stops background thread.
   */
  void WaitForRecoveryToFinish() {
    TERRIER_ASSERT(recovery_task_ != nullptr, "Recovery must already have been started");
    if (!thread_registry_->StopTask(this, recovery_task_.CastManagedPointerTo<common::DedicatedThreadTask>())) {
      throw std::runtime_error("Recovery task termination failed");
    }
  }

 private:
  friend class RecoveryTests;
  friend class terrier::RecoveryBenchmark;

  // Log provider for reading in logs
  AbstractLogProvider *log_provider_;

  // Catalog to fetch table pointers
  common::ManagedPointer<catalog::Catalog> catalog_;

  // Transaction manager to create transactions for recovery
  transaction::TransactionManager *txn_manager_;

  // Block store, used to create tables during recovery
  BlockStore *block_store_;

  // Used during recovery from log. Maps old tuple slot to new tuple slot
  // TODO(Gus): This map may get huge, benchmark whether this becomes a problem and if we need a more sophisticated data
  // structure
  std::unordered_map<TupleSlot, TupleSlot> tuple_slot_map_;

  // Used during recovery from log. Stores deferred transactions in reverse sorted order. Transactions are defered when
  // there is an older active transaction at the time it committed. Even though snapshot isolation would handle
  // write-write conflicts, DDL changes such as DROP TABLE combined with GC could lead to issues if we don't execute
  // transactions in complete serial order. We store them in sorted order to be able to execute them in order. Having
  // them in reverse order allows us faster inserts, since the stream of txn_ids is mostly monotonically increasing.
  std::list<transaction::timestamp_t> deferred_txns_;

  // Used during recovery from log. Maps a the txn id from the persisted txn to its changes we have buffered. We buffer
  // changes until commit time. This ensures serializability, and allows us to skip changes from aborted txns.
  std::unordered_map<transaction::timestamp_t, std::vector<std::pair<LogRecord *, std::vector<byte *>>>>
      buffered_changes_map_;

  // Background recovery task
  common::ManagedPointer<RecoveryTask> recovery_task_ = nullptr;

  // Number of recovered txns. Used for benchmarking
  uint32_t recovered_txns_;

  /**
   * Recovers the databases using the provided log provider
   * @return number of committed transactions replayed
   */
  void Recover() { recovered_txns_ += RecoverFromLogs(); }

  /**
   * Recovers the databases from the logs.
   * @note this is a separate method so in the future, we can also have a RecoverFromCheckpoint method
   * @return number of committed txns replayed
   */
  uint32_t RecoverFromLogs();

  /**
   * @brief Replay a committed transaction corresponding to txn_id.
   * @param txn_id start timestamp for committed transaction
   */
  void ProcessCommittedTransaction(transaction::timestamp_t txn_id);

  /**
   * @brief Process an aborted transaction corresponding to txn_id.
   * Aborted transaction will have it's allocated varlen entries freed
   * @param txn_id start timestamp for aborted transaction
   */
  void ProcessAbortedTransaction(transaction::timestamp_t txn_id);

  /**
   * Adds transaction to deferred txn list
   * @param txn_id txn start timestamp for txn to defer
   */
  void DeferTransaction(transaction::timestamp_t txn_id);

  /**
   * Replay any transaction who's txn start time is less than upper_bound. If upper_bound == transaction::NO_ACTIVE_TXN,
   * it will replay all deferred transactions
   * @param upper_bound upper bound for replaying
   * @return number of transactions replayed
   */
  uint32_t ProcessDeferredTransactions(transaction::timestamp_t oldest_active_txn);

  /**
   * Handles mapping of old tuple slot (before recovery) to new tuple slot (after recovery)
   * @param slot old tuple slot
   * @return new tuple slot
   */
  TupleSlot GetTupleSlotMapping(TupleSlot slot) {
    TERRIER_ASSERT(tuple_slot_map_.find(slot) != tuple_slot_map_.end(), "No tuple slot mapping exists");
    return tuple_slot_map_[slot];
  }

  /**
   * Wrapper over GetDatabaseCatalog method that asserts the database exists
   * @param txn txn for catalog lookup
   * @param database oid for database we want
   * @return pointer to database catalog
   */
  common::ManagedPointer<catalog::DatabaseCatalog> GetDatabaseCatalog(transaction::TransactionContext *txn,
                                                                      catalog::db_oid_t db_oid) {
    auto db_catalog_ptr = catalog_->GetDatabaseCatalog(txn, db_oid);
    TERRIER_ASSERT(db_catalog_ptr != nullptr, "No catalog for given database oid");
    return db_catalog_ptr;
  }

  /**
   * @param txn transaction to use for catalog lookup
   * @param db_oid database oid for requested table
   * @param table_oid table oid for requested table
   * @return pointer to requested Sql table
   */
  common::ManagedPointer<storage::SqlTable> GetSqlTable(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                                                        catalog::table_oid_t table_oid);

  /**
   * Inserts or deletes a tuple slot from all indexes on a table.
   * @warning For an insert, must be called after the tuple slot is inserted into the table, for a delete, it must be
   * called before it is deleted from the table
   * @param txn transaction to delete with
   * @param db_oid database oid for table
   * @param table_oid indexed table
   * @param tuple tuple slot to delete
   * @param table_pr pointer to PR with values if we should insert into the index, nullptr if we should delete
   */
  void UpdateIndexesOnTable(transaction::TransactionContext *txn, catalog::db_oid_t db_oid,
                            catalog::table_oid_t table_oid, const TupleSlot &tuple_slot, ProjectedRow *table_pr);

  /**
   * NYS = Not yet supported
   * Returns whether a delete or redo record is a special case catalog record. The special cases we consider are:
   *   1. Insert into pg_database (creating a database)
   *   2. Updates into pg_class (updating a pointer, updating a schema (NYS), update to next col_oid)
   *   3. Delete into pg_database (renaming a database, drop a database)
   *   4. Delete into pg_class (renaming a table/index, drop a table/index)
   *   5. Delete into pg_index (cascading delete from drop index)
   *   6. Delete into pg_attribute (drop column (NYS) / cascading delete from drop table)
   * @param record log record we want to determine if its a special case
   * @return true if log record is a special case catalog record, false otherwise
   */
  bool IsSpecialCaseCatalogRecord(const LogRecord *record) {
    TERRIER_ASSERT(record->RecordType() == LogRecordType::REDO || record->RecordType() == LogRecordType::DELETE,
                   "Special case catalog records must only be delete or redo records");

    if (record->RecordType() == LogRecordType::REDO) {
      auto *redo_record = record->GetUnderlyingRecordBodyAs<RedoRecord>();
      if (IsInsertRecord(redo_record)) {
        // Case 1
        return redo_record->GetTableOid() == catalog::DATABASE_TABLE_OID;
      }

      // Case 2
      return redo_record->GetTableOid() == catalog::CLASS_TABLE_OID;
    }

    // Case 3, 4, 5, and 6
    auto *delete_record = record->GetUnderlyingRecordBodyAs<DeleteRecord>();
    return delete_record->GetTableOid() == catalog::DATABASE_TABLE_OID ||
           delete_record->GetTableOid() == catalog::CLASS_TABLE_OID ||
           delete_record->GetTableOid() == catalog::INDEX_TABLE_OID ||
           delete_record->GetTableOid() == catalog::COLUMN_TABLE_OID;
  }

  /**
   * Returns whether a given record is an insert into a table. We know it is an insert record if the tuple slot it
   * contains is previously unseen. An update will contain a tuple slot that has been previously inserted.
   * @param record record we want to determine redo type of
   * @return true if record is an insert redo, false if it is an update redo
   */
  bool IsInsertRecord(const RedoRecord *record) const {
    return tuple_slot_map_.find(record->GetTupleSlot()) == tuple_slot_map_.end();
  }

  /**
   * Processes records that modify the catalog tables. Because catalog modifications usually result in multiple log
   * records that result in custom handling logic, this function can process more than one log record.
   * @param txn transaction to use to replay the catalog changes
   * @param buffered_changes list of buffered log records
   * @param start_idx index of current log record in the list
   * @return number of EXTRA log records processed
   */
  uint32_t ProcessSpecialCaseCatalogRecord(transaction::TransactionContext *txn,
                                           std::vector<std::pair<LogRecord *, std::vector<byte *>>> *buffered_changes,
                                           uint32_t start_idx);

  /**
   * Replays a redo record. Updates necessary metadata maps
   * @param txn txn to use for replay
   * @param record record to replay
   */
  void ReplayRedoRecord(transaction::TransactionContext *txn, LogRecord *record);

  /**
   * Replays a delete record. Updates necessary metadata
   * @param txn txn to use for delete
   * @param record record to replay
   */
  void ReplayDeleteRecord(transaction::TransactionContext *txn, LogRecord *record);

  /**
   * Returns the list of col oids this redo record modified
   * @param sql_table sql table that redo record modifies
   * @param record record we want oids for
   * @return list of oids
   */
  // TODO(John): Currently this is being used to extract values from redo records for catalog tables. You should look at
  // adding constants for col_id_t for catalog tables.
  std::vector<catalog::col_oid_t> GetOidsForRedoRecord(storage::SqlTable *sql_table, RedoRecord *record);

  /**
   * @param oid oid of catalog index
   * @param db_catalog database catalog that has given index
   * @return pointer to catalog index
   */
  storage::index::Index *GetCatalogIndex(catalog::index_oid_t oid,
                                         const common::ManagedPointer<catalog::DatabaseCatalog> &db_catalog);
};
}  // namespace terrier::storage
