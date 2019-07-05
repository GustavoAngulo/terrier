#pragma once
#include <algorithm>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {

class LargeSqlTableTestObject;
class RandomSqlTableTransaction;
using TupleEntry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>;
using VersionedSnapshots = std::map<transaction::timestamp_t, TableSnapshot>;

/**
 * A RandomSqlTableTransaction class provides a simple interface to simulate a transaction that interfaces with the
 * SqlTable layer running in the system. Does not provide bookkeeping for correctness functionality.
 */
class RandomSqlTableTransaction {
 public:
  /**
   * Initializes a new RandomSqlTableTransaction to work on the given test object
   * @param test_object the test object that runs this transaction
   */
  explicit RandomSqlTableTransaction(LargeSqlTableTestObject *test_object);

  /**
   * Destructs a random workload transaction
   */
  ~RandomSqlTableTransaction();

  /**
   * Randomly updates a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomUpdate(Random *generator);

  /**
   * Randomly deletes a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomDelete(Random *generator);

  /**
   * Randomly selects a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomSelect(Random *generator);

  /**
   * Finish the simulation of this transaction. The underlying transaction will either commit or abort.
   */
  void Finish();

 private:
  friend class LargeSqlTableTestObject;
  LargeSqlTableTestObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for correctness checks
  bool aborted_;
};

/**
 * A LargeSqlTableTestObject can bootstrap multiple databases and tables, and runs randomly generated workloads
 * concurrently against the tables to simulate a real run of the system.
 *
 * So far we only do updates and selects, as inserts and deletes are not given much special meaning without the index.
 */
class LargeSqlTableTestObject {
 private:
  /*
   * Holds meta data for tables created by test object
   */
  struct SqlTableMetadata {
    // Pointer to sql table
    storage::SqlTable *table_;
    // Schema for sql table
    catalog::Schema *schema_;
    // Tuple slots inserted into this sql table
    std::vector<storage::TupleSlot> inserted_tuples_;
    // Latch to protect inserted tuples to allow for concurrent transactions
    common::SpinLatch inserted_tuples_latch_;
    // Buffer for select queries. Not thread safe, but since we aren't doing bookkeeping, it doesn't matter
    byte *buffer_;
  };

 public:
  /**
   * Builder class for LargeSqlTableTestObject
   */
  class Builder {
   public:
    /**
     * @param num_databases the number of databases in the generated test object
     * @return self-reference for method chaining
     */
    Builder &SetNumDatabases(uint16_t num_databases) {
      builder_num_databases_ = num_databases;
      return *this;
    }

    /**
     * @param num_tables the number of tables PER DATABASE in the generated test object
     * @return self-reference for method chaining
     */
    Builder &SetNumTables(uint16_t num_tables) {
      builder_num_tables_ = num_tables;
      return *this;
    }

    /**
     * @param max_columns the max number of columns in the generated test tables
     * @return self-reference for method chaining
     */
    Builder &SetMaxColumns(uint16_t max_columns) {
      builder_max_columns_ = max_columns;
      return *this;
    }

    /**
     * @param initial_table_size number of tuples each table should have initially
     * @return self-reference for method chaining
     */
    Builder &SetInitialTableSize(uint32_t initial_table_size) {
      builder_initial_table_size_ = initial_table_size;
      return *this;
    }

    /**
     * @param txn_length length of every simulated transaction, in number of operations (select or update)
     * @return self-reference for method chaining
     */
    Builder &SetTxnLength(uint32_t txn_length) {
      builder_txn_length_ = txn_length;
      return *this;
    }

    /**
     * @param update_select_delete_ratio the ratio of updates vs. select vs. deletes in the generated transaction
     *                            (e.g. {0.2, 0.7. 0.1} will be 20% updates, 70% reads, and 10% deletes)
     * @warning the number of deletes should not exceed the number of tuples inserted
     * @return self-reference for method chaining
     */
    Builder &SetUpdateSelectDeleteRatio(std::vector<double> update_select_delete_ratio) {
      builder_update_select_delete_ratio_ = std::move(update_select_delete_ratio);
      return *this;
    }

    /**
     * @param block_store the block store to use for the underlying data table
     * @return self-reference for method chaining
     */
    Builder &SetBlockStore(storage::BlockStore *block_store) {
      builder_block_store_ = block_store;
      return *this;
    }

    /**
     * @param buffer_pool the buffer pool to use for simulated transactions
     * @return self-reference for method chaining
     */
    Builder &SetBufferPool(storage::RecordBufferSegmentPool *buffer_pool) {
      builder_buffer_pool_ = buffer_pool;
      return *this;
    }

    /**
     * @param generator the random generator to use for the test
     * @return self-reference for method chaining
     */
    Builder &SetGenerator(std::default_random_engine *generator) {
      builder_generator_ = generator;
      return *this;
    }

    /**
     * @param gc_on whether gc is enabled
     * @return self-reference for method chaining
     */
    Builder &SetGcOn(bool gc_on) {
      builder_gc_on_ = gc_on;
      return *this;
    }

    /**
     * @param log_manager the log manager to use for this test object, or nullptr (LOGGING_DISABLED) if
     *                    logging is not needed.
     * @return self-reference for method chaining
     */
    Builder &SetLogManager(storage::LogManager *log_manager) {
      builder_log_manager_ = log_manager;
      return *this;
    }

    /**
     * @param varlen_allowed if we allow varlen columns to show up in the block layout
     * @return self-reference for method chaining
     */
    Builder &SetVarlenAllowed(bool varlen_allowed) {
      varlen_allowed_ = varlen_allowed;
      return *this;
    }

    /**
     * @return the constructed LargeSqlTableTestObject using the parameters provided
     * (or default ones if not supplied).
     */
    LargeSqlTableTestObject build();

   private:
    uint16_t builder_num_databases_ = 1;
    uint16_t builder_num_tables_ = 1;
    uint16_t builder_max_columns_ = 25;
    uint32_t builder_initial_table_size_ = 25;
    uint32_t builder_txn_length_ = 25;
    std::vector<double> builder_update_select_delete_ratio_;
    storage::BlockStore *builder_block_store_ = nullptr;
    storage::RecordBufferSegmentPool *builder_buffer_pool_ = nullptr;
    std::default_random_engine *builder_generator_ = nullptr;
    bool builder_gc_on_ = true;
    storage::LogManager *builder_log_manager_ = LOGGING_DISABLED;
    bool varlen_allowed_ = false;
  };

  /**
   * Destructs a LargeSqlTableTestObject
   */
  ~LargeSqlTableTestObject();

  /**
   * @return the transaction manager used by this test
   */
  transaction::TransactionManager *GetTxnManager() { return &txn_manager_; }

  /**
   * Simulate an oltp workload, running the specified number of total transactions while allowing the specified number
   * of transactions to run concurrently. Transactions are generated using the configuration provided on construction.
   *
   * @param num_transactions total number of transactions to run
   * @param num_concurrent_txns number of transactions allowed to run concurrently
   * @return number of aborted transactions
   */
  uint64_t SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns);

  /**
   * @return database oids
   */
  const std::vector<catalog::db_oid_t> &GetDatabases() const { return database_oids_; }

  /**
   * @param oid database oid
   * @return tables oids for a given database
   */
  const std::vector<catalog::table_oid_t> &GetTablesForDatabase(catalog::db_oid_t oid) {
    TERRIER_ASSERT(table_oids_.find(oid) != table_oids_.end(), "Requested database was not created");
    return table_oids_[oid];
  }

  /**
   * @param db_oid database oid
   * @param table_oid table oid
   * @return SqlTable pointer for requested table
   */
  const storage::SqlTable *GetTable(catalog::db_oid_t db_oid, catalog::table_oid_t table_oid) {
    TERRIER_ASSERT(tables_.find(db_oid) != tables_.end(), "Requested database was not created");
    TERRIER_ASSERT(tables_[db_oid].find(table_oid) != tables_[db_oid].end(), "Requested table was not created");
    return tables_[db_oid][table_oid]->table_;
  }

  /**
   * @param db_oid database oid
   * @param table_oid table oid
   * @return schema requested table
   */
  const catalog::Schema *GetSchemaForTable(catalog::db_oid_t db_oid, catalog::table_oid_t table_oid) {
    TERRIER_ASSERT(tables_.find(db_oid) != tables_.end(), "Requested database was not created");
    TERRIER_ASSERT(tables_[db_oid].find(table_oid) != tables_[db_oid].end(), "Requested table was not created");
    return tables_[db_oid][table_oid]->schema_;
  }

  const std::vector<storage::TupleSlot> &GetTupleSlotsForTable(catalog::db_oid_t db_oid,
                                                               catalog::table_oid_t table_oid) {
    TERRIER_ASSERT(tables_.find(db_oid) != tables_.end(), "Requested database was not created");
    TERRIER_ASSERT(tables_[db_oid].find(table_oid) != tables_[db_oid].end(), "Requested table was not created");
    return tables_[db_oid][table_oid]->inserted_tuples_;
  }

 private:
  /**
   * Initializes a test object with the given configuration
   * @param num_databases number of databases to create
   * @param num_tables number of tables per database to create
   * @param max_columns the max number of columns in each generated test table
   * @param initial_table_size number of tuples each table should have
   * @param txn_length length of every simulated transaction, in number of operations (select or update)
   * @param update_select_delete_ratio the ratio of updates vs. select vs. deletes in the generated transaction (e.g.
   * {0.2, 0.7. 0.1} will be 20% updates, 70% reads, and 10% deletes)
   * @param block_store the block store to use for the underlying data table
   * @param buffer_pool the buffer pool to use for simulated transactions
   * @param generator the random generator to use for the test
   * @param gc_on whether gc is enabled
   */
  LargeSqlTableTestObject(uint16_t num_databases, uint16_t num_tables, uint16_t max_columns,
                          uint32_t initial_table_size, uint32_t txn_length,
                          std::vector<double> update_select_delete_ratio, storage::BlockStore *block_store,
                          storage::RecordBufferSegmentPool *buffer_pool, std::default_random_engine *generator,
                          bool gc_on, storage::LogManager *log_manager, bool varlen_allowed);

  void SimulateOneTransaction(RandomSqlTableTransaction *txn, uint32_t txn_id);

  template <class Random>
  void PopulateInitialTables(uint16_t num_databases, uint16_t num_tables, uint16_t max_columns, uint32_t num_tuples,
                             bool varlen_allowed, storage::BlockStore *block_store, Random *generator);

  friend class RandomSqlTableTransaction;
  uint32_t txn_length_;
  std::vector<double> update_select_delete_ratio_;
  std::default_random_engine *generator_;
  transaction::TransactionManager txn_manager_;
  transaction::TransactionContext *initial_txn_;
  bool gc_on_;
  uint64_t abort_count_ = 0;
  // So we can easily get a random database and table oid
  std::vector<catalog::db_oid_t> database_oids_;
  std::unordered_map<catalog::db_oid_t, std::vector<catalog::table_oid_t>> table_oids_;

  // Maps database and table oids to struct holding testing metadata for each table
  std::unordered_map<catalog::db_oid_t, std::unordered_map<catalog::table_oid_t, SqlTableMetadata *>> tables_;
};
}  // namespace terrier