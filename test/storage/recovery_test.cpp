#include <unordered_map>
#include <vector>
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "storage/garbage_collector_thread.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/sql_table_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class RecoveryTests : public TerrierTest {
 protected:
  storage::LogManager *log_manager_;
  storage::RecoveryManager *recovery_manager_;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::milliseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB

  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};

  const std::chrono::milliseconds gc_period_{10};
  storage::GarbageCollectorThread *gc_thread_;

  void SetUp() override {
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_FILE_NAME);
    log_manager_ = new LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                                  log_persist_threshold_, &pool_);
    TerrierTest::SetUp();
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    DedicatedThreadRegistry::GetInstance().TearDown();
    TerrierTest::TearDown();
  }
};

// This test inserts some tuples with a single transaction into a single table. It then recreates the test table from
// the log, and verifies that this new table is the same as the original table
// TODO(Gus): Test delete
// NOLINTNEXTLINE
TEST_F(RecoveryTests, SingleTableTest) {
  // Initialize table and run workload with logging enabled
  log_manager_->Start();
  LargeSqlTableTestObject tested = LargeSqlTableTestObject::Builder()
                                       .SetNumDatabases(1)
                                       .SetNumTables(1)
                                       .SetMaxColumns(5)
                                       .SetInitialTableSize(1000)
                                       .SetTxnLength(5)
                                       .SetUpdateSelectDeleteRatio({0.7, 0.2, 0.1})
                                       .SetBlockStore(&block_store_)
                                       .SetBufferPool(&pool_)
                                       .SetGenerator(&generator_)
                                       .SetGcOn(true)
                                       .SetVarlenAllowed(true)
                                       .SetLogManager(log_manager_)
                                       .build();

  EXPECT_EQ(1, tested.GetDatabases().size());
  auto database_oid = tested.GetDatabases()[0];
  EXPECT_EQ(1, tested.GetTablesForDatabase(database_oid).size());
  auto table_oid = tested.GetTablesForDatabase(database_oid)[0];

  // Run transactions
  tested.SimulateOltp(100, 4);
  log_manager_->PersistAndStop();

  auto *original_sql_table = tested.GetTable(database_oid, table_oid);
  auto *table_schema = tested.GetSchemaForTable(database_oid, table_oid);

  // Create recovery table and dummy catalog
  auto *recovered_sql_table = new storage::SqlTable(&block_store_, *table_schema, table_oid);
  storage::RecoveryCatalog catalog;
  catalog[database_oid][table_oid] = recovered_sql_table;

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager txn_manager_{&pool_, true, LOGGING_DISABLED};

  // Instantiate recovery manager, and recover the tables.
  recovery_manager_ = new RecoveryManager(LOG_FILE_NAME, &catalog, &txn_manager_);
  recovery_manager_->Recover();

  // Check we recovered all the original tuples
  EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(original_sql_table->Layout(), original_sql_table, recovered_sql_table,
                                                 tested.GetTupleSlotsForTable(database_oid, table_oid),
                                                 recovery_manager_->tuple_slot_map_, &txn_manager_));
}

// This test checks that we recover correctly in a high abort rate workload. We achieve the high abort rate by having
// large transaction lengths (number of updates). Further, to ensure that more aborted transactions flush logs before
// aborting, we have transactions make large updates (by having high number columns). This will cause RedoBuffers to
// fill quickly.
// NOLINTNEXTLINE
TEST_F(RecoveryTests, HighAbortRateTest) {
  // Initialize table and run workload with logging enabled
  log_manager_->Start();
  LargeSqlTableTestObject tested = LargeSqlTableTestObject::Builder()
                                       .SetNumDatabases(1)
                                       .SetNumTables(1)
                                       .SetMaxColumns(1000)
                                       .SetInitialTableSize(1000)
                                       .SetTxnLength(20)
                                       .SetUpdateSelectDeleteRatio({0.7, 0.2, 0.0})
                                       .SetBlockStore(&block_store_)
                                       .SetBufferPool(&pool_)
                                       .SetGenerator(&generator_)
                                       .SetGcOn(true)
                                       .SetVarlenAllowed(false)
                                       .SetLogManager(log_manager_)
                                       .build();

  EXPECT_EQ(1, tested.GetDatabases().size());
  auto database_oid = tested.GetDatabases()[0];
  EXPECT_EQ(1, tested.GetTablesForDatabase(database_oid).size());
  auto table_oid = tested.GetTablesForDatabase(database_oid)[0];

  // Run transactions
  tested.SimulateOltp(100, 4);
  log_manager_->PersistAndStop();

  auto *original_sql_table = tested.GetTable(database_oid, table_oid);
  auto *table_schema = tested.GetSchemaForTable(database_oid, table_oid);

  // Create recovery table and dummy catalog
  auto *recovered_sql_table = new storage::SqlTable(&block_store_, *table_schema, table_oid);
  storage::RecoveryCatalog catalog;
  catalog[database_oid][table_oid] = recovered_sql_table;

  // Start a transaction manager with logging disabled, we don't want to log the log replaying
  transaction::TransactionManager txn_manager_{&pool_, true, LOGGING_DISABLED};

  // Instantiate recovery manager, and recover the tables.
  recovery_manager_ = new RecoveryManager(LOG_FILE_NAME, &catalog, &txn_manager_);
  recovery_manager_->Recover();

  // Check we recovered all the original tuples
  EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(original_sql_table->Layout(), original_sql_table, recovered_sql_table,
                                                 tested.GetTupleSlotsForTable(database_oid, table_oid),
                                                 recovery_manager_->tuple_slot_map_, &txn_manager_));
}

}  // namespace terrier::storage