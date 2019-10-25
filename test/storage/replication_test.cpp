#include <string>
#include <unordered_map>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "network/itp/itp_protocol_interpreter.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/index_builder.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/sql_table_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

// Make sure that if you create additional files, you call unlink on them after the test finishes. Otherwise, repeated
// executions will read old test's data, and the cause of the errors will be hard to identify. Trust me it will drive
// you nuts...
#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class ReplicationTests : public TerrierTest {
 protected:
  // This is an estimate for how long we expect for all the logs to arrive at the replica and be replayed by the
  // recovery manager. You should be pessimistic in setting this number, be sure it is an overestimate. Each test should
  // set it to its own value depending on the amount of work it's doing
  std::chrono::seconds replication_delay_estimate_;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB
  const std::string ip_address_ = "127.0.0.1";
  const uint16_t replication_port_ = 9022;

  // Settings for server
  uint32_t max_connections_ = 1;
  uint32_t conn_backlog_ = 1;

  // Settings for replication
  const std::chrono::seconds replication_timeout_{10};

  // Settings for OLTP style tests
  const uint64_t num_txns_ = 100;
  const uint64_t num_concurrent_txns_ = 4;

  // General settings
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};

  // Settings for gc
  const std::chrono::milliseconds gc_period_{10};

  // Master node's components (prefixed with "master_") in order of initialization
  // We need:
  //  1. ThreadRegistry
  //  2. LogManager
  //  3. TxnManager
  //  4. Catalog
  //  4. GC
  common::DedicatedThreadRegistry *master_thread_registry_;
  LogManager *master_log_manager_;
  transaction::TimestampManager *master_timestamp_manager_;
  transaction::DeferredActionManager *master_deferred_action_manager_;
  transaction::TransactionManager *master_txn_manager_;
  catalog::Catalog *master_catalog_;
  storage::GarbageCollector *master_gc_;
  storage::GarbageCollectorThread *master_gc_thread_;

  // Replica node's components (prefixed with "replica_") in order of initialization
  //  1. Thread Registry
  //  2. TxnManager
  //  3. Catalog
  //  4. GC
  //  5. RecoveryManager
  //  6. TrafficCop
  //  7. TerrierServer
  common::DedicatedThreadRegistry *replica_thread_registry_;
  transaction::TimestampManager *replica_timestamp_manager_;
  transaction::DeferredActionManager *replica_deferred_action_manager_;
  transaction::TransactionManager *replica_txn_manager_;
  catalog::Catalog *replica_catalog_;
  storage::GarbageCollector *replica_gc_;
  storage::GarbageCollectorThread *replica_gc_thread_;
  storage::ReplicationLogProvider *replica_log_provider_;
  storage::RecoveryManager *replica_recovery_manager_;
  network::ITPCommandFactory *replica_itp_command_factory_;
  network::ITPProtocolInterpreter::Provider *replica_itp_protocol_provider_;
  network::ConnectionHandleFactory *replica_connection_handle_factory_;
  trafficcop::TrafficCop *replica_tcop_;
  network::TerrierServer *replica_server_;

  void SetUp() override {
    TerrierTest::SetUp();
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_FILE_NAME);

    // We first bring up the replica, then the master node
    replica_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    replica_timestamp_manager_ = new transaction::TimestampManager;
    replica_deferred_action_manager_ = new transaction::DeferredActionManager(replica_timestamp_manager_);
    replica_txn_manager_ = new transaction::TransactionManager(
        replica_timestamp_manager_, replica_deferred_action_manager_, &buffer_pool_, true, DISABLED);
    replica_catalog_ = new catalog::Catalog(replica_txn_manager_, &block_store_);
    replica_gc_ = new storage::GarbageCollector(replica_timestamp_manager_, replica_deferred_action_manager_,
                                                replica_txn_manager_, DISABLED);
    replica_gc_thread_ = new storage::GarbageCollectorThread(replica_gc_, gc_period_);  // Enable background GC

    // Bring up recovery manager
    replica_log_provider_ = new ReplicationLogProvider(replication_timeout_);
    replica_recovery_manager_ = new RecoveryManager(common::ManagedPointer<AbstractLogProvider>(replica_log_provider_),
                                                    common::ManagedPointer(replica_catalog_), replica_txn_manager_,
                                                    replica_deferred_action_manager_,
                                                    common::ManagedPointer(replica_thread_registry_), &block_store_);
    replica_recovery_manager_->StartRecovery();

    // Bring up network layer
    replica_itp_command_factory_ = new network::ITPCommandFactory;
    replica_itp_protocol_provider_ =
        new network::ITPProtocolInterpreter::Provider(common::ManagedPointer(replica_itp_command_factory_));
    replica_tcop_ = new trafficcop::TrafficCop(common::ManagedPointer(replica_log_provider_));
    replica_connection_handle_factory_ = new network::ConnectionHandleFactory(common::ManagedPointer(replica_tcop_));
    try {
      replica_server_ = new network::TerrierServer(common::ManagedPointer(replica_connection_handle_factory_),
                                                   common::ManagedPointer(replica_thread_registry_));
      replica_server_->RegisterProtocol(
          replication_port_,
          common::ManagedPointer<network::ProtocolInterpreter::Provider>(replica_itp_protocol_provider_),
          max_connections_, conn_backlog_);
      replica_server_->RunServer();
    } catch (NetworkProcessException &exception) {
      TEST_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }

    // Bring up components for master node
    master_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    master_log_manager_ = new LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_,
                                         log_persist_interval_, log_persist_threshold_, ip_address_, replication_port_,
                                         &buffer_pool_, common::ManagedPointer(master_thread_registry_));
    master_log_manager_->Start();
    master_timestamp_manager_ = new transaction::TimestampManager;
    master_deferred_action_manager_ = new transaction::DeferredActionManager(master_timestamp_manager_);
    master_txn_manager_ = new transaction::TransactionManager(
        master_timestamp_manager_, master_deferred_action_manager_, &buffer_pool_, true, master_log_manager_);
    master_catalog_ = new catalog::Catalog(master_txn_manager_, &block_store_);
    master_gc_ = new storage::GarbageCollector(master_timestamp_manager_, master_deferred_action_manager_,
                                               master_txn_manager_, DISABLED);
    master_gc_thread_ = new storage::GarbageCollectorThread(master_gc_, gc_period_);  // Enable background GC
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    TerrierTest::TearDown();

    // Destroy original catalog. We need to manually call GC followed by a ForceFlush because catalog deletion can defer
    // events that create new transactions, which then need to be flushed before they can be GC'd.
    master_catalog_->TearDown();
    delete master_gc_thread_;
    StorageTestUtil::FullyPerformGC(master_gc_, master_log_manager_);
    master_log_manager_->PersistAndStop();

    // Delete in reverse order of initialization
    delete master_gc_;
    delete master_catalog_;
    delete master_txn_manager_;
    delete master_deferred_action_manager_;
    delete master_timestamp_manager_;
    delete master_log_manager_;
    delete master_thread_registry_;

    // Replication should be finished by now, each test should ensure it waits for ample time for everything to
    // replicate
    replica_recovery_manager_->WaitForRecoveryToFinish();
    replica_catalog_->TearDown();
    delete replica_gc_thread_;
    StorageTestUtil::FullyPerformGC(replica_gc_, DISABLED);

    replica_server_->StopServer();
    delete replica_server_;
    delete replica_connection_handle_factory_;
    delete replica_tcop_;
    delete replica_itp_protocol_provider_;
    delete replica_itp_command_factory_;
    delete replica_recovery_manager_;
    delete replica_log_provider_;
    delete replica_gc_;
    delete replica_catalog_;
    delete replica_txn_manager_;
    delete replica_deferred_action_manager_;
    delete replica_timestamp_manager_;
  }

  void RunTest(const LargeSqlTableTestConfiguration &config, std::chrono::seconds delay) {
    // Run workload
    auto *tested = new LargeSqlTableTestObject(config, master_txn_manager_, master_catalog_, &block_store_, &generator_);
    tested->SimulateOltp(num_txns_, num_concurrent_txns_);

    replication_delay_estimate_ = delay;
    std::this_thread::sleep_for(replication_delay_estimate_);

    TERRIER_ASSERT(replica_log_provider_->arrived_buffer_queue_.empty(), "All buffers should be processed by now");
    TERRIER_ASSERT(replica_recovery_manager_->deferred_txns_.empty(), "All txns should be processed by now");

    // Check we recovered all the original tables
    for (auto &database : tested->GetTables()) {
      auto database_oid = database.first;
      for (auto &table_oid : database.second) {
        // Get original sql table
        auto original_txn = master_txn_manager_->BeginTransaction();
        auto original_sql_table =
            master_catalog_->GetDatabaseCatalog(original_txn, database_oid)->GetTable(original_txn, table_oid);

        // Get Recovered table
        auto *recovery_txn = replica_txn_manager_->BeginTransaction();
        auto db_catalog = replica_catalog_->GetDatabaseCatalog(recovery_txn, database_oid);
        EXPECT_TRUE(db_catalog != nullptr);
        auto recovered_sql_table = db_catalog->GetTable(recovery_txn, table_oid);
        EXPECT_TRUE(recovered_sql_table != nullptr);

        EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
            original_sql_table->table_.layout_, original_sql_table, recovered_sql_table,
            tested->GetTupleSlotsForTable(database_oid, table_oid), replica_recovery_manager_->tuple_slot_map_, master_txn_manager_,
            replica_txn_manager_));
        master_txn_manager_->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        replica_txn_manager_->Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    }
    delete tested;
  }

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn, catalog::Catalog *catalog,
                                   const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(txn, database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }

  catalog::namespace_oid_t CreateNamespace(transaction::TransactionContext *txn,
                                           common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                           const std::string &namespace_name) {
    auto namespace_oid = db_catalog->CreateNamespace(txn, namespace_name);
    EXPECT_TRUE(namespace_oid != catalog::INVALID_NAMESPACE_OID);
    return namespace_oid;
  }
};

// This test inserts some tuples into a single table. It then recreates the test table from
// the log, and verifies that this new table is the same as the original table
// NOLINTNEXTLINE
TEST_F(ReplicationTests, SingleTableTest) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
      .SetNumDatabases(1)
      .SetNumTables(1)
      .SetMaxColumns(5)
      .SetInitialTableSize(1000)
      .SetTxnLength(5)
      .SetInsertUpdateSelectDeleteRatio({0.2, 0.5, 0.2, 0.1})
      .SetVarlenAllowed(true)
      .Build();
  RunTest(config, std::chrono::seconds(1));
}

// This test checks that we recover correctly in a high abort rate workload. We achieve the high abort rate by having
// large transaction lengths (number of updates). Further, to ensure that more aborted transactions flush logs before
// aborting, we have transactions make large updates (by having high number columns). This will cause RedoBuffers to
// fill quickly.
// NOLINTNEXTLINE
TEST_F(ReplicationTests, HighAbortRateTest) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
      .SetNumDatabases(1)
      .SetNumTables(1)
      .SetMaxColumns(1000)
      .SetInitialTableSize(1000)
      .SetTxnLength(20)
      .SetInsertUpdateSelectDeleteRatio({0.2, 0.5, 0.3, 0.0})
      .SetVarlenAllowed(true)
      .Build();
  RunTest(config, std::chrono::seconds(3));
}

// This test inserts some tuples into multiple tables across multiple databases. It then recovers these tables, and
// verifies that the recovered tables are equal to the test tables.
// NOLINTNEXTLINE
TEST_F(ReplicationTests, MultiDatabaseTest) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
      .SetNumDatabases(3)
      .SetNumTables(5)
      .SetMaxColumns(5)
      .SetInitialTableSize(100)
      .SetTxnLength(5)
      .SetInsertUpdateSelectDeleteRatio({0.3, 0.6, 0.0, 0.1})
      .SetVarlenAllowed(true)
      .Build();
  RunTest(config, std::chrono::seconds(2));
}

TEST_F(ReplicationTests, CreateDatabaseTest) {
  std::string database_name = "testdb";
  // Create a database and commit, we should see this one after replication
  auto *txn = master_txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, master_catalog_, database_name);
  master_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  replication_delay_estimate_ = std::chrono::seconds(1);
  std::this_thread::sleep_for(replication_delay_estimate_);

  txn = replica_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, replica_catalog_->GetDatabaseOid(txn, database_name));
  EXPECT_TRUE(replica_catalog_->GetDatabaseCatalog(txn, db_oid));
  replica_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

TEST_F(ReplicationTests, CreateNamespaceTest) {
  std::string database_name = "testdb";
  std::string namespace_name = "testns";
  // Create a database and commit, we should see this one after replication
  auto *txn = master_txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, master_catalog_, database_name);
  auto db_catalog = master_catalog_->GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(db_catalog);
  auto ns_oid = CreateNamespace(txn, db_catalog, namespace_name);
  master_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  replication_delay_estimate_ = std::chrono::seconds(1);
  std::this_thread::sleep_for(replication_delay_estimate_);

  txn = replica_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, replica_catalog_->GetDatabaseOid(txn, database_name));
  auto recovered_db_catalog = replica_catalog_->GetDatabaseCatalog(txn, db_oid);
  EXPECT_TRUE(recovered_db_catalog);
  EXPECT_EQ(ns_oid, recovered_db_catalog->GetNamespaceOid(txn, namespace_name));
  replica_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier::storage
