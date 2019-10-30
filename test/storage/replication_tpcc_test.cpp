#include <string>
#include <unordered_map>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "metrics/logging_metric.h"
#include "metrics/metrics_thread.h"
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
#include "util/tpcc/builder.h"
#include "util/tpcc/database.h"
#include "util/tpcc/loader.h"
#include "util/tpcc/worker.h"
#include "util/tpcc/workload.h"

// Make sure that if you create additional files, you call unlink on them after the test finishes. Otherwise, repeated
// executions will read old test's data, and the cause of the errors will be hard to identify. Trust me it will drive
// you nuts...
#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class ReplicationTPCCTests : public TerrierTest {
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

  // Settings for TPCC
  const int8_t num_threads_ = 4;  // defines the number of terminals (workers running txns) and warehouses for the
  // benchmark. Sometimes called scale factor
  const uint32_t num_precomputed_txns_per_worker_ = 10000;  // Number of txns to run per terminal (worker thread)
  tpcc::TransactionWeights txn_weights_;                    // default txn_weights. See definition for values

  // General settings
  std::default_random_engine generator_;
  const uint64_t blockstore_size_limit_ = 1000;
  const uint64_t blockstore_reuse_limit_ = 1000;
  const uint64_t buffersegment_size_limit_ = 1000000;
  const uint64_t buffersegment_reuse_limit_ = 1000000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};

  // Settings for gc
  const std::chrono::milliseconds gc_period_{10};

  // Settings for metrics manager
  const std::chrono::milliseconds metrics_period_{100};
  const std::vector<metrics::MetricsComponent> metrics_components_ = {metrics::MetricsComponent::LOGGING,
                                                                      metrics::MetricsComponent::TRANSACTION};

  // Master node's components (prefixed with "master_") in order of initialization
  // We need:
  //  1. ThreadRegistry
  //  2. LogManager
  //  3. TxnManager
  //  4. Catalog
  //  4. GC
  metrics::MetricsThread *master_metrics_thread_ = DISABLED;
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
  metrics::MetricsThread *replica_metrics_thread_ = DISABLED;
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
  }

  void InternalSetUp(const bool replica_logging_enabled, const bool master_metrics_enabled,
                     const bool replica_metrics_enabled) {
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
    if (master_metrics_enabled) {
      master_metrics_thread_ = new metrics::MetricsThread(metrics_period_);
      for (const auto component : metrics_components_) {
        master_metrics_thread_->GetMetricsManager().EnableMetric(component);
      }
      master_thread_registry_ =
          new common::DedicatedThreadRegistry(common::ManagedPointer(&(master_metrics_thread_->GetMetricsManager())));
    } else {
      master_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    }

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
    thread_pool_.Shutdown();

    // Delete in reverse order of initialization
    delete master_gc_;
    delete master_catalog_;
    delete master_txn_manager_;
    delete master_deferred_action_manager_;
    delete master_timestamp_manager_;
    delete master_log_manager_;
    delete master_metrics_thread_;
    delete master_thread_registry_;

    // Replication should be finished by now, each test should ensure it waits for ample time for everything to
    // replicate
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
    delete replica_metrics_thread_;
    delete replica_thread_registry_;
  }

  void RunTPCC(const bool replica_logging_enabled, const bool master_metrics_enabled,
               const bool replica_metrics_enabled, const storage::index::IndexType type) {
    InternalSetUp(replica_logging_enabled, master_metrics_enabled, replica_metrics_enabled);

    // one TPCC worker = one TPCC terminal = one thread
    std::vector<tpcc::Worker> workers;
    workers.reserve(num_threads_);

    if (replica_metrics_enabled) {
      replica_metrics_thread_ = new metrics::MetricsThread(metrics_period_);
      for (const auto component : metrics_components_) {
        replica_metrics_thread_->GetMetricsManager().EnableMetric(component);
      }

      // Override thread registry
      delete replica_thread_registry_;
      replica_thread_registry_ =
          new common::DedicatedThreadRegistry(common::ManagedPointer(&(replica_metrics_thread_->GetMetricsManager())));
    }

    tpcc::Builder tpcc_builder(&block_store_, master_catalog_, master_txn_manager_);

    // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
    const auto precomputed_args =
        PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

    // build the TPCC database
    auto *const tpcc_db = tpcc_builder.Build(type);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes, as well as force log manager to log all changes
    tpcc::Loader::PopulateDatabase(master_txn_manager_, tpcc_db, &workers, &thread_pool_);
    master_log_manager_->ForceFlush();

    tpcc::Util::RegisterIndexesForGC(&(master_gc_thread_->GetGarbageCollector()), tpcc_db);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion
    for (int8_t i = 0; i < num_threads_; i++) {
      thread_pool_.SubmitTask([i, tpcc_db, precomputed_args, &workers, this] {
        Workload(i, tpcc_db, master_txn_manager_, precomputed_args, &workers);
      });
    }
    thread_pool_.WaitUntilAllFinished();
    replica_recovery_manager_->WaitForRecoveryToFinish();

    // cleanup
    tpcc::Util::UnregisterIndexesForGC(&(master_gc_thread_->GetGarbageCollector()), tpcc_db);
    delete tpcc_db;

    CleanUpVarlensInPrecomputedArgs(&precomputed_args);
  }
};

// NOLINTNEXTLINE
TEST_F(ReplicationTPCCTests, DISABLED_NoMetricsTest) {
  replication_delay_estimate_ = std::chrono::seconds(5);
  RunTPCC(false, false, false, storage::index::IndexType::BWTREE);
}

// NOLINTNEXTLINE
TEST_F(ReplicationTPCCTests, MasterMetricsTest) {
  replication_delay_estimate_ = std::chrono::seconds(5);
  RunTPCC(false, true, false, storage::index::IndexType::BWTREE);
}

}  // namespace terrier::storage
