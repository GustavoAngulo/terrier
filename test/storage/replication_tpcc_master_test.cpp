#include <random>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "metrics/logging_metric.h"
#include "metrics/metrics_thread.h"
#include "network/itp/itp_protocol_interpreter.h"
#include "network/terrier_server.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/index_builder.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_manager.h"
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

#define LOG_FILE_NAME "benchmark.txt"
#define REPLICA_LOG_FILE_NAME "benchmark.txt"

namespace terrier::storage {

class ReplicationTPCCMasterTest : public TerrierTest {
 protected:
  // Settings for log manager
  const uint64_t num_log_buffers_ = 10000;
  const std::chrono::microseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB
  const std::string replica_ip_address_ = "172.19.146.6";
  const uint16_t replication_port_ = 9022;
  const bool synchronous_replication_ = false;

  // Settings for server
  uint32_t max_connections_ = 1;
  uint32_t conn_backlog_ = 1;

  // Settings for replication
  const std::chrono::seconds replication_timeout_{10};

  // Settings for TPCC
  const int8_t num_threads_ = 6;  // defines the number of terminals (workers running txns) and warehouses for the
  // benchmark. Sometimes called scale factor
  const uint32_t num_precomputed_txns_per_worker_ = 100000;  // Number of txns to run per terminal (worker thread)
  tpcc::TransactionWeights txn_weights_;                   // default txn_weights. See definition for values

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
  network::ITPCommandFactory *master_itp_command_factory_;
  network::ITPProtocolInterpreter::Provider *master_itp_protocol_provider_;
  network::ConnectionHandleFactory *master_connection_handle_factory_;
  trafficcop::TrafficCop *master_tcop_;
  network::TerrierServer *master_server_;

  void SetUp() override {
    TerrierTest::SetUp();
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_FILE_NAME);

    // If we're doing asynchronous replication, sync the NTP clock
    if (!synchronous_replication_) {
      system("sudo ntpdate ntp-1.ece.cmu.edu");
      TEST_LOG_INFO("Synched NTP clock")
    }
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    TerrierTest::TearDown();
  }

  void InternalSetUp(const bool replica_logging_enabled, const bool master_metrics_enabled,
                     const bool replica_metrics_enabled) {
    // Bring up components for master node
    //    if (master_metrics_enabled) {
    //      master_metrics_thread_ = new metrics::MetricsThread(metrics_period_);
    //      for (const auto component : metrics_components_) {
    //        master_metrics_thread_->GetMetricsManager().EnableMetric(component);
    //      }
    //      master_thread_registry_ =
    //          new
    //          common::DedicatedThreadRegistry(common::ManagedPointer(&(master_metrics_thread_->GetMetricsManager())));
    //    } else {
    //      master_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    //    }

    TEST_LOG_INFO("REMINDER: Replica should be brought up first")
    master_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    master_log_manager_ = new LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_,
                                         log_persist_interval_, log_persist_threshold_, replica_ip_address_, replication_port_, synchronous_replication_,
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

    // Bring up master network layer
    master_itp_command_factory_ = new network::ITPCommandFactory;
    master_itp_protocol_provider_ =
        new network::ITPProtocolInterpreter::Provider(common::ManagedPointer(master_itp_command_factory_));
    master_tcop_ = new trafficcop::TrafficCop(DISABLED, common::ManagedPointer(master_log_manager_));
    master_connection_handle_factory_ = new network::ConnectionHandleFactory(common::ManagedPointer(master_tcop_));
    try {
      master_server_ = new network::TerrierServer(common::ManagedPointer(master_connection_handle_factory_),
                                                  common::ManagedPointer(master_thread_registry_));
      master_server_->RegisterProtocol(
          replication_port_ * 2,
          common::ManagedPointer<network::ProtocolInterpreter::Provider>(master_itp_protocol_provider_),
          max_connections_, conn_backlog_);
      master_server_->RunServer();
    } catch (NetworkProcessException &exception) {
      TEST_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }
    thread_pool_.Startup();
  }

  void InternalTearDown() {
    // Destroy original catalog. We need to manually call GC followed by a ForceFlush because catalog deletion can defer
    // events that create new transactions, which then need to be flushed before they can be GC'd.
    master_catalog_->TearDown();
    delete master_gc_thread_;
    StorageTestUtil::FullyPerformGC(master_gc_, master_log_manager_);
    master_log_manager_->PersistAndStop();
    thread_pool_.Shutdown();

    // Delete in reverse order of initialization
    master_server_->StopServer();
    delete master_server_;
    delete master_connection_handle_factory_;
    delete master_tcop_;
    delete master_itp_protocol_provider_;
    delete master_itp_command_factory_;
    delete master_gc_;
    delete master_catalog_;
    delete master_txn_manager_;
    delete master_deferred_action_manager_;
    delete master_timestamp_manager_;
    delete master_log_manager_;
    delete master_metrics_thread_;
    delete master_thread_registry_;
  }

  void RunTPCCMaster() {
    // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
    const auto precomputed_args =
        PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

    // NOLINTNEXTLINE
    unlink(LOG_FILE_NAME);
    InternalSetUp(false, false, false);
    // one TPCC worker = one TPCC terminal = one thread
    std::vector<tpcc::Worker> workers;
    workers.reserve(num_threads_);
    tpcc::Builder tpcc_builder(&block_store_, master_catalog_, master_txn_manager_);

    // build the TPCC database
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::BWTREE);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes, as well as force log manager to log all changes
    TEST_LOG_INFO("Populating database")
    tpcc::Loader::PopulateDatabase(master_txn_manager_, tpcc_db, &workers, &thread_pool_);
    master_log_manager_->ForceFlush();
    master_log_manager_->ForceReplicationFlush();
    // Because GC sends the oldest txn messages constantly, we pause it
    master_log_manager_->BlockUntilSync();
    TEST_LOG_INFO("Replica Synced")

    tpcc::Util::RegisterIndexesForGC(&(master_gc_thread_->GetGarbageCollector()), tpcc_db);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    TEST_LOG_INFO("Starting tpcc on master")
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      // run the TPCC workload to completion
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, tpcc_db, precomputed_args, &workers, this] {
          Workload(i, tpcc_db, master_txn_manager_, precomputed_args, &workers);
        });
      }
      thread_pool_.WaitUntilAllFinished();
      master_log_manager_->ForceReplicationFlush();
      master_log_manager_->BlockUntilSync();
    }
    // cleanup
    tpcc::Util::UnregisterIndexesForGC(&(master_gc_thread_->GetGarbageCollector()), tpcc_db);
    delete tpcc_db;
    InternalTearDown();
    CleanUpVarlensInPrecomputedArgs(&precomputed_args);
    unlink(LOG_FILE_NAME);
  }
};

// NOLINTNEXTLINE
TEST_F(ReplicationTPCCMasterTest, NoMetricsTest) {
  // TERRIER_ASSERT(ip_address_ != "127.0.0.1", "Cant use local host for address");
  RunTPCCMaster();
}

}  // namespace terrier::storage
