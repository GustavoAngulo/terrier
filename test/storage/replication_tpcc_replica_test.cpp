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

class ReplicationTPCCReplicaTest : public TerrierTest {
 protected:
  const uint8_t master_wait_time_ = 10;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 10000;
  const std::chrono::microseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB
  const std::string master_ip_address_ = "172.19.146.5";
  const uint16_t replication_port_ = 9022;
  const bool synchronous_replication_ = false;
  const bool replica_metrics_enabled_ = true;

  // Settings for RecoveryManager
  const std::chrono::milliseconds metrics_overhead_polling_interval_{100};
  const std::chrono::milliseconds metrics_overhead_threshold_{50};

  // Settings for server
  uint32_t max_connections_ = 1;
  uint32_t conn_backlog_ = 1;

  // Settings for replication
  const std::chrono::seconds replication_timeout_{10};

  // General settings
  std::default_random_engine generator_;
  const uint64_t blockstore_size_limit_ = 1000;
  const uint64_t blockstore_reuse_limit_ = 1000;
  const uint64_t buffersegment_size_limit_ = 1000000;
  const uint64_t buffersegment_reuse_limit_ = 1000000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};

  // Settings for gc
  const std::chrono::milliseconds gc_period_{10};

  // Settings for metrics manager
  const std::chrono::milliseconds metrics_period_{100};
  const std::vector<metrics::MetricsComponent> metrics_components_ = {metrics::MetricsComponent::LOGGING,
                                                                      metrics::MetricsComponent::TRANSACTION};

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
  LogManager *replica_log_manager_;
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
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
    for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());

    // If we're doing asynchronous replication, sync the NTP clock
    if (!synchronous_replication_) {
      auto status UNUSED_ATTRIBUTE = system("sudo ntpdate ntp-1.ece.cmu.edu");
      TERRIER_ASSERT(status >= 0, "NTP sync failed");
      TEST_LOG_INFO("Synched NTP clock")
    }
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    TerrierTest::TearDown();
  }

  void InternalSetUp() {
    if (replica_metrics_enabled_) {
      replica_metrics_thread_ = new metrics::MetricsThread(metrics_period_);
      for (const auto component : metrics_components_) {
        replica_metrics_thread_->GetMetricsManager().EnableMetric(component);
      }
      replica_thread_registry_ =
          new common::DedicatedThreadRegistry(common::ManagedPointer(&(replica_metrics_thread_->GetMetricsManager())));
    } else {
      replica_metrics_thread_ = DISABLED;
      replica_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    }

    replica_timestamp_manager_ = new transaction::TimestampManager;
    replica_deferred_action_manager_ = new transaction::DeferredActionManager(replica_timestamp_manager_);

    replica_log_manager_ = DISABLED;
    replica_txn_manager_ = new transaction::TransactionManager(
        replica_timestamp_manager_, replica_deferred_action_manager_, &buffer_pool_, true, replica_log_manager_);
    replica_catalog_ = new catalog::Catalog(replica_txn_manager_, &block_store_);
    replica_gc_ = new storage::GarbageCollector(replica_timestamp_manager_, replica_deferred_action_manager_,
                                                replica_txn_manager_, DISABLED);
    replica_gc_thread_ = new storage::GarbageCollectorThread(replica_gc_, gc_period_);  // Enable background GC

    // Bring up recovery manager
    replica_log_provider_ = new ReplicationLogProvider(replication_timeout_, synchronous_replication_);
    replica_recovery_manager_ = new RecoveryManager(common::ManagedPointer<AbstractLogProvider>(replica_log_provider_),
                                                    common::ManagedPointer(replica_catalog_), replica_txn_manager_,
                                                    replica_deferred_action_manager_, common::ManagedPointer(replica_thread_registry_),
        &block_store_, common::ManagedPointer(replica_metrics_thread_), metrics_overhead_polling_interval_,
        metrics_overhead_threshold_);

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

    // Once master is up, have replica connect, and start recovery
    TEST_LOG_INFO("Safe to bring up master, sleeping for {} sec", master_wait_time_)
    std::this_thread::sleep_for(std::chrono::seconds(master_wait_time_));
    TEST_LOG_INFO("Waking up, connecting to master...")
    replica_recovery_manager_->ConnectToMaster(master_ip_address_, replication_port_ * 2);
    TEST_LOG_INFO("Connected to master!")
    replica_recovery_manager_->StartRecovery();
  }

  void InternalTearDown() {
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
    delete replica_log_manager_;
    delete replica_metrics_thread_;
    delete replica_thread_registry_;
  }

  void RunTPCCReplica() {
    // NOLINTNEXTLINE
    unlink(LOG_FILE_NAME);
    InternalSetUp();

    TEST_LOG_INFO("Entering permanent loop")
    while (true) std::this_thread::yield();

    InternalTearDown();
    unlink(LOG_FILE_NAME);
  }
};

// NOLINTNEXTLINE
TEST_F(ReplicationTPCCReplicaTest, NoMetricsTest) { RunTPCCReplica(); }

}  // namespace terrier::storage
