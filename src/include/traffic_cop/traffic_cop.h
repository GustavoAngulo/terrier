#pragma once
#include <storage/write_ahead_log/replication_log_consumer_task.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "network/postgres/postgres_protocol_utils.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/write_ahead_log/log_manager.h"
#include "traffic_cop/portal.h"
#include "traffic_cop/sqlite.h"
#include "traffic_cop/statement.h"

namespace terrier::trafficcop {

/**
 *
 * Traffic Cop of the database. It provides access to all the backend components.
 *
 * *Should be a singleton*
 *
 */

class TrafficCop {
 public:
  /**
   * @param replication_log_provider if given, the tcop will forward replication logs to this provider
   */
  explicit TrafficCop(common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider = DISABLED,
                      common::ManagedPointer<storage::LogManager> log_manager = DISABLED)
      : replication_log_provider_(replication_log_provider), log_manager_(log_manager) {}

  virtual ~TrafficCop() = default;

  /**
   * Returns the execution engine.
   * @return the execution engine.
   */
  SqliteEngine *GetExecutionEngine() { return &sqlite_engine_; }

  /**
   * Hands a buffer of logs to replication
   * @param buffer buffer containing logs
   */
  void HandBufferToReplication(uint64_t message_id, std::unique_ptr<network::ReadBuffer> buffer) {
    TERRIER_ASSERT(replication_log_provider_ != DISABLED,
                   "Should not be handing off logs if no log provider was given");
    replication_log_provider_->HandBufferToReplication(message_id, std::move(buffer));
  }

  void NotifyOfCommits(const std::vector<terrier::transaction::timestamp_t> &commited_txns) {
    log_manager_->NotifyOfCommits(commited_txns);
  }

  void NotifyOfSync() {
    log_manager_->NotifyOfSync();
  }

  /**
   * Signals to the replication component to stop
   */
  void StopReplication() {
    TERRIER_ASSERT(replication_log_provider_ != DISABLED,
                   "Can't stop replication, no replication log provider was given");
    replication_log_provider_->EndReplication();
  }

 private:
  SqliteEngine sqlite_engine_;
  // Hands logs off to replication component. TCop should forward these logs through this provider.
  common::ManagedPointer<storage::ReplicationLogProvider> replication_log_provider_;
  common::ManagedPointer<storage::LogManager> log_manager_;
};

}  // namespace terrier::trafficcop
