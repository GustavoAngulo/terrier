#pragma once

#include <utility>
#include <vector>
#include "common/container/concurrent_map.h"
#include "network/network_io_utils.h"
#include "storage/write_ahead_log/log_consumer_task.h"

namespace terrier::storage {

/**
 * A ReplicationLogConsumerTask is responsible for writing serialized log records over network to any replicas listening
 */
class ReplicationLogConsumerTask final : public LogConsumerTask {
 public:
  // TODO(Gus): Take in IP addresses as vector to support multiple replicas
  /**
   * Constructs a new DiskLogConsumerTask
   * @param persist_interval Interval time for when to persist log file
   * @param persist_threshold threshold of data written since the last persist to trigger another persist
   */
  explicit ReplicationLogConsumerTask(
      const std::string &ip_address, uint16_t port, bool synchronous_replication,
      common::ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue,
                                      common::ConcurrentQueue<storage::SerializedLogsWithRawCommitTime > *filled_buffer_queue)
      : LogConsumerTask(empty_buffer_queue),
        synchronous_replication_(synchronous_replication),
        filled_buffer_queue_(filled_buffer_queue) {
    io_wrapper_ = std::make_unique<network::NetworkIoWrapper>(ip_address, port);
  }

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override;

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override;

  void NotifyOfCommits(const std::vector<transaction::timestamp_t> &commited_txns);

 private:
  // If synchronous replication is enabled. If so, we save the raw commit timestamps for shipped transaction
  const bool synchronous_replication_;

  // The queue containing filled buffers. Task should dequeue filled buffers from this queue to flush
  common::ConcurrentQueue<SerializedLogsWithRawCommitTime> *filled_buffer_queue_;

  friend class LogManager;
  std::unique_ptr<network::NetworkIoWrapper> io_wrapper_;

  // Unique, monotonically increasing identifier for each message sent
  uint64_t message_id_ = 0;

  // Synchronisation primitives to synchronize sending logs over network
  std::mutex replication_lock_;
  // Condition variable to signal replication task thread to wake up and send logs over network or if shutdown has
  // initiated, then quit
  std::condition_variable replication_log_sender_cv_;

  // Used to compute replication delay
  common::ConcurrentMap<transaction::timestamp_t, std::chrono::high_resolution_clock::time_point> raw_commit_ts_;

  /**
   * @brief Main task loop.
   * Loops until task is shut down. In the case of shutdown, guarantees sending of all serialized records at the time of
   * shutdown
   */
  void ReplicationLogConsumerTaskLoop();

  /**
   * @brief Sends all serialized logs to replica(s)
   */
  void SendLogsOverNetwork();

  /**
   * Sends a stop message to replica(s)
   */
  void SendStopReplicationMessage();
};
}  // namespace terrier::storage
