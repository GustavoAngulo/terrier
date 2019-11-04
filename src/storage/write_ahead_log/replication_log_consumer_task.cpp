#include "storage/write_ahead_log/replication_log_consumer_task.h"

#include "loggers/storage_logger.h"
#include "network/itp/itp_packet_writer.h"

namespace terrier::storage {

void ReplicationLogConsumerTask::RunTask() {
  run_task_ = true;
  ReplicationLogConsumerTaskLoop();
}

void ReplicationLogConsumerTask::Terminate() {
  // If the task hasn't run yet, yield the thread until it's started
  while (!run_task_) std::this_thread::yield();
  TERRIER_ASSERT(run_task_, "Cant terminate a task that isn't running");
  // Signal to terminate and force a flush so task persists before LogManager closes buffers
  run_task_ = false;
  replication_log_sender_cv_.notify_one();
}

void ReplicationLogConsumerTask::NotifyOfCommits(const std::vector<terrier::transaction::timestamp_t> &commited_txns) {
  auto now = std::chrono::high_resolution_clock::now();
  for (auto txn : commited_txns) {
    auto search = raw_commit_ts_.Find(txn);
    if (search == raw_commit_ts_.end()) continue;
    auto replication_delay_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - search->second).count();
    STORAGE_LOG_INFO("Txn {} committed with delay {} ns", txn, replication_delay_ns)
  }
}

void ReplicationLogConsumerTask::SendLogsOverNetwork() {
  TERRIER_ASSERT(!filled_buffer_queue_->Empty(), "No logs to send");
  // Grab all buffers availible, and compute total size of data
  std::deque<SerializedLogsWithRawCommitTime> temp_buffer_queue;
  uint64_t data_size = 0;
  SerializedLogsWithRawCommitTime temp_logs;
  while (!filled_buffer_queue_->Empty()) {
    // If our packet grows too big (rare), don't add more buffers
    if (data_size + (2 * common::Constants::LOG_BUFFER_SIZE) >= PACKET_LEN_LIMIT) break;

    filled_buffer_queue_->Dequeue(&temp_logs);
    data_size += temp_logs.first->buffer_size_;
    temp_buffer_queue.push_back(temp_logs);
  }
  // TODO(Gus): Figure out why this assert can fail
  // TERRIER_ASSERT(data_size > 0, "Amount of data to send must be greater than 0");

  // Build the packet
  // TODO(Gus): Consider stashing the packet writer in the class to avoid constant construction/destruction
  network::ITPPacketWriter packet_writer(io_wrapper_->GetWriteQueue());
  packet_writer.BeginReplicationCommand(message_id_++, data_size);
  for (auto &logs : temp_buffer_queue) {
    auto *buffer = logs.first;
    packet_writer.AppendRaw(&buffer->buffer_, buffer->buffer_size_);
    // Return buffer to log manager
    buffer->Reset();
    empty_buffer_queue_->Enqueue(buffer);

    for (const RawCommitTime &commit_ts : logs.second) {
      raw_commit_ts_.Insert(commit_ts.first, commit_ts.second);
    }
  }
  packet_writer.EndReplicationCommand();
  // STORAGE_LOG_INFO("Message {} sending of size {}", message_id_ - 1, data_size)

  // Send packet over network
  io_wrapper_->FlushAllWrites();
}

void ReplicationLogConsumerTask::SendStopReplicationMessage() {
  network::ITPPacketWriter packet_writer(io_wrapper_->GetWriteQueue());
  packet_writer.WriteStopReplicationCommand();
  io_wrapper_->FlushAllWrites();
}

void ReplicationLogConsumerTask::ReplicationLogConsumerTaskLoop() {
  // TODO(Gus): Add some sort of handshake messaging between master and replica before we begin streaming logs
  // TODO(Gus): Add metric exporting when task is finalized
  do {
    {
      std::unique_lock<std::mutex> lock(replication_lock_);
      // We use a CV because we need fast response to when a new buffer appears. We need fast response time because we
      // prioritize a low replication delay
      // TODO(Gus): We will probably need a more tunable heuristic here
      replication_log_sender_cv_.wait(lock, [&] { return !run_task_ || filled_buffer_queue_->UnsafeSize() > 10; });
    }

    // TODO(gus): perf if taking lock is expensive above. We can modify SendLogsOverNetwork to spin in a loop and grab
    // new buffers that may have arrived during last packet construction
    if (!filled_buffer_queue_->Empty()) SendLogsOverNetwork();

  } while (run_task_ || !filled_buffer_queue_->Empty());

  SendStopReplicationMessage();
}

}  // namespace terrier::storage
