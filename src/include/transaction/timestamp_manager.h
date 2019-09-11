#pragma once
#include <algorithm>
#include <unordered_set>
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {
// Forward declaration
class LogSerializerTask;
}  // namespace terrier::storage

namespace terrier::transaction {
class TransactionManager;
/**
 * Generates timestamps, and keeps track of the lifetime of transactions (whether they have entered or left the system)
 */
class TimestampManager {
 public:
  /**
   * @return unique timestamp based on current time, and advances one tick
   */
  timestamp_t CheckOutTimestamp() { return time_++; }

  /**
   * @return current time without advancing the tick
   */
  timestamp_t CurrentTime() const { return time_.load(); }

  /**
   * Get the oldest transaction alive (by start timestamp given out by this timestamp manager at this time)
   * Because of concurrent operations, it is not guaranteed that upon return the txn is still alive. However,
   * it is guaranteed that the return timestamp is older than any transactions live.
   * @return timestamp that is older than any transactions alive
   */
  timestamp_t OldestTransactionStartTime() {
    common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);

    // If we have the oldest start time cached, we return it
    if (oldest_txn_start_time_ != INVALID_TXN_TIMESTAMP) return oldest_txn_start_time_;

    const auto &oldest_txn = std::min_element(curr_running_txns_.cbegin(), curr_running_txns_.cend());
    timestamp_t result;
    if (oldest_txn != curr_running_txns_.end()) {
      result = *oldest_txn;
      // Only cache if there actually is an active txn
      oldest_txn_start_time_ = result;
    } else {
      result = time_.load();
    }
    return result;
  }

 private:
  friend class TransactionManager;
  friend class storage::LogSerializerTask;
  timestamp_t BeginTransaction() {
    timestamp_t start_time;
    {
      // There is a three-way race that needs to be prevented.  Specifically, we
      // cannot allow both a transaction to commit and the GC to poll for the
      // oldest running transaction in between this transaction acquiring its
      // begin timestamp and getting inserted into the current running
      // transactions list.  Using the current running transactions latch
      // prevents the GC from polling and stops the race.  This allows us to
      // replace acquiring a shared instance of the commit latch with a
      // read-only spin-latch and move the allocation out of a critical section.
      common::SpinLatch::ScopedSpinLatch running_guard(&curr_running_txns_latch_);
      start_time = time_++;

      const auto ret UNUSED_ATTRIBUTE = curr_running_txns_.emplace(start_time);
      TERRIER_ASSERT(ret.second, "commit start time should be globally unique");
    }  // Release latch on current running transactions
    return start_time;
  }

  void RemoveTransaction(timestamp_t timestamp) {
    common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
    const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(timestamp);
    TERRIER_ASSERT(ret == 1, "erased timestamp did not exist");

    // If we are removing the cached oldest active txn, we invalidate the cached timestamp
    if (timestamp == oldest_txn_start_time_) oldest_txn_start_time_ = INVALID_TXN_TIMESTAMP;
  }

  // TODO(Tianyu): Timestamp generation needs to be more efficient (batches)
  // TODO(Tianyu): We don't handle timestamp wrap-arounds. I doubt this would be an issue any time soon.
  std::atomic<timestamp_t> time_{INITIAL_TXN_TIMESTAMP};
  // We cache the oldest txn start time
  timestamp_t oldest_txn_start_time_{INVALID_TXN_TIMESTAMP};
  // TODO(Matt): consider a different data structure if this becomes a measured bottleneck
  std::unordered_set<timestamp_t> curr_running_txns_;
  mutable common::SpinLatch curr_running_txns_latch_;
};
}  // namespace terrier::transaction
