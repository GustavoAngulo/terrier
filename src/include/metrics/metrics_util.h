#pragma once

#include <chrono>  // NOLINT


#define NUM_METRICS 288
#define NUM_EXPORTERS 5
#define METRICS_FACTOR (NUM_METRICS / NUM_EXPORTERS)

namespace terrier::metrics {

/**
 * Static utility methods for the metrics component
 */
struct MetricsUtil {
  MetricsUtil() = delete;

  /**
   * Time since the epoch (however this architecture defines that) in microseconds. Really only useful as a
   * monotonically increasing time point
   */
  static uint64_t Now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::high_resolution_clock::now().time_since_epoch())
        .count();
  }
};
}  // namespace terrier::metrics
