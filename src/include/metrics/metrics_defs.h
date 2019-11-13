#pragma once

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { LOGGING, TRANSACTION };

constexpr uint8_t NUM_COMPONENTS = 2;

#define NUM_METRICS 288
#define NUM_EXPORTERS 5
#define METRICS_FACTOR (NUM_METRICS / NUM_EXPORTERS)

}  // namespace terrier::metrics
