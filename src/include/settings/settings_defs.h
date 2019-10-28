// SETTING_<type>(name, description, default_value, min_value, max_value, is_mutable, callback_fn)

// Terrier port
SETTING_int(
    psql_port,
    "Port number for psql protocol (default: 15721)",
    15721,
    1024,
    65535,
    false,
    terrier::settings::Callbacks::NoOp
)

// RecordBufferSegmentPool size limit
SETTING_int(
    record_buffer_segment_size,
    "The maximum number of record buffer segments in the system. (default: 100000)",
    100000,
    1,
    1000000,
    true,
    terrier::settings::Callbacks::BufferSegmentPoolSizeLimit
)

// RecordBufferSegmentPool reuse limit
SETTING_int(
    record_buffer_segment_reuse,
    "The minimum number of record buffer segments to keep allocated in the system (default: 1000)",
    1000,
    1,
    1000000,
    true,
    terrier::settings::Callbacks::BufferSegmentPoolReuseLimit
)

// Garbage collector thread interval
SETTING_int(
    gc_interval,
    "Garbage collector thread interval (default: 10)",
    10,
    1,
    10000,
    false,
    terrier::settings::Callbacks::NoOp
)

// Number of worker pool threads
SETTING_int(
    num_worker_threads,
    "The number of worker pool threads (default: 4)",
    4,
    1,
    1000,
    true,
    terrier::settings::Callbacks::WorkerPoolThreads
)

// Path to log file for WAL
SETTING_string(
    log_file_path,
    "The path to the log file for the WAL (default: wal.log)",
    "wal.log",
    false,
    terrier::settings::Callbacks::NoOp
)

// Number of buffers log manager can use to buffer logs
SETTING_int(
    num_log_manager_buffers,
    "The number of buffers the log manager uses to buffer logs to hand off to log consumer(s) (default: 4)",
    100,
    2,
    10000,
    true,
    terrier::settings::Callbacks::NumLogManagerBuffers
)

// Log Serialization interval
SETTING_int(
    log_serialization_interval,
    "Log serialization task interval (ms) (default: 10)",
    10,
    1,
    10000,
    false,
    terrier::settings::Callbacks::NoOp
)

// Log file persisting interval
SETTING_int(
    log_persist_interval,
    "Log file persisting interval (ms) (default: 10)",
    10,
    1,
    10000,
    false,
    terrier::settings::Callbacks::NoOp
)

// Log file persisting threshold
SETTING_int(
    log_persist_threshold,
    "Log file persisting write threshold (bytes) (default: 1MB)",
    (1 << 20) /* 1MB */,
    (1 << 12) /* 4KB */,
    (1 << 24) /* 16MB */,
    false,
    terrier::settings::Callbacks::NoOp
)

SETTING_bool(
    metrics_logging,
    "Metrics collection for the Logging component.",
    false,
    true,
    terrier::settings::Callbacks::MetricsLogging
)

SETTING_bool(
    metrics_transaction,
    "Metrics collection for the TransactionManager component.",
    false,
    true,
    terrier::settings::Callbacks::MetricsTransaction
)

SETTING_bool(replication_enabled, "Run system with replication", false, false, terrier::settings::Callbacks::NoOp)

SETTING_string(replication_ip_address, "Address to send replication logs to", "", false, terrier::settings::Callbacks::NoOp)

SETTING_int(itp_port, "Port number for internal terrier protocol (ITP)", 9022, 1024, 65535, false, terrier::settings::Callbacks::NoOp)

SETTING_int(replication_master_timeout, "Timeout replication if we don't receive a message from master in this much time (seconds)", 600, 5, 3600, false, terrier::settings::Callbacks::NoOp)