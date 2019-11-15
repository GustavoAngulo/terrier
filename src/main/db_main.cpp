#include "main/db_main.h"
#include <memory>
#include <unordered_map>
#include <utility>
#include "common/managed_pointer.h"
#include "common/settings.h"
#include "loggers/loggers_util.h"
#include "network/itp/itp_protocol_interpreter.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier {

DBMain::DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
    : param_map_(std::move(param_map)) {
  LoggersUtil::Initialize(false);

  // initialize stat registry
  main_stat_reg_ = std::make_shared<common::StatisticsRegistry>();

  // create the global transaction mgr
  buffer_segment_pool_ = new storage::RecordBufferSegmentPool(
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_size)->second.value_),
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_reuse)->second.value_));
  settings_manager_ = new settings::SettingsManager(this);
  metrics_manager_ = new metrics::MetricsManager;
  thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager_));

  // Create LogManager, if we have specified
  log_manager_ = new storage::LogManager(
      settings_manager_->GetString(settings::Param::log_file_path),
      settings_manager_->GetInt(settings::Param::num_log_manager_buffers),
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_serialization_interval)},
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_persist_interval)},
      settings_manager_->GetInt(settings::Param::log_persist_threshold),
      // TODO(Gus): Replace with settings manager fields
      "", 0, false, buffer_segment_pool_, common::ManagedPointer(thread_registry_));
  log_manager_->Start();

  timestamp_manager_ = new transaction::TimestampManager;
  txn_manager_ =
      new transaction::TransactionManager(timestamp_manager_, DISABLED, buffer_segment_pool_, true, log_manager_);
  garbage_collector_ = new storage::GarbageCollector(timestamp_manager_, DISABLED, txn_manager_, DISABLED);
  gc_thread_ = new storage::GarbageCollectorThread(garbage_collector_,
                                                   std::chrono::milliseconds{type::TransientValuePeeker::PeekInteger(
                                                       param_map_.find(settings::Param::gc_interval)->second.value_)});

  thread_pool_ = new common::WorkerPool(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::num_worker_threads)->second.value_), {});
  thread_pool_->Startup();

  t_cop_ = new trafficcop::TrafficCop;
  connection_handle_factory_ = new network::ConnectionHandleFactory(common::ManagedPointer(t_cop_));

  psql_command_factory_ = new network::PostgresCommandFactory;
  psql_provider_ = new network::PostgresProtocolInterpreter::Provider(common::ManagedPointer(psql_command_factory_));
  server_ = new network::TerrierServer(common::ManagedPointer(connection_handle_factory_),
                                       common::ManagedPointer(thread_registry_));
  // Register psql protocol
  server_->RegisterProtocol(
      static_cast<int16_t>(
          type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::psql_port)->second.value_)),
      common::ManagedPointer(psql_provider_), CONNECTION_THREAD_COUNT, common::Settings::CONNECTION_BACKLOG);

  // Register itp protocol if replication is enabled
  if (type::TransientValuePeeker::PeekBoolean(param_map_.find(settings::Param::replication_enabled)->second.value_)) {
    itp_command_factory_ = new network::ITPCommandFactory;
    itp_provider_ = new network::ITPProtocolInterpreter::Provider(common::ManagedPointer(itp_command_factory_));
    // For now, we only allow a single connection for the ITP Protocol
    server_->RegisterProtocol(static_cast<int16_t>(type::TransientValuePeeker::PeekInteger(
                                  param_map_.find(settings::Param::itp_port)->second.value_)),
                              common::ManagedPointer(itp_provider_), 1, 1);
  }

  LOG_INFO("Initialization complete")
}

void DBMain::Run() {
  running_ = true;
  server_->RunServer();

  {
    std::unique_lock<std::mutex> lock(server_->RunningMutex());
    server_->RunningCV().wait(lock, [=] { return !(server_->Running()); });
  }

  // server loop exited, begin cleaning up
  CleanUp();
}

void DBMain::ForceShutdown() {
  if (running_) {
    server_->StopServer();
  }
  CleanUp();
}

void DBMain::CleanUp() {
  main_stat_reg_->Shutdown(false);
  log_manager_->PersistAndStop();
  thread_pool_->Shutdown();
  LOG_INFO("Terrier has shut down.");
  LoggersUtil::ShutDown();
}

}  // namespace terrier
