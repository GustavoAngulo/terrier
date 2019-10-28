#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/settings.h"
#include "gtest/gtest.h"
#include "loggers/main_logger.h"
#include "network/connection_handle_factory.h"
#include "network/terrier_server.h"
#include "traffic_cop/traffic_cop.h"
#include "util/test_harness.h"

namespace terrier::trafficcop {
class TrafficCopTests : public TerrierTest {
 protected:
  std::unique_ptr<network::TerrierServer> server_;
  uint16_t port_ = common::Settings::SERVER_PORT;
  uint32_t max_connections_ = CONNECTION_THREAD_COUNT;
  uint32_t conn_backlog_ = common::Settings::CONNECTION_BACKLOG;

  TrafficCop tcop_;
  network::PostgresCommandFactory command_factory_;
  network::PostgresProtocolInterpreter::Provider protocol_provider_{common::ManagedPointer(&command_factory_)};
  std::unique_ptr<network::ConnectionHandleFactory> handle_factory_;
  common::DedicatedThreadRegistry thread_registry_ = common::DedicatedThreadRegistry(DISABLED);

  void SetUp() override {
    TerrierTest::SetUp();

    network::network_logger->set_level(spdlog::level::trace);
    test_logger->set_level(spdlog::level::debug);
    spdlog::flush_every(std::chrono::seconds(1));

    try {
      handle_factory_ = std::make_unique<network::ConnectionHandleFactory>(common::ManagedPointer(&tcop_));
      server_ = std::make_unique<network::TerrierServer>(
          common::ManagedPointer(handle_factory_.get()),
          common::ManagedPointer<common::DedicatedThreadRegistry>(&thread_registry_));
      server_->RegisterProtocol(port_,
                                common::ManagedPointer<network::ProtocolInterpreter::Provider>(&protocol_provider_),
                                max_connections_, conn_backlog_);
      server_->RunServer();
    } catch (NetworkProcessException &exception) {
      TEST_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }
    TEST_LOG_DEBUG("Server initialized");
  }

  void TearDown() override {
    server_->StopServer();
    TEST_LOG_DEBUG("Terrier has shut down");
    TerrierTest::TearDown();
  }

  std::unique_ptr<network::NetworkIoWrapper> StartPostgresConnection(uint16_t port) {
    auto io_socket = std::make_unique<network::NetworkIoWrapper>("127.0.0.1", port);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    std::unordered_map<std::string, std::string> params{
        {"user", "postgres"}, {"database", "postgres"}, {"application_name", "psql"}};

    writer.WriteStartupRequest(params);
    io_socket->FlushAllWrites();

    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
    return io_socket;
  }

/*
 * Read and write buffer size for the test
 */
#define TEST_BUF_SIZE 1000

  void TerminateConnection(int socket_fd) {
    char out_buffer[TEST_BUF_SIZE] = {};
    // Build a correct query message, "SELECT A FROM B"
    memset(out_buffer, 0, sizeof(out_buffer));
    out_buffer[0] = 'X';
    int len = sizeof(int32_t) + sizeof(char);
    reinterpret_cast<int32_t *>(out_buffer + 1)[0] = htonl(len);
    write(socket_fd, nullptr, len + 1);
  }
};

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, RoundTripTest) {
  try {
    pqxx::connection connection(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port_));

    pqxx::work txn1(connection);
    txn1.exec("DROP TABLE IF EXISTS TableA");
    txn1.exec("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    txn1.exec("INSERT INTO TableA VALUES (1, 'abc');");

    pqxx::result r = txn1.exec("SELECT * FROM TableA");
    for (const pqxx::row &row : r) {
      std::string row_str;
      for (const pqxx::field &col : row) {
        row_str += col.c_str();
        row_str += '\t';
      }
      TEST_LOG_INFO(row_str);
    }
    txn1.commit();

    EXPECT_EQ(r.size(), 1);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, ManualExtendedQueryTest) {
  try {
    auto io_socket = StartPostgresConnection(port_);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);

    writer.WriteSimpleQuery(
        "CREATE TABLE TableA (a_int INT PRIMARY KEY, a_dec DECIMAL, a_text TEXT, a_time TIMESTAMP, a_bigint BIGINT);");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);

    writer.WriteSimpleQuery("INSERT INTO TableA VALUES(100, 3.14, 'nico', 114514, 1234)");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);

    std::string stmt_name = "begin_statement";
    std::string query = "BEGIN";

    writer.WriteParseCommand(stmt_name, query, std::vector<int>());
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_COMMAND_COMPLETE);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      ReadUntilReadyOrClose(io_socket);
    }

    stmt_name = "test_statement";
    query = "SELECT * FROM TableA WHERE a_int = $1 AND a_dec = $2 AND a_text = $3 AND a_time = $4 AND a_bigint = $5";

    writer.WriteParseCommand(stmt_name, query,
                             std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER),
                                               static_cast<int32_t>(network::PostgresValueType::DECIMAL),
                                               static_cast<int32_t>(network::PostgresValueType::VARCHAR),
                                               static_cast<int32_t>(network::PostgresValueType::TIMESTAMPS),
                                               static_cast<int32_t>(network::PostgresValueType::BIGINT)}));
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_PARSE_COMPLETE);

    // Bind, param = "100", "3.14", "nico", "114514" expressed in vector form
    auto param1 = std::vector<char>({'1', '0', '0'});
    auto param2 = std::vector<char>({'3', '.', '1', '4'});
    auto param3 = std::vector<char>({'n', 'i', 'c', 'o'});
    auto param4 = std::vector<char>({'1', '1', '4', '5', '1', '4'});
    auto param5 = std::vector<char>({'1', '2', '3', '4'});

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1, &param2, &param3, &param4, &param5}, {});
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::STATEMENT, stmt_name);
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_PARAMETER_DESCRIPTION);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
    }

    {
      // Test single specifier for all paramters (1)
      // TODO(Weichen): Test binary format here

      std::string portal_name = "test_portal-2";
      // Use text format, don't care about result column formats, specify "0" for using text for all params
      writer.WriteBindCommand(portal_name, stmt_name, {0}, {&param1, &param2, &param3, &param4, &param5}, {});
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
    }

    {
      // Test individual specifier for each parameter

      std::string portal_name = "test_portal-3";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {0, 0, 0, 0, 0}, {&param1, &param2, &param3, &param4, &param5},
                              {});
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, portal_name);
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ROW_DESCRIPTION);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_DATA_ROW);

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
    }

    // CloseCommand
    writer.WriteCloseCommand(network::DescribeCommandObjectType::STATEMENT, stmt_name);
    io_socket->FlushAllWrites();

    stmt_name = "commit_statement";
    query = "COMMIT";

    writer.WriteParseCommand(stmt_name, query, std::vector<int>());
    io_socket->FlushAllWrites();
    ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_PARSE_COMPLETE);

    {
      std::string portal_name = "test_portal";
      // Use text format, don't care about result column formats
      writer.WriteBindCommand(portal_name, stmt_name, {}, {}, {});
      io_socket->FlushAllWrites();
      ReadUntilMessageOrClose(io_socket, network::NetworkMessageType::PG_BIND_COMPLETE);

      writer.WriteExecuteCommand(portal_name, 0);
      io_socket->FlushAllWrites();

      writer.WriteSyncCommand();
      io_socket->FlushAllWrites();
      ReadUntilReadyOrClose(io_socket);
    }
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// -------------------------------------------------------------------------

/*
 * The manual tests below are for debugging. They can be disabled when testing other components.
 * You can launch a Postgres backend and compare packets from terrier and from Postgres
 * to find if you have created correct packets.
 *
 * */

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, ManualRoundTripTest) {
  try {
    auto io_socket = StartPostgresConnection(port_);
    network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

    writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
    writer.WriteSimpleQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
    writer.WriteSimpleQuery("INSERT INTO TableA VALUES (1, 'abc');");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);

    writer.WriteSimpleQuery("SELECT * FROM TableA");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

// NOLINTNEXTLINE
TEST_F(TrafficCopTests, ErrorHandlingTest) {
  auto io_socket = StartPostgresConnection(port_);
  network::PostgresPacketWriter writer(io_socket->GetWriteQueue());

  writer.WriteSimpleQuery("DROP TABLE IF EXISTS TableA");
  io_socket->FlushAllWrites();
  io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
  writer.WriteSimpleQuery("CREATE TABLE TableA (id INT PRIMARY KEY, data TEXT);");
  io_socket->FlushAllWrites();
  io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);
  writer.WriteSimpleQuery("INSERT INTO TableA VALUES (1, 'abc');");
  io_socket->FlushAllWrites();
  io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_READY_FOR_QUERY);

  std::string stmt_name = "test_statement";
  std::string query = "SELECT * FROM TableA WHERE id = $1";
  writer.WriteParseCommand(stmt_name, query,
                           std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER)}));
  io_socket->FlushAllWrites();
  io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_PARSE_COMPLETE);

  {
    // Repeated statement name
    writer.WriteParseCommand(stmt_name, query,
                             std::vector<int>({static_cast<int32_t>(network::PostgresValueType::INTEGER)}));
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  std::string portal_name = "test_portal";
  auto param1 = std::vector<char>({'1', '0', '0'});

  {
    // Binding a statement that doesn't exist
    writer.WriteBindCommand(portal_name, "FakeStatementName", {}, {&param1}, {});
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Wrong number of format codes
    writer.WriteBindCommand(portal_name, stmt_name, {0, 0, 0, 0, 0}, {&param1}, {});
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Wrong number of parameters
    auto param2 = std::vector<char>({'f', 'a', 'k', 'e'});
    writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1, &param2}, {});
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  writer.WriteBindCommand(portal_name, stmt_name, {}, {&param1}, {});
  io_socket->FlushAllWrites();
  io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_BIND_COMPLETE);

  {
    // Describe a statement and a portal that doesn't exist
    writer.WriteDescribeCommand(network::DescribeCommandObjectType::STATEMENT, "FakeStatementName");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);

    writer.WriteDescribeCommand(network::DescribeCommandObjectType::PORTAL, "FakePortalName");
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // Execute a portal that doesn't exist
    writer.WriteExecuteCommand("FakePortal", 0);
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);
  }

  {
    // A bad describe request
    writer.BeginPacket(network::NetworkMessageType::PG_DESCRIBE_COMMAND)
        .AppendRawValue('?')
        .AppendString(stmt_name)
        .EndPacket();
    io_socket->FlushAllWrites();
    io_socket->ReadUntilMessageOrClose(network::NetworkMessageType::PG_ERROR_RESPONSE);
  }
}

/**
 * I disabled this test because pqxx sends PARSE query with num_params=0, but we are requiring the client to specify
 * all param types in the PARSE query.
 * Please use manual tests before this is supported.
 */
// NOLINTNEXTLINE
TEST_F(TrafficCopTests, DISABLED_ExtendedQueryTest) {
  try {
    pqxx::connection connection(
        fmt::format("host=127.0.0.1 port={0} user=postgres sslmode=disable application_name=psql", port_));

    pqxx::work txn(connection);
    pqxx::result res;
    connection.prepare("DROP TABLE IF EXISTS TableA");
    res = txn.exec_prepared("");
    connection.prepare("CREATE TABLE TableA (a_int INT PRIMARY KEY)");
    res = txn.exec_prepared("");

    connection.prepare("INSERT INTO TableA VALUES(114)");
    res = txn.exec_prepared("");

    connection.prepare("SELECT * from TableA where a_int = $1");
    res = txn.exec_prepared("", 114);
    EXPECT_EQ(1, res.size());

    txn.commit();

    /*
    connection.prepare("SELECT * from TableA where a_dec = $1");
    res = txn.exec_prepared("", 1919.81);
    EXPECT_EQ(1, res.size());

    connection.prepare("SELECT * from TableA where a_text = $1");
    res = txn.exec_prepared("", "blacktea");
    EXPECT_EQ(1, res.size());
    */
  } catch (const std::exception &e) {
    TEST_LOG_ERROR("Exception occurred: {0}", e.what());
    EXPECT_TRUE(false);
  }
}

}  // namespace terrier::trafficcop
