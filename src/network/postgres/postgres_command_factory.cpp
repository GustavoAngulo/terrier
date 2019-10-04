#include "network/postgres/postgres_command_factory.h"
#include <memory>
namespace terrier::network {

std::shared_ptr<AbstractNetworkCommand> PostgresCommandFactory::PacketToCommand(InputPacket *packet) {
  switch (packet->msg_type_) {
    case NetworkMessageType::PG_SIMPLE_QUERY_COMMAND:
      return MAKE_COMMAND(SimpleQueryCommand);
    case NetworkMessageType::PG_PARSE_COMMAND:
      return MAKE_COMMAND(ParseCommand);
    case NetworkMessageType::PG_BIND_COMMAND:
      return MAKE_COMMAND(BindCommand);
    case NetworkMessageType::PG_DESCRIBE_COMMAND:
      return MAKE_COMMAND(DescribeCommand);
    case NetworkMessageType::PG_EXECUTE_COMMAND:
      return MAKE_COMMAND(ExecuteCommand);
    case NetworkMessageType::PG_SYNC_COMMAND:
      return MAKE_COMMAND(SyncCommand);
    case NetworkMessageType::PG_CLOSE_COMMAND:
      return MAKE_COMMAND(CloseCommand);
    case NetworkMessageType::PG_TERMINATE_COMMAND:
      return MAKE_COMMAND(TerminateCommand);
    default:
      throw NETWORK_PROCESS_EXCEPTION("Unexpected Packet Type: ");
  }
}

}  // namespace terrier::network
