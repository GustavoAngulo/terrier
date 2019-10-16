#pragma once
#include "network/network_command.h"

#define DEFINE_ITP_COMMAND(name, flush)                                                                                \
  class name : public ITPNetworkCommand {                                                                              \
   public:                                                                                                             \
    explicit name(InputPacket *in) : ITPNetworkCommand(in, flush) {}                                                   \
    Transition Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,                                           \
                    common::ManagedPointer<ITPPacketWriter> out, common::ManagedPointer<trafficcop::TrafficCop> t_cop, \
                    common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) override;          \
  }

namespace terrier::network {

/**
 * Interface for the execution of the standard ITPNetworkCommands for the ITP protocol
 */
class ITPNetworkCommand : public NetworkCommand {
 public:
  /**
   * Executes the command
   * @param interpreter The protocol interpreter that called this
   * @param out The Writer on which to construct output packets for the client
   * @param t_cop The traffic cop pointer
   * @param connection The ConnectionContext which contains connection information
   * @param callback The callback function to trigger after
   * @return The next transition for the client's state machine
   */
  virtual Transition Exec(common::ManagedPointer<ProtocolInterpreter> interpreter,
                          common::ManagedPointer<ITPPacketWriter> out,
                          common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                          common::ManagedPointer<ConnectionContext> connection, NetworkCallback callback) = 0;

 protected:
  /**
   * Constructor for a ITPNetworkCommand instance
   * @param in The input packets to this command
   * @pram flush Whether or not to flush the output packets on completion
   */
  ITPNetworkCommand(InputPacket *in, bool flush) : NetworkCommand(in, flush), in_len_(in->len_) {}

 private:
  // Size of the input packet
  size_t in_len_;
};

DEFINE_ITP_COMMAND(ReplicationCommand, true);
DEFINE_ITP_COMMAND(StopReplicationCommand, true);

}  // namespace terrier::network