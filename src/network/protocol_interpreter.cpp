#include "network/protocol_interpreter.h"

namespace terrier::network {

bool ProtocolInterpreter::TryReadPacketHeader(const std::shared_ptr<ReadBuffer> &in) {
  if (curr_input_packet_.header_parsed_) return true;

  // Header format: 1 byte message type (only if non-startup)
  //              + 4 byte message size (inclusive of these 4 bytes)
  size_t header_size = GetPacketHeaderSize();
  // Make sure the entire header is readable
  if (!in->HasMore(header_size)) return false;

  // The header is ready to be read, fill in fields accordingly
  SetPacketMessageType(in);
  curr_input_packet_.len_ = in->ReadValue<uint32_t>() - sizeof(uint32_t);
  if (curr_input_packet_.len_ > PACKET_LEN_LIMIT) {
    NETWORK_LOG_ERROR("Packet size {} > limit {}", curr_input_packet_.len_, PACKET_LEN_LIMIT);
    throw NETWORK_PROCESS_EXCEPTION("'Arriving packet too large'");
  }

  // Extend the buffer as needed
  if (curr_input_packet_.len_ > in->Capacity()) {
    // Allocate a larger buffer and copy bytes off from the I/O layer's buffer
    curr_input_packet_.buf_ = std::make_shared<ReadBuffer>(curr_input_packet_.len_);
    NETWORK_LOG_TRACE("Extended Buffer size required for packet of size {0}", curr_input_packet_.len_);
    curr_input_packet_.extended_ = true;
  } else {
    curr_input_packet_.buf_ = in;
  }

  curr_input_packet_.header_parsed_ = true;
  return true;
}

bool ProtocolInterpreter::TryBuildPacket(const std::shared_ptr<ReadBuffer> &in) {
  if (!TryReadPacketHeader(in)) return false;

  size_t size_needed = curr_input_packet_.extended_
                           ? curr_input_packet_.len_ - curr_input_packet_.buf_->BytesAvailable()
                           : curr_input_packet_.len_;

  size_t can_read = std::min(size_needed, in->BytesAvailable());
  size_t remaining_bytes = size_needed - can_read;

  // copy bytes only if the packet is longer than the read buffer,
  // otherwise we can use the read buffer to save space
  if (curr_input_packet_.extended_) {
    // TODO(Gus): Figure out why this seems to fix things
    if (curr_input_packet_.buf_->SpaceAvailable() < can_read) return can_read >= 0;
    curr_input_packet_.buf_->FillBufferFrom(*in, can_read);
  }

  return remaining_bytes <= 0;
}

}  // namespace terrier::network
