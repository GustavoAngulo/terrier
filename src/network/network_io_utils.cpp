#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/file.h>

#include <memory>
#include <utility>

#include "network/network_io_utils.h"
#include "network/terrier_server.h"

namespace terrier::network {

NetworkIoWrapper::NetworkIoWrapper(const std::string &ip_address, uint16_t port) {
  // Manually open a socket
  int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(ip_address.c_str());
  serv_addr.sin_port = htons(port);

  int64_t ret UNUSED_ATTRIBUTE = connect(socket_fd, reinterpret_cast<sockaddr *>(&serv_addr), sizeof(serv_addr));
  // TODO(Gus): we need better exception handling here
  TERRIER_ASSERT(ret >= 0, "Connection to replica failed");

  sock_fd_ = socket_fd;
  in_ = std::make_shared<ReadBuffer>();
  in_->Reset();
  out_ = std::make_shared<WriteQueue>();
  out_->Reset();
  RestartState();
}

Transition NetworkIoWrapper::FlushAllWrites() {
  for (; out_->FlushHead() != nullptr; out_->MarkHeadFlushed()) {
    auto result = FlushWriteBuffer(&(*out_->FlushHead()));
    if (result != Transition::PROCEED) return result;
  }
  out_->Reset();
  return Transition::PROCEED;
}

Transition NetworkIoWrapper::FillReadBuffer() {
  if (!in_->HasMore()) in_->Reset();
  if (in_->HasMore() && in_->Full()) in_->MoveContentToHead();
  Transition result = Transition::NEED_READ;
  // Normal mode
  while (!in_->Full()) {
    auto bytes_read = in_->FillBufferFrom(sock_fd_);
    if (bytes_read > 0) {
      result = Transition::PROCEED;
    } else {
      if (bytes_read == 0) {
        return Transition::TERMINATE;
      }
      switch (errno) {
        case EAGAIN:
          // Equal to EWOULDBLOCK
          return result;
        case EINTR:
          continue;
        default:
          NETWORK_LOG_ERROR("Error writing: {0}", strerror(errno));
          throw NETWORK_PROCESS_EXCEPTION("Error when filling read buffer");
      }
    }
  }
  return result;
}

Transition NetworkIoWrapper::FlushWriteBuffer(WriteBuffer *wbuf) {
  while (wbuf->HasMore()) {
    auto bytes_written = wbuf->WriteOutTo(sock_fd_);
    if (bytes_written < 0) {
      switch (errno) {
        case EINTR:
          continue;
        case EAGAIN:
          return Transition::NEED_WRITE;
        case EPIPE:
          NETWORK_LOG_TRACE("Client closed during write");
          return Transition::TERMINATE;
        default:
          NETWORK_LOG_ERROR("Error writing: {0}", strerror(errno));
          throw NETWORK_PROCESS_EXCEPTION("Fatal error during write");
      }
    }
  }
  wbuf->Reset();
  return Transition::PROCEED;
}

bool NetworkIoWrapper::ReadUntilMessageOrClose(const NetworkMessageType &expected_msg_type) {
  while (true) {
    in_->Reset();
    Transition trans = FillReadBuffer();
    if (trans == Transition::TERMINATE) return false;

    while (in_->HasMore()) {
      auto type = in_->ReadValue<NetworkMessageType>();
      auto size = in_->ReadValue<int32_t>();
      if (size >= 4) in_->Skip(static_cast<size_t>(size - 4));
      if (type == expected_msg_type) return true;
    }
  }
}

void NetworkIoWrapper::RestartState() {
  // Set Non Blocking
  auto flags = fcntl(sock_fd_, F_GETFL);
  flags |= O_NONBLOCK;
  if (fcntl(sock_fd_, F_SETFL, flags) < 0) {
    NETWORK_LOG_ERROR("Failed to set non-blocking socket");
  }
  // Set TCP No Delay
  int one = 1;
  setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

  in_->Reset();
  out_->Reset();
}

void NetworkIoWrapper::Restart() { RestartState(); }
}  // namespace terrier::network
