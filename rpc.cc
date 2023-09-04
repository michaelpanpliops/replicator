#include "rpc.h"

using namespace Replicator;

namespace {
uint16_t client_port = 44445;
uint16_t server_port = 44444;
}

RpcChannel::RpcChannel(Pier pier, const std::string& pier_ip)
{
  uint16_t local_port = (pier == Pier::Client ? client_port : server_port); 
  uint16_t remote_port = (pier == Pier::Client ? server_port : client_port); 

  socket_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (-1 == socket_) {
    throw std::runtime_error(FormatString("Rpc: Failed creating socket with error %ld", errno));
  }

  struct sockaddr_in addr;
  memset(&addr, '0', sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(local_port);

  if ( -1 == ::bind(socket_, (struct sockaddr*)&addr, sizeof(addr)) ) {
    throw std::runtime_error(FormatString("Rpc: Failed binding socket with error %ld", errno));
  }

  memset(&addr, '0', sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(pier_ip.c_str());
  addr.sin_port = htons(remote_port);

  if (::connect(socket_, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    throw std::runtime_error(FormatString("Rpc: Failed to connect to the server: %d", errno));
  }
}

RpcChannel::~RpcChannel()
{
  ::close(socket_);
}
