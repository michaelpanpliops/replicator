#pragma once

#include <string>
#include <vector>
#include <exception>
#include <iostream>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>

#include "pliops/logger.h"
#include "utils/string_util.h"
#include "pliops/kv_pair_serializer.h"
#include "pliops/status.h"


namespace Replicator {

using RepStatus = Replicator::Status;

enum class ConnectionType : uint8_t {
  TCP_SOCKET = 0
};

template<ConnectionType Protocol>
class Connection {
  Connection() {
    static_assert("Unsupported protocol type");
  }
  // Send a KV pair over the connection
  RepStatus Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size, IKvPairSerializer& kv_pair_serializer);
  // Receive a KV pair from the connection
  RepStatus Receive(std::string& key, std::string& value, IKvPairSerializer& kv_pair_serializer);
};

template<>
class Connection<ConnectionType::TCP_SOCKET> {
  public:
    Connection(int socket_fd);
    virtual ~Connection();
    RepStatus Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size, IKvPairSerializer& kv_pair_serializer);
    RepStatus Receive(std::string& key, std::string& value, IKvPairSerializer& kv_pair_serializer);

  public:
    int socket_fd_;
    bool closed_;
};

template<ConnectionType Protocol>
RepStatus Accept(Connection<Protocol>& listen_c,
  std::unique_ptr<Connection<Protocol>>& accept_c,
  uint64_t timeout_msec)
{
  static_assert("Unsupported accept type");
  return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR);
}

template<>
RepStatus Accept(Connection<ConnectionType::TCP_SOCKET>& listen_c,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& accept_c,
  uint64_t timeout_msec);

template<ConnectionType Protocol>
RepStatus Bind(uint16_t& port,
  std::unique_ptr<Connection<Protocol>>& connection)
{
  static_assert("Unsupported connection type");
  return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR);
}

template<>
RepStatus Bind(uint16_t& port,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection);

template<ConnectionType Protocol>
RepStatus Connect(const std::string& destination_ip, uint32_t destination_port,
  std::unique_ptr<Connection<Protocol>>& connection, uint64_t timeout_msec)
{
  static_assert("Unsupported connection type");
  return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR);
}

template<>
RepStatus Connect(const std::string& destination_ip, uint32_t destination_port,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection, uint64_t timeout_msec);

}
