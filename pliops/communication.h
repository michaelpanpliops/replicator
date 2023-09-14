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

#include "log.h"


namespace Replicator {

enum class ConnectionType : uint8_t {
  TCP_SOCKET = 0
};

template<ConnectionType Protocol>
class Connection {
  Connection() {
    static_assert("Unsupported protocol type");
  }
  // Send a KV pair over the connection
  int Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size);
  // Receive a KV pair from the connection
  int Receive(std::string& key, std::string& value);
};

template<>
class Connection<ConnectionType::TCP_SOCKET> {
  public:
    Connection(int socket_fd);
    virtual ~Connection();
    int Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size);
    int Receive(std::string& key, std::string& value);

  public:
    int socket_fd_;
    bool closed_;
};

static int accept(Connection<ConnectionType::TCP_SOCKET>& listen_c,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& accept_c)
{
  int connfd = 0;
  connfd = accept(listen_c.socket_fd_, (struct sockaddr*)NULL, NULL);
  if (connfd == -1) {
    log_message(FormatString("Socket accepting failed: %d\n", errno));
    return -1;
  }
  log_message(FormatString("Shard connected.\n"));
  accept_c.reset(new Connection<ConnectionType::TCP_SOCKET>(connfd));
  return 0;
}

template<ConnectionType Protocol>
int bind(uint16_t& port,
  std::unique_ptr<Connection<Protocol>>& connection)
{
  static_assert("Usupported connection type");
  return -1;
}

template<>
int bind(uint16_t& port,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection);

template<ConnectionType Protocol>
int connect(const std::string& destination_ip, uint32_t destination_port,
  std::unique_ptr<Connection<Protocol>>& connection)
{
  static_assert("Usupported connection type");
  return -1;
}

template<>
int connect(const std::string& destination_ip, uint32_t destination_port,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection);

}
