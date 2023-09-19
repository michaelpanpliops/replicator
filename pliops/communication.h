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
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& accept_c,
  uint64_t timeout_msec)
{
    // Use select to monitor the server socket for incoming connections with a timeout
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(listen_c.socket_fd_, &read_fds);

  struct timeval tv_timeout;
  tv_timeout.tv_sec = timeout_msec / 1000;
  tv_timeout.tv_usec = (timeout_msec % 1000) * 1000;

  int select_result = select(listen_c.socket_fd_ + 1, &read_fds, NULL, NULL, &tv_timeout);
  if (select_result == -1) {
      log_message(FormatString("select error: %s\n", strerror(errno)));
      return -1;
  } else if (select_result == 0) {
      log_message(FormatString("Accept timeout reached.\n"));
      return -1;
  }

  int connfd = 0;
  connfd = accept(listen_c.socket_fd_, (struct sockaddr*)NULL, NULL);
  if (connfd == -1) {
    log_message(FormatString("Socket accepting failed: %d\n", errno));
    return -1;
  }

  // SO_RCVTIMEO
  if (setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
      log_message(FormatString("Failed connecting socket: setsockopt (set receive timeout) \n"));
      close(connfd);
      return -1;
  }

  // SO_SNDTIMEO
  if (setsockopt(connfd, SOL_SOCKET, SO_SNDTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
      log_message(FormatString("Failed connecting socket: setsockopt (set send timeout)\n"));
      close(connfd);
      return -1;
  }

  log_message(FormatString("Connection accepted.\n"));
  accept_c.reset(new Connection<ConnectionType::TCP_SOCKET>(connfd));
  return 0;
}

template<ConnectionType Protocol>
int bind(uint16_t& port,
  std::unique_ptr<Connection<Protocol>>& connection)
{
  static_assert("Unsupported connection type");
  return -1;
}

template<>
int bind(uint16_t& port,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection);

template<ConnectionType Protocol>
int connect(const std::string& destination_ip, uint32_t destination_port,
  std::unique_ptr<Connection<Protocol>>& connection, uint64_t timeout_msec)
{
  static_assert("Unsupported connection type");
  return -1;
}

template<>
int connect(const std::string& destination_ip, uint32_t destination_port,
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection, uint64_t timeout_msec);

}
