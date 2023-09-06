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
#include "replicator.pb.h"

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
    void Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size);
    // Receive a KV pair from the connection
    std::pair<std::string, std::string> Receive();
    // Close the connetion
    void Close();
  };

  template<>
  class Connection<ConnectionType::TCP_SOCKET> {
    public:
      Connection(int socket_fd);
      virtual ~Connection();
      void Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size);
      std::pair<std::string, std::string> Receive();
      void Close();

    // private:
    public:
      int socket_fd_;
      bool closed_;
  };

  static std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>
              accept(std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& listen_s) {
    int connfd = 0;
    connfd = accept(listen_s->socket_fd_, (struct sockaddr*)NULL, NULL);
    log_message(FormatString("Shard connected.\n"));
    // TODO: error handling?
    // listen_s->Close(); //???

    return std::make_unique<Connection<ConnectionType::TCP_SOCKET>>(connfd);
  }

  template<ConnectionType Protocol>
  std::vector<std::unique_ptr<Connection<Protocol>>> wait_for_connections(uint16_t& port) {
    static_assert("Usupported connection type");
  }
  std::vector<std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>> wait_for_connections(uint16_t& port);


  template<ConnectionType Protocol>
  std::unique_ptr<Connection<Protocol>> connect(const std::string& destination_ip, uint32_t destination_port) {
    static_assert("Usupported connection type");
  }

  template<>
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connect(const std::string& destination_ip, uint32_t destination_port);

  class ConnectionClosed : public std::exception {
    public:
      virtual inline const char* what() const noexcept{ return "Connection closed by other party (EOF)."; }
  };
}
