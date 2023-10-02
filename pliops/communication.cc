#include "communication.h"

#include <functional>


namespace Replicator {


Connection<ConnectionType::TCP_SOCKET>::Connection(int socket_fd)
  : socket_fd_(socket_fd), closed_(false)
{}

Connection<ConnectionType::TCP_SOCKET>::~Connection()
{
  if(!closed_){
    close(socket_fd_);
  }
}

RepStatus Connection<ConnectionType::TCP_SOCKET>::Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size, IKvPairSerializer& kv_pair_serializer)
{
  // Create the message
  std::vector<char> message;
  kv_pair_serializer.Serialize(key, key_size, value, value_size, message);

  // Send the size of the message
  uint32_t message_size = htonl(message.size());

  unsigned int total_bytes_sent = 0;
  while (total_bytes_sent < sizeof(uint32_t)) {
    int bytes_sent = send(socket_fd_,
                          reinterpret_cast<char*>(&message_size) + total_bytes_sent,
                          sizeof(uint32_t) - total_bytes_sent,
                          MSG_NOSIGNAL);
    if (bytes_sent == 0 && errno == 0) {
      // Connection closed by other party (EOF).
      logger->Log(Severity::ERROR, FormatString("Connection closed by other party (EOF).\n"));
      return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, "Connection closed by other party (EOF).\n");
    } else if (bytes_sent < 0) {
      logger->Log(Severity::ERROR, FormatString("Failed to send message size: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Failed to send message size: %d\n", errno));
    }
    total_bytes_sent += bytes_sent;
  }

  // Send the message
  total_bytes_sent = 0;
  while (total_bytes_sent < message.size()) {
  int bytes_sent = send(socket_fd_,
                        message.data() + total_bytes_sent,
                        message.size() - total_bytes_sent,
                        MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
      logger->Log(Severity::ERROR, FormatString("Failed to send message body: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Failed to send message body: %d\n", errno));
    }
    total_bytes_sent += bytes_sent;
  }
  return RepStatus();
}

constexpr unsigned int MAX_MESSAGE_LENGTH_ON_STACK = 100 * 1024;

// Receive a KV pair from the connection
RepStatus Connection<ConnectionType::TCP_SOCKET>::Receive(std::string& key, std::string& value, IKvPairSerializer& kv_pair_serializer)
{
  // Read the size of the incoming message from the socket
  char size_buffer[sizeof(uint32_t)];
  unsigned int total_bytes_read = 0;
  while (total_bytes_read < sizeof(uint32_t)) {
    int bytes_read = recv(socket_fd_,
                          size_buffer + total_bytes_read,
                          sizeof(uint32_t) - total_bytes_read,
                          0);
    if (bytes_read == 0 && errno == 0) {
      // Connection closed by other party (EOF).
      return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Connection closed by other party (EOF).\n"));
    } else if (bytes_read < 0) {
      logger->Log(Severity::ERROR, FormatString("Failed to read message size: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Failed to read message size: %d\n", errno));
    }
    total_bytes_read += bytes_read;
  }
  uint32_t message_size = *reinterpret_cast<uint32_t*>(size_buffer);
  message_size = ntohl(message_size);
  // Allocate a buffer for the incoming message
  char* buffer;
  char buffer_on_stack[message_size];
  std::vector<char> buffer_on_heap;
  if (message_size < MAX_MESSAGE_LENGTH_ON_STACK) {
    buffer = buffer_on_stack;
  } else {
    buffer_on_heap.reserve(message_size);
    buffer = buffer_on_heap.data();
  }
  // Read the message from the socket
  total_bytes_read = 0;
  while (total_bytes_read < message_size) {
    int bytes_read = recv(socket_fd_,
                          buffer + total_bytes_read,
                          message_size - total_bytes_read,
                          0);
    if (bytes_read <= 0) {
      logger->Log(Severity::ERROR, FormatString("Failed to read message body: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Failed to read message body %d\n", errno));
    }
    total_bytes_read += bytes_read;
  }
  // Parse the message
  std::tie(key, value) = kv_pair_serializer.Deserialize(buffer, message_size);
  return RepStatus();
}

template<>
RepStatus Accept(Connection<ConnectionType::TCP_SOCKET>& listen_c,
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
    logger->Log(Severity::ERROR, FormatString("select error: %s\n", strerror(errno)));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("select error: %s\n", strerror(errno)));
  } else if (select_result == 0) {
    logger->Log(Severity::ERROR, FormatString("Accept timeout reached.\n"));
    return RepStatus(Code::TIMEOUT, Severity::ERROR, FormatString("Accept timeout reached.\n"));
  }

  int connfd = 0;
  connfd = accept(listen_c.socket_fd_, (struct sockaddr*)NULL, NULL);
  if (connfd == -1) {
    logger->Log(Severity::ERROR, FormatString("Socket accepting failed: %d\n", errno));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Socket accepting failed: %d\n", errno));
  }

  // SO_RCVTIMEO
  if (setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
    logger->Log(Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set receive timeout) \n"));
    close(connfd);
    return RepStatus(Code::TIMEOUT, Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set receive timeout)\n"));
}

  // SO_SNDTIMEO
  if (setsockopt(connfd, SOL_SOCKET, SO_SNDTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
    logger->Log(Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set send timeout)\n"));
    close(connfd);
    return RepStatus(Code::TIMEOUT, Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set send timeout)\n"));
  }

  logger->Log(Severity::INFO, FormatString("Connection accepted.\n"));
  accept_c.reset(new Connection<ConnectionType::TCP_SOCKET>(connfd));
  return RepStatus();
}

template<>
RepStatus Bind(uint16_t& port, std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& listen_c)
{
  int listenfd = 0, connfd = 0;
  struct sockaddr_in serv_addr;

  listenfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == listenfd) {
    logger->Log(Severity::ERROR, FormatString("Socket creation failed: %d\n", errno));
    return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Socket creation failed: %d\n", errno));
  }

  memset(&serv_addr, '0', sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port = htons(0);

  if ( -1 == bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) ) {
    logger->Log(Severity::ERROR, FormatString("Socket binding failed: %d\n", errno));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Socket binding failed: %d\n", errno));
  }

  if ( -1 == listen(listenfd, 1)) {
    logger->Log(Severity::ERROR, FormatString("Socket listening failed: %d\n", errno));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Socket listenening failed: %d\n", errno));
  }

  socklen_t len = sizeof(serv_addr);
  if ( -1 == getsockname(listenfd, (struct sockaddr*)&serv_addr, &len) ) {
    logger->Log(Severity::ERROR, FormatString("Socket getsockname failed: %d\n", errno));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Socket getsockname failed: %d\n", errno));
  }

  // ip address cannot be retrieved after using INADDR_ANY binding
  // inet_ntop(AF_INET, &(serv_addr.sin_addr), ip, INET_ADDRSTRLEN);
  port = ntohs(serv_addr.sin_port);

  listen_c.reset(new Connection<ConnectionType::TCP_SOCKET>(listenfd));
  return RepStatus();
}

template<>
RepStatus Connect(const std::string& destination_ip, const uint32_t destination_port,
            std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection,
            uint64_t timeout_msec)
{
  int sockfd = 0;
  struct sockaddr_in serv_addr; 

  if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    logger->Log(Severity::ERROR, FormatString("Socket creation failed: %d\n", errno));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Socket creation failed: %d\n", errno));
  }

  // Set the timeout
  struct timeval tv_timeout;
  tv_timeout.tv_sec = timeout_msec / 1000;
  tv_timeout.tv_usec = (timeout_msec % 1000) * 1000;

  // SO_RCVTIMEO
  if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
      logger->Log(Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set receive timeout) \n"));
      close(sockfd);
      return RepStatus(Code::TIMEOUT, Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set receive timeout) \n"));
  }

  // SO_SNDTIMEO
  if (setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
      logger->Log(Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set send timeout)\n"));
      close(sockfd);
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Failed connecting socket: setsockopt (set send timeout)\n"));
  }

  memset(&serv_addr, '0', sizeof(serv_addr)); 

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(destination_port);

  if(inet_pton(AF_INET, destination_ip.c_str(), &serv_addr.sin_addr) <= 0) {
    logger->Log(Severity::ERROR, FormatString("Illegal server address: %d\n", errno));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Illegal server address: %d\n", errno));
  }

  if(connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    if (errno == EINPROGRESS) {
      logger->Log(Severity::ERROR, FormatString("Failed on timeout when connecting to socket: %d\n", errno));
    } else {
      logger->Log(Severity::ERROR, FormatString("Failed connecting socket: %d\n", errno));
    }
    close(sockfd);
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Failed connecting socket: %d\n", errno));
  }

  connection.reset(new Connection<ConnectionType::TCP_SOCKET>(sockfd));
  return RepStatus();
}

}
