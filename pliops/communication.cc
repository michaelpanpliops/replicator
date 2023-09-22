#include "communication.h"

#include <functional>


namespace Replicator {

struct KvPairMessage {
  void set_key_value(const char* key, uint32_t key_size, const char* value, uint32_t value_size)
  {
    uint32_t message_size = sizeof(key_size) + key_size + sizeof(value_size) + value_size;
    buffer_.resize(message_size);
    char* ptr = buffer_.data();
    memcpy(ptr, &key_size, sizeof(key_size));
    ptr += sizeof(key_size);
    memcpy(ptr, key, key_size);
    ptr += key_size;
    memcpy(ptr, &value_size, sizeof(value_size));
    ptr += sizeof(value_size);
    memcpy(ptr, value, value_size);
  }

  std::pair<std::string, std::string> get_key_val(const char* buf, uint32_t buf_size)
  {
    std::string key, value;
    uint32_t key_size, value_size;
    if (buf_size < sizeof(key_size)) {
      throw std::runtime_error(FormatString("Bad message: buf_size < sizeof(key_size)\n"));
    }
    memcpy(&key_size, buf, sizeof(key_size));
    buf += sizeof(key_size);
    if (buf_size < sizeof(key_size)+key_size) {
      throw std::runtime_error(FormatString("Bad message: buf_size < sizeof(key_size)+key_size\n"));
    }
    key.assign(buf, key_size);
    buf += key_size;
    if (buf_size < sizeof(key_size)+key_size+sizeof(value_size)) {
      throw std::runtime_error(FormatString("Bad message: buf_size < sizeof(key_size)+key_size+sizeof(value_size)\n"));
    }
    memcpy(&value_size, buf, sizeof(value_size));
    buf += sizeof(value_size);
    if (buf_size != sizeof(key_size)+key_size+sizeof(value_size)+value_size) {
      throw std::runtime_error(FormatString("Bad message: buf_size != sizeof(key_size)+key_size+sizeof(value_size)+value_size\n"));
    }
    value.assign(buf, value_size);
    return {key, value};
  }

  std::vector<char> buffer_;
};

Connection<ConnectionType::TCP_SOCKET>::Connection(int socket_fd)
  : socket_fd_(socket_fd), closed_(false)
{}

Connection<ConnectionType::TCP_SOCKET>::~Connection()
{
  if(!closed_){
    close(socket_fd_);
  }
}

int Connection<ConnectionType::TCP_SOCKET>::Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size)
{
  // Create the message
  KvPairMessage message;
  message.set_key_value(key, key_size, value, value_size);

  // Send the size of the message
  uint32_t message_size = htonl(message.buffer_.size());

  unsigned int total_bytes_sent = 0;
  while (total_bytes_sent < sizeof(uint32_t)) {
    int bytes_sent = send(socket_fd_,
                          reinterpret_cast<char*>(&message_size) + total_bytes_sent,
                          sizeof(uint32_t) - total_bytes_sent,
                          MSG_NOSIGNAL);
    if (bytes_sent == 0 && errno == 0) {
      // Connection closed by other party (EOF).
      return 1;
    } else if (bytes_sent < 0) {
      log_message(FormatString("Failed to send message size: %d\n", errno));
      return -1;
    }
    total_bytes_sent += bytes_sent;
  }

  // Send the message
  total_bytes_sent = 0;
  while (total_bytes_sent < message.buffer_.size()) {
  int bytes_sent = send(socket_fd_,
                        message.buffer_.data() + total_bytes_sent,
                        message.buffer_.size() - total_bytes_sent,
                        MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
      log_message(FormatString("Failed to send message body: %d\n", errno));
      return -1;
    }
    total_bytes_sent += bytes_sent;
  }
  return 0;
}

constexpr unsigned int MAX_MESSAGE_LENGTH_ON_STACK = 100 * 1024;

// Receive a KV pair from the connection
int Connection<ConnectionType::TCP_SOCKET>::Receive(std::string& key, std::string& value)
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
      return 1;
    } else if (bytes_read < 0) {
      log_message(FormatString("Failed to read message size: %d\n", errno));
      return -1;
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
      log_message(FormatString("Failed to read message body: %d\n", errno));
      return -1;
    }
    total_bytes_read += bytes_read;
  }
  // Parse the message
  KvPairMessage message;
  std::tie(key, value) = message.get_key_val(buffer, message_size);
  return 0;
}

template<>
int Accept(Connection<ConnectionType::TCP_SOCKET>& listen_c,
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

template<>
int Bind(uint16_t& port, std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& listen_c)
{
  int listenfd = 0, connfd = 0;
  struct sockaddr_in serv_addr;

  listenfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == listenfd) {
    log_message(FormatString("Socket creation failed: %d\n", errno));
    return -1;
  }

  memset(&serv_addr, '0', sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port = htons(0);

  if ( -1 == bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) ) {
    log_message(FormatString("Socket binding failed: %d\n", errno));
    return -1;
  }

  if ( -1 == listen(listenfd, 1)) {
    log_message(FormatString("Socket listening failed: %d\n", errno));
    return -1;
  }

  socklen_t len = sizeof(serv_addr);
  if ( -1 == getsockname(listenfd, (struct sockaddr*)&serv_addr, &len) ) {
    log_message(FormatString("Socket getsockname failed: %d\n", errno));
    return -1;
  }

  // ip address cannot be retrieved after using INADDR_ANY binding
  // inet_ntop(AF_INET, &(serv_addr.sin_addr), ip, INET_ADDRSTRLEN);
  port = ntohs(serv_addr.sin_port);

  listen_c.reset(new Connection<ConnectionType::TCP_SOCKET>(listenfd));
  return 0;
}

template<>
int Connect(const std::string& destination_ip, const uint32_t destination_port,
            std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection,
            uint64_t timeout_msec)
{
  int sockfd = 0;
  struct sockaddr_in serv_addr; 

  if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    log_message(FormatString("Socket creation failed: %d\n", errno));
    return -1;
  }

  // Set the timeout
  struct timeval tv_timeout;
  tv_timeout.tv_sec = timeout_msec / 1000;
  tv_timeout.tv_usec = (timeout_msec % 1000) * 1000;

  // SO_RCVTIMEO
  if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
      log_message(FormatString("Failed connecting socket: setsockopt (set receive timeout) \n"));
      close(sockfd);
      return -1;
  }

  // SO_SNDTIMEO
  if (setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv_timeout, sizeof(tv_timeout)) < 0) {
      log_message(FormatString("Failed connecting socket: setsockopt (set send timeout)\n"));
      close(sockfd);
      return -1;
  }

  memset(&serv_addr, '0', sizeof(serv_addr)); 

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(destination_port);

  if(inet_pton(AF_INET, destination_ip.c_str(), &serv_addr.sin_addr) <= 0) {
    log_message(FormatString("Illegal server address: %d\n", errno));
    return -1;
  }

  if(connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    if (errno == EINPROGRESS) {
      log_message(FormatString("Failed on timeout when connecting to socket: %d\n", errno));
    } else {
      log_message(FormatString("Failed connecting socket: %d\n", errno));
    }
    close(sockfd);
    return -1;
  }

  connection.reset(new Connection<ConnectionType::TCP_SOCKET>(sockfd));
  return 0;
}

}
