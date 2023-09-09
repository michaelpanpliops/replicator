#include "communication.h"

#include <functional>


namespace Replicator {

struct KvPairMessage {
  void set_key_value(const char* key, uint32_t key_size, const char* value, uint32_t value_size) {
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

  std::pair<std::string, std::string> get_key_val(const char* buf, uint32_t buf_size) {
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
    key.assign(buf, value_size);
    return {key, value};
  }

  std::vector<char> buffer_;
};

Connection<ConnectionType::TCP_SOCKET>::Connection(int socket_fd)
  : socket_fd_(socket_fd), closed_(false)
{}

Connection<ConnectionType::TCP_SOCKET>::~Connection() {
  if(!closed_){
    close(socket_fd_);
  }
}

void Connection<ConnectionType::TCP_SOCKET>::Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size) {
  // Create the message
  KvPairMessage message;
  message.set_key_value(key, key_size, value, value_size);

  // Send the size of the message
  uint32_t message_size = htonl(message.buffer_.size());

  unsigned int total_bytes_sent = 0;
  while (total_bytes_sent < sizeof(uint32_t)) {
    int bytes_sent = write(socket_fd_, reinterpret_cast<char*>(&message_size) + total_bytes_sent, sizeof(uint32_t) - total_bytes_sent);
    if (bytes_sent == 0 && errno == 0) {
      // Connection closed by other party (EOF).
      throw ConnectionClosed();
    } else if (bytes_sent < 0) {
      throw std::runtime_error(FormatString("Failed to send message size: %d\n", errno));
    }
    total_bytes_sent += bytes_sent;
  }

  // Send the message
  total_bytes_sent = 0;
  while (total_bytes_sent < message.buffer_.size()) {
    int bytes_sent = write(socket_fd_, message.buffer_.data() + total_bytes_sent, message.buffer_.size() - total_bytes_sent);
    if (bytes_sent <= 0) {
      throw std::runtime_error(FormatString("Failed to send message body: %d\n", errno));
    }
    total_bytes_sent += bytes_sent;
  }
}

constexpr unsigned int MAX_MESSAGE_LENGTH = 1024 * 1024 * 300;

// Receive a KV pair from the connection
std::pair<std::string, std::string> Connection<ConnectionType::TCP_SOCKET>::Receive() {
  // Read the size of the incoming message from the socket
  char size_buffer[sizeof(uint32_t)];
  unsigned int total_bytes_read = 0;
  while (total_bytes_read < sizeof(uint32_t)) {
    int bytes_read = read(socket_fd_, size_buffer + total_bytes_read, sizeof(uint32_t) - total_bytes_read);
    if (bytes_read == 0 && errno == 0) {
      // Connection closed by other party (EOF).
      throw ConnectionClosed();
    } else if (bytes_read < 0) {
      throw std::runtime_error(FormatString("Failed to read message size: %d\n", errno));
    }
    total_bytes_read += bytes_read;
  }
  uint32_t message_size = *reinterpret_cast<uint32_t*>(size_buffer);
  message_size = ntohl(message_size);
  if (message_size > MAX_MESSAGE_LENGTH) {
    throw std::runtime_error(FormatString("Message is too big: %d\n", message_size));
  }
  // Allocate a buffer for the incoming message
  char buffer[message_size];
  // Read the message from the socket
  total_bytes_read = 0;
  while (total_bytes_read < message_size) {
    int bytes_read = read(socket_fd_, buffer + total_bytes_read, message_size - total_bytes_read);
    if (bytes_read <= 0) {
      throw std::runtime_error(FormatString("Failed to read message body: %d\n", errno));
    }
    total_bytes_read += bytes_read;
  }
  // Parse the message
  KvPairMessage message;
  return message.get_key_val(buffer, message_size);
}

std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> bind(uint16_t& port) {
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> result;
  int listenfd = 0, connfd = 0;
  struct sockaddr_in serv_addr;

  listenfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == listenfd) {
    throw std::runtime_error(FormatString("failed creating socket with error %ld", errno));
  }

  memset(&serv_addr, '0', sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port = htons(0);

//   int reuse = 1;
//   if ( -1 == setsockopt(listenfd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) ) {
//     throw std::runtime_error(FormatString("failed setsockopt error %ld", errno));
//   }

  if ( -1 == bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) ) {
    throw std::runtime_error(FormatString("failed binding socket with error %ld", errno));
  }

  if ( -1 == listen(listenfd, 1)) { //number_of_connections) ) {
    throw std::runtime_error(FormatString("failed listening on socket with error %ld", errno));
  }

  socklen_t len = sizeof(serv_addr);
  if ( -1 == getsockname(listenfd, (struct sockaddr*)&serv_addr, &len) ) {
    throw std::runtime_error(FormatString("failed getsockname socket with error %ld", errno));
  }
  // TODO: ip address cannot be retrieved after using INADDR_ANY binding
  // inet_ntop(AF_INET, &(serv_addr.sin_addr), ip, INET_ADDRSTRLEN);
  port = ntohs(serv_addr.sin_port);

  return std::make_unique<Connection<ConnectionType::TCP_SOCKET>>(listenfd);
}

template<>
std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connect(const std::string& destination_ip, const uint32_t destination_port) {
  int sockfd = 0;
  struct sockaddr_in serv_addr; 

  if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    throw std::runtime_error(FormatString("Could not create client socket, error: %d", errno));
  }

  memset(&serv_addr, '0', sizeof(serv_addr)); 

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(destination_port);

  if(inet_pton(AF_INET, destination_ip.c_str(), &serv_addr.sin_addr)<=0)
  {
    throw std::runtime_error(FormatString("Illegal server address, error: %d", errno));
  }

  if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
  {
    throw std::runtime_error(FormatString("Failed connecting to server: %d", errno));
  }

  return std::make_unique<Connection<ConnectionType::TCP_SOCKET>>(sockfd);
}

}
