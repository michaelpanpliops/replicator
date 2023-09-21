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

int Connection<ConnectionType::TCP_SOCKET>::Send(const char* key, uint32_t key_size, const char* value, uint32_t value_size, IKvPairSerializer& kv_pair_serializer)
{
  // Create the message
  std::vector<char> message;
  kv_pair_serializer.Serialize(key, key_size, value, value_size, message);

  // Send the size of the message
  uint32_t message_size = htonl(message.size());

  unsigned int total_bytes_sent = 0;
  while (total_bytes_sent < sizeof(uint32_t)) {
    int bytes_sent = write(socket_fd_, reinterpret_cast<char*>(&message_size) + total_bytes_sent, sizeof(uint32_t) - total_bytes_sent);
    if (bytes_sent == 0 && errno == 0) {
      // Connection closed by other party (EOF).
      return 1;
    } else if (bytes_sent < 0) {
      logger->Log(LogLevel::ERROR, FormatString("Failed to send message size: %d\n", errno));
      return -1;
    }
    total_bytes_sent += bytes_sent;
  }

  // Send the message
  total_bytes_sent = 0;
  while (total_bytes_sent < message.size()) {
    int bytes_sent = write(socket_fd_, message.data() + total_bytes_sent, message.size() - total_bytes_sent);
    if (bytes_sent <= 0) {
      logger->Log(LogLevel::ERROR, FormatString("Failed to send message body: %d\n", errno));
      return -1;
    }
    total_bytes_sent += bytes_sent;
  }
  return 0;
}

constexpr unsigned int MAX_MESSAGE_LENGTH = 1024 * 1024 * 300;

// Receive a KV pair from the connection
int Connection<ConnectionType::TCP_SOCKET>::Receive(std::string& key, std::string& value, IKvPairSerializer& kv_pair_serializer)
{
  // Read the size of the incoming message from the socket
  char size_buffer[sizeof(uint32_t)];
  unsigned int total_bytes_read = 0;
  while (total_bytes_read < sizeof(uint32_t)) {
    int bytes_read = read(socket_fd_, size_buffer + total_bytes_read, sizeof(uint32_t) - total_bytes_read);
    if (bytes_read == 0 && errno == 0) {
      // Connection closed by other party (EOF).
      return 1;
    } else if (bytes_read < 0) {
      logger->Log(LogLevel::ERROR, FormatString("Failed to read message size: %d\n", errno));
      return -1;
    }
    total_bytes_read += bytes_read;
  }
  uint32_t message_size = *reinterpret_cast<uint32_t*>(size_buffer);
  message_size = ntohl(message_size);
  if (message_size > MAX_MESSAGE_LENGTH) {
    logger->Log(LogLevel::ERROR, FormatString("Message is too big: %d\n", message_size));
    return -1;
  }
  // Allocate a buffer for the incoming message
  char buffer[message_size];
  // Read the message from the socket
  total_bytes_read = 0;
  while (total_bytes_read < message_size) {
    int bytes_read = read(socket_fd_, buffer + total_bytes_read, message_size - total_bytes_read);
    if (bytes_read <= 0) {
      logger->Log(LogLevel::ERROR, FormatString("Failed to read message body: %d\n", errno));
      return -1;
    }
    total_bytes_read += bytes_read;
  }
  // Parse the message
  std::tie(key, value) = kv_pair_serializer.Deserialize(buffer, message_size);
  return 0;
}

template<>
int bind(uint16_t& port, std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& listen_c)
{
  int listenfd = 0, connfd = 0;
  struct sockaddr_in serv_addr;

  listenfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == listenfd) {
    logger->Log(LogLevel::ERROR, FormatString("Socket creation failed: %d\n", errno));
    return -1;
  }

  memset(&serv_addr, '0', sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port = htons(0);

  if ( -1 == bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) ) {
    logger->Log(LogLevel::ERROR, FormatString("Socket binding failed: %d\n", errno));
    return -1;
  }

  if ( -1 == listen(listenfd, 1)) { //number_of_connections) ) {
    logger->Log(LogLevel::ERROR, FormatString("Socket listening failed: %d\n", errno));
    return -1;
  }

  socklen_t len = sizeof(serv_addr);
  if ( -1 == getsockname(listenfd, (struct sockaddr*)&serv_addr, &len) ) {
    logger->Log(LogLevel::ERROR, FormatString("Socket getsockname failed: %d\n", errno));
    return -1;
  }

  // ip address cannot be retrieved after using INADDR_ANY binding
  // inet_ntop(AF_INET, &(serv_addr.sin_addr), ip, INET_ADDRSTRLEN);
  port = ntohs(serv_addr.sin_port);

  listen_c.reset(new Connection<ConnectionType::TCP_SOCKET>(listenfd));
  return 0;
}

template<>
int connect(const std::string& destination_ip, const uint32_t destination_port,
            std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>& connection)
{
  int sockfd = 0;
  struct sockaddr_in serv_addr; 

  if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    logger->Log(LogLevel::ERROR, FormatString("Socket creation failed: %d\n", errno));
    return -1;
  }

  memset(&serv_addr, '0', sizeof(serv_addr)); 

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(destination_port);

  if(inet_pton(AF_INET, destination_ip.c_str(), &serv_addr.sin_addr) <= 0) {
    logger->Log(LogLevel::ERROR, FormatString("Illegal server address: %d\n", errno));
    return -1;
  }

  if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    logger->Log(LogLevel::ERROR, FormatString("Failed connecting socket: %d\n", errno));
    return -1;
  }

  connection.reset(new Connection<ConnectionType::TCP_SOCKET>(sockfd));
  return 0;
}

}
