#include "communication.h"

#include <functional>


namespace {

static std::function<void(void)> DEFAULT_DESTROYER = [](){};

class Finally {
public:
    Finally(std::function<void(void)> destroyerLambda = DEFAULT_DESTROYER)
      : m_destroyer(destroyerLambda), m_released(false)
      {}
    virtual ~Finally() {
        if(!m_released){
            m_destroyer();
        }
    }
    void Release() {
        m_released = true;
    }
    void ReleaseAndExecute() {
        Release();
        m_destroyer();
    }
    void Set(std::function<void(void)> destroyerLambda) {
        m_destroyer = destroyerLambda;
    }
private:
    std::function<void(void)> m_destroyer;
    bool m_released;
};
}

namespace Replicator {

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
  proto::KVPair message;
  message.set_key(key, key_size);
  message.set_value(value, value_size);

  // Serialize the message to a buffer
  std::string buffer = message.SerializeAsString();

  // Send the size of the message
  uint32_t message_size = htonl(buffer.size());

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
  while (total_bytes_sent < buffer.size()) {
      int bytes_sent = write(socket_fd_, buffer.data() + total_bytes_sent, buffer.size() - total_bytes_sent);
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
      throw std::runtime_error(FormatString("Illogical message size: %d\n", message_size));
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
  proto::KVPair message;
  if (!message.ParseFromArray(buffer, message_size)) {
      throw std::runtime_error("Failed to parse message\n");
  }
  return {message.key(), message.value()};
}

std::vector<std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>> wait_for_connections(uint16_t& port) {
  std::vector<std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>> result;
  int listenfd = 0, connfd = 0;
  struct sockaddr_in serv_addr;

  listenfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == listenfd) {
      throw std::runtime_error(FormatString("failed creating socket with error %ld", errno));
  }

//   Finally close_socket([&listenfd]{
//       close(listenfd);
//   });

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

  result.push_back(std::make_unique<Connection<ConnectionType::TCP_SOCKET>>(listenfd));
  return result;
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
