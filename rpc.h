#pragma once

#include <string>
#include <vector>
#include <exception>
#include <iostream>
#include <functional>

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

using namespace Replicator;

#pragma pack(push, 0)

enum class ServerStatus : uint32_t {
  IN_PROGRESS, DONE, ERROR
};

struct CreateCheckpointRequest {
  uint32_t shard_number;
};

struct CreateCheckpointResponse {
  uint32_t checkpoint_id;  
  uint32_t size_estimation;  
};

struct StartStreamingRequest {
  uint32_t checkpoint_id;  
  uint32_t max_num_of_threads;
  uint16_t consumer_port;
};

struct StartStreamingResponse {
  ServerStatus status;
};

struct GetStatusRequest {
  uint32_t checkpoint_id;
};

struct GetStatusResponse {
  ServerStatus status;
  uint64_t num_ops;
  uint64_t num_bytes;
};

#pragma pack(pop)


class RpcChannel {
public:
  enum class Pier { Client, Server };

  RpcChannel(Pier pier, const std::string& pier_ip);
  ~RpcChannel();

  template<typename Tin, typename Tout>
  void SendCommand(const Tin& in, Tout& out)
  {
    if (::send(socket_, &in, sizeof(in), 0) != sizeof(in)) {
      throw std::runtime_error(FormatString("Rpc: Send failed: %d", errno));
    }
    if (::recv(socket_, &out, sizeof(out), 0) != sizeof(out)) {
      throw std::runtime_error(FormatString("Rpc: Recv failed: %d", errno));
    }
  }

  template<typename Tin, typename Tout>
  void ProcessCommand(std::function<void(const Tin& in, Tout& out)>& callback)
  {
    Tin in;
    Tout out;

    if (::recv(socket_, &in, sizeof(in), 0) != sizeof(in)) {
      throw std::runtime_error(FormatString("Rpc: Recv failed: %d", errno));
    }
    callback(in, out);
    if (::send(socket_, &out, sizeof(out), 0) != sizeof(out)) {
      throw std::runtime_error(FormatString("Rpc: Send failed: %d", errno));
    }
  }

  int socket_;
};

