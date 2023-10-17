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

#include "defs.h"
#include "pliops/logger.h"
#include "pliops/status.h"
#include "utils/string_util.h"


using namespace Replicator;

using ServerState = Replicator::State;
using RepStatus = Replicator::Status;

#pragma pack(push, 0)

struct CreateCheckpointRequest {
  uint32_t shard_number;
};

struct CreateCheckpointResponse {
  uint32_t checkpoint_id;  
};

struct StartStreamingRequest {
  uint32_t checkpoint_id;  
  uint16_t consumer_port;
};

struct StartStreamingResponse {
  ServerState state;
};

struct GetStatusRequest {
  uint32_t checkpoint_id;
};

struct GetStatusResponse {
  ServerState state;
  uint64_t num_kv_pairs;
  uint64_t num_bytes;
};

#pragma pack(pop)


class RpcChannel {
public:
  enum class Pier { Client, Server };

  RpcChannel(Pier pier, const std::string& pier_ip);
  ~RpcChannel();

  template<typename Tin, typename Tout>
  RepStatus SendCommand(const Tin& in, Tout& out)
  {
    if (::send(socket_, &in, sizeof(in), 0) != sizeof(in)) {
      logger->Log(Severity::ERROR, FormatString("Rpc: Send failed: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Rpc: Send failed: %d\n", errno));
    }
    if (::recv(socket_, &out, sizeof(out), 0) != sizeof(out)) {
      logger->Log(Severity::ERROR, FormatString("Rpc: Recv failed: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Rpc: Recv failed: %d\n", errno));
    }
    return RepStatus();
  }

  template<typename Tin, typename Tout>
  RepStatus ProcessCommand(std::function<RepStatus(const Tin& in, Tout& out)>& callback)
  {
    Tin in;
    Tout out;

    if (::recv(socket_, &in, sizeof(in), 0) != sizeof(in)) {
      logger->Log(Severity::ERROR,FormatString("Rpc: Recv failed: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Rpc: Recv failed: %d\n", errno));
    }
    auto rc = callback(in, out);
    if (!rc.ok()) {
      logger->Log(Severity::ERROR, FormatString("Rpc: Send failed: callback\n"));
      return rc;
    }
    if (::send(socket_, &out, sizeof(out), 0) != sizeof(out)) {
      logger->Log(Severity::ERROR, FormatString("Rpc: Send failed: %d\n", errno));
      return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Rpc: Send failed: callback\n"));
    }
    return RepStatus();
  }

  int socket_;
};

