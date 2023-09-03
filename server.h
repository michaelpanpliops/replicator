#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <functional>

#include "rocksdb/status.h"

#include "rpc.h"
#include "producer.h"

void ProvideCheckpoint(RpcChannel& rpc, const std::string &src_path);

class CheckpointProducer : public std::enable_shared_from_this<CheckpointProducer>
{
public:
  CheckpointProducer(const std::string &src_path);
  ~CheckpointProducer(){}

  void CreateCheckpoint(const CreateCheckpointRequest& req, CreateCheckpointResponse& res);
  void StartStreaming(const StartStreamingRequest& req, StartStreamingResponse& res);
  void GetStatus(const GetStatusRequest& req, GetStatusResponse& res);

private:
  const std::string& src_path_;

  // Pliops replication producer
  std::unique_ptr<Replicator::Producer> producer_;  
};
