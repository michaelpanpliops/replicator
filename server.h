#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <functional>

#include "rocksdb/status.h"

#include "rpc.h"
#include "producer.h"

void ProvideCheckpoint(RpcChannel& rpc, const std::string &src_path, const std::string& client_ip);

class CheckpointProducer : public std::enable_shared_from_this<CheckpointProducer>
{
public:
  CheckpointProducer(const std::string &src_path, const std::string& client_ip);
  ~CheckpointProducer(){}

  void CreateCheckpoint(const CreateCheckpointRequest& req, CreateCheckpointResponse& res);
  void StartStreaming(const StartStreamingRequest& req, StartStreamingResponse& res);
  bool GetStatus(const GetStatusRequest& req, GetStatusResponse& res);
  void ReplicationDone();
  void Stop() { producer_->Stop(); }

private:
  const std::string& src_path_;
  const std::string& client_ip_;
  std::atomic<bool> done_;

  // Pliops replication producer
  std::unique_ptr<Replicator::Producer> producer_;  
};
