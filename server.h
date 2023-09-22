#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <functional>

#include "rpc.h"
#include "producer.h"


// The main function for shard replication
int ProvideCheckpoint(RpcChannel& rpc,
                      const std::string &src_path,
                      const std::string& client_ip,
                      int parallelism,
                      uint64_t timeout_msec);

class CheckpointProducer
{
public:
  CheckpointProducer(const std::string &src_path, const std::string& client_ip, int parallelism);
  ~CheckpointProducer() {}

  // Client requests processing methods
  int CreateCheckpoint(const CreateCheckpointRequest& req, CreateCheckpointResponse& res);
  int StartStreaming(const StartStreamingRequest& req, StartStreamingResponse& res, uint64_t timeout_msec);
  int GetStatus(const GetStatusRequest& req, GetStatusResponse& res);

  // Synchronization and cleanup
  void ReplicationDone(ProducerState state, const std::string& error);
  int WaitForCompletion(uint32_t timeout_msec);
  int DestroyCheckpoint();

  // The client_done_ is set to true after sending client ERROR or DONE
  bool IsClientDone() { return client_done_; };

private:
  const std::string& src_path_;
  const std::string& client_ip_;
  const int parallelism_;
  uint32_t checkpoint_id_;
  std::string checkpoint_path_;
  bool client_done_ = false;

  // Producer state and its error are updated in the ReplicationDone callback
  ProducerState producer_state_;
  std::string producer_error_;
  std::mutex producer_state_mutex_;
  std::condition_variable producer_state_cv_;

  // Pliops replication producer
  std::unique_ptr<Replicator::Producer> producer_;
};
