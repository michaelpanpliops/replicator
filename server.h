#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <functional>

#include "rpc.h"
#include "producer.h"


// The main function for shard replication
RepStatus ProvideCheckpoint(RpcChannel& rpc,
                            const std::string &src_path,
                            const std::string& client_ip,
                            int max_num_ranges,
                            int parallelism,
                            int ops_timeout_msec,
                            int connect_timeout_msec,
                            IKvPairSerializer& kv_pair_serializer);

class CheckpointProducer
{
public:
  CheckpointProducer(const std::string& src_path, const std::string& client_ip, int max_num_ranges,
                      int parallelism, int ops_timeout_msec, int connect_timeout_msec,
                      IKvPairSerializer& kv_pair_serializer);
  ~CheckpointProducer();

  // Client requests processing methods
  RepStatus CreateCheckpoint(const CreateCheckpointRequest& req, CreateCheckpointResponse& res);
  RepStatus StartStreaming(const StartStreamingRequest& req, StartStreamingResponse& res);
  RepStatus GetStatus(const GetStatusRequest& req, GetStatusResponse& res);

  // Synchronization and cleanup
  void ReplicationDone(ProducerState state, const RepStatus&);
  RepStatus WaitForCompletion(uint32_t timeout_msec);
  RepStatus DestroyCheckpoint();

  // The client_done_ is set to true after sending to the client ERROR, DONE, STOPPED
  bool IsClientDone() { return client_done_; };

  const int ops_timeout_msec_;
  const int connect_timeout_msec_;

private:
  const std::string& src_path_;
  const std::string& client_ip_;
  const int max_num_ranges_;
  const int parallelism_;
  uint32_t checkpoint_id_;
  std::string checkpoint_path_;
  bool client_done_ = false;

  // Producer state is updated in the ReplicationDone callback
  ProducerState producer_state_;
  RepStatus producer_status_;
  std::mutex producer_state_mutex_;
  std::condition_variable producer_state_cv_;

  // Pliops replication producer
  std::unique_ptr<Replicator::Producer> producer_;
};
