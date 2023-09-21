#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <functional>

#include "rocksdb/status.h"
#include "rpc.h"
#include "consumer.h"


int ReplicateCheckpoint(RpcChannel& rpc,
                        int32_t shard,
                        const std::string &dst_path,
                        int32_t desired_num_of_threads,
                        IKvPairSerializer& kv_pair_serializer);
int CheckReplicationStatus(RpcChannel& rpc, bool& done);

class CheckpointConsumer
{
public:
  CheckpointConsumer(IKvPairSerializer& kv_pair_serializer);
  ~CheckpointConsumer() {}

  // Accessors
  Replicator::Consumer& ConsumerImpl() { return *replication_consumer_; }

  // Synchronization and cleanup
  void ReplicationDone(ConsumerState state, const std::string& error);
  int WaitForCompletion(uint32_t timeout_msec);

private:
  // Consumer state and its error are updated in the ReplicationDone callback
  ConsumerState consumer_state_;
  std::string consumer_error_;
  std::mutex consumer_state_mutex_;
  std::condition_variable consumer_state_cv_;

  // Pliops replication consumer
  std::unique_ptr<Replicator::Consumer> replication_consumer_;
};
