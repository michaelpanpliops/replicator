#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <functional>

#include "rocksdb/status.h"
#include "rpc.h"
#include "consumer.h"
#include "pliops/status.h"


RepStatus BeginReplication(RpcChannel& rpc,
                              int32_t shard,
                              const std::string &dst_path,
                              int ops_timeout_msec,
                              int connect_timeout_msec,
                              IKvPairSerializer& kv_pair_serializer);
RepStatus CheckReplicationStatus(RpcChannel& rpc, bool& done);
RepStatus EndReplication(RpcChannel& rpc);
void Cleanup();

class CheckpointConsumer
{
public:
  CheckpointConsumer(int ops_timeout_msec, int connect_timeout_msec,
                    IKvPairSerializer& kv_pair_serializer);
  ~CheckpointConsumer() {}

  // Accessors
  Replicator::Consumer& ConsumerImpl() { return *replication_consumer_; }

  // Synchronization and cleanup
  void ReplicationDone(ConsumerState state, const RepStatus& status);
  RepStatus WaitForCompletion(uint32_t timeout_msec);

  const int ops_timeout_msec_;
  const int connect_timeout_msec_;

private:
  // Consumer state and its error are updated in the ReplicationDone callback
  ConsumerState consumer_state_;
  RepStatus consumer_status_;
  std::mutex consumer_state_mutex_;
  std::condition_variable consumer_state_cv_;

  // Pliops replication consumer
  std::unique_ptr<Replicator::Consumer> replication_consumer_;
};
