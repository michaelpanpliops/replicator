#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <functional>

#include "rocksdb/status.h"

#include "rpc.h"
#include "consumer.h"


void RestoreCheckpoint(RpcChannel& rpc, int32_t shard, const std::string &dst_path);
bool CheckReplicationStatus(RpcChannel& rpc);

class CheckpointConsumer : public std::enable_shared_from_this<CheckpointConsumer>
{
public:
  CheckpointConsumer( const std::string &path, const std::string &host, int shard,
                      const std::string &name, const std::string &snapshot,
                      std::function<ROCKSDB_NAMESPACE::Status()> on_finished);
  ~CheckpointConsumer() {
  }

  Replicator::Consumer& ReplicationConsumer() { return *replication_consumer_; }

private:
  std::string name_;
  std::string host_;
  std::string sync_path_;
  int shard_;
  int thread_id_;

  std::string path_;
  std::string checkpoint_id_;

  // Pliops replication consumer
  std::unique_ptr<Replicator::Consumer> replication_consumer_;
};
