#include "client.h"

#include <map>
#include <iostream>

using namespace ROCKSDB_NAMESPACE;


namespace {
std::unique_ptr<CheckpointConsumer> consumer_;
}

CheckpointConsumer::CheckpointConsumer(
  const std::string& path, const std::string &host, int shard,
  const std::string &name, const std::string &snapshot,
  std::function<Status()> on_finished)
  : name_(name), host_(host)
  // , sync_path_(host + "-->" + GlobalData::GetInstance()->local_ip),
  , shard_(shard), thread_id_(0)
{
  replication_consumer_ = std::make_unique<Replicator::Consumer>();
}

// Send checkpoint request to the server
void CreateCheckpoint(RpcChannel &rpc, uint32_t shard, uint32_t checkpoint_id, uint32_t size)
{
  try {
    CreateCheckpointRequest req{shard};
    CreateCheckpointResponse res{};
    rpc.SendCommand(req, res);
    checkpoint_id = res.checkpoint_id;
    size = res.size_estimation;
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("CreateCheckpoint:\n\t%s", e.what()));
  }
}

// Send start streaming request to the server
void StartStreaming(RpcChannel &rpc, uint32_t checkpoint_id, uint16_t port,
                    uint32_t num_threads, ServerStatus& status)
{
  try {
    StartStreamingRequest req{checkpoint_id, num_threads, port };
    StartStreamingResponse res{};
    rpc.SendCommand(req, res);
    status = res.status;
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("StartStreaming:\n\t%s", e.what()));
  }
}

// Send status request to the server
void GetStatus(RpcChannel &rpc)
{
  try {
    GetStatusRequest req;
    GetStatusResponse res;
    rpc.SendCommand(req, res);
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("GetStatus:\n\t%s", e.what()));
  }
}

// Main entry function 
// Kuaishou function: SyncManager::ReStoreFrom(const std::string &host, int32_t shard)->Status
void RestoreCheckpoint(RpcChannel& rpc, int32_t shard, const std::string &dst_path)
{
  // Request checkpoint from the server
  uint32_t checkpoint_id;
  uint32_t size;
  CreateCheckpoint(rpc, shard, checkpoint_id, size);

  // Create path for the replica
  std::string name = std::to_string(shard) + "_" + "replica";
  auto replica_path = std::filesystem::path(dst_path)/name;
  if (!std::filesystem::exists(replica_path)) {
    std::filesystem::create_directories(replica_path);
  } if (!std::filesystem::is_empty(replica_path)) {
    auto s = DestroyDB(replica_path);
    if (!s.ok()) {
      throw std::runtime_error(FormatString("DestroyDB failed: %s", s.ToString()));
    }
  }

  // Create consumer object
  consumer_ = std::make_unique<CheckpointConsumer>(
                            "tmp_path", "host", shard, name, std::to_string(checkpoint_id),
                            []()->Status { return Status(); });  

  // Start consumer
  // TODO: add calculation of number of threads
  uint32_t num_of_threads = 1;
  uint16_t port;
  consumer_->ReplicationConsumer().Start(replica_path, num_of_threads, port);

  // Tell server to start streaming
  ServerStatus server_status;
  StartStreaming(rpc, checkpoint_id, port, num_of_threads, server_status);
  // Check status, stop if error
}

// Check status, should be called periodically, returns true if done
bool CheckReplicationStatus(RpcChannel& rpc)
{
  GetStatus(rpc);
  // send request to the source to get the replication status
  // -> possible responses { in_progress, done, error }
  // also check replication_server_ status
  // then proceed based on the collected statuses
  return true;
}
