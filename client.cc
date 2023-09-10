#include "client.h"

#include <map>
#include <iostream>

using namespace ROCKSDB_NAMESPACE;


namespace {
std::unique_ptr<CheckpointConsumer> consumer_;
}

CheckpointConsumer::CheckpointConsumer(
  const std::string& path, const std::string &host, int shard,
  const std::string &name, const uint32_t &snapshot,
  std::function<Status()> on_finished)
  : name_(name), host_(host)
  // , sync_path_(host + "-->" + GlobalData::GetInstance()->local_ip),
  , shard_(shard), thread_id_(0)
{
  checkpoint_id_ = snapshot;
  replication_consumer_ = std::make_unique<Replicator::Consumer>();
}

// Send checkpoint request to the server
void CreateCheckpoint(RpcChannel &rpc, uint32_t shard, uint32_t& checkpoint_id, uint32_t& size)
{
  try {
    CreateCheckpointRequest req{shard};
    CreateCheckpointResponse res{};
    rpc.SendCommand(req, res);
    checkpoint_id = res.checkpoint_id;
    size = res.db_size_estimation;
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("CreateCheckpoint:\n\t%s", e.what()));
  }
}

// Send start streaming request to the server
void StartStreaming(RpcChannel &rpc, uint16_t port,
                    uint32_t num_threads, ServerStatus& status)
{
  try {
    StartStreamingRequest req{consumer_->CheckpointID(), num_threads, port };
    StartStreamingResponse res{};
    rpc.SendCommand(req, res);
    status = res.status;
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("StartStreaming:\n\t%s", e.what()));
  }
}

// Send status request to the server
void GetStatus(RpcChannel& rpc, ServerStatus& status, uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  try {
    GetStatusRequest req{consumer_->CheckpointID()};
    GetStatusResponse res;
    rpc.SendCommand(req, res);
    status = res.status;
    num_kv_pairs = res.num_kv_pairs;
    num_bytes = res.num_bytes;
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("GetStatus:\n\t%s", e.what()));
  }
}

// Main entry function 
// Kuaishou function: SyncManager::ReStoreFrom(const std::string &host, int32_t shard)->Status
void RestoreCheckpoint(RpcChannel& rpc, int32_t shard, const std::string &dst_path,
                        int32_t desired_num_of_threads)
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
    auto s = DestroyDB(replica_path, Options());
    if (!s.ok()) {
      throw std::runtime_error(FormatString("DestroyDB failed: %s", s.ToString()));
    }
  }

  // Create consumer object
  consumer_ = std::make_unique<CheckpointConsumer>(
                            "tmp_path", "host", shard, name, checkpoint_id,
                            []()->Status { return Status(); });  

  // Start consumer
  // TODO: add calculation of number of threads
  // uint32_t num_of_threads = 2;//
  uint16_t port;
  consumer_->ReplicationConsumer().Start(replica_path, port);

  // Tell server to start streaming
  ServerStatus server_status;
  StartStreaming(rpc, port, desired_num_of_threads, server_status);

  if (server_status != ServerStatus::IN_PROGRESS) {
    throw std::runtime_error(FormatString("Server responded with error to StartStreaming"));
  }
}

// Check status, should be called periodically, returns true if done
bool CheckReplicationStatus(RpcChannel& rpc)
{
  ServerStatus server_status;
  uint64_t num_kv_pairs, num_bytes;
  GetStatus(rpc, server_status, num_kv_pairs, num_bytes);
  log_message(FormatString("Transferred so far:\n\tnum_kv_pairs = %lld, num_bytes = %lld\n",
                            num_kv_pairs, num_bytes));

  if (server_status == ServerStatus::ERROR) {
    throw std::runtime_error(FormatString("Server responded with error to GetStatus"));
  }

  if (server_status == ServerStatus::DONE) {
    consumer_->ReplicationConsumer().Finish();
    consumer_.reset();
    return true;
  }

  // TODO
  // -> possible responses { in_progress, done, error }
  // also check replication_server_ status
  // then proceed based on the collected statuses
  return false;
}
