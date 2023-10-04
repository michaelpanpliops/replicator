#include "client.h"

#include <map>
#include <iostream>


namespace {
std::unique_ptr<CheckpointConsumer> consumer_;
uint32_t checkpoint_id_ = 0; // the id received from the server
}

CheckpointConsumer::CheckpointConsumer(
  int ops_timeout_msec, int connect_timeout_msec,
  IKvPairSerializer& kv_pair_serializer)
  : ops_timeout_msec_(ops_timeout_msec)
  , connect_timeout_msec_(connect_timeout_msec)
{
  replication_consumer_ =
    std::make_unique<Replicator::Consumer>(ops_timeout_msec, connect_timeout_msec, kv_pair_serializer);
}

void CheckpointConsumer::ReplicationDone(ConsumerState state, const RepStatus& status)
{
  // Only mark here that the replication is done
  // The cleanup must be done a different thread (we cannot join threads from themself)
  std::lock_guard<std::mutex> lock(consumer_state_mutex_);
  auto severity = Severity::INFO;
  if (state == ConsumerState::ERROR) {
    severity = Severity::ERROR;
  }
  logger->Log(severity, FormatString("ReplicationDone callback: %s, %s\n", ToString(state), status.ToString()));
  consumer_state_ = state;
  consumer_status_ = status;
  consumer_state_cv_.notify_all();
}

RepStatus CheckpointConsumer::WaitForCompletion(uint32_t timeout_msec)
{
  using namespace std::literals;

  // Wait till the consumer is done. 
  logger->Log(Severity::INFO, FormatString("WaitForCompletion: %d msec\n", timeout_msec));
  std::unique_lock lock(consumer_state_mutex_);
  auto rc = consumer_state_cv_.wait_for(lock, 1ms*timeout_msec, [&] { 
    return consumer_state_ == ConsumerState::ERROR || consumer_state_ == ConsumerState::DONE;
  });

  return rc ? RepStatus() : RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, "WaitForCompletion failed.");
}

// Send checkpoint request to the server
RepStatus CreateCheckpoint(RpcChannel &rpc, uint32_t shard, uint32_t& checkpoint_id)
{
  CreateCheckpointRequest req{shard};
  CreateCheckpointResponse res{};
  auto rc = rpc.SendCommand(req, res);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("rpc.SendCommand failed\n"));
    return rc;
  }
  checkpoint_id_ = res.checkpoint_id;
  return RepStatus();
}

// Send start streaming request to the server
RepStatus StartStreaming(RpcChannel &rpc, uint16_t port,
                    uint32_t num_threads, ServerState& state)
{
  StartStreamingRequest req{checkpoint_id_, num_threads, port };
  StartStreamingResponse res{};
  auto rc = rpc.SendCommand(req, res);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("rpc.SendCommand failed\n"));
    return rc;
  }
  state = res.state;
  return RepStatus();
}

// Send status request to the server
RepStatus GetStatus(RpcChannel& rpc, ServerState& state, uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  GetStatusRequest req{checkpoint_id_};
  GetStatusResponse res;
  auto rc = rpc.SendCommand(req, res);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("rpc.SendCommand failed\n"));
    return rc;
  }
  state = res.state;
  num_kv_pairs = res.num_kv_pairs;
  num_bytes = res.num_bytes;
  return RepStatus();
}

// Main entry function 
// Kuaishou function: SyncManager::ReStoreFrom(const std::string &host, int32_t shard)->Status
RepStatus ReplicateCheckpoint(RpcChannel& rpc, int32_t shard, const std::string &dst_path,
                              int desired_num_of_threads,
                              int ops_timeout_msec,
                              int connect_timeout_msec,
                              IKvPairSerializer& kv_pair_serializer)
{
  // RPC call: request checkpoint from the server
  uint32_t checkpoint_id;
  auto rc = CreateCheckpoint(rpc, shard, checkpoint_id);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("ReplicateCheckpoint failed\n"));
    return rc;
  }

  // Create path for the replica
  std::string name = std::to_string(shard) + "_" + "replica";
  std::string replica_path = std::filesystem::path(dst_path)/name;
  logger->Log(Severity::INFO, FormatString("Replica path: %s\n", replica_path));

  // Cleanup replica path
  if (!std::filesystem::exists(replica_path)) {
    std::filesystem::create_directories(replica_path);
  } if (!std::filesystem::is_empty(replica_path)) {
    auto s = ROCKSDB_NAMESPACE::DestroyDB(replica_path, ROCKSDB_NAMESPACE::Options());
    if (!s.ok()) {
      logger->Log(Severity::ERROR, FormatString("DestroyDB failed: %s\n", s.ToString()));
      return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("DestroyDB failed: %s", s.ToString()));
    }
  }

  // Create consumer object
  consumer_ = std::make_unique<CheckpointConsumer>(
                        ops_timeout_msec, connect_timeout_msec, kv_pair_serializer);  

  // Bind ReplicationDone callback
  using namespace std::placeholders;
  std::function<void(ConsumerState, const RepStatus&)> done_cb =
    std::bind(&CheckpointConsumer::ReplicationDone, consumer_.get(), _1, _2); 

  // Start consumer and get the port number from it
  uint16_t port;
  rc = consumer_->ConsumerImpl().Start(replica_path, port, done_cb);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Consumer::Start failed\n"));
    return rc;
  }

  // RPC call: Tell server to start streaming
  ServerState server_status;
  rc = StartStreaming(rpc, port, desired_num_of_threads, server_status);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("ReplicateCheckpoint failed\n"));
    return rc;
  }

  if (server_status != ServerState::IN_PROGRESS) {
    logger->Log(Severity::ERROR, FormatString("Server responded with error to StartStreaming\n"));
    return RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Server responded with error to StartStreaming"));
  }

  return RepStatus();
}

// Check status, should be called periodically, returns true if done
RepStatus CheckReplicationStatus(RpcChannel& rpc, bool& done)
{
  // --- check the consumer state --- //

  // Get the state from the consumer
  ConsumerState consumer_state;
  RepStatus consumer_status;
  auto rc = consumer_->ConsumerImpl().GetState(consumer_state, consumer_status);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Consumer::GetState failed\n"));
    return rc;
  }

  // Get statistics from the consumer
  uint64_t client_num_kv_pairs, client_num_bytes;
  rc = consumer_->ConsumerImpl().GetStats(client_num_kv_pairs, client_num_bytes);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Consumer::GetStats failed\n"));
    return rc;
  }
  // Print statistics
  logger->Log(Severity::INFO, FormatString(
              "Received by the client: num_kv_pairs = %lld, num_bytes = %lld, state = %s\n",
              client_num_kv_pairs, client_num_bytes, ToString(consumer_state)));

#define CHECK_CONSUMER_STATE 
#ifdef CHECK_CONSUMER_STATE
  // Cleanup and return if the consumer it is done
  if (IsFinalState(consumer_state)) {
    rc = consumer_->ConsumerImpl().Stop();
    if (!rc.IsOk()) {
      logger->Log(Severity::ERROR, FormatString("Consumer::Stop failed\n"));
      return rc;
    }
    consumer_.reset();
    done = true;
  }
#endif

  // --- check the server state --- //

  // RPC call: get replication status from server
  ServerState server_state;
  uint64_t server_num_kv_pairs, server_num_bytes;
  rc = GetStatus(rpc, server_state, server_num_kv_pairs, server_num_bytes);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("GetStatus failed\n"));
    return rc;
  }

  // Print statistics
  logger->Log(Severity::INFO, FormatString(
              "Transferred by the server: num_kv_pairs = %lld, num_bytes = %lld, state = %s\n",
              server_num_kv_pairs, server_num_bytes, ToString(server_state)));

#define CHECK_SERVER_STATE 
#ifdef CHECK_SERVER_STATE
  // Cleanup and return if the server it is done
  if (IsFinalState(server_state) && !done) {
    // Wait for consumer to complete
    auto timeout_msec = std::max(consumer_->ops_timeout_msec_, consumer_->connect_timeout_msec_) + 1000;
    rc = consumer_->WaitForCompletion(timeout_msec);
    if (!rc.IsOk()) {
      logger->Log(Severity::ERROR, FormatString("CheckpointProducer::WaitForCompletion failed\n"));
      return rc;
    }

    rc = consumer_->ConsumerImpl().Stop();
    if (!rc.IsOk()) {
      logger->Log(Severity::ERROR, FormatString("Consumer::Stop failed\n"));
      return rc;
    }
    consumer_.reset();
    done = true;
  }
#endif

  return RepStatus();
}

void Cleanup() {
  if (consumer_) {
    auto rc = consumer_->ConsumerImpl().Stop();
    consumer_.reset();
  }
}
