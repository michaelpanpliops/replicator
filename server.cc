#include "server.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/checkpoint.h"


using namespace ROCKSDB_NAMESPACE;

namespace {
// For the sake of the example we use hardcoded checkpoint name
uint32_t GetUniqueCheckpointName() { return 12345; }
}

CheckpointProducer::CheckpointProducer(
  const std::string &src_path, const std::string& client_ip, int parallelism, IKvPairSerializer& kv_pair_serializer)
  : src_path_(src_path), client_ip_(client_ip), parallelism_(parallelism)
{
  producer_ = std::make_unique<Replicator::Producer>(kv_pair_serializer);
}

// Process create-checkpoint request
// Kuaishou function: SyncServiceImpl::RequireCheckpoint(...)
int CheckpointProducer::CreateCheckpoint(
                          const CreateCheckpointRequest& req,
                          CreateCheckpointResponse& res)
{
  logger->Log(LogLevel::INFO, FormatString("CreateCheckpoint: shard=%d\n", req.shard_number));

  // Path to the db
  std::string shard_path = std::filesystem::path(src_path_)/std::to_string(req.shard_number);
  logger->Log(LogLevel::INFO, FormatString("Shard path: %s\n", shard_path));

  // Open the db
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.disable_auto_compactions = true;
  options.pliops_db_options.graceful_close_timeout_sec = 0;
  options.OptimizeForXdpRocks();
  auto s = DB::Open(options, shard_path, &db);
  if (!s.ok()) {
    logger->Log(LogLevel::ERROR, FormatString("DB::Open failed: %s\n", s.ToString()));
    return -1;
  }

  // Create a checkpoint
  Checkpoint* checkpoint_creator = nullptr;
  s = Checkpoint::Create(db, &checkpoint_creator);
  if (!s.ok()) {
    logger->Log(LogLevel::INFO, FormatString("Error in Checkpoint::Create: %s\n", s.ToString()));
    return -1;
  }
  // The checkpoint must reside on the same partition with the database
  checkpoint_path_ = std::filesystem::path(src_path_)/(std::to_string(req.shard_number) + "_checkpoint");
  logger->Log(LogLevel::INFO, FormatString("Checkpoint path: %s\n", checkpoint_path_));
  s = checkpoint_creator->CreateCheckpoint(checkpoint_path_);
  if (!s.ok()) {
    logger->Log(LogLevel::ERROR, FormatString("Error in Checkpoint::CreateCheckpoint: %s\n", s.ToString()));
    return -1;
  }

  // Close the original DB, we don't need it anymore
  s = db->Close();
  if (!s.ok()) {
    logger->Log(LogLevel::ERROR, FormatString("Error in Checkpoint::Create: %s\n", s.ToString()));
    return -1;
  }
  delete db;
  delete checkpoint_creator;

  // Open the checkpoint
  checkpoint_id_ = GetUniqueCheckpointName();
  auto rc = producer_->OpenShard(checkpoint_path_);
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("Producer::OpenShard failed\n"));
    return -1;
  }
  res.checkpoint_id = checkpoint_id_;

  return 0;
}

// Process start-streaming request
// Kuaishou should create a new function for this request
int CheckpointProducer::StartStreaming(
                          const StartStreamingRequest& req,
                          StartStreamingResponse& res,
                          uint64_t timeout_msec)
{
  logger->Log(LogLevel::INFO, FormatString("StartStreaming: ip=%s, checkpoint_id=%d, port=%d, #thread=%d\n",
                client_ip_.c_str(), req.checkpoint_id, req.consumer_port, req.max_num_of_threads));

  // We expect to get the same checkpoint_id as we provided in the CreateCheckpoint call
  if (req.checkpoint_id != checkpoint_id_) {
    logger->Log(LogLevel::ERROR, FormatString("Invalid checkpoint id\n"));
    return -1;
  }

  // Bind ReplicationDone callback
  using namespace std::placeholders;
  std::function<void(ProducerState, const std::string&)> done_cb =
    std::bind(&CheckpointProducer::ReplicationDone, this, _1, _2); 

  // Staring producer
  auto rc = producer_->Start(client_ip_, req.consumer_port, req.max_num_of_threads,
                              parallelism_, timeout_msec, done_cb);
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("Producer::Start failed\n"));
    return -1;
  }

  res.state = ServerState::IN_PROGRESS;
  return 0;
}

// Process get-status request
// Kuaishou should create a new function for this request
int CheckpointProducer::GetStatus(
                          const GetStatusRequest& req,
                          GetStatusResponse& res)
{
  // We expect to get the same checkpoint_id as we provided in the CreateCheckpoint call
  if (req.checkpoint_id != checkpoint_id_) {
    logger->Log(LogLevel::ERROR, FormatString("Invalid checkpoint id\n"));
    return -1;
  }

  // Get producer statistics
  auto rc = producer_->GetStats(res.num_kv_pairs, res.num_bytes);
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("Producer::Stats failed\n"));
    return -1;
  }

  // Get producer state
  std::string error;
  rc = producer_->GetState(res.state, error);
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("Producer::State failed\n"));
    return -1;
  }

  // Update the client_done_
  client_done_ = (res.state == ProducerState::DONE || res.state == ProducerState::ERROR);

  return 0;
}

int CheckpointProducer::WaitForCompletion(uint32_t timeout_msec)
{
  using namespace std::literals;

  // Wait till the producer is done. 
  logger->Log(LogLevel::INFO, FormatString("WaitForCompletion: %d msec\n", timeout_msec));
  std::unique_lock lock(producer_state_mutex_);
  auto rc = producer_state_cv_.wait_for(lock, 1ms*timeout_msec, [&] { 
    return producer_state_ == ProducerState::ERROR || producer_state_ == ProducerState::DONE;
  });

  return rc ? 0 : -1;
}

int CheckpointProducer::DestroyCheckpoint() {
  // Cleanup producer
  auto rc = producer_->Stop();
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("Producer::Stop failed\n"));
    return -1;
  }

  // Destroy the checkpoint
  auto s = DestroyDB(checkpoint_path_, Options());
  if (!s.ok()) {
    logger->Log(LogLevel::ERROR, FormatString("DestroyDB failed to destroy checkpoint: %s\n", s.ToString()));
    return -1;
  }

  return 0;
}

void CheckpointProducer::ReplicationDone(ProducerState state, const std::string& error)
{
  // Only mark here that the replication is done
  // The cleanup must be done a different thread (we cannot join threads from themself)
  std::lock_guard<std::mutex> lock(producer_state_mutex_);
  logger->Log(LogLevel::INFO, FormatString("ReplicationDone callback: %s %s\n", ToString(state), error));
  producer_state_ = state;
  producer_error_ = error;
  producer_state_cv_.notify_all();
}

int ProvideCheckpoint(RpcChannel& rpc, const std::string& src_path, const std::string& client_ip,
                        int parallelism, uint64_t timeout_msec, IKvPairSerializer& kv_pair_serializer)
{
  using namespace std::placeholders;

  CheckpointProducer cp(src_path, client_ip, parallelism, kv_pair_serializer);

  std::function<int(const CreateCheckpointRequest&, CreateCheckpointResponse&)>
    create_checkpoint_cb = std::bind(&CheckpointProducer::CreateCheckpoint, &cp, _1, _2); 
  auto rc = rpc.ProcessCommand(create_checkpoint_cb);
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("CheckpointProducer::CreateCheckpoint failed\n"));
    return -1;
  }

  std::function<int(const StartStreamingRequest&, StartStreamingResponse&)>
    start_streaming_cb = std::bind(&CheckpointProducer::StartStreaming, &cp, _1, _2, timeout_msec); 
  rc = rpc.ProcessCommand(start_streaming_cb);
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("CheckpointProducer::StartStreaming failed\n"));
    return -1;
  }

  std::function<int(const GetStatusRequest&, GetStatusResponse&)>
    get_status_cb = std::bind(&CheckpointProducer::GetStatus, &cp, _1, _2); 
  while(!cp.IsClientDone()) {
    rc = rpc.ProcessCommand(get_status_cb);
    if (rc) {
      logger->Log(LogLevel::ERROR, FormatString("CheckpointProducer::GetStatus failed\n"));
      return -1;
    }
  }

  auto wait_rc = cp.WaitForCompletion(1000);
  if (wait_rc) {
    logger->Log(LogLevel::ERROR, FormatString("CheckpointProducer::WaitForCompletion failed\n"));
     // return -1; - do not stop here, try to cleanup
  }

  rc = cp.DestroyCheckpoint();
  if (rc) {
    logger->Log(LogLevel::ERROR, FormatString("CheckpointProducer::DestroyCheckpoint failed\n"));
    return -1;
  }

  return wait_rc ? -1 : 0;
}
