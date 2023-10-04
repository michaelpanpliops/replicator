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
  const std::string &src_path, const std::string& client_ip,
  int parallelism, int ops_timeout_msec, int connect_timeout_msec,
  IKvPairSerializer& kv_pair_serializer)
  : src_path_(src_path), client_ip_(client_ip), parallelism_(parallelism)
  , ops_timeout_msec_(ops_timeout_msec), connect_timeout_msec_(connect_timeout_msec)
{
  producer_ = std::make_unique<Replicator::Producer>(kv_pair_serializer);
}

CheckpointProducer::~CheckpointProducer()
{
  DestroyCheckpoint();
}

// Process create-checkpoint request
// Kuaishou function: SyncServiceImpl::RequireCheckpoint(...)
RepStatus CheckpointProducer::CreateCheckpoint(
                          const CreateCheckpointRequest& req,
                          CreateCheckpointResponse& res)
{
  logger->Log(Severity::INFO, FormatString("CreateCheckpoint: shard=%d\n", req.shard_number));

  // Path to the db
  std::string shard_path = std::filesystem::path(src_path_)/std::to_string(req.shard_number);
  logger->Log(Severity::INFO, FormatString("Shard path: %s\n", shard_path));

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
    logger->Log(Severity::ERROR, FormatString("DB::Open failed: %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("DB::Open failed: %s", s.ToString()));
  }

  // Create a checkpoint
  Checkpoint* checkpoint_creator = nullptr;
  s = Checkpoint::Create(db, &checkpoint_creator);
  if (!s.ok()) {
    logger->Log(Severity::ERROR, FormatString("Error in Checkpoint::Create: %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Error in Checkpoint::Create: %s", s.ToString()));
  }
  // The checkpoint must reside on the same partition with the database
  checkpoint_path_ = std::filesystem::path(src_path_)/(std::to_string(req.shard_number) + "_checkpoint");
  logger->Log(Severity::INFO, FormatString("Checkpoint path: %s\n", checkpoint_path_));
  s = checkpoint_creator->CreateCheckpoint(checkpoint_path_);
  if (!s.ok()) {
    logger->Log(Severity::ERROR, FormatString("Error in Checkpoint::CreateCheckpoint: %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Error in Checkpoint::CreateCheckpoint: %s", s.ToString()));
  }

  // Close the original DB, we don't need it anymore
  s = db->Close();
  if (!s.ok()) {
    logger->Log(Severity::ERROR, FormatString("Error in DB close %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Error in DB close %s", s.ToString()));
  }
  delete db;
  delete checkpoint_creator;

  // Open the checkpoint
  checkpoint_id_ = GetUniqueCheckpointName();
  auto rc = producer_->OpenShard(checkpoint_path_);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Producer::OpenShard failed\n"));
    return rc;
  }
  res.checkpoint_id = checkpoint_id_;

  return RepStatus();
}

// Process start-streaming request
// Kuaishou should create a new function for this request
RepStatus CheckpointProducer::StartStreaming(
                          const StartStreamingRequest& req,
                          StartStreamingResponse& res)
{
  logger->Log(Severity::INFO, FormatString("StartStreaming: ip=%s, checkpoint_id=%d, port=%d, #thread=%d\n",
                client_ip_.c_str(), req.checkpoint_id, req.consumer_port, req.max_num_of_threads));

  // We expect to get the same checkpoint_id as we provided in the CreateCheckpoint call
  if (req.checkpoint_id != checkpoint_id_) {
    logger->Log(Severity::ERROR, FormatString("Invalid checkpoint id\n"));
    return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Invalid checkpoint id"));
  }

  // Bind ReplicationDone callback
  using namespace std::placeholders;
  std::function<void(ProducerState, const RepStatus&)> done_cb =
    std::bind(&CheckpointProducer::ReplicationDone, this, _1, _2); 

  // Staring producer
  RepStatus rc = producer_->Start(client_ip_, req.consumer_port, req.max_num_of_threads,
                                  parallelism_, ops_timeout_msec_, connect_timeout_msec_,
                                  done_cb);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Producer::Start failed\n"));
    return rc;
  }

  res.state = ServerState::IN_PROGRESS;
  return RepStatus();
}

// Process get-status request
// Kuaishou should create a new function for this request
RepStatus CheckpointProducer::GetStatus(
                          const GetStatusRequest& req,
                          GetStatusResponse& res)
{
  // We expect to get the same checkpoint_id as we provided in the CreateCheckpoint call
  if (req.checkpoint_id != checkpoint_id_) {
    logger->Log(Severity::ERROR, FormatString("Invalid checkpoint id\n"));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Invalid checkpoint id"));
  }

  // Get producer statistics
  auto rc = producer_->GetStats(res.num_kv_pairs, res.num_bytes);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Producer::Stats failed\n"));
    return rc;
  }

  // Get producer state
  RepStatus status;
  rc = producer_->GetState(res.state, status);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Producer::GetState failed\n"));
    return rc;
  }

  // Update the client_done_
  client_done_ = IsFinalState(res.state);

  return RepStatus();
}

RepStatus CheckpointProducer::WaitForCompletion(uint32_t timeout_msec)
{
  using namespace std::literals;

  // Wait till the producer is done. 
  logger->Log(Severity::INFO, FormatString("WaitForCompletion: %d msec\n", timeout_msec));
  std::unique_lock lock(producer_state_mutex_);
  auto rc = producer_state_cv_.wait_for(lock, 1ms*timeout_msec, [&] { 
    return producer_state_ == ProducerState::ERROR || producer_state_ == ProducerState::DONE;
  });

  return rc ? RepStatus() : RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, "WaitForCompletion failed.");
}

RepStatus CheckpointProducer::DestroyCheckpoint() {
  // Cleanup producer
  auto rc = producer_->Stop();
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("Producer::Stop failed\n"));
    return rc;
  }

  // Destroy the checkpoint
  auto s = DestroyDB(checkpoint_path_, Options());
  if (!s.ok()) {
    logger->Log(Severity::ERROR, FormatString("DestroyDB failed to destroy checkpoint: %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("DestroyDB failed to destroy checkpoint: %s", s.ToString()));
  }

  return RepStatus();
}

void CheckpointProducer::ReplicationDone(ProducerState state, const RepStatus& status)
{
  // Only mark here that the replication is done
  // The cleanup must be done a different thread (we cannot join threads from themself)
  std::lock_guard<std::mutex> lock(producer_state_mutex_);
  auto severity = Severity::INFO;
  if (state == ProducerState::ERROR) {
    severity = Severity::ERROR;
  }
  logger->Log(severity, FormatString("ReplicationDone callback: %s, %s\n", ToString(state), status.ToString()));
  producer_state_ = state;
  producer_status_ = status;
  producer_state_cv_.notify_all();
}

RepStatus ProvideCheckpoint(RpcChannel& rpc,
                            const std::string& src_path,
                            const std::string& client_ip,
                            int parallelism, 
                            int ops_timeout_msec,
                            int connect_timeout_msec,
                            IKvPairSerializer& kv_pair_serializer)
{
  using namespace std::placeholders;

  CheckpointProducer cp(src_path, client_ip,
                        parallelism, ops_timeout_msec, connect_timeout_msec, kv_pair_serializer);

  std::function<RepStatus(const CreateCheckpointRequest&, CreateCheckpointResponse&)>
    create_checkpoint_cb = std::bind(&CheckpointProducer::CreateCheckpoint, &cp, _1, _2); 
  auto rc = rpc.ProcessCommand(create_checkpoint_cb);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("CheckpointProducer::CreateCheckpoint failed\n"));
    return rc;
  }

  std::function<RepStatus(const StartStreamingRequest&, StartStreamingResponse&)>
    start_streaming_cb = std::bind(&CheckpointProducer::StartStreaming, &cp, _1, _2); 
  rc = rpc.ProcessCommand(start_streaming_cb);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("CheckpointProducer::StartStreaming failed\n"));
    return rc;
  }

  std::function<RepStatus(const GetStatusRequest&, GetStatusResponse&)>
    get_status_cb = std::bind(&CheckpointProducer::GetStatus, &cp, _1, _2); 
  while(!cp.IsClientDone()) {
    rc = rpc.ProcessCommand(get_status_cb);
    if (!rc.IsOk()) {
      logger->Log(Severity::ERROR, FormatString("CheckpointProducer::GetStatus failed\n"));
      return rc;
    }
  }

  auto wait_rc = cp.WaitForCompletion(1000);
  if (!wait_rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("CheckpointProducer::WaitForCompletion failed\n"));
  }

  rc = cp.DestroyCheckpoint();
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, FormatString("CheckpointProducer::DestroyCheckpoint failed\n"));
    return rc;
  }

  return wait_rc.IsOk() ? RepStatus() : rc;
}
