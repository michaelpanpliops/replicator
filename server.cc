#include "server.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/checkpoint.h"


namespace {
// For the sake of the example we use hardcoded checkpoint name
uint32_t GetUniqueCheckpointName() { return 12345; }
}

ReplicationServer::ReplicationServer(
  const std::string &src_path, const std::string& client_ip,
  int parallelism, int ops_timeout_msec, int connect_timeout_msec,
  IKvPairSerializer& kv_pair_serializer)
  : src_path_(src_path), client_ip_(client_ip), parallelism_(parallelism)
  , ops_timeout_msec_(ops_timeout_msec), connect_timeout_msec_(connect_timeout_msec)
{
  producer_ = std::make_unique<Replicator::Producer>(kv_pair_serializer);
}

ReplicationServer::~ReplicationServer()
{
  DestroyCheckpoint();
}

// Process begin-replication request
RepStatus ReplicationServer::BeginReplicationRpc(
                          const CreateCheckpointRequest& req,
                          CreateCheckpointResponse& res)
{
  // Path to the db
  logger->Log(Severity::INFO, FormatString("Shard path: %s\n", src_path_));

  // Open the db
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.disable_auto_compactions = true;
#ifdef XDPROCKS
  options.OptimizeForXdpRocks();
  options.pliops_db_options.graceful_close_timeout_sec = 0;
#endif
  auto s = ROCKSDB_NAMESPACE::DB::Open(options, src_path_, &db);
  if (!s.ok()) {
    logger->Log(Severity::ERROR, FormatString("DB::Open failed: %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("DB::Open failed: %s", s.ToString()));
  }
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> db_ptr(db); // guarantee db deletion

  // Create a checkpoint
  ROCKSDB_NAMESPACE::Checkpoint* checkpoint_creator = nullptr;
  s = ROCKSDB_NAMESPACE::Checkpoint::Create(db, &checkpoint_creator);
  if (!s.ok()) {
    logger->Log(Severity::ERROR, FormatString("Error in Checkpoint::Create: %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Error in Checkpoint::Create: %s", s.ToString()));
  }
  std::unique_ptr<ROCKSDB_NAMESPACE::Checkpoint> checkpoint_ptr(checkpoint_creator); // guarantee checkpoint deletion

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
  db_ptr.reset();
  checkpoint_ptr.reset();

  // Open the checkpoint
  checkpoint_id_ = GetUniqueCheckpointName();
  auto rc = producer_->OpenShard(checkpoint_path_);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Producer::OpenShard failed\n"));
    return rc;
  }
  res.checkpoint_id = checkpoint_id_;

  return RepStatus();
}

// Process start-streaming request
RepStatus ReplicationServer::StartReplicationStreamingRpc(
                          const StartStreamingRequest& req,
                          StartStreamingResponse& res)
{
  logger->Log(Severity::INFO, FormatString("StartReplicationStreamingRpc: ip=%s, checkpoint_id=%d, port=%d\n",
                client_ip_.c_str(), req.checkpoint_id, req.consumer_port));

  // We expect to get the same checkpoint_id as we provided in the BeginReplicationRpc call
  if (req.checkpoint_id != checkpoint_id_) {
    logger->Log(Severity::ERROR, FormatString("Invalid checkpoint id\n"));
    return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Invalid checkpoint id"));
  }

  // Bind ReplicationDone callback
  using namespace std::placeholders;
  std::function<void(ProducerState, const RepStatus&)> done_cb =
    std::bind(&ReplicationServer::ReplicationDone, this, _1, _2); 

  // Staring producer
  RepStatus rc = producer_->Start(client_ip_, req.consumer_port,
                                  parallelism_, ops_timeout_msec_, connect_timeout_msec_,
                                  done_cb);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Producer::Start failed\n"));
    return rc;
  }

  res.state = ServerState::IN_PROGRESS;
  return RepStatus();
}

// Process get-status request
RepStatus ReplicationServer::GetReplicationStatusRpc(
                          const GetStatusRequest& req,
                          GetStatusResponse& res)
{
  // We expect to get the same checkpoint_id as we provided in the BeginReplicationRpc call
  if (req.checkpoint_id != checkpoint_id_) {
    logger->Log(Severity::ERROR, FormatString("Invalid checkpoint id\n"));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Invalid checkpoint id"));
  }

  // Get producer statistics
  auto rc = producer_->GetStats(res.num_kv_pairs, res.num_bytes);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Producer::Stats failed\n"));
    return rc;
  }

  // Get producer state
  RepStatus status;
  rc = producer_->GetState(res.state, status);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Producer::GetState failed\n"));
    return rc;
  }

  server_done_ = IsFinalState(res.state);
  client_done_ = IsFinalState(req.state);

  return RepStatus();
}

// Process end-replication request
RepStatus ReplicationServer::EndReplicationRpc(const EndReplicationRequest& req, EndReplicationResponse& res)
{
  // We expect to get the same checkpoint_id as we provided in the BeginReplicationRpc call
  if (req.checkpoint_id != checkpoint_id_) {
    logger->Log(Severity::ERROR, FormatString("Invalid checkpoint id\n"));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Invalid checkpoint id"));
  }

  // Get producer state
  RepStatus status;
  auto rc = producer_->GetState(res.state, status);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Producer::GetState failed\n"));
    return rc;
  }

  return RepStatus();
}

RepStatus ReplicationServer::WaitForCompletion(uint32_t timeout_msec)
{
  using namespace std::literals;

  // Wait till the producer is done. 
  logger->Log(Severity::INFO, FormatString("WaitForCompletion: %d msec\n", timeout_msec));
  std::unique_lock lock(producer_state_mutex_);
  auto rc = producer_state_cv_.wait_for(lock, 1ms*timeout_msec, [&] { 
    return IsFinalState(producer_state_);
  });

  return rc ? RepStatus() : RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, "WaitForCompletion failed.");
}

RepStatus ReplicationServer::DestroyCheckpoint() {
  // Cleanup producer
  auto rc = producer_->Stop();
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Producer::Stop failed\n"));
    return rc;
  }

  // Destroy the checkpoint
  auto s = ROCKSDB_NAMESPACE::DestroyDB(checkpoint_path_, ROCKSDB_NAMESPACE::Options());
  if (!s.ok()) {
    logger->Log(Severity::ERROR, FormatString("DestroyDB failed to destroy checkpoint: %s\n", s.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("DestroyDB failed to destroy checkpoint: %s", s.ToString()));
  }

  return RepStatus();
}

void ReplicationServer::ReplicationDone(ProducerState state, const RepStatus& status)
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

RepStatus RunReplicationServer(
                            RpcChannel& rpc,
                            const std::string& src_path,
                            const std::string& client_ip,
                            int parallelism, 
                            int ops_timeout_msec,
                            int connect_timeout_msec,
                            IKvPairSerializer& kv_pair_serializer)
{
  using namespace std::placeholders;

  ReplicationServer rs(src_path, client_ip, parallelism, ops_timeout_msec,
                       connect_timeout_msec, kv_pair_serializer);

  std::function<RepStatus(const CreateCheckpointRequest&, CreateCheckpointResponse&)>
    create_checkpoint_cb = std::bind(&ReplicationServer::BeginReplicationRpc, &rs, _1, _2); 
  auto rc = rpc.ProcessCommand(create_checkpoint_cb);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("ReplicationServer::BeginReplicationRpc failed\n"));
    return rc;
  }

  std::function<RepStatus(const StartStreamingRequest&, StartStreamingResponse&)>
    start_streaming_cb = std::bind(&ReplicationServer::StartReplicationStreamingRpc, &rs, _1, _2); 
  rc = rpc.ProcessCommand(start_streaming_cb);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("ReplicationServer::StartReplicationStreamingRpc failed\n"));
    return rc;
  }

  std::function<RepStatus(const GetStatusRequest&, GetStatusResponse&)>
    get_status_cb = std::bind(&ReplicationServer::GetReplicationStatusRpc, &rs, _1, _2); 
  while(!rs.IsClientDone() && !rs.IsServerDone()) {
    rc = rpc.ProcessCommand(get_status_cb);
    if (!rc.ok()) {
      logger->Log(Severity::ERROR, FormatString("ReplicationServer::GetReplicationStatusRpc failed\n"));
      return rc;
    }
  }

  std::function<RepStatus(const EndReplicationRequest&, EndReplicationResponse&)>
    end_replication_cb = std::bind(&ReplicationServer::EndReplicationRpc, &rs, _1, _2); 
  rc = rpc.ProcessCommand(end_replication_cb);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("ReplicationServer::EndReplicationRpc failed\n"));
    return rc;
  }

  // Wait till the producer is done.
  auto timeout_msec = std::max(rs.ops_timeout_msec_, rs.connect_timeout_msec_) + 1000;
  auto wait_rc = rs.WaitForCompletion(timeout_msec);
  if (!wait_rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("ReplicationServer::WaitForCompletion failed\n"));
  }

  rc = rs.DestroyCheckpoint();
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("ReplicationServer::DestroyCheckpoint failed\n"));
    return rc;
  }

  return wait_rc.ok() ? RepStatus() : rc;
}
