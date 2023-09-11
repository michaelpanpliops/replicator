#include "server.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/checkpoint.h"


namespace ROCKSDB_NAMESPACE {
Env* GetDefaultPosixEnv();
}

using namespace ROCKSDB_NAMESPACE;


namespace {
uint32_t GetUniqueCheckpointName() {
  return 123; // For the sake of the example we use hardcoded name
}
}

CheckpointProducer::CheckpointProducer(
  const std::string &src_path, const std::string& client_ip, int parallelism)
  : src_path_(src_path), client_ip_(client_ip), done_(false), parallelism_(parallelism)
{
  producer_ = std::make_unique<Replicator::Producer>();
}

// Process create-checkpoint request
// Kuaishou function: SyncServiceImpl::RequireCheckpoint(...)
void CheckpointProducer::CreateCheckpoint(
                          const CreateCheckpointRequest& req,
                          CreateCheckpointResponse& res)
{
  try {
    log_message(FormatString("CreateCheckpoint: shard=%d\n", req.shard_number));

    // Path to db
    auto shard_path =
      std::filesystem::path(src_path_)/std::to_string(req.shard_number);
    std::string dbname = shard_path;

    ROCKSDB_NAMESPACE::DB* db;
    ROCKSDB_NAMESPACE::Options options;
    options.create_if_missing = false;
    options.error_if_exists = false;
    options.disable_auto_compactions = true;
    options.pliops_db_options.graceful_close_timeout_sec = 0;
    options.OptimizeForXdpRocks();
    auto s = DB::Open(options, shard_path, &db);
    if (!s.ok()) {
      throw std::runtime_error(FormatString("DB::Open failed: %s\n", s.ToString()));
    }

    // Create a checkpoint
    Checkpoint* checkpoint_creator = nullptr;
    s = Checkpoint::Create(db, &checkpoint_creator);
    if (!s.ok()) {
      throw std::runtime_error(FormatString("Error in Checkpoint::Create: %s\n", s.ToString()));
    }

    // The checkpoint must reside on the same partition with the database
    checkpoint_path_ =
      std::filesystem::path(src_path_)/(std::to_string(req.shard_number)+"_checkpoint");
    log_message(FormatString("checkpoint_path_=%s\n", checkpoint_path_));
    s = checkpoint_creator->CreateCheckpoint(checkpoint_path_);
    if (!s.ok()) {
      throw std::runtime_error(FormatString("Error in Checkpoint::Create: %s\n", s.ToString()));
    }

    s = db->Close();
    if (!s.ok()) {
      throw std::runtime_error(FormatString("Error in Checkpoint::Create: %s\n", s.ToString()));
    }
    delete db;
    delete checkpoint_creator;

    checkpoint_id_ = GetUniqueCheckpointName();
    log_message(FormatString("checkpoint_id_=%d\n", checkpoint_id_));

    producer_->OpenShard(checkpoint_path_);
    res.checkpoint_id = checkpoint_id_;
    res.db_size_estimation = 1024*1024;
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("CreateCheckpoint:\n\t%s", e.what()));
  }
}

// Process start-streaming request
// Kuaishou should create a new function for this request
void CheckpointProducer::StartStreaming(
                          const StartStreamingRequest& req,
                          StartStreamingResponse& res)
{
  try {
    log_message(FormatString("StartStreaming: ip=%s, checkpoint_id=%d, port=%d, #thread=%d\n",
                  client_ip_.c_str(), req.checkpoint_id, req.consumer_port, req.max_num_of_threads));

    if (req.checkpoint_id != checkpoint_id_) {
      throw std::runtime_error(FormatString("Invalid checkpoint id\n"));
    }

    std::function<void()> done_cb = std::bind(&CheckpointProducer::ReplicationDone, this); 

    producer_->Start(client_ip_, req.consumer_port, req.max_num_of_threads, parallelism_, done_cb);
    res.status = ServerStatus::IN_PROGRESS;
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("StartStreaming:\n\t%s", e.what()));
  }
}

// Process get-status request
// Kuaishou should create a new function for this request
bool CheckpointProducer::GetStatus(
                          const GetStatusRequest& req,
                          GetStatusResponse& res)
{
  try {
    if (req.checkpoint_id != checkpoint_id_) {
      throw std::runtime_error(FormatString("Invalid checkpoint id\n"));
    }

    res.status = done_ ? ServerStatus::DONE : ServerStatus::IN_PROGRESS;
    producer_->Stats(res.num_kv_pairs, res.num_bytes);
    return done_; // done_returned_to_client_ = 
  } catch(const std::exception& e) {
    throw std::runtime_error(FormatString("StartStreaming:\n\t%s", e.what()));
  }
}

void CheckpointProducer::ReplicationDone()
{
  log_message(FormatString("ReplicationDone\n"));
  done_ = true;

  // Destroy the checkpoint
  auto s = DestroyDB(checkpoint_path_, Options());
  if (!s.ok()) {
    throw std::runtime_error(FormatString("DestroyDB failed to destroy checkpoint: %s\n", s.ToString()));
  } 
}

void ProvideCheckpoint(RpcChannel& rpc, const std::string& src_path, const std::string& client_ip,
                        int parallelism)
{
  using namespace std::placeholders;

  CheckpointProducer cp(src_path, client_ip, parallelism);

  std::function<void(const CreateCheckpointRequest& req, CreateCheckpointResponse& res)>
    create_checkpoint_cb = std::bind(&CheckpointProducer::CreateCheckpoint, &cp, _1, _2); 
  rpc.ProcessCommand(create_checkpoint_cb);

  std::function<void(const StartStreamingRequest& req, StartStreamingResponse& res)>
    start_streaming_cb = std::bind(&CheckpointProducer::StartStreaming, &cp, _1, _2); 
  rpc.ProcessCommand(start_streaming_cb);

  std::function<bool(const GetStatusRequest& req, GetStatusResponse& res)>
    get_status_cb = std::bind(&CheckpointProducer::GetStatus, &cp, _1, _2); 
  bool done = false;
  while(!done) {
    done = rpc.ProcessCommand(get_status_cb);
  }
  cp.Stop();
}
