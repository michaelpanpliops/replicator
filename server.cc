#include "server.h"

using namespace ROCKSDB_NAMESPACE;


namespace {
uint32_t GetUniqueCheckpointName() {
  return 123; // For the sake of the example we use hardcoded name
}

const std::string checkpoint_path = "/tmp";
}

CheckpointProducer::CheckpointProducer(const std::string &src_path, const std::string& client_ip)
  : src_path_(src_path), client_ip_(client_ip), done_(false)
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

    // TODO: create checkpoint and store its path/name

    producer_->OpenShard(shard_path);
    res.checkpoint_id = GetUniqueCheckpointName();
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
    log_message(FormatString("StartStreaming: ip=%s, port=%d, #thread=%d\n",
                  client_ip_.c_str(), req.consumer_port, req.max_num_of_threads));

    std::function<void()> done_cb = std::bind(&CheckpointProducer::ReplicationDone, this); 

    producer_->Start(client_ip_, req.consumer_port, req.max_num_of_threads, done_cb);
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
  res.status = done_ ? ServerStatus::DONE : ServerStatus::IN_PROGRESS;
  producer_->Stats(res.num_kv_pairs, res.num_bytes);
  return done_; // done_returned_to_client_ = 
}

void CheckpointProducer::ReplicationDone()
{
  log_message(FormatString("ReplicationDone\n"));
  done_ = true;
  // remove the checkpoint: here or in ProvideCheckpoint???
}

void ProvideCheckpoint(RpcChannel& rpc, const std::string& src_path, const std::string& client_ip)
{
  using namespace std::placeholders;

  CheckpointProducer cp(src_path, client_ip);

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
