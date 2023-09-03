#include "server.h"

using namespace ROCKSDB_NAMESPACE;


namespace {
uint32_t GetUniqueCheckpointName() {
  return 123; // For the sake of example we use hardcoded name
}

const std::string checkpoint_path = "/tmp";
}

CheckpointProducer::CheckpointProducer(const std::string &src_path)
  : src_path_(src_path)
{
}

// Process create-checkpoint request
// Kuaishou function: SyncServiceImpl::RequireCheckpoint(...)
void CheckpointProducer::CreateCheckpoint(
                          const CreateCheckpointRequest& req,
                          CreateCheckpointResponse& res)
{
  req.shard_number;
  // create path to db
  // create checkpoint and store its path/name
  auto shard_path =
    std::filesystem::path(src_path_)/std::to_string(req.shard_number);

  producer_->OpenShard(shard_path);
  res.checkpoint_id = GetUniqueCheckpointName();
  res.size_estimation = 1024*1024;

    // auto shard = shard_manager_->GetShard(request->shard());
    // if (!shard)  {
    // }

    // std::string name = GetUniqueCheckpointName();  // Allocate an unique name for the checkpoint

    // Status s;
    // auto dir_name = std::to_string(request->shard()) + "-" + name;
    // auto path = FsUtil::PathJoin({env_->db_path, "checkpoint", dir_name});  // Position to save checkpoint files
    // auto checkpoint = shard->CreateCheckpoint(name, path, &s);
    // if (!s.ok()) {
    // }

    // {
    //   std::lock_guard<std::mutex> locker(snapshot_lock_);
    //   checkpoint_map_[name] = checkpoint;
    //   LOG(INFO) << "SyncServiceImpl::RequireCheckpoint Create Checkpoint id:" << name
    //     << " shard:" << request->shard();
    // }

    // response->set_ok(true);
    // response->set_checkpoint_id(name);
    // done->Run();
}

// Process start-streaming request
// Kuaishou should create a new function for this request
void CheckpointProducer::StartStreaming(
                          const StartStreamingRequest& req,
                          StartStreamingResponse& res)
{
  producer_->Run(req.consumer_ip, req.consumer_port, req.max_num_of_threads);
  res.status = ServerStatus::IN_PROGRESS;
}

// Process get-status request
// Kuaishou should create a new function for this request
void CheckpointProducer::GetStatus(
                          const GetStatusRequest& req,
                          GetStatusResponse& res)
{
  res.status = ServerStatus::IN_PROGRESS;
}

void ProvideCheckpoint(RpcChannel& rpc, const std::string& src_path)
{
  using namespace std::placeholders;

  CheckpointProducer cp(src_path);

  std::function<void(const CreateCheckpointRequest& req, CreateCheckpointResponse& res)>
    create_checkpoint_cb = std::bind(&CheckpointProducer::CreateCheckpoint, &cp, _1, _2); 
  rpc.ProcessCommand(create_checkpoint_cb);

  std::function<void(const StartStreamingRequest& req, StartStreamingResponse& res)>
    start_streaming_cb = std::bind(&CheckpointProducer::StartStreaming, &cp, _1, _2); 
  rpc.ProcessCommand(start_streaming_cb);

  // while(true) {
  //   rpc.ProcessCommand(GetStatus);
  // }
}
