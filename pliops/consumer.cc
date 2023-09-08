#include <mutex>
#include <filesystem>

#include "rocksdb/db.h"

#include "consumer.h"
#include "log.h"

namespace Replicator {

Consumer::Consumer()
  : kill_(false)
{}

Consumer::~Consumer() {
  shard_->Close();
  delete shard_;
}

void Consumer::WriterThread() {
  log_message(FormatString("Writer thread started\n"));

  while(!kill_) {
    // Pop the next KV pair.
    std::pair<std::string,std::string> message;
    message_queue_->wait_dequeue(message);
    std::string& key = message.first;
    std::string& value = message.second;
    if (key.empty()) {
      // message buffer is closed, finish.
      return;
    }
    // Insert KV to DB
    ROCKSDB_NAMESPACE::WriteOptions wo;
    wo.disableWAL = true;
    auto status = shard_->Put(wo, key, value);
    if(!status.ok()) {
      throw std::runtime_error(FormatString("Failed inserting key %s, reason: %s\n", key, status.ToString()));
    }
  }
  log_message(FormatString("Writer thread ended\n"));
}

void Consumer::CommunicationThread() {
  log_message(FormatString("Communication thread started.\n"));
  auto& listen_s = connection_;
  auto connection = accept(listen_s);
  std::string key, value;
  while(!kill_){
    try {
      std::tie(key, value) = connection->Receive();
      message_queue_->wait_enqueue({key, value});
    } catch (const ConnectionClosed&) {
      // For server writer threads, this is normal, the sender side might have finished sending replication data.
      log_message("Communication thread: Connection closed.\n");
      message_queue_->wait_enqueue({"", ""}); // Empty element signals end of messages.

      // TODO: set kill_
      // TODO: update status
      break;
    }
  }
}

ROCKSDB_NAMESPACE::DB* Consumer::OpenReplica(const std::string& replica_path) {
  unsigned int shard_id = 0;
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
#ifndef LEGACY_ROCKSDB_SENDER
  options.OptimizeForXdpRocks();
#endif
  options.max_background_jobs = 16;
  options.max_subcompactions = 32;
  auto shard_path = replica_path;
  if (!std::filesystem::exists(shard_path)) {
    std::filesystem::create_directories(shard_path);
  }
  auto status = ROCKSDB_NAMESPACE::DB::Open(options,
                                            shard_path,
                                            &db);
  if (!status.ok()) {
    throw std::runtime_error(FormatString("Failed to open db for shard #%d, reason: %s\n", shard_id, status.ToString()));
  }
  return db;
}

void Consumer::Start(const std::string& replica_path, uint16_t& port) {
  //
  shard_ = OpenReplica(replica_path);

  // Listen for incoming connection
  connection_ = bind(port);

  // For each shard, we allocate the same number of writer threads.
  // We already made sure the total number of threads is divisible by the number of shards.
  // We start the writer threads and the communication threads for each shard
  log_message("Starting writer threads\n");
  message_queue_ = std::make_unique<ServerMessageQueue>(SERVER_MESSAGE_QUEUE_CAPACITY);

  // Start writer threads
  writer_thread_ = std::make_unique<std::thread>([this]() {
    this->WriterThread();
  });

  // Start the communication thread
  communication_thread_ =  std::make_unique<std::thread>([this]() {
    this->CommunicationThread();
  });
}

void Consumer::Stop()
{
  kill_ = true;

  communication_thread_->join();
  writer_thread_->join();

  log_message("Done.\n");
}

}