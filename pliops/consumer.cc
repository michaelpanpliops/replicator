#include <mutex>
#include <filesystem>
#include <future>

#include "rocksdb/db.h"

#include "consumer.h"
#include "log.h"

namespace Replicator {

Consumer::Consumer(uint64_t timeout_msec)
  : kill_(false), timeout_msec_(timeout_msec)
{}

Consumer::~Consumer() {
  if (shard_) {
    shard_->Close();
    delete shard_;
  }
}

void Consumer::WriterThread() {
  log_message(FormatString("Writer thread started\n"));

  while(!kill_) {
    // Pop the next KV pair.
    std::pair<std::string,std::string> message;
    if (!message_queue_->wait_dequeue_timed(message, msec_to_usec(timeout_msec_))) {
      log_message(FormatString("Writer thread: Failed to dequeue message, reason: timeout\n"));
      SetState(ConsumerState::ERROR, "");
      return;
    }
    std::string& key = message.first;
    std::string& value = message.second;
    if (key.empty()) {
      // message buffer is closed, finish.
      break;
    }

    // Insert KV to DB
    ROCKSDB_NAMESPACE::WriteOptions wo;
    wo.disableWAL = true;
    auto status = shard_->Put(wo, key, value);
    if(!status.ok()) {
      log_message(FormatString("Writer thread: Failed inserting key %s, reason: %s\n", key, status.ToString()));
      SetState(ConsumerState::ERROR, "");
      return;
    }

    statistics_.num_kv_pairs++; // atomic
    statistics_.num_bytes.fetch_add(key.size() + value.size()); // atomic
  }

  // Close the DB
  auto status = shard_->Close();
  delete shard_;
  shard_ = nullptr;
  if (!status.ok()) {
    log_message(FormatString("Writer thread: shard_->Close failed, reason: %s\n", status.ToString()));
    SetState(ConsumerState::ERROR, "");
    return;
  }

  // Print out replication performance metrics
  log_message(FormatString("Stat.num_kv_pairs: %lld, Stat.num_bytes: %lld \n",
              statistics_.num_kv_pairs.load(), statistics_.num_bytes.load()));
  auto current_time = std::chrono::system_clock::now();
  auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time_);
  log_message(FormatString("%.1f pairs/sec\n", statistics_.num_kv_pairs.load()/(double)elapsed_seconds.count()));
  log_message(FormatString("%.1f bytes/sec\n", statistics_.num_bytes.load()/(double)elapsed_seconds.count()));

  SetState(ConsumerState::DONE, "");

  log_message(FormatString("Writer thread ended\n"));
}

void Consumer::CommunicationThread()
{
  log_message(FormatString("Communication thread started.\n"));
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connection;
  auto rc = Accept(*connection_, connection, timeout_msec_);
  if (rc) {
    log_message(FormatString("Communication thread: accept failed\n"));
    SetState(ConsumerState::ERROR, "");
    return;
  }

  std::string key, value;
  while(!kill_) {
    rc = connection->Receive(key, value);
    if (rc) {
      log_message("Communication thread: recv failed.\n");
      SetState(ConsumerState::ERROR, "");
      return;
    }
    if(!message_queue_->wait_enqueue_timed({key, value}, msec_to_usec(timeout_msec_))) {
      log_message(FormatString("Communication thread: Failed to enqueue, reason: timeout\n"));
      SetState(ConsumerState::ERROR, "");
      return;
    }
    if (key.empty()) {
      // Finish thread
      return;
    }
  }
  log_message(FormatString("Communication thread ended.\n"));
}

int Consumer::OpenReplica(const std::string& replica_path)
{
  unsigned int shard_id = 0;
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
                                            &shard_);
  if (!status.ok()) {
    log_message(FormatString("Failed to open db for shard #%d, reason: %s\n", shard_id, status.ToString()));
    return -1;
  }

  return 0;
}

int Consumer::Start(const std::string& replica_path, uint16_t& port,
                    std::function<void(ConsumerState, const std::string&)>& done_callback)
{
  done_callback_ = done_callback;

  // Open replica
  auto rc = OpenReplica(replica_path);
  if (rc || !shard_) {
    log_message(FormatString("OpenReplica failed\n"));
    return -1;
  }

  // Listen for incoming connection
  rc = Bind(port, connection_);
  if (rc) {
    log_message(FormatString("Socket binding failed\n"));
    return -1;
  }

  message_queue_ = std::make_unique<ServerMessageQueue>(SERVER_MESSAGE_QUEUE_CAPACITY);
  start_time_ = std::chrono::system_clock::now();

  // Start writer thread
  log_message("Starting writer thread\n");
  writer_thread_ = std::make_unique<std::thread>([this]() {
    this->WriterThread();
  });

  // Start the communication thread
  log_message("Starting communication thread\n");
  communication_thread_ =  std::make_unique<std::thread>([this]() {
    this->CommunicationThread();
  });

  return 0;
}

void Consumer::StopImpl()
{
  // This is a best effort stopping, we will try to stop the threads
  // and close the shard, but won't report any error if something goes wrong

  // Tell threads to exit
  kill_ = true;
  communication_thread_->join();

  // We need to signal writer thread with poison pill
  // because it could be waiting on queue
  message_queue_->wait_enqueue_timed({"", ""}, 1000);
  writer_thread_->join();
  connection_.reset();
  message_queue_.reset();

  // Close shard
  if (shard_) {
    shard_->Close();
    delete shard_;
    shard_=nullptr;
  }
}

int Consumer::Stop()
{
  using namespace std::literals;

  // Move state into STOPPED, so we won't get any ERROR/DONE notifications from now
  SetState(ConsumerState::STOPPED, "");

  std::future<void>* future = new std::future<void>;
  *future = std::async(std::launch::async, &Consumer::StopImpl, this);
  if (future->wait_for(10s) == std::future_status::timeout) {
    log_message("Stop failed, some threads are stuck.\n");
    // If the app will try destroy the Producer object, it will crash
    return -1;
  } else {
    delete future;
  }

  log_message("Consumer finished its jobs\n");
  return 0;
}

int Consumer::GetState(ConsumerState& state, std::string& error)
{
  std::lock_guard<std::mutex> lock(state_mutex_);
  state = state_;
  error = error_;
  return 0;
}

int Consumer::GetStats(uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  num_kv_pairs = statistics_.num_kv_pairs;
  num_bytes = statistics_.num_bytes;
  return 0;
}

void Consumer::SetState(const ConsumerState& state, const std::string& error)
{
  std::lock_guard<std::mutex> lock(state_mutex_);
  // Never overwrite a state if it is already in a final state
  if (IsFinalState(state_)) {
    return;
  }
  state_ = state;
  error_ = error;
  // Call the callback for final states only
  if (IsFinalState(state_)) {
    done_callback_(state_, error_);
  }
}

}
