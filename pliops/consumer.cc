#include <mutex>
#include <filesystem>
#include <future>

#include "rocksdb/db.h"

#include "consumer.h"
#include "utils/string_util.h"
#include "pliops/logger.h"

namespace Replicator {

Consumer::Consumer(
  uint32_t ops_timeout_msec, uint32_t connect_timeout_msec,
  IKvPairSerializer& kv_pair_serializer)
  : kill_(false)
  , ops_timeout_msec_(ops_timeout_msec)
  , connect_timeout_msec_(connect_timeout_msec)
  , kv_pair_serializer_(kv_pair_serializer)
{}

Consumer::~Consumer() {
  if (shard_) {
    shard_->Close();
    delete shard_;
  }
}

void Consumer::WriterThread() {
  logger->Log(Severity::INFO, FormatString("Writer thread started\n"));

  while(!kill_) {
    // Pop the next KV pair.
    std::pair<std::string,std::string> message;
    if (!message_queue_->wait_dequeue_timed(message, msec_to_usec(ops_timeout_msec_))) {
      logger->Log(Severity::ERROR, FormatString("Writer thread: Failed to dequeue message, reason: timeout\n"));
      SetState(ConsumerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, "Writer thread: Failed to dequeue message, reason: timeout"));
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
      logger->Log(Severity::ERROR, FormatString("Writer thread: Failed inserting key %s, reason: %s\n", key, status.ToString()));
      SetState(ConsumerState::ERROR, RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Writer thread: Failed inserting key %s, reason: %s", key, status.ToString())));
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
    logger->Log(Severity::ERROR, FormatString("Writer thread: shard_->Close failed, reason: %s\n", status.ToString()));
    SetState(ConsumerState::ERROR, RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Writer thread: shard_->Close failed, reason: %s", status.ToString())));
    return;
  }

  // Print out replication performance metrics
  logger->Log(Severity::INFO, FormatString("Stat.num_kv_pairs: %lld, Stat.num_bytes: %lld \n",
              statistics_.num_kv_pairs.load(), statistics_.num_bytes.load()));
  auto current_time = std::chrono::system_clock::now();
  auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time_);
  logger->Log(Severity::INFO, FormatString("%.1f pairs/sec\n", statistics_.num_kv_pairs.load()/(double)elapsed_seconds.count()));
  logger->Log(Severity::INFO, FormatString("%.1f bytes/sec\n", statistics_.num_bytes.load()/(double)elapsed_seconds.count()));

  SetState(ConsumerState::DONE, RepStatus());

  logger->Log(Severity::INFO, FormatString("Writer thread ended\n"));
}

void Consumer::CommunicationThread()
{
  logger->Log(Severity::INFO, FormatString("Communication thread started.\n"));
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connection;
  auto rc = Accept(*connection_, connection, connect_timeout_msec_);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Communication thread: %s\n", rc.ToString()));
    SetState(ConsumerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Communication thread: %s", rc.ToString())));
    return;
  }

  std::string key, value;
  while(!kill_) {
    rc = connection->Receive(key, value, kv_pair_serializer_);
    if (!rc.ok()) {
      logger->Log(Severity::ERROR, FormatString("Communication thread: %s\n", rc.ToString()));
      SetState(ConsumerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Communication thread: %s", rc.ToString())));
      return;
    }
    if(!message_queue_->wait_enqueue_timed({key, value}, msec_to_usec(ops_timeout_msec_))) {
      logger->Log(Severity::ERROR, FormatString("Communication thread: Failed to enqueue, reason: timeout\n"));
      SetState(ConsumerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Communication thread: Failed to enqueue, reason: timeout")));
      return;
    }
    if (key.empty()) {
      // Finish thread
      return;
    }
  }
  logger->Log(Severity::INFO, FormatString("Communication thread ended.\n"));
}

RepStatus Consumer::OpenReplica(const std::string& replica_path)
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
    logger->Log(Severity::ERROR, FormatString("Failed to open db for shard #%d, reason: %s\n", shard_id, status.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Failed to open db for shard #%d, reason: %s\n", shard_id, status.ToString()));
  }

  if (!shard_) {
    logger->Log(Severity::ERROR, FormatString("Failed to open db for shard #%d, reason: shard = null\n", shard_id));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Failed to open db for shard #%d, reason: shard = null\n", shard_id));
  }

  return RepStatus();
}

RepStatus Consumer::Start(const std::string& replica_path, uint16_t& port,
                    std::function<void(ConsumerState, const RepStatus&)>& done_callback)
{
  done_callback_ = done_callback;

  // Open replica
  auto rc = OpenReplica(replica_path);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("OpenReplica failed\n"));
    return rc;
  }

  // Listen for incoming connection
  rc = Bind(port, connection_);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("Socket binding failed\n"));
    return rc;
  }

  message_queue_ = std::make_unique<ConsumerMessageQueue>(CONSUMER_MESSAGE_QUEUE_CAPACITY);
  start_time_ = std::chrono::system_clock::now();

  // Start writer thread
  logger->Log(Severity::INFO, "Starting writer thread\n");
  writer_thread_ = std::make_unique<std::thread>([this]() {
    this->WriterThread();
  });

  // Start the communication thread
  logger->Log(Severity::INFO, "Starting communication thread\n");
  communication_thread_ =  std::make_unique<std::thread>([this]() {
    this->CommunicationThread();
  });

  return RepStatus();
}

void Consumer::StopImpl()
{
  // This is a best effort stopping, we will try to stop the threads
  // and close the shard, but won't report any error if something goes wrong

  // Tell threads to exit
  kill_ = true;
  if (communication_thread_) communication_thread_->join();

  // We need to signal writer thread with poison pill
  // because it could be waiting on queue
  if (message_queue_) message_queue_->wait_enqueue_timed({"", ""}, 1000);
  if (writer_thread_) writer_thread_->join();
  connection_.reset();
  message_queue_.reset();

  // Close shard
  if (shard_) {
    shard_->Close();
    delete shard_;
    shard_=nullptr;
  }
}

RepStatus Consumer::Stop()
{
  using namespace std::literals;

  std::future<void>* future = new std::future<void>;
  *future = std::async(std::launch::async, &Consumer::StopImpl, this);
  auto max_timeout = std::max(std::chrono::milliseconds(ops_timeout_msec_), std::chrono::milliseconds(connect_timeout_msec_));
  if (future->wait_for(max_timeout + 10s) == std::future_status::timeout) {
    logger->Log(Severity::FATAL, "Stop failed, some threads are stuck.\n");
    SetState(ConsumerState::ERROR, RepStatus(Code::REPLICATOR_FAILURE, Severity::FATAL, 
                                            FormatString("Stop failed, some threads are stuck.")));
    // If the app will try destroy the Consumer object, it will crash
    return RepStatus(Code::REPLICATOR_FAILURE, Severity::FATAL, "Stop failed, some threads are stuck.\n");
  } else {
    delete future;
  }

  // Move state into STOPPED
  SetState(ConsumerState::STOPPED, RepStatus());
  logger->Log(Severity::INFO, "Consumer finished its jobs\n");
  return RepStatus();
}

RepStatus Consumer::GetState(ConsumerState& state, RepStatus& status)
{
  std::lock_guard<std::mutex> lock(state_mutex_);
  state = state_;
  status = status_;
  return RepStatus();
}

RepStatus Consumer::GetStats(uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  num_kv_pairs = statistics_.num_kv_pairs;
  num_bytes = statistics_.num_bytes;
  return RepStatus();
}

void Consumer::SetState(const ConsumerState& state, const RepStatus& status)
{
  std::lock_guard<std::mutex> lock(state_mutex_);
  // Never overwrite a state if it is already in a final state
  if (IsFinalState(state_)) {
    return;
  }
  state_ = state;
  status_ = status;
  // Call the callback for final states only
  if (IsFinalState(state_)) {
    done_callback_(state_, status_);
  }
}

}
