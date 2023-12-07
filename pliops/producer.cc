#include <algorithm>
#include <future>

#include "producer.h"


namespace Replicator {

Producer::Producer(IKvPairSerializer& kv_pair_serializer)
  : kill_(false), kv_pair_serializer_(kv_pair_serializer)
{}

Producer::~Producer()
{
  if (shard_) {
    shard_->Close();
    delete shard_;
  }
}

RepStatus Producer::OpenShard(const std::string& shard_path)
{
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.disable_auto_compactions = true;
#ifdef XDPROCKS
  options.OptimizeForXdpRocks();
  options.pliops_db_options.graceful_close_timeout_sec = 0;
#endif
  auto status = ROCKSDB_NAMESPACE::DB::Open(options, shard_path, &db);
  if (!status.ok()) {
    logger->Log(Severity::ERROR, FormatString("Failed to open shard, reason: %s", status.ToString()));
    return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Failed to open shard, reason: %s", status.ToString()));
  }

  shard_ = db;
  return RepStatus();
}

void Producer::ReaderThread(uint32_t iterator_parallelism_factor, uint32_t thread_id) {
  uint64_t total_number_of_operations = 0;
  logger->Log(Severity::INFO, FormatString("Reader thread #%d started\n", thread_id));

  ROCKSDB_NAMESPACE::ColumnFamilyDescriptor cf_desc;
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Iterator* iterator;

  db = shard_;
  auto status = db->DefaultColumnFamily()->GetDescriptor(&cf_desc);
  if (!status.ok()) {
    logger->Log(Severity::ERROR, FormatString("Reader thread: cf_handle_->GetDescriptor failed, reason: %s\n", status.ToString()));
    SetState(ProducerState::ERROR, RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Reader thread: cf_handle_->GetDescriptor failed, reason: %s", status.ToString())));
    return;
  }

  // Create iterator with internal parallelism
  ROCKSDB_NAMESPACE::ReadOptions read_opts;
#ifdef XDPROCKS
  read_opts.iterator_internal_parallelism_enabled = true; // default is false
  read_opts.iterator_internal_parallelism_factor = iterator_parallelism_factor; // number of threads
  read_opts.advanced_iterator_kvfs_parallelism_factor = 4;
  read_opts.readahead_size = (4 * 4096 * read_opts.advanced_iterator_kvfs_parallelism_factor); // determines the queue size (By dividing it with 4K).
#endif
  iterator = db->NewIterator(read_opts, db->DefaultColumnFamily());
  if (!iterator) {
    logger->Log(Severity::ERROR, "Reader thread: db->NewIterator returned nullptr\n");
    SetState(ProducerState::ERROR, RepStatus(Code::DB_FAILURE, Severity::ERROR, "Reader thread: db->NewIterator returned nullptr"));
    return;
  }

  // Seek to the database beginning
  iterator->SeekToFirst();

  while(!kill_) {
    // Send the next KV pair.
    ROCKSDB_NAMESPACE::Slice key, value;
    if(!iterator->Valid()){
      break; // Finished reading range for this thread.
    }

    key = iterator->key();
    value = iterator->value();

    if (kill_ || !message_queue_->wait_enqueue_timed({std::string(key.data(), key.size()), std::string(value.data(), value.size())}, msec_to_usec(ops_timeout_msec_))) {
      logger->Log(Severity::ERROR, FormatString("Reader thread: enqueue failed, reason: timeout\n"));
      // We must release the iterator here, otherwise DB::Close will crash
#ifdef XDPROCKS
      status = iterator->Close();
      if (!status.ok()) {
        logger->Log(Severity::ERROR, FormatString("Reader thread: iterator->Close failed, reason: %s\n", status.ToString()));
      }
#endif
      delete iterator;

      SetState(ProducerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Reader thread: enqueue failed, reason: timeout")));
      return;
    }

    total_number_of_operations++;
    statistics_.num_kv_pairs++; // atomic
    statistics_.num_bytes.fetch_add(key.size() + value.size()); // atomic

    iterator->Next();
  }
  logger->Log(Severity::INFO, FormatString("Reader thread #%d ended. Performed %lld operations.\n", thread_id, total_number_of_operations));

  // Release the iterator
#ifdef XDPROCKS
  status = iterator->Close();
#endif
  delete iterator;
  if (!status.ok()) {
    logger->Log(Severity::ERROR, FormatString("Reader thread: iterator->Close failed, reason: %s\n", status.ToString()));
    SetState(ProducerState::ERROR, RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Reader thread: iterator->Close failed, reason: %s", status.ToString())));
  }

  // The last active thread signals end of communications
  if (kill_ || !message_queue_->wait_enqueue_timed({"", ""}, msec_to_usec(ops_timeout_msec_))) {
    logger->Log(Severity::ERROR, FormatString("Reader thread: enqueue failed, reason: timeout\n"));
    SetState(ProducerState::ERROR, RepStatus(Code::TIMEOUT_FAILURE, Severity::ERROR, FormatString("Reader thread: enqueue failed, reason: timeout")));
  }
}

void Producer::CommunicationThread() {
  logger->Log(Severity::INFO, FormatString("Communication thread started.\n"));
  std::string key, value;
  while(!kill_) {
    std::pair<std::string, std::string> message;
    if (!message_queue_->wait_dequeue_timed(message, msec_to_usec(ops_timeout_msec_))) {
      logger->Log(Severity::ERROR, FormatString("Communication thread: Failed to dequeue message, reason: timeout\n"));
      SetState(ProducerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Communication thread: Failed to dequeue message, reason: timeout")));
      return;
    }
    auto rc = connection_->Send(message.first.c_str(), message.first.size(), message.second.c_str(), message.second.size(), kv_pair_serializer_);
    if (!rc.ok()) {
      logger->Log(Severity::ERROR, FormatString("Communication thread: connection_->Send failed\n"));
      SetState(ProducerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Communication thread: connection_->Send failed")));
      return;
    }
    // Poison pill, all readers finished their work
    // Close the DB and call the done-callback
    if (message.first.empty()) {
      auto status = shard_->Close();
      delete shard_;
      shard_ = nullptr;
      if (!status.ok()) {
        logger->Log(Severity::ERROR, FormatString("Communication thread: shard_->Close failed, reason: %s\n", status.ToString()));
        SetState(ProducerState::ERROR, RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Communication thread: shard_->Close failed, reason: %s", status.ToString())));
        return;
      }

      // Print out replication performance metrics
      logger->Log(Severity::INFO, FormatString("Stat.num_kv_pairs: %lld, Stat.num_bytes: %lld \n",
                  statistics_.num_kv_pairs.load(), statistics_.num_bytes.load()));
      auto current_time = std::chrono::system_clock::now();
      auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time_);
      logger->Log(Severity::INFO, FormatString("%.1f pairs/sec\n", statistics_.num_kv_pairs.load()/(double)elapsed_seconds.count()));
      logger->Log(Severity::INFO, FormatString("%.1f bytes/sec\n", statistics_.num_bytes.load()/(double)elapsed_seconds.count()));

      SetState(ProducerState::DONE, RepStatus());
      return;
    }
  }
  logger->Log(Severity::INFO, FormatString("Communication thread ended.\n"));
}

RepStatus Producer::Start(const std::string& ip, uint16_t port,
                    uint32_t parallelism, uint32_t ops_timeout_msec, uint32_t connect_timeout_msec,
                    std::function<void(ProducerState, const RepStatus&)>& done_callback)
{
  done_callback_ = done_callback;
  ops_timeout_msec_ = ops_timeout_msec;
  connect_timeout_msec_ = connect_timeout_msec;

  // Move state into IN_PROGRESS
  assert(state_ == ProducerState::IDLE);
  SetState(ProducerState::IN_PROGRESS, RepStatus());

  // Connect to consumer
  logger->Log(Severity::INFO, "Connecting to consumer...\n");
  message_queue_ = std::make_unique<MessageQueue>(MESSAGE_QUEUE_CAPACITY);
  auto rc = Connect<ConnectionType::TCP_SOCKET>(ip, port, connection_, connect_timeout_msec_);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, "Socket connect failed\n");
    return rc;
  }
  logger->Log(Severity::INFO, FormatString("Connected\n"));

  start_time_ = std::chrono::system_clock::now();

  // Start the communication thread
  logger->Log(Severity::INFO, "Starting communication thread\n");
  communication_thread_ = std::make_unique<std::thread>([this]() {
    this->CommunicationThread();
  });

  // Start the reader threads
  logger->Log(Severity::INFO, "Starting reader thread\n");

  uint32_t thread_id = 0;
  reader_thread_ = std::thread([this, parallelism, thread_id]() {
              this->ReaderThread(parallelism, thread_id);
    });

  return RepStatus();
}

void Producer::StopImpl()
{
  // This is a best effort stopping, we will try to stop the threads
  // and close the shard, but won't report any error if something goes wrong

  // Tell threads to exit
  kill_ = true;
  reader_thread_.join();

  // We need to signal communication thread with poison pill
  // because it could be waiting on queue
  std::atomic<bool> dummy(false);
  if (message_queue_) message_queue_->wait_enqueue_timed({"", ""}, msec_to_usec(ops_timeout_msec_));
  if (communication_thread_) communication_thread_->join();
  connection_.reset();
  message_queue_.reset();

  // Close shard
  if (shard_) {
    shard_->Close();
    delete shard_;
    shard_ = nullptr;
  }
}

RepStatus Producer::Stop()
{
  using namespace std::literals;

  std::future<void>* future = new std::future<void>;
  *future = std::async(std::launch::async, &Producer::StopImpl, this);
  auto max_timeout = std::max(std::chrono::milliseconds(ops_timeout_msec_), std::chrono::milliseconds(connect_timeout_msec_));
  if (future->wait_for(max_timeout + 10s) == std::future_status::timeout) {
    logger->Log(Severity::FATAL, "Stop failed, some threads are stuck.\n");
    SetState(ProducerState::ERROR, RepStatus(Code::REPLICATOR_FAILURE, Severity::FATAL, 
                                            FormatString("Stop failed, some threads are stuck.")));
    // If the app will try destroy the Producer object, it will crash
    return RepStatus(Code::REPLICATOR_FAILURE, Severity::FATAL, "Stop failed, some threads are stuck.");
  } else {
    delete future;
  }

  // Move state into STOPPED
  SetState(ProducerState::STOPPED, RepStatus());
  logger->Log(Severity::INFO, "Producer finished its jobs.\n");
  return RepStatus();
}

RepStatus Producer::GetState(ProducerState& state, RepStatus& status)
{
  std::lock_guard<std::mutex> lock(state_mutex_);
  state = state_;
  status = status_;
  return RepStatus();
}

RepStatus Producer::GetStats(uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  num_kv_pairs = statistics_.num_kv_pairs;
  num_bytes = statistics_.num_bytes;
  return RepStatus();
}

void Producer::SetState(const ProducerState& state, const RepStatus& status)
{
  std::lock_guard<std::mutex> lock(state_mutex_);
  // Never overwrite a state if it is already in a final state
  if (IsFinalState(state_)) {
    return;
  }
  state_ = state;
  status_ = status;
  // Call the callback for final states only
  if (IsFinalState(state_) && done_callback_) {
    done_callback_(state_, status_);
  }
}

}
