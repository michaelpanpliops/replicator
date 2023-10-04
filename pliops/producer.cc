#include <algorithm>
#include <future>

#include "pliops_calc_key_ranges.cc"
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
  RangeType range;

  db = shard_;
  range = thread_key_ranges_[thread_id];
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
  read_opts.iterator_internal_parallelism_factor = iterator_parallelism_factor;
#endif
  iterator = db->NewIterator(read_opts, db->DefaultColumnFamily());
  if (!iterator) {
    logger->Log(Severity::ERROR, "Reader thread: db->NewIterator returned nullptr\n");
    SetState(ProducerState::ERROR, RepStatus(Code::DB_FAILURE, Severity::ERROR, "Reader thread: db->NewIterator returned nullptr"));
    return;
  }

  if (range.first) {
    // Seek to the first element in the range
    iterator->Seek(*range.first);
  } else {
    // Seek to the database beginning
    iterator->SeekToFirst();
  }

  while(!kill_) {
    // Send the next KV pair.
    ROCKSDB_NAMESPACE::Slice key, value;
    if(!iterator->Valid()){
      break; // Finished reading range for this thread.
    }
    // Stop iterate if we have reached the end of the range
    // For the last range, the interator will stop on Valid() check above
    if (range.second && cf_desc.options.comparator->Compare(iterator->key(), *range.second) >= 0) {
      break;
    }

    key = iterator->key();
    value = iterator->value();

    if (!EnqueueTimed(message_queue_, {std::string(key.data(), key.size()), std::string(value.data(), value.size())}, kill_, ops_timeout_msec_)) {
      // We must release the iterator here, otherwise DB::Close will crash
#ifdef XDPROCKS
      iterator->Close();
#endif
      delete iterator;

      logger->Log(Severity::ERROR, FormatString("Reader thread: enqueue failed, reason: timeout\n"));
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

  // The following code must be under lock to ensure atomicity
  std::lock_guard<std::mutex> lock(active_reader_threads_mutex_);
  active_reader_threads_count_--;
  // The last active thread signals end of communications
  if (active_reader_threads_count_ == 0 && !EnqueueTimed(message_queue_, {"",""}, kill_, ops_timeout_msec_)) {
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
    if (!rc.IsOk()) {
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
        SetState(ProducerState::ERROR, RepStatus(Code::NETWORK_FAILURE, Severity::ERROR, FormatString("Communication thread: shard_->Close failed, reason: %s", status.ToString())));
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

RepStatus Producer::CalculateThreadKeyRanges(uint32_t max_num_of_threads, std::vector<RangeType>& ranges) {
  // Calculate ranges
  auto db = shard_;
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle = db->DefaultColumnFamily();
  ranges.clear();
  std::vector<std::string> range_split_keys;

  auto status = CalcKeyRanges(db, cf_handle, max_num_of_threads, range_split_keys);
  if (!status.ok()) {
    logger->Log(Severity::ERROR, FormatString("Error in CalcKeyRanges: %s", status.ToString()));
    return RepStatus(Code::DB_FAILURE, Severity::ERROR, FormatString("Error in CalcKeyRanges: %s", status.ToString()));
  }

  // Create vector of ranges
  RangeType range;
  range.first = std::optional<std::string>();
  range.second = range_split_keys.size() ? range_split_keys.front() : std::optional<std::string>();
  ranges.push_back(range);
  for (size_t i = 0; i < range_split_keys.size(); i++) {
    range.first = range_split_keys[i];
    range.second = (i == range_split_keys.size() - 1) ? std::optional<std::string>() : range_split_keys[i+1];
    ranges.push_back(range);
  }
  return RepStatus();
}

RepStatus Producer::Start(const std::string& ip, uint16_t port,
                    uint32_t max_num_of_threads, uint32_t parallelism,
                    uint32_t ops_timeout_msec, uint32_t connect_timeout_msec,
                    std::function<void(ProducerState, const RepStatus&)>& done_callback)
{
  done_callback_ = done_callback;
  ops_timeout_msec_ = ops_timeout_msec;
  connect_timeout_msec_ = connect_timeout_msec;

  // Move state into IN_PROGRESS
  assert(state_ == ProducerState::IDLE);
  SetState(ProducerState::IN_PROGRESS, RepStatus());

  // Split into ranges
  RepStatus rc = CalculateThreadKeyRanges(max_num_of_threads, thread_key_ranges_);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, "CalculateThreadKeyRanges failed\n");
    return rc;
  }
  logger->Log(Severity::INFO, FormatString("Shard is split into %d read ranges\n", thread_key_ranges_.size()));

  // Connect to consumer
  logger->Log(Severity::INFO, "Connecting to consumer...\n");
  message_queue_ = std::make_unique<MessageQueue>(MESSAGE_QUEUE_CAPACITY);
  rc = Connect<ConnectionType::TCP_SOCKET>(ip, port, connection_, connect_timeout_msec_);
  if (!rc.IsOk()) {
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
  logger->Log(Severity::INFO, "Starting reader threads\n");
  assert(max_num_of_threads >= thread_key_ranges_.size());
  auto threads_per_shard = thread_key_ranges_.size();

  active_reader_threads_count_ = threads_per_shard;
  for (uint32_t thread_id = 0; thread_id < threads_per_shard; ++thread_id) {
    reader_threads_.push_back(std::thread([this, parallelism, thread_id]() {
                this->ReaderThread(parallelism, thread_id);
    }));
  }

  return RepStatus();
}

void Producer::StopImpl()
{
  // This is a best effort stopping, we will try to stop the threads
  // and close the shard, but won't report any error if something goes wrong

  // Tell threads to exit
  kill_ = true;
  for (auto& reader_thread : reader_threads_) {
    reader_thread.join();
  }

  // We need to signal communication thread with poison pill
  // because it could be waiting on queue
  std::atomic<bool> dummy(false);
  if (message_queue_ && !EnqueueTimed(message_queue_, {"",""}, dummy, ops_timeout_msec_) ) {
      logger->Log(Severity::ERROR, FormatString("enqueue failed, reason: timeout\n"));
      SetState(ProducerState::ERROR, RepStatus(Code::TIMEOUT_FAILURE, Severity::ERROR, FormatString("Reader thread: enqueue failed, reason: timeout\n")));
  }
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

  // Move state into STOPPED, so we won't get any ERROR/DONE notifications from now
  SetState(ProducerState::STOPPED, RepStatus());

  std::future<void>* future = new std::future<void>;
  *future = std::async(std::launch::async, &Producer::StopImpl, this);
  auto max_timeout = std::max(std::chrono::milliseconds(ops_timeout_msec_), std::chrono::milliseconds(connect_timeout_msec_));
  if (future->wait_for(max_timeout + 10s) == std::future_status::timeout) {
    logger->Log(Severity::FATAL, "Stop failed, some threads are stuck.\n");
    // If the app will try destroy the Producer object, it will crash
    return RepStatus(Code::REPLICATOR_FAILURE, Severity::FATAL, "Stop failed, some threads are stuck.");
  } else {
    delete future;
  }

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

bool Producer::EnqueueTimed(std::unique_ptr<Replicator::MessageQueue>& message_queue,
                   std::pair<std::string, std::string>&& message,
                   std::atomic<bool>& kill,
                   uint64_t timeout_msec) {
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
  bool enqueued = false;
  while (!enqueued && !kill) {
    enqueued = message_queue->try_enqueue(message);

    if (!enqueued) {
      auto current_time = std::chrono::steady_clock::now();
      auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);

      if (elapsed_time.count() >= timeout_msec) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }  
  }
  return enqueued;
}

}
