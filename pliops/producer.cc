#include <algorithm>
#include <future>

#include "pliops_calc_key_ranges.cc"
#include "producer.h"


namespace Replicator {

Producer::Producer()
  : kill_(false)
{}

Producer::~Producer()
{
  if (shard_) {
    shard_->Close();
    delete shard_;
  }
}

int Producer::OpenShard(const std::string& shard_path)
{
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.disable_auto_compactions = true;
#ifndef LEGACY_ROCKSDB_SENDER
  options.OptimizeForXdpRocks();
  options.pliops_db_options.graceful_close_timeout_sec = 0;
#else
  // Tune legacy RocksDB options here
#endif
  auto status = ROCKSDB_NAMESPACE::DB::Open(options, shard_path, &db);
  if (!status.ok()) {
    log_message(FormatString("Failed to open shard, reason: %s", status.ToString()));
    return -1;
  }

  shard_ = db;
  return 0;
}

void Producer::ReaderThread(uint32_t iterator_parallelism_factor, uint32_t thread_id) {
  uint64_t total_number_of_operations = 0;
  log_message(FormatString("Reader thread #%d started\n", thread_id));

  ROCKSDB_NAMESPACE::ColumnFamilyDescriptor cf_desc;
  ROCKSDB_NAMESPACE::DB* db;
  ROCKSDB_NAMESPACE::Iterator* iterator;
  RangeType range;

  db = shard_;
  range = thread_key_ranges_[thread_id];
  Status status = db->DefaultColumnFamily()->GetDescriptor(&cf_desc);
  if (!status.ok()) {
    log_message(FormatString("cf_handle_->GetDescriptor failed, reason: %s\n", status.ToString()));
    SetState(ProducerState::ERROR, "");
    return;
  }

  // Create iterator with internal parallelism
  ReadOptions read_opts;
  read_opts.iterator_internal_parallelism_enabled = true; // default is false
  read_opts.iterator_internal_parallelism_factor = iterator_parallelism_factor;
  iterator = db->NewIterator(read_opts, db->DefaultColumnFamily());
  if (!iterator) {
    log_message("db->NewIterator returned nullptr\n");
    SetState(ProducerState::ERROR, "");
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

    iterator->Next();

    bool enqueued = message_queue_->try_enqueue({std::string(key.data(), key.size()), std::string(value.data(), value.size())});
    while (!enqueued && !kill_) {
      // Server side is not fast enough, message queue is full. re-attempt enqueueing to shard's message queue in a short bit.
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      enqueued = message_queue_->try_enqueue({std::string(key.data(), key.size()), std::string(value.data(), value.size())});
    }

    total_number_of_operations++;
    statistics_.num_kv_pairs++; // atomic
    statistics_.num_bytes.fetch_add(key.size() + value.size()); // atomic
  }
  log_message(FormatString("Reader thread #%d ended. Performed %lld operations.\n", thread_id, total_number_of_operations));

  // Release the iterator
  status = iterator->Close();
  delete iterator;
  if (!status.ok()) {
    log_message(FormatString("iterator->Close failed, reason: %s\n", status.ToString()));
    SetState(ProducerState::ERROR, "");
  }

  // The following code must be under lock to ensure atomicity
  std::lock_guard<std::mutex> lock(active_reader_threads_mutex_);
  active_reader_threads_count_--;
  if (active_reader_threads_count_ == 0) {
    // The last active thread signals end of communications
    message_queue_->enqueue({"", ""}); 
  }
}

void Producer::CommunicationThread() {
  log_message(FormatString("Communication thread started.\n"));
  std::string key, value;
  while(!kill_) {
    std::pair<std::string, std::string> message;
    message_queue_->wait_dequeue(message);
    auto rc = connection_->Send(message.first.c_str(), message.first.size(), message.second.c_str(), message.second.size());
    if (rc) {
      log_message(FormatString("connection_->Send failed\n"));
      SetState(ProducerState::ERROR, "");
      return;
    }
    // Poison pill, all readers finished their work
    // Close the DB and call the done-callback
    if (message.first.empty()) {
      auto status = shard_->Close();
      delete shard_;
      shard_ = nullptr;
      if (!status.ok()) {
        log_message(FormatString("shard_->Close failed, reason: %s\n", status.ToString()));
        SetState(ProducerState::ERROR, "");
        return;
      }

      // Print out replication performance metrics
      log_message(FormatString("Stat.num_kv_pairs: %lld, Stat.num_bytes: %lld \n",
                  statistics_.num_kv_pairs.load(), statistics_.num_bytes.load()));
      auto current_time = std::chrono::system_clock::now();
      auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time_);
      log_message(FormatString("%.1f pairs/sec\n", statistics_.num_kv_pairs.load()/(double)elapsed_seconds.count()));
      log_message(FormatString("%.1f bytes/sec\n", statistics_.num_bytes.load()/(double)elapsed_seconds.count()));

      SetState(ProducerState::DONE, "");
      return;
    }
  }
  log_message(FormatString("Communication thread ended.\n"));
}

int Producer::CalculateThreadKeyRanges(uint32_t max_num_of_threads, std::vector<RangeType>& ranges) {
  // Calculate ranges
  auto db = shard_;
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle = db->DefaultColumnFamily();
  ranges.clear();
  std::vector<std::string> range_split_keys;

  auto status = CalcKeyRanges(db, cf_handle, max_num_of_threads, range_split_keys);
  if (!status.ok()) {
    log_message(FormatString("Error in CalcKeyRanges: %s", status.ToString()));
    return -1;
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
  return 0;
}

int Producer::Start(const std::string& ip, uint16_t port,
                    uint32_t max_num_of_threads, uint32_t parallelism,
                    std::function<void(ProducerState, const std::string&)>& done_callback)
{
  done_callback_ = done_callback;

  // Move state into IN_PROGRESS
  assert(state_ == ProducerState::IDLE);
  SetState(ProducerState::IN_PROGRESS, "");

  // Split into ranges
  auto rc = CalculateThreadKeyRanges(max_num_of_threads, thread_key_ranges_);
  if (rc) {
    log_message("CalculateThreadKeyRanges failed\n");
    return -1;
  }
  log_message(FormatString("Shard is split into %d read ranges\n", thread_key_ranges_.size()));

  // Connect to consumer
  log_message("Connecting to consumer...\n");
  message_queue_ = std::make_unique<MessageQueue>(MESSAGE_QUEUE_CAPACITY);
  rc = connect<ConnectionType::TCP_SOCKET>(ip, port, connection_);
  if (rc) {
    log_message("Socket connect failed\n");
    return -1;
  }
  log_message(FormatString("Connected\n"));

  start_time_ = std::chrono::system_clock::now();

  // Start the communication thread
  log_message("Starting communication thread\n");
  communication_thread_ = std::make_unique<std::thread>([this]() {
    this->CommunicationThread();
  });

  // Start the reader threads
  log_message("Starting reader threads\n");
  assert(max_num_of_threads >= thread_key_ranges_.size());
  auto threads_per_shard = thread_key_ranges_.size();

  active_reader_threads_count_ = threads_per_shard;
  for (uint32_t thread_id = 0; thread_id < threads_per_shard; ++thread_id) {
    reader_threads_.push_back(std::thread([this, parallelism, thread_id]() {
                this->ReaderThread(parallelism, thread_id);
    }));
  }

  return 0;
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
  message_queue_->enqueue({"", ""});
  communication_thread_->join();
  connection_.reset();
  message_queue_.reset();

  // Close shard
  if (shard_) {
    shard_->Close();
    delete shard_;
    shard_ = nullptr;
  }
}

int Producer::Stop()
{
  using namespace std::literals;

  // Move state into STOPPED, so we won't get any ERROR/DONE notifications from now
  SetState(ProducerState::STOPPED, "");

  std::future<void>* future = new std::future<void>;
  *future = std::async(std::launch::async, &Producer::StopImpl, this);
  if (future->wait_for(10s) == std::future_status::timeout) {
    log_message("Stop failed, some threads are stuck.\n");
    // If the app will try destroy the Producer object, it will crash
    return -1;
  } else {
    delete future;
  }

  log_message("Producer finished its jobs.\n");
  return 0;
}

int Producer::GetState(ProducerState& state, std::string& error)
{
  std::lock_guard<std::mutex> lock(state_mutex_);
  state = state_;
  error = error_;
  return 0;
}

int Producer::GetStats(uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  num_kv_pairs = statistics_.num_kv_pairs;
  num_bytes = statistics_.num_bytes;
  return 0;
}

void Producer::SetState(const ProducerState& state, const std::string& error)
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
