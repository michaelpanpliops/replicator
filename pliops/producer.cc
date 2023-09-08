#include <algorithm>

#include "pliops_calc_key_ranges.cc"

#include "producer.h"

namespace Replicator {

Producer::Producer()
    : kill_(false)
{}

Producer::~Producer() {
  assert(!shard_);
}

void Producer::OpenShard(const std::string& shard_path) {
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
  auto status = ROCKSDB_NAMESPACE::DB::Open(options,
                                            shard_path,
                                            &db);
  if (!status.ok()) {
    throw std::runtime_error(FormatString("Failed to open shard, reason: %s", status.ToString()));
  }

  shard_ = db;
}

void Producer::ReaderThread(uint32_t thread_id, std::function<void()> done_callback) {
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
    throw std::runtime_error("cf_handle_->GetDescriptor(&cf_desc) failed");
  }
  iterator = db->NewIterator(ReadOptions(), db->DefaultColumnFamily());
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
    // TODO: check kill_ in the loop
    while (!enqueued) {
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
  // TODO: check rc
  iterator->Close();
  delete iterator;

  // The following code must be under lock to ensure atomicity
  std::lock_guard<std::mutex> lock(active_reader_threads_mutex_);
  active_reader_threads_count_--;
  if (active_reader_threads_count_ == 0) {
    // The last active thread closes the DB and calls the done-callback
    message_queue_->enqueue({"", ""}); // Signal finish
    // TODO: check rc
    shard_->Close();
    delete shard_;
    shard_ = nullptr;
    log_message(FormatString("Stat.num_kv_pairs: %lld, Stat.num_bytes: %lld \n", statistics_.num_kv_pairs.load(), statistics_.num_bytes.load()));
    done_callback();
  }
}

void Producer::CommunicationThread() {
  log_message(FormatString("Communication thread started.\n"));
  auto& connection = connection_;
  std::string key, value;
  while(!kill_){
    try {
      std::pair<std::string, std::string> message;
      message_queue_->wait_dequeue(message);
      if (message.first.empty()) {
        // Finish thread
        return;
      }
      connection->Send(message.first.c_str(), message.first.size(), message.second.c_str(), message.second.size());
    } catch (const std::exception& e) {
      log_message(FormatString("Exception in the communication thread: %s\n", e.what()));
      kill_ = true;
      return;
    }
  }
}

std::vector<RangeType> Producer::CalculateThreadKeyRanges(uint32_t max_num_of_threads) {
  // Calculate ranges
  auto db = shard_;
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle = db->DefaultColumnFamily();
  std::vector<RangeType> result;
  std::vector<std::string> range_split_keys;

  auto status = CalcKeyRanges(db, cf_handle, max_num_of_threads, range_split_keys);
  if (!status.ok()) {
    throw std::runtime_error(FormatString("Error in CalcKeyRanges: %s", status.ToString()));
  }

  // Create vector of ranges
  RangeType range;
  range.first = std::optional<std::string>();
  range.second = range_split_keys.size() ? range_split_keys.front() : std::optional<std::string>();
  result.push_back(range);
  for (size_t i = 0; i < range_split_keys.size(); i++) {
    range.first = range_split_keys[i];
    range.second = (i == range_split_keys.size() - 1) ? std::optional<std::string>() : range_split_keys[i+1];
    result.push_back(range);
  }
  return result;
}

void Producer::Start(const std::string& ip, uint16_t port, uint32_t max_num_of_threads, std::function<void()>& done_callback) {
  thread_key_ranges_ = CalculateThreadKeyRanges(max_num_of_threads);
  log_message(FormatString("Shard is split into %d read ranges\n", thread_key_ranges_.size()));

  // Get connections for all shards
  log_message("Connecting to server...\n");
  message_queue_ = std::make_unique<MessageQueue>(MESSAGE_QUEUE_CAPACITY);
  connection_ = connect<ConnectionType::TCP_SOCKET>(ip, port);
  log_message(FormatString("Connected\n"));

  // Start the communication thread
  communication_thread_ = std::make_unique<std::thread>([this]() {
    this->CommunicationThread();
  });

  log_message("Starting reader threads\n");

  assert(max_num_of_threads >= thread_key_ranges_.size());
  auto threads_per_shard = thread_key_ranges_.size();

  active_reader_threads_count_ = threads_per_shard;
  for (uint32_t thread_id = 0; thread_id < threads_per_shard; ++thread_id) {
    reader_threads_.push_back(std::thread([this, thread_id, done_callback]() {
                this->ReaderThread(thread_id, done_callback);
    }));
  }
}

void Producer::Stop() {
  kill_ = true;

  for (auto& reader_thread : reader_threads_) {
    reader_thread.join();
  }

  message_queue_->enqueue({"", ""}); // Signal finish.
  communication_thread_->join();

  log_message("Producer finished sending replication.\n");
}

void Producer::Stats(uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  num_kv_pairs = statistics_.num_kv_pairs;
  num_bytes = statistics_.num_bytes;
}

}
