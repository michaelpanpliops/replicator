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

void Producer::ReaderThread(uint32_t shard_id, uint32_t thread_id, std::function<void()> done_callback) {
    uint64_t total_number_of_operations = 0;
    log_message(FormatString("Reader thread #%d for shard %d started\n", thread_id, shard_id));

    ROCKSDB_NAMESPACE::ColumnFamilyDescriptor cf_desc;
    ROCKSDB_NAMESPACE::DB* db;
    ROCKSDB_NAMESPACE::Iterator* iterator;
    RangeType range;

    db = shard_;
    range = thread_key_ranges_[shard_id][thread_id];
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

        bool enqueued = message_queues_[shard_id]->try_enqueue({std::string(key.data(), key.size()), std::string(value.data(), value.size())});
        // TODO: check kill_ in the loop
        while (!enqueued) {
            // Server side is not fast enough, message queue is full. re-attempt enqueueing to shard's message queue in a short bit.
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            enqueued = message_queues_[shard_id]->try_enqueue({std::string(key.data(), key.size()), std::string(value.data(), value.size())});
        }

        total_number_of_operations++;
        statistics_.num_kv_pairs++; // atomic
        statistics_.num_bytes.fetch_add(key.size() + value.size()); // atomic
    }
    log_message(FormatString("Reader thread #%d shard %d ended. Performed %lld operations.\n", thread_id, shard_id, total_number_of_operations));

  // Release the iterator
  // TODO: check rc
  iterator->Close();
  delete iterator;

  // The following code must be under lock to ensure atomicity
  std::lock_guard<std::mutex> lock(active_reader_threads_mutex_);
  active_reader_threads_count_--;
  if (active_reader_threads_count_ == 0) {
    // The last active thread closes the DB and calls the done-callback
    message_queues_[shard_id]->enqueue({"", ""}); // Signal finish
    // TODO: check rc
    shard_->Close();
    delete shard_;
    shard_ = nullptr;
    done_callback();
  }
}

void Producer::CommunicationThread(uint32_t shard_id) {
    log_message(FormatString("Communication thread for shard #%d: Started.\n", shard_id));
    auto& connection = connection_;
    std::string key, value;
    while(!kill_){
        try {
            std::pair<std::string, std::string> message;
            message_queues_[shard_id]->wait_dequeue(message);
            if (message.first.empty()) {
                // Finish thread
                return;
            }
            connection->Send(message.first.c_str(), message.first.size(), message.second.c_str(), message.second.size());
        } catch (const std::exception& e) {
            log_message(FormatString("Communication thread for shard #%d: %s\n", shard_id, e.what()));
            kill_ = true;
            return;
        }
    }
}

std::vector<RangeType> Producer::CalculateThreadKeyRanges(uint32_t shard_id, uint32_t num_of_threads) {
  // Calculate ranges
  auto db = shard_;
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle = db->DefaultColumnFamily();
  std::vector<RangeType> result;
  std::vector<std::string> range_split_keys;

    auto status = CalcKeyRanges(db, cf_handle, num_of_threads, range_split_keys);
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
    const uint32_t shard_id = 0;
    thread_key_ranges_.push_back(CalculateThreadKeyRanges(shard_id, max_num_of_threads));
    log_message(FormatString("Shard #%d is split into %d read ranges\n", shard_id, thread_key_ranges_.back().size()));

    // Get connections for all shards
    log_message("Connecting to server...\n");
    message_queues_.push_back(std::make_unique<MessageQueue>(MESSAGE_QUEUE_CAPACITY));
    connection_ = connect<ConnectionType::TCP_SOCKET>(ip, port);
    log_message(FormatString("Connected shard #%d\n", shard_id));

    // Start the single communication thread per shard (done separately to ensure the integrity of Producer's internal connections queue)
    communication_thread_ = std::make_unique<std::thread>([this, shard_id]() {
        this->CommunicationThread(shard_id);
    });

    log_message("Starting reader threads\n");
    auto threads_per_current_shard = max_num_of_threads;

    assert(threads_per_current_shard >= thread_key_ranges_[shard_id].size());
    threads_per_current_shard = std::min<size_t>(threads_per_current_shard, thread_key_ranges_[shard_id].size());

    active_reader_threads_count_ = threads_per_current_shard;
    for (uint32_t thread_id = 0; thread_id < threads_per_current_shard; ++thread_id) {
        reader_threads_.push_back(std::thread([this, shard_id, thread_id, done_callback]() {
            this->ReaderThread(shard_id, thread_id, done_callback);
        }));
    }
}

void Producer::Stop() {
    kill_ = true;
    const uint32_t shard_id = 0;

  for (auto& reader_thread : reader_threads_) {
    reader_thread.join();
  }

    message_queues_[shard_id]->enqueue({"", ""}); // Signal finish.
    communication_thread_->join();

    log_message("Producer finished sending replication.\n");
}

void Producer::Stats(uint64_t& num_kv_pairs, uint64_t& num_bytes)
{
  num_kv_pairs = statistics_.num_kv_pairs;
  num_bytes = statistics_.num_bytes;
}

}