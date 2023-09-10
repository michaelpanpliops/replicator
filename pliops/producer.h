#pragma once

#include <thread>
#include <mutex>
#include <string_view>
#include <filesystem>
#include <map>

#include "rocksdb/db.h"

#include "communication.h"

#include "utils/blocking_concurrent_queue.h"

namespace Replicator {

using RangeType = std::pair<std::optional<std::string>, std::optional<std::string>>;
using MessageQueue = moodycamel::BlockingConcurrentQueue<std::pair<std::string, std::string>>;
constexpr size_t MESSAGE_QUEUE_CAPACITY = 32 * 10000;

struct Statistics {
  std::atomic<uint64_t> num_kv_pairs = 0;
  std::atomic<uint64_t> num_bytes = 0;
};

class Producer {
public:
  explicit Producer();
  virtual ~Producer();
  void OpenShard(const std::string& shard_path);
  void Start(const std::string& ip, uint16_t port, uint32_t max_num_of_threads,
              uint32_t parallelism, std::function<void()>& done_callback);
  void Stop();
  void Stats(uint64_t& num_kv_pairs, uint64_t& num_bytes);

private:
  // Shard connection and its connection thread
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connection_;
  std::unique_ptr<std::thread> communication_thread_;

  // Reader threads
  std::vector<std::thread> reader_threads_;

  // Statistics
  Statistics statistics_;

  // Stores KV pairs to send over the network. The reader thread will push messages to the queue.
  std::unique_ptr<MessageQueue> message_queue_;
  ROCKSDB_NAMESPACE::DB* shard_ = nullptr;

  std::vector<RangeType> thread_key_ranges_;
  bool kill_; // TODO: atomic
  void ReaderThread(uint32_t iterator_parallelism_factor, uint32_t thread_id,
                    std::function<void()> done_callback);
  void StatisticsThread();
  std::vector<RangeType> CalculateThreadKeyRanges(uint32_t max_num_of_threads);
  // Pushes messages to the message queue.
  void CommunicationThread();

  // Track the active reading threads, the last active thread does the cleanup
  unsigned int active_reader_threads_count_;
  std::mutex active_reader_threads_mutex_;

  // The replication starting time
  std::chrono::time_point<std::chrono::system_clock> start_time_;
};

}
