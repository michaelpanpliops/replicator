#pragma once

#include <thread>
#include <mutex>
#include <string_view>
#include <filesystem>

#include "communication.h"
#include "defs.h"
#include "rocksdb/db.h"
#include "utils/blocking_concurrent_queue.h"
#include "pliops/status.h"


namespace Replicator {

using ProducerState = Replicator::State;
using RepStatus = Replicator::Status;

using RangeType = std::pair<std::optional<std::string>, std::optional<std::string>>;
using MessageQueue = moodycamel::BlockingConcurrentQueue<std::pair<std::string, std::string>>;
constexpr size_t MESSAGE_QUEUE_CAPACITY = 32 * 10000;

struct Statistics {
  std::atomic<uint64_t> num_kv_pairs = 0;
  std::atomic<uint64_t> num_bytes = 0;
};

class Producer {
public:
  explicit Producer(IKvPairSerializer& kv_pair_serializer);
  virtual ~Producer();
  RepStatus OpenShard(const std::string& shard_path);
  RepStatus Start(const std::string& ip, uint16_t port,
            uint32_t max_num_of_threads, uint32_t parallelism,
            uint32_t ops_timeout_msec, uint32_t connect_timeout_msec,
            std::function<void(ProducerState, const RepStatus&)>& done_callback);
  RepStatus Stop();
  RepStatus GetState(ProducerState& state, RepStatus& status);
  RepStatus GetStats(uint64_t& num_kv_pairs, uint64_t& num_bytes);

private:
  // Callback to be called on completion/error
  std::function<void(ProducerState, const RepStatus&)> done_callback_;

  // Shard connection and its connection thread
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connection_;
  std::unique_ptr<std::thread> communication_thread_;

  // Reader threads
  std::vector<std::thread> reader_threads_;

  // Statistics
  Statistics statistics_;

  // Serialization
  IKvPairSerializer& kv_pair_serializer_;

  // Stores KV pairs to send over the network. The readers push messages to the queue.
  std::unique_ptr<MessageQueue> message_queue_;

  // DB
  ROCKSDB_NAMESPACE::DB* shard_ = nullptr;

  // Key range per reader thread
  std::vector<RangeType> thread_key_ranges_;
  RepStatus CalculateThreadKeyRanges(uint32_t max_num_of_threads, std::vector<RangeType>& ranges);

  // Signal threads to exit
  std::atomic<bool> kill_;
  uint32_t ops_timeout_msec_;
  uint32_t connect_timeout_msec_;

  // Worker threads
  void ReaderThread(uint32_t iterator_parallelism_factor, uint32_t thread_id);
  void CommunicationThread();
  void StopImpl();

  // Track the active reading threads, the last active thread does the cleanup
  unsigned int active_reader_threads_count_;
  std::mutex active_reader_threads_mutex_;

  // The replication starting time
  std::chrono::time_point<std::chrono::system_clock> start_time_;

  // State and status
  ProducerState state_ = ProducerState::IDLE;
  RepStatus status_;
  std::mutex state_mutex_;
  void SetState(const ProducerState& state, const RepStatus& status);

  // Enqueue limit timeout
  bool EnqueueTimed(std::unique_ptr<Replicator::MessageQueue>& message_queue,
                     std::pair<std::string, std::string>&& message,
                     std::atomic<bool>& kill,
                     uint64_t timeout_msec);
};

}
