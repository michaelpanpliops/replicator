#pragma once

#include <thread>
#include <mutex>
#include <filesystem>

#include "communication.h"
#include "defs.h"
#include "rocksdb/db.h"
#include "utils/ring_buffer.h"


namespace Replicator {

using ConsumerState = Replicator::State;

using ServerMessageQueue = moodycamel::SingleProducerSingleConsumerRingBuffer<std::pair<std::string, std::string>>;
constexpr size_t SERVER_MESSAGE_QUEUE_CAPACITY = 32 * 10000;

struct Statistics {
  std::atomic<uint64_t> num_kv_pairs = 0;
  std::atomic<uint64_t> num_bytes = 0;
};

class Consumer {
public:
  explicit Consumer(IKvPairSerializer& kv_pair_serializer);
  virtual ~Consumer();
  int Start(const std::string& replica_path, uint16_t& port,
            std::function<void(ConsumerState, const std::string&)>& done_callback);
  int Stop();
  int GetState(ConsumerState& state, std::string& error);
  int GetStats(uint64_t& num_kv_pairs, uint64_t& num_bytes);

private:
  // Callback to be called on completion/error
  std::function<void(ConsumerState, const std::string&)> done_callback_;

  // Socket to listen for an incoming connection
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connection_;

  // Writer and communication threads
  std::unique_ptr<std::thread> writer_thread_;
  std::unique_ptr<std::thread> communication_thread_;

  // Statistics
  Statistics statistics_;

  // Serialization
  IKvPairSerializer& kv_pair_serializer_;
 
  // The KV pairs received over the network. The writer pull messages out of the queue.
  std::unique_ptr<ServerMessageQueue> message_queue_;

  // DB
  ROCKSDB_NAMESPACE::DB* shard_ = nullptr;
  int OpenReplica(const std::string& replica_path);

  // Signal threads to exit
  std::atomic<bool> kill_;

  // Worker threads
  void WriterThread();
  void CommunicationThread();
  void StopImpl();

  // The replication starting time
  std::chrono::time_point<std::chrono::system_clock> start_time_;

  // State and error message
  ConsumerState state_ = ConsumerState::IDLE;
  std::string error_;
  std::mutex state_mutex_;
  void SetState(const ConsumerState& state, const std::string& error);
};

}
