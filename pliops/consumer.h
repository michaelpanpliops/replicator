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
using DBStatus = ROCKSDB_NAMESPACE::Status;
using RepStatus = Replicator::Status;

using ServerMessageQueue = moodycamel::SingleProducerSingleConsumerRingBuffer<std::pair<std::string, std::string>>;
constexpr size_t SERVER_MESSAGE_QUEUE_CAPACITY = 32 * 1024;

struct Statistics {
  std::atomic<uint64_t> num_kv_pairs = 0;
  std::atomic<uint64_t> num_bytes = 0;
};

class Consumer {
public:
  explicit Consumer(uint32_t ops_timeout_msec, uint32_t connect_timeout_msec,
                    IKvPairSerializer& kv_pair_serializer);
  virtual ~Consumer();
  RepStatus Start(const std::string& replica_path, uint16_t& port,
            std::function<void(ConsumerState, const RepStatus&)>& done_callback);
  RepStatus Stop();
  RepStatus GetState(ConsumerState& state, RepStatus& status);
  RepStatus GetStats(uint64_t& num_kv_pairs, uint64_t& num_bytes);

private:
  // Callback to be called on completion/error
  std::function<void(ConsumerState, const RepStatus&)> done_callback_;

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
  RepStatus OpenReplica(const std::string& replica_path);

  // Signal threads to exit
  std::atomic<bool> kill_;
  uint32_t ops_timeout_msec_;
  uint32_t connect_timeout_msec_;

  // Worker threads
  void WriterThread();
  void CommunicationThread();
  void StopImpl();

  // The replication starting time
  std::chrono::time_point<std::chrono::system_clock> start_time_;

  // State and error message
  ConsumerState state_ = ConsumerState::IDLE;
  RepStatus status_;
  std::mutex state_mutex_;
  void SetState(const ConsumerState& state, const RepStatus& status);
};

}
