#pragma once

#include <thread>
#include <mutex>
#include <filesystem>

#include "communication.h"
#include "rocksdb/db.h"
#include "utils/ring_buffer.h"


namespace Replicator {

using ServerMessageQueue = moodycamel::SingleProducerSingleConsumerRingBuffer<std::pair<std::string, std::string>>;
constexpr size_t SERVER_MESSAGE_QUEUE_CAPACITY = 32 * 10000;

struct Statistics {
  std::atomic<uint64_t> num_kv_pairs = 0;
  std::atomic<uint64_t> num_bytes = 0;
};

class Consumer {
public:
  explicit Consumer();
  virtual ~Consumer();
  void Start(const std::string& replica_path, uint16_t& port);
  void Stop();
private:
  // Socket to listen for an incomming connection
  std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>> connection_;

  // Writer and communication threads
  std::unique_ptr<std::thread> writer_thread_;
  std::unique_ptr<std::thread> communication_thread_;

  // Statistics
  Statistics statistics_;
 
  // The KV pairs received over the network. The writer threads pull messages out of the queue.
  std::unique_ptr<ServerMessageQueue> message_queue_;
  ROCKSDB_NAMESPACE::DB* shard_ = nullptr;

  // Signal all threads in the server to finish.
  bool kill_;
  // Writes data to destination DBs
  void WriterThread();
  // Pulls messages to the message queue.
  void CommunicationThread();
  ROCKSDB_NAMESPACE::DB* OpenReplica(const std::string& replica_path);
};

}
