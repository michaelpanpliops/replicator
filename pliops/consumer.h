#pragma once

#include <thread>
#include <mutex>
#include <filesystem>

#include "rocksdb/db.h"

#include "communication.h"

#include "utils/ring_buffer.h"

namespace Replicator {

using ServerMessageQueue = moodycamel::SingleProducerSingleConsumerRingBuffer<std::pair<std::string, std::string>>;
constexpr size_t SERVER_MESSAGE_QUEUE_CAPACITY = 32 * 10000;

class Consumer {
public:
  explicit Consumer();
  virtual ~Consumer();
  void Start(const std::string& replica_path, uint32_t num_of_threads, std::string& ip, uint16_t& port);
  void Stop();
private:
    // Contains connections to all shards. Each shard has a single connection.
    std::vector<std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>> connections_;
    // For each shard, we maintain the same specified number of writer threads.
    std::vector<std::vector<std::thread>> writer_threads_;
    std::vector<std::thread> communication_threads_;
    // Stores KV pairs received over the network. The writer thread will pull messages out of the queue.
    // There is a message queue for each shard.
    std::vector<std::unique_ptr<ServerMessageQueue>> message_queues_;
    std::vector<ROCKSDB_NAMESPACE::DB*> shards_;
    // Signal all threads in the server to finish.
    bool kill_;
    // Writes data to destination DBs
    void WriterThread(uint32_t shard_id, uint32_t thread_id);
    // Pulls messages to the message queue.
    void CommunicationThread(uint32_t shard_id);
    std::vector<ROCKSDB_NAMESPACE::DB*> OpenReplica(const std::string& replica_path);
};

}