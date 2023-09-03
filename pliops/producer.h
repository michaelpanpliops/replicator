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

class Producer {
public:
    explicit Producer();
    virtual ~Producer();
    void OpenShard(const std::string& shard_path);
    void Run(const std::string& ip, uint16_t port, uint32_t max_num_of_threads);

private:
    // Contains connections to all shards. Each shard has a single connection.
    std::vector<std::unique_ptr<Connection<ConnectionType::TCP_SOCKET>>> connections_;
    // For each shard, we maintain the same specified number of reader threads.
    std::vector<std::vector<std::thread>> reader_threads_;
    std::vector<std::thread> communication_threads_;
    // Stores KV pairs to send over the network. The reader thread will push messages to the queue.
    // There is a message queue for each shard.
    std::vector<std::unique_ptr<MessageQueue>> message_queues_;
    std::vector<ROCKSDB_NAMESPACE::DB*> shards_;
    std::vector<std::vector<RangeType>> thread_key_ranges_;
    bool kill_;
    void ReaderThread(uint32_t shard_id, uint32_t thread_id, bool single_thread_per_shard);
    void StatisticsThread();
    std::vector<RangeType> CalculateThreadKeyRanges(uint32_t shard_id, uint32_t num_of_threads);
    // Pushes messages to the message queue.
    void CommunicationThread(uint32_t shard_id);
};

}