#include <mutex>
#include <filesystem>

#include "rocksdb/db.h"

#include "consumer.h"
#include "log.h"

namespace Replicator {

Consumer::Consumer()
    : connections_(), writer_threads_(), kill_(false)
{}

Consumer::~Consumer() {
    for (auto* db_pointer : shards_) {
        db_pointer->Close();
        delete db_pointer;
    }
}

void Consumer::WriterThread(uint32_t shard_id, uint32_t thread_id) {
    log_message(FormatString("Writer thread #%d for shard %d started\n", thread_id, shard_id));

    while(!kill_) {
        // Pop the next KV pair.
        std::pair<std::string,std::string> message;
        message_queues_[shard_id]->wait_dequeue(message);
        std::string& key = message.first;
        std::string& value = message.second;
        if (key.empty()) {
            // message buffer is closed, finish.
            return;
        }
        // Insert KV to DB
        ROCKSDB_NAMESPACE::WriteOptions wo;
        wo.disableWAL = true;
        auto status = shards_[shard_id]->Put(wo, key, value);
        if(!status.ok()) {
            throw std::runtime_error(FormatString("Failed inserting key %s to shard #%d, reason: %s\n", key, shard_id, status.ToString()));
        }
    }
    log_message(FormatString("Writer thread #%d shard %d ended\n", thread_id, shard_id));
}

void Consumer::CommunicationThread(uint32_t shard_id) {
    log_message(FormatString("Communication thread for shard #%d: Started.\n", shard_id));
    auto& listen_s = connections_[shard_id];
    auto connection = accept(listen_s);
    std::string key, value;
    while(!kill_){
        try {
            std::tie(key, value) = connection->Receive();
            message_queues_[shard_id]->wait_enqueue({key, value});
        } catch (const ConnectionClosed&) {
            // For server writer threads, this is normal, the sender side might have finished sending replication data.
            log_message(FormatString("Communication thread for shard #%d: Connection closed.\n", shard_id));
            message_queues_[shard_id]->wait_enqueue({"", ""}); // Empty element signals end of messages.
            break;
        }
    }
}

std::vector<ROCKSDB_NAMESPACE::DB*> Consumer::OpenReplica(const std::string& replica_path) {
    std::vector<ROCKSDB_NAMESPACE::DB*> result;
    unsigned int shard_id = 0;
    ROCKSDB_NAMESPACE::DB* db;
    ROCKSDB_NAMESPACE::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
#ifndef LEGACY_ROCKSDB_SENDER
    options.OptimizeForXdpRocks();
#endif
    options.max_background_jobs = 16;
    options.max_subcompactions = 32;
    auto shard_path = replica_path;
    if (!std::filesystem::exists(shard_path)) {
        std::filesystem::create_directories(shard_path);
    }
    auto status = ROCKSDB_NAMESPACE::DB::Open(options,
                                                shard_path,
                                                &db);
    if (!status.ok()) {
        throw std::runtime_error(FormatString("Failed to open db for shard #%d, reason: %s\n", shard_id, status.ToString()));
    }
    result.push_back(db);
    return result;
}

void Consumer::Start(const std::string& replica_path, uint32_t num_of_threads, uint16_t& port) {
    shards_ = OpenReplica(replica_path);

    // Listen for connections from all shards. connections are ordered by the shard ID
    // connections_ = wait_for_connections(config_.server_port, config_.number_of_shards);
    connections_ = wait_for_connections(port);

    // For each shard, we allocate the same number of writer threads.
    // We already made sure the total number of threads is divisible by the number of shards.
    // We start the writer threads and the communication threads for each shard
    log_message("Starting writer threads\n");
    unsigned int i = 0;
    message_queues_.push_back(std::make_unique<ServerMessageQueue>(SERVER_MESSAGE_QUEUE_CAPACITY));
    writer_threads_.push_back({});
    auto threads_per_shard = num_of_threads;
    // Start writer threads
    for (unsigned int j = 0; j < threads_per_shard; ++j) {
        writer_threads_[i].push_back(std::thread([this, i, j]() {
            this->WriterThread(i, j);
        }));
    }
    // Start the single communication thread per shard
    communication_threads_.push_back(std::thread([this, i]() {
        this->CommunicationThread(i);
    }));
}

void Consumer::Stop()
{
    kill_ = true;

    for (auto& communication_thread : communication_threads_) {
        communication_thread.join();
    }

    for (auto& writer_threads_per_shard : writer_threads_) {
        for (auto& writer_thread : writer_threads_per_shard) {
            writer_thread.join();
        }
    }

    log_message("Done.\n");
}

}