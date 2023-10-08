#include "client.h"
#include "logger.h"
#include "pliops/simple_logger.h"
#include "rpc.h"
#include "pliops/kv_pair_simple_serializer.h"

#include <thread>
#include <chrono>

std::unique_ptr<ILogger> logger;

using namespace Replicator;
using namespace std::literals;


static void PrintHelp() {
  std::cout << "Usage: client_exe -s shard -p path -a ip_addr - " << std::endl;
  std::cout << "  -s the index of the shard" << std::endl;
  std::cout << "  -p the path to the replication directory" << std::endl;
  std::cout << "  -a the ip address of the server" << std::endl;
  std::cout << "  -n desired number of threads" << std::endl;
  std::cout << "  -t timeout [msec] (default: 50000)" << std::endl;
}

static void ParseArgs(int argc, char *argv[], int& shard, int& threads, std::string& path, std::string& ip, uint64_t& timeout_msec) {
  shard = -1;
  threads = -1;
  path.clear();
  ip.clear();

  for (int i = 1; i < argc; ++i) {
    if (!strcmp("-s", argv[i]) && i+1 < argc) {
      shard = atoi(argv[++i]);
      continue;
    }
    if (!strcmp("-n", argv[i]) && i+1 < argc) {
      threads = atoi(argv[++i]);
      continue;
    }
    if (!strcmp("-p", argv[i]) && i+1 < argc) {
      path = argv[++i];
      continue;
    }
    if (!strcmp("-a", argv[i]) && i+1 < argc) {
      ip = argv[++i];
      continue;
    }
    if (!strcmp("-t", argv[i]) && i+1 < argc) {
      timeout_msec = std::stoull(argv[++i]);
      continue;
    }
    if (!strcmp("-h", argv[i])) {
      PrintHelp();
      exit(0);
    }
    std::cout << "Wrong input: " << argv[i] << std::endl << std::endl;
    PrintHelp();
    exit(1);
  }

  if (shard < 0 || threads < 0 || path.empty() || ip.empty()) {
    PrintHelp();
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  int shard;
  int threads;
  std::string dsp_path;
  std::string server_ip;
  uint64_t timeout_msec = 50000; // default timeout
  ParseArgs(argc, argv, shard, threads, dsp_path, server_ip, timeout_msec);
  logger.reset(new SimpleLogger());

  RpcChannel rpc(RpcChannel::Pier::Client, server_ip);
  KvPairSimpleSerializer kv_pair_serializer;
  auto rc = ReplicateCheckpoint(rpc, shard, dsp_path, threads, timeout_msec, kv_pair_serializer);
  if (!rc.IsOk()) {
    Cleanup();
    logger->Log(Severity::ERROR, FormatString("ReplicateCheckpoint failed\n"));
    exit(1);
  }

  bool done = false;
  while (!done) {
    std::this_thread::sleep_for(10s);
    rc = CheckReplicationStatus(rpc, done);
    if (!rc.IsOk()) {
      Cleanup();
      logger->Log(Severity::ERROR, FormatString("CheckReplicationStatus failed\n"));
      exit(1);
    }
  }

  logger->Log(Severity::INFO, "All done!\n");
  return 0;
}
