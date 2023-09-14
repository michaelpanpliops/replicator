#include "client.h"
#include "log.h"
#include "rpc.h"

#include <thread>
#include <chrono>

using namespace Replicator;
using namespace std::literals;


static void PrintHelp() {
  std::cout << "Usage: client_exe -s shard -p path -a ip_addr - " << std::endl;
  std::cout << "  -s the index of the shard" << std::endl;
  std::cout << "  -p the path to the replication directory" << std::endl;
  std::cout << "  -a the ip address of the server" << std::endl;
  std::cout << "  -t desired number of threads" << std::endl;
}

static void ParseArgs(int argc, char *argv[], int& shard, int& threads, std::string& path, std::string& ip) {
  shard = -1;
  threads = -1;
  path.clear();
  ip.clear();

  for (int i = 1; i < argc; ++i) {
    if (!strcmp("-s", argv[i]) && i+1 < argc) {
      shard = atoi(argv[++i]);
      continue;
    }
    if (!strcmp("-t", argv[i]) && i+1 < argc) {
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
  ParseArgs(argc, argv, shard, threads, dsp_path, server_ip);

  RpcChannel rpc(RpcChannel::Pier::Client, server_ip);
  auto rc = ReplicateCheckpoint(rpc, shard, dsp_path, threads);
  if (rc) {
    log_message(FormatString("ReplicateCheckpoint failed\n"));
    exit(1);
  }

  bool done = false;
  while (!done) {
    std::this_thread::sleep_for(10s);
    rc = CheckReplicationStatus(rpc, done);
    if (rc) {
      log_message(FormatString("CheckReplicationStatus failed\n"));
      exit(1);
    }
  }

  log_message("All done!\n");
  return 0;
}
