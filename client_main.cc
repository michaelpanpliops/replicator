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
  std::cout << "  -ot operations timeout [msec] (optional, default: 60000)" << std::endl;
  std::cout << "  -ct connect timeout [msec] (optional, default: 60000)" << std::endl;
}

static void ParseArgs(int argc, char *argv[],
                      int& shard,
                      std::string& path, std::string& ip,
                      int& ops_timeout_msec, int& connect_timeout_msec) {
  path.clear();
  ip.clear();
  char* endptr;

  for (int i = 1; i < argc; ++i) {
    if (!strcmp("-s", argv[i]) && i+1 < argc) {
      shard = std::strtol(argv[++i], &endptr, 10);
      shard = (*endptr == '\0' ? shard : -1);
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
    if (!strcmp("-ot", argv[i]) && i+1 < argc) {
      ops_timeout_msec = std::strtol(argv[++i], &endptr, 10);
      ops_timeout_msec = (*endptr == '\0' ? ops_timeout_msec : -1);
      continue;
    }
    if (!strcmp("-ct", argv[i]) && i+1 < argc) {
      connect_timeout_msec = std::strtol(argv[++i], &endptr, 10);
      connect_timeout_msec = (*endptr == '\0' ? connect_timeout_msec : -1);
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

  if (shard < 0 || path.empty() || ip.empty()
      || ops_timeout_msec < 0 || connect_timeout_msec < 0) {
    std::cout << "Wrong input parameters" << std::endl << std::endl;
    PrintHelp();
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  int shard = -1;
  std::string dsp_path;
  std::string server_ip;
  int ops_timeout_msec = 60000; // default timeout
  int connect_timeout_msec = 60000; // default timeout
  ParseArgs(argc, argv, shard, dsp_path, server_ip,
            ops_timeout_msec, connect_timeout_msec);

  logger.reset(new SimpleLogger());
  RpcChannel rpc;
  auto rc = rpc.Connect(RpcChannel::Pier::Client, server_ip);
  if (!rc.ok()) {
    logger->Log(Severity::ERROR, FormatString("rpc failed: %s\n", rc.ToString()));
    exit(1);
  }
  KvPairSimpleSerializer kv_pair_serializer;
  rc = BeginReplication(rpc, shard, dsp_path,
                          ops_timeout_msec, connect_timeout_msec, kv_pair_serializer);
  if (!rc.ok()) {
    Cleanup();
    logger->Log(Severity::ERROR, FormatString("BeginReplication failed\n"));
    exit(1);
  }

  bool done = false;
  while (!done) {
    std::this_thread::sleep_for(10s);
    rc = CheckReplicationStatus(rpc, done);
    if (!rc.ok()) {
      Cleanup();
      logger->Log(Severity::ERROR, FormatString("CheckReplicationStatus failed\n"));
      exit(1);
    }
  }

  rc = EndReplication(rpc);
  if (!rc.ok()) {
    Cleanup();
    logger->Log(Severity::ERROR, FormatString("EndReplication failed\n"));
    exit(1);
  }

  logger->Log(Severity::INFO, "All done!\n");
  return 0;
}
