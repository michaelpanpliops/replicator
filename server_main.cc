#include "server.h"
#include "rpc.h"
#include "logger.h"
#include "pliops/simple_logger.h"
#include "pliops/kv_pair_simple_serializer.h"

#include <thread>
#include <chrono>

std::unique_ptr<ILogger> logger;

using namespace Replicator;
using namespace std::literals;


static void PrintHelp() {
  std::cout << "Usage: server_exe -p path -a ip_addr -i parallelism" << std::endl;
  std::cout << "  -p the path to the db directory" << std::endl;
  std::cout << "  -a the ip address of the client" << std::endl;
  std::cout << "  -i internal iterator parallelism" << std::endl;
  std::cout << "  -ot operations timeout [msec] (optional, default: 60000)" << std::endl;
  std::cout << "  -ct connect timeout [msec] (optional, default: 60000)" << std::endl;
}

static void ParseArgs(int argc, char *argv[],
                      int& parallelism, std::string& path, std::string& ip,
                      int& ops_timeout_msec, int& connect_timeout_msec) {
  path.clear();
  ip.clear();
  char* endptr;

  for (int i = 1; i < argc; ++i) {
    if (!strcmp("-p", argv[i]) && i+1 < argc) {
      path = argv[++i];
      continue;
    }
    if (!strcmp("-a", argv[i]) && i+1 < argc) {
      ip = argv[++i];
      continue;
    }
    if (!strcmp("-i", argv[i]) && i+1 < argc) {
      parallelism = std::strtol(argv[++i], &endptr, 10);
      parallelism = (*endptr == '\0' ? parallelism : -1);
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

  if (parallelism < 0 || path.empty() || ip.empty()
      || ops_timeout_msec < 0 || connect_timeout_msec < 0) {
    std::cout << "Wrong input parameters" << std::endl << std::endl;
    PrintHelp();
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  int parallelism;
  std::string src_path;
  std::string client_ip;
  int ops_timeout_msec = 60000; // default timeout
  int connect_timeout_msec = 60000; // default timeout
  ParseArgs(argc, argv, parallelism, src_path, client_ip,
            ops_timeout_msec, connect_timeout_msec);

  logger.reset(new SimpleLogger());
  RpcChannel rpc(RpcChannel::Pier::Server, client_ip);
  KvPairSimpleSerializer kv_pair_serializer;
  auto rc = ProvideCheckpoint(rpc, src_path, client_ip, parallelism,
                              ops_timeout_msec, connect_timeout_msec, kv_pair_serializer);
  if (!rc.IsOk()) {
    logger->Log(Severity::ERROR, "ProvideCheckpoint failed\n");
    exit(1);
  }

  logger->Log(Severity::INFO, "All done!\n");
  return 0;
}
