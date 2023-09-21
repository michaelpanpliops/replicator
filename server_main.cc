#include "server.h"
#include "log.h"
#include "rpc.h"

#include <thread>
#include <chrono>

using namespace Replicator;
using namespace std::literals;


static void PrintHelp() {
  std::cout << "Usage: server_exe -p path -a ip_addr -i parallelism" << std::endl;
  std::cout << "  -p the path to the db directory" << std::endl;
  std::cout << "  -a the ip address of the client" << std::endl;
  std::cout << "  -i internal iterator parallelism" << std::endl;
  std::cout << "  -t timeout [msec] (default: 50000)" << std::endl;
}

static void ParseArgs(int argc, char *argv[], int& parallelism, std::string& path, std::string& ip, uint64_t& timeout_msec) {
  parallelism = -1;
  path.clear();
  ip.clear();

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
      parallelism = atoi(argv[++i]);
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

  if (parallelism < 0 || path.empty() || ip.empty()) {
    PrintHelp();
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  int parallelism;
  std::string src_path;
  std::string client_ip;
  uint64_t timeout_msec = 50000; // default timeout
  ParseArgs(argc, argv, parallelism, src_path, client_ip, timeout_msec);

  RpcChannel rpc(RpcChannel::Pier::Server, client_ip);
  auto rc = ProvideCheckpoint(rpc, src_path, client_ip, parallelism, timeout_msec);
  if (rc) {
    log_message(FormatString("ProvideCheckpoint failed\n"));
    exit(1);
  }

  log_message("All done!\n");
  return 0;
}
