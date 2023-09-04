#include "server.h"
#include "log.h"
#include "rpc.h"

#include <thread>
#include <chrono>

using namespace Replicator;
using namespace std::literals;


static void PrintHelp() {
  std::cout << "Usage: server_exe -p path -a ip_addr" << std::endl;
  std::cout << "  -p the path to the db directory" << std::endl;
  std::cout << "  -a the ip address of the client" << std::endl;
}

static void ParseArgs(int argc, char *argv[], std::string& path, std::string& ip) {
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
    if (!strcmp("-h", argv[i])) {
      PrintHelp();
      exit(0);
    }
    std::cout << "Wrong input: " << argv[i] << std::endl << std::endl;
    PrintHelp();
    exit(1);
  }

  if (path.empty() || ip.empty()) {
    PrintHelp();
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  std::string src_path;
  std::string client_ip;
  ParseArgs(argc, argv, src_path, client_ip);

  try {
    RpcChannel rpc(RpcChannel::Pier::Server, client_ip);
    ProvideCheckpoint(rpc, src_path, client_ip);
  } catch (const std::exception& e) {
    log_message(FormatString("ERROR\n\t%s\n", e.what()));
    return 1;
  }
  log_message("All done!\n");
  // while(true) { std::this_thread::sleep_for(60s); }
  return 0;
}
