#include "server.h"
#include "log.h"
#include "rpc.h"

#include <thread>
#include <chrono>

using namespace Replicator;
using namespace std::literals;


static void PrintHelp() {
  std::cout << "Usage: server_exe -p path" << std::endl;
  std::cout << "  -p the path to the db directory" << std::endl;
}

static void ParseArgs(int argc, char *argv[], std::string& path) {
  path.clear();

  for (int i = 1; i < argc; ++i) {
    if (!strcmp("-p", argv[i]) && i+1 < argc) {
      path = argv[++i];
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

  if (path.empty()) {
    PrintHelp();
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  std::string src_path;
  ParseArgs(argc, argv, src_path);

  try {
    RpcChannel rpc(RpcChannel::Pier::Server);
    ProvideCheckpoint(rpc, src_path);
  } catch (const std::exception& e) {
    log_message(FormatString("ERROR\n\t%s\n", e.what()));
    return 1;
  }
  log_message("All done!\n");
  // while(true) { std::this_thread::sleep_for(60s); }
  return 0;
}
