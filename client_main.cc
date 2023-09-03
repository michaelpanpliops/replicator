#include "client.h"
#include "log.h"
#include "rpc.h"

#include <thread>
#include <chrono>

using namespace Replicator;
using namespace std::literals;


static void PrintHelp() {
  std::cout << "Usage: client_exe -s shard -p path" << std::endl;
  std::cout << "  -s the index of the shard" << std::endl;
  std::cout << "  -p the path to the replication directory" << std::endl;
}

static void ParseArgs(int argc, char *argv[], int& shard, std::string& path) {
  shard = -1;
  path.clear();

  for (int i = 1; i < argc; ++i) {
    if (!strcmp("-s", argv[i]) && i+1 < argc) {
      shard = atoi(argv[++i]);
      continue;
    }
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

  if (shard < 0 || path.empty()) {
    PrintHelp();
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  int shard;
  std::string dsp_path;
  ParseArgs(argc, argv, shard, dsp_path);

  try {
    RpcChannel rpc(RpcChannel::Pier::Client);
    RestoreCheckpoint(rpc, shard, dsp_path);

    bool done = false;
    while (!done) {
      std::this_thread::sleep_for(10s);
      done = CheckReplicationStatus(rpc);
    }
  } catch (const std::exception& e) {
    log_message(FormatString("ERROR\n\t%s\n", e.what()));
    return 1;
  }
  log_message("All done!\n");
  // while(true) { std::this_thread::sleep_for(60s); }
  return 0;
}
