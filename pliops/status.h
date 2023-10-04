#pragma once

#include <string>

#include "defs.h"

namespace Replicator {

enum class Code : int {
  OK = 0,
  TIMEOUT_FAILURE = 1,
  NETWORK_FAILURE = 2,
  DB_FAILURE = 3,
  REPLICATOR_FAILURE = 4
};

namespace {
std::string CodeToString(Code code) {
  switch (code) {
  case Code::OK:
    return "OK";
  case Code::TIMEOUT_FAILURE:
    return "TIMEOUT_FAILURE";
  case Code::NETWORK_FAILURE:
    return "NETWORK_FAILURE";
  case Code::DB_FAILURE:
    return "DB_FAILRE";
  case Code::REPLICATOR_FAILURE:
    return "REPLICATOR_FAILURE";
  default:
    return "UNKNOWN";
  }
}
}

class Status {
public:
  Status(Code code = Code::OK, Severity severity = Severity::INFO, const std::string& message = "");
  std::string ToString() const;
  bool IsOk();

  Severity severity_;
  std::string message_;
  Code code_;
};

}
