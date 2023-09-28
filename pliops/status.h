#ifndef STATUS_H
#define STATUS_H

#include <string>

#include "defs.h"

namespace Replicator {

enum class Code : int {
    OK = 0,
    TIMEOUT = 1,
    NETWORK_FAILURE = 2,
    DB_FAILURE = 3,
    REPLICATOR_FAILURE
};

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
#endif  // STATUS_H
