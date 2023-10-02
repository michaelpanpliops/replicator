
#include "status.h"

namespace Replicator {

Status::Status(Code code, Severity severity, const std::string& message)
    : severity_(severity), message_(message), code_(code) {}

std::string Status::ToString() const {
    std::string severity_str;
    switch (severity_) {
        case Severity::INFO:
            severity_str = "INFO";
            break;
        case Severity::WARNING:
            severity_str = "WARNING";
            break;
        case Severity::ERROR:
            severity_str = "ERROR";
            break;
    }

    std::string code_str;
    switch (code_)
    {
    case Code::OK:
        code_str = "OK";
        break;
    case Code::TIMEOUT:
        code_str = "TIMEOUT";
        break;
    case Code::DB_FAILURE:
        code_str = "DB_FAILRE";
        break;
    case Code::NETWORK_FAILURE:
        code_str = "NETWORK_FAILURE";
        break;
    case Code::REPLICATOR_FAILURE:
        code_str = "REPLICATOR_FAILURE";
        break;
    default:
        code_str = "UNKNOWN";
        break;
    }

    return code_str + " - " + severity_str + ": " + message_;
}

bool Status::IsOk() {
    return code_ == Code::OK;
}

}