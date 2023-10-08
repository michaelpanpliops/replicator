
#include "status.h"

namespace Replicator {

Status::Status(Code code, Severity severity, const std::string& message)
    : severity_(severity), message_(message), code_(code) {}

std::string Status::ToString() const {
    return CodeToString(code_) + " - " + SeverityToString(severity_) + ": " + message_;
}

bool Status::IsOk() {
    return code_ == Code::OK;
}

}