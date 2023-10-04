#pragma once

#include <stdlib.h>
#include <unistd.h>

enum class Severity {
  DEBUG = 0,
  INFO = 1,
  WARNING = 2,
  ERROR = 3,
  FATAL = 4
};

namespace Replicator {

enum class State : uint32_t {
  IDLE = 0, IN_PROGRESS = 1, DONE = 2, ERROR = 3, STOPPED = 4
};

namespace {

std::string ToString(State state) {
  switch (state)
  {
  case State::IDLE:
    return "IDLE";
  case State::IN_PROGRESS:
    return "IN_PROGRESS";
  case State::DONE:
    return "DONE";
  case State::ERROR:
    return "ERROR";
  case State::STOPPED:
    return "STOPPED";
  }
  return "UNKNOWN";
}

bool IsFinalState(const State& s) { return (s >= State::DONE); }
uint64_t msec_to_usec(uint64_t msec) { return msec * 1000; }

std::string SeverityToString(Severity severity) {
  switch (severity) {
    case Severity::DEBUG:
      return "DEBUG";
    case Severity::INFO:
      return "INFO";
    case Severity::WARNING:
      return "WARNING";
    case Severity::ERROR:
      return "ERROR";
    case Severity::FATAL:
      return "FATAL";
    default:
      return "UNKNOWN";
  }
}

}
}
