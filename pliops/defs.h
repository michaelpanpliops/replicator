#pragma once

#include <stdlib.h>
#include <unistd.h>

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
}

}
