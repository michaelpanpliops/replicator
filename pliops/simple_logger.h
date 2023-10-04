#pragma once

#include <mutex>

#include "logger.h"

// It is okay to globally lock the messages, as those are sparse, and don't affect performance.
inline std::mutex log_lock;

class SimpleLogger : public ILogger {

  void Log(Severity level, const std::string& message) override;
  std::string GetCurrentTimeString();
  std::string GetSeverity(Severity level);
};
