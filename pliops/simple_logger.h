#ifndef SIMPLE_LOGGER_H
#define SIMPLE_LOGGER_H

#include <mutex>

#include "logger.h"

// It is okay to globally lock the messages, as those are sparse, and don't affect performance.
inline std::mutex message_lock;

class SimpleLogger : public ILogger {

  void Log(LogLevel level, const std::string& message) override;
  std::string GetCurrentTimeString();
  std::string GetSeverity(LogLevel level);
};


#endif // SIMPLE_LOGGER_H