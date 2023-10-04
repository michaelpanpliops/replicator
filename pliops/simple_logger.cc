#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <memory>

#include "simple_logger.h"
#include "utils/string_util.h"


void SimpleLogger::Log(Severity level, const std::string& message) {
  std::unique_lock lock(log_lock);
  auto current_time = GetCurrentTimeString();
  auto severity = GetSeverity(level);
  std::cout << FormatString("[%s][%s]  %s", current_time, severity, message);
}

std::string SimpleLogger::GetCurrentTimeString() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(now);
  auto tp = std::chrono::time_point<std::chrono::system_clock>(us);
  auto time = std::chrono::system_clock::to_time_t(tp);
  auto tm = std::localtime(&time);
  std::ostringstream oss;
  oss << std::put_time(tm, "%Y-%m-%d %H:%M:%S") << "." << std::setw(6) << std::setfill('0') << (us % 1000000).count();

  return oss.str();
}

std::string SimpleLogger::GetSeverity(Severity level) {
  switch (level)
  {
  case Severity::DEBUG:
    return "DEBUG";
  case Severity::ERROR:
    return "ERROR";
  case Severity::INFO:
    return "INFO";
  case Severity::WARNING:
    return "WARNING";
  case Severity::FATAL:
    return "FATAL";
  default:
    return "unknown";
  }
}