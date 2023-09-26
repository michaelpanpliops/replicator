#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <memory>

#include "simple_logger.h"
#include "utils/string_util.h"


void SimpleLogger::Log(LogLevel level, const std::string& message) {
  std::unique_lock lock(message_lock);
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

std::string SimpleLogger::GetSeverity(LogLevel level) {
  switch (level)
  {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::ERROR:
    return "ERROR";
  case LogLevel::INFO:
    return "INFO";
  case LogLevel::WARNING:
    return "WARNING";
  default:
    return "unknown";
  }
}