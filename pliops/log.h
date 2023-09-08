#pragma once

#include <mutex>
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>


namespace Replicator {

template<typename T>
auto Convert(T&& t) {
  if constexpr (std::is_same<std::remove_cv_t<std::remove_reference_t<T>>, std::string>::value) {
    return std::forward<T>(t).c_str();
  }
  else {
    return std::forward<T>(t);
  }
}

// It is okay to globally lock the messages, as those are sparse, and don't affect performance.
inline std::mutex message_lock;

inline std::string GetCurrentTimeString() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(now);
  auto tp = std::chrono::time_point<std::chrono::system_clock>(us);
  auto time = std::chrono::system_clock::to_time_t(tp);
  auto tm = std::localtime(&time);
  std::ostringstream oss;
  oss << std::put_time(tm, "%Y-%m-%d %H:%M:%S") << "." << std::setw(6) << std::setfill('0') << (us % 1000000).count();

  return oss.str();
}

template<typename ... Args>
std::string StringFormatInternal(const std::string& format, Args&& ... args)
{
  // First call is to calculate the required buffer size
  int size_s = std::snprintf(nullptr, 0, format.data(), args ... ) + 1; // Extra byte for null terminator

  if( size_s <= 0 ) {
      throw std::runtime_error( "Error during formatting." );
  }
  auto size = static_cast<size_t>( size_s ); // Prevent compiled conversion warnings

  auto buf = std::make_unique<char[]>( size );
  snprintf( buf.get(), size, format.data(), args ... );
  return std::string( buf.get(), buf.get() + size - 1 ); // -1 as We don't want the '\0' inside the string
}

template<typename ... Args>
std::string FormatString(const std::string& format, Args&& ... args) {
  return StringFormatInternal(format, Convert(std::forward<Args>(args))...);
}

inline void log_message(const std::string& message) {
  std::unique_lock lock(message_lock);
  auto current_time = GetCurrentTimeString();
  std::cout << FormatString("[%s]  %s", current_time, message);
}

}