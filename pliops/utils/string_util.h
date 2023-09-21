#pragma once

#include <iostream>
#include <sstream>


template<typename T>
auto Convert(T&& t) {
  if constexpr (std::is_same<std::remove_cv_t<std::remove_reference_t<T>>, std::string>::value) {
    return std::forward<T>(t).c_str();
  }
  else {
    return std::forward<T>(t);
  }
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