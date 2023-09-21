#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <memory>

enum class LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR
};

class ILogger {
public:
    virtual ~ILogger() {}

    virtual void Log(LogLevel level, const std::string& message) = 0;
};


extern std::unique_ptr<ILogger> logger;


#endif // LOGGER_H