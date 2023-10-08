#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <memory>

#include "defs.h"

class ILogger {
public:
    virtual ~ILogger() {}

    virtual void Log(Severity level, const std::string& message) = 0;
};


extern std::unique_ptr<ILogger> logger;


#endif // LOGGER_H