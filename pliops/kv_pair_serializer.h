#pragma once

#include "pliops/status.h"


namespace Replicator {

using RepStatus = Replicator::Status;


class IKvPairSerializer {
public:
  virtual ~IKvPairSerializer() {}

    virtual RepStatus Serialize(const char* key, uint32_t key_size, const char* value, uint32_t value_size, std::vector<char>& buffer) = 0;

    virtual RepStatus Deserialize(const char* buf, uint32_t buf_size, std::string& key, std::string& value) = 0;
};

}
