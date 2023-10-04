#pragma once

namespace Replicator {

class IKvPairSerializer {
public:
    virtual ~IKvPairSerializer() {}

    virtual void Serialize(const char* key, uint32_t key_size, const char* value, uint32_t value_size, std::vector<char>& buffer) = 0;

    virtual std::pair<std::string, std::string> Deserialize(const char* buf, uint32_t buf_size) = 0;
};

}

