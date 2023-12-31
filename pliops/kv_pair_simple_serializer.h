#pragma once

#include "pliops/kv_pair_serializer.h"

namespace Replicator {

class KvPairSimpleSerializer : public IKvPairSerializer {
public:
    RepStatus Serialize(const char* key, uint32_t key_size, const char* value, uint32_t value_size, std::vector<char>& buffer) override;
    RepStatus Deserialize(const char* buf, uint32_t buf_size, std::string& key, std::string& value) override;
};

}
