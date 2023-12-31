#include <cstdint>
#include <string>
#include <vector>
#include <cstring> 
#include <stdexcept>
#include <memory>

#include "pliops/kv_pair_simple_serializer.h"
#include "utils/string_util.h"
#include "logger.h"

namespace Replicator {


RepStatus KvPairSimpleSerializer::Serialize(const char* key, uint32_t key_size, const char* value, uint32_t value_size, std::vector<char>& buffer) {
    uint32_t message_size = sizeof(key_size) + key_size + sizeof(value_size) + value_size;
    buffer.resize(message_size);
    char* ptr = buffer.data();
    memcpy(ptr, &key_size, sizeof(key_size));
    ptr += sizeof(key_size);
    memcpy(ptr, key, key_size);
    ptr += key_size;
    memcpy(ptr, &value_size, sizeof(value_size));
    ptr += sizeof(value_size);
    memcpy(ptr, value, value_size);
    return RepStatus();
}
RepStatus KvPairSimpleSerializer::Deserialize(const char* buf, uint32_t buf_size, std::string& key, std::string& value) {
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    if (buf_size < sizeof(key_size)) {
        return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Bad message: buf_size < sizeof(key_size)\n"));
    }
    memcpy(&key_size, buf, sizeof(key_size));
    buf += sizeof(key_size);
    if (buf_size < sizeof(key_size)+key_size) {
        return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Bad message: buf_size < sizeof(key_size)+key_size\n"));
    }
    key.assign(buf, key_size);
    buf += key_size;
    if (buf_size < sizeof(key_size)+key_size+sizeof(value_size)) {
        return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Bad message: buf_size < sizeof(key_size)+key_size+sizeof(value_size)\n"));
    }
    memcpy(&value_size, buf, sizeof(value_size));
    buf += sizeof(value_size);
    if (buf_size != sizeof(key_size)+key_size+sizeof(value_size)+value_size) {
        return RepStatus(Code::REPLICATOR_FAILURE, Severity::ERROR, FormatString("Bad message: buf_size != sizeof(key_size)+key_size+sizeof(value_size)+value_size\n"));
    }
    value.assign(buf, value_size);
    return RepStatus();
}

}
