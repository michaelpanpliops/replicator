#ifndef KV_PAIR_SERIALIZER_H
#define KV_PAIR_SERIALIZER_H

namespace Replicator {

class IKvPairSerializer {
public:
    virtual ~IKvPairSerializer() {}

    virtual void Serialize(const char* key, uint32_t key_size, const char* value, uint32_t value_size, std::vector<char>& buffer) = 0;

    virtual std::pair<std::string, std::string> Deserialize(const char* buf, uint32_t buf_size) = 0;
};

}

#endif // KV_PAIR_SERIALIZER_H