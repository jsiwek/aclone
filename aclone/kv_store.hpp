#ifndef ACLONE_KV_STORE_HPP
#define ACLONE_KV_STORE_HPP

#include <string>
#include <cstdint>
#include <map>

#include "kv_sequence.hpp"

namespace aclone {

using val_type = int64_t;
using key_type = std::string;

class kv_store {
public:

    void update(const key_type& key, const val_type& val)
        {
        ++sequence;
        store[key] = val;
        }

    void remove(const key_type& key)
        {
        ++sequence;
        store.erase(key);
        }

    void clear()
        {
        ++sequence;
        store.clear();
        }

    kv_sequence nextseq() const
        { return sequence.next(); }

    std::map<key_type, val_type> store;
    kv_sequence sequence;
};

inline bool operator==(const kv_store& lhs, const kv_store& rhs)
    { return lhs.sequence == rhs.sequence && lhs.store == rhs.store; }

} // namespace aclone

#endif // ACLONE_KV_STORE_HPP
