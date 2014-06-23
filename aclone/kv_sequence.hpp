#ifndef ACLONE_KV_SEQUENCE_HPP
#define ACLONE_KV_SEQUENCE_HPP

#include <vector>
#include <cstdint>

namespace aclone {

class kv_sequence {
public:

    kv_sequence next() const
        {
        kv_sequence rval = *this;
        std::vector<uint64_t>& n = rval.sequence;

        for ( int i = n.size() - 1; i >= 0; --i )
            {
            ++n[i];

            if ( n[i] != 0 )
                break;

            if ( i == 0 )
                n.insert(n.begin(), 1);
            }

        return rval;
        }

    kv_sequence& operator++()
        {
        sequence = move(next().sequence);
        return *this;
        }

    kv_sequence operator++(int)
        {
        kv_sequence tmp = *this;
        operator++();
        return tmp;
        }

    std::vector<uint64_t> sequence = { 0 };
};

inline bool operator==(const kv_sequence& lhs, const kv_sequence& rhs)
    { return lhs.sequence == rhs.sequence; }
inline bool operator!=(const kv_sequence& lhs, const kv_sequence& rhs)
    { return ! operator==(lhs,rhs); }
inline bool operator<(const kv_sequence& lhs, const kv_sequence& rhs)
    {
    if ( lhs.sequence.size() == rhs.sequence.size() )
        return lhs.sequence < rhs.sequence;
    return lhs.sequence.size() < rhs.sequence.size();
    }
inline bool operator>(const kv_sequence& lhs, const kv_sequence& rhs)
    { return operator<(rhs,lhs); }
inline bool operator<=(const kv_sequence& lhs, const kv_sequence& rhs)
    { return ! operator>(lhs,rhs); }
inline bool operator>=(const kv_sequence& lhs, const kv_sequence& rhs)
    { return ! operator<(lhs,rhs); }

} // namespace aclone

#endif // ACLONE_KV_SEQUENCE_HPP
