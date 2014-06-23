#ifndef ACLONE_MASTER_HPP
#define ACLONE_MASTER_HPP

#include <string>
#include <sstream>
#include <iostream>
#include <unordered_map>
#include <cppa/cppa.hpp>

#include "kv_store.hpp"

namespace aclone {

static void dbg_dump(const cppa::actor& a, const std::string& store_id,
                     const aclone::kv_store& store)
    {
    std::string header = "===== " + store_id + " Contents =====";
    std::stringstream ss;
    ss << header << std::endl;

    for ( const auto& kv : store.store )
        ss << kv.first << ": " << kv.second << std::endl;

    for ( size_t i = 0; i < header.size(); ++i )
        ss << "=";

    ss << std::endl;
    aout(a) << ss.str();
    }

class master : public cppa::sb_actor<master> {
friend class cppa::sb_actor<master>;

public:

    master()
        {
        using namespace cppa;
        serving = (
        on(atom("quit")) >> [=]()
            {
            quit();
            },
        // Update Messages
        on(atom("insert"), arg_match) >> [=](key_type& key, val_type& val)
            {
            store.update(key, val);
            publish(make_cow_tuple(atom("insert"), store.sequence, key, val));
            dbg_dump(this, idstr(), store);
            },
        on(atom("increment"), arg_match) >> [=](key_type& key, val_type& by)
            {
            store.update(key, store.store[key] + by);
            publish(make_cow_tuple(atom("increment"), store.sequence, key, by));
            dbg_dump(this, idstr(), store);
            },
        on(atom("decrement"), arg_match) >> [=](key_type& key, val_type& by)
            {
            store.update(key, store.store[key] - by);
            publish(make_cow_tuple(atom("decrement"), store.sequence, key, by));
            dbg_dump(this, idstr(), store);
            },
        on(atom("remove"), arg_match) >> [=](key_type& key)
            {
            store.remove(key);
            publish(make_cow_tuple(atom("remove"), store.sequence, key));
            dbg_dump(this, idstr(), store);
            },
        on(atom("clear")) >> [=]()
            {
            store.clear();
            publish(make_cow_tuple(atom("clear"), store.sequence));
            dbg_dump(this, idstr(), store);
            },
        // Request Messages
        on(atom("snapshot"), arg_match) >> [=](actor& sender)
            {
            auto sender_addr = last_sender();

            if ( subscribers.find(sender_addr) == subscribers.end() )
                {
                monitor(sender_addr);
                subscribers[sender_addr] = sender;
                }

            return make_cow_tuple(store);
            },
        on(atom("lookup"), arg_match) >> [=](key_type& key)
            {
            auto it = store.store.find(key);
            if ( it == store.store.end() )
                return make_cow_tuple(atom("null"), static_cast<val_type>(0));
            else
                return make_cow_tuple(atom("ok"), it->second);
            },
        on(atom("haskey"), arg_match) >> [=](key_type& key)
            {
            bool rval = store.store.find(key) != store.store.end();
            return make_cow_tuple(rval);
            },
        on(atom("size")) >> [=]()
            {
            return make_cow_tuple(static_cast<uint64_t>(store.store.size()));
            },
        on_arg_match >> [=](down_msg& d)
            {
            auto sender_addr = last_sender();
            demonitor(sender_addr);
            subscribers.erase(sender_addr);
            }
        );
        }

private:

    void publish(const cppa::any_tuple& msg)
        {
        for ( auto s : subscribers ) send_tuple(s.second, msg);
        }

    std::string idstr() const
        {
        std::stringstream ss;
        ss << "master(" << this << ")";
        return ss.str();
        }

    kv_store store;
    std::unordered_map<cppa::actor_addr, cppa::actor> subscribers;
    cppa::behavior serving;
    cppa::behavior& init_state = serving;
};

} // namespace aclone

#endif //ACLONE_MASTER_HPP
