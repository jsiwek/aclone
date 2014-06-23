#ifndef ACLONE_CLONER_HPP
#define ACLONE_CLONER_HPP

#include <cstdint>
#include <string>
#include <sstream>
#include <iostream>

#include <cppa/cppa.hpp>

#include "kv_store.hpp"

namespace aclone {

class cloner : public cppa::sb_actor<cloner> {
friend class cppa::sb_actor<cloner>;

public:

    cloner(const std::string& addr, uint16_t port)
        {
        using namespace cppa;

        bootstrap = (
        after(std::chrono::seconds(0)) >> [=]()
            {
            reconnect();
            }
        );
        synchronizing = (
        on(atom("sync")) >> [=]()
            {
            sync_send(master, atom("snapshot"), this).then(
                on_arg_match >> [=](kv_store& sto)
                    {
                    store = sto;
                    become(synchronized);
                    aout(this) << "INFO: " << idstr() << " sync'd."
                               << std::endl;
                    },
                on(atom("quit")) >> [=]()
                    {
                    quit();
                    },
                on_arg_match >> [=](down_msg& d)
                    {
                    aout(this) << "WARN: lost connection to kv_master"
                               << std::endl;
                    demonitor(master);
                    master = invalid_actor;
                    reconnect();
                    }
            );
            }
        );
        disconnected = (
        on(atom("quit")) >> [=]()
            {
            quit();
            },
        on(atom("reconnect")) >> [=]()
            {
            if ( try_connect(addr, port) )
                synchronize();
            else
                reconnect();
            }
        );
        synchronized = (
        on(atom("quit")) >> [=]()
            {
            quit();
            },
        // Update Messages
        on(atom("insert"), arg_match) >> [=](key_type& key, val_type& val)
            {
            forward_to(master);
            },
        on(atom("insert"), arg_match) >> [=](kv_sequence& seq, key_type& key,
                                             val_type& val)
            {
            kv_sequence next = store.nextseq();

            if ( seq == next )
                {
                store.update(key, val);
                dbg_dump(this, idstr(), store);
                }
            else if ( seq > next )
                out_of_sync();
            },
        on(atom("increment"), arg_match) >> [=](key_type& key, val_type& by)
            {
            forward_to(master);
            },
        on(atom("increment"), arg_match) >> [=](kv_sequence& seq, key_type& key,
                                                val_type& by)
            {
            kv_sequence next = store.nextseq();

            if ( seq == next )
                {
                store.update(key, store.store[key] + by);
                dbg_dump(this, idstr(), store);
                }
            else if ( seq > next )
                out_of_sync();
            },
        on(atom("decrement"), arg_match) >> [=](key_type& key, val_type& by)
            {
            forward_to(master);
            },
        on(atom("decrement"), arg_match) >> [=](kv_sequence& seq, key_type& key,
                                                val_type& by)
            {
            kv_sequence next = store.nextseq();

            if ( seq == next )
                {
                store.update(key, store.store[key] - by);
                dbg_dump(this, idstr(), store);
                }
            else if ( seq > next )
                out_of_sync();
            },
        on(atom("remove"), arg_match) >> [=](key_type& key)
            {
            forward_to(master);
            },
        on(atom("remove"), arg_match) >> [=](kv_sequence& seq, key_type& key)
            {
            kv_sequence next = store.nextseq();

            if ( seq == next )
                {
                store.remove(key);
                dbg_dump(this, idstr(), store);
                }
            else if ( seq > next )
                out_of_sync();
            },
        on(atom("clear"), arg_match) >> [=]()
            {
            forward_to(master);
            },
        on(atom("clear"), arg_match) >> [=](kv_sequence& seq)
            {
            kv_sequence next = store.nextseq();

            if ( seq == next )
                {
                store.clear();
                dbg_dump(this, idstr(), store);
                }
            else if ( seq > next )
                out_of_sync();
            },
        // Request Messages
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
            return make_cow_tuple(store.store.find(key) != store.store.end());
            },
        on(atom("size")) >> [=]()
            {
            return make_cow_tuple(static_cast<uint64_t>(store.store.size()));
            },
        on_arg_match >> [=](down_msg& d)
            {
            aout(this) << "WARN: lost connection to kv_master" << std::endl;
            demonitor(master);
            master = invalid_actor;
            reconnect();
            }
        );
        }

private:

    bool try_connect(const std::string& addr, uint16_t port)
        {
        try
            {
            master = cppa::remote_actor(addr, port);
            monitor(master);
            aout(this) << "INFO: connected to kv_master: " << addr << ":"
                       << port << std::endl;
            return true;
            }
        catch ( std::exception& e)
            {
            aout(this) << "WARN: failed to connect to kv_master: "
                       << e.what() << ", will retry in 3s." << std::endl;
            }

        master = cppa::invalid_actor;
        return false;
        }

    void reconnect()
        {
        using namespace cppa;
        become(disconnected);
        delayed_send(this, std::chrono::seconds(3), atom("reconnect"));
        }

    void synchronize()
        {
        using namespace cppa;
        become(synchronizing);
        send(this, atom("sync"));
        }

    void out_of_sync()
        {
        // TODO: should never be able to get in to this state?
        aout(this) << "ERROR: " << idstr() << " out of sync." << std::endl;
        synchronize();
        }

    std::string idstr() const
        {
        std::stringstream ss;
        ss << "cloner(" << this << ")";
        return ss.str();
        }

    kv_store store;
    cppa::actor master = cppa::invalid_actor;
    cppa::behavior bootstrap;
    cppa::behavior disconnected;
    cppa::behavior synchronizing;
    cppa::behavior synchronized;
    cppa::behavior& init_state = bootstrap;
};

} // namespace aclone

#endif // ACLONE_CLONER_HPP
