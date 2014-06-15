#include <string>
#include <iostream>
#include <sstream>
#include <cstdint>
#include <map>
#include <getopt.h>

#include <cppa/cppa.hpp>

using namespace std;
using namespace cppa;

using val_type = int;
using key_type = string;

class kv_sequence {
public:

    kv_sequence next() const
        {
        kv_sequence rval = *this;
        vector<uint64_t>& n = rval.sequence;

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

    vector<uint64_t> sequence = { 0 };
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

    map<key_type, val_type> store;
    kv_sequence sequence;
};

inline bool operator==(const kv_store& lhs, const kv_store& rhs)
    { return lhs.sequence == rhs.sequence && lhs.store == rhs.store; }

static void dbg_dump(const actor& a, const string& store_id,
                     const kv_store& store)
    {
    string header = "===== " + store_id + " Contents =====";
    stringstream ss;
    ss << header << endl;

    for ( const auto& kv : store.store )
        ss << kv.first << ": " << kv.second << endl;

    for ( size_t i = 0; i < header.size(); ++i )
        ss << "=";

    ss << endl;
    aout(a) << ss.str();
    }

class kv_master : public sb_actor<kv_master> {
friend class sb_actor<kv_master>;

public:

    kv_master(const group& arg_topic)
        : topic(arg_topic)
        {
        join(topic);
        serving = (
        // Update Messages
        on(atom("update"), arg_match) >> [=](key_type& key, val_type& val)
            {
            store.update(key, val);
            send(topic, atom("update"), store.sequence, key, val);
            dbg_dump(this, idstr(), store);
            },
        on(atom("remove"), arg_match) >> [=](key_type& key)
            {
            store.remove(key);
            send(topic, atom("remove"), store.sequence, key);
            },
        on(atom("clear")) >> [=]()
            {
            store.clear();
            send(topic, atom("clear"), store.sequence);
            },
        // Request Messages
        on(atom("snapshot")) >> [=]()
            {
            return make_cow_tuple(store);
            },
        on(atom("lookup"), arg_match) >> [=](key_type& key)
            {
            auto it = store.store.find(key);
            if ( it == store.store.end() )
                return make_cow_tuple(atom("null"), 0);
            else
                return make_cow_tuple(atom("ok"), it->second);
            },
        on(atom("haskey"), arg_match) >> [=](key_type& key)
            {
            return make_cow_tuple(store.store.find(key) != store.store.end());
            },
        on(atom("size")) >> [=]()
            {
            return make_cow_tuple(store.store.size());
            }
        );
        }

private:

    string idstr() const
        {
        stringstream ss;
        ss << "kv_master(" << this << ")";
        return ss.str();
        }

    kv_store store;
    group topic;
    behavior serving;
    behavior& init_state = serving;
};

class kv_cloner : public sb_actor<kv_cloner> {
friend class sb_actor<kv_cloner>;

public:

    kv_cloner(const actor& master, const group& topic)
        {
        join(topic);
        // TODO: implement a reconnection behavior
        bootstrap = (
        after(chrono::seconds(0)) >> [=]()
            {
            synchronize();
            }
        );
        synchronizing = (
        on(atom("sync")) >> [=]()
            {
            sync_send(master, atom("snapshot")).then(
                on_arg_match >> [=](kv_store& sto)
                    {
                    store = sto;
                    become(synchronized);
                    aout(this) << "INFO: " << idstr() << " sync'd." << endl;
                    }
            );
            }
        );
        synchronized = (
        // Update Messages
        on(atom("update"), arg_match) >> [=](kv_sequence& seq, key_type& key,
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
        on(atom("remove"), arg_match) >> [=](kv_sequence& seq, key_type& key)
            {
            kv_sequence next = store.nextseq();

            if ( seq == next )
                store.remove(key);
            else if ( seq > next )
                out_of_sync();
            },
        on(atom("clear"), arg_match) >> [=](kv_sequence& seq)
            {
            kv_sequence next = store.nextseq();

            if ( seq == next )
                store.clear();
            else if ( seq > next )
                out_of_sync();
            },
        // Request Messages
        on(atom("lookup"), arg_match) >> [=](key_type& key)
            {
            auto it = store.store.find(key);
            if ( it == store.store.end() )
                return make_cow_tuple(atom("null"), 0);
            else
                return make_cow_tuple(atom("ok"), it->second);
            },
        on(atom("haskey"), arg_match) >> [=](key_type& key)
            {
            return make_cow_tuple(store.store.find(key) != store.store.end());
            },
        on(atom("size")) >> [=]()
            {
            return make_cow_tuple(store.store.size());
            }
        );
        }

private:

    void synchronize()
        {
        become(synchronizing);
        send(this, atom("sync"));
        }

    void out_of_sync()
        {
        // TODO: should never be able to get in to this state?
        aout(this) << "ERROR: " << idstr() << " out of sync." << endl;
        synchronize();
        }

    string idstr() const
        {
        stringstream ss;
        ss << "kv_cloner(" << this << ")";
        return ss.str();
        }

    kv_store store;
    behavior bootstrap;
    behavior synchronizing;
    behavior synchronized;
    behavior& init_state = bootstrap;
};

class kv_counter : public sb_actor<kv_counter> {
friend class sb_actor<kv_counter>;

public:

    kv_counter(const actor& master, const string& key,
               chrono::duration<double> freq)
        {
        bootstrap = (
        after(chrono::seconds(0)) >> [=]()
            {
            become(counting);
            send(this, atom("count"));
            }
        );
        counting = (
        on(atom("count")) >> [=]()
            {
            send(master, atom("update"), key, count);
            ++count;
            delayed_send(this, freq, atom("count"));
            }
        );
        }

private:

    val_type count = 0;
    behavior bootstrap;
    behavior counting;
    behavior& init_state = bootstrap;
};

class kv_requester : public sb_actor<kv_requester> {
friend class sb_actor<kv_requester>;

public:

    kv_requester(const actor& kv_store, const string& key,
                 chrono::duration<double> freq)
        {
        bootstrap = (
        after(chrono::seconds(0)) >> [=]()
            {
            become(requesting);
            send(this, atom("request"));
            }
        );
        requesting = (
        on(atom("request")) >> [=]()
            {
            sync_send(kv_store, atom("lookup"), key).then(
                on(atom("ok"), arg_match) >> [=](val_type& val)
                    {
                    aout(this) << "request(" << key << "): " << val << endl;
                    },
                on(atom("null"), any_vals) >> [=]()
                    {
                    aout(this) << "request(" << key << "): null" << endl;
                    }
            );
            delayed_send(this, freq, atom("request"));
            }
        );
        }

private:

    behavior bootstrap;
    behavior requesting;
    behavior& init_state = bootstrap;
};

static void usage(const string& program)
    {
    fprintf(stderr, "%s --server|--client [options]\n", program.c_str());
    fprintf(stderr, "    -m|--master      | server/authoritative mode\n");
    fprintf(stderr, "    -c|--cloner      | client/non-authoritative mode\n");
    fprintf(stderr, "    -p|--port        | TCP port to bind or connect\n");
    fprintf(stderr, "    -a|--addr        | addr to bind or connect\n");
    fprintf(stderr, "    -r|--requester   | sends requests periodically\n");
    fprintf(stderr, "    -u|--updater     | sends updates periodically\n");
    fprintf(stderr, "    -k|--key         | key to update/request\n");
    fprintf(stderr, "    -f|--freq        | frequency to update/request\n");
    fprintf(stderr, "    -t|--topic       | topic/group name\n");
    }

static option long_options[] = {
    {"master",       no_argument,          0, 'm'},
    {"cloner",       no_argument,          0, 'c'},
    {"port",         required_argument,    0, 'p'},
    {"addr",         required_argument,    0, 'a'},
    {"requester",    no_argument,          0, 'r'},
    {"updater",      no_argument,          0, 'u'},
    {"key",          required_argument,    0, 'k'},
    {"freq",         required_argument,    0, 'f'},
    {"topic",        required_argument,    0, 't'},
};

static const char* opt_string = "p:a:k:f:t:mcru";

enum KVmode {
    KV_MODE_MASTER,
    KV_MODE_CLONER,
    KV_MODE_REQUESTER,
    KV_MODE_UPDATER,
};

int main(int argc, char** argv)
    {
    announce<kv_sequence>(&kv_sequence::sequence);
    announce<kv_store>(&kv_store::store, &kv_store::sequence);
    KVmode mode = KV_MODE_MASTER;
    string portstr = "9999";
    string key = "bogus";
    string freqstr = "1";
    string topic = "test";
    string addr = "127.0.0.1";

    for ( ; ; )
        {
        int o = getopt_long(argc, argv, opt_string, long_options, 0);

        if ( o == -1 )
            break;

        switch ( o ) {
        case 'm':
            mode = KV_MODE_MASTER;
            break;
        case 'c':
            mode = KV_MODE_CLONER;
            break;
        case 'r':
            mode = KV_MODE_REQUESTER;
            break;
        case 'u':
            mode = KV_MODE_UPDATER;
            break;
        case 'p':
            portstr = optarg;
            break;
        case 'a':
            addr = optarg;
            break;
        case 'k':
            key = optarg;
            break;
        case 'f':
            freqstr = optarg;
            break;
        case 't':
            topic = optarg;
            break;
        default:
            usage(argv[0]);
            return 1;
        }
        }

    uint16_t port = stoul(portstr);
    uint16_t freq = stoul(freqstr);

    switch ( mode ) {
    case KV_MODE_MASTER:
        {
        auto grp = group::get("local", topic);
        auto master = spawn<kv_master>(grp);
        publish(master, port, addr.c_str());
        publish_local_groups(port + 1, addr.c_str());
        }
        break;
    case KV_MODE_CLONER:
        {
        stringstream groupid;
        groupid << topic << "@" << addr << ":" << port + 1;
        auto remote = remote_actor(addr, port);
        auto grp = group::get("remote", groupid.str());
        spawn<kv_cloner>(remote, grp);
        }
        break;
    case KV_MODE_REQUESTER:
        {
        auto remote = remote_actor(addr, port);
        spawn<kv_requester>(remote, key, chrono::seconds(freq));
        }
        break;
    case KV_MODE_UPDATER:
        {
        auto remote = remote_actor(addr, port);
        spawn<kv_counter>(remote, key, chrono::seconds(freq));
        }
        break;
    }

    /*
    auto master = spawn<kv_master>("test");
    auto cloner0 = spawn<kv_cloner>(master, group::get("local", "test"));
    auto cloner1 = spawn<kv_cloner>(master, group::get("local", "test"));
    auto updater0 = spawn<kv_counter>(master, "one", chrono::seconds(1));
    auto updater1 = spawn<kv_counter>(master, "two", chrono::seconds(10));
    auto requester0 = spawn<kv_requester>(cloner0, "one", chrono::seconds(2));
    auto requester1 = spawn<kv_requester>(master, "two", chrono::seconds(5));
    auto requester3 = spawn<kv_requester>(master, "nope", chrono::seconds(13));
    */

    await_all_actors_done();
    shutdown();
    return 0;
    }
