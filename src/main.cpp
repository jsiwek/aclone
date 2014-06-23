#include <string>
#include <cstdint>
#include <getopt.h>

#include <cppa/cppa.hpp>

#include "aclone/aclone.h"
#include "aclone/master.hpp"
#include "aclone/cloner.hpp"
#include "aclone/requester.hpp"

using namespace std;
using namespace cppa;
using namespace aclone;

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
};

static const char* opt_string = "p:a:k:f:mcru";

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
    string key = "testkey";
    string freqstr = "1";
    string addr = "127.0.0.1";
    const char* topic = "dummy";

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
        default:
            usage(argv[0]);
            return 1;
        }
        }

    uint16_t port = stoul(portstr);
    uint16_t freq = stoul(freqstr);
    aclone_context* ctx = aclone_context_create(0);

    switch ( mode ) {
    case KV_MODE_MASTER:
        {
        //auto mstr = spawn<master>();
        //publish(mstr, port, addr.c_str());
        aclone_store* master = aclone_store_open_master(ctx, topic, 0);
        aclone_store_publish_master(ctx, master, addr.c_str(), port);
        }
        break;
    case KV_MODE_CLONER:
        {
        //spawn<cloner>(addr, port);
        aclone_store_open_cloner(ctx, topic, addr.c_str(), port, 0);
        }
        break;
    case KV_MODE_REQUESTER:
        {
        //auto remote = remote_actor(addr, port);
        //spawn<requester>(remote, key, chrono::seconds(freq));
        aclone_store* remote = aclone_store_open_remote(ctx, topic,
                                                        addr.c_str(), port, 0);

        aclone_key k{const_cast<char*>(key.data()), key.size()};
        bool sync = true;
        string mycookie = "mycookie";

        if ( sync )
            for ( ; ; )
                {
                uint64_t size;
                int res = aclone_store_size_sync(ctx, remote, &size);

                if ( res )
                    cout << "Store size: " << size << endl;
                else
                    cout << "Failed to get store size." << endl;

                int haskey;
                res = aclone_store_haskey_sync(ctx, remote, k, &haskey);

                if ( res )
                    cout << "Has key '" << key << "': "
                         << (haskey ? "T" : "F") << endl;
                else
                    cout << "Failed to check key membership." << endl;

                aclone_val v{0, 0};
                res = aclone_store_lookup_sync(ctx, remote, k, &v);

                if ( res )
                    cout << "Value of key '" << key << "': "
                         << *static_cast<int64_t*>(v.val) << endl;
                else
                    cout << "Failed to lookup key." << endl;

                free(v.val);

                sleep(freq);
                }
        else
            for ( ; ; )
                {
                auto size_cb = [](aclone_async_result res, void* cookie,
                                  uint64_t size)
                    {
                    if ( res == ACLONE_ASYNC_SUCCESS )
                        cout << *static_cast<string*>(cookie) << " store size: "
                             << size << endl;
                    else
                        cout << "Failed to get store size: " << res << endl;
                    };

                auto haskey_cb = [](aclone_async_result res, void* cookie,
                                    aclone_key key, int exists)
                    {
                    if ( res == ACLONE_ASYNC_SUCCESS )
                        cout << *static_cast<string*>(cookie) << " key exists: "
                             << exists << endl;
                    else
                        cout << "Failed to check key exists: " << res << endl;
                    };

                auto lookup_cb = [](aclone_async_result res, void* cookie,
                                    aclone_key key, aclone_val val)
                    {
                    if ( res == ACLONE_ASYNC_SUCCESS )
                        {
                        string keystr(static_cast<const char*>(key.key),
                                      key.size);
                        stringstream ss;

                        if ( val.val )
                            ss << *static_cast<int64_t*>(val.val);
                        else
                            ss << "null";

                        string valstr = ss.str();

                        cout << *static_cast<string*>(cookie) << " lookup '"
                             << keystr << "': " << valstr << endl;
                        }
                    else
                        cout << "Failed to lookup key: " << res << endl;
                    };

                aclone_store_size_async(ctx, remote, 10, size_cb, &mycookie);
                sleep(freq);
                aclone_store_haskey_async(ctx, remote, k, 10, haskey_cb,
                                          &mycookie);
                sleep(freq);
                aclone_store_lookup_async(ctx, remote, k, 10, lookup_cb,
                                          &mycookie);
                sleep(freq);
                }
        }
        break;
    case KV_MODE_UPDATER:
        {
        //auto remote = remote_actor(addr, port);
        //spawn<updater>(remote, key, chrono::seconds(freq));
        aclone_store* remote = aclone_store_open_remote(ctx, topic,
                                                        addr.c_str(), port, 0);

        for ( ; ; )
            {
            aclone_key k{const_cast<char*>(key.data()), key.size()};
            uint64_t i = 1;
            aclone_val v{&i, sizeof(i)};
            aclone_store_increment(ctx, remote, k, v);
            sleep(freq);
            }
        }
        break;
    }

    await_all_actors_done();
    shutdown();
    return 0;
    }
