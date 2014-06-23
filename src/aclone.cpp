#include "aclone/aclone.h"
#include "aclone/master.hpp"
#include "aclone/cloner.hpp"
#include "aclone/requester.hpp"

#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <condition_variable>
#include <cppa/cppa.hpp>

using namespace std;
using namespace cppa;

struct aclone_context {
    unordered_map<string, aclone_store*> masters;
};

enum ACloneStoreMode {
    ACLONE_STORE_MODE_MASTER,
    ACLONE_STORE_MODE_REMOTE,
    ACLONE_STORE_MODE_CLONER,
    ACLONE_STORE_MODE_COUNT,
};

struct aclone_store {

    ~aclone_store()
        {
		if ( mode != ACLONE_STORE_MODE_REMOTE )
			anon_send(a, atom("quit"));
        }

    string topic;
    ACloneStoreMode mode;
    actor a;
};

aclone_context* aclone_context_create(int flags)
    {
    return new aclone_context{};
    }

void aclone_context_destroy(aclone_context *ctx)
    {
    delete ctx;
    }

const char* aclone_store_get_topic(const aclone_store* store)
    {
    return store->topic.c_str();
    }

aclone_store* aclone_store_open_master(aclone_context* ctx,
                                       const char* topic, int flags)
    {
    auto it = ctx->masters.find(topic);

    if ( it != ctx->masters.end() )
        return it->second;

    auto rval = new aclone_store{ topic, ACLONE_STORE_MODE_MASTER,
                                  spawn<aclone::master>() };
    ctx->masters[topic] = rval;
    return rval;
    }

int aclone_store_publish_master(aclone_context* ctx, aclone_store* master,
                                const char* addr, uint16_t port)
    {
    if ( master->mode != ACLONE_STORE_MODE_MASTER )
        return 0;

    try
        {
        publish(master->a, port, addr);
        }
    catch ( exception& )
        {
        return 0;
        }

    return 1;
    }

aclone_store* aclone_store_open_remote(aclone_context* ctx, const char* topic,
                                       const char* addr, uint16_t port,
                                       int flags)
    {
    actor remote;

    try
        {
        remote = remote_actor(addr, port);
        }
    catch ( exception& e)
        {
        return 0;
        }

    return new aclone_store{ topic, ACLONE_STORE_MODE_REMOTE, remote };
    }

aclone_store* aclone_store_open_cloner(aclone_context* ctx, const char* topic,
                                       const char* addr, uint16_t port,
                                       int flags)
    {
    return new aclone_store{ topic, ACLONE_STORE_MODE_CLONER,
                             spawn<aclone::cloner>(addr, port) };
    }

int aclone_store_close(aclone_context* ctx, aclone_store* store)
    {
    if ( store->mode == ACLONE_STORE_MODE_MASTER )
        ctx->masters.erase(store->topic);

    delete store;
    return 1;
    }

int aclone_store_clear(aclone_context* ctx, aclone_store* store)
    {
    anon_send(store->a, atom("clear"));
    return 1;
    }

int aclone_store_insert(aclone_context* ctx, aclone_store* store,
                        aclone_key key, aclone_val val)
    {
    anon_send(store->a, atom("insert"),
              string(static_cast<const char*>(key.key), key.size),
              // TODO: fix val type assumption
              *static_cast<int64_t*>(val.val));
    return 1;
    }

int aclone_store_remove(aclone_context* ctx, aclone_store* store,
                        aclone_key key)
    {
    anon_send(store->a, atom("remove"),
              string(static_cast<const char*>(key.key), key.size));
    return 1;
    }

int aclone_store_increment(aclone_context* ctx, aclone_store* store,
                           aclone_key key, aclone_val by)
    {
    anon_send(store->a, atom("increment"),
              string(static_cast<const char*>(key.key), key.size),
              // TODO: fix val type assumption
              *static_cast<int64_t*>(by.val));
    return 1;
    }

int aclone_store_decrement(aclone_context* ctx, aclone_store* store,
                           aclone_key key, aclone_val by)
    {
    anon_send(store->a, atom("decrement"),
              string(static_cast<const char*>(key.key), key.size),
              // TODO: fix val type assumption
              *static_cast<int64_t*>(by.val));
    return 1;
    }

static bool sync_request(const actor& store, const any_tuple& request,
                         any_tuple& response)
    {
    int rval = -1;
    mutex mtx;
    condition_variable cv;
    unique_lock<mutex> guard{mtx};
    spawn<aclone::sync_requester>(store, mtx, cv, rval, request, response);
    cv.wait(guard, [&]{ return rval >= 0; });
    return rval > 0;
    }

static bool lookup_response_extract(const any_tuple& response, aclone_val* val)
    {
    auto resp_opt = tuple_cast<atom_value, aclone::val_type>(response);

    if ( ! resp_opt.valid() )
        return false;

    auto flag = get<0>(*resp_opt);

    if ( flag == atom("ok") )
        {
        auto v = get<1>(*resp_opt);

        if ( ! (val->val = malloc(sizeof(v))) )
            return false;

        val->size = sizeof(v);
        memcpy(val->val, &v, sizeof(v));
        return true;
        }
    else if ( flag == atom("null") )
        {
        val->size = 0;
        val->val = 0;
        return true;
        }
    else
        return false;
    }

int aclone_store_lookup_sync(aclone_context* ctx, aclone_store* store,
                             aclone_key key, aclone_val* result)
    {
    auto k = aclone::key_type(static_cast<char*>(key.key), key.size);
    any_tuple resp;

    if ( ! sync_request(store->a, make_cow_tuple(atom("lookup"), k), resp) )
        return 0;

    return lookup_response_extract(resp, result) ? 1 : 0;
    }

static void lookup_cb(aclone_async_result result, const any_tuple& response,
                      aclone_lookup_cb callback, void* cookie, aclone_key key)
    {
    if ( result != ACLONE_ASYNC_SUCCESS )
        {
        callback(result, cookie, key, {0, 0});
        return;
        }

    aclone_val val;

    if ( lookup_response_extract(response, &val) )
        {
        callback(result, cookie, key, val);
        free(val.val);
        }
    else
        callback(ACLONE_ASYNC_FAILURE, cookie, key, val);
    }

int aclone_store_lookup_async(aclone_context* ctx, aclone_store* store,
                              aclone_key key, double timeout,
                              aclone_lookup_cb callback, void* cookie)
    {
    using namespace std::placeholders;
    auto k = aclone::key_type(static_cast<char*>(key.key), key.size);
    auto bf = bind(lookup_cb, _1, _2, callback, cookie, key);
    spawn<aclone::async_requester>(store->a, make_cow_tuple(atom("lookup"), k),
                                   timeout, bf);
    return 1;
    }

static bool haskey_response_extract(const any_tuple& response, int* haskey)
    {
    auto resp_opt = tuple_cast<bool>(response);

    if ( ! resp_opt.valid() )
        return false;

    *haskey = get<0>(*resp_opt) ? 1 : 0;
    return true;
    }

int aclone_store_haskey_sync(aclone_context* ctx, aclone_store* store,
                             aclone_key key, int* result)
    {
    auto k = aclone::key_type(static_cast<char*>(key.key), key.size);
    any_tuple resp;

    if ( ! sync_request(store->a, make_cow_tuple(atom("haskey"), k), resp) )
        return 0;

    return haskey_response_extract(resp, result) ? 1 : 0;
    }

static void haskey_cb(aclone_async_result result, const any_tuple& response,
                      aclone_haskey_cb callback, void* cookie, aclone_key key)
    {
    if ( result != ACLONE_ASYNC_SUCCESS )
        {
        callback(result, cookie, key, 0);
        return;
        }

    int exists;

    if ( haskey_response_extract(response, &exists) )
        callback(result, cookie, key, exists);
    else
        callback(ACLONE_ASYNC_FAILURE, cookie, key, exists);
    }

int aclone_store_haskey_async(aclone_context* ctx, aclone_store* store,
                              aclone_key key, double timeout,
                              aclone_haskey_cb callback, void* cookie)
    {
    using namespace std::placeholders;
    auto k = aclone::key_type(static_cast<char*>(key.key), key.size);
    auto bf = bind(haskey_cb, _1, _2, callback, cookie, key);
    spawn<aclone::async_requester>(store->a, make_cow_tuple(atom("haskey"), k),
                                   timeout, bf);
    return 1;
    }

static bool size_response_extract(const any_tuple& response, uint64_t* size)
    {
    auto resp_opt = tuple_cast<uint64_t>(response);

    if ( ! resp_opt.valid() )
        return false;

    *size = get<0>(*resp_opt);
    return true;
    }

int aclone_store_size_sync(aclone_context* ctx, aclone_store* store,
                           uint64_t* result)
    {
    any_tuple resp;

    if ( ! sync_request(store->a, make_cow_tuple(atom("size")), resp) )
        return 0;

    return size_response_extract(resp, result) ? 1 : 0;
    }

static void size_cb(aclone_async_result result, const any_tuple& response,
                    aclone_size_cb callback, void* cookie)
    {
    if ( result != ACLONE_ASYNC_SUCCESS )
        {
        callback(result, cookie, 0);
        return;
        }

    uint64_t size;

    if ( size_response_extract(response, &size) )
        callback(result, cookie, size);
    else
        callback(ACLONE_ASYNC_FAILURE, cookie, size);
    }

int aclone_store_size_async(aclone_context* ctx, aclone_store* store,
                            double timeout, aclone_size_cb callback,
                            void* cookie)
    {
    using namespace std::placeholders;
    auto bf = bind(size_cb, _1, _2, callback, cookie);
    spawn<aclone::async_requester>(store->a, make_cow_tuple(atom("size")),
                                   timeout, bf);
    return 1;
    }
