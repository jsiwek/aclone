#ifndef ACLONE_H
#define ACLONE_H

#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Key/Value Management

struct aclone_key {
	void* key;
	size_t size;
};

struct aclone_val {
	void* val;
	size_t size;
};

// Context Management

struct aclone_context;

aclone_context* aclone_context_create(int flags);

void aclone_context_destroy(aclone_context* ctx);

// Store Management

struct aclone_store;

const char* aclone_store_get_topic(const aclone_store* store);

aclone_store* aclone_store_open_master(aclone_context* ctx, const char* topic,
                                       int flags);

// TODO: eventually it should be possible for more than one master store
//       to be served over a single port.

int aclone_store_publish_master(aclone_context* ctx, aclone_store* master,
                                const char* addr, uint16_t port);

aclone_store* aclone_store_open_remote(aclone_context* ctx, const char* topic,
                                       const char* addr, uint16_t port,
                                       int flags);

aclone_store* aclone_store_open_cloner(aclone_context* ctx, const char* topic,
                                       const char* addr, uint16_t port,
                                       int flags);

int aclone_store_close(aclone_context* ctx, aclone_store* store);

// Store Updates

int aclone_store_clear(aclone_context* ctx, aclone_store* store);

int aclone_store_insert(aclone_context* ctx, aclone_store* store,
                        aclone_key key, aclone_val val);

int aclone_store_remove(aclone_context* ctx, aclone_store* store,
                        aclone_key key);

int aclone_store_increment(aclone_context* ctx, aclone_store* store,
                           aclone_key key, aclone_val by);

int aclone_store_decrement(aclone_context* ctx, aclone_store* store,
                           aclone_key key, aclone_val by);

// Store Queries

enum aclone_async_result {
    ACLONE_ASYNC_SUCCESS,
    ACLONE_ASYNC_FAILURE,
    ACLONE_ASYNC_TIMEOUT,
};

int aclone_store_lookup_sync(aclone_context* ctx, aclone_store* store,
                             aclone_key key, aclone_val* result);

typedef void (*aclone_lookup_cb)(aclone_async_result result, void* cookie,
                                 aclone_key key, aclone_val val);

int aclone_store_lookup_async(aclone_context* ctx, aclone_store* store,
                              aclone_key key, double timeout,
                              aclone_lookup_cb callback, void* cookie);

int aclone_store_haskey_sync(aclone_context* ctx, aclone_store* store,
                             aclone_key key, int* result);

typedef void (*aclone_haskey_cb)(aclone_async_result result, void* cookie,
                                 aclone_key key, int exists);

int aclone_store_haskey_async(aclone_context* ctx, aclone_store* store,
                              aclone_key key, double timeout,
                              aclone_haskey_cb callback, void* cookie);

int aclone_store_size_sync(aclone_context* ctx, aclone_store* store,
                           uint64_t* result);

typedef void (*aclone_size_cb)(aclone_async_result result, void* cookie,
                               uint64_t size);

int aclone_store_size_async(aclone_context* ctx, aclone_store* store,
                            double timeout, aclone_size_cb callback,
                            void* cookie);

// TODO: aclone_store_get_keys_{sync,async}

#ifdef __cplusplus
} // extern "C"
#endif

#endif // ACLONE_H
