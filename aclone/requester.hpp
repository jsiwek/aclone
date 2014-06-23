#ifndef ACLONE_REQUESTER_HPP
#define ACLONE_REQUESTER_HPP

#include <string>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <cppa/cppa.hpp>

#include "aclone/aclone.h"

namespace aclone {

class sync_requester : public cppa::sb_actor<sync_requester> {
friend class cppa::sb_actor<sync_requester>;

public:

    sync_requester(const cppa::actor& store, std::mutex& mtx,
                   std::condition_variable& cv, int& flag,
                   const cppa::any_tuple& request, cppa::any_tuple& response)
        {
        using namespace cppa;
        using namespace std;
        monitor(store);
        on_sync_failure(
            [=]() { send(this, atom("quit"), false); });
        bootstrap = (
        after(std::chrono::seconds(0)) >> [=]()
            {
            become(requesting);
            send(this, atom("request"));
            }
        );
        requesting = (
        on(atom("request")) >> [=, &response]()
            {
            if ( store->exit_reason() == exit_reason::not_exited )
                {
                sync_send_tuple(store, request).then(
                    on_arg_match >> [=](down_msg& d)
                        {
                        send(this, atom("quit"), false);
                        },
                    others() >> [=, &response]()
                        {
                        response = last_dequeued();
                        send(this, atom("quit"), true);
                        }
                );
                }
            else
                {
                send(this, atom("quit"), false);
                }
            },
        on(atom("quit"), arg_match) >> [=, &flag, &mtx, &cv](bool rval)
            {
            flag = rval ? 1 : 0;
            unique_lock<mutex> guard{mtx};
            cv.notify_all();
            quit();
            }
        );
        }

private:

    cppa::behavior bootstrap;
    cppa::behavior requesting;
    cppa::behavior& init_state = bootstrap;
};

class async_requester : public cppa::sb_actor<async_requester> {
friend class cppa::sb_actor<async_requester>;
using async_cb = std::function<void (aclone_async_result,
                                     const cppa::any_tuple&)>;

public:

    async_requester(const cppa::actor& store, const cppa::any_tuple& request,
                    double timeout, async_cb cb)
        {
        using namespace cppa;
        using namespace std;
        monitor(store);
        on_sync_failure([=]()
            {
            cb(ACLONE_ASYNC_FAILURE, {});
            quit();
            }
        );
        on_sync_timeout([=]()
            {
            cb(ACLONE_ASYNC_TIMEOUT, {});
            quit();
            }
        );
        bootstrap = (
        after(std::chrono::seconds(0)) >> [=]()
            {
            become(requesting);
            send(this, atom("request"));
            }
        );
        requesting = (
        on(atom("request")) >> [=]()
            {
            if ( store->exit_reason() == exit_reason::not_exited )
                {
                timed_sync_send_tuple(store, chrono::duration<double>(timeout),
                                      request).then(
                    on_arg_match >> [=](down_msg& d)
                        {
                        cb(ACLONE_ASYNC_FAILURE, {});
                        quit();
                        },
                    others() >> [=]()
                        {
                        cb(ACLONE_ASYNC_SUCCESS, last_dequeued());
                        quit();
                        }
                );
                }
            else
                {
                cb(ACLONE_ASYNC_FAILURE, {});
                quit();
                }
            }
        );
        }

private:

    cppa::behavior bootstrap;
    cppa::behavior requesting;
    cppa::behavior& init_state = bootstrap;
};

} // namespace aclone

#endif // ACLONE_REQUESTER_HPP
