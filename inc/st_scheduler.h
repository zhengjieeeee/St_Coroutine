#pragma once

#include <ucontext.h>
#include <functional>
#include <deque>
#include <map>
#include <set>
#include <sys/epoll.h>

#include "st_utils.h"

namespace St_Coroutine 
{
class Coroutine;

class Scheduler 
{
public:
    Scheduler();
    ~Scheduler();

    static Scheduler* get() { return _tls_scheduler; }

    bool init();

    Coroutine* spawn(std::function<void()> func, bool joinable = false, size_t stack_size = 64 * 1024);

    void run();

    bool sleep(uint64_t ms);

    void yield();

    void exit_current();

    void shut_down();

    Coroutine* get_current_coro() { return _current_coro; }

    ucontext_t& get_main_context() { return _main_ctx; }

    free_list& get_stack_pool() { return _stack_pool; }

    bool register_io(int fd, int op, uint64_t timeout_ms, Coroutine* coro);

    void wakeup(Coroutine* coro);

    void add_timer(Coroutine* coro, uint64_t deadline);

    void remove_timer(Coroutine* coro);

    void remove_io(Coroutine* coro);

private:
    void epoll_wait(int timeout);

    void process_timers();

    void process_io_events(epoll_event* events, int num_events);
    
private:
    inline static thread_local Scheduler* _tls_scheduler = nullptr;

    bool _shutdown;
    
    ucontext_t _main_ctx;
    int _epoll_fd;

    Coroutine* _current_coro;

    std::deque<Coroutine*> _ready_coroutines;

    std::set<std::pair<uint64_t, Coroutine*>> _timers;

    std::map<int, Coroutine*> _io_wait_map;

    free_list _stack_pool;
};
} // namespace St_Coroutine
