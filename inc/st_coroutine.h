#pragma once

#include <stdint.h>
#include <functional>
#include <ucontext.h>

#include "st_scheduler.h"

namespace St_Coroutine 
{
enum class CoroutineState 
{
    RUNNING,
    RUNABLE,
    IO_WAIT,
    LOCK_WAIT,
    COND_WAIT,
    SLEEPING,
    SUSPENDED,
    ZOMBIE,
};

class Coroutine
{
public:
    Coroutine(std::function<void()> func, bool joinable, size_t stack_size);
    ~Coroutine();

    void run();

    bool join();

    void interrupt();

    ucontext_t& get_context() { return _ctx; }

    CoroutineState get_state() const { return _state; }

    void set_state(CoroutineState state) { _state = state; }

    bool is_joinable() const { return _joinable; }

    uint64_t get_timer_deadline() const { return _timer_deadline; }
    void set_timer_deadline(uint64_t deadline) { _timer_deadline = deadline; }

    int get_wait_fd() const { return _wait_fd; }
    void set_wait_fd(int fd) { _wait_fd = fd; }

    bool is_interrupted() const { return _interrupted; }
    void set_interrupted(bool interrupted) { _interrupted = interrupted; }

private:
    static void trampoline();

private:
    std::function<void()>   _func;
    bool                    _joinable;
    size_t                  _stack_size;
    char*                   _stack;
    ucontext_t              _ctx;
    CoroutineState          _state;
    uint64_t                _timer_deadline = 0;
    int                     _wait_fd = -1;
    free_list*              _pool = nullptr;
    std::set<Coroutine*>    _joiners;
    bool                    _interrupted = false;
};
} // namespace St_Coroutine
